package sandbox

import (
	"errors"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Тесты по чек-листу docs/sandbox.md, раздел 8. Главный принцип:
// проверяется ЧЕСТНОСТЬ отчёта, а не наличие полей. Реализация, которая
// отмечает свойство применённым, не применив его, должна падать.

// ── Классификация свойств ───────────────────────────────────────────

func TestMandatoryProperties(t *testing.T) {
	// P1, P6, P7 обязательны: без них изоляции нет вообще.
	for _, p := range []Property{PropSeparateProcess, PropSingleChannel, PropTerminable} {
		if !IsMandatory(p) {
			t.Fatalf("%s must be mandatory", p)
		}
	}
	// Остальные best-effort: их недостижимость не запрещает запуск.
	for _, p := range []Property{
		PropFilesystemLimit, PropNetworkLimit, PropResourceLimit, PropNoEscalation,
	} {
		if IsMandatory(p) {
			t.Fatalf("%s must be best-effort, not mandatory", p)
		}
	}
}

// ── Verify: обязательные свойства ───────────────────────────────────

func TestVerify_AllMandatoryPresent(t *testing.T) {
	a := Applied{Properties: map[Property]bool{
		PropSeparateProcess: true,
		PropSingleChannel:   true,
		PropTerminable:      true,
	}}
	if err := Verify(a); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestVerify_MissingMandatoryRejectsLaunch(t *testing.T) {
	// Отказ запуска, а не предупреждение: без P1 плагин исполнялся бы в
	// адресном пространстве Runtime.
	a := Applied{Properties: map[Property]bool{
		PropSeparateProcess: false,
		PropSingleChannel:   true,
		PropTerminable:      true,
	}}
	err := Verify(a)
	if !errors.Is(err, ErrMandatoryPropertyUnavailable) {
		t.Fatalf("expected ErrMandatoryPropertyUnavailable, got %v", err)
	}
	if !strings.Contains(err.Error(), string(PropSeparateProcess)) {
		t.Fatalf("error must name the missing property, got: %v", err)
	}
}

func TestVerify_MissingBestEffortAllowsLaunch(t *testing.T) {
	// Best-effort свойства не блокируют запуск — но обязаны попасть в
	// Warnings (проверяется отдельно).
	a := Applied{Properties: map[Property]bool{
		PropSeparateProcess: true,
		PropSingleChannel:   true,
		PropTerminable:      true,
		PropFilesystemLimit: false,
		PropNetworkLimit:    false,
	}}
	if err := Verify(a); err != nil {
		t.Fatalf("missing best-effort properties must not block launch: %v", err)
	}
}

func TestVerify_ReportsAllMissingMandatory(t *testing.T) {
	a := Applied{Properties: map[Property]bool{}}
	err := Verify(a)
	if err == nil {
		t.Fatal("expected an error when nothing is applied")
	}
	for _, p := range mandatoryProperties {
		if !strings.Contains(err.Error(), string(p)) {
			t.Fatalf("error must list %s, got: %v", p, err)
		}
	}
}

// ── None: честность заглушки ────────────────────────────────────────

func TestNone_IsDistinguishableFromRealSandbox(t *testing.T) {
	// Отсутствие sandbox должно быть явно отличимо от применённого
	// sandbox (чек-лист, п.6).
	a, err := NewNone().Prepare(exec.Command("true"), Spec{})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if a.Platform != "none" {
		t.Fatalf("platform = %q, want %q", a.Platform, "none")
	}
	if len(a.Warnings) == 0 {
		t.Fatal("None must warn that no OS-level isolation is applied")
	}
}

func TestNone_DoesNotClaimLayer2(t *testing.T) {
	// Ключевой тест честности: заглушка не должна отмечать
	// применёнными свойства, которых не обеспечивает.
	a, _ := NewNone().Prepare(exec.Command("true"), Spec{})

	for _, p := range []Property{
		PropFilesystemLimit, PropNetworkLimit, PropResourceLimit, PropNoEscalation,
	} {
		if a.Has(p) {
			t.Fatalf("None claims %s is applied — it provides no OS-level isolation", p)
		}
	}
}

func TestNone_ProvidesLayer1(t *testing.T) {
	// Layer 1 обеспечивается фактом запуска отдельного процесса через
	// host, поэтому присутствует даже без OS-изоляции.
	a, _ := NewNone().Prepare(exec.Command("true"), Spec{})
	if err := Verify(a); err != nil {
		t.Fatalf("None must still satisfy Layer 1: %v", err)
	}
}

func TestNone_WarningMentionsWhatIsMissing(t *testing.T) {
	// Предупреждение должно быть конкретным, а не "изоляция неполная".
	a, _ := NewNone().Prepare(exec.Command("true"), Spec{})
	joined := strings.ToLower(strings.Join(a.Warnings, " "))
	for _, word := range []string{"filesystem", "network", "resource"} {
		if !strings.Contains(joined, word) {
			t.Fatalf("warnings must name what is not restricted; %q missing from: %v", word, a.Warnings)
		}
	}
}

func TestNone_SetsWorkDir(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command("true")
	if _, err := NewNone().Prepare(cmd, Spec{PackageRoot: dir}); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if cmd.Dir != dir {
		t.Fatalf("cmd.Dir = %q, want %q", cmd.Dir, dir)
	}
}

func TestNone_CleanupIsIdempotent(t *testing.T) {
	s := NewNone()
	for i := 0; i < 3; i++ {
		if err := s.Cleanup(); err != nil {
			t.Fatalf("Cleanup #%d: %v", i+1, err)
		}
	}
}

// ── Тихая деградация запрещена ──────────────────────────────────────

func TestNoSilentDegradation_EveryUnappliedPropertyIsExplained(t *testing.T) {
	// Главный инвариант раздела 6: не должно существовать пути, где
	// ограничение не применилось, и об этом не сообщили.
	//
	// Проверяем обе реализации одним правилом.
	sandboxes := map[string]Sandbox{"none": NewNone()}
	if runtime.GOOS == "linux" {
		sandboxes["linux"] = NewLinux()
	}

	for name, sb := range sandboxes {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			a, err := sb.Prepare(exec.Command("true"), Spec{PackageRoot: dir})
			if err != nil {
				t.Fatalf("Prepare: %v", err)
			}

			// Считаем best-effort свойства, которые НЕ применены.
			var unapplied []Property
			for _, p := range bestEffortProperties {
				if !a.Has(p) {
					unapplied = append(unapplied, p)
				}
			}

			if len(unapplied) > 0 && len(a.Warnings) == 0 {
				t.Fatalf("%d best-effort properties are not applied (%v) but no warnings were "+
					"produced — this is exactly the silent degradation the spec forbids",
					len(unapplied), unapplied)
			}
		})
	}
}

func TestApplied_StringShowsBothAppliedAndMissing(t *testing.T) {
	// Сводка для логов должна показывать и то, и другое: строка,
	// перечисляющая только применённое, вводила бы в заблуждение.
	a := Applied{
		Platform: "test",
		Properties: map[Property]bool{
			PropSeparateProcess: true,
			PropFilesystemLimit: false,
		},
		Warnings: []string{"fs not restricted"},
	}
	s := a.String()
	if !strings.Contains(s, string(PropSeparateProcess)) {
		t.Fatalf("String must list applied properties: %s", s)
	}
	if !strings.Contains(s, string(PropFilesystemLimit)) {
		t.Fatalf("String must list missing properties: %s", s)
	}
	if !strings.Contains(s, "fs not restricted") {
		t.Fatalf("String must include warnings: %s", s)
	}
}

// ── Linux ───────────────────────────────────────────────────────────

func TestLinux_Prepare(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux-specific: current platform is " + runtime.GOOS)
	}

	dir := t.TempDir()
	cmd := exec.Command("true")
	a, err := NewLinux().Prepare(cmd, Spec{PackageRoot: dir})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	if a.Platform != "linux" {
		t.Fatalf("platform = %q", a.Platform)
	}
	if err := Verify(a); err != nil {
		t.Fatalf("Linux must satisfy mandatory properties: %v", err)
	}
	if cmd.Dir != dir {
		t.Fatalf("cmd.Dir = %q, want %q", cmd.Dir, dir)
	}
}

func TestLinux_SetsProcessGroup(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux-specific: current platform is " + runtime.GOOS)
	}
	// Отдельная process group нужна, чтобы SIGKILL по группе завершил и
	// плагин, и всё, что он породил. Без этого P7 выполнялось бы
	// формально, но не фактически.
	cmd := exec.Command("true")
	if _, err := NewLinux().Prepare(cmd, Spec{PackageRoot: t.TempDir()}); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatal("Setpgid was not set: killing the group would not reach the plugin's children")
	}
}

func TestLinux_SetsPdeathsig(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux-specific: current platform is " + runtime.GOOS)
	}
	// Без Pdeathsig аварийная смерть Runtime оставила бы процессы
	// плагинов сиротами, работающими без надзора.
	cmd := exec.Command("true")
	NewLinux().Prepare(cmd, Spec{PackageRoot: t.TempDir()})
	if cmd.SysProcAttr == nil || cmd.SysProcAttr.Pdeathsig == 0 {
		t.Fatal("Pdeathsig was not set: plugins would survive a Runtime crash as orphans")
	}
}

func TestLinux_DoesNotClaimUnimplementedProperties(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux-specific: current platform is " + runtime.GOOS)
	}
	// Честность важнее полноты: реализация не должна отмечать
	// применёнными namespace/seccomp/cgroups, которых не делает.
	a, _ := NewLinux().Prepare(exec.Command("true"), Spec{PackageRoot: t.TempDir()})

	if a.Has(PropFilesystemLimit) {
		t.Fatal("Linux claims filesystem_limit, but mount namespace/chroot are not implemented")
	}
	if a.Has(PropNetworkLimit) {
		t.Fatal("Linux claims network_limit, but network namespace is not implemented")
	}
	if a.Has(PropNoEscalation) {
		t.Fatal("Linux claims no_escalation, but PR_SET_NO_NEW_PRIVS is not applied")
	}
}

func TestLinux_ResourceLimitsRequestedButNotApplied(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux-specific: current platform is " + runtime.GOOS)
	}
	// Лимиты запрошены, но применить их корректно нельзя (setrlimit в
	// Go затронул бы сам Runtime). Обязаны сообщить, а не промолчать.
	a, err := NewLinux().Prepare(exec.Command("true"), Spec{
		PackageRoot:    t.TempDir(),
		MaxMemoryBytes: 100 << 20,
	})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if a.Has(PropResourceLimit) {
		t.Fatal("Linux claims resource_limit was applied — setrlimit in Go would limit the Runtime itself")
	}

	found := false
	for _, w := range a.Warnings {
		if strings.Contains(w, "resource limits") {
			found = true
		}
	}
	if !found {
		t.Fatalf("requested-but-unapplied limits must produce a warning, got: %v", a.Warnings)
	}
}

func TestLinux_NoWarningWhenLimitsNotRequested(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux-specific: current platform is " + runtime.GOOS)
	}
	// Разница между «не применили запрошенное» и «нечего было
	// применять»: во втором случае предупреждение о лимитах было бы
	// шумом.
	a, _ := NewLinux().Prepare(exec.Command("true"), Spec{PackageRoot: t.TempDir()})
	for _, w := range a.Warnings {
		if strings.Contains(w, "resource limits were requested") {
			t.Fatal("warned about resource limits that were never requested")
		}
	}
}

func TestLinux_MissingWorkDirIsError(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux-specific: current platform is " + runtime.GOOS)
	}
	_, err := NewLinux().Prepare(exec.Command("true"), Spec{
		PackageRoot: filepath.Join(t.TempDir(), "does-not-exist"),
	})
	if err == nil {
		t.Fatal("Prepare must fail when the package root does not exist")
	}
}

func TestLinux_CleanupIsIdempotent(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux-specific: current platform is " + runtime.GOOS)
	}
	s := NewLinux()
	for i := 0; i < 3; i++ {
		if err := s.Cleanup(); err != nil {
			t.Fatalf("Cleanup #%d: %v", i+1, err)
		}
	}
}

// ── P7 не должно ломаться другими свойствами ────────────────────────

func TestTerminability_SurvivesSandboxPreparation(t *testing.T) {
	// Чек-лист, п.5: sandbox не должен помешать принудительному
	// завершению. Запускаем настоящий долгий процесс и убиваем его.
	if runtime.GOOS != "linux" {
		t.Skip("requires a POSIX shell: current platform is " + runtime.GOOS)
	}

	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh not available: " + err.Error())
	}

	cmd := exec.Command(sh, "-c", "sleep 30")
	a, err := NewLinux().Prepare(cmd, Spec{PackageRoot: t.TempDir()})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if !a.Has(PropTerminable) {
		t.Fatal("terminable was not claimed")
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}

	select {
	case <-done:
		// Процесс завершился — свойство подтверждено на практике, а не
		// только заявлено.
	case <-time.After(5 * time.Second):
		t.Fatal("process survived Kill: terminability is claimed but not real")
	}
}
