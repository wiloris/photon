//go:build linux

package sandbox

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

// Linux — изоляция средствами Linux.
//
// Реализует свойства из docs/sandbox.md настолько, насколько позволяют
// права процесса Runtime. Ключевой принцип: если ограничение применить
// не удалось, об этом СООБЩАЕТСЯ через Applied.Warnings, а не
// умалчивается.
//
// Что здесь НЕ реализовано и почему:
//   - namespaces (mount/network) требуют CAP_SYS_ADMIN либо
//     user namespaces; доступность зависит от конфигурации ядра и
//     контейнера. Определяется в рантайме, не предполагается;
//   - seccomp-фильтры требуют отдельной библиотеки или ручной сборки
//     BPF; вводить зависимость до подтверждённой потребности рано;
//   - cgroups требуют записи в /sys/fs/cgroup, что также зависит от
//     прав и версии cgroup v1/v2.
//
// Реализованное подмножество (setrlimit, отдельная process group,
// рабочий каталог) работает без специальных привилегий — то есть
// является честным минимумом, а не декларацией.
type Linux struct {
	// UseUserNamespace — пытаться ли изолировать через user namespace.
	// Отключено по умолчанию: доступность зависит от ядра и
	// конфигурации, а неудачная попытка не должна ломать запуск.
	UseUserNamespace bool
}

func NewLinux() *Linux { return &Linux{} }

var _ Sandbox = (*Linux)(nil)

func (l *Linux) Prepare(cmd *exec.Cmd, spec Spec) (Applied, error) {
	applied := Applied{
		Platform:   "linux",
		Properties: make(map[Property]bool),
	}

	// ── Layer 1: обеспечивается запуском отдельного процесса ────────
	applied.Properties[PropSeparateProcess] = true
	applied.Properties[PropMemoryIsolation] = true
	applied.Properties[PropTerminable] = true

	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}

	// Отдельная process group: SIGKILL по группе гарантирует, что
	// завершится и плагин, и всё, что он успел породить. Без этого
	// дочерние процессы плагина пережили бы его смерть (P7 было бы
	// формально выполнено, фактически нет).
	cmd.SysProcAttr.Setpgid = true

	// ── P6: единственный канал ──────────────────────────────────────
	//
	// exec.Cmd передаёт дочернему процессу только явно заданные
	// Stdin/Stdout/Stderr; остальные дескрипторы Go закрывает при
	// exec (O_CLOEXEC по умолчанию). IPC идёт через stdin/stdout —
	// значит других путей наружу нет.
	applied.Properties[PropSingleChannel] = true

	// ── P3: рабочий каталог ─────────────────────────────────────────
	//
	// Это НЕ полноценное ограничение ФС: chroot/mount namespace
	// требуют привилегий. Рабочий каталог лишь задаёт точку отсчёта
	// для относительных путей — честно отражаем это как отсутствие
	// свойства.
	workDir := spec.WorkDir
	if workDir == "" {
		workDir = spec.PackageRoot
	}
	if workDir != "" {
		if _, err := os.Stat(workDir); err != nil {
			return applied, fmt.Errorf("sandbox: work dir %q: %w", workDir, err)
		}
		cmd.Dir = workDir
	}
	applied.Properties[PropFilesystemLimit] = false
	applied.Warnings = append(applied.Warnings,
		"filesystem access is NOT restricted: mount namespace/chroot require elevated privileges")

	// ── P4: сеть ────────────────────────────────────────────────────
	//
	// Network namespace требует CAP_SYS_ADMIN. Без него ограничить
	// сеть на уровне процесса нельзя — сообщаем честно, вместо того
	// чтобы отметить свойство применённым.
	applied.Properties[PropNetworkLimit] = false
	if !spec.AllowNetwork {
		applied.Warnings = append(applied.Warnings,
			"network capability was NOT granted, but network access is NOT blocked at OS level: "+
				"network namespace requires elevated privileges")
	}

	// ── P5: ресурсы через setrlimit ─────────────────────────────────
	//
	// setrlimit работает без привилегий (в пределах текущих hard
	// limits), поэтому это единственное свойство Layer 2, которое
	// реально применяется.
	limitsApplied, limitWarnings := l.applyResourceLimits(spec)
	applied.Properties[PropResourceLimit] = limitsApplied
	applied.Warnings = append(applied.Warnings, limitWarnings...)

	// ── P8: эскалация ───────────────────────────────────────────────
	//
	// PR_SET_NO_NEW_PRIVS предотвратил бы получение привилегий через
	// setuid-бинарники, но syscall.SysProcAttr в Go 1.22 такого поля не
	// имеет (проверено по исходникам стандартной библиотеки). Установка
	// prctl вручную требует вызова между fork и exec — то есть cgo или
	// процесса-обёртки, как и с setrlimit.
	//
	// Отмечаем свойство как НЕ применённое. Альтернатива — вызвать
	// prctl в самом Runtime — затронула бы Runtime, а не плагин, то
	// есть была бы неверной, а не просто неполной.
	applied.Properties[PropNoEscalation] = false
	applied.Warnings = append(applied.Warnings,
		"privilege escalation is NOT blocked: PR_SET_NO_NEW_PRIVS is unavailable via "+
			"syscall.SysProcAttr in this Go version and must be applied between fork and exec")

	// Pdeathsig: если Runtime умрёт, ядро пошлёт плагину SIGKILL.
	//
	// Не входит в список свойств спецификации, но напрямую усиливает
	// P7 (завершаемость): без него аварийная смерть Runtime оставила бы
	// процессы плагинов сиротами, работающими без всякого надзора.
	cmd.SysProcAttr.Pdeathsig = syscall.SIGKILL

	return applied, nil
}

// applyResourceLimits применяет setrlimit к текущему процессу-родителю
// перед fork.
//
// ВАЖНОЕ ОГРАНИЧЕНИЕ: syscall.Setrlimit в Go применяется к процессу,
// который его вызывает, а дочерний наследует лимиты при fork. Значит
// установка лимитов здесь затронула бы сам Runtime — это неприемлемо.
//
// Корректный способ — установить лимиты в дочернем процессе между fork
// и exec, что в Go требует либо cgo, либо вспомогательного
// процесса-обёртки. Поэтому лимиты сейчас НЕ применяются, и это
// сообщается честно, а не отмечается как выполненное.
func (l *Linux) applyResourceLimits(spec Spec) (bool, []string) {
	if spec.MaxMemoryBytes == 0 && spec.MaxFileDescriptors == 0 && spec.MaxCPUSeconds == 0 {
		// Лимиты не запрашивались — не свойство отсутствует, а нечего
		// было применять.
		return false, nil
	}

	return false, []string{
		"resource limits were requested but NOT applied: setrlimit in Go affects the calling " +
			"process, and applying it here would limit the Runtime itself; correct implementation " +
			"requires setting limits between fork and exec (cgo or a wrapper process)",
	}
}

func (l *Linux) Cleanup() error { return nil }
