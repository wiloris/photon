package main_test

/*
 * Тесты devtools-runtime НАСТОЯЩИМИ ПРОЦЕССАМИ.
 *
 * In-process проверка здесь ничего не стоила бы: весь смысл среза в том, что
 * runtime живёт отдельным процессом, управляется трубами родителя и умирает
 * вместе с ним. Ровно то же основание, по которому Phase 11 завела
 * tests/lifecycle: fakeSpawner и net.Pipe не порождают процессов, не имеют
 * кодов возврата и не воспроизводят тайминги настоящего завершения — три
 * реальных бага нашлись только на процессах.
 */

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

type ctlResponse struct {
	ID           int    `json:"id"`
	OK           bool   `json:"ok"`
	Cmd          string `json:"cmd"`
	Error        string `json:"error"`
	RuntimeState string `json:"runtime_state"`
	PluginID     string `json:"plugin_id"`
	PluginState  string `json:"plugin_state"`
	Granted      []string
	Isolation    *struct {
		Platform   string          `json:"platform"`
		Properties map[string]bool `json:"properties"`
	} `json:"isolation"`
	Operations []struct {
		Op         string `json:"op"`
		Authorized bool   `json:"authorized"`
	} `json:"operations"`
}

// lab — управляющая сторона: то, чем в проде будет Developer Tools.
type lab struct {
	cmd  *exec.Cmd
	in   io.WriteCloser
	out  *bufio.Scanner
	next int
}

// build собирает бинарь из исходников. Не `go run`: нужен настоящий процесс с
// предсказуемым PID и без промежуточного родителя, иначе проверка сиротства
// измеряла бы поведение go run, а не наше.
func build(t *testing.T, dir, out, pkg string) string {
	t.Helper()
	bin := filepath.Join(dir, out)
	cmd := exec.Command("go", "build", "-o", bin, pkg)
	if o, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("сборка %s: %v\n%s", pkg, err, o)
	}
	return bin
}

func startLab(t *testing.T) (*lab, string) {
	t.Helper()
	dir := t.TempDir()
	runtimeBin := build(t, dir, "devtools-runtime", ".")
	// Фикстура — существующий плагин лаборатории Phase 11, а не новый:
	// переиспользование проверенного стоит дешевле собственного двойника.
	pluginBin := build(t, dir, "fixture-plugin", "../../tests/plugins/hello")

	cmd := exec.Command(runtimeBin, "-plugin", pluginBin)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("запуск runtime: %v", err)
	}
	l := &lab{cmd: cmd, in: stdin, out: bufio.NewScanner(stdout)}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	})
	return l, pluginBin
}

func (l *lab) read(t *testing.T) ctlResponse {
	t.Helper()
	if !l.out.Scan() {
		t.Fatalf("управляющий канал закрылся раньше ответа: %v", l.out.Err())
	}
	var r ctlResponse
	if err := json.Unmarshal(l.out.Bytes(), &r); err != nil {
		t.Fatalf("ответ не разобран: %v (%q)", err, l.out.Text())
	}
	return r
}

func (l *lab) call(t *testing.T, cmd string) ctlResponse {
	t.Helper()
	l.next++
	if _, err := l.in.Write([]byte(`{"id":` + strconv.Itoa(l.next) + `,"cmd":"` + cmd + `"}` + "\n")); err != nil {
		t.Fatalf("команда %s не отправлена: %v", cmd, err)
	}
	r := l.read(t)
	if r.ID != l.next {
		t.Fatalf("ответ не на ту команду: id=%d, ожидался %d", r.ID, l.next)
	}
	return r
}

// alive — жив ли процесс. Сигнал 0 ничего не делает, но проверяет
// существование: единственный способ спросить это у ОС, не убивая.
func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// pluginPIDs — процессы фикстуры, живые в системе. Ищутся по имени бинаря:
// после смерти родителя спросить у runtime уже некого.
func pluginPIDs(t *testing.T, binary string) []int {
	t.Helper()
	out, err := exec.Command("pgrep", "-f", filepath.Base(binary)).Output()
	if err != nil {
		return nil // pgrep возвращает 1, когда не нашёл — это не сбой
	}
	var pids []int
	for _, line := range strings.Fields(string(out)) {
		if pid, err := strconv.Atoi(line); err == nil {
			pids = append(pids, pid)
		}
	}
	return pids
}

// ── жизненный цикл целиком ───────────────────────────────────────────────────

func TestLifecycle_StartReadyLoadDiagnoseShutdown(t *testing.T) {
	l, _ := startLab(t)

	ready := l.read(t)
	if !ready.OK || ready.Cmd != "ready" || ready.RuntimeState != "ready" {
		t.Fatalf("готовность не объявлена: %+v", ready)
	}

	if st := l.call(t, "status"); !st.OK || st.RuntimeState != "ready" {
		t.Fatalf("status: %+v", st)
	}

	loaded := l.call(t, "load")
	if !loaded.OK {
		t.Fatalf("load: %s", loaded.Error)
	}
	if loaded.PluginState != "running" || loaded.PluginID == "" {
		t.Fatalf("плагин не объявлен запущенным: %+v", loaded)
	}

	// Диагностика — то, ради чего лаборатория и существует.
	var diag ctlResponse
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		diag = l.call(t, "diagnose")
		if len(diag.Operations) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !diag.OK || diag.PluginState != "running" {
		t.Fatalf("diagnose: %+v", diag)
	}
	if len(diag.Granted) != 1 || diag.Granted[0] != "storage.read" {
		t.Fatalf("выданные права: %v, ожидалось [storage.read]", diag.Granted)
	}
	if diag.Isolation == nil || diag.Isolation.Platform == "" {
		t.Fatalf("изоляция не сообщена: %+v", diag.Isolation)
	}
	// Плагин обязан был дойти до storage.get: операция требует storage.read,
	// то есть её появление здесь доказывает, что путь авторизации ПРОЙДЕН, а
	// не обойдён.
	var sawGet bool
	for _, op := range diag.Operations {
		if op.Op == "storage.get" && op.Authorized {
			sawGet = true
		}
	}
	if !sawGet {
		t.Fatalf("плагин не выполнил storage.get: %+v", diag.Operations)
	}

	down := l.call(t, "shutdown")
	if !down.OK || down.RuntimeState != "stopped" {
		t.Fatalf("shutdown: %+v", down)
	}
	if err := l.cmd.Wait(); err != nil {
		t.Fatalf("runtime не завершился чисто: %v", err)
	}
}

// ── сиротство ────────────────────────────────────────────────────────────────

// Смерть родителя обязана уносить и runtime, и плагин (ADR-057 §9).
//
// Родитель убивается СИГНАЛОМ KILL, а не закрытием канала: перехватить его
// нельзя, и это худший случай — ровно тот, при котором «мы аккуратно
// завершаемся в defer» не работает. Гарантию даёт закрытие труб: ОС закрывает
// их при смерти процесса, runtime видит EOF на stdin и останавливает плагин.
func TestOrphan_ParentDeathStopsRuntimeAndPlugin(t *testing.T) {
	dir := t.TempDir()
	runtimeBin := build(t, dir, "devtools-runtime", ".")
	pluginBin := build(t, dir, "fixture-plugin", "../../tests/plugins/hello")

	// Промежуточный родитель: убиваем ЕГО, а не тест, и наблюдаем судьбу
	// внуков. Без него пришлось бы убивать сам go test.
	parent := exec.Command("sh", "-c",
		`exec "$1" -plugin "$2" <&0`, "sh", runtimeBin, pluginBin)
	stdin, err := parent.StdinPipe()
	if err != nil {
		t.Fatalf("stdin: %v", err)
	}
	stdout, err := parent.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout: %v", err)
	}
	parent.Stderr = os.Stderr
	if err := parent.Start(); err != nil {
		t.Fatalf("запуск: %v", err)
	}

	out := bufio.NewScanner(stdout)
	if !out.Scan() {
		t.Fatalf("готовность не пришла")
	}
	if _, err := stdin.Write([]byte(`{"id":1,"cmd":"load"}` + "\n")); err != nil {
		t.Fatalf("load: %v", err)
	}
	if !out.Scan() {
		t.Fatalf("ответ на load не пришёл")
	}
	var loaded ctlResponse
	if err := json.Unmarshal(out.Bytes(), &loaded); err != nil || !loaded.OK {
		t.Fatalf("плагин не загрузился: %v %q", err, out.Text())
	}

	pidsBefore := pluginPIDs(t, pluginBin)
	if len(pidsBefore) == 0 {
		t.Fatalf("процесс плагина не найден — проверять сиротство не на чем")
	}
	runtimePID := parent.Process.Pid

	// Родитель умирает НЕОЖИДАННО.
	if err := parent.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_ = parent.Wait()
	// Трубы держит и тестовый процесс: закрываем свою сторону, иначе EOF на
	// stdin у потомка не наступит и проверка измерит нашу забывчивость.
	_ = stdin.Close()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if !alive(runtimePID) && len(pluginPIDs(t, pluginBin)) == 0 {
			return // ни runtime, ни плагина — сирот нет
		}
		time.Sleep(200 * time.Millisecond)
	}

	leftover := pluginPIDs(t, pluginBin)
	for _, pid := range leftover { // не оставлять мусор после красного теста
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
	t.Fatalf("СИРОТЫ: runtime жив=%v, процессов плагина=%d — родитель мёртв, "+
		"а runtime с плагинами пережил его (ADR-057 §9)", alive(runtimePID), len(leftover))
}

// Закрытие управляющего канала — штатный путь того же требования: родитель
// ушёл аккуратно, плагин всё равно обязан остановиться.
func TestOrphan_ClosedControlChannelStopsPlugin(t *testing.T) {
	l, pluginBin := startLab(t)
	l.read(t) // ready
	if r := l.call(t, "load"); !r.OK {
		t.Fatalf("load: %s", r.Error)
	}
	if len(pluginPIDs(t, pluginBin)) == 0 {
		t.Fatalf("процесс плагина не найден")
	}

	if err := l.in.Close(); err != nil {
		t.Fatalf("закрытие канала: %v", err)
	}
	if err := l.cmd.Wait(); err != nil {
		t.Fatalf("runtime не завершился после закрытия канала: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(pluginPIDs(t, pluginBin)) == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("плагин пережил закрытие управляющего канала")
}

// ── границы протокола ────────────────────────────────────────────────────────

func TestControl_UnknownCommandIsRefusedWithoutKillingChannel(t *testing.T) {
	l, _ := startLab(t)
	l.read(t)

	r := l.call(t, "definitely-not-a-command")
	if r.OK || !strings.Contains(r.Error, "unknown command") {
		t.Fatalf("неизвестная команда принята: %+v", r)
	}
	// Канал обязан пережить отказ: иначе одна опечатка родителя роняла бы
	// весь экземпляр.
	if st := l.call(t, "status"); !st.OK {
		t.Fatalf("канал умер после неизвестной команды: %+v", st)
	}
}

func TestControl_SecondLoadIsRefused(t *testing.T) {
	l, _ := startLab(t)
	l.read(t)
	if r := l.call(t, "load"); !r.OK {
		t.Fatalf("первый load: %s", r.Error)
	}
	if r := l.call(t, "load"); r.OK {
		t.Fatalf("второй load принят — экземпляр обязан быть один")
	}
}

// diagnose до загрузки — не ошибка, а честный ответ «плагина нет».
func TestControl_DiagnoseBeforeLoadReportsNotLoaded(t *testing.T) {
	l, _ := startLab(t)
	l.read(t)
	r := l.call(t, "diagnose")
	if !r.OK || r.PluginState != "not loaded" {
		t.Fatalf("diagnose до load: %+v", r)
	}
	if len(r.Operations) != 0 {
		t.Fatalf("операции до загрузки: %+v", r.Operations)
	}
}
