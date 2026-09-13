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
 *
 * С B4b исполнитель — настоящий coreops (ADR-058, docs/plugin-lab.md). Эти
 * тесты проверяют ЛАБОРАТОРИЮ: канал, каталог, композицию, уборку. Свойства
 * S1–S7 здесь не доказываются — это делает B4a
 * (tests/lifecycle/sdkscenario_test.go).
 */

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type labV struct {
	KeyMaterial           string `json:"key_material"`
	Storage               string `json:"storage"`
	StorageDir            string `json:"storage_dir"`
	ProductionComposition bool   `json:"production_composition"`
}

type scenarioV struct {
	ID          string   `json:"id"`
	Fixture     string   `json:"fixture"`
	PluginID    string   `json:"plugin_id"`
	Granted     []string `json:"granted"`
	Namespace   string   `json:"namespace"`
	Description string   `json:"description"`
}

type ctlResponse struct {
	ID           int               `json:"id"`
	OK           bool              `json:"ok"`
	Cmd          string            `json:"cmd"`
	Error        string            `json:"error"`
	RuntimeState string            `json:"runtime_state"`
	Scenario     string            `json:"scenario"`
	Fixture      string            `json:"fixture"`
	Namespace    string            `json:"namespace"`
	PluginID     string            `json:"plugin_id"`
	PluginState  string            `json:"plugin_state"`
	Granted      []string          `json:"granted"`
	Results      map[string]string `json:"results"`
	Scenarios    []scenarioV       `json:"scenarios"`
	Lab          *labV             `json:"lab"`
	Isolation    *struct {
		Platform   string          `json:"platform"`
		Properties map[string]bool `json:"properties"`
	} `json:"isolation"`
	Operations []struct {
		Op         string `json:"op"`
		Authorized bool   `json:"authorized"`
	} `json:"operations"`
}

func (r ctlResponse) ops() []string {
	var out []string
	for _, o := range r.Operations {
		out = append(out, o.Op)
	}
	return out
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// ── сборка: один раз на прогон пакета ────────────────────────────────────────

var (
	buildOnce sync.Once
	buildDir  string
	buildErr  error
)

type binaries struct{ runtime, hello, sdkscenario string }

func TestMain(m *testing.M) {
	code := m.Run()
	if buildDir != "" {
		_ = os.RemoveAll(buildDir)
	}
	os.Exit(code)
}

// bins собирает бинари из исходников. Не `go run`: нужен настоящий процесс с
// предсказуемым PID и без промежуточного родителя, иначе проверка сиротства
// измеряла бы поведение go run, а не наше.
func bins(t *testing.T) binaries {
	t.Helper()
	buildOnce.Do(func() {
		buildDir, buildErr = os.MkdirTemp("", "devtools-runtime-test-*")
		if buildErr != nil {
			return
		}
		for out, pkg := range map[string]string{
			"devtools-runtime": ".",
			"fixture-hello":    "../../tests/plugins/hello",
			"fixture-sdk":      "../../tests/plugins/sdkscenario",
		} {
			if o, err := exec.Command("go", "build", "-o", filepath.Join(buildDir, out), pkg).CombinedOutput(); err != nil {
				buildErr = errors.New("сборка " + pkg + ": " + err.Error() + "\n" + string(o))
				return
			}
		}
	})
	if buildErr != nil {
		t.Fatalf("%v", buildErr)
	}
	return binaries{
		runtime:     filepath.Join(buildDir, "devtools-runtime"),
		hello:       filepath.Join(buildDir, "fixture-hello"),
		sdkscenario: filepath.Join(buildDir, "fixture-sdk"),
	}
}

func (b binaries) args() []string {
	return []string{"-fixture-hello", b.hello, "-fixture-sdkscenario", b.sdkscenario}
}

// ── лаборатория как родитель ─────────────────────────────────────────────────

// lab — управляющая сторона: то, чем в проде будет Developer Tools.
type lab struct {
	cmd   *exec.Cmd
	in    io.WriteCloser
	out   *bufio.Scanner
	next  int
	ready ctlResponse
}

func startLabEnv(t *testing.T, extraEnv []string) *lab {
	t.Helper()
	b := bins(t)
	cmd := exec.Command(b.runtime, b.args()...)
	if extraEnv != nil {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
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
	l.ready = l.read(t)
	if !l.ready.OK || l.ready.Cmd != "ready" || l.ready.RuntimeState != "ready" {
		t.Fatalf("готовность не объявлена: %+v", l.ready)
	}
	return l
}

func startLab(t *testing.T) *lab { return startLabEnv(t, nil) }

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

// send отправляет кадр как есть и читает ответ, не сверяя id: нужен для
// кадров, которые runtime обязан отвергнуть до разбора id.
func (l *lab) send(t *testing.T, frame string) ctlResponse {
	t.Helper()
	if _, err := l.in.Write([]byte(frame + "\n")); err != nil {
		t.Fatalf("кадр не отправлен: %v", err)
	}
	return l.read(t)
}

func (l *lab) call(t *testing.T, cmd string) ctlResponse {
	t.Helper()
	l.next++
	r := l.send(t, `{"id":`+strconv.Itoa(l.next)+`,"cmd":"`+cmd+`"}`)
	if r.ID != l.next {
		t.Fatalf("ответ не на ту команду: id=%d, ожидался %d", r.ID, l.next)
	}
	return r
}

func (l *lab) load(t *testing.T, scenario string) ctlResponse {
	t.Helper()
	l.next++
	r := l.send(t, `{"id":`+strconv.Itoa(l.next)+`,"cmd":"load","scenario":"`+scenario+`"}`)
	if r.ID != l.next {
		t.Fatalf("ответ не на ту команду: id=%d, ожидался %d", r.ID, l.next)
	}
	return r
}

// settle ждёт, пока плагин сообщит о завершении сценария ("done" у
// sdkscenario, "ready" у hello), и возвращает последнюю диагностику.
func (l *lab) settle(t *testing.T) ctlResponse {
	t.Helper()
	var diag ctlResponse
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		diag = l.call(t, "diagnose")
		if _, done := diag.Results["done"]; done {
			return diag
		}
		if _, ready := diag.Results["ready"]; ready {
			return diag
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("сценарий не завершился: %+v", diag)
	return diag
}

func (l *lab) shutdownAndWait(t *testing.T) {
	t.Helper()
	down := l.call(t, "shutdown")
	if !down.OK || down.RuntimeState != "stopped" {
		t.Fatalf("shutdown: %+v", down)
	}
	if err := l.cmd.Wait(); err != nil {
		t.Fatalf("runtime не завершился чисто: %v", err)
	}
}

// alive — жив ли процесс. Сигнал 0 ничего не делает, но проверяет
// существование: единственный способ спросить это у ОС, не убивая.
func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// pluginPIDs — процессы фикстуры, живые в системе. После смерти родителя
// спросить у runtime уже некого, поэтому ищем в /proc.
//
// Сверка идёт по ИСПОЛНЯЕМОМУ ФАЙЛУ (/proc/PID/exe), а не по командной строке.
// Первая редакция звала `pgrep -f <имя бинаря>` и давала ЛОЖНЫЕ СРАБАТЫВАНИЯ:
// под шаблон попадает любой процесс, в чьей cmdline встретилась строка, — в
// первую очередь сам `go build -o .../fixture-plugin`, а заодно и оболочка,
// запустившая прогон. Изолированно тест был зелёным, а в общем `go test ./...`
// сборки соседних пакетов идут параллельно, живут дольше и попадают в окно
// опроса: тест сообщал о сиротах, которых нет. Найдено обязательным
// послекоммитным прогоном; та же ошибка ранее встретилась в живой проверке и
// была там исправлена этим же способом.
func pluginPIDs(t *testing.T, binary string) []int {
	t.Helper()
	want, err := filepath.EvalSymlinks(binary)
	if err != nil {
		want = binary
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatalf("чтение /proc: %v", err)
	}
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		exe, err := os.Readlink(filepath.Join("/proc", e.Name(), "exe"))
		if err != nil {
			continue // чужой процесс либо уже умер — и то и другое не наше дело
		}
		if exe == want {
			pids = append(pids, pid)
		}
	}
	return pids
}

// ── жизненный цикл целиком ───────────────────────────────────────────────────

func TestLifecycle_StartReadyLoadDiagnoseShutdown(t *testing.T) {
	l := startLab(t)

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
	diag := l.settle(t)
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
	if !contains(diag.ops(), "storage.get") {
		t.Fatalf("плагин не выполнил storage.get: %v", diag.ops())
	}
	l.shutdownAndWait(t)
}

// ── сиротство ────────────────────────────────────────────────────────────────

// Смерть процесса runtime от SIGKILL не должна оставлять плагин сиротой
// (ADR-057 §9).
//
// ФАКТИЧЕСКИЙ МЕХАНИЗМ ЭТОГО ТЕСТА. `sh -c 'exec …'` заменяет оболочку самим
// runtime, поэтому Kill ниже — SIGKILL САМОМУ runtime, а не промежуточному
// родителю. Перехватить его нельзя: ни остановка плагина, ни уборка в runtime
// не выполняются. Плагин уходит не потому, что runtime увидел EOF, — runtime
// уже мёртв, — а потому, что ОС закрывает трубы убитого процесса и у плагина
// заканчивается его собственный канал к runtime. Это подтверждено мутацией:
// с отключённым сторожем EOF в runtime этот тест проходит.
//
// Сторож EOF runtime (штатный уход родителя, закрытый stdin) проверяет
// TestOrphan_ClosedControlChannelStopsPlugin — под той же мутацией падает он.
//
// Итого здесь проверяется: после SIGKILL runtime не остаётся ни runtime, ни
// плагина. И здесь же видно, что уборка временного хранилища после SIGKILL не
// гарантируется (ADR-058 п. 3): тест убирает каталог сам, ничего не утверждая
// о нём.
func TestOrphan_ParentDeathStopsRuntimeAndPlugin(t *testing.T) {
	b := bins(t)

	// Запуск через sh с exec: PID sh становится PID runtime, и Kill приходит
	// ровно в runtime. Без этого пришлось бы убивать сам go test.
	parent := exec.Command("sh", "-c",
		`exec "$1" -fixture-hello "$2" -fixture-sdkscenario "$3" <&0`, "sh", b.runtime, b.hello, b.sdkscenario)
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
	// Kill ниже — SIGKILL самому runtime: уборка временного хранилища в этом
	// случае НЕ гарантируется (ADR-058 п. 3). Тест ничего не утверждает об
	// этом каталоге — поведение runtime после SIGKILL не контракт. Он убирает
	// ЗА СОБОЙ: каталог, который создал именно этот экземпляр и сообщил в
	// ready. Уборка регистрируется ДО проверок, поэтому выполняется и при их
	// провале.
	var ready ctlResponse
	if err := json.Unmarshal(out.Bytes(), &ready); err != nil || ready.Lab == nil || ready.Lab.StorageDir == "" {
		t.Fatalf("ready не сообщил временный каталог: %v %q", err, out.Text())
	}
	labDir := ready.Lab.StorageDir
	// Удаляется только каталог лаборатории во временном каталоге ОС: путь из
	// канала не должен суметь направить RemoveAll куда-то ещё.
	if filepath.Dir(labDir) != filepath.Clean(os.TempDir()) ||
		!strings.HasPrefix(filepath.Base(labDir), "photon-plugin-lab-") {
		t.Fatalf("ready сообщил каталог вне временного каталога лаборатории: %q", labDir)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(labDir); err != nil {
			t.Errorf("уборка каталога после SIGKILL: %v", err)
		}
	})
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

	pidsBefore := pluginPIDs(t, b.hello)
	if len(pidsBefore) == 0 {
		t.Fatalf("процесс плагина не найден — проверять сиротство не на чем")
	}
	runtimePID := parent.Process.Pid

	// Runtime умирает НЕОЖИДАННО: SIGKILL, без шанса на остановку и уборку.
	if err := parent.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_ = parent.Wait()
	// Свою сторону трубы тест закрывает как уборку: runtime уже убит, и на
	// исход проверки это не влияет — плагин уходит по закрытию СВОЕГО канала
	// к runtime, который ОС закрыла вместе с убитым процессом.
	_ = stdin.Close()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if !alive(runtimePID) && len(pluginPIDs(t, b.hello)) == 0 {
			return // ни runtime, ни плагина — сирот нет
		}
		time.Sleep(200 * time.Millisecond)
	}

	leftover := pluginPIDs(t, b.hello)
	for _, pid := range leftover { // не оставлять мусор после красного теста
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
	t.Fatalf("СИРОТЫ: runtime жив=%v, процессов плагина=%d — родитель мёртв, "+
		"а runtime с плагинами пережил его (ADR-057 §9)", alive(runtimePID), len(leftover))
}

// Закрытие управляющего канала — штатный путь того же требования: родитель
// ушёл аккуратно, плагин всё равно обязан остановиться.
func TestOrphan_ClosedControlChannelStopsPlugin(t *testing.T) {
	l := startLab(t)
	if r := l.call(t, "load"); !r.OK {
		t.Fatalf("load: %s", r.Error)
	}
	hello := bins(t).hello
	if len(pluginPIDs(t, hello)) == 0 {
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
		if len(pluginPIDs(t, hello)) == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("плагин пережил закрытие управляющего канала")
}

// ── границы протокола ────────────────────────────────────────────────────────

func TestControl_UnknownCommandIsRefusedWithoutKillingChannel(t *testing.T) {
	l := startLab(t)

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
	l := startLab(t)
	if r := l.call(t, "load"); !r.OK {
		t.Fatalf("первый load: %s", r.Error)
	}
	if r := l.call(t, "load"); r.OK {
		t.Fatalf("второй load принят — экземпляр обязан быть один")
	}
}

// diagnose до загрузки — не ошибка, а честный ответ «плагина нет».
func TestControl_DiagnoseBeforeLoadReportsNotLoaded(t *testing.T) {
	l := startLab(t)
	r := l.call(t, "diagnose")
	if !r.OK || r.PluginState != "not loaded" {
		t.Fatalf("diagnose до load: %+v", r)
	}
	if len(r.Operations) != 0 {
		t.Fatalf("операции до загрузки: %+v", r.Operations)
	}
}

// ── B4b: настоящий исполнитель (docs/plugin-lab.md §11.1) ────────────────────

// T1 — hello на настоящем coreops. Предзагрузка (§3.1) выполнена, но в
// operations её нет: список означает только то, что сделал плагин.
func TestLab_T1_HelloOnRealExecutorWithBootstrap(t *testing.T) {
	l := startLab(t)
	if r := l.load(t, "hello"); !r.OK {
		t.Fatalf("load hello: %s", r.Error)
	}
	diag := l.settle(t)
	if diag.PluginState != "running" {
		t.Fatalf("hello не работает на настоящем исполнителе: %+v", diag)
	}
	if !contains(diag.ops(), "storage.get") {
		t.Fatalf("storage.get не дошла до исполнителя: %v", diag.ops())
	}
	if contains(diag.ops(), "storage.put") {
		t.Fatalf("предзагрузка видна среди операций плагина: %v", diag.ops())
	}
}

// T2 — каталог ровно из четырёх строк §3.
func TestLab_T2_ScenariosIsExactClosedCatalog(t *testing.T) {
	l := startLab(t)
	r := l.call(t, "scenarios")
	if !r.OK {
		t.Fatalf("scenarios: %+v", r)
	}
	a := []string{"storage.read", "storage.write", "identity.read", "identity.sign", "crypto.use"}
	b := []string{"storage.read", "storage.write", "identity.read", "crypto.use"}
	want := map[string]scenarioV{
		"hello":                 {ID: "hello", Fixture: "hello", PluginID: "com.engram.test.hello", Granted: []string{"storage.read"}, Namespace: "lab.hello"},
		"sdk-storage-roundtrip": {ID: "sdk-storage-roundtrip", Fixture: "sdkscenario", PluginID: "com.engram.test.sdkscenario-a", Granted: a, Namespace: "lab.sdkscenario"},
		"sdk-sign":              {ID: "sdk-sign", Fixture: "sdkscenario", PluginID: "com.engram.test.sdkscenario-a", Granted: a, Namespace: "lab.sdkscenario"},
		"sdk-sign-forbidden":    {ID: "sdk-sign-forbidden", Fixture: "sdkscenario", PluginID: "com.engram.test.sdkscenario-b", Granted: b, Namespace: "lab.sdkscenario"},
	}
	if len(r.Scenarios) != len(want) {
		t.Fatalf("каталог: %d строк, ожидалось %d: %+v", len(r.Scenarios), len(want), r.Scenarios)
	}
	for _, got := range r.Scenarios {
		w, ok := want[got.ID]
		if !ok {
			t.Fatalf("лишний сценарий в каталоге: %+v", got)
		}
		sort.Strings(got.Granted)
		ws := append([]string(nil), w.Granted...)
		sort.Strings(ws)
		if got.Fixture != w.Fixture || got.PluginID != w.PluginID || got.Namespace != w.Namespace ||
			strings.Join(got.Granted, ",") != strings.Join(ws, ",") || got.Description == "" {
			t.Fatalf("строка каталога %s: %+v, ожидалось %+v", got.ID, got, w)
		}
	}
}

// T3 — неизвестный сценарий не запускает ничего и не убивает канал.
func TestLab_T3_UnknownScenarioRefused(t *testing.T) {
	l := startLab(t)
	r := l.load(t, "no-such-scenario")
	if r.OK || !strings.Contains(r.Error, "unknown scenario") {
		t.Fatalf("неизвестный сценарий принят: %+v", r)
	}
	if d := l.call(t, "diagnose"); d.PluginState != "not loaded" {
		t.Fatalf("после отказа плагин запущен: %+v", d)
	}
	if st := l.call(t, "status"); !st.OK {
		t.Fatalf("канал умер после отказа: %+v", st)
	}
}

// T4 — лишние поля: кадр отвергается целиком, права не меняются.
func TestLab_T4_ExtraFieldsRefusedAndGrantsUnchanged(t *testing.T) {
	l := startLab(t)
	for _, frame := range []string{
		`{"id":1,"cmd":"load","grants":["identity.sign"]}`,
		`{"id":2,"cmd":"load","plugin":"/bin/sh"}`,
		`{"id":3,"cmd":"load","key":"AAAA"}`,
		`{"id":4,"cmd":"load","mode":"sign"}`,
	} {
		if r := l.send(t, frame); r.OK {
			t.Fatalf("кадр с лишним полем принят: %s → %+v", frame, r)
		}
	}
	// scenario вне load — тоже отказ.
	if r := l.send(t, `{"id":5,"cmd":"diagnose","scenario":"hello"}`); r.OK {
		t.Fatalf("scenario в diagnose принят: %+v", r)
	}
	if d := l.send(t, `{"id":6,"cmd":"diagnose"}`); d.PluginState != "not loaded" {
		t.Fatalf("отвергнутый кадр что-то запустил: %+v", d)
	}
	l.next = 6
	if r := l.load(t, "hello"); !r.OK || strings.Join(r.Granted, ",") != "storage.read" {
		t.Fatalf("права после отвергнутых кадров: %+v", r)
	}
}

// T5 — отказ без identity.sign: FORBIDDEN, и операция не дошла до исполнителя.
func TestLab_T5_SignForbiddenNeverReachesExecutor(t *testing.T) {
	l := startLab(t)
	if r := l.load(t, "sdk-sign-forbidden"); !r.OK {
		t.Fatalf("load: %s", r.Error)
	}
	diag := l.settle(t)
	if diag.Results["sig"] != "FORBIDDEN" {
		t.Fatalf("sig = %q, ожидалось FORBIDDEN: %+v", diag.Results["sig"], diag.Results)
	}
	if pub, err := base64.StdEncoding.DecodeString(diag.Results["pub"]); err != nil || len(pub) != 32 {
		t.Fatalf("pub = %q — настоящий исполнитель выдаёт 32-байтный ключ устройства", diag.Results["pub"])
	}
	if !contains(diag.ops(), "identity.public_key") {
		t.Fatalf("разрешённая identity.public_key не видна: %v", diag.ops())
	}
	if contains(diag.ops(), "identity.sign") {
		t.Fatalf("identity.sign дошла до исполнителя без права: %v", diag.ops())
	}
}

// T6 — подпись: показываются pub, sig, verify_raw; verify_domain — нет (§8).
func TestLab_T6_SignShowsOnlyAllowedValues(t *testing.T) {
	l := startLab(t)
	if r := l.load(t, "sdk-sign"); !r.OK {
		t.Fatalf("load: %s", r.Error)
	}
	diag := l.settle(t)
	if _, shown := diag.Results["verify_domain"]; shown {
		t.Fatalf("verify_domain показан — это проверка по пустому входу: %+v", diag.Results)
	}
	if pub, err := base64.StdEncoding.DecodeString(diag.Results["pub"]); err != nil || len(pub) != 32 {
		t.Fatalf("pub = %q", diag.Results["pub"])
	}
	if !strings.HasPrefix(diag.Results["sig"], "len=64 ") {
		t.Fatalf("sig = %q, ожидалась подпись 64 байта от настоящего исполнителя", diag.Results["sig"])
	}
	if diag.Results["verify_raw"] != "false" {
		t.Fatalf("verify_raw = %q", diag.Results["verify_raw"])
	}
	var keys []string
	for k := range diag.Results {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if strings.Join(keys, ",") != "done,pub,sig,verify_raw" {
		t.Fatalf("results: %v, ожидалось ровно done,pub,sig,verify_raw", keys)
	}
}

// T7 — хранилище: put → get → list → delete → get на настоящем bbolt.
func TestLab_T7_StorageRoundtripOnRealStore(t *testing.T) {
	l := startLab(t)
	if r := l.load(t, "sdk-storage-roundtrip"); !r.OK {
		t.Fatalf("load: %s", r.Error)
	}
	diag := l.settle(t)
	if diag.Results["roundtrip"] != "OK" || diag.Results["after_delete"] != "NOT_FOUND" {
		t.Fatalf("results: %+v", diag.Results)
	}
	var storage []string
	for _, op := range diag.ops() {
		if strings.HasPrefix(op, "storage.") {
			storage = append(storage, op)
		}
	}
	if strings.Join(storage, ",") != "storage.put,storage.get,storage.list,storage.delete,storage.get" {
		t.Fatalf("операции хранилища: %v", storage)
	}
}

func mustExist(t *testing.T, dir string) {
	t.Helper()
	if dir == "" {
		t.Fatalf("ready не сообщил временный каталог")
	}
	if _, err := os.Stat(filepath.Join(dir, "lab.db")); err != nil {
		t.Fatalf("временное хранилище не создано до ready: %v", err)
	}
}

func mustBeRemoved(t *testing.T, dir string) {
	t.Helper()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("временный каталог %s не удалён при штатном завершении (err=%v)", dir, err)
	}
}

// T8 — уборка по shutdown.
func TestLab_T8_ShutdownRemovesTemporaryStorage(t *testing.T) {
	l := startLab(t)
	dir := l.ready.Lab.StorageDir
	mustExist(t, dir)
	if r := l.load(t, "sdk-storage-roundtrip"); !r.OK {
		t.Fatalf("load: %s", r.Error)
	}
	l.settle(t)
	l.shutdownAndWait(t)
	mustBeRemoved(t, dir)
}

// T9 — уборка по закрытию stdin: тот же штатный путь.
func TestLab_T9_ClosedChannelRemovesTemporaryStorage(t *testing.T) {
	l := startLab(t)
	dir := l.ready.Lab.StorageDir
	mustExist(t, dir)
	if r := l.load(t, "hello"); !r.OK {
		t.Fatalf("load: %s", r.Error)
	}
	l.settle(t)
	_ = l.in.Close()
	if err := l.cmd.Wait(); err != nil {
		t.Fatalf("runtime не завершился после закрытия канала: %v", err)
	}
	mustBeRemoved(t, dir)
}

// T10 — окружение родителя не меняет сценарий.
func TestLab_T10_ParentEnvironmentDoesNotChangeScenario(t *testing.T) {
	l := startLabEnv(t, []string{
		"PHOTON_TEST_MODE=storage-roundtrip",
		"PHOTON_TEST_PLUGIN_ID=com.engram.test.injected",
		"PHOTON_TEST_DOMAIN_INPUT=@@@not-base64@@@",
	})
	if r := l.load(t, "sdk-sign"); !r.OK {
		t.Fatalf("load: %s", r.Error)
	}
	diag := l.settle(t)
	if !strings.HasPrefix(diag.Results["sig"], "len=64 ") || diag.Results["done"] != "sign" {
		t.Fatalf("сценарий изменён окружением родителя: %+v", diag.Results)
	}
}

// T11 — ключи одноразовые: два экземпляра выдают разные ключи устройства.
func TestLab_T11_DeviceKeyIsEphemeralPerInstance(t *testing.T) {
	pub := func() string {
		l := startLab(t)
		if r := l.load(t, "sdk-sign"); !r.OK {
			t.Fatalf("load: %s", r.Error)
		}
		p := l.settle(t).Results["pub"]
		l.shutdownAndWait(t)
		return p
	}
	first, second := pub(), pub()
	if first == "" || second == "" {
		t.Fatalf("ключ устройства не получен: %q / %q", first, second)
	}
	if first == second {
		t.Fatalf("два экземпляра выдали один ключ устройства — ключ не одноразовый")
	}
}

// T12 — namespace на фикстуру.
func TestLab_T12_NamespacePerFixture(t *testing.T) {
	for scenario, want := range map[string]string{"hello": "lab.hello", "sdk-storage-roundtrip": "lab.sdkscenario"} {
		l := startLab(t)
		r := l.load(t, scenario)
		if !r.OK || r.Namespace != want {
			t.Fatalf("%s: namespace %q, ожидался %q (%+v)", scenario, r.Namespace, want, r)
		}
	}
}
