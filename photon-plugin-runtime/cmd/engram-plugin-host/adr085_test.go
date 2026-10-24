package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wiloris/photon/photon-plugin-runtime/coreops"
	"github.com/wiloris/photon/photon-plugin-runtime/host"
)

/*
 * Файл результата для приложения (ADR-085) — на НАСТОЯЩЕМ бинаре хоста и
 * настоящем процессе плагина.
 *
 * Плагин — reporter, как в ADR-074: контракт общий и проверяется плагином,
 * который не является ни одним из его потребителей. Кошелька здесь нет.
 *
 * Бинарь, а не Run: удаление прежнего файла, запись на пути ошибки и коды
 * выхода живут в main, и проверять их вызовом функции значило бы проверять
 * не то, что уедет на устройство.
 */

// hostRun — один запуск бинаря хоста.
type hostRun struct {
	dir     string // каталог запуска: база, ключи, плагин, манифест
	result  string // путь -result-out; "" — флаг не передаётся
	output  string // слитые stdout+stderr
	code    int    // код выхода хоста
	bin     string
	root    string
	mpath   string
	started bool
}

// newHostRun собирает хост и reporter в свежем каталоге.
func newHostRun(t *testing.T) *hostRun {
	t.Helper()
	dir := t.TempDir()
	r := &hostRun{dir: dir, result: filepath.Join(dir, "out", "result.json")}
	if err := os.MkdirAll(filepath.Dir(r.result), 0o700); err != nil {
		t.Fatal(err)
	}
	r.bin = buildHostBinary(t, dir)
	root, entry := buildReporter(t, dir)
	r.root = root
	r.mpath = writeManifest(t, dir, reporterManifest(entry))
	return r
}

// run запускает хост. extra дописывается после стандартных аргументов.
func (r *hostRun) run(t *testing.T, spec, eventSpec string, extra ...string) {
	t.Helper()
	args := []string{
		"-db", filepath.Join(r.dir, "engram.db"),
		"-keys", filepath.Join(r.dir, "keys"),
		"-manifest", r.mpath,
		"-package-root", r.root,
		"-grants", "",
		"-timeout", "5s",
	}
	// Идентичность и установка — только в первый раз: повторный -init с
	// существующим device.key — отказ по ADR-069 п. 4.2.
	if !r.started {
		args = append(args, "-init", "-install")
		r.started = true
	}
	if r.result != "" {
		args = append(args, "-result-out", r.result)
	}
	args = append(args, extra...)

	cmd := exec.Command(r.bin, args...)
	cmd.Env = append(os.Environ(), "REPORTER_SPEC="+spec, "REPORTER_EVENT_SPEC="+eventSpec)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()

	r.output = out.String()
	r.code = 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		r.code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("запуск хоста: %v", err)
	}
}

// read разбирает файл результата. Строго: неизвестное поле — провал, чтобы
// лишнее в конверте не прошло незамеченным.
func (r *hostRun) read(t *testing.T) hostResult {
	t.Helper()
	raw, err := os.ReadFile(r.result)
	if err != nil {
		t.Fatalf("файл результата не прочитан: %v\nвывод хоста:\n%s", err, r.output)
	}
	var res hostResult
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&res); err != nil {
		t.Fatalf("файл результата не разбирается: %v\n%s", err, raw)
	}
	if res.Format != resultFormat || res.Version != resultVersion {
		t.Fatalf("конверт %q/%d, ожидался %q/%d", res.Format, res.Version, resultFormat, resultVersion)
	}
	if res.PluginID != reporterID {
		t.Fatalf("plugin_id %q, ожидался %q", res.PluginID, reporterID)
	}
	return res
}

func payloadOf(t *testing.T, rep resultReport) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(rep.Payload)
	if err != nil {
		t.Fatalf("payload не base64: %v", err)
	}
	return b
}

func sizePattern(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i % 251)
	}
	return out
}

func assertNoTemp(t *testing.T, dir string) {
	t.Helper()
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Fatalf("временный файл остался: %s", e.Name())
		}
	}
}

// R1 — success + payload: байт-в-байт, включая 0x00 и 0xFF; права 0600.
func TestADR085_R1_SuccessPayloadByteForByte(t *testing.T) {
	r := newHostRun(t)
	r.run(t, "success/vector", "")
	if r.code != 0 {
		t.Fatalf("код хоста %d\n%s", r.code, r.output)
	}

	res := r.read(t)
	if !res.Host.OK || res.Host.Error != "" {
		t.Fatalf("host = %+v, ожидался ok", res.Host)
	}
	if !res.Exit.Known || res.Exit.Kind != host.ExitCleanStop.String() || res.Exit.Code != 0 {
		t.Fatalf("exit = %+v, ожидался clean_stop/0", res.Exit)
	}
	if len(res.Reports) != 1 || res.Reports[0].Status != string(coreops.WorkSuccess) {
		t.Fatalf("reports = %+v", res.Reports)
	}
	if got := payloadOf(t, res.Reports[0]); !bytes.Equal(got, payloadVector) {
		t.Fatalf("payload изменился:\n получено %v\n ожидалось %v", got, payloadVector)
	}

	st, err := os.Stat(r.result)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("права %v, ожидались 0600", st.Mode().Perm())
	}
	assertNoTemp(t, filepath.Dir(r.result))
}

// R1' — несколько докладов: все, в порядке поступления, без last-write-wins.
func TestADR085_R1_ReportsKeepOrder(t *testing.T) {
	r := newHostRun(t)
	r.run(t, "failure/vector", "success/size:300", "-events", "e1,e2")
	if r.code != 0 {
		t.Fatalf("код хоста %d\n%s", r.code, r.output)
	}
	res := r.read(t)
	if len(res.Reports) != 3 {
		t.Fatalf("докладов %d, ожидалось 3: %+v", len(res.Reports), res.Reports)
	}
	if res.Reports[0].Status != "failure" || !bytes.Equal(payloadOf(t, res.Reports[0]), payloadVector) {
		t.Fatalf("первый доклад не тот: %+v", res.Reports[0])
	}
	for i := 1; i < 3; i++ {
		if res.Reports[i].Status != "success" || !bytes.Equal(payloadOf(t, res.Reports[i]), sizePattern(300)) {
			t.Fatalf("доклад %d не тот: %+v", i, res.Reports[i])
		}
	}
}

// R2 — прикладной отказ: доклад доставлен, процесс остановлен штатно.
func TestADR085_R2_FailureIsApplicationLevel(t *testing.T) {
	r := newHostRun(t)
	r.run(t, "failure/vector", "")
	res := r.read(t)
	if !res.Host.OK {
		t.Fatalf("прикладной отказ стал ошибкой хоста: %+v", res.Host)
	}
	if res.Exit.Kind != "clean_stop" || !res.Exit.Known {
		t.Fatalf("exit = %+v: failure не меняет ExitKind", res.Exit)
	}
	if len(res.Reports) != 1 || res.Reports[0].Status != "failure" {
		t.Fatalf("reports = %+v", res.Reports)
	}
	if !bytes.Equal(payloadOf(t, res.Reports[0]), payloadVector) {
		t.Fatal("payload отказа изменился")
	}
}

// R3 — пустой payload: пустая строка, не null и не пропуск.
func TestADR085_R3_EmptyPayload(t *testing.T) {
	r := newHostRun(t)
	r.run(t, "success/empty", "")
	raw, _ := os.ReadFile(r.result)
	if !bytes.Contains(raw, []byte(`"payload":""`)) {
		t.Fatalf("пустой payload записан не пустой строкой: %s", raw)
	}
	res := r.read(t)
	if len(res.Reports) != 1 || res.Reports[0].Payload != "" {
		t.Fatalf("reports = %+v", res.Reports)
	}
}

// R4 — payload сверх 64 KiB: доклад отвергнут, в файле его нет, обрезка
// не появилась. Хост дождался таймаута — ошибка хоста, а не отказ плагина.
func TestADR085_R4_OversizedPayloadNeverAppears(t *testing.T) {
	r := newHostRun(t)
	r.run(t, "success/size:65537", "", "-timeout", "2s")
	res := r.read(t)
	if res.Host.OK {
		t.Fatal("запуск без принятого доклада помечен успешным")
	}
	if len(res.Reports) != 0 {
		t.Fatalf("отвергнутый доклад попал в файл: %d шт., первый %d Б",
			len(res.Reports), len(payloadOf(t, res.Reports[0])))
	}
}

// R5 — больше 16 докладов: отказ too_many_reports, список пуст. Ровно 16 —
// принимается целиком (граница с обеих сторон).
func TestADR085_R5_TooManyReports(t *testing.T) {
	events := func(n int) string {
		ev := make([]string, n)
		for i := range ev {
			ev[i] = "e"
		}
		return strings.Join(ev, ",")
	}

	t.Run("16 проходит", func(t *testing.T) {
		r := newHostRun(t)
		r.run(t, "success/empty", "success/empty", "-events", events(15))
		res := r.read(t)
		if !res.Host.OK || len(res.Reports) != 16 {
			t.Fatalf("host=%+v, докладов %d; ожидалось ok и 16", res.Host, len(res.Reports))
		}
	})

	t.Run("17 отказ", func(t *testing.T) {
		r := newHostRun(t)
		r.run(t, "success/empty", "success/empty", "-events", events(16))
		res := r.read(t)
		if res.Host.OK || res.Host.Error != errTooManyReports {
			t.Fatalf("host = %+v, ожидался отказ %q", res.Host, errTooManyReports)
		}
		if len(res.Reports) != 0 {
			t.Fatalf("при отказе в файле %d докладов — обрезка вместо отказа", len(res.Reports))
		}
	})
}

// R6 — падение без доклада: exit crash/7, reports пуст, failure не
// синтезирован. Код хоста — 1, файл при этом есть.
func TestADR085_R6_CrashIsNotAReport(t *testing.T) {
	r := newHostRun(t)
	r.run(t, "crash", "")
	if r.code != 1 {
		t.Fatalf("код хоста %d, ожидался 1\n%s", r.code, r.output)
	}
	res := r.read(t)
	if res.Host.OK {
		t.Fatal("падение плагина помечено успешным запуском")
	}
	if !res.Exit.Known || res.Exit.Kind != "crash" || res.Exit.Code != 7 {
		t.Fatalf("exit = %+v, ожидался crash/7", res.Exit)
	}
	if len(res.Reports) != 0 {
		t.Fatalf("падение превращено в доклад: %+v", res.Reports)
	}
}

// R7 — ошибка хоста после начала Run: файл есть, host.ok=false, причина
// текстом, статуса плагина нет.
func TestADR085_R7_HostErrorStillWritesResult(t *testing.T) {
	r := newHostRun(t)
	r.started = true // без -install: плагин не зарегистрирован
	r.run(t, "success/empty", "", "-init")
	if r.code != 1 {
		t.Fatalf("код хоста %d, ожидался 1\n%s", r.code, r.output)
	}
	res := r.read(t)
	if res.Host.OK || !strings.Contains(res.Host.Error, "не установлен") {
		t.Fatalf("host = %+v, ожидалась ошибка «не установлен»", res.Host)
	}
	if res.Exit.Known {
		t.Fatalf("exit.known = true, хотя плагин не запускался: %+v", res.Exit)
	}
	if len(res.Reports) != 0 {
		t.Fatalf("reports = %+v", res.Reports)
	}
}

// R8 — устаревший файл удаляется ДО проверки аргументов: негодный запуск
// оставляет «файла нет», а не прошлый результат.
func TestADR085_R8_StaleResultRemovedBeforeAnything(t *testing.T) {
	r := newHostRun(t)
	if err := os.WriteFile(r.result, []byte(`{"stale":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	r.mpath = filepath.Join(r.dir, "нет-такого-манифеста.json")
	r.run(t, "success/empty", "")
	if r.code != 2 {
		t.Fatalf("код хоста %d, ожидался 2\n%s", r.code, r.output)
	}
	if _, err := os.Stat(r.result); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("устаревший результат пережил негодный запуск: %v", err)
	}
}

// R9 — прежний файл не удаляется: отказ кодом 2, плагин не запускался.
func TestADR085_R9_UnremovableStaleResultRefusesStart(t *testing.T) {
	r := newHostRun(t)
	// Непустой каталог на месте файла: os.Remove на нём отказывает.
	if err := os.MkdirAll(filepath.Join(r.result, "занято"), 0o700); err != nil {
		t.Fatal(err)
	}
	r.run(t, "success/empty", "")
	if r.code != 2 {
		t.Fatalf("код хоста %d, ожидался 2\n%s", r.code, r.output)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "engram.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("хранилище создано: побочные эффекты случились до отказа")
	}
	if !strings.Contains(r.output, "прежний файл результата") {
		t.Fatalf("причина отказа не названа:\n%s", r.output)
	}
}

// R10 — атомарность: до rename целевого имени нет, временный файл уже
// полон; после — временного нет.
func TestADR085_R10_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "result.json")
	want := buildResult(reporterID, Result{Reports: []WorkReport{{Status: coreops.WorkSuccess, Payload: payloadVector}}}, nil)

	called := false
	prev := resultBeforeRename
	resultBeforeRename = func(tmp string) {
		called = true
		// Тот же каталог — условие атомарности rename. Проверяется прямо:
		// в тестовой среде /tmp и каталог теста на одной файловой системе,
		// и rename из чужого каталога прошёл бы, а на устройстве — нет.
		if filepath.Dir(tmp) != dir {
			t.Errorf("временный файл в %s, а не в каталоге результата %s", filepath.Dir(tmp), dir)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("целевой файл виден до rename: %v", err)
		}
		raw, err := os.ReadFile(tmp)
		if err != nil {
			t.Errorf("временный файл не читается: %v", err)
			return
		}
		var got hostResult
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Errorf("временный файл до rename неполон: %v", err)
		}
	}
	t.Cleanup(func() { resultBeforeRename = prev })

	if err := writeResult(path, want); err != nil {
		t.Fatalf("writeResult: %v", err)
	}
	if !called {
		t.Fatal("запись прошла мимо rename")
	}
	assertNoTemp(t, dir)
}

// R10' — неудачная запись не оставляет ни целевого, ни временного файла.
// На бинаре: каталога для результата нет — код 1, причина в stderr.
func TestADR085_R10_FailedWriteLeavesNothing(t *testing.T) {
	r := newHostRun(t)
	r.result = filepath.Join(r.dir, "нет-каталога", "result.json")
	r.run(t, "success/vector", "")
	if r.code != 1 {
		t.Fatalf("код хоста %d, ожидался 1\n%s", r.code, r.output)
	}
	if !strings.Contains(r.output, "файл результата:") {
		t.Fatalf("причина неудачной записи не названа:\n%s", r.output)
	}
	if _, err := os.Stat(r.result); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("файл появился: %v", err)
	}
}

// R11 — повторный запуск: второй файл отражает ТОЛЬКО второй запуск.
func TestADR085_R11_RepeatedRun(t *testing.T) {
	r := newHostRun(t)
	r.run(t, "success/vector", "")
	first := r.read(t)
	if len(first.Reports) != 1 {
		t.Fatalf("первый запуск: %+v", first.Reports)
	}

	r.run(t, "failure/size:7", "")
	second := r.read(t)
	if len(second.Reports) != 1 || second.Reports[0].Status != "failure" ||
		!bytes.Equal(payloadOf(t, second.Reports[0]), sizePattern(7)) {
		t.Fatalf("второй файл несёт не второй запуск: %+v", second.Reports)
	}
}

// R12 — без флага: код выхода и диагностический вывод прежние, файлов не
// появилось.
func TestADR085_R12_NoFlagNoChange(t *testing.T) {
	r := newHostRun(t)
	r.result = ""
	before, _ := os.ReadDir(filepath.Join(r.dir, "out"))
	r.run(t, "success/vector", "")
	if r.code != 0 {
		t.Fatalf("код хоста %d\n%s", r.code, r.output)
	}
	after, _ := os.ReadDir(filepath.Join(r.dir, "out"))
	if len(before) != 0 || len(after) != 0 {
		t.Fatalf("без флага появились файлы: %v", after)
	}

	// Та же форма вывода, что и прежде: блок диагностики без новых строк.
	lines := strings.Split(strings.TrimSpace(r.output), "\n")
	want := []string{"плагин:", "счётчик:", "подпись:", "завершение:", "доклады:", "  0: success, нагрузка 10 Б", "журнал:"}
	for i, w := range want {
		if i >= len(lines) || !strings.HasPrefix(lines[i], w) {
			t.Fatalf("строка %d вывода %q, ожидалось начало %q\n%s", i, at(lines, i), w, r.output)
		}
	}
}

func at(s []string, i int) string {
	if i < len(s) {
		return s[i]
	}
	return "<нет>"
}

// R13 — payload не попадает в вывод хоста ни сырыми байтами, ни base64.
func TestADR085_R13_PayloadNeverPrinted(t *testing.T) {
	r := newHostRun(t)
	payload := sizePattern(4096)
	r.run(t, "success/size:4096", "")
	res := r.read(t)
	if !bytes.Equal(payloadOf(t, res.Reports[0]), payload) {
		t.Fatal("payload в файле не тот")
	}
	if strings.Contains(r.output, base64.StdEncoding.EncodeToString(payload)[:64]) {
		t.Fatalf("base64 payload в выводе хоста:\n%s", r.output)
	}
	// Отрезок узора из печатаемых байтов: будь он в выводе, payload напечатан.
	if bytes.Contains([]byte(r.output), payload[65:120]) {
		t.Fatalf("сырой payload в выводе хоста:\n%s", r.output)
	}
}

// Конверт без докладов — пустой массив, не null.
func TestADR085_EmptyReportsIsArray(t *testing.T) {
	raw, err := json.Marshal(buildResult(reporterID, Result{}, errors.New("x")))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"reports":[]`)) {
		t.Fatalf("пустой список докладов закодирован не массивом: %s", raw)
	}
	if !bytes.Contains(raw, []byte(`"exit":{"known":false`)) {
		t.Fatalf("неизвестный статус не помечен: %s", raw)
	}
}
