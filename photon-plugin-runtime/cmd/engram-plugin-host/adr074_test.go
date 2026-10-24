package main

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-plugin-runtime/coreops"
	"github.com/wiloris/photon/photon-plugin-runtime/host"
	"github.com/wiloris/photon/photon-plugin-runtime/manifest"
)

/*
 * Контракт доклада (ADR-074) на НАСТОЯЩИХ процессах.
 *
 * Плагин здесь — reporter: он не знает ни про кошелёк, ни про счётчик, и
 * докладывает ровно то, что велено переменной окружения. Это требование
 * ADR-074 §13: контракт платформы обязан проверяться плагином, который не
 * является ни одним из её потребителей.
 *
 * Проверяется вся цепь целиком — SDK → IPC → Authorizer → coreops →
 * композиция, — потому что каждое звено способно испортить байты по-своему:
 * SDK кодированием, кодек длиной, coreops разбором, композиция хранением.
 */

const reporterID = "dev.photon.reporter"

// payloadVector — тот же вектор, что в самой фикстуре.
//
// Копия НАМЕРЕННАЯ и названа прямо: сослаться на константу плагина нельзя —
// это отдельный исполняемый модуль, собираемый в отдельный процесс. Ошибка
// проявится несошедшимися байтами, то есть падением T2, а не тихим согласием.
var payloadVector = []byte{0x00, 0xFF, 0xFE, 0x01, 0x80, 0x81, 0xC0, 0xAF, 0x00, 'z'}

func buildReporter(t *testing.T, dir string) (root, entry string) {
	t.Helper()

	out := filepath.Join(dir, "reporter")
	cmd := exec.Command("go", "build", "-o", out, filepath.Join("..", "..", "tests", "plugins", "reporter"))
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("сборка reporter: %v\n%s", err, b)
	}
	return dir, "reporter"
}

func reporterManifest(entry string) *manifest.Manifest {
	return &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: reporterID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      entry,
		},
		// Прав не запрашивается вовсе: и доклад, и журнал их не требуют.
		// Это здесь же и проверяется — сценарии проходят с пустым набором.
		Permissions: manifest.Permissions{},
	}
}

// runReporter запускает фикстуру с заданной спецификацией докладов.
func runReporter(t *testing.T, spec, eventSpec string, events []string) (Result, error) {
	t.Helper()
	return runReporterTimeout(t, spec, eventSpec, events, 30*time.Second)
}

// runReporterTimeout — то же с явным пределом ожидания доклада.
//
// Отдельный параметр нужен проверкам таймаута: ждать там положено МЕНЬШЕ, и
// подгонять под них общий предел значило бы сделать остальные тесты
// чувствительными к скорости машины.
func runReporterTimeout(t *testing.T, spec, eventSpec string, events []string, timeout time.Duration) (Result, error) {
	t.Helper()

	t.Setenv("REPORTER_SPEC", spec)
	t.Setenv("REPORTER_EVENT_SPEC", eventSpec)

	dir := t.TempDir()
	root, entry := buildReporter(t, dir)

	return Run(context.Background(), Options{
		Manifest:     reporterManifest(entry),
		DBPath:       filepath.Join(dir, "engram.db"),
		PackageRoot:  root,
		Grants:       nil,
		Events:       events,
		InitIdentity: true,
		Install:      true,
		Timeout:      timeout,
	})
}

// T1 + T2 — доклад об успехе доходит со статусом и с нагрузкой БАЙТ-В-БАЙТ.
func TestADR074_T1_T2_SuccessPayloadArrivesByteForByte(t *testing.T) {
	res, err := runReporter(t, "success/vector", "", nil)
	if err != nil {
		t.Fatalf("запуск: %v", err)
	}

	if len(res.Reports) != 1 {
		t.Fatalf("докладов %d, ожидался 1", len(res.Reports))
	}
	if res.Reports[0].Status != coreops.WorkSuccess {
		t.Fatalf("статус %q, ожидался %q", res.Reports[0].Status, coreops.WorkSuccess)
	}

	// Сравнение байтов, а не разобранного JSON: разбор скрыл бы и
	// перекодировку, и потерю хвоста (ADR-074 §6).
	if !bytes.Equal(res.Reports[0].Payload, payloadVector) {
		t.Fatalf("нагрузка изменилась в пути:\n получено %v\n ожидалось %v",
			res.Reports[0].Payload, payloadVector)
	}
}

/*
 * T3 + T12 — ОТКАЗ РАБОТЫ не является отказом процесса.
 *
 * Самый важный тест среза. Он проверяет три разных утверждения сразу, и
 * каждое по отдельности бессмысленно:
 *
 *   1. запуск НЕ считается неудавшимся: Run возвращает nil;
 *   2. доклад об отказе доставлен вместе с нагрузкой;
 *   3. жизненный цикл прошёл полностью: STOP, STOP_ACK, clean_stop. Отказ не
 *      меняет ExitKind и менять не вправе (§5, §9).
 *
 * Чего этот тест НЕ проверяет: что WorkComplete вернул nil (T12). Строку
 * журнала, написанную ПОСЛЕ последнего ожидаемого доклада, хост может не
 * успеть получить — STOP уходит сразу за докладом. Это та же гонка, из-за
 * которой ADR-072 убрал "count:" из роли барьера, и чинить её строкой журнала
 * значило бы вернуть барьер на разбор журнала. T12 проверяется там, где он
 * детерминирован, — в самом SDK (photon-plugin-sdk/workcomplete_test.go).
 */
func TestADR074_T3_FailureIsAnApplicationResultNotAProcessFailure(t *testing.T) {
	res, err := runReporter(t, "failure/vector", "", nil)
	if err != nil {
		t.Fatalf("запуск завершился ошибкой, хотя отказ РАБОТЫ не является отказом запуска: %v", err)
	}

	if len(res.Reports) != 1 {
		t.Fatalf("докладов %d, ожидался 1", len(res.Reports))
	}
	if res.Reports[0].Status != coreops.WorkFailure {
		t.Fatalf("статус %q, ожидался %q", res.Reports[0].Status, coreops.WorkFailure)
	}
	if !bytes.Equal(res.Reports[0].Payload, payloadVector) {
		t.Fatalf("нагрузка отказа изменилась:\n получено %v\n ожидалось %v",
			res.Reports[0].Payload, payloadVector)
	}

	// Жизненный цикл прошёл целиком.
	if res.Exit.Kind != host.ExitCleanStop {
		t.Fatalf("завершение %v, ожидалось %v: отказ работы не меняет ExitKind",
			res.Exit.Kind, host.ExitCleanStop)
	}
	if res.Exit.Code != 0 {
		t.Fatalf("код выхода %d, ожидался 0", res.Exit.Code)
	}
	// Статус ПОЛУЧЕН, а не оставлен нулевым: иначе clean_stop выше был бы
	// нулевым значением ExitKind, а не утверждением о плагине.
	if !res.ExitKnown {
		t.Fatal("ExitKnown = false: clean_stop оказался нулевым значением, а не полученным статусом")
	}
}

// T4 и T5 — пустая нагрузка допустима при ОБОИХ статусах.
func TestADR074_T4_T5_EmptyPayloadAcceptedForBothStatuses(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec string
		want coreops.WorkStatus
	}{
		{"успех", "success/empty", coreops.WorkSuccess},
		{"отказ", "failure/empty", coreops.WorkFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := runReporter(t, tc.spec, "", nil)
			if err != nil {
				t.Fatalf("запуск: %v", err)
			}
			if len(res.Reports) != 1 {
				t.Fatalf("докладов %d, ожидался 1", len(res.Reports))
			}
			if res.Reports[0].Status != tc.want {
				t.Fatalf("статус %q, ожидался %q", res.Reports[0].Status, tc.want)
			}
			if len(res.Reports[0].Payload) != 0 {
				t.Fatalf("нагрузка непуста: %d байт", len(res.Reports[0].Payload))
			}
		})
	}
}

// T6 — нагрузка РОВНО в предел проходит через настоящий IPC целиком.
//
// Отдельно от такой же проверки в coreops: там кадра не было, здесь байты
// действительно едут через кодек, трубу и разбор.
func TestADR074_T6_PayloadAtLimitTravelsWholeThroughRealIPC(t *testing.T) {
	const limit = 64 << 10

	res, err := runReporter(t, "success/size:65536", "", nil)
	if err != nil {
		t.Fatalf("запуск: %v", err)
	}
	if len(res.Reports) != 1 {
		t.Fatalf("докладов %d, ожидался 1", len(res.Reports))
	}

	got := res.Reports[0].Payload
	if len(got) != limit {
		t.Fatalf("длина нагрузки %d, ожидалось %d", len(got), limit)
	}
	for i, b := range got {
		if b != byte(i%251) {
			t.Fatalf("байт %d испорчен: %#x вместо %#x", i, b, byte(i%251))
		}
	}
}

// T7 — нагрузка сверх предела отвергается SDK до записи в канал.
//
// Сессия при этом ЖИВА: отказ по форме доклада не обязан рвать соединение, и
// проверяется это тем, что следующий доклад проходит.
func TestADR074_T7_OverLimitRejectedAndSessionSurvives(t *testing.T) {
	res, err := runReporter(t, "success/size:65537;success/empty", "", nil)
	if err != nil {
		t.Fatalf("запуск: %v", err)
	}

	// Первый доклад до хоста не дошёл вовсе.
	if len(res.Reports) != 1 {
		t.Fatalf("докладов %d, ожидался 1: сверхпредельный не должен был дойти", len(res.Reports))
	}
	if len(res.Reports[0].Payload) != 0 {
		t.Fatalf("дошла обрезанная нагрузка в %d байт — обрезка запрещена (§7)",
			len(res.Reports[0].Payload))
	}
	/*
	 * Отказ произошёл именно в SDK, до записи в канал. Эта строка журнала
	 * надёжна, в отличие от строки после ПОСЛЕДНЕГО доклада: за ней следует
	 * доклад, которого хост ждёт, — значит она заведомо уже принята.
	 *
	 * Runtime-половина того же правила (кадр, обошедший SDK) проверяется
	 * отдельно и не здесь: coreops/adr074_test.go, T7. Составить такой кадр
	 * настоящим плагином нельзя — он ходит через SDK.
	 */
	if !hasLine(res.Lines, "wc:0:err:") {
		t.Fatalf("сверхпредельный доклад не был отвергнут: %v", res.Lines)
	}

	// Приход ВТОРОГО доклада и есть доказательство, что сессия пережила
	// отказ по форме: доклад идёт по той же сессии.
	if res.Reports[0].Status != coreops.WorkSuccess {
		t.Fatalf("статус дошедшего доклада %q", res.Reports[0].Status)
	}
}

// T8 — повторные доклады НЕ ЗАМЕНЯЮТ друг друга.
//
// Порядок задан протоколом: хост ждёт доклад, шлёт событие, ждёт снова. Он
// не опирается ни на одну паузу — потому и детерминирован.
func TestADR074_T8_RepeatedReportsDoNotReplaceEachOther(t *testing.T) {
	res, err := runReporter(t, "success/size:16", "failure/vector", []string{"tick"})
	if err != nil {
		t.Fatalf("запуск: %v", err)
	}

	if len(res.Reports) != 2 {
		t.Fatalf("докладов %d, ожидалось 2 — второй не заменяет первый", len(res.Reports))
	}
	if res.Reports[0].Status != coreops.WorkSuccess || len(res.Reports[0].Payload) != 16 {
		t.Fatalf("первый доклад испорчен вторым: статус %q, нагрузка %d байт",
			res.Reports[0].Status, len(res.Reports[0].Payload))
	}
	if res.Reports[1].Status != coreops.WorkFailure || !bytes.Equal(res.Reports[1].Payload, payloadVector) {
		t.Fatalf("второй доклад испорчен: статус %q, нагрузка %v",
			res.Reports[1].Status, res.Reports[1].Payload)
	}
	if res.Completions != 2 {
		t.Fatalf("Completions = %d, ожидалось 2", res.Completions)
	}
}

// Неизвестный статус отвергается SDK, и сессия это переживает.
func TestADR074_UnknownStatusRejectedBySDK(t *testing.T) {
	res, err := runReporter(t, "maybe/empty;success/empty", "", nil)
	if err != nil {
		t.Fatalf("запуск: %v", err)
	}
	if len(res.Reports) != 1 {
		t.Fatalf("докладов %d, ожидался 1: доклад с негодным статусом дойти не должен", len(res.Reports))
	}
	if !hasLine(res.Lines, "wc:0:err:") {
		t.Fatalf("негодный статус не отвергнут: %v", res.Lines)
	}
}

/*
 * T9 — ПАДЕНИЕ БЕЗ ДОКЛАДА не превращается в доклад об отказе.
 *
 * Проверяется в ДВУХ местах, и оба нужны.
 *
 * Здесь — сквозной путь: плагин умирает, не доложив, и запуск обязан
 * закончиться ошибкой, а не тихим «работа не удалась».
 *
 * Второе место — TestADR074_T9_CrashIsNotSynthesizedIntoAReport ниже: оно
 * проверяет ровно тот шов, где синтез и был бы написан. Этот тест его не
 * ловит: Run на пути ошибки возвращает нулевой Result (известный дефект
 * композиции, ADR-074 §15), поэтому «докладов нет» здесь истинно по
 * построению и доказывало бы ноль.
 */
func TestADR074_T9_CrashBeforeReportFailsTheRun(t *testing.T) {
	_, err := runReporter(t, "crash", "", nil)
	if err == nil {
		t.Fatal("падение без доклада прошло как успешный запуск")
	}
	if !strings.Contains(err.Error(), "завершился до доклада") {
		t.Fatalf("причина названа неверно: %v", err)
	}
}

// T9 на шве: waitComplete увидел завершение процесса и НЕ создал доклада.
//
// Это и есть проверка мутации M3. Синтез «отказа из падения» писался бы
// именно здесь — в ветке, где ожидание доклада прерывается завершением.
func TestADR074_T9_CrashIsNotSynthesizedIntoAReport(t *testing.T) {
	rep := newReports()
	exited := make(chan host.ExitStatus, 1)
	exited <- host.ExitStatus{Kind: host.ExitCrash, Code: 7}

	err := rep.waitComplete(time.Second, exited)
	if err == nil {
		t.Fatal("завершение процесса не прервало ожидание доклада")
	}
	if n := rep.completions(); n != 0 {
		t.Fatalf("после падения учтено %d докладов, ожидалось 0", n)
	}
	if got := rep.workReports(); len(got) != 0 {
		t.Fatalf("падение превращено в доклад: %+v", got)
	}
}

func hasLine(lines []string, substr string) bool {
	for _, l := range lines {
		if strings.Contains(l, substr) {
			return true
		}
	}
	return false
}
