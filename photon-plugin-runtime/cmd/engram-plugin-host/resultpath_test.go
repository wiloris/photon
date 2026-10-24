package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-plugin-runtime/host"
)

/*
 * Result доходит до вызывающего НА ПУТИ ОШИБКИ (ADR-074 §15).
 *
 * Прежде он заполнялся только после успешного Wait, и два десятка ранних
 * возвратов отдавали нулевую структуру. Упавший плагин уносил с собой всё
 * сразу: свой журнал, свои доклады и типизированную причину завершения — при
 * том что хост это собрал и держал в руках.
 *
 * Отдельно проверяется то, чего нельзя восстановить задним числом: статус
 * завершения выдаётся каналом ОДИН раз, и повторное чтение даёт нулевое
 * значение, то есть ложный clean_stop с кодом 0.
 */

// T-R1 — падение с кодом 7 сохраняет ТИПИЗИРОВАННЫЙ статус.
//
// Этот же тест сторожит отсутствие второго чтения exitCh: появись где-нибудь
// повторный Wait «ради восстановления статуса», crash/7 немедленно стал бы
// clean_stop/0 и проверка упала бы.
func TestResultPath_CrashKeepsTypedExitStatus(t *testing.T) {
	res, err := runReporter(t, "crash", "", nil)
	if err == nil {
		t.Fatal("падение без доклада прошло как успешный запуск")
	}

	if !res.ExitKnown {
		t.Fatal("ExitKnown = false: статус завершения был получен и потерян")
	}
	if res.Exit.Kind != host.ExitCrash {
		t.Fatalf("завершение %v, ожидалось %v — типизированная причина потеряна",
			res.Exit.Kind, host.ExitCrash)
	}
	if res.Exit.Code != 7 {
		t.Fatalf("код выхода %d, ожидался 7", res.Exit.Code)
	}

	// Падение НЕ превращается в доклад: приписывать плагину утверждение,
	// которого он не делал, runtime не вправе (ADR-074 §9).
	if len(res.Reports) != 0 {
		t.Fatalf("падение превращено в доклад: %+v", res.Reports)
	}
}

// T-R2 — таймаут НЕ выдаётся за штатную остановку.
//
// Статуса на этом пути нет вовсе, а нулевое значение ExitKind есть
// ExitCleanStop. Без отдельного признака Result утверждал бы «плагин штатно
// остановился, код 0» ровно тогда, когда плагин жив или завис.
func TestResultPath_TimeoutDoesNotClaimCleanStop(t *testing.T) {
	res, err := runReporterTimeout(t, "", "", nil, 2*time.Second)
	if err == nil {
		t.Fatal("молчащий плагин прошёл как успешный запуск")
	}
	if !errors.Is(err, errNoReport) {
		t.Fatalf("ошибка %v, ожидалась errNoReport — тип отказа потерян", err)
	}

	if res.ExitKnown {
		t.Fatalf("ExitKnown = true, хотя статус не получен: Exit = %v", res.Exit)
	}
	// Нулевое значение читается именно как clean_stop — поэтому одного его
	// наличия недостаточно, и проверяется признак, а не само поле.
	if res.Exit.Kind != host.ExitCleanStop {
		t.Fatalf("нулевой Exit перестал быть clean_stop (%v) — проверка потеряла смысл", res.Exit.Kind)
	}
}

// T-R3 — ошибка ПОСЛЕ полученного доклада не уносит его с собой.
//
// Плагин докладывает один раз, на событие не отвечает — хост упирается в
// таймаут второго ожидания. Первый доклад к этому моменту уже принят, и
// обязан дойти до вызывающего вместе с нагрузкой.
func TestResultPath_ErrorAfterReportKeepsIt(t *testing.T) {
	res, err := runReporterTimeout(t, "success/vector", "", []string{"tick"}, 2*time.Second)
	if err == nil {
		t.Fatal("отсутствие доклада после события прошло как успех")
	}

	if len(res.Reports) != 1 {
		t.Fatalf("докладов %d, ожидался 1 — принятый доклад потерян на пути ошибки", len(res.Reports))
	}
	if !bytes.Equal(res.Reports[0].Payload, payloadVector) {
		t.Fatalf("нагрузка принятого доклада испорчена:\n получено %v\n ожидалось %v",
			res.Reports[0].Payload, payloadVector)
	}
	if res.Completions != 1 {
		t.Fatalf("Completions = %d, ожидался 1", res.Completions)
	}
}

// T-R4 — на пути ошибки доходит и журнал плагина.
//
// Плагин отвергает сверхпредельную нагрузку сам и пишет об этом в журнал,
// после чего падает, не доложив. Раньше обе половины терялись разом: и
// причина отказа, и факт падения.
func TestResultPath_ErrorPathDeliversPluginLog(t *testing.T) {
	res, err := runReporterTimeout(t, "success/size:65537;crash", "", nil, 5*time.Second)
	if err == nil {
		t.Fatal("падение после отвергнутого доклада прошло как успех")
	}

	if len(res.Lines) == 0 {
		t.Fatal("журнал плагина пуст: на пути ошибки он терялся целиком")
	}
	if !hasLine(res.Lines, "wc:0:err:") {
		t.Fatalf("в журнале нет строки об отвергнутом докладе: %v", res.Lines)
	}
}

/*
 * T-R5 — почему статус нельзя перечитать.
 *
 * Тест на самом механизме, а не на Run: он воспроизводит форму host.exitCh —
 * буфер на единицу, одна отправка, затем close — и показывает обе половины
 * правила сразу.
 *
 * Без него правило «не читать второй раз» осталось бы комментарием: сама по
 * себе потеря статуса выглядит безобидно, и опасно в ней ровно то, что
 * второе чтение НЕ ПАДАЕТ, а тихо возвращает clean_stop с кодом 0.
 */
func TestResultPath_ExitChannelYieldsStatusOnlyOnce(t *testing.T) {
	exited := make(chan host.ExitStatus, 1)
	exited <- host.ExitStatus{Kind: host.ExitCrash, Code: 7}
	close(exited)

	rep := newReports()
	if err := rep.waitComplete(time.Second, exited); err == nil {
		t.Fatal("завершение процесса не прервало ожидание доклада")
	}

	// Половина первая: статус сохранён тем, кто его извлёк.
	var res Result
	rep.fillInto(&res)
	if !res.ExitKnown || res.Exit.Kind != host.ExitCrash || res.Exit.Code != 7 {
		t.Fatalf("статус не сохранён при извлечении: known=%v exit=%v", res.ExitKnown, res.Exit)
	}

	// Половина вторая: перечитать его неоткуда — канал отдаёт нулевое
	// значение, а оно неотличимо от штатной остановки.
	again := <-exited
	if again.Kind != host.ExitCleanStop || again.Code != 0 {
		t.Fatalf("повторное чтение дало %v — модель канала изменилась, и правило нужно пересмотреть", again)
	}
}

/*
 * T-R6 — код выхода CLI есть КОНСТАНТА и кодом плагина не становится.
 *
 * Проверяется на настоящем бинаре: подмена os.Exit(1) на os.Exit(res.Exit.Code)
 * внутри пакета ничем иным не ловится — Run про код выхода хоста не знает.
 */
func TestResultPath_CLIExitCodeIsNotThePluginExitCode(t *testing.T) {
	dir := t.TempDir()
	hostBin := filepath.Join(dir, "engram-plugin-host")
	if b, err := exec.Command("go", "build", "-o", hostBin, ".").CombinedOutput(); err != nil {
		t.Fatalf("сборка хоста: %v\n%s", err, b)
	}

	root, entry := buildReporter(t, dir)

	// Манифест кладётся ФАЙЛОМ: CLI принимает его только так (ADR-071), и
	// помощник для этого уже есть — свой завёл бы второе место с тем же.
	manifestPath := writeManifest(t, dir, reporterManifest(entry))

	cmd := exec.Command(hostBin,
		"-db", filepath.Join(dir, "engram.db"),
		"-keys", filepath.Join(dir, "keys"),
		"-manifest", manifestPath,
		"-package-root", root,
		"-id", reporterID,
		"-grants", "",
		"-init", "-install",
	)
	cmd.Env = append(os.Environ(), "REPORTER_SPEC=crash", "REPORTER_EVENT_SPEC=")

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	runErr := cmd.Run()

	var ee *exec.ExitError
	if !errors.As(runErr, &ee) {
		t.Fatalf("хост завершился без ошибки (%v), хотя плагин упал:\n%s", runErr, out.String())
	}
	if ee.ExitCode() != 1 {
		t.Fatalf("код выхода хоста %d, ожидался 1: код плагина (7) не должен становиться кодом хоста\n%s",
			ee.ExitCode(), out.String())
	}

	// И диагностика на пути ошибки печатается, а не отбрасывается.
	text := out.String()
	for _, want := range []string{"запуск плагина:", "завершение:", "доклады:", "журнал:"} {
		if !strings.Contains(text, want) {
			t.Fatalf("в выводе нет %q — диагностика на пути ошибки потеряна:\n%s", want, text)
		}
	}
	if !strings.Contains(text, "crash (code=7)") {
		t.Fatalf("в выводе нет типизированной причины завершения:\n%s", text)
	}
}
