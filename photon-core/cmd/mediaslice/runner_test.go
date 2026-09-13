package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Локальные тесты РАННЕРА, а не среза.
//
// Проверяется одно свойство: сценарий ЛОВИТ нарушение. Каждый тест ниже
// портит поддельный Bridge ровно в одном месте и требует, чтобы прогон стал
// красным. Без этого «зелёный прогон» на живом сервере не значил бы ничего —
// ровно та ошибка, что была найдена в Phase 13.5, где проверка «отвергнут без
// сети» ничего не измеряла.

func newRunner(t *testing.T, f *fakeBridge, withB bool, only ...string) *Runner {
	t.Helper()
	r := &Runner{
		a:   f.actor("user-A"),
		rep: &report{},
		opts: Options{
			Size:      8 << 10,
			Streams:   120,
			StallWait: 2 * time.Second,
			// Серверный дедлайн в миниатюре. Порог S1 — половина от него;
			// исправный поддельный Bridge держит брошенный стрим 150 мс.
			StallDeadline: 200 * time.Millisecond,
			CallTimeout:   5 * time.Second,
			WorkDir:       t.TempDir(),
			Only:          parseOnly(strings.Join(only, ",")),
		},
	}
	if withB {
		r.b = f.actor("user-B")
	}
	return r
}

func statusOf(t *testing.T, r *Runner, id string) status {
	t.Helper()
	for _, c := range r.rep.checks {
		if c.id == id {
			return c.st
		}
	}
	t.Fatalf("проверки %s в отчёте нет; есть: %v", id, allIDs(r))
	return ""
}

func allIDs(r *Runner) []string {
	out := make([]string, 0, len(r.rep.checks))
	for _, c := range r.rep.checks {
		out = append(out, c.id+"="+string(c.st))
	}
	return out
}

func requireFailed(t *testing.T, r *Runner, err error, ids ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("прогон зелёный, хотя сервер сломан; отчёт: %v", allIDs(r))
	}
	for _, id := range ids {
		if got := statusOf(t, r, id); got != statusFail {
			t.Errorf("%s: статус %s, ожидался FAIL; отчёт: %v", id, got, allIDs(r))
		}
	}
}

// ── Исправный сервер ────────────────────────────────────────────────

func TestRunnerPassesAgainstCorrectBridge(t *testing.T) {
	f := newFakeBridge()
	r := newRunner(t, f, true)
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("исправный сервер дал красный прогон: %v; отчёт: %v", err, allIDs(r))
	}
	for _, id := range []string{"P1", "P4", "U1", "I3", "I4", "A4", "A6", "D2", "D3", "N1", "N3", "N6", "N7", "S1", "C1", "E1"} {
		if got := statusOf(t, r, id); got != statusPass {
			t.Errorf("%s: статус %s, ожидался PASS", id, got)
		}
	}
	// Невыполнимое обязано быть объявлено невыполненным, а не зелёным.
	//
	// N6 отсюда УШЛА: до уборки C (ADR-055) стимула не существовало —
	// DELETED не присваивался нигде, — и проверка честно объявлялась
	// невыполненной. Теперь стимул есть, и вместе с N6 появилась N7.
	for _, id := range []string{"E2", "E3"} {
		if got := statusOf(t, r, id); got != statusSkip {
			t.Errorf("%s: статус %s, ожидался NOT EXECUTED", id, got)
		}
	}
}

func TestRunnerMarksForeignCheckNotExecutedWithoutSecondSubject(t *testing.T) {
	f := newFakeBridge()
	r := newRunner(t, f, false)
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("прогон без субъекта B должен быть зелёным: %v; отчёт: %v", err, allIDs(r))
	}
	if got := statusOf(t, r, "N1"); got != statusSkip {
		t.Fatalf("N1 без -session-b: статус %s, ожидался NOT EXECUTED", got)
	}
}

// ── Мутации сервера ─────────────────────────────────────────────────

func TestRunnerCatchesCorruptedDownload(t *testing.T) {
	f := newFakeBridge()
	f.corruptDownload = true
	r := newRunner(t, f, false, "prep", "upload", "attach", "download", "evidence")
	err := r.Run(context.Background())
	// D1 падает первым: клиент media обнаруживает расхождение хеша сам.
	// E1 обязан упасть следом — утверждение о прозрачности тракта опирается
	// на побайтовое совпадение и без него держаться не может.
	requireFailed(t, r, err, "D1", "E1")
}

// Согласованная подмена: сервер отдаёт искажённые байты И объявляет их хеш.
// Проверка клиента media проходит целиком — расхождения с заявленным нет.
// Ловит это только независимая сверка с исходником, и если её убрать, прогон
// объявит успех о файле, который не равен отправленному.
func TestRunnerCatchesSilentlyAlteredDownload(t *testing.T) {
	f := newFakeBridge()
	f.corruptDownload = true
	f.relabelDownload = true
	r := newRunner(t, f, false, "prep", "upload", "attach", "download")
	err := r.Run(context.Background())
	requireFailed(t, r, err, "D3", "D4")
	if got := statusOf(t, r, "D1"); got != statusPass {
		t.Fatalf("D1: статус %s — подмена должна быть НЕЗАМЕТНА для клиента, иначе тест ловит не то", got)
	}
}

func TestRunnerCatchesClientSuppliedProps(t *testing.T) {
	f := newFakeBridge()
	f.echoClientProps = true
	r := newRunner(t, f, false, "prep", "upload", "attach")
	requireFailed(t, r, r.Run(context.Background()), "A4", "A5")
}

func TestRunnerCatchesReattachAccepted(t *testing.T) {
	f := newFakeBridge()
	f.allowReattach = true
	r := newRunner(t, f, false, "prep", "upload", "attach")
	requireFailed(t, r, r.Run(context.Background()), "A6")
}

// Сервер БЕЗ уборки C: удаление поста не трогает media. Ровно то поведение,
// что было до ADR-055, и ровно то, ради чего N6 и N7 заведены.
func TestRunnerCatchesMediaSurvivingItsPost(t *testing.T) {
	f := newFakeBridge()
	f.keepMediaAfterDelete = true
	r := newRunner(t, f, false, "prep", "upload", "attach", "negative")
	requireFailed(t, r, r.Run(context.Background()), "N6", "N7")
}

// Сервер, снёсший метаданные вместе с байтами: удалённый объект отвечает
// forbidden, то есть становится неотличим от никогда не существовавшего.
// Отказ приходит — и проверка «пришла какая-то ошибка» зеленела бы. Ловит
// это только сравнение ТОЧНОЙ причины (media-transfer.md §5).
func TestRunnerCatchesDeletedMediaReportedAsForbidden(t *testing.T) {
	f := newFakeBridge()
	f.deletedLooksForbidden = true
	r := newRunner(t, f, false, "prep", "upload", "attach", "negative")
	requireFailed(t, r, r.Run(context.Background()), "N6", "N7")
}

func TestRunnerCatchesWrongInfoMetadata(t *testing.T) {
	f := newFakeBridge()
	f.wrongInfo = true
	r := newRunner(t, f, false, "prep", "upload", "info")
	requireFailed(t, r, r.Run(context.Background()), "I3", "I4")
}

func TestRunnerCatchesAcceptedTruncatedUpload(t *testing.T) {
	f := newFakeBridge()
	f.acceptTruncated = true
	r := newRunner(t, f, false, "prep", "deadline")
	requireFailed(t, r, r.Run(context.Background()), "S1")
}

func TestRunnerCatchesStalledStreamNeverReleased(t *testing.T) {
	f := newFakeBridge()
	f.neverRelease = true
	r := newRunner(t, f, false, "prep", "deadline")
	start := time.Now()
	err := r.Run(context.Background())
	// Существенно: провал наступает по НАШЕЙ границе ожидания, и это ровно
	// то, что записано в проверке как отказ, — иначе измерялось бы наше
	// терпение, а не поведение сервера.
	requireFailed(t, r, err, "S1")
	if elapsed := time.Since(start); elapsed < r.opts.StallWait {
		t.Fatalf("провал наступил за %s, раньше границы ожидания %s", elapsed, r.opts.StallWait)
	}
}

func TestRunnerCatchesCreditLeak(t *testing.T) {
	f := newFakeBridge()
	f.creditLimit = 100
	r := newRunner(t, f, false, "prep", "upload", "streams")
	requireFailed(t, r, r.Run(context.Background()), "C1")
}

func TestRunnerCatchesForeignMediaAllowed(t *testing.T) {
	f := newFakeBridge()
	f.foreignAllowed = true
	r := newRunner(t, f, true, "prep", "upload", "negative")
	requireFailed(t, r, r.Run(context.Background()), "N1", "N2")
}

func TestRunnerCatchesUnattachedMediaUnreadable(t *testing.T) {
	f := newFakeBridge()
	f.attachedOnly = true
	r := newRunner(t, f, false, "prep", "negative")
	// N5 не видит состояния напрямую: STORED измеряется поведением —
	// читается до прикрепления и прикрепляется ровно один раз. Мутация бьёт
	// в первую половину, и без этого теста проверка зеленела бы на сервере,
	// у которого STORED недостижим.
	requireFailed(t, r, r.Run(context.Background()), "N5")
}

func TestRunnerCatchesGhostMediaAccepted(t *testing.T) {
	f := newFakeBridge()
	f.ghostAllowed = true
	r := newRunner(t, f, false, "prep", "negative")
	requireFailed(t, r, r.Run(context.Background()), "N3", "N4")
}

// ── Мутации ослабленных oracle ──────────────────────────────────────
//
// Эти четыре теста заведены находкой Stage 6: прогон против Bridge, у
// которого умерло RPC-соединение с хостом, дал PASS по N1–N4 и S1. Причина
// одна — проверки принимали ЛЮБОЙ отказ и любую не-контекстную ошибку,
// поэтому проходили и там, где сервер не мог принять ни одного решения.
// Каждый тест ниже воспроизводит ровно ту форму сервера и требует, чтобы
// проверка больше не зеленела.

// Сервер отвергает медиа не по существу, а как непосвящённого. Отказ есть —
// но контракт для чужого и несуществующего объекта требует forbidden, и
// подмена причины обязана быть провалом, а не успехом.
func TestRunnerCatchesDenialWithWrongReason(t *testing.T) {
	f := newFakeBridge()
	f.mediaDenyReason = "unauthorized"
	r := newRunner(t, f, true, "prep", "upload", "negative")
	requireFailed(t, r, r.Run(context.Background()), "N1", "N2", "N3", "N4")
}

// Сервер перестаёт подтверждать субъекта посреди прогона: getMe отвечает
// unauthorized. Отказы после этого не значат ничего — и обязаны попасть в
// отчёт как НЕ ВЫПОЛНЕННЫЕ. Требование здесь сильнее, чем «не PASS»: PASS
// был бы ложью, а FAIL — обвинением сервера в том, чего не измеряли.
func TestRunnerReportsNegativesNotExecutedWhenWitnessIsDead(t *testing.T) {
	f := newFakeBridge()
	f.denyGetMeAfter = 1 // одного хватает на P1; свидетелям уже не достанется
	r := newRunner(t, f, true, "prep", "upload", "negative")
	_ = r.Run(context.Background())
	for _, id := range []string{"N1", "N2", "N3", "N4", "N5"} {
		if got := statusOf(t, r, id); got != statusSkip {
			t.Errorf("%s: статус %s, ожидался NOT EXECUTED — сервер не подтверждает субъекта, "+
				"измерить отказ нечем; отчёт: %v", id, got, allIDs(r))
		}
	}
}

// Брошенный стрим отвергается МГНОВЕННО. Это не освобождение по дедлайну, а
// отказ принять передачу: ждать было нечего. Прежняя проверка засчитывала
// такой ответ как успех — «освобождён за 0 s».
func TestRunnerCatchesInstantRejectionOfStalledStream(t *testing.T) {
	f := newFakeBridge()
	f.stallHold = 0
	r := newRunner(t, f, false, "prep", "deadline")
	requireFailed(t, r, r.Run(context.Background()), "S1")
}

// Брошенный стрим отвергается по авторизации. Ровно то, что видел живой
// прогон: причина unauthorized, время 0 s. Даже выдержав паузу, такой ответ
// не доказывает освобождения стрима — сервер его не принимал.
func TestRunnerCatchesStalledStreamRejectedByAuthorization(t *testing.T) {
	f := newFakeBridge()
	f.stallReason = "unauthorized"
	r := newRunner(t, f, false, "prep", "deadline")
	requireFailed(t, r, r.Run(context.Background()), "S1")
}

// ── Учёт исходов ────────────────────────────────────────────────────

func TestReportResultFailsWhenAnyCheckFailed(t *testing.T) {
	r := &report{}
	r.pass("X1", "всё хорошо")
	r.skip("X2", "недостижимо", "нет стимула")
	if err := r.result(); err != nil {
		t.Fatalf("PASS+NOT EXECUTED дали ошибку: %v", err)
	}
	r.fail("X3", "сломано")
	if err := r.result(); err == nil {
		t.Fatal("после провалившейся проверки прогон объявлен успешным")
	}
	p, f, s := r.counts()
	if p != 1 || f != 1 || s != 1 {
		t.Fatalf("счётчики: pass=%d fail=%d skip=%d, ожидалось 1/1/1", p, f, s)
	}
}

// ── Данные и независимая сверка ─────────────────────────────────────

func TestGenerateSourceIsDeterministicAndCarriesAdversarialBytes(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.bin"), filepath.Join(dir, "b.bin")
	const size = 16 << 10
	for _, p := range []string{a, b} {
		if err := generateSource(p, size); err != nil {
			t.Fatalf("generateSource: %v", err)
		}
	}
	ha, na, err := hashFile(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, nb, err := hashFile(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb || na != nb || na != size {
		t.Fatalf("генератор недетерминирован: %s/%d против %s/%d", ha, na, hb, nb)
	}

	content, err := os.ReadFile(a)
	if err != nil {
		t.Fatal(err)
	}
	// Ровно те свойства, на которых стоит утверждение E1.
	seen := map[byte]bool{}
	for _, c := range content {
		seen[c] = true
	}
	if len(seen) != 256 {
		t.Errorf("в файле %d различных значений байта, ожидалось 256", len(seen))
	}
	for _, marker := range [][]byte{
		{0x00},
		{0xff, 0xfe, 0xc3, 0x28},
		[]byte(`{"ok":true,"error":"","data":null}`),
		[]byte(`{"ok":false,"error":"forbidden"}`),
	} {
		if !bytes.Contains(content, marker) {
			t.Errorf("в файле нет врезки %q", marker)
		}
	}
	if err := verifySourceMarkers(a); err != nil {
		t.Errorf("verifySourceMarkers на исправном файле: %v", err)
	}
}

func TestVerifySourceMarkersDetectsLostTail(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.bin")
	if err := generateSource(p, 4096); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(p, 4000); err != nil {
		t.Fatal(err)
	}
	if err := verifySourceMarkers(p); err == nil {
		t.Fatal("потеря хвоста не обнаружена — концевая врезка не проверяется")
	}
}

func TestGenerateSourceRejectsSizeBelowMarkers(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.bin")
	if err := generateSource(p, minSourceSize()-1); err == nil {
		t.Fatal("размер меньше врезок принят")
	}
}

func TestHashFileDetectsSingleByteFlip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.bin")
	if err := generateSource(p, 4096); err != nil {
		t.Fatal(err)
	}
	before, _, err := hashFile(p)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0x01}, 2048); err != nil {
		t.Fatal(err)
	}
	f.Close()
	after, _, err := hashFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("независимый хеш не заметил изменения одного байта")
	}
}

// ── Разбор ленты ────────────────────────────────────────────────────

func TestFindPostInFeedAndMediaExtraction(t *testing.T) {
	feed := []byte(`{"order":["p1"],"posts":{"p1":{"id":"p1","props":{"photon_media":[{"media_id":"m1","sha256":"ab","size":7,"name":"n"}]}}}}`)
	raw, err := findPostInFeed(feed, "p1")
	if err != nil {
		t.Fatalf("пост не найден: %v", err)
	}
	refs, err := mediaFromPost(raw)
	if err != nil {
		t.Fatalf("photon_media не извлечён: %v", err)
	}
	if len(refs) != 1 || refs[0].MediaID != "m1" || refs[0].Size != 7 {
		t.Fatalf("извлечено не то: %+v", refs)
	}
	if _, err := findPostInFeed(feed, "p2"); err == nil {
		t.Error("отсутствующий пост найден")
	}
	if _, err := findPostInFeed([]byte(`{}`), "p1"); err == nil {
		t.Error("лента без posts принята")
	}
}

func TestMediaFromPostRejectsMissingOrEmptyProps(t *testing.T) {
	for name, post := range map[string]string{
		"без props":       `{"id":"p1"}`,
		"props без ключа": `{"id":"p1","props":{"other":1}}`,
		"пустой список":   `{"id":"p1","props":{"photon_media":[]}}`,
		"не список":       `{"id":"p1","props":{"photon_media":{"media_id":"m1"}}}`,
	} {
		if _, err := mediaFromPost(json.RawMessage(post)); err == nil {
			t.Errorf("%s: принято, ожидался отказ", name)
		}
	}
}
