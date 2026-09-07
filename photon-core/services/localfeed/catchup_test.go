package localfeed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
)

// Тесты догрузки после offline. Спецификация — docs/feed-catchup.md.
//
// Проверяется не «сегменты сохраняются», а различие, ради которого они
// заведены: «дальше ничего нет» против «дальше я не знаю». Тест, который
// смотрит только на число записей, оба случая пропустит.

func pos(order int64, id string) Position { return Position{OrderKey: order, ID: id} }

// ── симулированный сервер ────────────────────────────────────────────────
//
// Нужен потому, что сценарий проверяется целиком, включая сходимость цикла
// закрытия пропуска. Порядок выдачи скопирован с Mattermost: свежие первыми,
// а after отдаёт БЛИЖАЙШИЕ к якорю (это и проверяется живьём, §7
// спецификации; тест фиксирует ожидание, а не подменяет прогон).
type fakeServer struct{ recs []Record } // от старых к свежим

func newFakeServer(n int) *fakeServer {
	s := &fakeServer{}
	for i := 0; i < n; i++ {
		s.recs = append(s.recs, rec(fmt.Sprintf("s%03d", i), int64(1000+i)))
	}
	return s
}

// newest — limit самых свежих, свежие первыми.
func (f *fakeServer) newest(limit int) []Record {
	out := []Record{}
	for i := len(f.recs) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, f.recs[i])
	}
	return out
}

// after — ближайшие к якорю записи НОВЕЕ его, отданные свежими первыми.
func (f *fakeServer) after(anchorID string, limit int) []Record {
	idx := -1
	for i, r := range f.recs {
		if r.ID == anchorID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil
	}
	block := f.recs[idx+1:]
	if len(block) > limit {
		block = block[:limit] // БЛИЖАЙШИЕ к якорю, не самые свежие
	}
	out := make([]Record, 0, len(block))
	for i := len(block) - 1; i >= 0; i-- {
		out = append(out, block[i])
	}
	return out
}

// ── контракт позиции ─────────────────────────────────────────────────────

// Сравнение позиций ОБЯЗАНО совпадать с порядком обхода ключей. При равном
// OrderKey обход отдаёт меньший ID первым, значит меньший ID и есть более
// свежий. Тест сверяет сравнение с фактическим обходом, а не с рассуждением.
func TestPositionOrderMatchesTraversalOrder(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	in := []Record{rec("b", 100), rec("a", 100), rec("z", 200)}
	if err := s.Put(ctx, "ch", in); err != nil {
		t.Fatalf("Put: %v", err)
	}
	pg, err := s.Page(ctx, "ch", 0, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	for i := 1; i < len(pg.Records); i++ {
		prev := pos(pg.Records[i-1].OrderKey, pg.Records[i-1].ID)
		cur := pos(pg.Records[i].OrderKey, pg.Records[i].ID)
		if !prev.newerThan(cur) {
			t.Fatalf("сравнение позиций разошлось с обходом: %v не считается свежее %v",
				prev, cur)
		}
	}
}

// ── контракт Ingest ──────────────────────────────────────────────────────

func TestIngestNewestCreatesSingleSegment(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	res, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("a", 300), rec("b", 200), rec("c", 100)},
		Dir:     DirNewest, AtEnd: true,
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Segments != 1 {
		t.Fatalf("сегментов %d, ожидался 1", res.Segments)
	}
	pg, err := s.Page(ctx, "ch", 0, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if pg.GapAfter {
		t.Fatal("непрерывный блок объявлен разорванным")
	}
}

// ЯДРО СРЕЗА: блок, не смыкающийся с известным, даёт ПРОПУСК, а не склейку.
func TestIngestNonAdjacentBlockCreatesGap(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	// прошлая сессия: старый кусок
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("old2", 200), rec("old1", 100)}, Dir: DirNewest,
	}); err != nil {
		t.Fatalf("Ingest(old): %v", err)
	}
	// возвращение: самые свежие, между ними и старым — дыра
	res, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("new2", 900), rec("new1", 800)}, Dir: DirNewest,
	})
	if err != nil {
		t.Fatalf("Ingest(new): %v", err)
	}
	if res.Segments != 2 {
		t.Fatalf("сегментов %d, ожидалось 2 — пропуск не обнаружен", res.Segments)
	}
	// Появление пропуска — ОТРИЦАТЕЛЬНЫЙ GapsClosed. Свойство добавлено по
	// итогам живого прогона: комментарий обещал, что отрицательным значение
	// не бывает, а Bridge вернул -1 на первом же шаге возвращения.
	if res.GapsClosed != -1 {
		t.Fatalf("GapsClosed=%d, ожидалось -1 — появление пропуска не отражено", res.GapsClosed)
	}

	pg, err := s.Page(ctx, "ch", 0, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(pg.Records) != 2 {
		t.Fatalf("страница отдала %d записей — обход не остановился на границе: %v",
			len(pg.Records), ids(pg.Records))
	}
	if !pg.GapAfter {
		t.Fatal("ПРОПУСК СКЛЕЕН МОЛЧА: страница не сообщила о разрыве")
	}
}

// Пересекающиеся блоки сливаются: пропуска нет.
func TestIngestOverlappingBlocksMerge(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("c", 300), rec("b", 200), rec("a", 100)}, Dir: DirNewest,
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	res, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("e", 500), rec("d", 400), rec("c", 300)}, Dir: DirNewest,
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Segments != 1 {
		t.Fatalf("сегментов %d — пересечение не слито", res.Segments)
	}
}

// СИЛЬНЕЙШЕЕ СВОЙСТВО МОДЕЛИ: ответ на after доказывает непрерывность даже
// когда у блока и старого сегмента НЕТ НИ ОДНОЙ общей записи. Пересечением
// это не выводится — только якорем.
func TestIngestAfterAnchorMergesWithoutAnyOverlap(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("a", 100)}, Dir: DirNewest,
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	anchor := pos(100, "a")
	res, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("c", 300), rec("b", 200)},
		Anchor:  &anchor, Dir: DirAfter, AtEnd: true,
	})
	if err != nil {
		t.Fatalf("Ingest(after): %v", err)
	}
	if res.Segments != 1 {
		t.Fatalf("сегментов %d — слияние по якорю не сработало", res.Segments)
	}
}

// Пустой ответ на after с AtEnd закрывает пропуск: сервер заявил, что выше
// якоря ничего нет, значит верхний сегмент — та же непрерывная верхушка.
func TestIngestAfterAtEndMergesEverythingAbove(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("old", 100)}, Dir: DirNewest,
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("new", 900)}, Dir: DirNewest,
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	anchor := pos(100, "old")
	res, err := s.Ingest(ctx, "ch", Block{
		Records: nil, Anchor: &anchor, Dir: DirAfter, AtEnd: true,
	})
	if err != nil {
		t.Fatalf("Ingest(empty after): %v", err)
	}
	if res.Segments != 1 || res.GapsClosed != 1 {
		t.Fatalf("сегментов %d, закрыто %d — пустой ответ не закрыл пропуск",
			res.Segments, res.GapsClosed)
	}
}

// Блок без записей и без якоря не доказывает ничего и обязан быть отвергнут.
func TestIngestRejectsBlocksThatProveNothing(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Ingest(ctx, "ch", Block{Dir: DirNewest}); !errors.Is(err, ErrEmptyBlock) {
		t.Fatalf("пустой DirNewest: got %v, want ErrEmptyBlock", err)
	}
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("a", 100)}, Dir: DirAfter,
	}); !errors.Is(err, ErrAnchorRequired) {
		t.Fatalf("DirAfter без якоря: got %v, want ErrAnchorRequired", err)
	}
}

// Записи и сегменты пишутся ОДНОЙ транзакцией: отказ в середине не оставляет
// ни записей без сегмента, ни сегмента без записей.
func TestIngestIsAtomic(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("a", 100)}, Dir: DirNewest,
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	segsBefore, _ := s.Segments(ctx, "ch")
	countBefore, _ := s.Count(ctx, "ch")

	// вторая запись блока невалидна — транзакция обязана откатиться целиком
	_, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{
			rec("b", 200),
			{ID: "bad", OrderKey: 300, Body: json.RawMessage(`{не json`)},
		},
		Dir: DirNewest,
	})
	if err == nil {
		t.Fatal("невалидное тело принято")
	}
	segsAfter, _ := s.Segments(ctx, "ch")
	countAfter, _ := s.Count(ctx, "ch")
	if countAfter != countBefore {
		t.Fatalf("записей было %d, стало %d — откат неполный", countBefore, countAfter)
	}
	if len(segsAfter) != len(segsBefore) || segsAfter[0] != segsBefore[0] {
		t.Fatalf("сегменты изменились при откате: было %v, стало %v", segsBefore, segsAfter)
	}
}

// ── контракт чтения ──────────────────────────────────────────────────────

// Ниже самого старого сегмента пропуска НЕТ: там граница загруженного, а не
// дыра. Смешать эти два случая — значит показать разрыв там, где надо
// предложить догрузку назад.
func TestPageReportsNoGapBelowOldestSegment(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("b", 200), rec("a", 100)}, Dir: DirNewest,
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	pg, err := s.Page(ctx, "ch", 0, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if pg.GapAfter {
		t.Fatal("граница загруженного объявлена пропуском")
	}
}

// Следующая страница ПОСЛЕ пропуска продолжает чтение в старом сегменте:
// разрыв останавливает страницу, но не чтение вообще.
func TestPageContinuesPastGapOnNextCall(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("old", 100)}, Dir: DirNewest,
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("new", 900)}, Dir: DirNewest,
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	first, err := s.Page(ctx, "ch", 0, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(first.Records) != 1 || !first.GapAfter {
		t.Fatalf("первая страница: %v gap=%v", ids(first.Records), first.GapAfter)
	}
	last := first.Records[len(first.Records)-1]
	second, err := s.Page(ctx, "ch", last.OrderKey, last.ID, 10)
	if err != nil {
		t.Fatalf("Page 2: %v", err)
	}
	if len(second.Records) != 1 || second.Records[0].ID != "old" {
		t.Fatalf("вторая страница не дошла до старого сегмента: %v", ids(second.Records))
	}
}

// ── якорь ────────────────────────────────────────────────────────────────

// ЛОВУШКА, ради которой GapAnchor вынесен в отдельный метод: якорь для
// закрытия дыры — свежий край сегмента ПОД ней, а не самая свежая запись.
// Взяв watermark, клиент запрашивал бы уже известное и не закрыл бы дыру
// никогда.
func TestGapAnchorIsBelowGapNotWatermark(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("old2", 200), rec("old1", 100)}, Dir: DirNewest,
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("new2", 900), rec("new1", 800)}, Dir: DirNewest,
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	anchor, ok, err := s.GapAnchor(ctx, "ch")
	if err != nil || !ok {
		t.Fatalf("GapAnchor: %v ok=%v", err, ok)
	}
	if anchor.ID != "old2" {
		t.Fatalf("якорь %q, ожидался old2 — свежий край сегмента ПОД пропуском", anchor.ID)
	}

	wm, ok, err := s.Watermark(ctx, "ch")
	if err != nil || !ok {
		t.Fatalf("Watermark: %v ok=%v", err, ok)
	}
	if wm.ID != "new2" {
		t.Fatalf("watermark %q, ожидался new2", wm.ID)
	}
	if wm.ID == anchor.ID {
		t.Fatal("GapAnchor совпал с Watermark — дыра не закроется никогда")
	}
}

// Пропусков нет — якоря нет. Иначе клиент крутил бы цикл впустую.
func TestGapAnchorAbsentWhenFeedIsContiguous(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("a", 100)}, Dir: DirNewest,
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if _, ok, err := s.GapAnchor(ctx, "ch"); err != nil || ok {
		t.Fatalf("GapAnchor на непрерывной ленте: ok=%v err=%v", ok, err)
	}
}

// ── сценарий целиком ─────────────────────────────────────────────────────

// Догрузка сходится: цикл закрывает пропуск за конечное число шагов, каждая
// запись оказывается ровно одна, порядок не нарушен, разрывов не остаётся.
func TestCatchupConvergesWithoutGapOrDuplicate(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	const pageSize = 10

	srv := newFakeServer(120)

	// прошлая сессия: самые старые 10 записей
	oldBlock := []Record{}
	for i := 9; i >= 0; i-- {
		oldBlock = append(oldBlock, srv.recs[i])
	}
	if _, err := s.Ingest(ctx, "ch", Block{Records: oldBlock, Dir: DirNewest}); err != nil {
		t.Fatalf("Ingest(old): %v", err)
	}

	// возвращение: самые свежие
	nb := srv.newest(pageSize)
	if _, err := s.Ingest(ctx, "ch", Block{
		Records: nb, Dir: DirNewest, AtEnd: len(nb) < pageSize,
	}); err != nil {
		t.Fatalf("Ingest(newest): %v", err)
	}

	// закрытие пропуска
	steps, maxSteps := 0, 60
	for {
		anchor, ok, err := s.GapAnchor(ctx, "ch")
		if err != nil {
			t.Fatalf("GapAnchor: %v", err)
		}
		if !ok {
			break
		}
		steps++
		if steps > maxSteps {
			t.Fatalf("цикл не сошёлся за %d шагов — якорь не продвигается", maxSteps)
		}
		block := srv.after(anchor.ID, pageSize)
		if _, err := s.Ingest(ctx, "ch", Block{
			Records: block, Anchor: &anchor, Dir: DirAfter, AtEnd: len(block) < pageSize,
		}); err != nil {
			t.Fatalf("Ingest(after): %v", err)
		}
	}
	if steps == 0 {
		t.Fatal("пропуск не был обнаружен — сценарий ничего не проверил")
	}
	t.Logf("пропуск закрыт за %d заливок по %d записей", steps, pageSize)

	// лента непрерывна, каждая запись ровно одна, порядок соблюдён
	seen := map[string]int{}
	var prev int64 = 1<<63 - 1
	var curKey int64
	var curID string
	pages := 0
	for {
		if pages > 100 {
			t.Fatal("более 100 страниц — курсор не двигается")
		}
		pg, err := s.Page(ctx, "ch", curKey, curID, pageSize)
		if err != nil {
			t.Fatalf("Page: %v", err)
		}
		if len(pg.Records) == 0 {
			break
		}
		pages++
		if pg.GapAfter {
			t.Fatalf("после схождения остался разрыв на странице %d", pages)
		}
		for _, r := range pg.Records {
			seen[r.ID]++
			if r.OrderKey > prev {
				t.Fatalf("порядок нарушен: %d после %d", r.OrderKey, prev)
			}
			prev = r.OrderKey
		}
		last := pg.Records[len(pg.Records)-1]
		curKey, curID = last.OrderKey, last.ID
	}
	if len(seen) != len(srv.recs) {
		t.Fatalf("покрыто %d записей из %d", len(seen), len(srv.recs))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("запись %s отдана %d раз", id, n)
		}
	}
}

// Сегменты переживают перезапуск: без этого offline reopen не знал бы, что
// известно, и склеил бы дыру.
func TestSegmentsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "feed.db")
	migs := feedMigrations()
	ctx := context.Background()

	st, err := bboltadapter.New(path, migs)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s := New(st, nil)
	if err := s.Prepare(ctx); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if _, err := s.Ingest(ctx, "ch", Block{Records: []Record{rec("old", 100)}, Dir: DirNewest}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if _, err := s.Ingest(ctx, "ch", Block{Records: []Record{rec("new", 900)}, Dir: DirNewest}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	st.Close(ctx)

	st2, err := bboltadapter.New(path, migs)
	if err != nil {
		t.Fatalf("повторное открытие: %v", err)
	}
	defer st2.Close(ctx)
	s2 := New(st2, nil)

	segs, err := s2.Segments(ctx, "ch")
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segs) != 2 {
		t.Fatalf("после перезапуска сегментов %d, ожидалось 2", len(segs))
	}
	pg, err := s2.Page(ctx, "ch", 0, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if !pg.GapAfter {
		t.Fatal("после перезапуска пропуск склеен молча")
	}
}

// Канал без сегментов (данные легли через Put) читается как раньше: разрывов
// не заявляется. Иначе прежние базы начали бы показывать выдуманные дыры.
func TestLegacyDataWithoutSegmentsReportsNoGap(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if err := s.Put(ctx, "ch", []Record{rec("a", 100), rec("b", 900)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	pg, err := s.Page(ctx, "ch", 0, "", 10)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	if len(pg.Records) != 2 || pg.GapAfter {
		t.Fatalf("данные без сегментов прочитаны иначе: %v gap=%v", ids(pg.Records), pg.GapAfter)
	}
}

// Сегменты не попадают в ленту как запись: их ключ лежит в другом
// пространстве. Иначе Count и Page спотыкались бы о собственные метаданные.
func TestSegmentRecordIsNotVisibleAsFeedEntry(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Ingest(ctx, "ch", Block{Records: []Record{rec("a", 100)}, Dir: DirNewest}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	n, err := s.Count(ctx, "ch")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 1 {
		t.Fatalf("Count вернул %d — запись сегментов попала в ленту", n)
	}
}

// ── алгебра сегментов напрямую ───────────────────────────────────────────

// overlaps объявлен СИММЕТРИЧНЫМ предикатом, и проверять его надо как
// предикат, а не через normalize.
//
// Через normalize вторая половина условия недостижима: список отсортирован по
// свежему краю, поэтому строго вложенным может оказаться только младший
// аргумент, и одной проверки хватает. Мутация «убрать вторую половину»
// проходила зелёной именно поэтому — не потому, что случай невозможен, а
// потому, что путь до него закрыт сортировкой. Предикат от этого корректным
// не становится: он публичен внутри пакета, и следующий вызывающий сортировки
// не унаследует.
func TestSegmentOverlapIsSymmetric(t *testing.T) {
	outer := Segment{Oldest: pos(100, "a"), Newest: pos(900, "z")}
	inner := Segment{Oldest: pos(400, "m"), Newest: pos(500, "n")}

	if !outer.overlaps(inner) {
		t.Fatal("вложенный сегмент не распознан как пересекающийся (внешний слева)")
	}
	if !inner.overlaps(outer) {
		t.Fatal("вложенный сегмент не распознан как пересекающийся (внешний справа)")
	}

	// Непересекающиеся — в обе стороны тоже.
	low := Segment{Oldest: pos(100, "a"), Newest: pos(200, "b")}
	high := Segment{Oldest: pos(800, "y"), Newest: pos(900, "z")}
	if low.overlaps(high) || high.overlaps(low) {
		t.Fatal("разделённые сегменты объявлены пересекающимися")
	}

	// Касание одной позицией — пересечение, иначе слияние не состоялось бы.
	touch := Segment{Oldest: pos(200, "b"), Newest: pos(300, "c")}
	if !low.overlaps(touch) || !touch.overlaps(low) {
		t.Fatal("сегменты, делящие одну позицию, не слились бы")
	}
}

// РЕГРЕССИЯ ЖИВОГО ПРОГОНА: пропуск на границе, совпавшей с концом страницы.
//
// Сегмент из 3 записей при странице в 3 записи: limit достигался раньше, чем
// проверка сегмента, и разрыв молча склеивался. При странице в 30 на тех же
// данных он был виден — то есть дефект зависел от размера страницы, а не от
// данных, и любой тест с «удобным» размером его пропускал.
//
// Проверяется на ВСЕХ размерах страницы вокруг границы, а не на одном.
func TestPageReportsGapWhenSegmentEndsExactlyAtPageLimit(t *testing.T) {
	for _, size := range []int{1, 2, 3, 4, 5} {
		t.Run(fmt.Sprintf("страница_%d", size), func(t *testing.T) {
			s, _ := newStore(t)
			ctx := context.Background()
			// верхний сегмент ровно из 3 записей
			if _, err := s.Ingest(ctx, "ch", Block{
				Records: []Record{rec("n3", 903), rec("n2", 902), rec("n1", 901)},
				Dir:     DirNewest,
			}); err != nil {
				t.Fatalf("Ingest(new): %v", err)
			}
			// старый сегмент, отделённый пропуском
			if _, err := s.Ingest(ctx, "ch", Block{
				Records: []Record{rec("o2", 102), rec("o1", 101)},
				Dir:     DirBefore, Anchor: &Position{OrderKey: 103, ID: "o3"},
			}); err != nil {
				t.Fatalf("Ingest(old): %v", err)
			}

			// Читаем, пока не кончится верхний сегмент, и требуем, чтобы
			// разрыв был объявлен ровно один раз — на его границе.
			var curKey int64
			var curID string
			gaps, yielded, pages := 0, 0, 0
			for {
				if pages > 20 {
					t.Fatal("более 20 страниц — курсор не двигается")
				}
				pg, err := s.Page(ctx, "ch", curKey, curID, size)
				if err != nil {
					t.Fatalf("Page: %v", err)
				}
				if len(pg.Records) == 0 {
					break
				}
				pages++
				yielded += len(pg.Records)
				if pg.GapAfter {
					gaps++
					if yielded != 3 {
						t.Fatalf("разрыв объявлен после %d записей, а сегмент содержит 3", yielded)
					}
				}
				last := pg.Records[len(pg.Records)-1]
				curKey, curID = last.OrderKey, last.ID
			}
			if gaps != 1 {
				t.Fatalf("разрыв объявлен %d раз при странице %d — ожидался ровно один", gaps, size)
			}
			if yielded != 5 {
				t.Fatalf("отдано %d записей из 5", yielded)
			}
		})
	}
}
