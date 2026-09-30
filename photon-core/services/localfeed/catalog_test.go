package localfeed

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	"github.com/wiloris/photon-core/ports"
)

// Курсор синхронизации, каталог каналов и работа «в той же транзакции»
// (IngestWith). Проверяется на настоящем bbolt, в том числе после закрытия
// и повторного открытия файла.

func openAt(t *testing.T, path string) (*Store, func()) {
	t.Helper()
	st, err := bboltadapter.New(path, feedMigrations())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s := New(st, nil)
	if err := s.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	return s, func() { st.Close(context.Background()) }
}

func afterBlock(anchor Position, atEnd bool, recs ...Record) Block {
	a := anchor
	return Block{Records: recs, Dir: DirAfter, Anchor: &a, AtEnd: atEnd}
}

func TestCursorStoredWithBlock(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	if _, found, err := s.Cursor(ctx, "ch"); err != nil || found {
		t.Fatalf("курсора до первой синхронизации быть не должно: found=%v err=%v", found, err)
	}
	if _, err := s.IngestWith(ctx, "ch", newestBlock(rec("a", 100)), IngestOptions{Cursor: []byte("c1")}); err != nil {
		t.Fatalf("IngestWith: %v", err)
	}
	cur, found, err := s.Cursor(ctx, "ch")
	if err != nil || !found || string(cur) != "c1" {
		t.Fatalf("курсор = %q found=%v err=%v", cur, found, err)
	}

	// nil — курсор не меняется.
	if _, err := s.IngestWith(ctx, "ch", afterBlock(Position{100, "a"}, true, rec("b", 200)), IngestOptions{}); err != nil {
		t.Fatalf("IngestWith без курсора: %v", err)
	}
	if cur, _, _ := s.Cursor(ctx, "ch"); string(cur) != "c1" {
		t.Fatalf("курсор изменился без указания: %q", cur)
	}

	// Пустой, но заданный — ошибка аргумента, блок не пишется.
	if _, err := s.IngestWith(ctx, "ch", afterBlock(Position{200, "b"}, true, rec("c", 300)), IngestOptions{Cursor: []byte{}}); !errors.Is(err, ErrEmptyCursor) {
		t.Fatalf("пустой курсор: err = %v, ожидался ErrEmptyCursor", err)
	}
	if n, _ := s.Count(ctx, "ch"); n != 2 {
		t.Fatalf("после отказа записей %d, ожидалось 2", n)
	}
}

// Ошибка Also откатывает всё: записи, сегменты, курсор, каталог.
func TestIngestWithAlsoFailureRollsBackEverything(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.IngestWith(ctx, "ch", newestBlock(rec("a", 100)), IngestOptions{Cursor: []byte("c1")}); err != nil {
		t.Fatalf("IngestWith: %v", err)
	}
	segsBefore, _ := s.Segments(ctx, "ch")
	catBefore, _ := s.Catalog(ctx)

	boom := errors.New("boom")
	_, err := s.IngestWith(ctx, "ch", afterBlock(Position{100, "a"}, true, rec("b", 200)), IngestOptions{
		Cursor: []byte("c2"),
		Also:   func(ports.WriteTx) error { return boom },
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, ожидалась ошибка Also", err)
	}
	if n, _ := s.Count(ctx, "ch"); n != 1 {
		t.Fatalf("запись блока осталась после отката: записей %d", n)
	}
	if cur, _, _ := s.Cursor(ctx, "ch"); string(cur) != "c1" {
		t.Fatalf("курсор сдвинулся при откате: %q", cur)
	}
	if segs, _ := s.Segments(ctx, "ch"); len(segs) != len(segsBefore) || segs[0] != segsBefore[0] {
		t.Fatalf("сегменты изменились при откате: %v → %v", segsBefore, segs)
	}
	if cat, _ := s.Catalog(ctx); cat[0] != catBefore[0] {
		t.Fatalf("каталог изменился при откате: %v → %v", catBefore, cat)
	}
}

// Also видит блок, уже записанный в той же транзакции.
func TestIngestWithAlsoRunsInsideTheSameTransaction(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	var seen bool
	_, err := s.IngestWith(ctx, "ch", newestBlock(rec("a", 100)), IngestOptions{
		Cursor: []byte("c1"),
		Also: func(tx ports.WriteTx) error {
			if _, err := tx.Get(Namespace, key("ch", 100, "a")); err != nil {
				return err
			}
			v, err := tx.Get(Namespace, cursorKey("ch"))
			if err != nil || !bytes.Equal(v, []byte("c1")) {
				return errors.New("курсор не виден внутри транзакции")
			}
			seen = true
			return nil
		},
	})
	if err != nil || !seen {
		t.Fatalf("Also не увидел блок и курсор своей транзакции: seen=%v err=%v", seen, err)
	}
}

func TestCatalogTracksNewestPerChannel(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	if cat, err := s.Catalog(ctx); err != nil || len(cat) != 0 {
		t.Fatalf("пустое хранилище: каталог %v err=%v", cat, err)
	}
	// Put тоже ведёт каталог.
	if err := s.Put(ctx, "a", []Record{rec("x", 100), rec("y", 300)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.Ingest(ctx, "b", newestBlock(rec("z", 200))); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	cat, err := s.Catalog(ctx)
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	want := []CatalogEntry{
		{ChannelID: "a", Newest: Position{300, "y"}},
		{ChannelID: "b", Newest: Position{200, "z"}},
	}
	if len(cat) != 2 || cat[0] != want[0] || cat[1] != want[1] {
		t.Fatalf("каталог = %v, ожидался %v", cat, want)
	}

	// Более старая запись позицию не опускает; более свежая — поднимает и
	// переставляет каналы.
	if err := s.Put(ctx, "a", []Record{rec("old", 50)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.Ingest(ctx, "b", afterBlock(Position{200, "z"}, true, rec("w", 400))); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	cat, _ = s.Catalog(ctx)
	if cat[0] != (CatalogEntry{"b", Position{400, "w"}}) || cat[1] != (CatalogEntry{"a", Position{300, "y"}}) {
		t.Fatalf("каталог после записей = %v", cat)
	}

	// Пустой блок с якорем ничего в каталог не добавляет.
	if _, err := s.Ingest(ctx, "c", afterBlock(Position{1, "q"}, true)); err != nil {
		t.Fatalf("Ingest пустого блока: %v", err)
	}
	if cat, _ := s.Catalog(ctx); len(cat) != 2 {
		t.Fatalf("канал без записей попал в каталог: %v", cat)
	}
}

// Одинаковый OrderKey: свежее — меньший ID (замороженное сравнение ADR-052),
// и каталог обязан с ним совпадать.
func TestCatalogUsesFrozenPositionOrder(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if err := s.Put(ctx, "ch", []Record{rec("b", 100), rec("a", 100)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cat, _ := s.Catalog(ctx)
	pg, _ := s.Page(ctx, "ch", 0, "", 1)
	if cat[0].Newest != (Position{pg.Records[0].OrderKey, pg.Records[0].ID}) {
		t.Fatalf("каталог %v расходится с первой записью Page %v", cat[0].Newest, pg.Records[0])
	}
}

// Несколько разговоров не смешивают курсоры, сегменты и каталог.
func TestChannelsDoNotShareSyncState(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.IngestWith(ctx, "a", newestBlock(rec("a1", 100)), IngestOptions{Cursor: []byte("ca")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.IngestWith(ctx, "b", newestBlock(rec("b1", 200)), IngestOptions{Cursor: []byte("cb")}); err != nil {
		t.Fatal(err)
	}
	ca, _, _ := s.Cursor(ctx, "a")
	cb, _, _ := s.Cursor(ctx, "b")
	if string(ca) != "ca" || string(cb) != "cb" {
		t.Fatalf("курсоры смешались: a=%q b=%q", ca, cb)
	}
	if n, _ := s.Count(ctx, "a"); n != 1 {
		t.Fatalf("в канале a записей %d", n)
	}
	if segs, _ := s.Segments(ctx, "a"); len(segs) != 1 || segs[0].Newest.ID != "a1" {
		t.Fatalf("сегменты a: %v", segs)
	}
}

// Курсор и каталог переживают закрытие и повторное открытие файла.
func TestCursorAndCatalogSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "feed.db")
	ctx := context.Background()
	s, closeFn := openAt(t, path)
	if _, err := s.IngestWith(ctx, "ch", newestBlock(rec("a", 100)), IngestOptions{Cursor: []byte("c1")}); err != nil {
		t.Fatal(err)
	}
	closeFn()

	s, closeFn = openAt(t, path)
	defer closeFn()
	cur, found, _ := s.Cursor(ctx, "ch")
	cat, _ := s.Catalog(ctx)
	if !found || string(cur) != "c1" || len(cat) != 1 || cat[0].Newest != (Position{100, "a"}) {
		t.Fatalf("после переоткрытия: курсор %q/%v, каталог %v", cur, found, cat)
	}
}

// Окно перекрытия (ADR-092 §2.2) ложится на ADR-065 п. 3.9 без поправки:
// якорь — нижняя граница окна L, выраженная как (L−1, ""), самая свежая
// возможная позиция ниже L. Записи окна, уже известные локально, законны,
// блок сливается с верхним сегментом, пропуска не появляется.
func TestOverlapWindowAnchorMergesWithoutCrossing(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	if _, err := s.Ingest(ctx, "ch", newestBlock(rec("a", 100), rec("b", 200))); err != nil {
		t.Fatal(err)
	}
	// Окно с L = 150: сервер возвращает всё с OrderKey ≥ 150, включая уже
	// известную b и запись c той же миллисекунды, что b.
	const L = 150
	res, err := s.Ingest(ctx, "ch", afterBlock(Position{L - 1, ""}, true, rec("b", 200), rec("c", 200), rec("d", 300)))
	if err != nil {
		t.Fatalf("блок окна отвергнут: %v", err)
	}
	if res.Segments != 1 {
		t.Fatalf("окно не слилось с верхним сегментом: сегментов %d", res.Segments)
	}
	if n, _ := s.Count(ctx, "ch"); n != 4 {
		t.Fatalf("записей %d, ожидалось 4 (b не задвоилась)", n)
	}

	// Водяной знак вместо нижней границы — громкий отказ, а не тихая
	// склейка.
	_, err = s.Ingest(ctx, "ch", afterBlock(Position{300, "d"}, true, rec("c", 200)))
	if !errors.Is(err, ErrRecordCrossesAnchor) {
		t.Fatalf("якорь-водяной знак с записями окна: err = %v, ожидался ErrRecordCrossesAnchor", err)
	}
}

// Инвариант 3.11 (ADR-095 п. 2.8): channel_id с 0x00 отвергается каждой
// операцией, а не только записью.
func TestChannelWithNulRejectedEverywhere(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	const ch = "a\x00b"
	checks := map[string]error{}
	checks["Put"] = s.Put(ctx, ch, []Record{rec("x", 1)})
	_, checks["IngestWith"] = s.IngestWith(ctx, ch, newestBlock(rec("x", 1)), IngestOptions{})
	_, checks["Page"] = s.Page(ctx, ch, 0, "", 10)
	_, checks["Count"] = s.Count(ctx, ch)
	_, checks["Segments"] = s.Segments(ctx, ch)
	_, _, checks["GapAnchor"] = s.GapAnchor(ctx, ch)
	_, _, checks["Watermark"] = s.Watermark(ctx, ch)
	_, _, checks["Cursor"] = s.Cursor(ctx, ch)
	for op, err := range checks {
		if !errors.Is(err, ErrInvalidChannelID) {
			t.Fatalf("%s с 0x00 в channel_id: %v", op, err)
		}
	}
	if cat, _ := s.Catalog(ctx); len(cat) != 0 {
		t.Fatalf("канал с 0x00 попал в каталог: %v", cat)
	}
}
