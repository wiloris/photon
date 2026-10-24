package localfeed

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon/photon-core/adapters/bbolt"
	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-core/services/outbox"
)

// Store.Clear — ADR-112, контракт §5b.6. Настоящий bbolt: ошибка записи
// посреди транзакции проверяется на откате адаптера, а не на предположении.

// failDeleteStorage — обёртка над настоящим хранилищем: N-й вызов Delete
// внутри Update возвращает ошибку.
type failDeleteStorage struct {
	ports.Storage
	failOn int // 1-based; 0 — не ломать
	calls  int
}

func (f *failDeleteStorage) Update(ctx context.Context, fn func(ports.WriteTx) error) error {
	f.calls = 0
	return f.Storage.Update(ctx, func(tx ports.WriteTx) error {
		return fn(&failDeleteTx{WriteTx: tx, f: f})
	})
}

type failDeleteTx struct {
	ports.WriteTx
	f *failDeleteStorage
}

func (t *failDeleteTx) Delete(ns string, k []byte) error {
	t.f.calls++
	if t.f.failOn != 0 && t.f.calls == t.f.failOn {
		return errors.New("injected delete failure")
	}
	return t.WriteTx.Delete(ns, k)
}

func clearMigrations() []bboltadapter.Migration {
	return []bboltadapter.Migration{
		{Version: SchemaVersion, Up: func(tx *bolt.Tx) error {
			_, e := tx.CreateBucketIfNotExists([]byte(Namespace))
			return e
		}},
		{Version: 2, Up: func(tx *bolt.Tx) error {
			_, e := tx.CreateBucketIfNotExists([]byte(outbox.Namespace))
			return e
		}},
	}
}

type clearEnv struct {
	feed *Store
	box  *outbox.Store
	st   *failDeleteStorage
}

func newClearEnv(t *testing.T) clearEnv {
	t.Helper()
	real, err := bboltadapter.New(filepath.Join(t.TempDir(), "f.db"), clearMigrations())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { real.Close(context.Background()) })
	st := &failDeleteStorage{Storage: real}
	feed := New(st, nil)
	if err := feed.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if err := st.Migrate(context.Background(), 2); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return clearEnv{feed: feed, box: outbox.New(st), st: st}
}

// fill — канал с записями, сегментом, курсором и каталогом.
func (e clearEnv) fill(t *testing.T, ch string) {
	t.Helper()
	ctx := context.Background()
	if _, err := e.feed.IngestWith(ctx, ch, newestBlock(rec("a", 100), rec("b", 200), rec("c", 300)),
		IngestOptions{Cursor: []byte("cur-" + ch)}); err != nil {
		t.Fatalf("IngestWith(%s): %v", ch, err)
	}
}

type channelState struct {
	records  int
	segments int
	cursor   bool
	catalog  bool
}

func (e clearEnv) state(t *testing.T, ch string) channelState {
	t.Helper()
	ctx := context.Background()
	var s channelState
	pg, err := e.feed.Page(ctx, ch, 0, "", 100)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	s.records = len(pg.Records)
	segs, err := e.feed.Segments(ctx, ch)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	s.segments = len(segs)
	_, s.cursor, err = e.feed.Cursor(ctx, ch)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	cat, err := e.feed.Catalog(ctx)
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	for _, c := range cat {
		if c.ChannelID == ch {
			s.catalog = true
		}
	}
	return s
}

func TestClear_RemovesEverythingOfTheChannel(t *testing.T) {
	e := newClearEnv(t)
	ctx := context.Background()
	e.fill(t, "grp:a:c1")

	if got := e.state(t, "grp:a:c1"); got.records != 3 || got.segments == 0 || !got.cursor || !got.catalog {
		t.Fatalf("предусловие: %+v", got)
	}
	n, err := e.feed.Clear(ctx, "grp:a:c1")
	if err != nil || n != 3 {
		t.Fatalf("Clear = %d, %v; ожидалось 3, nil", n, err)
	}
	if got := e.state(t, "grp:a:c1"); got != (channelState{}) {
		t.Fatalf("после Clear осталось: %+v", got)
	}
	if _, found, err := e.feed.GapAnchor(ctx, "grp:a:c1"); err != nil || found {
		t.Fatalf("GapAnchor после Clear: found=%v err=%v", found, err)
	}
}

func TestClear_RepeatRemovesZero(t *testing.T) {
	e := newClearEnv(t)
	e.fill(t, "ch")
	if _, err := e.feed.Clear(context.Background(), "ch"); err != nil {
		t.Fatal(err)
	}
	n, err := e.feed.Clear(context.Background(), "ch")
	if err != nil || n != 0 {
		t.Fatalf("повтор: %d, %v; ожидалось 0, nil", n, err)
	}
	// Канал, которого не было вовсе.
	if n, err := e.feed.Clear(context.Background(), "never"); err != nil || n != 0 {
		t.Fatalf("несуществующий канал: %d, %v", n, err)
	}
}

// Соседний канал с общим префиксом имени (c1 / c10) не затрагивается.
func TestClear_NeighbourChannelsIntact(t *testing.T) {
	e := newClearEnv(t)
	for _, ch := range []string{"grp:a:c1", "grp:a:c10", "grp:a:c", "other"} {
		e.fill(t, ch)
	}
	if _, err := e.feed.Clear(context.Background(), "grp:a:c1"); err != nil {
		t.Fatal(err)
	}
	if got := e.state(t, "grp:a:c1"); got != (channelState{}) {
		t.Fatalf("очищаемый канал: %+v", got)
	}
	for _, ch := range []string{"grp:a:c10", "grp:a:c", "other"} {
		if got := e.state(t, ch); got.records != 3 || got.segments == 0 || !got.cursor || !got.catalog {
			t.Fatalf("канал %s задет: %+v", ch, got)
		}
	}
}

// Outbox — другой namespace; очередь с тем же именем, что у канала, цела.
func TestClear_OutboxUntouched(t *testing.T) {
	e := newClearEnv(t)
	ctx := context.Background()
	e.fill(t, "ch")
	op, err := e.box.Enqueue(ctx, "ch", []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := e.feed.Clear(ctx, "ch"); err != nil {
		t.Fatal(err)
	}
	ops, err := e.box.List(ctx, "ch")
	if err != nil || len(ops) != 1 || ops[0].OpID != op.OpID {
		t.Fatalf("очередь после Clear: %v, err=%v", ops, err)
	}
}

// Ошибка Delete на КАЖДОМ шаге оставляет всё как было: одна транзакция.
func TestClear_WriteErrorMidwayLeavesEverything(t *testing.T) {
	e := newClearEnv(t)
	ctx := context.Background()
	e.fill(t, "ch")
	want := e.state(t, "ch")
	// 3 записи + сегменты + курсор + каталог = 6 вызовов Delete.
	for failOn := 1; failOn <= 6; failOn++ {
		e.st.failOn = failOn
		n, err := e.feed.Clear(ctx, "ch")
		e.st.failOn = 0
		if err == nil || n != 0 {
			t.Fatalf("failOn=%d: Clear = %d, %v; ожидалась ошибка", failOn, n, err)
		}
		if got := e.state(t, "ch"); got != want {
			t.Fatalf("failOn=%d: состояние изменилось: %+v, было %+v", failOn, got, want)
		}
	}
	if n, err := e.feed.Clear(ctx, "ch"); err != nil || n != 3 {
		t.Fatalf("Clear после сбоев = %d, %v", n, err)
	}
}

func TestClear_InvalidChannel(t *testing.T) {
	e := newClearEnv(t)
	if _, err := e.feed.Clear(context.Background(), ""); !errors.Is(err, ErrEmptyChannel) {
		t.Fatalf("пустой: %v", err)
	}
	if _, err := e.feed.Clear(context.Background(), "a\x00b"); !errors.Is(err, ErrInvalidChannelID) {
		t.Fatalf("с NUL: %v", err)
	}
}
