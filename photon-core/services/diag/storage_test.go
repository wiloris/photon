package diag

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	"github.com/wiloris/photon-core/ports"
)

// Инварианты декоратора (ADR-130 §2.6) — на НАСТОЯЩЕМ bbolt во временном
// каталоге, каждый сценарий дважды: на голом адаптере и под декоратором.

const ns = "t"

func openBolt(t testing.TB) *bboltadapter.Storage {
	t.Helper()
	st, err := bboltadapter.New(filepath.Join(t.TempDir(), "d.db"), []bboltadapter.Migration{{
		Version: 1,
		Up: func(tx *bolt.Tx) error {
			_, e := tx.CreateBucketIfNotExists([]byte(ns))
			return e
		},
	}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := st.Migrate(context.Background(), 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(func() { st.Close(context.Background()) })
	return st
}

func boltStats(st *bboltadapter.Storage) func() BoltStats {
	return func() BoltStats { b := st.Stats(); return BoltStats{FileSize: b.FileSize, Writes: b.Writes} }
}

// pair — голый адаптер и декорированный, на разных файлах.
type pair struct {
	raw, dec ports.Storage
	d        *Diag
}

func newPair(t *testing.T) pair {
	t.Helper()
	d := newTestDiag(t, 64)
	b := openBolt(t)
	return pair{raw: openBolt(t), dec: WrapStorage(b, d, boltStats(b)), d: d}
}

func (p pair) both(t *testing.T, f func(name string, st ports.Storage)) {
	t.Helper()
	f("raw", p.raw)
	f("decorated", p.dec)
}

type customErr struct{ msg string }

func (e *customErr) Error() string { return e.msg }

// Ошибка не меняется: тот же экземпляр (==), та же цепочка Is/As — для
// ошибки fn, sentinel порта, ctx до старта.
func TestStorageErrorsUnchanged(t *testing.T) {
	p := newPair(t)
	sentinel := errors.New("fn failed")
	custom := &customErr{"custom"}
	wrapped := fmt.Errorf("wrap: %w", ports.ErrNotFound)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	p.both(t, func(name string, st ports.Storage) {
		ctx := context.Background()
		if err := st.Update(ctx, func(ports.WriteTx) error { return sentinel }); err != sentinel {
			t.Errorf("%s Update: err = %v, want тот же sentinel", name, err)
		}
		if err := st.View(ctx, func(ports.ReadTx) error { return sentinel }); err != sentinel {
			t.Errorf("%s View: err = %v, want тот же sentinel", name, err)
		}
		err := st.Update(ctx, func(ports.WriteTx) error { return custom })
		var ce *customErr
		if !errors.As(err, &ce) || ce != custom {
			t.Errorf("%s: errors.As не нашёл тот же *customErr: %v", name, err)
		}
		err = st.View(ctx, func(tx ports.ReadTx) error {
			_, e := tx.Get(ns, []byte("absent"))
			return e
		})
		if err != ports.ErrNotFound {
			t.Errorf("%s Get: err = %v, want ports.ErrNotFound (==)", name, err)
		}
		if err := st.Update(ctx, func(ports.WriteTx) error { return wrapped }); err != wrapped || !errors.Is(err, ports.ErrNotFound) {
			t.Errorf("%s wrapped: err = %v", name, err)
		}
		if err := st.Update(canceled, func(ports.WriteTx) error { return nil }); err != context.Canceled {
			t.Errorf("%s canceled Update: err = %v, want context.Canceled (==)", name, err)
		}
		if err := st.View(canceled, func(ports.ReadTx) error { return nil }); err != context.Canceled {
			t.Errorf("%s canceled View: err = %v", name, err)
		}
	})
}

// failingCommit — хранилище, у которого фиксация падает: fn вызывается, её
// успех заменяется ошибкой commit. Настоящий bbolt такой сбой (fsync,
// нехватка места) в unit-тесте не воспроизводит; ветку декоратора
// проверяет эта подстановка.
type failingCommit struct {
	ports.Storage
	err   error
	calls int
}

func (f *failingCommit) Update(ctx context.Context, fn func(ports.WriteTx) error) error {
	f.calls++
	if err := fn(nil); err != nil {
		return err
	}
	return f.err
}

// fn вызывается ровно столько раз, сколько вызвал бы адаптер: успех,
// ошибка fn, отмена до старта, сбой фиксации.
func TestStorageFnCalledOnce(t *testing.T) {
	p := newPair(t)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	counts := map[string][4]int{}
	p.both(t, func(name string, st ports.Storage) {
		var c [4]int
		st.Update(context.Background(), func(tx ports.WriteTx) error { c[0]++; return tx.Put(ns, []byte("k"), []byte("v")) })
		st.Update(context.Background(), func(ports.WriteTx) error { c[1]++; return errors.New("x") })
		st.Update(canceled, func(ports.WriteTx) error { c[2]++; return nil })
		st.View(context.Background(), func(ports.ReadTx) error { c[3]++; return nil })
		counts[name] = c
	})
	if counts["raw"] != counts["decorated"] || counts["raw"] != [4]int{1, 1, 0, 1} {
		t.Fatalf("входов в fn: raw %v, decorated %v, want [1 1 0 1] у обоих", counts["raw"], counts["decorated"])
	}

	commitErr := errors.New("commit failed")
	inner := &failingCommit{err: commitErr}
	dec := WrapStorage(inner, newTestDiag(t, 64), nil)
	n := 0
	if err := dec.Update(context.Background(), func(ports.WriteTx) error { n++; return nil }); err != commitErr {
		t.Fatalf("сбой фиксации: err = %v, want тот же commitErr", err)
	}
	if n != 1 || inner.calls != 1 {
		t.Fatalf("сбой фиксации: fn вызвана %d раз, Update адаптера — %d, want 1/1", n, inner.calls)
	}
}

// Исход тот же: ошибка fn — данных нет; успех — виден следующему View;
// паника проходит наружу и транзакция откатывается.
func TestStorageOutcomeSame(t *testing.T) {
	p := newPair(t)
	p.both(t, func(name string, st ports.Storage) {
		ctx := context.Background()
		st.Update(ctx, func(tx ports.WriteTx) error {
			tx.Put(ns, []byte("rolled"), []byte("x"))
			return errors.New("abort")
		})
		st.Update(ctx, func(tx ports.WriteTx) error { return tx.Put(ns, []byte("kept"), []byte("y")) })

		func() {
			defer func() {
				if r := recover(); r != "boom" {
					t.Errorf("%s: recover = %v, want boom — паника перехвачена или подменена", name, r)
				}
			}()
			st.Update(ctx, func(tx ports.WriteTx) error {
				tx.Put(ns, []byte("panicked"), []byte("z"))
				panic("boom")
			})
		}()

		got := map[string]string{}
		st.View(ctx, func(tx ports.ReadTx) error {
			return tx.ForEach(ns, func(k, v []byte) error { got[string(k)] = string(v); return nil })
		})
		if len(got) != 1 || got["kept"] != "y" {
			t.Errorf("%s: содержимое %v, want только kept=y", name, got)
		}
	})
}

func contents(t *testing.T, st ports.Storage) []string {
	t.Helper()
	var out []string
	st.View(context.Background(), func(tx ports.ReadTx) error {
		return tx.ForEach(ns, func(k, v []byte) error { out = append(out, string(k)+"="+string(v)); return nil })
	})
	sort.Strings(out)
	return out
}

// Порядок и видимость: одинаковый сценарий из нескольких горутин даёт
// одинаковое содержимое базы с декоратором и без.
func TestStorageOrderVisibility(t *testing.T) {
	p := newPair(t)
	run := func(st ports.Storage) {
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					k := []byte(fmt.Sprintf("g%d-k%d", g, i%10))
					st.Update(context.Background(), func(tx ports.WriteTx) error {
						if i%7 == 6 {
							return tx.Delete(ns, k)
						}
						return tx.Put(ns, k, []byte(fmt.Sprint(i)))
					})
					st.View(context.Background(), func(tx ports.ReadTx) error { _, e := tx.Get(ns, k); _ = e; return nil })
				}
			}(g)
		}
		wg.Wait()
	}
	run(p.raw)
	run(p.dec)
	a, b := contents(t, p.raw), contents(t, p.dec)
	if strings.Join(a, ",") != strings.Join(b, ",") || len(a) == 0 {
		t.Fatalf("содержимое разошлось:\nraw %v\ndec %v", a, b)
	}
}

// Сбой диагностики не трогает транзакцию: переполненный журнал даёт тот же
// исход; выключенная диагностика — вовсе без декоратора.
func TestStorageDiagFailureIsolated(t *testing.T) {
	d := newTestDiag(t, 64)
	recordN(d, 10_000)
	b := openBolt(t)
	st := WrapStorage(b, d, nil)
	fnErr := errors.New("x")
	if err := st.Update(context.Background(), func(ports.WriteTx) error { return fnErr }); err != fnErr {
		t.Fatalf("переполненный журнал: err = %v", err)
	}
	if err := st.Update(context.Background(), func(tx ports.WriteTx) error { return tx.Put(ns, []byte("a"), []byte("1")) }); err != nil {
		t.Fatalf("переполненный журнал: %v", err)
	}
	if got := contents(t, st); len(got) != 1 {
		t.Fatalf("содержимое = %v", got)
	}

	if WrapStorage(b, nil, nil) != ports.Storage(b) {
		t.Fatal("выключенная диагностика обернула хранилище — поведение уже не «как без неё» по построению")
	}
}

func events(t *testing.T, d *Diag, k Kind) []Event {
	t.Helper()
	p, err := d.Read(0, MaxReadLimit)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	var out []Event
	for _, e := range p.Events {
		if e.Kind == k {
			out = append(out, e)
		}
	}
	return out
}

func fieldStr(e Event, k Key) string { f, _ := e.Get(k); return f.Str }
func fieldNum(e Event, k Key) int64  { f, _ := e.Get(k); return f.Num }

// Отказ транзакции — событие с классом без текста ошибки: текст fn
// принадлежит приложению (здесь — «секрет» и путь), в журнал он не попадает.
func TestStorageFailedEvents(t *testing.T) {
	d := newTestDiag(t, 64)
	b := openBolt(t)
	st := WrapStorage(b, d, nil)
	ctx := context.Background()
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	secret := errors.New("token=SECRET-123 /home/user/private.db")
	st.Update(ctx, func(ports.WriteTx) error { return secret })
	st.View(ctx, func(tx ports.ReadTx) error { _, e := tx.Get(ns, []byte("absent")); return e })
	st.Update(canceled, func(ports.WriteTx) error { return nil })
	WrapStorage(&failingCommit{Storage: b, err: errors.New("disk full at /home/user")}, d, nil).
		Update(ctx, func(ports.WriteTx) error { return nil })

	got := events(t, d, KindTxFailed)
	want := []struct {
		tx, class       string
		started, rolled int64
	}{
		{"update", "fn", 1, 1},
		{"view", "fn:not_found", 1, 0},
		{"update", "not_started:canceled", 0, 0},
		{"update", "commit", 1, 1},
	}
	if len(got) != len(want) {
		t.Fatalf("tx_failed: %d событий, want %d", len(got), len(want))
	}
	for i, w := range want {
		e := got[i]
		if fieldStr(e, KeyTx) != w.tx || fieldStr(e, KeyClass) != w.class ||
			fieldNum(e, KeyStarted) != w.started || fieldNum(e, KeyRolledBack) != w.rolled {
			t.Errorf("событие %d: tx=%q class=%q started=%d rolled=%d, want %+v", i,
				fieldStr(e, KeyTx), fieldStr(e, KeyClass), fieldNum(e, KeyStarted), fieldNum(e, KeyRolledBack), w)
		}
		if e.Component != CompStorage {
			t.Errorf("событие %d: компонент %v", i, e.Component)
		}
	}
	assertNoLeak(t, d, "SECRET", "/home/user", "private.db", "disk full")
}

// assertNoLeak — ни одно строковое поле журнала и снимка не содержит
// запрещённого (ADR-130 §2.8).
func assertNoLeak(t *testing.T, d *Diag, forbidden ...string) {
	t.Helper()
	p, _ := d.Read(0, MaxReadLimit)
	for _, e := range p.Events {
		for i := 0; i < int(e.N); i++ {
			for _, f := range forbidden {
				if strings.Contains(e.Fields[i].Str, f) {
					t.Fatalf("seq %d поле %s содержит %q: %q", e.Seq, e.Fields[i].Key, f, e.Fields[i].Str)
				}
			}
		}
	}
	snap := fmt.Sprintf("%+v", d.Snapshot())
	for _, f := range forbidden {
		if strings.Contains(snap, f) {
			t.Fatalf("снимок содержит %q", f)
		}
	}
}

// Три измерения раздельно: ожидание писательской блокировки, выполнение,
// фиксация. Вторая транзакция ждёт первую, которая держит блокировку
// ~60 мс: её ожидание большое, выполнение маленькое.
func TestStorageThreeMeasurements(t *testing.T) {
	d, err := New(Config{Capacity: 64, SlowTx: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	b := openBolt(t)
	st := WrapStorage(b, d, nil)
	ctx := context.Background()

	inFirst := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		st.Update(ctx, func(tx ports.WriteTx) error {
			close(inFirst)
			time.Sleep(60 * time.Millisecond)
			return tx.Put(ns, []byte("a"), []byte("1"))
		})
	}()
	<-inFirst
	st.Update(ctx, func(tx ports.WriteTx) error { return tx.Put(ns, []byte("b"), []byte("2")) })
	wg.Wait()
	st.View(ctx, func(ports.ReadTx) error { time.Sleep(25 * time.Millisecond); return nil })

	slow := events(t, d, KindTxSlow)
	if len(slow) != 3 {
		t.Fatalf("tx_slow: %d событий, want 3", len(slow))
	}
	var holder, waiter, view Event
	for _, e := range slow {
		switch {
		case fieldStr(e, KeyTx) == "view":
			view = e
		case fieldNum(e, KeyExecUS) >= 50_000:
			holder = e
		default:
			waiter = e
		}
	}
	if fieldNum(holder, KeyWaitUS) > 20_000 {
		t.Errorf("держатель блокировки: wait_us = %d — не должен был ждать", fieldNum(holder, KeyWaitUS))
	}
	if fieldNum(waiter, KeyWaitUS) < 30_000 || fieldNum(waiter, KeyExecUS) > 20_000 {
		t.Errorf("ждущий: wait_us = %d (want ≥ 30 мс), exec_us = %d (want мало)",
			fieldNum(waiter, KeyWaitUS), fieldNum(waiter, KeyExecUS))
	}
	if _, ok := waiter.Get(KeyCommitUS); !ok {
		t.Error("у Update нет commit_us")
	}
	if _, ok := view.Get(KeyCommitUS); ok || fieldNum(view, KeyExecUS) < 20_000 {
		t.Errorf("View: exec_us = %d, commit_us не должно быть", fieldNum(view, KeyExecUS))
	}

	s := d.Snapshot().Storage
	if s.UpdateTx != 2 || s.ViewTx != 1 || s.SlowTx != 3 || s.UpdateWait.N != 2 || s.UpdateCommit.N != 2 || s.ViewExec.N != 1 {
		t.Fatalf("снимок хранилища: %+v", s)
	}
}

// Снимок: счётчики, версия схемы после Migrate, статистика файла у
// открытого и её отсутствие у закрытого; события migrate и close.
func TestStorageSnapshotAndLifecycle(t *testing.T) {
	d := newTestDiag(t, 64)
	b, err := bboltadapter.New(filepath.Join(t.TempDir(), "s.db"), []bboltadapter.Migration{
		{Version: 1, Up: func(tx *bolt.Tx) error { _, e := tx.CreateBucketIfNotExists([]byte(ns)); return e }},
		{Version: 2, Up: func(tx *bolt.Tx) error { return nil }},
	})
	if err != nil {
		t.Fatal(err)
	}
	st := WrapStorage(b, d, boltStats(b))
	ctx := context.Background()
	if err := st.Migrate(ctx, 2); err != nil {
		t.Fatal(err)
	}
	st.Migrate(ctx, 1) // идемпотентно, версия не уменьшается
	st.Update(ctx, func(tx ports.WriteTx) error { return tx.Put(ns, []byte("k"), []byte("v")) })
	st.Update(ctx, func(ports.WriteTx) error { return errors.New("x") })

	s := d.Snapshot().Storage
	if !s.Open || s.SchemaVersion != 2 || s.UpdateTx != 2 || s.UpdateErrors != 1 || s.Rollbacks != 1 {
		t.Fatalf("снимок: %+v", s)
	}
	if s.Bolt == nil || s.Bolt.FileSize <= 0 || s.Bolt.Writes <= 0 {
		t.Fatalf("статистика bbolt: %+v", s.Bolt)
	}
	if m := events(t, d, KindStorageMigrate); len(m) != 2 || fieldNum(m[0], KeyTo) != 2 || fieldStr(m[0], KeyClass) != "ok" {
		t.Fatalf("migrate: %+v", m)
	}

	if err := st.Close(ctx); err != nil {
		t.Fatal(err)
	}
	s = d.Snapshot().Storage
	if s.Open || s.Bolt != nil {
		t.Fatalf("после Close: open=%v bolt=%v", s.Open, s.Bolt)
	}
	if c := events(t, d, KindStorageClose); len(c) != 1 || fieldStr(c[0], KeyClass) != "ok" {
		t.Fatalf("close: %+v", c)
	}
}

// Полнота схемы на уровне пакета: каждое событие ХРАНИЛИЩА порождается своим
// сценарием здесь. События сессии, диспетчера, подписки и транспорта
// порождает инстанс — их полнота проверяется в services/dispatcher
// (TestDiagEveryKindEmitted); Autodial не подключён к рабочему пути.
func TestEveryStorageKindEmitted(t *testing.T) {
	pending := map[Kind]string{
		KindSessionState: "dispatcher", KindSessionFailed: "dispatcher", KindSessionClosed: "dispatcher",
		KindConnStart: "dispatcher", KindConnResult: "dispatcher", KindSubBackoff: "dispatcher",
		KindCallFailed: "dispatcher", KindCallSlow: "dispatcher", KindEventSent: "dispatcher",
		KindDial: "dispatcher", KindConnClosed: "dispatcher",
		KindRouteSelected: "Autodial не подключён", KindRouteAttempt: "Autodial не подключён",
	}
	d, _ := New(Config{Capacity: 256, SlowTx: time.Nanosecond})
	b := openBolt(t)
	d.StorageOpened(d.Mono(), "ok")
	st := WrapStorage(b, d, nil)
	ctx := context.Background()
	st.Migrate(ctx, 1)
	st.Update(ctx, func(ports.WriteTx) error { return errors.New("x") })
	st.Close(ctx)

	p, _ := d.Read(0, MaxReadLimit)
	got := map[Kind]bool{}
	for _, e := range p.Events {
		got[e.Kind] = true
	}
	for k := Kind(1); k < kindEnd; k++ {
		_, isPending := pending[k]
		if got[k] == isPending {
			t.Errorf("%s.%s: порождено=%v, в pending=%v", k.Component(), k, got[k], isPending)
		}
	}
}
