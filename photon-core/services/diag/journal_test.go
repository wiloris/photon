package diag

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

func newTestDiag(t testing.TB, capacity int) *Diag {
	t.Helper()
	d, err := New(Config{Capacity: capacity})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d
}

func recordN(d *Diag, n int) {
	for i := 0; i < n; i++ {
		d.Record(CompStorage, KindTxFailed, ConnNone, Num(KeyDurUS, int64(i)))
	}
}

// seq растёт с 1 без пропусков, порядок чтения — порядок записи, время по
// монотонным часам не убывает.
func TestJournalSeqMonotonic(t *testing.T) {
	d := newTestDiag(t, 64)
	recordN(d, 10)
	p, err := d.Read(0, 512)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(p.Events) != 10 || p.NextSeq != 10 || p.Dropped != 0 {
		t.Fatalf("events=%d next=%d dropped=%d, want 10/10/0", len(p.Events), p.NextSeq, p.Dropped)
	}
	for i, e := range p.Events {
		if e.Seq != uint64(i+1) {
			t.Fatalf("events[%d].Seq = %d, want %d", i, e.Seq, i+1)
		}
		if f, _ := e.Get(KeyDurUS); f.Num != int64(i) {
			t.Fatalf("events[%d] dur_us = %d, want %d — порядок не совпал", i, f.Num, i)
		}
		if i > 0 && e.AtMonoMS < p.Events[i-1].AtMonoMS {
			t.Fatalf("at_mono_ms убывает: %d после %d", e.AtMonoMS, p.Events[i-1].AtMonoMS)
		}
	}
	if p.InstanceID != d.InstanceID() || len(p.InstanceID) != 32 {
		t.Fatalf("instance_id = %q", p.InstanceID)
	}
}

// Переполнение: вытесняется старейшая, растёт evicted; читатель с отставшим
// курсором получает явный разрыв dropped, а не молчаливую потерю.
func TestJournalOverflowDropped(t *testing.T) {
	d := newTestDiag(t, 64)
	recordN(d, 100)
	p, err := d.Read(0, 512)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(p.Events) != 64 || p.Events[0].Seq != 37 || p.NextSeq != 100 {
		t.Fatalf("events=%d first=%d next=%d, want 64/37/100", len(p.Events), p.Events[0].Seq, p.NextSeq)
	}
	if p.Dropped != 36 {
		t.Fatalf("dropped = %d, want 36 (seq 1…36 вытеснены)", p.Dropped)
	}
	if s := d.Snapshot().Journal; s.Evicted != 36 || s.Len != 64 || s.LastSeq != 100 || s.Capacity != 64 {
		t.Fatalf("journal stats = %+v", s)
	}
	// Курсор внутри вытесненного — разрыв только на вытесненную часть.
	p, _ = d.Read(30, 5)
	if p.Dropped != 6 || p.Events[0].Seq != 37 || p.NextSeq != 41 {
		t.Fatalf("after 30: dropped=%d first=%d next=%d, want 6/37/41", p.Dropped, p.Events[0].Seq, p.NextSeq)
	}
	// Курсор в живой части — без разрыва.
	p, _ = d.Read(50, 5)
	if p.Dropped != 0 || p.Events[0].Seq != 51 {
		t.Fatalf("after 50: dropped=%d first=%d", p.Dropped, p.Events[0].Seq)
	}
}

// Продолжение чтения по next_seq проходит журнал без пропусков и повторов;
// повтор того же запроса отдаёт те же записи; конец журнала — пусто и
// next_seq = after_seq.
func TestJournalCursorContinuation(t *testing.T) {
	d := newTestDiag(t, 128)
	recordN(d, 95)
	var seen []uint64
	cur := uint64(0)
	for {
		p, err := d.Read(cur, 10)
		if err != nil {
			t.Fatalf("Read(%d): %v", cur, err)
		}
		again, _ := d.Read(cur, 10)
		if len(again.Events) != len(p.Events) || again.NextSeq != p.NextSeq {
			t.Fatalf("повтор Read(%d) дал другой ответ", cur)
		}
		if len(p.Events) == 0 {
			if p.NextSeq != cur {
				t.Fatalf("пустой ответ: next_seq = %d, want %d", p.NextSeq, cur)
			}
			break
		}
		for _, e := range p.Events {
			seen = append(seen, e.Seq)
		}
		cur = p.NextSeq
	}
	if len(seen) != 95 {
		t.Fatalf("прочитано %d, want 95", len(seen))
	}
	for i, s := range seen {
		if s != uint64(i+1) {
			t.Fatalf("seen[%d] = %d", i, s)
		}
	}
}

func TestJournalReadErrors(t *testing.T) {
	d := newTestDiag(t, 64)
	if p, err := d.Read(0, 1); err != nil || len(p.Events) != 0 || p.NextSeq != 0 {
		t.Fatalf("пустой журнал: %+v, %v", p, err)
	}
	recordN(d, 3)
	if _, err := d.Read(4, 10); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("курсор впереди журнала: err = %v", err)
	}
	if _, err := d.Read(3, 10); err != nil {
		t.Fatalf("курсор на последней записи законен: %v", err)
	}
	for _, l := range []int{0, -1, MaxReadLimit + 1} {
		if _, err := d.Read(0, l); !errors.Is(err, ErrInvalidLimit) {
			t.Fatalf("limit %d: err = %v", l, err)
		}
	}
}

func TestCapacityBounds(t *testing.T) {
	for _, c := range []int{1, MinCapacity - 1, MaxCapacity + 1, -5} {
		if _, err := New(Config{Capacity: c}); !errors.Is(err, ErrInvalidCapacity) {
			t.Errorf("capacity %d: err = %v", c, err)
		}
	}
	d, err := New(Config{})
	if err != nil || d.Snapshot().Journal.Capacity != DefaultCapacity {
		t.Fatalf("умолчание: %v", err)
	}
	a, b := newTestDiag(t, 64), newTestDiag(t, 64)
	if a.InstanceID() == b.InstanceID() {
		t.Fatal("два инстанса с одним instance_id")
	}
}

// Писатели и читатели одновременно (-race). Каждый читатель видит строго
// возрастающие seq, и прочитанное плюс dropped покрывает (0, next_seq]
// целиком: ни одна запись не пропала молча.
func TestJournalConcurrent(t *testing.T) {
	d := newTestDiag(t, 64)
	const writers, perWriter = 8, 5000
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recordN(d, perWriter)
		}()
	}
	done := make(chan struct{})
	var rwg sync.WaitGroup
	for r := 0; r < 3; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			cur, accounted := uint64(0), uint64(0)
			for {
				p, err := d.Read(cur, 7)
				if err != nil {
					t.Errorf("Read: %v", err)
					return
				}
				prev := cur
				for _, e := range p.Events {
					if e.Seq <= prev {
						t.Errorf("seq не растёт: %d после %d", e.Seq, prev)
						return
					}
					prev = e.Seq
				}
				accounted += p.Dropped + uint64(len(p.Events))
				if len(p.Events) > 0 && accounted != p.NextSeq {
					t.Errorf("прочитано+dropped = %d, next_seq = %d — молчаливая потеря", accounted, p.NextSeq)
					return
				}
				cur = p.NextSeq
				if len(p.Events) == 0 {
					select {
					case <-done:
						return
					default:
					}
				}
			}
		}()
	}
	wg.Wait()
	close(done)
	rwg.Wait()
	if s := d.Snapshot().Journal; s.LastSeq != writers*perWriter || s.Evicted != writers*perWriter-64 {
		t.Fatalf("journal = %+v", s)
	}
}

// Запись не аллоцирует — ни с числовыми, ни со строковыми полями.
func TestRecordZeroAllocs(t *testing.T) {
	d := newTestDiag(t, 64)
	class := "fn:not_found"
	allocs := testing.AllocsPerRun(10000, func() {
		d.Record(CompStorage, KindTxFailed, ConnAction,
			Str(KeyTx, "update"), Str(KeyClass, class), Bool(KeyStarted, true),
			Bool(KeyRolledBack, true), Num(KeyDurUS, 42))
	})
	if allocs != 0 {
		t.Fatalf("Record: %.1f аллокаций на запись, want 0", allocs)
	}
}

// Выключенная диагностика — nil: каждый метод безопасен и ничего не делает.
func TestNilDiag(t *testing.T) {
	var d *Diag
	d.Record(CompStorage, KindTxFailed, ConnNone, Num(KeyDurUS, 1))
	d.StorageOpened(0, "ok")
	if p, err := d.Read(0, 0); err != nil || len(p.Events) != 0 {
		t.Fatalf("nil Read: %+v, %v", p, err)
	}
	if s := d.Snapshot(); s.Config.Enabled || s.InstanceID != "" {
		t.Fatalf("nil Snapshot = %+v", s)
	}
	if d.InstanceID() != "" || d.SlowTx() != 0 || d.SlowCall() != 0 {
		t.Fatal("nil Diag вернул значения")
	}
}

// Снимок и чтение в журнал не пишут и его не меняют.
func TestReadsDoNotRecord(t *testing.T) {
	d := newTestDiag(t, 64)
	recordN(d, 3)
	for i := 0; i < 5; i++ {
		d.Snapshot()
		d.Read(0, 10)
	}
	if s := d.Snapshot().Journal; s.LastSeq != 3 {
		t.Fatalf("last_seq = %d после чтений, want 3", s.LastSeq)
	}
}

func TestStrTruncatesOnRuneBoundary(t *testing.T) {
	s := strings.Repeat("ж", 150) // 300 байт
	f := Str(KeyText, s)
	if len(f.Str) > MaxText || !utf8.ValidString(f.Str) || len(f.Str) < MaxText-1 {
		t.Fatalf("len = %d, valid = %v", len(f.Str), utf8.ValidString(f.Str))
	}
	if Str(KeyText, "короткий").Str != "короткий" {
		t.Fatal("короткая строка изменена")
	}
}

// Схема: у каждого вида — имя и компонент, имена уникальны внутри
// компонента; у каждого ключа — уникальное имя.
func TestSchemaClosed(t *testing.T) {
	seen := map[string]bool{}
	for k := Kind(1); k < kindEnd; k++ {
		if k.String() == "unknown" || k.Component() == 0 || k.Component().String() == "unknown" {
			t.Fatalf("вид %d без имени или компонента", k)
		}
		id := k.Component().String() + "." + k.String()
		if seen[id] {
			t.Fatalf("повтор вида %s", id)
		}
		seen[id] = true
	}
	keys := map[string]bool{}
	for k := Key(1); k < keyEnd; k++ {
		if k.String() == "unknown" || keys[k.String()] {
			t.Fatalf("ключ %d: имя %q пустое или повторяется", k, k.String())
		}
		keys[k.String()] = true
	}
	e := Event{Seq: 1, Component: CompStorage, Kind: KindTxSlow, Conn: ConnMedia, N: 2,
		Fields: [MaxFields]Field{Str(KeyTx, "view"), Num(KeyWaitUS, 7)}}
	w := e.Wire()
	f := w["fields"].(map[string]any)
	if w["component"] != "storage" || w["kind"] != "tx_slow" || w["conn"] != "media" || f["tx"] != "view" || f["wait_us"] != int64(7) {
		t.Fatalf("Wire = %v", w)
	}
}

// Полей больше MaxFields — лишние отбрасываются, запись не растёт.
func TestRecordExtraFieldsDropped(t *testing.T) {
	d := newTestDiag(t, 64)
	fs := make([]Field, MaxFields+3)
	for i := range fs {
		fs[i] = Num(KeyDurUS, int64(i))
	}
	d.Record(CompStorage, KindTxSlow, ConnNone, fs...)
	p, _ := d.Read(0, 1)
	if p.Events[0].N != MaxFields {
		t.Fatalf("N = %d, want %d", p.Events[0].N, MaxFields)
	}
}
