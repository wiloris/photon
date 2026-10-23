package diag

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
)

// Профиль конкуренции писателей (решение владельца 2026-10-23, направление A
// п. 1): раздельно ожидание мьютекса, удержание блокировки и работа вне
// критической секции. Тело recordAt повторено здесь с отметками RDTSC —
// встраивать отметки в рабочий код ради профиля нельзя. Расхождение копии с
// оригиналом ловит TestProfiledCopyMatchesRecordAt.

// mutexRing — прежний журнал (один мьютекс на кольцо), сохранённый как
// эталон для сравнений: профиль конкуренции, замеры «до/после» и проверка,
// что новое кольцо в последовательном режиме даёт тот же журнал.
type mutexRing struct {
	mu      sync.Mutex
	ring    []Event
	next    int
	count   int
	lastSeq uint64
	evicted uint64
}

func newMutexRing(capacity int) *mutexRing { return &mutexRing{ring: make([]Event, capacity)} }

func (r *mutexRing) put(mono int64, c Component, k Kind, conn Conn, fields []Field) {
	r.mu.Lock()
	r.putLocked(mono, c, k, conn, fields)
	r.mu.Unlock()
}

func (r *mutexRing) putLocked(mono int64, c Component, k Kind, conn Conn, fields []Field) {
	e := &r.ring[r.next]
	r.lastSeq++
	e.Seq = r.lastSeq
	e.AtMonoMS = mono
	e.Component, e.Kind, e.Conn = c, k, conn
	e.N = uint8(copy(e.Fields[:], fields))
	r.next++
	if r.next == len(r.ring) {
		r.next = 0
	}
	if r.count == len(r.ring) {
		r.evicted++
	} else {
		r.count++
	}
}

// profiledPut — put с отметками: ожидание мьютекса и удержание.
func (r *mutexRing) profiledPut(fields []Field) (wait, hold int64) {
	t1 := cputicks()
	r.mu.Lock()
	t2 := cputicks()
	r.putLocked(1, CompStorage, KindTxFailed, ConnNone, fields)
	t3 := cputicks()
	r.mu.Unlock()
	return t2 - t1, t3 - t2
}

// read — прежний алгоритм чтения (до ADR-130 направления A).
func (r *mutexRing) read(afterSeq uint64, limit int) ([]Event, uint64, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if afterSeq > r.lastSeq {
		return nil, 0, 0, ErrInvalidCursor
	}
	if r.count == 0 || afterSeq == r.lastSeq {
		return nil, afterSeq, 0, nil
	}
	oldest := r.lastSeq - uint64(r.count) + 1
	first, dropped := afterSeq+1, uint64(0)
	if first < oldest {
		dropped, first = oldest-first, oldest
	}
	n := r.lastSeq - first + 1
	if n > uint64(limit) {
		n = uint64(limit)
	}
	out := make([]Event, n)
	pos := r.next - int(r.lastSeq-first) - 1
	for pos < 0 {
		pos += len(r.ring)
	}
	for i := range out {
		out[i] = r.ring[pos]
		if pos++; pos == len(r.ring) {
			pos = 0
		}
	}
	return out, out[n-1].Seq, dropped, nil
}

// В последовательном режиме новое кольцо даёт РОВНО тот же журнал, что
// прежнее: те же записи, next_seq, dropped, ошибки — на всех курсорах и
// лимитах, до и после переполнения.
func TestSlotRingMatchesMutexRingSequential(t *testing.T) {
	a, b := newMutexRing(64), newSlotRing(64)
	for n := 1; n <= 200; n++ {
		f := []Field{Str(KeyTx, "update"), Num(KeyDurUS, int64(n))}
		a.put(int64(n), CompStorage, KindTxFailed, ConnAction, f)
		b.put(int64(n), CompStorage, KindTxFailed, ConnAction, f)
		if n%7 != 0 && n < 190 {
			continue
		}
		for _, after := range []uint64{0, 1, uint64(n / 2), uint64(n) - 1, uint64(n), uint64(n) + 1} {
			for _, limit := range []int{1, 5, 64, 512} {
				ea, na, da, erra := a.read(after, limit)
				eb, nb, db, errb := b.read(after, limit)
				if !errors.Is(erra, errb) || na != nb || da != db || len(ea) != len(eb) {
					t.Fatalf("n=%d after=%d limit=%d: mutex (%d,%d,%d,%v) slot (%d,%d,%d,%v)",
						n, after, limit, len(ea), na, da, erra, len(eb), nb, db, errb)
				}
				for i := range ea {
					if ea[i] != eb[i] {
						t.Fatalf("n=%d after=%d limit=%d: запись %d разошлась", n, after, limit, i)
					}
				}
			}
		}
	}
}

func pct(all []int64, q float64) int64 { return all[int(float64(len(all)-1)*q)] }

// TestProfileContention — профиль ПРЕЖНЕГО журнала (mutexRing), по
// которому выбрано кольцо на слотах: при 1 и 8 писателях, по 10⁶ записей, 5 повторов:
// перцентили ожидания и удержания. Только при PHOTON_DIAG_MEASURE=1;
// PHOTON_DIAG_MEASURE_OUT — файл для сырых перцентилей.
func TestProfileContention(t *testing.T) {
	if os.Getenv("PHOTON_DIAG_MEASURE") != "1" {
		t.Skip("замер — только при PHOTON_DIAG_MEASURE=1")
	}
	const per, repeats = 1_000_000, 5
	tpn := ticksPerNS()
	ns := func(v int64) float64 { return float64(v) / tpn }
	var out strings.Builder
	fmt.Fprintf(&out, "# tsc ticks/ns = %.4f; per = %d; repeats = %d\n", tpn, per, repeats)
	fmt.Fprintf(&out, "# writers repeat part p50_ns p99_ns p99.9_ns max_ns\n")
	for _, writers := range []int{1, 8} {
		for r := 1; r <= repeats; r++ {
			mr := newMutexRing(DefaultCapacity)
			waits := make([][]int64, writers)
			holds := make([][]int64, writers)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for w := 0; w < writers; w++ {
				waits[w], holds[w] = make([]int64, per), make([]int64, per)
				wg.Add(1)
				go func(ws, hs []int64) {
					defer wg.Done()
					<-start
					for i := range ws {
						ws[i], hs[i] = mr.profiledPut([]Field{Str(KeyTx, "update"), Num(KeyDurUS, int64(i))})
					}
				}(waits[w], holds[w])
			}
			close(start)
			wg.Wait()
			for _, part := range []struct {
				name string
				v    [][]int64
			}{{"wait", waits}, {"hold", holds}} {
				all := make([]int64, 0, writers*per)
				for _, s := range part.v {
					all = append(all, s...)
				}
				sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
				fmt.Fprintf(&out, "%d %d %s %.0f %.0f %.0f %.0f\n", writers, r, part.name,
					ns(pct(all, .5)), ns(pct(all, .99)), ns(pct(all, .999)), ns(all[len(all)-1]))
			}
		}
	}
	t.Log("\n" + out.String())
	if path := os.Getenv("PHOTON_DIAG_MEASURE_OUT"); path != "" {
		os.WriteFile(path, []byte(out.String()), 0o644)
	}
}
