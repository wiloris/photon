package diag

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

/*
 * Раскладка горячего пути put по шагам (решение владельца 2026-10-23:
 * найти причину p99 на Redmi). Только при PHOTON_DIAG_DECOMPOSE=1; отдельно
 * от контрольных замеров (TestMeasureRecordLatency) — профиль не должен
 * искажать их.
 *
 * Режимы (каждый — с номером, как настоящий put):
 *   atomic   — только r.last.Add(1);
 *   lock     — + Lock/Unlock мьютекса своего слота, без записи;
 *   copy     — + запись события в слот, БЕЗ мьютекса (гонка намеренна: этот
 *              режим не запускается под -race и только меряет цену записи);
 *   put      — рабочий put целиком;
 *   before / padslot / padctr — эталоны раскладки (см. ниже).
 * Ёмкость кольца — 64, 1024 (по умолчанию), 8192: влияние объёма памяти.
 */

// Эталоны раскладки (slotring.go, «Раскладка памяти»): рабочее кольцо уже
// с выровненными слотами и счётчиком на своей линии; ниже — прежняя
// раскладка и каждая правка по отдельности, чтобы вклад каждой
// воспроизводился замером.
//
//   before  — прежняя: слот 272 Б (slotBody), счётчик сразу за заголовком slots;
//   padslot — только выровненные слоты, счётчик — как в before;
//   padctr  — только счётчик на своей линии, слоты — 272 Б.

type beforeRing struct {
	slots []slotBody
	last  atomic.Uint64
	_     [56]byte
}

type padSlotRing struct {
	slots []slot
	last  atomic.Uint64
	_     [56]byte
}

type padCtrRing struct {
	_     [64]byte
	last  atomic.Uint64
	_     [56]byte
	slots []slotBody
}

func putInto(mu *sync.Mutex, e *Event, seq uint64, mono int64, c Component, k Kind, conn Conn, fields []Field) {
	mu.Lock()
	if e.Seq < seq {
		e.Seq, e.AtMonoMS = seq, mono
		e.Component, e.Kind, e.Conn = c, k, conn
		e.N = uint8(copy(e.Fields[:], fields))
	}
	mu.Unlock()
}

func (r *beforeRing) put(mono int64, c Component, k Kind, conn Conn, fields []Field) {
	seq := r.last.Add(1)
	s := &r.slots[(seq-1)%uint64(len(r.slots))]
	putInto(&s.mu, &s.ev, seq, mono, c, k, conn, fields)
}

func (r *padSlotRing) put(mono int64, c Component, k Kind, conn Conn, fields []Field) {
	seq := r.last.Add(1)
	s := &r.slots[(seq-1)%uint64(len(r.slots))]
	putInto(&s.mu, &s.ev, seq, mono, c, k, conn, fields)
}

func (r *padCtrRing) put(mono int64, c Component, k Kind, conn Conn, fields []Field) {
	seq := r.last.Add(1)
	s := &r.slots[(seq-1)%uint64(len(r.slots))]
	putInto(&s.mu, &s.ev, seq, mono, c, k, conn, fields)
}

func TestDecomposePut(t *testing.T) {
	if os.Getenv("PHOTON_DIAG_DECOMPOSE") != "1" {
		t.Skip("раскладка — только при PHOTON_DIAG_DECOMPOSE=1")
	}
	per, repeats := 200_000, 3
	fmt.Sscan(os.Getenv("PHOTON_DIAG_DECOMPOSE_PER"), &per)
	tpn := ticksPerNS()
	ns := func(v int64) float64 { return float64(v) / tpn }
	fields := []Field{Str(KeyTx, "update"), Num(KeyDurUS, 42)}
	var out strings.Builder
	fmt.Fprintf(&out, "# sizeof before-slot %d, slot %d; tsc ticks/ns %.4f; per %d; repeats %d\n",
		unsafe.Sizeof(slotBody{}), unsafe.Sizeof(slot{}), tpn, per, repeats)
	fmt.Fprintf(&out, "# mode capacity writers repeat p50_ns p99_ns p99.9_ns\n")
	modes := []string{"atomic", "lock", "copy", "put", "before", "padslot", "padctr"}
	if m := os.Getenv("PHOTON_DIAG_DECOMPOSE_MODES"); m != "" {
		modes = strings.Split(m, ",")
	}
	for _, mode := range modes {
		for _, capacity := range []int{64, 1024, 8192} {
			for _, writers := range []int{1, 8} {
				var p99s []float64
				for rep := 1; rep <= repeats; rep++ {
					r := newSlotRing(capacity)
					br := &beforeRing{slots: make([]slotBody, capacity)}
					ps := &padSlotRing{slots: make([]slot, capacity)}
					pc := &padCtrRing{slots: make([]slotBody, capacity)}
					var op func()
					switch mode {
					case "atomic":
						op = func() { r.last.Add(1) }
					case "lock":
						op = func() {
							s := r.slotOf(r.last.Add(1))
							s.mu.Lock()
							s.mu.Unlock()
						}
					case "copy":
						op = func() {
							seq := r.last.Add(1)
							e := &r.slotOf(seq).ev
							e.Seq, e.AtMonoMS = seq, 1
							e.Component, e.Kind, e.Conn = CompStorage, KindTxFailed, ConnNone
							e.N = uint8(copy(e.Fields[:], fields))
						}
					case "put":
						op = func() { r.put(1, CompStorage, KindTxFailed, ConnNone, fields) }
					case "before":
						op = func() { br.put(1, CompStorage, KindTxFailed, ConnNone, fields) }
					case "padslot":
						op = func() { ps.put(1, CompStorage, KindTxFailed, ConnNone, fields) }
					case "padctr":
						op = func() { pc.put(1, CompStorage, KindTxFailed, ConnNone, fields) }
					default:
						t.Fatalf("режим %q", mode)
					}
					all := make([]int64, 0, writers*per)
					var mu sync.Mutex
					var wg sync.WaitGroup
					start := make(chan struct{})
					for w := 0; w < writers; w++ {
						wg.Add(1)
						go func() {
							defer wg.Done()
							s := make([]int64, per)
							<-start
							for i := range s {
								c0 := cputicks()
								op()
								s[i] = cputicks() - c0
							}
							mu.Lock()
							all = append(all, s...)
							mu.Unlock()
						}()
					}
					close(start)
					wg.Wait()
					sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
					p99 := ns(all[len(all)*99/100])
					p99s = append(p99s, p99)
					fmt.Fprintf(&out, "%s %d %d %d %.0f %.0f %.0f\n", mode, capacity, writers, rep,
						ns(all[len(all)/2]), p99, ns(all[len(all)*999/1000]))
				}
				sort.Float64s(p99s)
				t.Logf("%-7s ёмкость %-5d писателей %d: медиана p99 %.0f нс", mode, capacity, writers, p99s[len(p99s)/2])
			}
		}
	}
	if p := os.Getenv("PHOTON_DIAG_MEASURE_OUT"); p != "" {
		os.WriteFile(p, []byte(out.String()), 0o644)
	}
}

// BenchmarkPutSlotRing / BenchmarkPutBefore — пара для счётчиков процессора
// (simpleperf stat) и pprof: рабочий put против прежней раскладки, ёмкость
// по умолчанию.
func BenchmarkPutSlotRing(b *testing.B) {
	r := newSlotRing(DefaultCapacity)
	fields := []Field{Str(KeyTx, "update"), Num(KeyDurUS, 42)}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.put(1, CompStorage, KindTxFailed, ConnNone, fields)
		}
	})
}

func BenchmarkPutBefore(b *testing.B) {
	r := &beforeRing{slots: make([]slotBody, DefaultCapacity)}
	fields := []Field{Str(KeyTx, "update"), Num(KeyDurUS, 42)}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.put(1, CompStorage, KindTxFailed, ConnNone, fields)
		}
	})
}
