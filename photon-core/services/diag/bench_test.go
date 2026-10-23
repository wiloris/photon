package diag

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	_ "unsafe" // go:linkname runtime.cputicks — часы измерителя

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	"github.com/wiloris/photon-core/ports"
)

// Замеры ADR-130 §5.6. Команды и результаты — IMPLEMENTATION_LOG.md.

// BenchmarkRecord — стоимость одной записи в журнал. -cpu 1,8: при 8
// писатели идут параллельно (RunParallel), то есть меряется и ожидание
// мьютекса. Аллокаций обязано быть 0 (-benchmem).
func BenchmarkRecord(b *testing.B) {
	d, err := New(Config{Capacity: DefaultCapacity})
	if err != nil {
		b.Fatal(err)
	}
	class := "fn:not_found"
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d.Record(CompStorage, KindTxFailed, ConnNone,
				Str(KeyTx, "update"), Str(KeyClass, class), Bool(KeyStarted, true),
				Bool(KeyRolledBack, true), Num(KeyDurUS, 42))
		}
	})
}

// BenchmarkTimeNow — стоимость одного чтения часов на этой машине. На
// clocksource hpet она сравнима с порогом §5.4 сама по себе.
func BenchmarkTimeNow(b *testing.B) {
	var sink time.Time
	var mu sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		var t time.Time
		for pb.Next() {
			t = time.Now()
		}
		mu.Lock()
		sink = t
		mu.Unlock()
	})
	_ = sink
}

// BenchmarkRecordMono — вставка в журнал без чтения часов (время прочитано
// заранее). Разница с BenchmarkRecord — цена часов внутри Record.
func BenchmarkRecordMono(b *testing.B) {
	d, err := New(Config{Capacity: DefaultCapacity})
	if err != nil {
		b.Fatal(err)
	}
	m := monotime()
	class := "fn:not_found"
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d.recordMono(m, CompStorage, KindTxFailed, ConnNone,
				Str(KeyTx, "update"), Str(KeyClass, class), Bool(KeyStarted, true),
				Bool(KeyRolledBack, true), Num(KeyDurUS, 42))
		}
	})
}

// BenchmarkMutexRecord — прежний журнал целиком (time.Now + один мьютекс)
// для сравнения «до/после».
func BenchmarkMutexRecord(b *testing.B) {
	r := newMutexRing(DefaultCapacity)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			now := time.Now()
			r.put(now.UnixMilli(), CompStorage, KindTxFailed, ConnNone,
				[]Field{Str(KeyTx, "update"), Num(KeyDurUS, 42)})
		}
	})
}

// BenchmarkUpdate — Update поверх настоящего bbolt во временном каталоге:
// декоратор при PHOTON_BENCH_DIAG=1, голый адаптер иначе. Сравнение — двумя
// чередующимися прогонами (ADR-130 §5.6), а не подбенчмарками одного
// процесса: тот же файл, тот же код, единственное различие — декоратор.
func BenchmarkUpdate(b *testing.B) {
	raw, err := bboltadapter.New(filepath.Join(b.TempDir(), "b.db"), []bboltadapter.Migration{{
		Version: 1,
		Up:      func(tx *bolt.Tx) error { _, e := tx.CreateBucketIfNotExists([]byte(ns)); return e },
	}})
	if err != nil {
		b.Fatal(err)
	}
	defer raw.Close(context.Background())
	ctx := context.Background()
	if err := raw.Migrate(ctx, 1); err != nil {
		b.Fatal(err)
	}
	var st ports.Storage = raw
	if os.Getenv("PHOTON_BENCH_DIAG") == "1" {
		d, _ := New(Config{})
		st = WrapStorage(raw, d, nil)
	}
	key, val := []byte("key"), make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.Update(ctx, func(tx ports.WriteTx) error { return tx.Put(ns, key, val) }); err != nil {
			b.Fatal(err)
		}
	}
}

// cputicks — счётчик TSC процессора (RDTSC на amd64). Часы ИЗМЕРИТЕЛЯ, не
// журнала: на машине, где ядро перевело clocksource на hpet, time.Now стоит
// микросекунды и сам по себе превышает порог §5.4, а TSC процессор ведёт
// по-прежнему (constant_tsc, nonstop_tsc). Только в тестовом файле.
//
//go:linkname cputicks runtime.cputicks
func cputicks() int64

// ticksPerNS — калибровка TSC по монотонным часам за 300 мс.
func ticksPerNS() float64 {
	t0, c0 := time.Now(), cputicks()
	time.Sleep(300 * time.Millisecond)
	return float64(cputicks()-c0) / float64(time.Since(t0).Nanoseconds())
}

// measureMode — одна серия: writers горутин по per операций, время каждой
// операции — разность cputicks. Возвращает все замеры в тиках.
func measureMode(writers, per int, op func(d *Diag, i int)) []int64 {
	d, _ := New(Config{Capacity: DefaultCapacity})
	samples := make([][]int64, writers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		samples[w] = make([]int64, per)
		wg.Add(1)
		go func(s []int64) {
			defer wg.Done()
			<-start
			for i := range s {
				c0 := cputicks()
				op(d, i)
				s[i] = cputicks() - c0
			}
		}(samples[w])
	}
	close(start)
	wg.Wait()
	all := make([]int64, 0, writers*per)
	for _, s := range samples {
		all = append(all, s...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return all
}

var (
	measureSinkTime time.Time
	measureSinkMono int64
)

// BenchmarkView — View поверх настоящего bbolt: чтение без fsync, где цена
// декоратора (чтения часов, счётчики, гистограммы) не прячется за диском.
// Декоратор при PHOTON_BENCH_DIAG=1; сравнение — чередующимися прогонами.
func BenchmarkView(b *testing.B) {
	raw, err := bboltadapter.New(filepath.Join(b.TempDir(), "v.db"), []bboltadapter.Migration{{
		Version: 1,
		Up:      func(tx *bolt.Tx) error { _, e := tx.CreateBucketIfNotExists([]byte(ns)); return e },
	}})
	if err != nil {
		b.Fatal(err)
	}
	defer raw.Close(context.Background())
	ctx := context.Background()
	if err := raw.Migrate(ctx, 1); err != nil {
		b.Fatal(err)
	}
	key := []byte("key")
	if err := raw.Update(ctx, func(tx ports.WriteTx) error { return tx.Put(ns, key, make([]byte, 256)) }); err != nil {
		b.Fatal(err)
	}
	var st ports.Storage = raw
	if os.Getenv("PHOTON_BENCH_DIAG") == "1" {
		d, _ := New(Config{})
		st = WrapStorage(raw, d, nil)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.View(ctx, func(tx ports.ReadTx) error { _, e := tx.Get(ns, key); return e }); err != nil {
			b.Fatal(err)
		}
	}
}

// TestMeasureRecordLatency — методика §5.6, разложенная на составляющие
// (решения владельца 2026-10-23): часы по отдельности, вставка без чтения
// часов, полная операция — для прежнего журнала (эталон mutexRing) и для
// текущего; каждая при 1 и при 8 писателях;
// по 10⁶ операций на писателя, 5 повторов, перцентили по всем замерам.
// Критерий §5.4 — медиана p99 полного Record при 8 писателях ≤ 2 мкс.
//
// Только при PHOTON_DIAG_MEASURE=1; при PHOTON_DIAG_MEASURE_OUT=<файл> сырые
// перцентили каждого повтора пишутся в файл.
func TestMeasureRecordLatency(t *testing.T) {
	if os.Getenv("PHOTON_DIAG_MEASURE") != "1" {
		t.Skip("замер — только при PHOTON_DIAG_MEASURE=1")
	}
	const per, repeats = 1_000_000, 5
	tpn := ticksPerNS()
	ns := func(ticks int64) float64 { return float64(ticks) / tpn }
	m0 := monotime()
	mutexRef := newMutexRing(DefaultCapacity)
	modes := []struct {
		name string
		op   func(d *Diag, i int)
	}{
		// Часы по отдельности: time.Now (два чтения) и monotime (одно).
		{"time.Now", func(_ *Diag, _ int) { measureSinkTime = time.Now() }},
		{"monotime", func(_ *Diag, _ int) { measureSinkMono = monotime() }},
		// Прежний журнал: вставка под одним мьютексом и полная запись с time.Now.
		{"mutexPut", func(_ *Diag, i int) {
			mutexRef.put(1, CompStorage, KindTxFailed, ConnNone, []Field{Str(KeyTx, "update"), Num(KeyDurUS, int64(i))})
		}},
		{"mutexRecord", func(_ *Diag, i int) {
			now := time.Now()
			mutexRef.put(now.UnixMilli(), CompStorage, KindTxFailed, ConnNone,
				[]Field{Str(KeyTx, "update"), Num(KeyDurUS, int64(i))})
		}},
		// Текущий журнал: вставка без часов и полная операция Record.
		{"recordMono", func(d *Diag, i int) {
			d.recordMono(m0, CompStorage, KindTxFailed, ConnNone, Str(KeyTx, "update"), Num(KeyDurUS, int64(i)))
		}},
		{"Record", func(d *Diag, i int) {
			d.Record(CompStorage, KindTxFailed, ConnNone, Str(KeyTx, "update"), Num(KeyDurUS, int64(i)))
		}},
	}
	if probe, err := New(Config{Capacity: MinCapacity}); err == nil {
		t.Logf("часы метки Record на этой машине: %s", probe.stampName)
	}
	var out strings.Builder
	fmt.Fprintf(&out, "# tsc ticks/ns = %.4f; per = %d; repeats = %d\n", tpn, per, repeats)
	fmt.Fprintf(&out, "# mode writers repeat p50_ns p99_ns p99.9_ns max_ns\n")
	var verdict float64
	only := os.Getenv("PHOTON_DIAG_MEASURE_MODES") // "recordAt/8,Record/8" — подмножество
	for _, m := range modes {
		for _, writers := range []int{1, 8} {
			if only != "" && !strings.Contains(","+only+",", fmt.Sprintf(",%s/%d,", m.name, writers)) {
				continue
			}
			var p99s []float64
			for r := 1; r <= repeats; r++ {
				all := measureMode(writers, per, m.op)
				p50, p99 := ns(all[len(all)/2]), ns(all[len(all)*99/100])
				p999, max := ns(all[len(all)*999/1000]), ns(all[len(all)-1])
				fmt.Fprintf(&out, "%s %d %d %.0f %.0f %.0f %.0f\n", m.name, writers, r, p50, p99, p999, max)
				p99s = append(p99s, p99)
			}
			sort.Float64s(p99s)
			med := p99s[len(p99s)/2]
			t.Logf("%-9s писателей %d: медиана p99 %.0f нс (повторы %v)", m.name, writers, med, roundAll(p99s))
			if m.name == "Record" && writers == 8 {
				verdict = med
			}
		}
	}
	if path := os.Getenv("PHOTON_DIAG_MEASURE_OUT"); path != "" {
		if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
			t.Errorf("запись %s: %v", path, err)
		}
	}
	t.Logf("критерий §5.4: медиана p99 Record при 8 писателях = %.0f нс (порог 2000)", verdict)
	if verdict == 0 {
		return // режим Record/8 не выбран — критерия нет
	}
	if verdict > 2000 {
		t.Fatalf("медиана p99 %.0f нс > 2 мкс — по ADR-130 §5.4 пересматривается решение, а не порог", verdict)
	}
}

func roundAll(v []float64) []int64 {
	out := make([]int64, len(v))
	for i, x := range v {
		out[i] = int64(x + 0.5)
	}
	return out
}
