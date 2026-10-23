package diag

import (
	"errors"
	"sync"
	"testing"
	"unsafe"
)

// Корректность slotRing — те же свойства, что у прежнего журнала (§2.4),
// плюс два состояния, которых у одного мьютекса не было: номер
// зарезервирован, но не записан; писатель обогнан на круг.

func putN(r *slotRing, n int) {
	for i := 0; i < n; i++ {
		r.put(0, CompStorage, KindTxFailed, ConnNone, []Field{Num(KeyDurUS, int64(i))})
	}
}

// writeSlot — запись задержавшегося писателя: рабочий write для заданного
// seq с одним полем v.
func (r *slotRing) writeSlot(seq uint64, v int64) {
	r.write(seq, 0, CompStorage, KindTxFailed, ConnNone, []Field{Num(KeyDurUS, v)})
}

func TestSlotRingOrderOverflowCursor(t *testing.T) {
	r := newSlotRing(64)
	if ev, next, dr, err := r.read(0, 10); err != nil || len(ev) != 0 || next != 0 || dr != 0 {
		t.Fatal("пустой журнал")
	}
	putN(r, 100)
	ev, next, dr, err := r.read(0, 512)
	if err != nil || len(ev) != 64 || ev[0].Seq != 37 || next != 100 || dr != 36 {
		t.Fatalf("переполнение: n=%d first=%d next=%d dropped=%d err=%v", len(ev), ev[0].Seq, next, dr, err)
	}
	for i, e := range ev {
		if e.Seq != uint64(37+i) {
			t.Fatalf("порядок: [%d] = %d", i, e.Seq)
		}
		if f, _ := e.Get(KeyDurUS); f.Num != int64(36+i) {
			t.Fatalf("содержимое [%d] = %d", i, f.Num)
		}
	}
	if ev, next, dr, _ := r.read(30, 5); dr != 6 || ev[0].Seq != 37 || next != 41 {
		t.Fatalf("курсор в вытесненном: dropped=%d first=%d next=%d", dr, ev[0].Seq, next)
	}
	if ev, _, dr, _ := r.read(50, 5); dr != 0 || ev[0].Seq != 51 {
		t.Fatalf("курсор в живой части: dropped=%d first=%d", dr, ev[0].Seq)
	}
	if _, next, _, _ := r.read(100, 5); next != 100 {
		t.Fatal("конец журнала: next_seq != after_seq")
	}
	if _, _, _, err := r.read(101, 5); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("курсор впереди: %v", err)
	}
	if s := r.stats(); s.LastSeq != 100 || s.Evicted != 36 || s.Len != 64 || s.Capacity != 64 {
		t.Fatalf("stats: %+v", s)
	}
	// Продолжение без пропусков и повторов.
	r2 := newSlotRing(128)
	putN(r2, 95)
	cur, seen := uint64(0), 0
	for {
		ev, next, _, _ := r2.read(cur, 10)
		if len(ev) == 0 {
			break
		}
		for _, e := range ev {
			seen++
			if e.Seq != uint64(seen) {
				t.Fatalf("продолжение: %d вместо %d", e.Seq, seen)
			}
		}
		cur = next
	}
	if seen != 95 {
		t.Fatalf("прочитано %d", seen)
	}
}

// Номер зарезервирован, но не записан: чтение останавливается перед ним и
// не перепрыгивает; после записи — продолжает с него.
func TestSlotRingReservedNotWrittenStopsReader(t *testing.T) {
	r := newSlotRing(64)
	putN(r, 3)    // 1..3
	r.last.Add(1) // 4 — зарезервирован, писатель задержался
	putN(r, 2)    // 5, 6
	ev, next, dr, _ := r.read(0, 512)
	if len(ev) != 3 || next != 3 || dr != 0 {
		t.Fatalf("до записи 4: n=%d next=%d dropped=%d", len(ev), next, dr)
	}
	if ev, next, _, _ := r.read(3, 512); len(ev) != 0 || next != 3 {
		t.Fatalf("с курсора 3 до записи 4: n=%d next=%d", len(ev), next)
	}
	r.writeSlot(4, 44)
	ev, next, _, _ = r.read(3, 512)
	if len(ev) != 3 || ev[0].Seq != 4 || next != 6 {
		t.Fatalf("после записи 4: n=%d next=%d", len(ev), next)
	}
}

// Писатель обогнан на круг: его запись в слот не пишется (слот не
// откатывается назад), а номер считается вытесненным.
func TestSlotRingLappedWriterDoesNotOverwrite(t *testing.T) {
	r := newSlotRing(64)
	r.last.Add(1)      // seq 1 зарезервирован и задержался
	putN(r, 64)        // 2..65; 65 ложится в слот номера 1
	r.writeSlot(1, -1) // опоздавший писатель
	if got := r.slotOf(1).ev.Seq; got != 65 {
		t.Fatalf("слот откатился: seq %d, want 65", got)
	}
	ev, next, dr, _ := r.read(0, 512)
	if dr != 1 || ev[0].Seq != 2 || next != 65 || len(ev) != 64 {
		t.Fatalf("n=%d first=%d next=%d dropped=%d", len(ev), ev[0].Seq, next, dr)
	}
}

// 8 писателей и 3 читателя, малая ёмкость — обгоны на круг постоянны.
// Каждый читатель: seq строго растут, прочитанное + dropped = next_seq
// (ни одной молчаливой потери), содержимое записи цельное.
func TestSlotRingConcurrent(t *testing.T) {
	r := newSlotRing(64)
	const writers, per = 8, 20000
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				// Цельность: оба поля одной записи согласованы.
				v := int64(w*per + i)
				r.put(v, CompStorage, KindTxFailed, ConnNone, []Field{Num(KeyDurUS, v), Num(KeyAttempt, v)})
			}
		}(w)
	}
	done := make(chan struct{})
	var rwg sync.WaitGroup
	for k := 0; k < 3; k++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			cur, accounted := uint64(0), uint64(0)
			for {
				ev, next, dr, err := r.read(cur, 7)
				if err != nil {
					t.Errorf("read: %v", err)
					return
				}
				prev := cur
				for _, e := range ev {
					if e.Seq <= prev {
						t.Errorf("seq не растёт: %d после %d", e.Seq, prev)
						return
					}
					a, _ := e.Get(KeyDurUS)
					b, _ := e.Get(KeyAttempt)
					if a.Num != b.Num || e.AtMonoMS != a.Num {
						t.Errorf("запись разорвана: %d/%d/%d", a.Num, b.Num, e.AtMonoMS)
						return
					}
					prev = e.Seq
				}
				if len(ev) == 0 && dr != 0 {
					t.Errorf("dropped без записей")
					return
				}
				accounted += dr + uint64(len(ev))
				if len(ev) > 0 && accounted != next {
					t.Errorf("прочитано+dropped = %d, next_seq = %d", accounted, next)
					return
				}
				cur = next
				if len(ev) == 0 {
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
	if s := r.stats(); s.LastSeq != writers*per {
		t.Fatalf("last = %d", s.LastSeq)
	}
	// После остановки писателей все номера окна записаны и читаются подряд.
	ev, next, _, _ := r.read(writers*per-64, 512)
	if len(ev) != 64 || next != writers*per {
		t.Fatalf("хвост: n=%d next=%d", len(ev), next)
	}
}

func TestSlotRingPutZeroAllocs(t *testing.T) {
	r := newSlotRing(64)
	class := "fn:not_found"
	if a := testing.AllocsPerRun(10000, func() {
		r.put(1, CompStorage, KindTxFailed, ConnAction, []Field{Str(KeyTx, "update"), Str(KeyClass, class), Num(KeyDurUS, 42)})
	}); a != 0 {
		t.Fatalf("put: %.1f аллокаций", a)
	}
}

// Раскладка (slotring.go): слот кратен линии кэша, записываемые части
// соседних слотов не делят линию при всех допустимых ёмкостях, а линия
// счётчика не содержит ничего, кроме счётчика. Свойство производительности, не
// корректности, — но молча потерять его при правке Event нельзя.
func TestSlotRingLayout(t *testing.T) {
	if sz := unsafe.Sizeof(slot{}); sz%cacheLine != 0 {
		t.Fatalf("sizeof slot = %d, не кратно %d", sz, cacheLine)
	}
	if pad := unsafe.Sizeof(slot{}) - unsafe.Sizeof(slotBody{}); pad >= cacheLine {
		t.Fatalf("дополнение слота %d Б — лишняя линия", pad)
	}
	var r slotRing
	off := unsafe.Offsetof(r.last)
	if off < cacheLine-8 || unsafe.Offsetof(r.slots) < off+cacheLine {
		t.Fatalf("счётчик делит линию: last @%d, slots @%d", off, unsafe.Offsetof(r.slots))
	}
	// Ровного выравнивания массива не требуется: у малых объектов с
	// указателями (≤ 32 КБ) Go 1.22 кладёт 8-байтный заголовок типа, и массив
	// начинается со смещением 8. Нужно другое — записываемая часть слота не
	// заходит в линию, где начинается соседний: смещение + полезная часть ≤
	// размер слота (дополнение поглощает смещение).
	for _, c := range []int{MinCapacity, DefaultCapacity, MaxCapacity, 100, 1000} {
		r := newSlotRing(c)
		a := uintptr(unsafe.Pointer(&r.slots[0])) % cacheLine
		if a+unsafe.Sizeof(slotBody{}) > unsafe.Sizeof(slot{}) {
			t.Errorf("ёмкость %d: смещение массива %d — соседние слоты делят линию", c, a)
		}
	}
}
