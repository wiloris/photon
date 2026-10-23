package diag

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// slotRing — журнал на слотах: номер записи резервируется атомарным
// инкрементом, запись в слот — под мьютексом СЛОТА. Восемь писателей
// конкурируют за один счётчик (атомарная операция, без парковки), а не за
// один мьютекс: профиль показал, что хвост задержки прежнего кольца — это
// ожидание единственного мьютекса, а не работа под ним
// (measurements/2026-10-23-contention).
//
// Инварианты и почему они держатся:
//
//  1. Глобальный порядок seq. seq выдаёт один атомарный счётчик; слот записи
//     с номером s — (s−1) mod cap. Порядок в журнале — порядок seq, как и
//     прежде.
//  2. Слот не откатывается назад. Писатель, обогнанный на круг (его слот уже
//     занят записью с большим seq), свою запись не пишет: она уже вне окна
//     журнала и считается вытесненной. Проверка — под мьютексом слота.
//  3. Читатель не видит половину записи: копирует слот под его мьютексом.
//  4. Читатель не теряет запись молча. Для каждого s из (after, last] слот
//     даёт одно из трёх: seq == s — отдать; seq > s — запись вытеснена;
//     seq < s — номер зарезервирован, но ещё не записан. На третьем чтение
//     ОСТАНАВЛИВАЕТСЯ: next_seq не перепрыгивает незаписанную запись, и
//     следующее чтение её получит (или посчитает вытесненной).
//  5. dropped — как прежде: только вместе с отданными записями и только за
//     номера в (after, первый отданный). Вытеснение посреди чтения (писатели
//     обогнали читателя на круг) обрывает ответ на уже отданном — разрыв
//     увидит следующее чтение.
//
// Раскладка памяти (профиль на arm64, measurements/2026-10-23-redmi-profile):
// счётчик last занимает СВОЮ линию кэша — отступ и до, и после него. Прежде
// перед ним лежал заголовок среза slots, который put читает сразу после
// инкремента: при восьми писателях каждое чтение заголовка промахивалось на
// линии, которую соседнее ядро только что забрало инкрементом (39 % профиля
// в slotOf). Слот дополнен до кратного 64 Б: записываемые части соседних
// слотов не делят линию, и запись в слот не задевает мьютекс соседнего.
// Дополнение (48 Б) поглощает и 8-байтный заголовок, с которым Go кладёт
// малые массивы с указателями (TestSlotRingLayout).
type slotRing struct {
	_     [cacheLine]byte
	last  atomic.Uint64 // последний зарезервированный seq
	_     [cacheLine - 8]byte
	slots []slot
}

// cacheLine — размер линии кэша на amd64 и arm64 (Cortex-A55/A75).
const cacheLine = 64

type slot struct {
	mu sync.Mutex
	ev Event // ev.Seq == 0 — слот ещё не писался
	_  [(cacheLine - unsafe.Sizeof(slotBody{})%cacheLine) % cacheLine]byte
}

// slotBody — полезная часть слота, по ней считается дополнение.
type slotBody struct {
	mu sync.Mutex
	ev Event
}

func newSlotRing(capacity int) *slotRing {
	return &slotRing{slots: make([]slot, capacity)}
}

func (r *slotRing) slotOf(seq uint64) *slot {
	return &r.slots[(seq-1)%uint64(len(r.slots))]
}

// put резервирует номер и пишет запись. mono — уже вычисленное время
// записи: работы под мьютексом слота — только копирование.
func (r *slotRing) put(mono int64, c Component, k Kind, conn Conn, fields []Field) {
	r.write(r.last.Add(1), mono, c, k, conn, fields)
}

// write — запись зарезервированного номера seq в его слот. Отдельно от
// резервирования, чтобы тест воспроизводил задержавшегося писателя на том
// же коде, что работает.
func (r *slotRing) write(seq uint64, mono int64, c Component, k Kind, conn Conn, fields []Field) {
	s := r.slotOf(seq)
	s.mu.Lock()
	if s.ev.Seq < seq { // инвариант 2
		e := &s.ev
		e.Seq, e.AtMonoMS = seq, mono
		e.Component, e.Kind, e.Conn = c, k, conn
		e.N = uint8(copy(e.Fields[:], fields))
	}
	s.mu.Unlock()
}

// read — форма ответа 820 (без instance_id). Ошибки — те же, что у прежнего
// журнала; limit проверен вызывающим.
func (r *slotRing) read(afterSeq uint64, limit int) (events []Event, nextSeq, dropped uint64, err error) {
	last := r.last.Load()
	if afterSeq > last {
		return nil, 0, 0, ErrInvalidCursor
	}
	nextSeq = afterSeq
	capacity := uint64(len(r.slots))
	first := afterSeq + 1
restart:
	if last > capacity && first < last-capacity+1 {
		first = last - capacity + 1
	}
	for s := first; s <= last && len(events) < limit; s++ {
		sl := r.slotOf(s)
		sl.mu.Lock()
		got := sl.ev.Seq
		if got == s {
			events = append(events, sl.ev)
		}
		sl.mu.Unlock()
		switch {
		case got == s:
			nextSeq = s
		case got > s:
			if len(events) > 0 {
				return events, nextSeq, first - (afterSeq + 1), nil // инвариант 5
			}
			// Писатели обогнали на круг ещё до первой отданной: окно
			// сдвинулось — пересчитать начало по новому last.
			last = r.last.Load()
			first = s + 1
			goto restart
		default:
			// Зарезервировано, не записано — стоп (инвариант 4).
			if len(events) == 0 {
				return nil, afterSeq, 0, nil
			}
			return events, nextSeq, first - (afterSeq + 1), nil
		}
	}
	if len(events) == 0 {
		return nil, afterSeq, 0, nil
	}
	return events, nextSeq, first - (afterSeq + 1), nil
}

// stats — ёмкость, занятость, последний номер, вытеснено. Вытеснено — всё,
// что вышло за окно ёмкости: last − cap.
func (r *slotRing) stats() JournalStats {
	last := r.last.Load()
	capacity := uint64(len(r.slots))
	js := JournalStats{Capacity: len(r.slots), LastSeq: last}
	if last > capacity {
		js.Len, js.Evicted = len(r.slots), last-capacity
	} else {
		js.Len = int(last)
	}
	return js
}
