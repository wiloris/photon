package main

import (
	"fmt"
	"sync"
	"testing"
)

// Модульные тесты механизма ADR-059 п. 5 — на чистом Go, без C-памяти.
//
// Переиспользование адреса ни ABI-тестом, ни ASan не доказать: карантин ASan
// откладывает выдачу освобождённого адреса. Поэтому реестру подставляется
// аллокатор, который ведёт себя хуже настоящего: освобождённый адрес выдаёт
// СЛЕДУЮЩЕМУ же alloc (LIFO). Если механизм освободит ячейку раньше времени,
// новый объект немедленно окажется на старом адресе, и тест это увидит.

// reuseCells — контролируемый аллокатор: LIFO-переиспользование, учёт
// освобождений, громкий отказ на двойном освобождении и на освобождении
// невыданного адреса.
type reuseCells struct {
	mu       sync.Mutex
	next     uintptr
	freeList []uintptr
	live     map[uintptr]bool
	freed    map[uintptr]int // сколько раз освобождён каждый адрес за всё время

	// errs копит нарушения: free вызывается под мьютексом реестра и из
	// горутин, t.Fatal там неуместен.
	errs []string

	// forceKey, если не 0, выдаётся alloc вместо честного адреса — для
	// проверки И6.
	forceKey uintptr
}

func newReuseCells() *reuseCells {
	return &reuseCells{next: 0x1000, live: map[uintptr]bool{}, freed: map[uintptr]int{}}
}

func (a *reuseCells) alloc() uintptr {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.forceKey != 0 {
		return a.forceKey
	}
	var k uintptr
	if n := len(a.freeList); n > 0 {
		k = a.freeList[n-1]
		a.freeList = a.freeList[:n-1]
	} else {
		k = a.next
		a.next += 8
	}
	a.live[k] = true
	return k
}

func (a *reuseCells) free(k uintptr) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.live[k] {
		a.errs = append(a.errs, fmt.Sprintf("free невыданного или уже освобождённого адреса %#x", k))
		return
	}
	delete(a.live, k)
	a.freed[k]++
	a.freeList = append(a.freeList, k)
}

func (a *reuseCells) freedCount(k uintptr) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.freed[k]
}

func (a *reuseCells) check(t *testing.T) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, e := range a.errs {
		t.Error(e)
	}
}

// Главный сценарий редакции 2: вызов, законно вошедший до снятия с учёта,
// удерживает адрес. Пока он активен, ячейка не освобождается, и новый объект
// получает ДРУГОЙ адрес — хотя аллокатор выдал бы старый при первой
// возможности. После выхода старого вызова ячейка освобождается ровно один
// раз, и только тогда адрес уходит новому объекту (И5, И6, И7, п. 5.1).
func TestRegistry_OldEpochCallHoldsAddressAgainstReuse(t *testing.T) {
	cells := newReuseCells()
	r := newHandleRegistry[uintptr](cells)

	old := &struct{ name string }{"old"}
	a := r.register(old)

	// Вызов старой эпохи вошёл до снятия с учёта и нашёл свой объект.
	v, eOld, ok := r.enter(a)
	if !ok || v != old {
		t.Fatalf("вход старого вызова: ok=%v v=%v", ok, v)
	}

	// Уничтожение: снятие с учёта. Проход освобождения при снятии обязан
	// оставить ячейку — старый вызов активен.
	if !r.retire(a) {
		t.Fatal("retire живого handle вернул false")
	}
	if n := cells.freedCount(a); n != 0 {
		t.Fatalf("ячейка освобождена при активном вызове старой эпохи (%d раз)", n)
	}

	// Новый объект, пока старый вызов активен, получает другой адрес.
	fresh := &struct{ name string }{"new"}
	b := r.register(fresh)
	if b == a {
		t.Fatal("адрес старого handle выдан новому объекту, пока вызов старой эпохи активен")
	}
	// Поиск по старому адресу — промах, а не чужой объект.
	if v, e, ok := r.enter(a); ok {
		t.Fatalf("поиск по снятому адресу нашёл объект %v", v)
	} else {
		r.exit(e)
	}

	// Старый вызов вышел — ячейка освобождена ровно один раз.
	r.exit(eOld)
	if n := cells.freedCount(a); n != 1 {
		t.Fatalf("после выхода старого вызова ячейка освобождена %d раз, ожидался 1", n)
	}

	// Аллокатор действительно переиспользует немедленно: следующий объект
	// получает старый адрес. Без этого проверка выше ничего бы не доказывала.
	if c := r.register("newer"); c != a {
		t.Fatalf("контролируемый аллокатор не переиспользовал адрес: %#x, ожидался %#x", c, a)
	}
	cells.check(t)
}

// Не-реентерабельный Destroy снимает свой handle, сам оставаясь активным
// вызовом: при снятии ячейка не освобождается, на его выходе — освобождается.
func TestRegistry_RetiringCallHoldsOwnCellUntilExit(t *testing.T) {
	cells := newReuseCells()
	r := newHandleRegistry[uintptr](cells)

	a := r.register("inst")
	_, e, ok := r.enter(a)
	if !ok {
		t.Fatal("вход не нашёл handle")
	}
	r.retire(a)
	if n := cells.freedCount(a); n != 0 {
		t.Fatalf("снимающий вызов ещё активен, а ячейка освобождена (%d)", n)
	}
	r.exit(e)
	if n := cells.freedCount(a); n != 1 {
		t.Fatalf("на выходе снимающего вызова освобождено %d раз, ожидался 1", n)
	}
	cells.check(t)
}

// Отложенное уничтожение (ADR-059 редакция 3): снятие выполняет насос, а не
// экспортируемый вызов. Если активных вызовов старых эпох нет, ячейка
// освобождается прямо при снятии — без единого последующего enter/exit.
func TestRegistry_RetireWithoutActiveCallsReclaimsImmediately(t *testing.T) {
	cells := newReuseCells()
	r := newHandleRegistry[uintptr](cells)

	a := r.register("inst")
	// Реентерабельный Destroy вошёл и вышел раньше, чем насос снял handle.
	_, e, _ := r.enter(a)
	r.exit(e)

	r.retire(a) // насос; после этого ни одного вызова
	if n := cells.freedCount(a); n != 1 {
		t.Fatalf("снятие без активных вызовов освободило ячейку %d раз, ожидался 1 — "+
			"ячейка ждёт экспортируемого вызова, которого может не быть", n)
	}
	cells.check(t)
}

// Освобождение ждёт только вызовы СТАРЫХ эпох: непрерывный поток новых входов,
// перекрывающих друг друга так, что активный вызов есть всегда, освобождение
// не блокирует.
func TestRegistry_NewEpochCallsDoNotStarveReclaim(t *testing.T) {
	cells := newReuseCells()
	r := newHandleRegistry[uintptr](cells)

	other := r.register("other")
	a := r.register("inst")

	_, prev, _ := r.enter(other) // эпоха не новее снятия
	r.retire(a)
	for i := 0; i < 50; i++ {
		_, cur, _ := r.enter(other) // новая эпоха, вход ДО выхода предыдущего
		r.exit(prev)
		prev = cur
		if n := cells.freedCount(a); n != 1 {
			t.Fatalf("итерация %d: ячейка освобождена %d раз при активных вызовах только новых эпох", i, n)
		}
	}
	r.exit(prev)
	if n := cells.freedCount(a); n != 1 {
		t.Fatalf("итог: ячейка освобождена %d раз, ожидался 1", n)
	}
	cells.check(t)
}

// И7: ячейка освобождается ровно один раз, сколько бы проходов освобождения ни
// было после; повторное снятие того же адреса ничего не делает.
func TestRegistry_CellFreedExactlyOnce(t *testing.T) {
	cells := newReuseCells()
	r := newHandleRegistry[uintptr](cells)

	keep := r.register("keep")
	a := r.register("inst")
	_, e, _ := r.enter(a)
	r.retire(a)
	if r.retire(a) {
		t.Fatal("повторное снятие того же адреса вернуло true")
	}
	r.exit(e)
	for i := 0; i < 20; i++ {
		_, e, _ := r.enter(keep)
		r.exit(e)
	}
	if n := cells.freedCount(a); n != 1 {
		t.Fatalf("ячейка освобождена %d раз, ожидался 1", n)
	}
	if n := cells.freedCount(keep); n != 0 {
		t.Fatalf("живой handle освобождён %d раз", n)
	}
	cells.check(t)
}

// И6: регистрация на занятом адресе — дефект механизма, и он громкий: запись
// не затирает живой объект молча.
func TestRegistry_RegisterOnLiveAddressPanics(t *testing.T) {
	cells := newReuseCells()
	r := newHandleRegistry[uintptr](cells)

	a := r.register("live")
	cells.forceKey = a // аллокатор «ошибся» и выдал занятый адрес

	defer func() {
		if recover() == nil {
			t.Fatal("регистрация на занятом адресе не отказала")
		}
		if v, e, ok := r.enter(a); !ok || v != "live" {
			t.Fatalf("живой объект затёрт: ok=%v v=%v", ok, v)
		} else {
			r.exit(e)
		}
	}()
	r.register("intruder")
}

// Конкурентная проверка под -race при немедленном переиспользовании адресов:
// вызов, вошедший до снятия с учёта, находит свой объект, а не чужой; И6 не
// срабатывает (паника регистрации уронила бы тест); двойных освобождений нет;
// после выхода всех вызовов реестр пуст — ни одной зависшей ячейки.
func TestRegistry_ConcurrentCallsNeverSeeForeignObject(t *testing.T) {
	cells := newReuseCells()
	r := newHandleRegistry[uintptr](cells)

	const workers, rounds, readers = 8, 300, 3
	var wg sync.WaitGroup
	errs := make(chan string, workers*rounds)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				obj := &struct{ w, i int }{w, i}
				k := r.register(obj)

				var rw sync.WaitGroup
				for j := 0; j < readers; j++ {
					_, e, ok := r.enter(k) // вошёл до снятия
					if !ok {
						errs <- "вход до снятия не нашёл объект"
					}
					rw.Add(1)
					go func(e uint64) {
						defer rw.Done()
						// Повторного поиска вызов не делает (один поиск на
						// вызов), но адрес k держит, пока не вышел.
						r.exit(e)
					}(e)
				}
				if v, e, ok := r.enter(k); !ok || v != obj {
					errs <- fmt.Sprintf("объект на адресе подменён до снятия: %v", v)
				} else {
					r.retire(k)
					r.exit(e)
				}
				rw.Wait()
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	cells.check(t)

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.retired) != 0 || len(r.active) != 0 || len(r.entries) != 0 {
		t.Fatalf("после выхода всех вызовов: retired=%d active=%d entries=%d",
			len(r.retired), len(r.active), len(r.entries))
	}
}
