package main

import "sync"

// Реестр живых непрозрачных handle — механизм ADR-059 п. 5.
//
// Файл намеренно без cgo: механизм проверяется модульным тестом на чистом Go,
// которому подставляется контролируемый аллокатор, способный выдать только что
// освобождённый адрес. Карантин ASan такую выдачу откладывает, поэтому
// переиспользование адреса иначе не проверить (ADR-059, «Тестовая стратегия»).
//
// Адрес — только ключ. Реестр никогда не разыменовывает ячейку (И1): объект
// удерживает запись реестра, а не значение в C-памяти.
//
// Инварианты (ADR-059 п. 5):
//   - И1. Поиск не разыменовывает ячейку ни в одном пути.
//   - И2. Эпоха фиксируется при входе; active[e]++ и поиск — один
//     критический участок.
//   - И3. close(teardownDone) — строго до снятия с учёта (обязанность
//     вызывающего, см. core.teardown).
//   - И4. Удаление записи, помещение ячейки в retired и epoch++ атомарны
//     относительно новых входов.
//   - И5. Ячейка не освобождается, пока активен вызов эпохи не новее эпохи её
//     снятия.
//   - И6. Регистрация никогда не находит занятый ключ.
//   - И7. Каждая ячейка освобождается ровно один раз.

// cellAllocator выдаёт и освобождает уникальные адреса ячеек. В рабочем коде —
// C.malloc/C.free; в модульном тесте реестра — контролируемая реализация.
type cellAllocator[K comparable] interface {
	alloc() K
	free(K)
}

type retiredCell[K comparable] struct {
	key   K
	epoch uint64
}

type handleRegistry[K comparable] struct {
	mu      sync.Mutex
	cells   cellAllocator[K]
	entries map[K]interface{}

	// epoch — текущая эпоха; active[e] — число экспортируемых вызовов,
	// вошедших в эпоху e и ещё не вышедших; retired — снятые с учёта ячейки,
	// ожидающие освобождения.
	epoch   uint64
	active  map[uint64]int
	retired []retiredCell[K]
}

func newHandleRegistry[K comparable](cells cellAllocator[K]) *handleRegistry[K] {
	return &handleRegistry[K]{
		cells:   cells,
		entries: make(map[K]interface{}),
		active:  make(map[uint64]int),
	}
}

// register выделяет ячейку и регистрирует за её адресом v.
func (r *handleRegistry[K]) register(v interface{}) K {
	key := r.cells.alloc()
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, busy := r.entries[key]; busy {
		// И6: адрес выдаётся аллокатором только после освобождения, а
		// освобождение — только после удаления записи. Занятый ключ — дефект
		// механизма, а не ошибка вызывающего; продолжать нельзя: запись
		// затёрла бы живой объект.
		panic("handle registry: И6 нарушен — адрес уже зарегистрирован")
	}
	r.entries[key] = v
	return key
}

// enter — вход экспортируемого вызова: фиксирует эпоху и в том же критическом
// участке ищет запись (И2). Возвращённую эпоху вызывающий ОБЯЗАН отдать в exit
// при любом исходе поиска, последним действием вызова.
func (r *handleRegistry[K]) enter(key K) (v interface{}, epoch uint64, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	epoch = r.epoch
	r.active[epoch]++
	v, ok = r.entries[key]
	return v, epoch, ok
}

// exit — выход экспортируемого вызова, вошедшего в эпоху epoch.
func (r *handleRegistry[K]) exit(epoch uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch n := r.active[epoch]; {
	case n <= 0:
		panic("handle registry: exit без парного enter")
	case n == 1:
		delete(r.active, epoch)
	default:
		r.active[epoch] = n - 1
	}
	r.reclaimLocked()
}

// retire — снятие с учёта (И4). Ячейка сразу не освобождается: после снятия
// выполняется тот же проход освобождения, что и на выходе вызова, с тем же
// условием И5. Вторая точка нужна отложенному уничтожению — его снятие
// выполняет насос, а не экспортируемый вызов, и без неё ячейка ждала бы
// вызова, которого может не быть (ADR-059 п. 5, редакция 3).
//
// ok=false — записи нет; при соблюдении И7 вызывающим недостижимо.
func (r *handleRegistry[K]) retire(key K) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[key]; !ok {
		return false
	}
	delete(r.entries, key)
	r.retired = append(r.retired, retiredCell[K]{key: key, epoch: r.epoch})
	r.epoch++
	r.reclaimLocked()
	return true
}

// reclaimLocked — проход освобождения (И5): освобождаются ячейки, чья эпоха
// снятия меньше наименьшей эпохи активных вызовов; при отсутствии активных —
// все. Вызывается под r.mu.
func (r *handleRegistry[K]) reclaimLocked() {
	if len(r.retired) == 0 {
		return
	}
	minActive, hasActive := uint64(0), false
	for e := range r.active {
		if !hasActive || e < minActive {
			minActive, hasActive = e, true
		}
	}
	kept := r.retired[:0]
	for _, rc := range r.retired {
		if hasActive && rc.epoch >= minActive {
			kept = append(kept, rc)
			continue
		}
		r.cells.free(rc.key)
	}
	// Хвост обнуляется, чтобы освобождённые ключи не держались срезом.
	var zero retiredCell[K]
	for i := len(kept); i < len(r.retired); i++ {
		r.retired[i] = zero
	}
	r.retired = kept
}
