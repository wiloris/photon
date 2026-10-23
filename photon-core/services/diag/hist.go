package diag

import (
	"sync/atomic"
	"time"
)

// bucketsUS — верхние границы корзин гистограмм длительности в
// микросекундах; последняя корзина — всё, что больше. Фиксированные: снимки
// разных инстансов и разных версий сравнимы корзина в корзину.
var bucketsUS = [...]int64{10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000}

// Histogram — гистограмма длительностей на atomic-счётчиках. Наблюдение не
// аллоцирует и не берёт мьютекс.
type Histogram struct {
	counts [len(bucketsUS) + 1]atomic.Uint64
	sumUS  atomic.Uint64
	n      atomic.Uint64
}

// Observe учитывает одну длительность.
func (h *Histogram) Observe(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	i := 0
	for i < len(bucketsUS) && us > bucketsUS[i] {
		i++
	}
	h.counts[i].Add(1)
	h.sumUS.Add(uint64(us))
	h.n.Add(1)
}

// HistogramSnapshot — форма гистограммы в снимке. Counts[i] — число
// наблюдений ≤ LeUS[i]; последний элемент Counts — больше последней границы.
type HistogramSnapshot struct {
	LeUS   []int64  `json:"le_us"`
	Counts []uint64 `json:"counts"`
	SumUS  uint64   `json:"sum_us"`
	N      uint64   `json:"n"`
}

func (h *Histogram) snapshot() HistogramSnapshot {
	s := HistogramSnapshot{LeUS: append([]int64(nil), bucketsUS[:]...), Counts: make([]uint64, len(h.counts))}
	for i := range h.counts {
		s.Counts[i] = h.counts[i].Load()
	}
	s.SumUS = h.sumUS.Load()
	s.N = h.n.Load()
	return s
}
