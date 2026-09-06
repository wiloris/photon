package localfeed

import (
	"fmt"
	"sync"
)

// Stats — журнал фактического access pattern локального чтения.
//
// По условию Phase 12 это не вспомогательная диагностика, а основной результат
// среза: решение о расширении ports.Storage принимается по числам отсюда, а не
// по ощущению «медленно». Входной критерий следующего ADR (ADR-046 §6.3)
// требует пункт 3 — «измеренная стоимость, с числами».
//
// Ключевая величина — Skipped: сколько записей пришлось просмотреть и
// ВЫБРОСИТЬ, чтобы добраться до нужной страницы. Именно она растёт от
// отсутствия seek в порте и именно она отвечает на вопрос «где появляется
// неприемлемая стоимость».
type Stats struct {
	mu sync.Mutex

	Writes int // записей сохранено
	Reads  int // вызовов Page

	Scanned int // всего записей просмотрено обходом
	Skipped int // из них выброшено ради выхода на курсор
	Yielded int // отдано вызывающему

	// PerPage — по странице отдельно: средние прячут рост.
	PerPage []PageStat
}

type PageStat struct {
	Scanned int
	Skipped int
	Yielded int
}

func (s *Stats) countWrite() {
	s.mu.Lock()
	s.Writes++
	s.mu.Unlock()
}

func (s *Stats) countRead(scanned, skipped, yielded int) {
	s.mu.Lock()
	s.Reads++
	s.Scanned += scanned
	s.Skipped += skipped
	s.Yielded += yielded
	s.PerPage = append(s.PerPage, PageStat{Scanned: scanned, Skipped: skipped, Yielded: yielded})
	s.mu.Unlock()
}

// Snapshot — копия для отчёта, без гонки. Отдельный тип, а не Stats: копировать
// структуру с мьютексом нельзя, и go vet это ловит.
type Snapshot struct {
	Writes, Reads             int
	Scanned, Skipped, Yielded int
	PerPage                   []PageStat
}

func (s *Stats) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Snapshot{Writes: s.Writes, Reads: s.Reads, Scanned: s.Scanned,
		Skipped: s.Skipped, Yielded: s.Yielded,
		PerPage: append([]PageStat(nil), s.PerPage...)}
}

// Report — журнал в человекочитаемом виде.
func (s *Stats) Report() string {
	c := s.Snapshot()
	out := fmt.Sprintf("записей сохранено: %d | страниц прочитано: %d\n", c.Writes, c.Reads)
	out += fmt.Sprintf("просмотрено всего: %d | выброшено ради курсора: %d | отдано: %d\n",
		c.Scanned, c.Skipped, c.Yielded)
	if c.Yielded > 0 {
		out += fmt.Sprintf("СТОИМОСТЬ: %.2f просмотренных записей на одну отданную\n",
			float64(c.Scanned)/float64(c.Yielded))
	}
	out += "по страницам (просмотрено / выброшено / отдано):\n"
	for i, p := range c.PerPage {
		out += fmt.Sprintf("  стр. %2d: %6d / %6d / %4d\n", i+1, p.Scanned, p.Skipped, p.Yielded)
	}
	return out
}
