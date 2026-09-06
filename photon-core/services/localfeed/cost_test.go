package localfeed

import (
	"context"
	"fmt"
	"testing"
)

// ИЗМЕРЕНИЕ СТОИМОСТИ — основной результат среза, а не диагностика.
//
// Вопрос, на который отвечают эти тесты: где отсутствие seek в ports.Storage
// начинает стоить неприемлемо. Ответ нужен числами — входной критерий
// следующего ADR (ADR-046 §6.3, пункт 3) иного не принимает.
//
// Механика ограничения: ForEachPrefix умеет идти ТОЛЬКО от начала префикса.
// Чтобы отдать N-ю страницу, приходится пройти и выбросить все предыдущие.
// Стоимость страницы линейна по её глубине, а суммарная стоимость обхода —
// квадратична по числу страниц.

func seedChannel(t *testing.T, s *Store, channel string, n int) {
	t.Helper()
	ctx := context.Background()
	const batch = 500
	var recs []Record
	for i := 0; i < n; i++ {
		recs = append(recs, rec(fmt.Sprintf("p%06d", i), int64(1_000_000+i)))
		if len(recs) == batch {
			if err := s.Put(ctx, channel, recs); err != nil {
				t.Fatalf("Put: %v", err)
			}
			recs = recs[:0]
		}
	}
	if len(recs) > 0 {
		if err := s.Put(ctx, channel, recs); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
}

// Стоимость одной страницы растёт ЛИНЕЙНО по глубине пролистывания.
// Это и есть цена отсутствия seek, выраженная числом.
func TestCost_PageScanGrowsLinearlyWithDepth(t *testing.T) {
	s, stats := newStore(t)
	const total, pageSize = 2000, 50
	seedChannel(t, s, "ch", total)

	ctx := context.Background()
	var curKey int64
	var curID string
	pages := 0
	for {
		page, err := s.Page(ctx, "ch", curKey, curID, pageSize)
		if err != nil {
			t.Fatalf("Page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		pages++
		last := page[len(page)-1]
		curKey, curID = last.OrderKey, last.ID
	}

	snap := stats.Snapshot()
	if pages != total/pageSize {
		t.Fatalf("страниц %d, ожидалось %d", pages, total/pageSize)
	}

	// Первая страница не выбрасывает ничего, последняя выбрасывает почти всё.
	first, last := snap.PerPage[0], snap.PerPage[len(snap.PerPage)-2]
	if first.Skipped != 0 {
		t.Fatalf("первая страница выбросила %d записей — курсора не было", first.Skipped)
	}
	if last.Skipped < total-2*pageSize {
		t.Fatalf("последняя страница выбросила %d — рост не линейный", last.Skipped)
	}

	// Суммарно: квадратичный рост. Для 2000 записей по 50 — около 40к
	// просмотров ради 2000 отданных, то есть примерно ×20.
	ratio := float64(snap.Scanned) / float64(snap.Yielded)
	t.Logf("\n%s", stats.Report())
	t.Logf("ИТОГ: %d записей, страница %d → %.1f просмотренных на одну отданную",
		total, pageSize, ratio)
	if ratio < 5 {
		t.Fatalf("стоимость не выросла (%.1f) — измерение не измеряет", ratio)
	}
}

// Открытие канала — самый частый сценарий, и он ДЁШЕВ: первая страница
// читается без пропусков независимо от размера канала. Это существенно:
// ограничение бьёт не по «открыть чат», а по «листать вглубь».
func TestCost_OpenChannelIsCheapRegardlessOfSize(t *testing.T) {
	for _, total := range []int{100, 1000, 5000} {
		t.Run(fmt.Sprintf("канал_%d", total), func(t *testing.T) {
			s, stats := newStore(t)
			seedChannel(t, s, "ch", total)
			if _, err := s.Page(context.Background(), "ch", 0, "", 50); err != nil {
				t.Fatalf("Page: %v", err)
			}
			snap := stats.Snapshot()
			p := snap.PerPage[len(snap.PerPage)-1]
			t.Logf("канал %5d записей: просмотрено %d, выброшено %d, отдано %d",
				total, p.Scanned, p.Skipped, p.Yielded)
			if p.Skipped != 0 {
				t.Fatalf("открытие канала выбросило %d записей", p.Skipped)
			}
			if p.Scanned > 51 {
				t.Fatalf("открытие просмотрело %d записей вместо ~50 — обход не останавливается", p.Scanned)
			}
		})
	}
}

// Глубина, на которой стоимость становится неприемлемой. Числа, а не оценка.
func TestCost_DepthProfile(t *testing.T) {
	s, _ := newStore(t)
	const total, pageSize = 5000, 50
	seedChannel(t, s, "ch", total)
	ctx := context.Background()

	for _, depth := range []int{1, 5, 10, 20, 50, 100} {
		st := &Stats{}
		probe := New(s.st, st)
		var curKey int64
		var curID string
		for i := 0; i < depth; i++ {
			page, err := probe.Page(ctx, "ch", curKey, curID, pageSize)
			if err != nil {
				t.Fatalf("Page: %v", err)
			}
			if len(page) == 0 {
				break
			}
			last := page[len(page)-1]
			curKey, curID = last.OrderKey, last.ID
		}
		snap := st.Snapshot()
		if len(snap.PerPage) == 0 {
			continue
		}
		p := snap.PerPage[len(snap.PerPage)-1]
		t.Logf("страница №%3d (глубина %5d записей): просмотрено %5d, выброшено %5d ради %d отданных",
			depth, depth*pageSize, p.Scanned, p.Skipped, p.Yielded)
	}
}
