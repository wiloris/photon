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

// ЯДРО ДОКАЗАТЕЛЬСТВА ADR-047: стоимость страницы НЕ зависит от глубины.
//
// До ADR-047 эта же проверка утверждала обратное — что стоимость растёт
// линейно, — и была верна: ForEachPrefix умел идти только от начала префикса,
// и N-я страница проходила и выбрасывала все предыдущие. Теперь позиционирует
// сам порт, и утверждение перевёрнуто. Тест сохранён на том же месте
// намеренно: он же и мерил проблему, он же и доказывает её отсутствие.
func TestCost_PageScanIsConstantRegardlessOfDepth(t *testing.T) {
	s, stats := newStore(t)
	const total, pageSize = 2000, 50
	seedChannel(t, s, "ch", total)

	ctx := context.Background()
	var curKey int64
	var curID string
	pages := 0
	// Предел обязателен: без него страница, игнорирующая курсор, даёт
	// бесконечный цикл вместо падения — тест зависает, а зависший тест
	// мутацию не ловит. Предел заведомо больше честного числа страниц.
	const maxPages = total/pageSize + 10
	for {
		if pages > maxPages {
			t.Fatalf("более %d страниц на %d записях — курсор не двигается", maxPages, total)
		}
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

	// Ни одна страница не выбрасывает ничего: линейного пропуска больше нет.
	for i, p := range snap.PerPage {
		if p.Skipped != 0 {
			t.Fatalf("страница %d выбросила %d записей — линейный пропуск вернулся", i+1, p.Skipped)
		}
	}

	// Просмотр каждой непустой страницы ограничен её размером, а не глубиной.
	for i, p := range snap.PerPage {
		if p.Yielded == 0 {
			continue // хвостовая пустая страница
		}
		if p.Scanned > pageSize {
			t.Fatalf("страница %d просмотрела %d при размере %d — стоимость зависит от глубины",
				i+1, p.Scanned, pageSize)
		}
	}

	ratio := float64(snap.Scanned) / float64(snap.Yielded)
	t.Logf("\n%s", stats.Report())
	t.Logf("ИТОГ: %d записей, страница %d → %.2f просмотренных на одну отданную (было 21.50)",
		total, pageSize, ratio)
	if ratio > 1.05 {
		t.Fatalf("стоимость %.2f на одну отданную — ожидалась ≈1.0", ratio)
	}
}

// Открытие канала дёшево независимо от размера. Свойство было верным и до
// ADR-047 (обход останавливается на limit) — проверка сохранена, чтобы
// изменение порта его не сломало.
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
				t.Fatalf("открытие просмотрело %d записей вместо ~50", p.Scanned)
			}
		})
	}
}

// Профиль глубины — тот же, что снимался до ADR-047, ради сравнения
// «до → после» на одних и тех же точках. Прежние числа: страница №5
// просматривала 250, №20 — 1000, №100 — 5000.
func TestCost_DepthProfile(t *testing.T) {
	s, _ := newStore(t)
	const total, pageSize = 5000, 50
	seedChannel(t, s, "ch", total)
	ctx := context.Background()

	for _, depth := range []int{1, 5, 20, 100} {
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
			if last.OrderKey == curKey && last.ID == curID {
				t.Fatalf("страница №%d вернула ту же позицию — курсор не двигается", i+1)
			}
			curKey, curID = last.OrderKey, last.ID
		}
		snap := st.Snapshot()
		if len(snap.PerPage) == 0 {
			continue
		}
		p := snap.PerPage[len(snap.PerPage)-1]
		t.Logf("страница №%3d (глубина %5d записей): просмотрено %5d, выброшено %5d ради %d отданных",
			depth, depth*pageSize, p.Scanned, p.Skipped, p.Yielded)
		if p.Yielded > 0 && p.Scanned > pageSize {
			t.Fatalf("страница №%d просмотрела %d при размере %d", depth, p.Scanned, pageSize)
		}
	}
}

// Полный обход: суммарная стоимость линейна по числу записей, а не
// квадратична по числу страниц. Было 21.5 просмотра на одну отданную.
func TestCost_FullTraversalIsLinear(t *testing.T) {
	s, stats := newStore(t)
	const total, pageSize = 2000, 50
	seedChannel(t, s, "ch", total)

	ctx := context.Background()
	var curKey int64
	var curID string
	seen := 0
	const maxPages = total/pageSize + 10
	pages := 0
	for {
		if pages > maxPages {
			t.Fatalf("более %d страниц — курсор не двигается", maxPages)
		}
		pages++
		page, err := s.Page(ctx, "ch", curKey, curID, pageSize)
		if err != nil {
			t.Fatalf("Page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		seen += len(page)
		last := page[len(page)-1]
		curKey, curID = last.OrderKey, last.ID
	}
	if seen != total {
		t.Fatalf("обход покрыл %d из %d", seen, total)
	}
	snap := stats.Snapshot()
	t.Logf("полный обход %d записей: просмотрено %d, отдано %d → %.2f на одну (было 21.50)",
		total, snap.Scanned, snap.Yielded, float64(snap.Scanned)/float64(snap.Yielded))
	if snap.Scanned > total+total/pageSize+1 {
		t.Fatalf("полный обход просмотрел %d при %d записях — рост не линейный", snap.Scanned, total)
	}
}
