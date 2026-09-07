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
		pg, err := s.Page(ctx, "ch", curKey, curID, pageSize)
		if err != nil {
			t.Fatalf("Page: %v", err)
		}
		if len(pg.Records) == 0 {
			break
		}
		pages++
		last := pg.Records[len(pg.Records)-1]
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
			pg, err := probe.Page(ctx, "ch", curKey, curID, pageSize)
			if err != nil {
				t.Fatalf("Page: %v", err)
			}
			if len(pg.Records) == 0 {
				break
			}
			last := pg.Records[len(pg.Records)-1]
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
		pg, err := s.Page(ctx, "ch", curKey, curID, pageSize)
		if err != nil {
			t.Fatalf("Page: %v", err)
		}
		if len(pg.Records) == 0 {
			break
		}
		seen += len(pg.Records)
		last := pg.Records[len(pg.Records)-1]
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

// ── стоимость догрузки (второй срез Phase 12) ────────────────────────────
//
// Локальные числа и только они. Сетевую задержку меряет cmd/feedslice
// отдельной колонкой: сумма двух величин не значила бы ни одной из них.

// Число сегментов растёт на каждое возвращение без закрытия пропуска — и это
// главный риск модели. Измеряется, а не предполагается.
func TestCost_SegmentsGrowPerUnfilledReopen(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	for reopen := 1; reopen <= 20; reopen++ {
		// каждое возвращение приносит блок, не смыкающийся с предыдущим
		base := int64(reopen * 1000)
		if _, err := s.Ingest(ctx, "ch", Block{
			Records: []Record{
				rec(fmt.Sprintf("r%02d-b", reopen), base+1),
				rec(fmt.Sprintf("r%02d-a", reopen), base),
			},
			Dir: DirNewest,
		}); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	segs, err := s.Segments(ctx, "ch")
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	t.Logf("20 возвращений без закрытия пропусков → сегментов: %d, пропусков: %d",
		len(segs), len(segs)-1)
	if len(segs) != 20 {
		t.Fatalf("сегментов %d, ожидалось 20 — слияние сработало там, где не должно", len(segs))
	}

	// Чтение при 20 пропусках: страница обязана остановиться на ПЕРВОМ
	// разрыве, а не просматривать всю ленту.
	st := &Stats{}
	probe := New(s.st, st)
	pg, err := probe.Page(ctx, "ch", 0, "", 50)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	snap := st.Snapshot()
	p := snap.PerPage[0]
	t.Logf("страница при 20 пропусках: просмотрено %d, отдано %d, разрыв=%v",
		p.Scanned, p.Yielded, pg.GapAfter)
	if !pg.GapAfter {
		t.Fatal("разрыв не показан")
	}
	// 2 отданных + 1 просмотренная за границей — обход не идёт дальше.
	if p.Scanned > 3 {
		t.Fatalf("просмотрено %d при 2 отданных — обход не остановился на границе", p.Scanned)
	}
}

// Закрытие пропусков ОБЯЗАНО уменьшать число сегментов до одного: иначе
// модель копила бы разрывы вечно.
func TestCost_FillingGapsCollapsesSegments(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	const pageSize = 10

	srv := newFakeServer(200)
	first := []Record{}
	for i := 4; i >= 0; i-- {
		first = append(first, srv.recs[i])
	}
	if _, err := s.Ingest(ctx, "ch", Block{Records: first, Dir: DirNewest}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	nb := srv.newest(pageSize)
	if _, err := s.Ingest(ctx, "ch", Block{Records: nb, Dir: DirNewest, AtEnd: false}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	steps := 0
	for {
		anchor, ok, err := s.GapAnchor(ctx, "ch")
		if err != nil {
			t.Fatalf("GapAnchor: %v", err)
		}
		if !ok {
			break
		}
		steps++
		if steps > 100 {
			t.Fatal("цикл не сходится")
		}
		block := srv.after(anchor.ID, pageSize)
		if _, err := s.Ingest(ctx, "ch", Block{
			Records: block, Anchor: &anchor, Dir: DirAfter, AtEnd: len(block) < pageSize,
		}); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	segs, _ := s.Segments(ctx, "ch")
	t.Logf("200 записей, пропуск шириной 185, страница %d → закрыт за %d заливок, сегментов: %d",
		pageSize, steps, len(segs))
	if len(segs) != 1 {
		t.Fatalf("после закрытия сегментов %d, ожидался 1", len(segs))
	}
	// Заливок ровно столько, сколько страниц в пропуске: ни одна не впустую.
	if steps > 20 {
		t.Fatalf("заливок %d при пропуске в 185 записей и странице %d — есть холостые",
			steps, pageSize)
	}
}

// Стоимость Ingest при растущем числе сегментов: список разбирается целиком,
// значит O(#сегментов). Проверяется, что число сегментов, а не записей.
func TestCost_IngestScalesWithSegmentsNotRecords(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	// 5000 записей одним непрерывным сегментом
	var big []Record
	for i := 0; i < 5000; i++ {
		big = append(big, rec(fmt.Sprintf("b%04d", i), int64(10_000+i)))
	}
	if _, err := s.Ingest(ctx, "ch", Block{Records: big, Dir: DirNewest}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	res, err := s.Ingest(ctx, "ch", Block{
		Records: []Record{rec("top", 999_999)}, Dir: DirNewest,
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	t.Logf("лента 5000 записей, 2 сегмента: Ingest разобрал список из %d элементов",
		res.Segments)
	if res.Segments != 2 {
		t.Fatalf("сегментов %d, ожидалось 2", res.Segments)
	}
}
