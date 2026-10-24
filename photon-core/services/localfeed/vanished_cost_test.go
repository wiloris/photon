package localfeed

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
)

// СТОИМОСТЬ countVanished — открытый пункт Phase 12, названный в ADR-053 и в
// docs/feed-reconcile.md §9.3: «не измерено — обход префикса канала на каждую
// заливку».
//
// Меряется то, чего Stats не видит: Stats считает только Page (countRead), у
// countVanished своего счётчика нет, и добавлять его в production ради замера
// нельзя — baseline снимается с кода как он есть.
//
// Вопрос замера сформулирован так, чтобы на него можно было ответить НЕТ:
// зависит ли стоимость от ширины блока и ширины покрытого диапазона, или
// только от размера всей ленты.

// countingTx — обёртка ports.ReadTx, считающая, сколько пар обход отдал в
// callback.
//
// Обёрнуты ОБА обхода, а не только используемый сегодня ForEachPrefix:
// измеритель, слепой к ForEachPrefixFrom, после перехода на seek показал бы
// «просмотрено 0» и был бы принят за улучшение. Это ровно тот дефект
// измерителя, что находили прогоны Phase 13.5 и Media MVP.
type countingTx struct {
	ports.ReadTx
	scanned int
}

func (c *countingTx) ForEachPrefix(ns string, p []byte, fn func(k, v []byte) error) error {
	return c.ReadTx.ForEachPrefix(ns, p, func(k, v []byte) error {
		c.scanned++
		return fn(k, v)
	})
}

func (c *countingTx) ForEachPrefixFrom(ns string, p, start []byte, fn func(k, v []byte) error) error {
	return c.ReadTx.ForEachPrefixFrom(ns, p, start, func(k, v []byte) error {
		c.scanned++
		return fn(k, v)
	})
}

// feedRec — запись ленты в той же раскладке, что кладёт seedChannel:
// индекс 0 — самая старая, n-1 — самая свежая.
func feedRec(i int) Record { return rec(fmt.Sprintf("p%06d", i), int64(1_000_000+i)) }

// vanishedCase — блок и то, что он ОБЯЗАН намерить.
//
// wantVanished хранится рядом с блоком намеренно: сценарий, в котором
// исчезнувших нет, измерил бы стоимость не того случая и выглядел бы
// нормально.
type vanishedCase struct {
	name string
	desc string
	blk  Block

	wantVanished int // локальных записей внутри диапазона, отсутствующих в блоке
	wantRange    int // сколько локальных записей покрывает диапазон блока
}

// vanishedCases — три случая на ленте из n записей.
//
//	top      — обычная догрузка: 50 самых свежих, ничего не исчезло;
//	page-half— страница 50 поверх диапазона, где исчезла половина: диапазон
//	           узкий, исчезнувших много;
//	wide-half— worst case: блок растянут на ВСЮ ленту и половина записей в
//	           него не пришла.
//
// Пара (узкий диапазон, много исчезнувших) и (широкий диапазон, много
// исчезнувших) разделены специально: без неё нельзя отличить «стоимость
// растёт от числа исчезнувших» от «растёт от ширины диапазона».
func vanishedCases(n int) []vanishedCase {
	const page = 50

	top := make([]Record, 0, page)
	for i := n - 1; i >= 0 && len(top) < page; i-- {
		top = append(top, feedRec(i))
	}

	// Через одну, начиная с самой свежей: диапазон покрывает 2*page-1
	// локальных позиций, в блок пришла ровно половина.
	half := make([]Record, 0, page)
	for i := n - 1; i >= 0 && len(half) < page; i -= 2 {
		half = append(half, feedRec(i))
	}
	halfRange := 2*len(half) - 1

	// Через одну по всей ленте, обе границы включены.
	wide := make([]Record, 0, n/2+2)
	for i := n - 1; i >= 0; i -= 2 {
		wide = append(wide, feedRec(i))
	}
	if wide[len(wide)-1].ID != feedRec(0).ID {
		wide = append(wide, feedRec(0)) // граница диапазона — самая старая запись
	}

	return []vanishedCase{
		{name: "top", desc: fmt.Sprintf("%d свежих, всё на месте", len(top)),
			blk: Block{Records: top, Dir: DirNewest}, wantVanished: 0, wantRange: len(top)},
		{name: "page-half", desc: fmt.Sprintf("страница %d, диапазон %d, исчезла половина", len(half), halfRange),
			blk: Block{Records: half, Dir: DirNewest}, wantVanished: halfRange - len(half), wantRange: halfRange},
		{name: "wide-half", desc: fmt.Sprintf("блок %d на всю ленту, исчезла половина", len(wide)),
			blk: Block{Records: wide, Dir: DirNewest}, wantVanished: n - len(wide), wantRange: n},
	}
}

// runCountVanished — один вызов в собственной read-транзакции, со счётчиком.
//
// Транзакция открывается на каждый вызов, как и на каждую заливку: Ingest
// зовёт countVanished внутри своей. Стоимость открытия входит в замер
// намеренно — в production она тоже входит.
func runCountVanished(tb testing.TB, s *Store, ch string, b Block) (vanished, scanned int) {
	tb.Helper()
	if len(b.Records) == 0 {
		tb.Fatal("блок пуст: countVanished выйдет по короткому пути и не измерит ничего")
	}
	err := s.st.View(context.Background(), func(tx ports.ReadTx) error {
		c := &countingTx{ReadTx: tx}
		v, err := countVanished(c, ch, b)
		vanished, scanned = v, c.scanned
		return err
	})
	if err != nil {
		tb.Fatalf("countVanished: %v", err)
	}
	return vanished, scanned
}

// plainCountVanished — то же самое без счётчика: обёртка сама аллоцирует, и в
// b.N-цикле её аллокации попали бы в allocs/op как аллокации измеряемого кода.
func plainCountVanished(tb testing.TB, s *Store, ch string, b Block) int {
	tb.Helper()
	var n int
	err := s.st.View(context.Background(), func(tx ports.ReadTx) error {
		var err error
		n, err = countVanished(tx, ch, b)
		return err
	})
	if err != nil {
		tb.Fatalf("countVanished: %v", err)
	}
	return n
}

// ГРАНИЦА ОБХОДА — то, что защищает исправление.
//
// До исправления здесь стояло обратное утверждение: «просмотрено = вся лента,
// всегда». Оно было верным, измеренным (feed-reconcile.md §10) и записано
// именно как наблюдённое поведение, с пометкой, что при переходе на обход по
// диапазону тест упадёт и должен быть перевёрнут вместе с кодом. Ровно это и
// произошло — так же, как с TestCost_PageScanIsConstantRegardlessOfDepth при
// ADR-047. Прежняя редакция не «починена», а перевёрнута: она мерила проблему,
// эта доказывает её отсутствие.
//
// ПОЧЕМУ НЕ ХВАТИЛО БЫ ПРОВЕРКИ ЧИСЛА ИСЧЕЗНУВШИХ. Полный обход даёт РОВНО ТОТ
// ЖЕ Vanished, что и обход по диапазону, — это проверено мутацией и записано в
// §10.6. Значит возврат к полному обходу по результату не виден вообще: его
// видно только по объёму обхода, и меряет его countingTx.

// scanBounds — сколько записей обход ОБЯЗАН посмотреть.
//
// Нижняя граница: каждую запись диапазона надо увидеть, иначе исчезнувшая
// осталась бы незамеченной. Верхняя: диапазон плюс не более двух записей —
// одна за нижней границей (на ней обход и обрывается, и её просмотр как раз
// доказывает, что обрыв случился) и запас на соседей по равному OrderKey.
const scanSlack = 2

func TestCost_CountVanishedVisitsOnlyCoveredRange(t *testing.T) {
	const n = 2000
	s, _ := newStore(t)
	seedChannel(t, s, "ch", n)

	// Блок В СЕРЕДИНЕ ленты: под ним ~1000 записей и над ним ~950. Полный
	// обход прошёл бы и те и другие; обход по диапазону не должен ни одной.
	const from, size = 1000, 50
	mid := make([]Record, 0, size)
	for i := from + size - 1; i >= from; i-- {
		mid = append(mid, feedRec(i))
	}
	blk := Block{Records: mid, Dir: DirNewest}

	v, scanned := runCountVanished(t, s, "ch", blk)
	t.Logf("лента %d, блок %d в середине (записи %d..%d): просмотрено %d, исчезло %d",
		n, size, from, from+size-1, scanned, v)

	if v != 0 {
		t.Fatalf("исчезнувших %d при полном блоке — сценарий измеряет не то, что заявляет", v)
	}
	if scanned < size {
		t.Fatalf("просмотрено %d при диапазоне %d — обход не увидел часть диапазона, "+
			"исчезнувшие внутри него остались бы незамеченными", scanned, size)
	}
	if scanned > size+scanSlack {
		t.Fatalf("просмотрено %d при диапазоне %d (лента %d) — обход вышел за диапазон; "+
			"полный обход префикса вернулся", scanned, size, n)
	}
}

// ОБЪЁМ ОБХОДА НЕ ЗАВИСИТ ОТ РАЗМЕРА ЛЕНТЫ. Это и есть тот тест, который
// ловит возврат к полному prefix scan: блок один и тот же, лента растёт в 200
// раз, просмотр обязан остаться прежним. Полный обход дал бы 100 / 1 000 /
// 5 000 / 20 000 — то есть провалил бы проверку на первом же шаге.
//
// Блок взят из САМЫХ СТАРЫХ записей намеренно: с ростом ленты записи
// прибавляются НАД ним, и обход, начинающийся с начала префикса, обязан их
// пройти. Блок сверху такую ошибку скрыл бы.
func TestCost_CountVanishedScanDoesNotGrowWithFeedSize(t *testing.T) {
	const size = 50
	blk := Block{Dir: DirNewest}
	for i := size - 1; i >= 0; i-- {
		blk.Records = append(blk.Records, feedRec(i))
	}

	first := -1
	for _, n := range []int{100, 1000, 5000, 20000} {
		s, _ := newStore(t)
		seedChannel(t, s, "ch", n)
		v, scanned := runCountVanished(t, s, "ch", blk)
		t.Logf("лента %6d записей, тот же блок %d → просмотрено %4d, исчезло %d", n, size, scanned, v)
		if v != 0 {
			t.Fatalf("лента %d: исчезнувших %d при полном блоке", n, v)
		}
		if scanned > size+scanSlack {
			t.Fatalf("лента %d: просмотрено %d при блоке %d — обход зависит от размера ленты, "+
				"полный обход префикса вернулся", n, scanned, size)
		}
		if first < 0 {
			first = scanned
			continue
		}
		if scanned != first {
			t.Fatalf("лента %d: просмотрено %d против %d на ленте 100 — объём обхода "+
				"перестал быть постоянным", n, scanned, first)
		}
	}
}

// Обход обязан ОБРЫВАТЬСЯ за нижней границей, а не доходить до конца ленты.
//
// Отдельно от проверки выше: там блок лежал в середине и обе ошибки — «пошёл
// сверху» и «не остановился снизу» — дали бы один и тот же симптом. Здесь блок
// взят у самой свежей верхушки, поэтому сверху отсекать нечего: превышение
// может значить только одно — обход не остановился, пройдя lo.
func TestCost_CountVanishedStopsBelowOldestBoundary(t *testing.T) {
	const n, size = 5000, 50
	s, _ := newStore(t)
	seedChannel(t, s, "ch", n)

	blk := Block{Dir: DirNewest}
	for i := n - 1; i >= n-size; i-- {
		blk.Records = append(blk.Records, feedRec(i))
	}
	_, scanned := runCountVanished(t, s, "ch", blk)
	t.Logf("лента %d, блок %d с верхушки: просмотрено %d (ниже диапазона лежит %d записей)",
		n, size, scanned, n-size)
	if scanned > size+scanSlack {
		t.Fatalf("просмотрено %d при диапазоне %d — обход не остановился за нижней границей "+
			"и ушёл в оставшиеся %d записей", scanned, size, n-size)
	}
}

// ── тот же ответ, что у полного обхода ──────────────────────────────────
//
// Оракул — НЕЗАВИСИМАЯ реализация по определению из ADR-053/054: пройти всю
// ленту, отобрать записи внутри покрытого диапазона, посчитать отсутствующие
// в блоке. Это буквально прежний алгоритм. Сужение обхода не имеет права
// менять ответ ни в одном случае, и проверяется это сравнением с оракулом, а
// не переписанными вручную ожиданиями: ожидания, выведенные из новой
// реализации, подтвердили бы сами себя.

func countVanishedNaive(tb testing.TB, s *Store, ch string, b Block) int {
	tb.Helper()
	if len(b.Records) == 0 {
		return 0
	}
	present := make(map[string]struct{}, len(b.Records))
	lo := Position{OrderKey: b.Records[0].OrderKey, ID: b.Records[0].ID}
	hi := lo
	for _, r := range b.Records {
		present[r.ID] = struct{}{}
		p := Position{OrderKey: r.OrderKey, ID: r.ID}
		lo = oldest(lo, p)
		hi = newest(hi, p)
	}
	var n int
	err := s.st.View(context.Background(), func(tx ports.ReadTx) error {
		return tx.ForEachPrefix(Namespace, prefix(ch), func(_, v []byte) error {
			r, err := decode(v)
			if err != nil {
				return err
			}
			p := Position{OrderKey: r.OrderKey, ID: r.ID}
			if p.newerThan(hi) || lo.newerThan(p) {
				return nil
			}
			if _, ok := present[r.ID]; !ok {
				n++
			}
			return nil
		})
	})
	if err != nil {
		tb.Fatalf("оракул: %v", err)
	}
	return n
}

func TestCountVanishedAgreesWithFullScanOracle(t *testing.T) {
	ctx := context.Background()

	// Лента сценария: пять записей с УНИКАЛЬНЫМ OrderKey и три с общим —
	// равный OrderKey не краевой случай, а норма (create_at в миллисекундах).
	feed := []Record{
		rec("a", 100), rec("b", 200), rec("c", 300),
		rec("e1", 400), rec("e2", 400), rec("e3", 400),
		rec("d", 500), rec("f", 600),
	}
	// Порядок обхода, свежие первыми: f(600) d(500) e1 e2 e3(400) c b a.

	cases := []struct {
		name string
		blk  Block
	}{
		{"пустой блок", Block{Dir: DirNewest}},
		{"одна запись", Block{Records: []Record{rec("c", 300)}, Dir: DirNewest}},
		{"одна запись, вокруг неё исчезли соседи",
			Block{Records: []Record{rec("d", 500)}, Dir: DirNewest}},
		{"равный OrderKey: пришли все три",
			Block{Records: []Record{rec("e1", 400), rec("e2", 400), rec("e3", 400)}, Dir: DirNewest}},
		{"равный OrderKey: средняя исчезла",
			Block{Records: []Record{rec("e1", 400), rec("e3", 400)}, Dir: DirNewest}},
		{"равный OrderKey: границей стала средняя из трёх",
			Block{Records: []Record{rec("d", 500), rec("e2", 400)}, Dir: DirNewest}},
		{"верхняя граница ленты",
			Block{Records: []Record{rec("f", 600), rec("d", 500)}, Dir: DirNewest}},
		{"нижняя граница ленты",
			Block{Records: []Record{rec("b", 200), rec("a", 100)}, Dir: DirNewest}},
		{"вся лента через одну (wide-half)",
			Block{Records: []Record{rec("f", 600), rec("e1", 400), rec("c", 300), rec("a", 100)}, Dir: DirNewest}},
		{"несколько исчезнувших подряд",
			Block{Records: []Record{rec("f", 600), rec("a", 100)}, Dir: DirNewest}},
		{"диапазон шире ленты с обеих сторон",
			Block{Records: []Record{rec("top", 9000), rec("bottom", 1)}, Dir: DirNewest}},
		{"диапазон целиком выше ленты",
			Block{Records: []Record{rec("t1", 9000), rec("t2", 8000)}, Dir: DirNewest}},
		{"диапазон целиком ниже ленты",
			Block{Records: []Record{rec("z1", 10), rec("z2", 5)}, Dir: DirNewest}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _ := newStore(t)
			if err := s.Put(ctx, "ch", feed); err != nil {
				t.Fatalf("Put: %v", err)
			}
			want := countVanishedNaive(t, s, "ch", c.blk)
			var got, scanned int
			if len(c.blk.Records) == 0 {
				got = plainCountVanished(t, s, "ch2empty", c.blk) // короткий путь
			} else {
				got, scanned = runCountVanished(t, s, "ch", c.blk)
			}
			t.Logf("%-40s → исчезло %d (оракул %d), просмотрено %d из %d",
				c.name, got, want, scanned, len(feed))
			if got != want {
				t.Fatalf("исчезнувших %d, оракул полного обхода даёт %d — сужение обхода "+
					"изменило ОТВЕТ, а не только цену", got, want)
			}
			if scanned > len(feed) {
				t.Fatalf("просмотрено %d при ленте %d", scanned, len(feed))
			}
		})
	}
}

// Удалённая запись НА ГРАНИЦЕ диапазона: граница блока — живая запись, а
// исчезла соседняя с ней внутри диапазона. Случай назван отдельно, потому что
// именно на границах обход теперь стартует и обрывается: ошибка в строгости
// сравнения (> вместо >=) съела бы ровно одну запись — крайнюю.
func TestCountVanishedCountsRecordAdjacentToBoundary(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	feed := []Record{rec("a", 100), rec("b", 200), rec("c", 300), rec("d", 400), rec("e", 500)}
	if err := s.Put(ctx, "ch", feed); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Блок объявляет полноту от e(500) до b(200); внутри исчезли d и c.
	blk := Block{Records: []Record{rec("e", 500), rec("b", 200)}, Dir: DirNewest}
	got, scanned := runCountVanished(t, s, "ch", blk)
	want := countVanishedNaive(t, s, "ch", blk)
	t.Logf("границы e..b, внутри исчезли d и c → %d (оракул %d), просмотрено %d", got, want, scanned)
	if got != 2 {
		t.Fatalf("исчезнувших %d, ожидалось 2 (d и c)", got)
	}
	if got != want {
		t.Fatalf("оракул даёт %d", want)
	}
	// a лежит НИЖЕ нижней границы и исчезнувшей не считается — обход обязан
	// был её увидеть и отбросить либо не дойти до неё вовсе, но не посчитать.
	if scanned > 5 {
		t.Fatalf("просмотрено %d при ленте 5", scanned)
	}
}

// ── числа: bench ─────────────────────────────────────────────────────────
//
// В обычный `go test ./...` не входит. Запуск:
//
//	go test ./services/localfeed -run '^$' -count 1 -benchmem \
//	    -bench 'CountVanished|IngestOnFeed' -benchtime 20x
//	go test ./services/localfeed -run '^$' -count 1 -benchmem \
//	    -bench 'CatchupBackwards' -benchtime 3x
//
// -count 1 обязателен: без него повторный запуск отдаёт закэшированный вывод
// предыдущего, и замер, который ничего не запускал, выглядит подтверждением.
//
// scanned/op и vanished выводятся отдельными метриками: ns/op без них не
// отличает «дорого, потому что лента большая» от «дорого, потому что
// исчезнувших много».

var benchSizes = []int{100, 1000, 10000, 100000}

func BenchmarkCountVanished(b *testing.B) {
	for _, n := range benchSizes {
		s, _ := newStore(b)
		seedChannel(b, s, "ch", n)
		for _, c := range vanishedCases(n) {
			b.Run(fmt.Sprintf("n=%d/%s", n, c.name), func(b *testing.B) {
				v, scanned := runCountVanished(b, s, "ch", c.blk)
				if v != c.wantVanished {
					b.Fatalf("исчезнувших %d, ожидалось %d", v, c.wantVanished)
				}
				warmUntilStable(b, func() { sinkInt = plainCountVanished(b, s, "ch", c.blk) })
				b.ReportAllocs()
				runtime.GC()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					sinkInt = plainCountVanished(b, s, "ch", c.blk)
				}
				b.StopTimer()
				b.ReportMetric(float64(scanned), "scanned/op")
				b.ReportMetric(float64(v), "vanished")
				b.ReportMetric(float64(len(c.blk.Records)), "block")
			})
		}
	}
}

// Полный Ingest на ленте того же размера — ради интерпретации: доля
// countVanished в заливке видна только из сравнения с целым.
//
// Блок повторяется идентичным, поэтому лента не растёт: все записи приходят
// с той же версией и считаются Unchanged, сегмент сливается сам с собой.
func BenchmarkIngestOnFeed(b *testing.B) {
	for _, n := range benchSizes {
		s, _ := newStore(b)
		seedChannel(b, s, "ch", n)
		c := vanishedCases(n)[0]
		b.Run(fmt.Sprintf("n=%d/top", n), func(b *testing.B) {
			ctx := context.Background()
			res, err := s.Ingest(ctx, "ch", c.blk)
			if err != nil {
				b.Fatalf("Ingest: %v", err)
			}
			if res.Vanished != 0 || res.Segments != 1 {
				b.Fatalf("исчезло %d, сегментов %d — сценарий не в стабильном состоянии",
					res.Vanished, res.Segments)
			}
			warmUntilStable(b, func() {
				if _, err := s.Ingest(ctx, "ch", c.blk); err != nil {
					b.Fatalf("Ingest: %v", err)
				}
			})
			b.ReportAllocs()
			runtime.GC()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := s.Ingest(ctx, "ch", c.blk); err != nil {
					b.Fatalf("Ingest: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(c.blk.Records)), "block")
		})
	}
}

// sinkInt не даёт компилятору счесть результат неиспользуемым.
var sinkInt int

// РАЗОГРЕВ ПРОЦЕССОРА — не гигиена, а условие осмысленности чисел.
//
// Дефект измерителя, найденный первым же прогоном и исправленный здесь.
// Симптом: на ленте 100 000 первый прогон давал 293 мс, второй и третий в том
// же процессе — 146 мс, стабильно; при GOGC=off картина та же. Покадровый
// замер (40 обходов подряд) показал ступеньку: до 27-го обхода ≈296 мс, после
// 35-го ≈145 мс, heapSys и NumGC при этом ровные. Отношение 296/146 = 2.03
// совпало с отношением частот этой машины: 4280/2096 = 2.04 (governor
// powersave, amd-pstate active). То есть первые ~8 секунд нагрузки процессор
// работает на базовой частоте и только потом уходит в boost.
//
// Следствие для метода: любой замер короче ~10 с даёт число базовой частоты,
// а замер, идущий ВТОРЫМ в том же процессе, — число boost. Сравнивать их
// между собой нельзя, и именно так выглядела первая таблица: n=100000/top
// 272 мс против n=100000/wide-half 146 мс — «широкий блок вдвое дешевле
// узкого», чего быть не может.
//
// Второй дефект того же прогона: повторный `go test -run` вернул
// БАЙТ-В-БАЙТ тот же вывод — сработал кэш результатов тестов. Замер, который
// ничего не запускал, выглядел подтверждением. Отсюда -count=1 в команде
// запуска ниже.
var rampOnce sync.Once

func rampCPU() {
	rampOnce.Do(func() {
		deadline := time.Now().Add(12 * time.Second)
		x := 1.000001
		for time.Now().Before(deadline) {
			for i := 0; i < 1_000_000; i++ {
				x *= 1.0000001
				if x > 2 {
					x = 1.000001
				}
			}
		}
		sinkFloat = x
	})
}

var sinkFloat float64

// warmUntilStable — операция повторяется ДО включения таймера, пока стоимость
// не перестанет меняться: помимо частоты есть ещё page cache БД и рост кучи.
func warmUntilStable(b *testing.B, op func()) {
	b.Helper()
	rampCPU()
	// Критерий — ОТСУТСТВИЕ УЛУЧШЕНИЯ, а не совпадение соседних замеров.
	// Прежняя редакция сравнивала соседей и принимала плавный спад за
	// стабильность: каждый следующий обход был на 3 % быстрее предыдущего,
	// три подряд укладывались в допуск, разогрев выходил на середине спуска.
	// Так n=100000/top и остался 248 мс против 146 мс у соседних случаев.
	// minWarmUp — нижняя граница помимо критерия сходимости. Только что
	// засеянная лента на 100 000 оставляет ~50 МБ грязных страниц, и первые
	// секунды обход идёт наперегонки со сбросом на диск; спад при этом не
	// монотонный, и «улучшений больше нет» срабатывает посреди него. Без
	// нижней границы первый случай на 100 000 оставался 246 мс против 136 мс
	// у соседних.
	const (
		minWarmUp = 5 * time.Second
		maxWarmUp = 60 * time.Second
		tolerance = 0.05
		stableRun = 3
	)
	floor := time.Now().Add(minWarmUp)
	deadline := time.Now().Add(maxWarmUp)
	best := time.Duration(0)
	stable := 0
	for time.Now().Before(deadline) {
		t0 := time.Now()
		op()
		d := time.Since(t0)
		switch {
		case best == 0 || float64(d) < float64(best)*(1-tolerance):
			best, stable = d, 0 // ещё ускоряемся
		default:
			if d < best {
				best = d
			}
			stable++
			if stable >= stableRun && time.Now().After(floor) {
				return
			}
		}
	}
	b.Logf("разогрев не сошёлся за %v — числа этого случая сравнивать с осторожностью", maxWarmUp)
}

// ── догрузка целиком: где линейная стоимость перестаёт быть линейной ─────
//
// Отдельный замер, без которого вывод был бы неверным. Один вызов
// countVanished линеен по размеру ленты — само по себе это ещё ничего не
// говорит: лента ЧИТАЕТСЯ СТРАНИЦАМИ, и каждая страница платит за всю уже
// накопленную ленту. Сумма по странице даёт квадрат.
//
// Сценарий — обычная догрузка назад: страница за страницей, каждая следующая
// старше предыдущей, локальная лента растёт. Ровно то, что делает клиент,
// впервые открывший канал.
func catchupBackwards(tb testing.TB, s *Store, srv []Record, page int) int {
	tb.Helper()
	ctx := context.Background()
	var anchor *Position
	ingests := 0
	for i := 0; i < len(srv); i += page {
		hi := i + page
		if hi > len(srv) {
			hi = len(srv)
		}
		chunk := srv[i:hi]
		blk := Block{Records: chunk, Dir: DirNewest}
		if anchor != nil {
			blk = Block{Records: chunk, Anchor: anchor, Dir: DirBefore}
		}
		if _, err := s.Ingest(ctx, "ch", blk); err != nil {
			tb.Fatalf("Ingest: %v", err)
		}
		last := chunk[len(chunk)-1]
		p := Position{OrderKey: last.OrderKey, ID: last.ID}
		anchor = &p
		ingests++
	}
	return ingests
}

// serverFeed — «лента сервера», свежие первыми.
func serverFeed(n int) []Record {
	out := make([]Record, 0, n)
	for i := n - 1; i >= 0; i-- {
		out = append(out, feedRec(i))
	}
	return out
}

func BenchmarkCatchupBackwards(b *testing.B) {
	const page = 50
	for _, n := range []int{2000, 4000, 8000, 16000} {
		srv := serverFeed(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			rampCPU()
			// Разогрев одной полной догрузкой на отдельном хранилище: она же
			// проверяет, что сценарий доходит до конца и лента сходится в
			// ОДИН сегмент. Догрузка, распавшаяся на сегменты, мерила бы
			// другой случай.
			warm, _ := newStore(b)
			ing := catchupBackwards(b, warm, srv, page)
			segs, err := warm.Segments(context.Background(), "ch")
			if err != nil {
				b.Fatalf("Segments: %v", err)
			}
			if len(segs) != 1 {
				b.Fatalf("после догрузки сегментов %d, ожидался 1 — страницы не сомкнулись", len(segs))
			}
			if ing != n/page {
				b.Fatalf("заливок %d, ожидалось %d", ing, n/page)
			}
			b.ReportAllocs()
			runtime.GC()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				s, _ := newStore(b)
				b.StartTimer()
				catchupBackwards(b, s, srv, page)
			}
			b.StopTimer()
			b.ReportMetric(float64(ing), "ingests/op")
			b.ReportMetric(float64(page), "page")
		})
	}
}
