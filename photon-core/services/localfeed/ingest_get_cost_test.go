package localfeed

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/wiloris/photon-core/ports"
)

// ЦЕНА ДОПОЛНИТЕЛЬНОГО Get — последний неизмеренный пункт четвёртого среза.
//
// Названа как минус в ADR-053 («Ingest теперь читает существующую запись перед
// записью») и оставлена открытой в ROADMAP. Меряется БЛОКОМ, а не лентой:
// Get выполняется по одному на каждую запись блока, и от длины ленты зависит
// только логарифмически (глубина B-дерева).
//
// Из чего состоит замеряемое (по коду adapters/bbolt и codec.go):
//   промах (записи нет)  — спуск по B-дереву и ErrNotFound, без копий;
//   попадание            — спуск + КОПИЯ значения (Get копирует: срезы bbolt
//                          живут только внутри транзакции) + decode, то есть
//                          json.Unmarshal и ещё одна копия Body.
//
// Отсюда два режима, а не один: догрузка канала — сплошные промахи,
// повторный опрос той же страницы — сплошные попадания.

// ingestWithoutGet — копия транзакции Ingest БЕЗ чтения существующей записи.
//
// ЭТО НЕ ПРЕДЛОЖЕНИЕ И НЕ АЛЬТЕРНАТИВА, а база отсчёта: именно так Ingest
// работал ДО четвёртого среза, и именно поэтому переупорядоченная доставка
// молча откатывала запись к прежнему телу (ADR-053). Что теряется без Get,
// зафиксировано отдельным тестом ниже — чтобы измеренная цена читалась как
// «сколько стоит согласование версий», а не как «сколько можно сэкономить».
//
// Копия обязана отличаться от Ingest РОВНО на Get: остальное — те же
// loadSegments, applyBlock, encode, Put, countVanished, запись сегментов и тот
// же счётчик статистики. Что она не разошлась, проверяет
// TestIngestWithoutGetIsFaithfulReplica: расхождение сделало бы «дешевле» по
// постороннй причине — классический дефект измерителя.
func ingestWithoutGet(tb testing.TB, s *Store, channelID string, b Block) {
	tb.Helper()
	err := s.st.Update(context.Background(), func(tx ports.WriteTx) error {
		before, err := loadSegments(tx, channelID)
		if err != nil {
			return err
		}
		after, err := applyBlock(before, b, b.Records)
		if err != nil {
			return err
		}
		for _, r := range b.Records {
			v, err := encode(r)
			if err != nil {
				return err
			}
			if err := tx.Put(Namespace, key(channelID, r.OrderKey, r.ID), v); err != nil {
				return err
			}
			s.stats.countWrite()
		}
		if _, err := countVanished(tx, channelID, b); err != nil {
			return err
		}
		enc, err := encodeSegments(after)
		if err != nil {
			return err
		}
		return tx.Put(Namespace, segmentsKey(channelID), enc)
	})
	if err != nil {
		tb.Fatalf("ingestWithoutGet: %v", err)
	}
}

// dumpNamespace — всё содержимое пространства имён, ключ → значение.
func dumpNamespace(tb testing.TB, s *Store) map[string]string {
	tb.Helper()
	out := make(map[string]string)
	err := s.st.View(context.Background(), func(tx ports.ReadTx) error {
		return tx.ForEach(Namespace, func(k, v []byte) error {
			out[string(k)] = string(v)
			return nil
		})
	})
	if err != nil {
		tb.Fatalf("dump: %v", err)
	}
	return out
}

func sameDump(a, b map[string]string) (string, bool) {
	if len(a) != len(b) {
		return fmt.Sprintf("ключей %d против %d", len(a), len(b)), false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return fmt.Sprintf("ключа %q нет во втором", k), false
		}
		if va != vb {
			return fmt.Sprintf("значение %q: %q против %q", k, va, vb), false
		}
	}
	return "", true
}

// База отсчёта обязана отличаться от Ingest ТОЛЬКО отсутствием Get.
//
// Проверяется не по глазам, а сравнением всего содержимого хранилища после
// одинаковых входов: пропусти копия запись сегментов или countVanished — она
// оказалась бы дешевле по посторонней причине, и «цена Get» вобрала бы в себя
// эту разницу.
func TestIngestWithoutGetIsFaithfulReplica(t *testing.T) {
	ctx := context.Background()
	seed := []Record{recv("a", 100, 100, `{"m":"a"}`), recv("b", 200, 200, `{"m":"b"}`)}
	// Блок: одна уже известная запись с ТОЙ ЖЕ версией и одна новая.
	blk := newestBlock(recv("b", 200, 200, `{"m":"b"}`), recv("c", 300, 300, `{"m":"c"}`))

	real1, _ := newStore(t)
	replica, _ := newStore(t)
	for _, s := range []*Store{real1, replica} {
		if err := s.Put(ctx, "ch", seed); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if _, err := real1.Ingest(ctx, "ch", blk); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	ingestWithoutGet(t, replica, "ch", blk)

	if why, ok := sameDump(dumpNamespace(t, real1), dumpNamespace(t, replica)); !ok {
		t.Fatalf("копия разошлась с Ingest: %s — замер приписал бы эту разницу цене Get", why)
	}
}

// Что именно покупается измеряемой ценой.
//
// Без Get блок со СТАРОЙ версией затирает более свежее локальное тело — ровно
// та потеря, ради которой Get и появился (ADR-053). Тест держит базу отсчёта
// в её роли: она база, а не вариант.
func TestIngestWithoutGetLosesStaleProtection(t *testing.T) {
	ctx := context.Background()
	fresh := recv("a", 100, 250, `{"m":"v2"}`)
	stale := recv("a", 100, 100, `{"m":"v1"}`)

	withGet, _ := newStore(t)
	if _, err := withGet.Ingest(ctx, "ch", newestBlock(fresh)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	res, err := withGet.Ingest(ctx, "ch", newestBlock(stale))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Stale != 1 {
		t.Fatalf("Stale=%d, ожидалась 1", res.Stale)
	}
	if got := string(onlyRecord(t, withGet, "a").Body); got != `{"m":"v2"}` {
		t.Fatalf("с Get тело стало %s — откат не предотвращён", got)
	}

	noGet, _ := newStore(t)
	ingestWithoutGet(t, noGet, "ch", newestBlock(fresh))
	ingestWithoutGet(t, noGet, "ch", newestBlock(stale))
	if got := string(onlyRecord(t, noGet, "a").Body); got != `{"m":"v1"}` {
		t.Fatalf("без Get тело %s — тест перестал показывать потерю, ради которой Get и введён", got)
	}
}

// ── замер ────────────────────────────────────────────────────────────────
//
// В обычный `go test ./...` не входит. Запуск:
//
//	go test ./services/localfeed -run '^$' -count 1 -benchmem \
//	    -bench 'IngestGetCost' -benchtime 20x
//
// -count 1 обязателен: кэш результатов уже один раз выдавал прошлый вывод за
// новый прогон (feed-reconcile.md §10.7).

const getCostPage = 50

// topBlockAt — блок из page записей ленты, начиная со смещения off сверху.
func topBlockAt(n, off, page int, version func(order int64) int64) Block {
	recs := make([]Record, 0, page)
	for i := n - 1 - off; i >= 0 && len(recs) < page; i-- {
		order := int64(1_000_000 + i)
		recs = append(recs, Record{ID: fmt.Sprintf("p%06d", i), OrderKey: order,
			Version: version(order), Body: feedRec(i).Body})
	}
	return Block{Records: recs, Dir: DirNewest}
}

func sameVersion(order int64) int64  { return order }     // как лежит локально
func staleVersion(order int64) int64 { return order - 1 } // младше локальной

// freshBlock — блок записей, которых в ленте НЕТ: OrderKey выше верхушки.
func freshBlock(n, round, page int) Block {
	recs := make([]Record, 0, page)
	base := 2_000_000 + int64(round*page)
	for j := page - 1; j >= 0; j-- {
		order := base + int64(j)
		recs = append(recs, Record{ID: fmt.Sprintf("n%08d", order), OrderKey: order,
			Version: order, Body: []byte(`{"m":"new"}`)})
	}
	return Block{Records: recs, Dir: DirNewest}
}

// BenchmarkIngestGetCost — Ingest как он есть против той же транзакции без Get.
//
// Сценарии различают два режима Get, а не оттенки одного:
//
//	hit-unchanged — повторный опрос той же страницы: все 50 попаданий,
//	                у каждого decode; версия та же, запись всё равно кладётся;
//	hit-stale     — пришло старьё: Get попадает, decode выполняется, а вот
//	                Put НЕ выполняется — единственный случай, где Get экономит
//	                работу, а не добавляет её;
//	miss-new      — догрузка: ни одной записи блока локально нет, значит
//	                copy и decode не выполняются вовсе, остаётся только спуск
//	                по дереву.
func BenchmarkIngestGetCost(b *testing.B) {
	ctx := context.Background()

	type variant struct {
		name string
		run  func(s *Store, blk Block)
	}

	for _, n := range []int{1000, 10000, 100000} {
		// hit-режимы: состояние установившееся, лента не растёт.
		for _, sc := range []struct {
			name string
			blk  Block
		}{
			{"hit-unchanged", topBlockAt(n, 0, getCostPage, sameVersion)},
			{"hit-stale", topBlockAt(n, 0, getCostPage, staleVersion)},
		} {
			for _, v := range []variant{
				{"with-get", func(s *Store, blk Block) {
					if _, err := s.Ingest(ctx, "ch", blk); err != nil {
						b.Fatalf("Ingest: %v", err)
					}
				}},
				{"no-get", func(s *Store, blk Block) { ingestWithoutGet(b, s, "ch", blk) }},
			} {
				b.Run(fmt.Sprintf("%s/feed=%d/%s", sc.name, n, v.name), func(b *testing.B) {
					s, _ := newStore(b)
					seedChannel(b, s, "ch", n)
					warmUntilStable(b, func() { v.run(s, sc.blk) })
					b.ReportAllocs()
					runtime.GC()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						v.run(s, sc.blk)
					}
					b.StopTimer()
					b.ReportMetric(float64(len(sc.blk.Records)), "block")
				})
			}
		}
	}

	// miss-режим меряется ТОЛЬКО на ленте 100 000 и с ограниченным разогревом.
	//
	// Причина названа, а не обойдена: промах невозможно повторить на месте —
	// Ingest записывает блок, и со второго раза это уже попадание. Значит
	// каждая итерация обязана приносить НОВЫЕ записи, то есть лента растёт.
	// На 100 000 разогрев 20 + замер 20 добавляют 2 000 записей — 2 %, что
	// глубину B-дерева не меняет; на ленте 1 000 та же процедура утроила бы
	// её и мерила бы уже другую ленту.
	for _, v := range []string{"with-get", "no-get"} {
		b.Run(fmt.Sprintf("miss-new/feed=100000/%s", v), func(b *testing.B) {
			const n = 100000
			s, _ := newStore(b)
			seedChannel(b, s, "ch", n)
			rampCPU()
			round := 0
			step := func() {
				blk := freshBlock(n, round, getCostPage)
				round++
				if v == "with-get" {
					if _, err := s.Ingest(ctx, "ch", blk); err != nil {
						b.Fatalf("Ingest: %v", err)
					}
					return
				}
				ingestWithoutGet(b, s, "ch", blk)
			}
			for i := 0; i < 20; i++ { // разогрев фиксированным числом, см. выше
				step()
			}
			b.ReportAllocs()
			runtime.GC()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				step()
			}
			b.StopTimer()
			b.ReportMetric(float64(getCostPage), "block")
		})
	}
}

// Цена Get линейна по РАЗМЕРУ БЛОКА, а не по ленте: Get выполняется по одному
// на запись блока. Проверяется разверткой блока на одной и той же ленте.
func BenchmarkIngestGetCostByBlockSize(b *testing.B) {
	ctx := context.Background()
	const n = 10000
	for _, page := range []int{50, 200, 1000} {
		blk := topBlockAt(n, 0, page, sameVersion)
		for _, v := range []string{"with-get", "no-get"} {
			b.Run(fmt.Sprintf("hit-unchanged/block=%d/%s", page, v), func(b *testing.B) {
				s, _ := newStore(b)
				seedChannel(b, s, "ch", n)
				run := func() {
					if v == "with-get" {
						if _, err := s.Ingest(ctx, "ch", blk); err != nil {
							b.Fatalf("Ingest: %v", err)
						}
						return
					}
					ingestWithoutGet(b, s, "ch", blk)
				}
				warmUntilStable(b, run)
				b.ReportAllocs()
				runtime.GC()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					run()
				}
				b.StopTimer()
				b.ReportMetric(float64(len(blk.Records)), "block")
			})
		}
	}
}
