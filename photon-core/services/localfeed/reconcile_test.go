package localfeed

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

/*
Четвёртый вертикальный срез Phase 12: согласование изменившейся записи.
Спецификация — photon-core/docs/feed-reconcile.md, §6 «Failure-oriented».

Проверяется то, чего до среза не было вовсе: Version объявлялся,
сериализовался и НЕ ЧИТАЛСЯ — Ingest клал вслепую. Тесты ниже написаны так,
чтобы падать при возврате к любому из трёх наивных правил: «всегда
перезаписывать», «никогда» и перепутанное направление сравнения.

Наблюдения провода, на которых стоят ожидания (живой прогон 2026-09-11):
  • правка НЕ меняет create_at — позиция записи неизменна;
  • правка увеличивает update_at;
  • удаление — ИСЧЕЗНОВЕНИЕ из ленты, надгробия нет.
*/

func recv(id string, order, version int64, body string) Record {
	return Record{ID: id, OrderKey: order, Version: version,
		Body: json.RawMessage(body)}
}

func newestBlock(recs ...Record) Block {
	return Block{Records: recs, Dir: DirNewest, AtEnd: true}
}

func onlyRecord(t *testing.T, s *Store, id string) Record {
	t.Helper()
	pg, err := s.Page(context.Background(), "ch", 0, "", 100)
	if err != nil {
		t.Fatalf("Page: %v", err)
	}
	for _, r := range pg.Records {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("записи %s нет локально", id)
	return Record{}
}

// Правка: версия старше — тело обновляется, позиция НЕ двигается.
func TestReconcileNewerVersionUpdatesBodyKeepsPosition(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	if _, err := s.Ingest(ctx, "ch", newestBlock(recv("a", 100, 100, `{"m":"v1"}`))); err != nil {
		t.Fatalf("Ingest v1: %v", err)
	}
	res, err := s.Ingest(ctx, "ch", newestBlock(recv("a", 100, 250, `{"m":"v2"}`)))
	if err != nil {
		t.Fatalf("Ingest v2: %v", err)
	}

	if res.Updated != 1 || res.Unchanged != 0 || res.Stale != 0 {
		t.Fatalf("исходы согласования: updated=%d unchanged=%d stale=%d, ожидалось 1/0/0",
			res.Updated, res.Unchanged, res.Stale)
	}
	got := onlyRecord(t, s, "a")
	if string(got.Body) != `{"m":"v2"}` {
		t.Fatalf("тело не обновилось: %s", got.Body)
	}
	if got.Version != 250 {
		t.Fatalf("версия не обновилась: %d", got.Version)
	}
	// Позиция — замороженный пункт ADR-052. Правка её не двигает: живой
	// прогон показал, что create_at при редактировании не меняется.
	if got.OrderKey != 100 {
		t.Fatalf("правка сдвинула позицию: OrderKey=%d — это задевает ADR-052", got.OrderKey)
	}
}

// Переупорядоченная доставка: версия МЛАДШЕ локальной не применяется.
// Без этого правила запись молча откатывалась бы к прежнему телу.
func TestReconcileOlderVersionIsRejected(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	if _, err := s.Ingest(ctx, "ch", newestBlock(recv("a", 100, 250, `{"m":"v2"}`))); err != nil {
		t.Fatalf("Ingest v2: %v", err)
	}
	res, err := s.Ingest(ctx, "ch", newestBlock(recv("a", 100, 100, `{"m":"v1"}`)))
	if err != nil {
		t.Fatalf("Ingest v1: %v", err)
	}

	if res.Stale != 1 || res.Updated != 0 {
		t.Fatalf("устаревшая версия не отвергнута: stale=%d updated=%d", res.Stale, res.Updated)
	}
	if res.Stored != 0 {
		t.Fatalf("Stored обязан считать применённые записи, получено %d", res.Stored)
	}
	got := onlyRecord(t, s, "a")
	if string(got.Body) != `{"m":"v2"}` || got.Version != 250 {
		t.Fatalf("запись откатилась: version=%d body=%s", got.Version, got.Body)
	}
}

// Равная версия — не обновление и не откат; отдельный исход.
func TestReconcileEqualVersionCountedSeparately(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	if _, err := s.Ingest(ctx, "ch", newestBlock(recv("a", 100, 100, `{"m":"v1"}`))); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	res, err := s.Ingest(ctx, "ch", newestBlock(recv("a", 100, 100, `{"m":"v1"}`)))
	if err != nil {
		t.Fatalf("Ingest повторный: %v", err)
	}
	if res.Unchanged != 1 || res.Updated != 0 || res.Stale != 0 {
		t.Fatalf("равная версия: updated=%d unchanged=%d stale=%d, ожидалось 0/1/0",
			res.Updated, res.Unchanged, res.Stale)
	}
}

// Новая запись — не обновление: у неё нет локального предшественника.
func TestReconcileFirstSeenIsNeitherUpdateNorStale(t *testing.T) {
	s, _ := newStore(t)
	res, err := s.Ingest(context.Background(), "ch", newestBlock(
		recv("a", 100, 100, `{"m":"a"}`), recv("b", 200, 200, `{"m":"b"}`)))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Stored != 2 || res.Updated != 0 || res.Unchanged != 0 || res.Stale != 0 {
		t.Fatalf("первая встреча: stored=%d updated=%d unchanged=%d stale=%d",
			res.Stored, res.Updated, res.Unchanged, res.Stale)
	}
}

// Удаление видно ТОЛЬКО как исчезновение внутри покрытого диапазона.
func TestReconcileDetectsVanishedRecordInsideBlockRange(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	if _, err := s.Ingest(ctx, "ch", newestBlock(
		recv("a", 100, 100, `{"m":"a"}`),
		recv("b", 200, 200, `{"m":"b"}`),
		recv("c", 300, 300, `{"m":"c"}`))); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// b удалён на сервере: блок покрывает тот же диапазон, но b в нём нет.
	res, err := s.Ingest(ctx, "ch", newestBlock(
		recv("a", 100, 100, `{"m":"a"}`),
		recv("c", 300, 300, `{"m":"c"}`)))
	if err != nil {
		t.Fatalf("Ingest после удаления: %v", err)
	}
	if res.Vanished != 1 {
		t.Fatalf("исчезновение не замечено: vanished=%d", res.Vanished)
	}
	// Считает, но не удаляет: политика удаления — отдельное решение.
	if _, err := s.Page(ctx, "ch", 0, "", 10); err != nil {
		t.Fatalf("Page: %v", err)
	}
	if r := onlyRecord(t, s, "b"); r.ID != "b" {
		t.Fatal("запись исчезла из хранилища — срез удалять не должен")
	}
}

// Запись ВНЕ покрытого диапазона исчезнувшей не считается: блок о ней
// ничего не утверждал.
func TestReconcileRecordOutsideBlockRangeIsNotVanished(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	if _, err := s.Ingest(ctx, "ch", newestBlock(
		recv("old", 100, 100, `{"m":"old"}`),
		recv("mid", 200, 200, `{"m":"mid"}`))); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	// Блок только про верхнюю часть: old ниже его диапазона.
	res, err := s.Ingest(ctx, "ch", newestBlock(recv("mid", 200, 200, `{"m":"mid"}`)))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Vanished != 0 {
		t.Fatalf("запись вне диапазона объявлена исчезнувшей: vanished=%d", res.Vanished)
	}
}

// Правка на границе страницы — тот же класс, что нашёл живой прогон второго
// среза, где разрыв на границе склеивался молча.
func TestReconcileEditAtPageBoundary(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	const n = 12
	recs := make([]Record, 0, n)
	for i := 0; i < n; i++ {
		recs = append(recs, recv(fmt.Sprintf("r%02d", i), int64(1000+i*10), int64(1000+i*10),
			fmt.Sprintf(`{"m":"r%02d"}`, i)))
	}
	if _, err := s.Ingest(ctx, "ch", newestBlock(recs...)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	for _, size := range []int{1, 2, 3, 5, 11, 12} {
		pg, err := s.Page(ctx, "ch", 0, "", size)
		if err != nil {
			t.Fatalf("Page(%d): %v", size, err)
		}
		if len(pg.Records) == 0 {
			t.Fatalf("Page(%d): пусто", size)
		}
		// Правим последнюю запись страницы — ту, что стоит на стыке.
		last := pg.Records[len(pg.Records)-1]
		res, err := s.Ingest(ctx, "ch", newestBlock(
			recv(last.ID, last.OrderKey, last.Version+1, `{"m":"edited"}`)))
		if err != nil {
			t.Fatalf("Ingest правки (size=%d): %v", size, err)
		}
		if res.Updated != 1 {
			t.Fatalf("size=%d: правка на границе не применилась: updated=%d", size, res.Updated)
		}
		if got := onlyRecord(t, s, last.ID); string(got.Body) != `{"m":"edited"}` {
			t.Fatalf("size=%d: тело на границе не обновилось: %s", size, got.Body)
		}
	}
}

// Удаление и повторная заливка того же id: что сильнее.
func TestReconcileReAddAfterVanishRestoresRecord(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	if _, err := s.Ingest(ctx, "ch", newestBlock(
		recv("a", 100, 100, `{"m":"a"}`), recv("b", 200, 200, `{"m":"b"}`))); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	// b исчез…
	if _, err := s.Ingest(ctx, "ch", newestBlock(recv("a", 100, 100, `{"m":"a"}`))); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	// …и вернулся с бо́льшей версией.
	res, err := s.Ingest(ctx, "ch", newestBlock(
		recv("a", 100, 100, `{"m":"a"}`), recv("b", 200, 500, `{"m":"b-new"}`)))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Updated != 1 {
		t.Fatalf("повторная заливка не применилась: updated=%d", res.Updated)
	}
	if got := onlyRecord(t, s, "b"); string(got.Body) != `{"m":"b-new"}` || got.Version != 500 {
		t.Fatalf("возврат записи не применился: version=%d body=%s", got.Version, got.Body)
	}
}
