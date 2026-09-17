package main

import (
	"encoding/json"
	"testing"
)

// Разбор ответа сервера в записи ленты — единственное место инструмента, где
// ошибка не видна на глаз: неверное поле в OrderKey или порядок из map дают
// правдоподобный вывод и неправильную ленту. Всё остальное здесь — печать и
// вызовы уже покрытого тестами localfeed.
//
// Тесты держат три свойства: порядок берётся из order, поля отображаются
// именно так, а идентификатор без поста записью не становится.

func post1() post {
	return post{ID: "a", CreateAt: 100, UpdateAt: 150, Message: "первый"}
}

func post2() post {
	return post{ID: "b", CreateAt: 200, UpdateAt: 200, Message: "второй"}
}

func TestRecordsFromKeepsServerOrder(t *testing.T) {
	// Порядок в order намеренно обратный «естественному» по времени: если
	// реализация начнёт брать порядок из map или сортировать сама, тест
	// это увидит.
	order := []string{"b", "a"}
	posts := map[string]post{"a": post1(), "b": post2()}

	recs, missing := recordsFrom(order, posts)
	if len(missing) != 0 {
		t.Fatalf("пропущенных быть не должно: %v", missing)
	}
	if len(recs) != 2 {
		t.Fatalf("записей %d, ожидалось 2", len(recs))
	}
	if recs[0].ID != "b" || recs[1].ID != "a" {
		t.Fatalf("порядок записей [%s %s], ожидался [b a]", recs[0].ID, recs[1].ID)
	}
}

func TestRecordsFromMapsOrderKeyAndVersion(t *testing.T) {
	recs, _ := recordsFrom([]string{"a"}, map[string]post{"a": post1()})
	if len(recs) != 1 {
		t.Fatalf("записей %d, ожидалась 1", len(recs))
	}
	r := recs[0]

	// OrderKey — create_at: позиция в ленте не должна двигаться при правке
	// (ADR-053). Version — update_at: именно он меняется при правке.
	if r.OrderKey != 100 {
		t.Errorf("OrderKey = %d, ожидался create_at = 100", r.OrderKey)
	}
	if r.Version != 150 {
		t.Errorf("Version = %d, ожидался update_at = 150", r.Version)
	}

	// Тело — исходный пост целиком: хранилище держит его непрозрачным, и
	// потеря поля здесь обнаружилась бы только у потребителя.
	var back post
	if err := json.Unmarshal(r.Body, &back); err != nil {
		t.Fatalf("тело записи не разбирается: %v", err)
	}
	if back != post1() {
		t.Errorf("тело записи = %+v, ожидалось %+v", back, post1())
	}
}

// Идентификатор из order, которого нет в posts, — не запись.
//
// Прежде такой случай давал Record с пустым ID и OrderKey 0. Ingest пустой ID
// не проверяет, поэтому в ленте появлялась бы вечно самая старая запись, не
// относящаяся ни к какому посту, и заметить это можно было бы только по
// странному выводу много позже.
func TestRecordsFromSkipsIDsWithoutPost(t *testing.T) {
	order := []string{"a", "нет-такого", "b"}
	posts := map[string]post{"a": post1(), "b": post2()}

	recs, missing := recordsFrom(order, posts)

	if len(recs) != 2 {
		t.Fatalf("записей %d, ожидалось 2 — запись без поста создана", len(recs))
	}
	for _, r := range recs {
		if r.ID == "" {
			t.Fatal("создана запись с пустым ID: локальная лента будет испорчена")
		}
		if r.OrderKey == 0 {
			t.Fatalf("запись %q с OrderKey 0 — она станет вечно самой старой", r.ID)
		}
	}
	if len(missing) != 1 || missing[0] != "нет-такого" {
		t.Fatalf("пропущенные = %v, ожидался ровно [нет-такого]", missing)
	}
}

func TestRecordsFromEmptyOrderGivesNoRecords(t *testing.T) {
	recs, missing := recordsFrom(nil, map[string]post{"a": post1()})
	if len(recs) != 0 {
		t.Fatalf("записей %d, ожидалось 0: порядок задаёт order, а не posts", len(recs))
	}
	if len(missing) != 0 {
		t.Fatalf("пропущенные = %v, ожидалось пусто", missing)
	}
}
