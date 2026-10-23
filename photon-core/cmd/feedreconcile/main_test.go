package main

import (
	"encoding/json"
	"testing"
)

// Те же три свойства разбора, что и в cmd/feedslice, плюс специфика этого
// инструмента: у его post есть edit_at, и оно обязано доезжать до тела
// записи — согласование версий (ADR-053) наблюдает именно правку.

func sample() post {
	return post{ID: "a", Message: "первый", CreateAt: 100, UpdateAt: 150, EditAt: 150}
}

func other() post {
	return post{ID: "b", Message: "второй", CreateAt: 200, UpdateAt: 200}
}

func TestRecordsFromKeepsServerOrder(t *testing.T) {
	recs, missing := recordsFrom([]string{"b", "a"}, map[string]post{"a": sample(), "b": other()})
	if len(missing) != 0 {
		t.Fatalf("пропущенных быть не должно: %v", missing)
	}
	if len(recs) != 2 || recs[0].ID != "b" || recs[1].ID != "a" {
		t.Fatalf("порядок записей не из order: %+v", recs)
	}
}

func TestRecordsFromMapsFieldsAndKeepsEditAt(t *testing.T) {
	recs, _ := recordsFrom([]string{"a"}, map[string]post{"a": sample()})
	if len(recs) != 1 {
		t.Fatalf("записей %d, ожидалась 1", len(recs))
	}
	r := recs[0]
	if r.OrderKey != 100 {
		t.Errorf("OrderKey = %d, ожидался create_at = 100", r.OrderKey)
	}
	if r.Version != 150 {
		t.Errorf("Version = %d, ожидался update_at = 150", r.Version)
	}

	var back post
	if err := json.Unmarshal(r.Body, &back); err != nil {
		t.Fatalf("тело записи не разбирается: %v", err)
	}
	if back.EditAt != 150 {
		t.Errorf("edit_at потерян при переносе в тело: %+v", back)
	}
	if back != sample() {
		t.Errorf("тело записи = %+v, ожидалось %+v", back, sample())
	}
}

// Идентификатор без поста записью не становится: иначе в ленте появится
// запись с пустым ID и OrderKey 0 — вечно самая старая и ничья.
func TestRecordsFromSkipsIDsWithoutPost(t *testing.T) {
	recs, missing := recordsFrom(
		[]string{"a", "нет-такого"},
		map[string]post{"a": sample()},
	)
	if len(recs) != 1 {
		t.Fatalf("записей %d, ожидалась 1 — запись без поста создана", len(recs))
	}
	if recs[0].ID == "" || recs[0].OrderKey == 0 {
		t.Fatalf("создана пустая запись: %+v", recs[0])
	}
	if len(missing) != 1 || missing[0] != "нет-такого" {
		t.Fatalf("пропущенные = %v, ожидался ровно [нет-такого]", missing)
	}
}
