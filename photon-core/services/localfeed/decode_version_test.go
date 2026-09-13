package localfeed

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

// СОГЛАСОВАНИЕ ВЕРСИЙ ПОСЛЕ СУЖЕНИЯ РАЗБОРА.
//
// Ingest перестал разбирать хранимую запись целиком и читает одно поле
// (codec.go, decodeVersion). Формат хранения при этом не менялся, поэтому
// проверять надо ровно одно: та ли версия прочитана — и, как следствие, те
// же ли Updated / Unchanged / Stale.
//
// Главная ловушка сужения — ТЕЛО, СОДЕРЖАЩЕЕ СВОЁ СОБСТВЕННОЕ "version".
// Разбор, который ищет подстроку или берёт первое совпадение, вернёт версию
// из тела и будет выглядеть работающим на любом обычном сообщении. Поэтому
// такое тело стоит первым в каждом тесте ниже, а не в конце как экзотика.

// bodyWithNestedVersion — тело, в котором есть и "version", и "order_key",
// причём численно больше настоящих: ошибка разбора даст не мусор, а
// правдоподобное неверное число.
const bodyWithNestedVersion = `{"version":999999,"order_key":888888,"id":"чужой","text":"правка"}`

// decodeVersion обязан совпадать с полем Version полного decode — на всех
// значениях, включая тело-ловушку.
func TestDecodeVersionAgreesWithFullDecode(t *testing.T) {
	cases := []struct {
		name string
		rec  Record
	}{
		{"тело со своим version", Record{ID: "a", OrderKey: 100, Version: 100,
			Body: json.RawMessage(bodyWithNestedVersion)}},
		{"версия ноль", Record{ID: "b", OrderKey: 200, Version: 0,
			Body: json.RawMessage(`{"m":"x"}`)}},
		{"версия отрицательная", Record{ID: "c", OrderKey: 300, Version: -42,
			Body: json.RawMessage(`{"m":"x"}`)}},
		{"версия на границе int64", Record{ID: "d", OrderKey: 400, Version: 1<<62 + 12345,
			Body: json.RawMessage(`{"m":"x"}`)}},
		{"тело пустое", Record{ID: "e", OrderKey: 500, Version: 7}},
		{"тело — массив", Record{ID: "f", OrderKey: 600, Version: 8,
			Body: json.RawMessage(`[{"version":1},{"version":2}]`)}},
		{"тело — строка с version внутри", Record{ID: "g", OrderKey: 700, Version: 9,
			Body: json.RawMessage(`{"t":"{\"version\":5}"}`)}},
		{"id содержит version", Record{ID: `x"version":123`, OrderKey: 800, Version: 11,
			Body: json.RawMessage(`{"m":"x"}`)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			enc, err := encode(c.rec)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			full, err := decode(enc)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			got, err := decodeVersion(enc)
			if err != nil {
				t.Fatalf("decodeVersion: %v", err)
			}
			if got != full.Version {
				t.Fatalf("decodeVersion=%d, полный decode=%d — сужение разбора читает не то поле",
					got, full.Version)
			}
			if got != c.rec.Version {
				t.Fatalf("decodeVersion=%d, записано %d", got, c.rec.Version)
			}
		})
	}
}

// ПОРЯДОК ПОЛЕЙ НЕ ДОЛЖЕН ИМЕТЬ ЗНАЧЕНИЯ — и это отдельный тест, а не строка
// в таблице выше.
//
// Таблица строит значение через encode, а encode выкладывает поля в порядке
// структуры: `version` идёт ДО `body`. Разбор «первое попавшееся version»
// на таких документах работает правильно по совпадению — мутация это и
// показала: наивный поиск подстроки прошёл тело-ловушку насквозь и упал лишь
// на испорченном входе. В JSON порядок полей ничего не значит, и документ с
// `body` впереди — такой же законный документ.
func TestDecodeVersionIsIndependentOfFieldOrder(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want int64
	}{
		{"body раньше version",
			`{"body":{"version":999999},"id":"a","order_key":100,"version":100}`, 100},
		{"body раньше, тело вложенное",
			`{"body":{"a":{"version":7},"version":8},"version":42,"id":"b","order_key":1}`, 42},
		{"version последним полем",
			`{"id":"c","order_key":5,"body":{"version":3},"version":9}`, 9},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeVersion([]byte(c.raw))
			if err != nil {
				t.Fatalf("decodeVersion: %v", err)
			}
			full, err := decode([]byte(c.raw))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != c.want {
				t.Fatalf("decodeVersion=%d, ожидалось %d — разбор берёт version из ТЕЛА, "+
					"а не из записи", got, c.want)
			}
			if got != full.Version {
				t.Fatalf("decodeVersion=%d, полный decode=%d", got, full.Version)
			}
		})
	}
}

// Сужение не ослабило строгость: испорченное значение обязано ошибаться, а не
// молча давать ноль. Ноль здесь означал бы «версия 0», то есть ЛЮБАЯ пришедшая
// запись оказалась бы новее — тихая потеря защиты от отката.
func TestDecodeVersionRejectsMalformedValue(t *testing.T) {
	bad := [][]byte{
		[]byte(``),
		[]byte(`{`),
		[]byte(`{"version":}`),
		[]byte(`{"version":"сто"}`),
		[]byte(`{"version":1}{"version":2}`),
		[]byte("\x00\x01\x02"),
	}
	for i, v := range bad {
		t.Run(fmt.Sprintf("мусор_%d", i), func(t *testing.T) {
			if _, err := decodeVersion(v); err == nil {
				t.Fatalf("decodeVersion(%q) ошибки не вернул", v)
			}
			// Полный decode на том же входе тоже обязан падать: поведение
			// сравнивается, а не подгоняется.
			if _, err := decode(v); err == nil {
				t.Fatalf("decode(%q) ошибки не вернул — сравнение потеряло смысл", v)
			}
		})
	}
}

// Сквозная проверка на уровне Ingest: три вердикта на теле-ловушке.
//
// Если бы Ingest читал версию из тела, «999999» победило бы любую настоящую:
// первая же правка считалась бы Stale, и запись перестала бы обновляться
// молча — без ошибки, без исключения, с виду нормально.
func TestIngestVerdictsSurviveBodyCarryingItsOwnVersion(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)

	first := Record{ID: "a", OrderKey: 100, Version: 100,
		Body: json.RawMessage(bodyWithNestedVersion)}
	if res, err := s.Ingest(ctx, "ch", newestBlock(first)); err != nil {
		t.Fatalf("Ingest: %v", err)
	} else if res.Stored != 1 || res.Updated != 0 || res.Unchanged != 0 || res.Stale != 0 {
		t.Fatalf("первое появление: %+v", res)
	}

	// Та же версия — Unchanged.
	same := Record{ID: "a", OrderKey: 100, Version: 100,
		Body: json.RawMessage(`{"version":999999,"text":"то же"}`)}
	if res, err := s.Ingest(ctx, "ch", newestBlock(same)); err != nil {
		t.Fatalf("Ingest: %v", err)
	} else if res.Unchanged != 1 || res.Updated != 0 || res.Stale != 0 {
		t.Fatalf("равная версия: %+v — вердикт взят не из поля version записи", res)
	}

	// Версия больше — Updated, тело меняется.
	newer := Record{ID: "a", OrderKey: 100, Version: 250,
		Body: json.RawMessage(`{"version":1,"text":"новее"}`)}
	if res, err := s.Ingest(ctx, "ch", newestBlock(newer)); err != nil {
		t.Fatalf("Ingest: %v", err)
	} else if res.Updated != 1 || res.Unchanged != 0 || res.Stale != 0 {
		t.Fatalf("версия больше: %+v", res)
	}
	if got := onlyRecord(t, s, "a").Version; got != 250 {
		t.Fatalf("локальная версия %d, ожидалась 250", got)
	}

	// Версия меньше — Stale, тело НЕ откатывается.
	older := Record{ID: "a", OrderKey: 100, Version: 200,
		Body: json.RawMessage(`{"version":999999,"text":"откат"}`)}
	if res, err := s.Ingest(ctx, "ch", newestBlock(older)); err != nil {
		t.Fatalf("Ingest: %v", err)
	} else if res.Stale != 1 || res.Updated != 0 || res.Unchanged != 0 {
		t.Fatalf("версия меньше: %+v", res)
	}
	got := onlyRecord(t, s, "a")
	if got.Version != 250 {
		t.Fatalf("после Stale версия %d — откат состоялся", got.Version)
	}
	if string(got.Body) != `{"version":1,"text":"новее"}` {
		t.Fatalf("после Stale тело %s — откат состоялся", got.Body)
	}
}

// Тело больше не копируется при согласовании — и не должно ни во что
// протекать: запись, лежащая локально, обязана остаться нетронутой байт в
// байт, когда пришла та же версия.
func TestIngestUnchangedKeepsStoredBodyByteForByte(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(t)
	body := json.RawMessage(`{"version":999999,"nested":{"version":7},"text":"пусть лежит"}`)
	r := Record{ID: "a", OrderKey: 100, Version: 100, Body: body}
	if _, err := s.Ingest(ctx, "ch", newestBlock(r)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	before := string(onlyRecord(t, s, "a").Body)
	if _, err := s.Ingest(ctx, "ch", newestBlock(r)); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if after := string(onlyRecord(t, s, "a").Body); after != before {
		t.Fatalf("тело изменилось при Unchanged:\nбыло:  %s\nстало: %s", before, after)
	}
	if before != string(body) {
		t.Fatalf("тело не совпадает с исходным: %s", before)
	}
}
