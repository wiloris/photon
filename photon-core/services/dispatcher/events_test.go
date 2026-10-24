package dispatcher

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/wiloris/photon/photon-core/services/session"
)

// Форма event_json — нормативная (event-delivery.md §8). Проверяется побайтово,
// а не «разобрали и сравнили поля»: клиент читает именно строку, и порядок
// полей с наличием/отсутствием user_id — часть того, что он видит.

func TestEventConnectedForm(t *testing.T) {
	if got := string(eventConnected()); got != `{"type":"CONNECTED"}` {
		t.Fatalf("CONNECTED: %s", got)
	}
}

func TestEventNetworkChangedForm(t *testing.T) {
	want := `{"type":"NETWORK_CHANGED","state":"disconnected"}`
	if got := string(eventNetworkChanged()); got != want {
		t.Fatalf("NETWORK_CHANGED: %s", got)
	}
}

// user_id ОПУСКАЕТСЯ при пустом значении — ловушка провода §8.2: поле объявлено
// сервером с omitempty и заполняется одним хуком из восьми, а идентификатор
// автора для остальных лежит внутри data. Подставить сюда пустую строку значило
// бы придумать данные.
func TestEventMessageOmitsEmptyUserID(t *testing.T) {
	got, err := eventMessage(session.Push{Event: "posted", Data: []byte(`{"id":"p1"}`)})
	if err != nil {
		t.Fatalf("eventMessage: %v", err)
	}
	want := `{"type":"MESSAGE","event":"posted","data":{"id":"p1"}}`
	if string(got) != want {
		t.Fatalf("получено %s\nожидалось %s", got, want)
	}
	if strings.Contains(string(got), "user_id") {
		t.Fatal("user_id присутствует при пустом значении")
	}
}

func TestEventMessageIncludesUserIDWhenPresent(t *testing.T) {
	got, err := eventMessage(session.Push{
		Event: "status_change", UserID: "u7", Data: []byte(`{"status":"online"}`),
	})
	if err != nil {
		t.Fatalf("eventMessage: %v", err)
	}
	want := `{"type":"MESSAGE","event":"status_change","user_id":"u7","data":{"status":"online"}}`
	if string(got) != want {
		t.Fatalf("получено %s\nожидалось %s", got, want)
	}
}

// data вставляются СЫРЫМИ: то же свойство, что у envelope. Разбор и пересборка
// потеряли бы точность больших чисел на float64 и переставили бы ключи.
func TestEventMessagePreservesRawData(t *testing.T) {
	raw := `{"big":123456789012345678901234567890,"z":1,"a":2}`
	got, err := eventMessage(session.Push{Event: "posted", Data: []byte(raw)})
	if err != nil {
		t.Fatalf("eventMessage: %v", err)
	}
	if !strings.Contains(string(got), raw) {
		t.Fatalf("data пересобраны, а не вставлены сырыми: %s", got)
	}
}

// Неизвестное имя события проходит НАСКВОЗЬ. Ядро не обязано знать доменный
// словарь Bridge, а отбрасывать событие из-за незнакомого имени значило бы
// терять его молча.
func TestEventMessagePassesUnknownEventName(t *testing.T) {
	got, err := eventMessage(session.Push{Event: "brand_new_thing"})
	if err != nil {
		t.Fatalf("eventMessage: %v", err)
	}
	if !strings.Contains(string(got), `"event":"brand_new_thing"`) {
		t.Fatalf("имя события не прошло насквозь: %s", got)
	}
}

// Имя события — строка с провода, и её нужно ЭКРАНИРОВАТЬ: склейка без
// json.Marshal отдала бы клиенту битую строку.
func TestEventMessageEscapesEventName(t *testing.T) {
	got, err := eventMessage(session.Push{Event: `a"b\c`})
	if err != nil {
		t.Fatalf("eventMessage: %v", err)
	}
	if !json.Valid(got) {
		t.Fatalf("event_json невалиден при кавычках в имени: %s", got)
	}
}

func TestEventMessageAllFormsAreValidJSON(t *testing.T) {
	for _, p := range []session.Push{
		{Event: "posted"},
		{Event: "posted", UserID: "u1"},
		{Event: "posted", Data: []byte(`[1,2,3]`)},
		{Event: "posted", UserID: "u1", Data: []byte(`{"a":null}`)},
	} {
		got, err := eventMessage(p)
		if err != nil {
			t.Fatalf("eventMessage(%+v): %v", p, err)
		}
		if !json.Valid(got) {
			t.Fatalf("невалидный event_json: %s", got)
		}
		var probe struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(got, &probe); err != nil || probe.Type != EventMessage {
			t.Fatalf("поле type отсутствует или неверно: %s", got)
		}
	}
}

// Битые data отвергаются ЗДЕСЬ, а не уезжают клиенту склеенной строкой.
func TestEventMessageRejectsInvalidData(t *testing.T) {
	if _, err := eventMessage(session.Push{Event: "posted", Data: []byte(`{oops`)}); err == nil {
		t.Fatal("битые data приняты")
	}
}
