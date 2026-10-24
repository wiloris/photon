package dispatcher

import (
	"errors"
	"fmt"
	"testing"

	"github.com/wiloris/photon/photon-core/services/diag"
	"github.com/wiloris/photon/photon-core/services/session"
)

// 819/820 — диагностика через Photon_SendAction (ADR-130 §2.7, §2.8;
// docs/local-actions.md). Главные свойства: исполняются без сети и без
// хранилища; сами себя не наблюдают; курсор и dropped — по контракту.

type diagEventsWire struct {
	InstanceID          string           `json:"instance_id"`
	InstanceStartWallMS int64            `json:"instance_start_wall_ms"`
	Events              []map[string]any `json:"events"`
	NextSeq             uint64           `json:"next_seq"`
	Dropped             uint64           `json:"dropped"`
}

func diagEvents(t *testing.T, d *Dispatcher, after uint64, limit int) diagEventsWire {
	t.Helper()
	resp, err := d.SendAction(820, []byte(fmt.Sprintf(`{"after_seq":%d,"limit":%d}`, after, limit)))
	if err != nil {
		t.Fatalf("820: %v", err)
	}
	var out diagEventsWire
	okData(t, resp, &out)
	return out
}

func diagSnapshot(t *testing.T, d *Dispatcher) map[string]any {
	t.Helper()
	resp, err := d.SendAction(819, []byte(`{}`))
	if err != nil {
		t.Fatalf("819: %v", err)
	}
	var out map[string]any
	okData(t, resp, &out)
	return out
}

// На инстансе с адресом Bridge 819/820 не открывают ни одного соединения и
// не требуют хранилища; в журнал и счётчики вызовов не попадают.
func TestDiagActionsAreLocalAndSilent(t *testing.T) {
	skipIfDiagForcedOff(t)
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	for i := 0; i < 3; i++ {
		d.SendAction(424242, []byte(`{}`)) // три отказа — три call_failed
	}
	snap := diagSnapshot(t, d)
	if snap["instance_id"] != d.Diag().InstanceID() || snap["config"].(map[string]any)["enabled"] != true {
		t.Fatalf("снимок: %v", snap)
	}
	before := d.Diag().Snapshot()
	for i := 0; i < 5; i++ {
		diagSnapshot(t, d)
		diagEvents(t, d, 0, 512)
		d.SendAction(820, []byte(`{"limit":0}`)) // отказ чтения — тоже не наблюдается
	}
	after := d.Diag().Snapshot()
	if after.Journal.LastSeq != before.Journal.LastSeq || after.Calls.Total != before.Calls.Total || after.Calls.Total != 3 {
		t.Fatalf("чтение диагностики наблюдает само себя: журнал %d→%d, вызовы %d→%d",
			before.Journal.LastSeq, after.Journal.LastSeq, before.Calls.Total, after.Calls.Total)
	}
	if n := br.connectCount(); n != 0 {
		t.Fatalf("819/820 открыли %d соединений", n)
	}
	ev := diagEvents(t, d, 0, 512)
	if ev.InstanceID != d.Diag().InstanceID() || len(ev.Events) != 3 || ev.NextSeq != 3 {
		t.Fatalf("820: %+v", ev)
	}
	e := ev.Events[0]
	f := e["fields"].(map[string]any)
	if e["component"] != "dispatcher" || e["kind"] != "call_failed" || f["action"] != float64(424242) || f["class"] != "invalid_action" {
		t.Fatalf("запись: %v", e)
	}
	for _, k := range []string{"seq", "at_mono_ms"} {
		if _, ok := e[k]; !ok {
			t.Fatalf("нет %s: %v", k, e)
		}
	}
	// Поправка ADR-130 §2.3: календарного времени у записи нет — только
	// instance_start_wall_ms инстанса в ответах 819 и 820.
	if _, ok := e["at_wall_ms"]; ok {
		t.Fatalf("запись несёт at_wall_ms: %v", e)
	}
	start := d.Diag().InstanceStartWallMS()
	if ev.InstanceStartWallMS != start || snap["instance_start_wall_ms"] != float64(start) || start < 1_700_000_000_000 {
		t.Fatalf("instance_start_wall_ms: 820 %d, 819 %v, diag %d", ev.InstanceStartWallMS, snap["instance_start_wall_ms"], start)
	}
}

// Инстанс без Bridge и без хранилища — `{}` — исполняет 819/820.
func TestDiagActionsWithoutBridgeAndStorage(t *testing.T) {
	skipIfDiagForcedOff(t)
	d, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	diagSnapshot(t, d)
	if ev := diagEvents(t, d, 0, 10); len(ev.Events) != 0 || ev.NextSeq != 0 || ev.InstanceID == "" {
		t.Fatalf("820 на пустом журнале: %+v", ev)
	}
}

// Курсор: продолжение без пропусков и повторов; повтор запроса — те же
// записи; переполнение — dropped вместе с записями.
func TestDiagEventsCursor(t *testing.T) {
	skipIfDiagForcedOff(t)
	d, err := New(Config{Diagnostics: DiagnosticsConfig{Capacity: 64}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 20; i++ {
		d.SendAction(424242, []byte(`{}`))
	}
	var seen []float64
	cur := uint64(0)
	for {
		ev := diagEvents(t, d, cur, 3)
		again := diagEvents(t, d, cur, 3)
		if len(again.Events) != len(ev.Events) || again.NextSeq != ev.NextSeq {
			t.Fatal("повтор запроса дал другой ответ")
		}
		if len(ev.Events) == 0 {
			if ev.NextSeq != cur {
				t.Fatalf("пустой ответ: next_seq %d, want %d", ev.NextSeq, cur)
			}
			break
		}
		for _, e := range ev.Events {
			seen = append(seen, e["seq"].(float64))
		}
		cur = ev.NextSeq
	}
	if len(seen) != 20 || seen[0] != 1 || seen[19] != 20 {
		t.Fatalf("прочитано: %v", seen)
	}

	for i := 0; i < 100; i++ {
		d.SendAction(424242, []byte(`{}`))
	}
	ev := diagEvents(t, d, 20, 512)
	if ev.Dropped != 120-64-20 || len(ev.Events) != 64 || ev.NextSeq != 120 {
		t.Fatalf("переполнение: dropped %d, записей %d, next %d", ev.Dropped, len(ev.Events), ev.NextSeq)
	}
}

// Отказы 820 — отказ вызова класса invalid_argument (ADR-065 §8.2), с
// сохранённым sentinel'ом.
func TestDiagEventsRejects(t *testing.T) {
	skipIfDiagForcedOff(t)
	d, _ := New(Config{})
	defer d.Close()
	d.SendAction(424242, []byte(`{}`))
	for _, tc := range []struct {
		payload string
		want    error
	}{
		{`{"after_seq":0,"limit":0}`, diag.ErrInvalidLimit},
		{`{"after_seq":0}`, diag.ErrInvalidLimit},
		{`{"after_seq":0,"limit":513}`, diag.ErrInvalidLimit},
		{`{"after_seq":2,"limit":10}`, diag.ErrInvalidCursor},
		{`{"after_seq":-1,"limit":10}`, session.ErrInvalidPayload},
		{`not json`, session.ErrInvalidPayload},
	} {
		_, err := d.SendAction(820, []byte(tc.payload))
		if !errors.Is(err, tc.want) || Classify(err) != ClassInvalidArgument {
			t.Errorf("%s: err = %v (класс %v), want %v / invalid_argument", tc.payload, err, Classify(err), tc.want)
		}
	}
	if _, err := d.SendAction(819, []byte(`[1]`)); Classify(err) != ClassInvalidArgument {
		t.Errorf("819 с не-объектом: %v", err)
	}
}

// Выключенная диагностика: 819 — снимок с enabled=false, 820 — пустая
// страница с next_seq = after_seq; отказом не считается.
func TestDiagActionsWhenDisabled(t *testing.T) {
	d, err := New(Config{Diagnostics: DiagnosticsConfig{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	snap := diagSnapshot(t, d)
	if snap["config"].(map[string]any)["enabled"] != false || snap["instance_id"] != "" {
		t.Fatalf("снимок выключенной: %v", snap)
	}
	if ev := diagEvents(t, d, 7, 10); len(ev.Events) != 0 || ev.NextSeq != 7 || ev.InstanceID != "" || ev.InstanceStartWallMS != 0 {
		t.Fatalf("820 выключенной: %+v", ev)
	}
}
