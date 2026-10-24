package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/wiloris/photon/photon-core/services/dispatcher"
)

/*
 * Диагностика через границу ABI — ADR-130 §2.7, §2.8.
 *
 * Цепочка Photon_Create → Photon_SendAction(819/820) → services/diag →
 * response_json целиком, через настоящие экспортируемые функции. Новых
 * символов нет: вход — тот же Photon_SendAction; C ABI 1/0 и ровно 12
 * экспортируемых функций проверяет TestABISurfaceUnchanged
 * (abi_surface_test.go).
 */

const (
	actionDiagSnapshotC = 819
	actionDiagEventsC   = 820
	actionUnknownC      = 424242
)

// sendOK — вызов, обязанный вернуть PHOTON_OK и конверт ok:true; data.
func sendOK(t *testing.T, h unsafe.Pointer, action int32, payload string) json.RawMessage {
	t.Helper()
	rc, respOut := tsSendAction(h, action, cstr(t, payload), true)
	if rc != tsOK || respOut == nil {
		t.Fatalf("SendAction(%d, %s): rc = %d (%s)", action, payload, rc, tsGoString(tsGetLastError(h)))
	}
	defer tsFreeString(respOut)
	var env struct {
		OK   bool            `json:"ok"`
		Data json.RawMessage `json:"data"`
	}
	body := tsGoString(tsStringData(respOut))
	if err := json.Unmarshal([]byte(body), &env); err != nil || !env.OK {
		t.Fatalf("конверт %d: %s", action, body)
	}
	return env.Data
}

type diagEventsC struct {
	InstanceID          string           `json:"instance_id"`
	InstanceStartWallMS int64            `json:"instance_start_wall_ms"`
	Events              []map[string]any `json:"events"`
	NextSeq             uint64           `json:"next_seq"`
	Dropped             uint64           `json:"dropped"`
}

// 819 и 820 на инстансе без bridge и storage: снимок и журнал с записями
// об отвергнутых вызовах; чтение по курсору до конца.
func TestDiag_SnapshotAndEventsThroughABI(t *testing.T) {
	h, rc := tsCreate(cstr(t, `{}`), false)
	if rc != tsOK || h == nil {
		t.Fatalf("Photon_Create: rc = %d", rc)
	}
	defer tsDestroy(h)

	for i := 0; i < 5; i++ {
		if rc, _ := tsSendAction(h, actionUnknownC, cstr(t, `{}`), false); rc != tsErrInvalidAction {
			t.Fatalf("неизвестный код: rc = %d", rc)
		}
	}

	var snap struct {
		InstanceID          string `json:"instance_id"`
		InstanceStartWallMS int64  `json:"instance_start_wall_ms"`
		Calls               struct {
			Total   uint64            `json:"total"`
			ByClass map[string]uint64 `json:"by_class"`
		} `json:"calls"`
		Journal struct {
			LastSeq uint64 `json:"last_seq"`
		} `json:"journal"`
		Config struct {
			Enabled  bool `json:"enabled"`
			Capacity int  `json:"capacity"`
		} `json:"config"`
	}
	if err := json.Unmarshal(sendOK(t, h, actionDiagSnapshotC, `{}`), &snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.InstanceID) != 32 || !snap.Config.Enabled || snap.Config.Capacity != 1024 ||
		snap.Calls.Total != 5 || snap.Calls.ByClass["invalid_action"] != 5 || snap.Journal.LastSeq != 5 {
		t.Fatalf("снимок: %+v", snap)
	}

	var seen []float64
	cur := uint64(0)
	for {
		var ev diagEventsC
		json.Unmarshal(sendOK(t, h, actionDiagEventsC, fmt.Sprintf(`{"after_seq":%d,"limit":2}`, cur)), &ev)
		if ev.InstanceID != snap.InstanceID || ev.InstanceStartWallMS != snap.InstanceStartWallMS || ev.InstanceStartWallMS < 1_700_000_000_000 {
			t.Fatalf("instance_id %q / старт %d, снимок %q / %d", ev.InstanceID, ev.InstanceStartWallMS, snap.InstanceID, snap.InstanceStartWallMS)
		}
		for _, e := range ev.Events {
			if _, ok := e["at_wall_ms"]; ok {
				t.Fatalf("запись несёт at_wall_ms: %v", e)
			}
		}
		if len(ev.Events) == 0 {
			break
		}
		for _, e := range ev.Events {
			if e["kind"] != "call_failed" || e["component"] != "dispatcher" {
				t.Fatalf("запись: %v", e)
			}
			seen = append(seen, e["seq"].(float64))
		}
		cur = ev.NextSeq
	}
	if len(seen) != 5 || seen[0] != 1 || seen[4] != 5 {
		t.Fatalf("прочитано %v", seen)
	}

	// Чтение диагностики само себя не наблюдает: после всех 819/820 — те же
	// 5 вызовов и 5 записей.
	json.Unmarshal(sendOK(t, h, actionDiagSnapshotC, `{}`), &snap)
	if snap.Calls.Total != 5 || snap.Journal.LastSeq != 5 {
		t.Fatalf("819/820 попали в учёт: вызовов %d, записей %d", snap.Calls.Total, snap.Journal.LastSeq)
	}
}

// Отказы 820 — PHOTON_ERR_INVALID_ARGUMENT с причиной в GetLastError,
// response_out не устанавливается.
func TestDiag_EventsRejectedThroughABI(t *testing.T) {
	h, rc := tsCreate(cstr(t, `{}`), false)
	if rc != tsOK {
		t.Fatalf("Photon_Create: rc = %d", rc)
	}
	defer tsDestroy(h)
	for _, tc := range []struct{ payload, why string }{
		{`{"after_seq":0,"limit":0}`, "limit"},
		{`{"after_seq":0,"limit":513}`, "limit"},
		{`{"after_seq":7,"limit":10}`, "cursor"},
		{`oops`, "payload"},
	} {
		rc, respOut := tsSendAction(h, actionDiagEventsC, cstr(t, tc.payload), true)
		if rc != tsErrInvalidArgument || respOut != nil {
			t.Fatalf("%s: rc = %d, response_out = %v", tc.payload, rc, respOut)
		}
		if msg := tsGoString(tsGetLastError(h)); !strings.Contains(msg, tc.why) {
			t.Fatalf("%s: last error %q, ожидалось упоминание %q", tc.payload, msg, tc.why)
		}
	}
}

// Секция diagnostics в config_json: выключенная — 819 говорит
// enabled=false, 820 — пустая страница; ёмкость вне пределов — отказ
// создания.
func TestDiag_ConfigThroughABI(t *testing.T) {
	h, rc := tsCreate(cstr(t, `{"diagnostics":{"enabled":false}}`), false)
	if rc != tsOK {
		t.Fatalf("Photon_Create: rc = %d", rc)
	}
	var snap struct {
		Config struct {
			Enabled bool `json:"enabled"`
		} `json:"config"`
	}
	json.Unmarshal(sendOK(t, h, actionDiagSnapshotC, `{}`), &snap)
	var ev diagEventsC
	json.Unmarshal(sendOK(t, h, actionDiagEventsC, `{"after_seq":3,"limit":10}`), &ev)
	tsDestroy(h)
	if snap.Config.Enabled || len(ev.Events) != 0 || ev.NextSeq != 3 {
		t.Fatalf("выключенная: enabled=%v, событий %d, next %d", snap.Config.Enabled, len(ev.Events), ev.NextSeq)
	}

	if h, rc := tsCreate(cstr(t, `{"diagnostics":{"capacity":10}}`), false); rc != tsErrInvalidArgument || h != nil {
		t.Fatalf("ёмкость 10: rc = %d", rc)
	}
}

// На инстансе С адресом Bridge 819/820 не открывают ни одного соединения:
// исполняются в инстансе (диспетчер), на провод не выходят (ADR-130 §2.8).
func TestDiag_NoConnectionWithBridgeConfigured(t *testing.T) {
	tr := &fakeTransport{}
	h, err := tsNewCoreWithFakeDispatcher(dispatcher.Config{
		Endpoint: "fake://bridge", SessionToken: "tok", Transport: tr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tsDestroy(h)
	for i := 0; i < 10; i++ {
		sendOK(t, h, actionDiagSnapshotC, `{}`)
		sendOK(t, h, actionDiagEventsC, `{"after_seq":0,"limit":512}`)
	}
	tr.mu.Lock()
	n := len(tr.conns)
	tr.mu.Unlock()
	if n != 0 {
		t.Fatalf("819/820 открыли %d соединений", n)
	}
}
