package dispatcher

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	edadapter "github.com/wiloris/photon/photon-core/adapters/ed25519"
	"github.com/wiloris/photon/photon-core/services/session"
)

// ADR-116 §3: P2P-исполнитель через SendAction — ту же точку, что
// Photon_SendAction. Два инстанса, настоящие device.key, настоящий Pion.

type p2pNode struct {
	d   *Dispatcher
	pub []byte
}

func writeDeviceKey(t *testing.T, dir string) (string, []byte) {
	t.Helper()
	id := edadapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "device.key")
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(pair.Private)), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, pair.Public
}

func newP2PNode(t *testing.T) p2pNode {
	t.Helper()
	path, pub := writeDeviceKey(t, t.TempDir())
	raw, _ := json.Marshal(map[string]any{"p2p": map[string]any{
		"device_key_path": path, "peer_loss_timeout_ms": 3000, "establish_timeout_ms": 15000,
	}})
	cfg, err := ParseConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	d, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return p2pNode{d: d, pub: pub}
}

type p2pEnv struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data"`
	Error string          `json:"error"`
}

func p2pCall(t *testing.T, n p2pNode, action int32, payload any) p2pEnv {
	t.Helper()
	raw, _ := json.Marshal(payload)
	out, err := n.d.SendAction(action, raw)
	if err != nil {
		t.Fatalf("SendAction %d: %v", action, err)
	}
	var env p2pEnv
	if err := json.Unmarshal(out, &env); err != nil {
		t.Fatalf("конверт: %v", err)
	}
	return env
}

func p2pOK(t *testing.T, n p2pNode, action int32, payload any, out any) {
	t.Helper()
	env := p2pCall(t, n, action, payload)
	if !env.OK {
		t.Fatalf("действие %d: вердикт %q", action, env.Error)
	}
	if out != nil {
		if err := json.Unmarshal(env.Data, out); err != nil {
			t.Fatal(err)
		}
	}
}

type p2pEvent struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	StreamID  string `json:"stream_id"`
	Service   string `json:"service"`
	Payload   []byte `json:"payload"`
	Reason    string `json:"reason"`
	Path      string `json:"path"`
}

// awaitP2P опрашивает 865 до нужного события; прочие события копит.
func awaitP2P(t *testing.T, n p2pNode, store *[]p2pEvent, typ, sid string) p2pEvent {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for {
		for i, ev := range *store {
			if ev.Type == typ && ev.SessionID == sid {
				*store = append((*store)[:i:i], (*store)[i+1:]...)
				return ev
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("нет события %s для %s; есть %+v", typ, sid, *store)
		}
		var got struct {
			Events []p2pEvent `json:"events"`
		}
		p2pOK(t, n, actionP2PEvents, map[string]int{"max": 64}, &got)
		*store = append(*store, got.Events...)
		time.Sleep(30 * time.Millisecond)
	}
}

func TestP2PActionsEndToEndThroughSendAction(t *testing.T) {
	a, b := newP2PNode(t), newP2PNode(t)
	var evA, evB []p2pEvent

	var off struct {
		NegotiationID string `json:"negotiation_id"`
		Offer         []byte `json:"offer"`
	}
	p2pOK(t, a, actionP2POffer, map[string]any{"peer_public_key": b.pub}, &off)
	var acc struct {
		SessionID string `json:"session_id"`
		Answer    []byte `json:"answer"`
	}
	p2pOK(t, b, actionP2PAccept, map[string]any{"peer_public_key": a.pub, "offer": off.Offer}, &acc)
	var cmp struct {
		SessionID string `json:"session_id"`
	}
	p2pOK(t, a, actionP2PComplete, map[string]any{"negotiation_id": off.NegotiationID, "answer": acc.Answer}, &cmp)

	if ev := awaitP2P(t, a, &evA, "P2P_ESTABLISHED", cmp.SessionID); ev.Path != "direct" {
		t.Fatalf("путь %q", ev.Path)
	}
	awaitP2P(t, b, &evB, "P2P_ESTABLISHED", acc.SessionID)

	p2pOK(t, a, actionP2PSend, map[string]any{"session_id": cmp.SessionID, "payload": []byte("hello over core")}, nil)
	if ev := awaitP2P(t, b, &evB, "P2P_MESSAGE", acc.SessionID); string(ev.Payload) != "hello over core" {
		t.Fatalf("B получил %q", ev.Payload)
	}
	p2pOK(t, b, actionP2PSend, map[string]any{"session_id": acc.SessionID, "payload": []byte("reply")}, nil)
	if ev := awaitP2P(t, a, &evA, "P2P_MESSAGE", cmp.SessionID); string(ev.Payload) != "reply" {
		t.Fatalf("A получил %q", ev.Payload)
	}

	// Дубль signalling: повторный complete — вердикт not_found, не вторая сессия.
	if env := p2pCall(t, a, actionP2PComplete, map[string]any{"negotiation_id": off.NegotiationID, "answer": acc.Answer}); env.OK || env.Error != p2pNotFound {
		t.Fatalf("повторный complete: %+v", env)
	}
	// Чужой session_id неотличим от несуществующего.
	if env := p2pCall(t, b, actionP2PSend, map[string]any{"session_id": cmp.SessionID, "payload": []byte("x")}); env.OK || env.Error != p2pNotFound {
		t.Fatalf("чужой session_id: %+v", env)
	}

	p2pOK(t, b, actionP2PClose, map[string]any{"session_id": acc.SessionID}, nil)
	if ev := awaitP2P(t, b, &evB, "P2P_CLOSED", acc.SessionID); ev.Reason != "local" {
		t.Fatalf("закрывший: %q", ev.Reason)
	}
	if ev := awaitP2P(t, a, &evA, "P2P_CLOSED", cmp.SessionID); ev.Reason != "remote" {
		t.Fatalf("собеседник: %q", ev.Reason)
	}
}

func TestP2PActionsWithoutSectionAreNotConfigured(t *testing.T) {
	d, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	n := p2pNode{d: d}
	for _, a := range []int32{actionP2POffer, actionP2PAccept, actionP2PComplete, actionP2PSend, actionP2PClose, actionP2PEvents,
		actionP2PStreamOpen, actionP2PStreamSend, actionP2PStreamClose} {
		if env := p2pCall(t, n, a, map[string]any{}); env.OK || env.Error != p2pNotConfigured {
			t.Fatalf("действие %d без секции p2p: %+v", a, env)
		}
	}
}

func TestP2PInvalidPayloadIsCallError(t *testing.T) {
	a := newP2PNode(t)
	_, err := a.d.SendAction(actionP2POffer, []byte("{не json"))
	if !errors.Is(err, session.ErrInvalidPayload) {
		t.Fatalf("негодный JSON: %v", err)
	}
	_, err = a.d.SendAction(actionP2POffer, []byte(`{}`)) // нет ключа собеседника
	if !errors.Is(err, session.ErrInvalidPayload) {
		t.Fatalf("без peer_public_key: %v", err)
	}
	if env := p2pCall(t, a, actionP2PAccept, map[string]any{"peer_public_key": []byte("k"), "offer": []byte("garbage")}); env.OK || env.Error != p2pFailed {
		t.Fatalf("негодный offer: %+v", env)
	}
}

func TestP2PDeviceKeyRequirements(t *testing.T) {
	dir := t.TempDir()
	mk := func(path string) error {
		raw, _ := json.Marshal(map[string]any{"p2p": map[string]any{"device_key_path": path}})
		cfg, err := ParseConfig(raw)
		if err != nil {
			return err
		}
		d, err := New(cfg)
		if err == nil {
			_ = d.Close()
		}
		return err
	}
	if err := mk(filepath.Join(dir, "absent.key")); err == nil {
		t.Fatal("отсутствующий device.key принят: Core не создаёт идентичность")
	}
	if _, err := os.Stat(filepath.Join(dir, "absent.key")); !os.IsNotExist(err) {
		t.Fatal("Core создал device.key — идентичность создаёт только ENGRAM (ADR-069 §5)")
	}
	path, _ := writeDeviceKey(t, dir)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := mk(path); err == nil {
		t.Fatal("device.key, доступный группе, принят")
	}
	bad := filepath.Join(dir, "bad.key")
	_ = os.WriteFile(bad, []byte("не base64!"), 0o600)
	if err := mk(bad); err == nil {
		t.Fatal("негодный device.key принят")
	}
	if _, err := ParseConfig([]byte(`{"p2p":{"device_key_path":"x","peer_loss_timeout_ms":-1}}`)); err == nil {
		t.Fatal("отрицательный срок принят")
	}
}

// Закрытие инстанса закрывает его P2P-сессии: собеседник видит remote, а не
// висит.
func TestDispatcherCloseShutsP2PSessions(t *testing.T) {
	a, b := newP2PNode(t), newP2PNode(t)
	var evB []p2pEvent
	var off struct {
		NegotiationID string `json:"negotiation_id"`
		Offer         []byte `json:"offer"`
	}
	p2pOK(t, a, actionP2POffer, map[string]any{"peer_public_key": b.pub}, &off)
	var acc struct {
		SessionID string `json:"session_id"`
		Answer    []byte `json:"answer"`
	}
	p2pOK(t, b, actionP2PAccept, map[string]any{"peer_public_key": a.pub, "offer": off.Offer}, &acc)
	p2pOK(t, a, actionP2PComplete, map[string]any{"negotiation_id": off.NegotiationID, "answer": acc.Answer}, nil)
	awaitP2P(t, b, &evB, "P2P_ESTABLISHED", acc.SessionID)
	_ = a.d.Close()
	if ev := awaitP2P(t, b, &evB, "P2P_CLOSED", acc.SessionID); ev.Reason != "remote" {
		t.Fatalf("причина %q", ev.Reason)
	}
}

// p2pPair устанавливает сессию через SendAction: A — инициатор.
func p2pPair(t *testing.T, a, b p2pNode, evA, evB *[]p2pEvent) (string, string) {
	t.Helper()
	var off struct {
		NegotiationID string `json:"negotiation_id"`
		Offer         []byte `json:"offer"`
	}
	p2pOK(t, a, actionP2POffer, map[string]any{"peer_public_key": b.pub}, &off)
	var acc struct {
		SessionID string `json:"session_id"`
		Answer    []byte `json:"answer"`
	}
	p2pOK(t, b, actionP2PAccept, map[string]any{"peer_public_key": a.pub, "offer": off.Offer}, &acc)
	var cmp struct {
		SessionID string `json:"session_id"`
	}
	p2pOK(t, a, actionP2PComplete, map[string]any{"negotiation_id": off.NegotiationID, "answer": acc.Answer}, &cmp)
	awaitP2P(t, a, evA, "P2P_ESTABLISHED", cmp.SessionID)
	awaitP2P(t, b, evB, "P2P_ESTABLISHED", acc.SessionID)
	return cmp.SessionID, acc.SessionID
}

// Служебные потоки через SendAction (866–868) рядом с потоком по умолчанию
// (863): события 865 несут stream_id и service, вердикты — закрытый список.
func TestP2PStreamActionsThroughSendAction(t *testing.T) {
	a, b := newP2PNode(t), newP2PNode(t)
	var evA, evB []p2pEvent
	sa, sb := p2pPair(t, a, b, &evA, &evB)
	awaitP2P(t, a, &evA, "P2P_STREAMS_READY", sa)
	awaitP2P(t, b, &evB, "P2P_STREAMS_READY", sb)

	var op struct {
		StreamID string `json:"stream_id"`
	}
	p2pOK(t, a, actionP2PStreamOpen, map[string]any{"session_id": sa, "service": "photon.call/1"}, &op)
	opened := awaitP2P(t, b, &evB, "P2P_STREAM_OPENED", sb)
	if opened.Service != "photon.call/1" || opened.StreamID == "" {
		t.Fatalf("OPENED: %+v", opened)
	}

	p2pOK(t, a, actionP2PStreamSend, map[string]any{"stream_id": op.StreamID, "payload": []byte("offer-sdp")}, nil)
	if ev := awaitP2P(t, b, &evB, "P2P_STREAM_MESSAGE", sb); string(ev.Payload) != "offer-sdp" || ev.StreamID != opened.StreamID {
		t.Fatalf("сообщение потока: %+v", ev)
	}
	p2pOK(t, b, actionP2PStreamSend, map[string]any{"stream_id": opened.StreamID, "payload": []byte("answer-sdp")}, nil)
	if ev := awaitP2P(t, a, &evA, "P2P_STREAM_MESSAGE", sa); string(ev.Payload) != "answer-sdp" || ev.StreamID != op.StreamID {
		t.Fatalf("ответ потока: %+v", ev)
	}

	// Поток по умолчанию не изменился.
	p2pOK(t, a, actionP2PSend, map[string]any{"session_id": sa, "payload": []byte("text")}, nil)
	if ev := awaitP2P(t, b, &evB, "P2P_MESSAGE", sb); string(ev.Payload) != "text" || ev.StreamID != "" {
		t.Fatalf("поток по умолчанию: %+v", ev)
	}

	for _, tc := range []struct {
		action  int32
		payload map[string]any
		want    string
	}{
		{actionP2PStreamOpen, map[string]any{"session_id": sa, "service": "Bad Name"}, p2pInvalidService},
		{actionP2PStreamOpen, map[string]any{"session_id": "nope", "service": "svc"}, p2pNotFound},
		{actionP2PStreamSend, map[string]any{"stream_id": "nope", "payload": []byte("x")}, p2pNotFound},
		{actionP2PStreamSend, map[string]any{"stream_id": op.StreamID, "payload": make([]byte, 64*1024+1)}, p2pTooLarge},
		{actionP2PStreamClose, map[string]any{"stream_id": "nope"}, p2pNotFound},
		// Чужой поток неотличим от несуществующего.
		{actionP2PStreamSend, map[string]any{"stream_id": opened.StreamID, "payload": []byte("x")}, p2pNotFound},
	} {
		if env := p2pCall(t, a, tc.action, tc.payload); env.OK || env.Error != tc.want {
			t.Fatalf("действие %d %v: %+v, ожидался %s", tc.action, tc.payload, env, tc.want)
		}
	}

	p2pOK(t, a, actionP2PStreamClose, map[string]any{"stream_id": op.StreamID}, nil)
	if ev := awaitP2P(t, a, &evA, "P2P_STREAM_CLOSED", sa); ev.Reason != "local" {
		t.Fatalf("закрывший поток: %+v", ev)
	}
	if ev := awaitP2P(t, b, &evB, "P2P_STREAM_CLOSED", sb); ev.Reason != "remote" {
		t.Fatalf("собеседник потока: %+v", ev)
	}
	// Сессия жива.
	p2pOK(t, b, actionP2PSend, map[string]any{"session_id": sb, "payload": []byte("still")}, nil)
	if ev := awaitP2P(t, a, &evA, "P2P_MESSAGE", sa); string(ev.Payload) != "still" {
		t.Fatalf("после закрытия потока: %+v", ev)
	}
}
