// Command p2pdial — фикстура условия 6B ADR-076.
//
// Доказываемая цепочка целиком, без единого фиктивного слоя между двумя
// плагинами:
//
//	Plugin A → IPC → coreops.p2p → adapters/pion → ICE → DTLS → SCTP →
//	p2pmsg → SCTP → DTLS → ICE → adapters/pion → coreops.p2p → IPC → Plugin B
//
// Говорит по проводу, а не через SDK, по той же причине, что p2pguard и
// permission: типизированный API SDK операций p2p.* не выражает, а
// недоверенный плагин пользоваться SDK не обязан.
//
// Signaling — TCP-рандеву, поднятое ТЕСТОМ. Это иллюстрация ADR-042
// («доставку блобов выполняет вызывающий»), а не часть платформы: ровно то
// же, что рандеву в tests/p2pcheck и tests/p2pslice.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/wiloris/photon-plugin-runtime/ipc"
)

func role() string       { return os.Getenv("PHOTON_TEST_ROLE") }
func rendezvous() string { return os.Getenv("PHOTON_TEST_RENDEZVOUS") }
func wrongPeer() bool    { return os.Getenv("PHOTON_TEST_WRONG_PEER") == "1" }
func pluginID() string   { return os.Getenv("PHOTON_TEST_PLUGIN_ID") }

// payloads — то, что A отправляет, а B сверяет ПОБАЙТОВО.
//
// Вычисляется одной функцией у обеих сторон: это один и тот же бинарь, и
// расхождение ожиданий невозможно по построению.
func payloads() [][]byte {
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	return [][]byte{
		{},                           // пустой
		[]byte("hello from A"),       // обычный
		{'a', 0x00, 0x00, 'b', 0x00}, // нули внутри
		{0xff, 0xfe, 0xc0, 0x80, 0xed, 0xa0, 0x80}, // невалидный UTF-8
		all,                                                  // все 256 значений
		[]byte("tail 1"), []byte("tail 2"), []byte("tail 3"), // подряд
	}
}

func main() {
	rw := stdio{}
	if err := rawHandshake(rw); err != nil {
		os.Exit(1)
	}
	c := &conn{rw: rw}

	switch role() {
	case "initiator":
		initiator(c)
	case "responder":
		responder(c)
	default:
		c.log("p2p_e2e:bad-role")
	}
	c.log("p2p_e2e:done")
	c.workComplete()
	c.waitForStop()
}

// ── инициатор ───────────────────────────────────────────────────────

func initiator(c *conn) {
	self := c.ownPublicKey()
	if self == nil {
		c.log("p2p_e2e:no-identity")
		return
	}
	rv, err := dialRendezvous()
	if err != nil {
		c.log("p2p_e2e:rendezvous-failed")
		return
	}
	defer rv.Close()

	peer, err := rv.exchange(self)
	if err != nil {
		c.log("p2p_e2e:key-exchange-failed")
		return
	}
	expected := peer
	if wrongPeer() {
		// Портим ОЖИДАНИЕ, а не сам ключ на проводе: handshake ADR-041
		// обязан отвергнуть согласование, потому что transcript включает
		// оба публичных ключа и подписи перестают сходиться у ОБЕИХ сторон.
		expected = flip(peer)
		c.log("p2p_e2e:expect-wrong-peer")
	}

	var offer struct {
		NegotiationID string `json:"negotiation_id"`
		Offer         []byte `json:"offer"`
	}
	if !c.callInto("p2p.create_offer", map[string]any{"peer_pubkey": expected}, &offer) {
		return
	}
	c.log("p2p_e2e:offer_bytes=" + itoa(len(offer.Offer)))

	if err := rv.send("offer", offer.Offer); err != nil {
		c.log("p2p_e2e:offer-send-failed")
		return
	}
	answer, err := rv.recv("answer")
	if err != nil {
		c.log("p2p_e2e:answer-recv-failed")
		return
	}

	var done struct {
		SessionID string `json:"session_id"`
	}
	if !c.callInto("p2p.complete",
		map[string]any{"negotiation_id": offer.NegotiationID, "answer": answer}, &done) {
		return
	}
	c.log("p2p_e2e:session=established-call-ok")

	// p2p.peer — post-establishment query: блокирует до established либо
	// терминального исхода.
	var pr struct {
		PeerPubKey []byte `json:"peer_pubkey"`
	}
	raw, code := c.callRaw("p2p.peer", map[string]any{"session_id": done.SessionID})
	if code != "" {
		c.log("p2p_e2e:peer=denied:" + code)
		return
	}
	_ = json.Unmarshal(raw, &pr)
	if !bytes.Equal(pr.PeerPubKey, peer) {
		c.log("p2p_e2e:peer=mismatch")
		return
	}
	c.log("p2p_e2e:peer=confirmed")

	for i, p := range payloads() {
		if code := c.call("p2p.send",
			map[string]any{"session_id": done.SessionID, "payload": p}); code != "" {
			c.log("p2p_e2e:send_" + itoa(i) + "=" + code)
			return
		}
	}
	c.log("p2p_e2e:sent=" + itoa(len(payloads())))

	// Закрывает ОТВЕЧАЮЩИЙ; инициатор обязан узнать об этом событием.
	ev, ok := c.waitEvent("p2p.closed", 25*time.Second)
	if !ok {
		c.log("p2p_e2e:closed=timeout")
		return
	}
	reason, _ := ev["reason"].(string)
	c.log("p2p_e2e:closed=" + reason)
}

// ── отвечающий ──────────────────────────────────────────────────────

func responder(c *conn) {
	self := c.ownPublicKey()
	if self == nil {
		c.log("p2p_e2e:no-identity")
		return
	}
	rv, err := dialRendezvous()
	if err != nil {
		c.log("p2p_e2e:rendezvous-failed")
		return
	}
	defer rv.Close()

	peer, err := rv.exchange(self)
	if err != nil {
		c.log("p2p_e2e:key-exchange-failed")
		return
	}
	offer, err := rv.recv("offer")
	if err != nil {
		c.log("p2p_e2e:offer-recv-failed")
		return
	}

	var acc struct {
		SessionID string `json:"session_id"`
		Answer    []byte `json:"answer"`
	}
	if !c.callInto("p2p.accept_offer",
		map[string]any{"peer_pubkey": peer, "offer": offer}, &acc) {
		return
	}
	if err := rv.send("answer", acc.Answer); err != nil {
		c.log("p2p_e2e:answer-send-failed")
		return
	}

	var pr struct {
		PeerPubKey []byte `json:"peer_pubkey"`
	}
	raw, code := c.callRaw("p2p.peer", map[string]any{"session_id": acc.SessionID})
	if code != "" {
		c.log("p2p_e2e:peer=denied:" + code)
		return
	}
	_ = json.Unmarshal(raw, &pr)
	if !bytes.Equal(pr.PeerPubKey, peer) {
		c.log("p2p_e2e:peer=mismatch")
		return
	}
	c.log("p2p_e2e:peer=confirmed")

	// Сообщения принимаются СОБЫТИЯМИ и сверяются побайтово И ПО ПОРЯДКУ:
	// i-е принятое обязано равняться i-му отправленному.
	want := payloads()
	for i, w := range want {
		ev, ok := c.waitEvent("p2p.message", 25*time.Second)
		if !ok {
			c.log("p2p_e2e:recv_" + itoa(i) + "=timeout")
			return
		}
		if id, _ := ev["session_id"].(string); id != acc.SessionID {
			c.log("p2p_e2e:recv_" + itoa(i) + "=wrong-session")
			return
		}
		got := decodeB64(ev["payload"])
		if !bytes.Equal(got, w) {
			c.log("p2p_e2e:recv_" + itoa(i) + "=mismatch:" + itoa(len(got)) + "/" + itoa(len(w)))
			return
		}
	}
	c.log("p2p_e2e:received=" + itoa(len(want)))

	if code := c.call("p2p.close", map[string]any{"session_id": acc.SessionID}); code != "" {
		c.log("p2p_e2e:close=" + code)
		return
	}
	c.log("p2p_e2e:close=ok")
}

// ── рандеву ─────────────────────────────────────────────────────────

type rv struct {
	c   net.Conn
	enc *json.Encoder
	dec *json.Decoder
}

type rvMsg struct {
	Kind string `json:"kind"`
	Data []byte `json:"data"`
}

func dialRendezvous() (*rv, error) {
	c, err := net.DialTimeout("tcp", rendezvous(), 20*time.Second)
	if err != nil {
		return nil, err
	}
	return &rv{c: c, enc: json.NewEncoder(c), dec: json.NewDecoder(c)}, nil
}

func (r *rv) Close() { _ = r.c.Close() }

func (r *rv) send(kind string, data []byte) error {
	return r.enc.Encode(rvMsg{Kind: kind, Data: data})
}

func (r *rv) recv(kind string) ([]byte, error) {
	for {
		var m rvMsg
		if err := r.dec.Decode(&m); err != nil {
			return nil, err
		}
		if m.Kind == kind {
			return m.Data, nil
		}
	}
}

// exchange — discovery, лежащее ВНЕ P2P-порта: порт требует знать ключ
// peer'а уже в create_offer (ports/p2p.go, ADR-017/018).
func (r *rv) exchange(self []byte) ([]byte, error) {
	if err := r.send("key", self); err != nil {
		return nil, err
	}
	return r.recv("key")
}

// ── провод IPC ──────────────────────────────────────────────────────

type stdio struct{}

func (stdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }

func rawHandshake(rw io.ReadWriter) error {
	hello, err := ipc.Decode(rw)
	if err != nil {
		return fmt.Errorf("read HELLO: %w", err)
	}
	if hello.Type != ipc.MsgHello {
		return fmt.Errorf("expected HELLO, got %s", hello.Type)
	}
	return ipc.Encode(rw, &ipc.Message{
		Type:                 ipc.MsgHelloAck,
		HelloProtocolVersion: hello.HelloProtocolVersion,
		HelloPluginID:        pluginID(),
	})
}

type conn struct {
	rw     io.ReadWriter
	next   uint64
	events []*ipc.Message // события, пришедшие между запросом и ответом
	stop   bool
}

// pump читает одно сообщение, складывая события в буфер.
//
// События НЕ отбрасываются: p2p.message приходит именно ими, и «прочитать
// следующее сообщение» потеряло бы их между запросом и ответом.
func (c *conn) pump() (*ipc.Message, error) {
	m, err := ipc.Decode(c.rw)
	if err != nil {
		return nil, err
	}
	switch m.Type {
	case ipc.MsgEvent:
		c.events = append(c.events, m)
	case ipc.MsgStop:
		c.stop = true
		_ = ipc.Encode(c.rw, &ipc.Message{Type: ipc.MsgStopAck})
	}
	return m, nil
}

func (c *conn) callRaw(op string, body any) ([]byte, string) {
	c.next++
	id := c.next
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, "MARSHAL_FAILED"
	}
	if err := ipc.Encode(c.rw, &ipc.Message{
		Type: ipc.MsgRequest, RequestID: id, Op: op, Payload: payload,
	}); err != nil {
		return nil, "WRITE_FAILED"
	}
	for {
		m, err := c.pump()
		if err != nil {
			return nil, "READ_FAILED"
		}
		if m.Type != ipc.MsgResponse || m.RequestID != id {
			continue
		}
		if m.ErrorCode != "" {
			return nil, string(m.ErrorCode)
		}
		return m.Payload, ""
	}
}

func (c *conn) call(op string, body any) string {
	_, code := c.callRaw(op, body)
	return code
}

func (c *conn) callInto(op string, body any, out any) bool {
	raw, code := c.callRaw(op, body)
	if code != "" {
		c.log("p2p_e2e:" + op + "=" + code)
		return false
	}
	if err := json.Unmarshal(raw, out); err != nil {
		c.log("p2p_e2e:" + op + "=malformed")
		return false
	}
	return true
}

// waitEvent достаёт событие нужного типа из буфера либо дожидается его.
func (c *conn) waitEvent(typ string, timeout time.Duration) (map[string]any, bool) {
	deadline := time.Now().Add(timeout)
	for {
		for i, m := range c.events {
			if m.EventType == typ {
				c.events = append(c.events[:i], c.events[i+1:]...)
				var body map[string]any
				_ = json.Unmarshal(m.Payload, &body)
				return body, true
			}
		}
		if c.stop || time.Now().After(deadline) {
			return nil, false
		}
		if _, err := c.pump(); err != nil {
			return nil, false
		}
	}
}

func (c *conn) ownPublicKey() []byte {
	var resp struct {
		PublicKey []byte `json:"public_key"`
	}
	raw, code := c.callRaw("identity.public_key", map[string]any{})
	if code != "" {
		return nil
	}
	_ = json.Unmarshal(raw, &resp)
	return resp.PublicKey
}

func (c *conn) log(msg string) {
	c.next++
	id := c.next
	payload, _ := json.Marshal(map[string]string{"level": "info", "message": msg})
	_ = ipc.Encode(c.rw, &ipc.Message{
		Type: ipc.MsgRequest, RequestID: id, Op: "log.write", Payload: payload,
	})
	for {
		m, err := c.pump()
		if err != nil {
			return
		}
		if m.Type == ipc.MsgResponse && m.RequestID == id {
			return
		}
	}
}

func (c *conn) workComplete() {
	c.next++
	id := c.next
	payload, _ := json.Marshal(map[string]any{"status": "success"})
	_ = ipc.Encode(c.rw, &ipc.Message{
		Type: ipc.MsgRequest, RequestID: id, Op: "work.complete", Payload: payload,
	})
	for {
		m, err := c.pump()
		if err != nil {
			return
		}
		if m.Type == ipc.MsgResponse && m.RequestID == id {
			return
		}
	}
}

func (c *conn) waitForStop() {
	for !c.stop {
		if _, err := c.pump(); err != nil {
			return
		}
	}
}

// ── мелочи ──────────────────────────────────────────────────────────

func flip(k []byte) []byte {
	out := make([]byte, len(k))
	copy(out, k)
	out[0] ^= 0xff
	return out
}

func itoa(n int) string { return fmt.Sprintf("%d", n) }

// decodeB64 — encoding/json кодирует []byte в base64, и обратно из
// map[string]any он приходит строкой.
func decodeB64(v any) []byte {
	s, ok := v.(string)
	if !ok {
		return nil
	}
	var out []byte
	if err := json.Unmarshal([]byte(`"`+s+`"`), &out); err != nil {
		return nil
	}
	return out
}
