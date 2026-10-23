package coreops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	ed25519adapter "github.com/wiloris/photon-core/adapters/ed25519"
	pionadapter "github.com/wiloris/photon-core/adapters/pion"
	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-core/services/p2phost"
	"github.com/wiloris/photon-plugin-runtime/coreops/internal/legacy"
	"github.com/wiloris/photon-plugin-runtime/ipc"
)

// Совместимость coreops поверх единого исполнителя Core со СТАРЫМ coreops —
// неизменённым пакетом из 8da6ee7 (internal/legacy, хеши сверяются ниже),
// через настоящий adapters/pion. Провод ADR-076: без «привета», каждый кадр
// — p2p.message, включая пустой первый.

// legacyHashes — sha256 файлов coreops в 8da6ee7; копии отличаются только
// строкой package.
var legacyHashes = map[string]string{
	"coreops.go": "8e30ab5aa101e1a352c2d897bfcd2afc09f27af1838c4b5aa01216534b6f73ba",
	"keys.go":    "c18273625b1397d338ce676e21e0863be0c40908affe7bd6b0da4749f74b8e8b",
	"p2p.go":     "a439e4f66b128b0f399e8169fab7a05f6fe796014875ada6d66163059d785d11",
}

func TestLegacyCoreopsCopyIsVerbatim(t *testing.T) {
	for name, want := range legacyHashes {
		raw, err := os.ReadFile("internal/legacy/" + name)
		if err != nil {
			t.Fatal(err)
		}
		orig := bytes.Replace(raw, []byte("package legacy\n"), []byte("package coreops\n"), 1)
		sum := sha256.Sum256(orig)
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Fatalf("копия старого coreops изменена: %s → %s", name, got)
		}
	}
}

// compatNode — плагин-сторона одного хоста: исполнитель операций и его
// события.
type compatNode struct {
	h      ipc.Handler
	pub    ports.PublicKey
	sink   *eventSink
	plugin string
}

func (n compatNode) call(t *testing.T, op string, req, out any) {
	t.Helper()
	payload, _ := json.Marshal(req)
	raw, err := n.h.Handle(context.Background(), ipc.CallerInfo{PluginID: n.plugin}, op, payload)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatal(err)
		}
	}
}

// pionFor — настоящий адаптер композиции со своей идентичностью.
func pionFor(t *testing.T) (ports.P2P, ports.PublicKey) {
	t.Helper()
	id := ed25519adapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	a, err := pionadapter.New(pionadapter.Config{Identity: id, Self: pair, EstablishTimeout: 15 * time.Second, PeerLossTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a, pair.Public
}

func newCompatNode(t *testing.T, plugin string) compatNode {
	t.Helper()
	a, pub := pionFor(t)
	sink := newSink()
	return compatNode{h: newP2PHandler(t, a, sink), pub: pub, sink: sink, plugin: plugin}
}

func newLegacyCompatNode(t *testing.T, plugin string) compatNode {
	t.Helper()
	a, pub := pionFor(t)
	sink := newSink()
	c := p2pConfig(t, a, sink)
	h, err := legacy.New(legacy.Config{
		Storage: c.Storage, StorageNamespace: c.StorageNamespace, Identity: c.Identity,
		DeviceKey: c.DeviceKey, Crypto: c.Crypto, DeriveKey: c.DeriveKey, Log: c.Log,
		P2P: c.P2P, EmitEvent: c.EmitEvent, P2PEstablishTimeout: c.P2PEstablishTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	return compatNode{h: h, pub: pub, sink: sink, plugin: plugin}
}

// compatPair: ini — инициатор, rsp — отвечающий; signaling — сам тест.
func compatPair(t *testing.T, ini, rsp compatNode) (string, string) {
	t.Helper()
	var off p2pCreateOfferResponse
	ini.call(t, "p2p.create_offer", p2pCreateOfferRequest{PeerPubKey: rsp.pub}, &off)
	var acc p2pAcceptOfferResponse
	rsp.call(t, "p2p.accept_offer", p2pAcceptOfferRequest{PeerPubKey: ini.pub, Offer: off.Offer}, &acc)
	var cmp p2pCompleteResponse
	ini.call(t, "p2p.complete", p2pCompleteRequest{NegotiationID: off.NegotiationID, Answer: acc.Answer}, &cmp)
	return cmp.SessionID, acc.SessionID
}

func expectMessage(t *testing.T, n compatNode, sid string, want []byte) {
	t.Helper()
	e := n.sink.waitFor(t, eventMessage)
	var m p2pMessageEvent
	if err := json.Unmarshal(e.payload, &m); err != nil {
		t.Fatal(err)
	}
	if m.SessionID != sid || !bytes.Equal(m.Payload, want) {
		t.Fatalf("p2p.message %+v, ожидалось %q в %s", m, want, sid)
	}
}

func compatExchange(t *testing.T, ini, rsp compatNode) {
	t.Helper()
	si, sr := compatPair(t, ini, rsp)

	// p2p.peer у обеих сторон — подтверждённый ключ собеседника.
	var pi, pr p2pPeerResponse
	ini.call(t, "p2p.peer", p2pSessionRequest{SessionID: si}, &pi)
	rsp.call(t, "p2p.peer", p2pSessionRequest{SessionID: sr}, &pr)
	if !bytes.Equal(pi.PeerPubKey, rsp.pub) || !bytes.Equal(pr.PeerPubKey, ini.pub) {
		t.Fatal("p2p.peer вернул не подтверждённый ключ собеседника")
	}

	// Первый кадр — пустой: на проводе ADR-076 он доставляется как есть.
	// Старый узел, получи он «привет», доставил бы лишний пустой кадр, и
	// следующий ожидаемый не совпал бы.
	for _, msg := range [][]byte{{}, []byte("one"), []byte("two")} {
		ini.call(t, "p2p.send", p2pSendRequest{SessionID: si, Payload: msg}, nil)
		expectMessage(t, rsp, sr, msg)
	}
	for _, msg := range [][]byte{[]byte("back"), {}} {
		rsp.call(t, "p2p.send", p2pSendRequest{SessionID: sr, Payload: msg}, nil)
		expectMessage(t, ini, si, msg)
	}

	ini.call(t, "p2p.close", p2pSessionRequest{SessionID: si}, nil)
	var ci, cr p2pClosedEvent
	_ = json.Unmarshal(ini.sink.waitFor(t, eventClosed).payload, &ci)
	_ = json.Unmarshal(rsp.sink.waitFor(t, eventClosed).payload, &cr)
	if ci.Reason != reasonLocal || cr.Reason != reasonRemote {
		t.Fatalf("причины: закрывший %q, собеседник %q", ci.Reason, cr.Reason)
	}
	// Лишних p2p.message не было ни у кого.
	for _, n := range []compatNode{ini, rsp} {
		n.sink.mu.Lock()
		count := 0
		for _, e := range n.sink.got {
			if e.typ == eventMessage {
				count++
			}
		}
		n.sink.mu.Unlock()
		if want := map[bool]int{true: 2, false: 3}[n.plugin == ini.plugin]; count != want {
			t.Fatalf("%s: сообщений %d, ожидалось %d", n.plugin, count, want)
		}
	}
}

func TestCompatNewInitiatorLegacyResponder(t *testing.T) {
	compatExchange(t, newCompatNode(t, "plugin.new"), newLegacyCompatNode(t, "plugin.old"))
}

func TestCompatLegacyInitiatorNewResponder(t *testing.T) {
	compatExchange(t, newLegacyCompatNode(t, "plugin.old"), newCompatNode(t, "plugin.new"))
}

func TestCompatNewNewOverRealPion(t *testing.T) {
	compatExchange(t, newCompatNode(t, "plugin.a"), newCompatNode(t, "plugin.b"))
}

// Эталон: старый со старым — тест меряет прежнее поведение, а не своё
// представление о нём.
func TestCompatLegacyLegacyBaseline(t *testing.T) {
	compatExchange(t, newLegacyCompatNode(t, "plugin.a"), newLegacyCompatNode(t, "plugin.b"))
}

// Изоляция владельцев на одном исполнителе: события сессии приходят только
// её плагину, чужой handle — NOT_FOUND, остановка одного плагина не трогает
// сессии другого.
func TestOwnersAreIsolatedOnOneExecutor(t *testing.T) {
	a, apub := pionFor(t)
	sink := newSink()
	host := newP2PHandler(t, a, sink)
	n1 := compatNode{h: host, pub: apub, sink: sink, plugin: "plugin.one"}
	n2 := compatNode{h: host, pub: apub, sink: sink, plugin: "plugin.two"}
	peer := newCompatNode(t, "plugin.peer")

	s1, p1 := compatPair(t, n1, peer)
	s2, p2 := compatPair(t, n2, peer)

	peer.call(t, "p2p.send", p2pSendRequest{SessionID: p1, Payload: []byte("to-one")}, nil)
	peer.call(t, "p2p.send", p2pSendRequest{SessionID: p2, Payload: []byte("to-two")}, nil)
	deadline := time.Now().Add(10 * time.Second)
	got := map[string]string{}
	for len(got) < 2 && time.Now().Before(deadline) {
		sink.mu.Lock()
		for _, e := range sink.got {
			if e.typ == eventMessage {
				var m p2pMessageEvent
				_ = json.Unmarshal(e.payload, &m)
				got[e.pluginID+"/"+m.SessionID] = string(m.Payload)
			}
		}
		sink.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	if got["plugin.one/"+s1] != "to-one" || got["plugin.two/"+s2] != "to-two" || len(got) != 2 {
		t.Fatalf("события ушли не своим владельцам: %v", got)
	}

	// Чужой handle неотличим от несуществующего (ADR-076 §3.7).
	_, err := call(t, host, "plugin.two", "p2p.send", p2pSendRequest{SessionID: s1, Payload: []byte("x")})
	requireCode(t, err, ipc.ErrNotFound, "send в чужую сессию")
	_, err = call(t, host, "plugin.two", "p2p.close", p2pSessionRequest{SessionID: s1})
	requireCode(t, err, ipc.ErrNotFound, "close чужой сессии")

	// Остановка plugin.one: его сессия закрыта plugin_stopped, сессия
	// plugin.two жива.
	host.ClosePluginSessions("plugin.one")
	var closed p2pClosedEvent
	e := sink.waitFor(t, eventClosed)
	_ = json.Unmarshal(e.payload, &closed)
	if e.pluginID != "plugin.one" || closed.SessionID != s1 || closed.Reason != reasonPluginStopped {
		t.Fatalf("закрытие при остановке: %s %+v", e.pluginID, closed)
	}
	n2.call(t, "p2p.send", p2pSendRequest{SessionID: s2, Payload: []byte("alive")}, nil)
	expectMessage(t, peer, p2, []byte("alive"))
}

// Провод ADR-076 не получает служебных потоков: исполнитель coreops не
// открывает ни одного uni-потока (объявления photon-streams нет).
type uniCountingSession struct {
	ports.Session
	mu  sync.Mutex
	uni int
}

func (s *uniCountingSession) OpenUniStream(ctx context.Context) (ports.SendStream, error) {
	s.mu.Lock()
	s.uni++
	s.mu.Unlock()
	return s.Session.OpenUniStream(ctx)
}

type uniCountingP2P struct {
	ports.P2P
	mu       sync.Mutex
	sessions []*uniCountingSession
}

func (p *uniCountingP2P) wrap(s ports.Session) ports.Session {
	w := &uniCountingSession{Session: s}
	p.mu.Lock()
	p.sessions = append(p.sessions, w)
	p.mu.Unlock()
	return w
}
func (p *uniCountingP2P) AcceptOffer(ctx context.Context, peer ports.PublicKey, offer []byte) ([]byte, ports.Session, error) {
	a, s, err := p.P2P.AcceptOffer(ctx, peer, offer)
	if err != nil {
		return nil, nil, err
	}
	return a, p.wrap(s), nil
}
func (p *uniCountingP2P) CompleteSession(ctx context.Context, peer ports.PublicKey, answer []byte) (ports.Session, error) {
	s, err := p.P2P.CompleteSession(ctx, peer, answer)
	if err != nil {
		return nil, err
	}
	return p.wrap(s), nil
}

func TestCoreopsWireHasNoServiceStreamAnnouncement(t *testing.T) {
	newCounted := func(plugin string) (compatNode, *uniCountingP2P) {
		a, pub := pionFor(t)
		c := &uniCountingP2P{P2P: a}
		sink := newSink()
		return compatNode{h: newP2PHandler(t, c, sink), pub: pub, sink: sink, plugin: plugin}, c
	}
	ini, ci := newCounted("plugin.a")
	rsp, cr := newCounted("plugin.b")
	si, sr := compatPair(t, ini, rsp)
	var p p2pPeerResponse
	ini.call(t, "p2p.peer", p2pSessionRequest{SessionID: si}, &p)
	rsp.call(t, "p2p.peer", p2pSessionRequest{SessionID: sr}, &p)
	ini.call(t, "p2p.send", p2pSendRequest{SessionID: si, Payload: []byte("x")}, nil)
	expectMessage(t, rsp, sr, []byte("x"))
	time.Sleep(200 * time.Millisecond)
	for _, c := range []*uniCountingP2P{ci, cr} {
		c.mu.Lock()
		for _, s := range c.sessions {
			s.mu.Lock()
			if s.uni != 0 {
				t.Fatalf("coreops открыл %d uni-потоков — провод ADR-076 изменён", s.uni)
			}
			s.mu.Unlock()
		}
		c.mu.Unlock()
	}
}

// ADR-076 §7, решение 9: лимита сессий нет — больше восьми (предел хоста
// приложения) у плагина допустимо.
func TestCoreopsHasNoSessionLimit(t *testing.T) {
	ini := newCompatNode(t, "plugin.a")
	rsp := newCompatNode(t, "plugin.b")
	for i := 0; i < p2phost.MaxSessions+2; i++ {
		compatPair(t, ini, rsp)
	}
}

// ADR-076 §3.6: незавершённое согласование освобождается по
// EstablishTimeout, а не живёт до остановки плагина; после этого его handle
// неотличим от неизвестного (§3.7) — NOT_FOUND. Старый coreops держал запись
// до остановки и отвечал INVALID_REQUEST («answer rejected»).
func TestExpiredNegotiationIsNotFound(t *testing.T) {
	a, _ := pionFor(t)
	cfg := p2pConfig(t, a, newSink())
	cfg.P2PEstablishTimeout = 300 * time.Millisecond
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, peer := pionFor(t)
	var off p2pCreateOfferResponse
	callInto(t, h, "plugin.a", "p2p.create_offer", p2pCreateOfferRequest{PeerPubKey: peer}, &off)
	time.Sleep(600 * time.Millisecond)
	_, err = call(t, h, "plugin.a", "p2p.complete", p2pCompleteRequest{NegotiationID: off.NegotiationID, Answer: []byte("answer")})
	requireCode(t, err, ipc.ErrNotFound, "complete после истечения согласования")
}
