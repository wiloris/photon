package p2phost

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/pion/turn/v3"
	edadapter "github.com/wiloris/photon-core/adapters/ed25519"
	pionadapter "github.com/wiloris/photon-core/adapters/pion"
	"github.com/wiloris/photon-core/ports"
)

func newExec(t *testing.T, mod func(*Config)) (*Executor, ports.IdentityPair) {
	t.Helper()
	id := edadapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Identity: id, Self: pair, EstablishTimeout: 15 * time.Second, PeerLossTimeout: 3 * time.Second}
	if mod != nil {
		mod(&cfg)
	}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Shutdown)
	return e, pair
}

// seen — события, уже забранные опросом, но ещё не востребованные тестом:
// ожидание одного события не должно выбрасывать остальные.
var seen = map[*Executor][]Event{}

// waitEvent ждёт событие нужного типа для сессии; прочие сохраняются.
func waitEvent(t *testing.T, e *Executor, typ, sid string, d time.Duration) Event {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		buf := seen[e]
		for i, ev := range buf {
			if ev.Type == typ && (sid == "" || ev.SessionID == sid) {
				seen[e] = append(buf[:i:i], buf[i+1:]...)
				return ev
			}
			if ev.Type == EventClosed && typ != EventClosed && ev.SessionID == sid {
				t.Fatalf("ждали %s, а сессия закрылась: %s", typ, ev.Reason)
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("не дождались события %s для %s", typ, sid)
		}
		seen[e] = append(seen[e], e.Poll(MaxPoll)...)
		time.Sleep(20 * time.Millisecond)
	}
}

// pair проводит три шага: A — инициатор, B — отвечающий.
func pair(t *testing.T, a, b *Executor, pa, pb ports.IdentityPair, turnA, turnB *TURN) (string, string) {
	t.Helper()
	ctx := context.Background()
	neg, offer, err := a.Offer(ctx, pb.Public, turnA)
	if err != nil {
		t.Fatalf("Offer: %v", err)
	}
	sb, answer, err := b.Accept(ctx, pa.Public, offer, turnB)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	sa, err := a.Complete(ctx, neg, answer)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	return sa, sb
}

func TestEstablishExchangeAndClose(t *testing.T) {
	a, pa := newExec(t, nil)
	b, pb := newExec(t, nil)
	sa, sb := pair(t, a, b, pa, pb, nil, nil)
	evA := waitEvent(t, a, EventEstablished, sa, 20*time.Second)
	waitEvent(t, b, EventEstablished, sb, 20*time.Second)
	if evA.Path != "direct" {
		t.Fatalf("на loopback путь %q, ожидался direct", evA.Path)
	}

	// Отвечающий пишет первым — поток уже принят благодаря кадру-«привету».
	if err := b.Send(sb, []byte("привет от B")); err != nil {
		t.Fatalf("B.Send: %v", err)
	}
	if got := waitEvent(t, a, EventMessage, sa, 10*time.Second); string(got.Payload) != "привет от B" {
		t.Fatalf("A получил %q", got.Payload)
	}
	bin := []byte{0, 1, 2, 255, 0}
	if err := a.Send(sa, bin); err != nil {
		t.Fatalf("A.Send: %v", err)
	}
	if got := waitEvent(t, b, EventMessage, sb, 10*time.Second); !bytes.Equal(got.Payload, bin) {
		t.Fatalf("B получил %v", got.Payload)
	}
	// Пустое сообщение после «привета» доходит как пустое, не проглатывается.
	if err := a.Send(sa, nil); err != nil {
		t.Fatal(err)
	}
	if got := waitEvent(t, b, EventMessage, sb, 10*time.Second); len(got.Payload) != 0 {
		t.Fatalf("пустое сообщение пришло как %v", got.Payload)
	}

	if err := a.Close(sa); err != nil {
		t.Fatal(err)
	}
	if ev := waitEvent(t, a, EventClosed, sa, 5*time.Second); ev.Reason != ReasonLocal {
		t.Fatalf("закрывший видит %q", ev.Reason)
	}
	if ev := waitEvent(t, b, EventClosed, sb, 15*time.Second); ev.Reason != ReasonRemote {
		t.Fatalf("собеседник видит %q, ожидался remote", ev.Reason)
	}
	if err := a.Send(sa, []byte("x")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Send после закрытия: %v", err)
	}
	if err := a.Close(sa); !errors.Is(err, ErrNotFound) {
		t.Fatalf("повторный Close: %v", err)
	}
}

// Ожидаемый ключ — не ключ собеседника: handshake ADR-041 отвергает, сессия
// не устанавливается ни у одной стороны.
func TestWrongPeerKeyNeverEstablishes(t *testing.T) {
	a, pa := newExec(t, nil)
	b, _ := newExec(t, nil)
	_, stranger := newExec(t, nil)
	ctx := context.Background()
	neg, offer, err := a.Offer(ctx, stranger.Public, nil) // A ждёт чужой ключ
	if err != nil {
		t.Fatal(err)
	}
	sb, answer, err := b.Accept(ctx, pa.Public, offer, nil)
	if err != nil {
		t.Fatal(err)
	}
	sa, err := a.Complete(ctx, neg, answer)
	if err != nil {
		t.Fatal(err)
	}
	if ev := waitEvent(t, a, EventClosed, sa, 20*time.Second); ev.Reason != ReasonFailed {
		t.Fatalf("A: %q", ev.Reason)
	}
	if ev := waitEvent(t, b, EventClosed, sb, 20*time.Second); ev.Reason != ReasonFailed {
		t.Fatalf("B: %q", ev.Reason)
	}
}

func TestCompleteIsSingleUse(t *testing.T) {
	a, pa := newExec(t, nil)
	b, pb := newExec(t, nil)
	ctx := context.Background()
	neg, offer, err := a.Offer(ctx, pb.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, answer, err := b.Accept(ctx, pa.Public, offer, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Complete(ctx, neg, answer); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Complete(ctx, neg, answer); !errors.Is(err, ErrNotFound) {
		t.Fatalf("повторный complete (дубль signalling): %v", err)
	}
	if _, err := a.Complete(ctx, "unknown", answer); !errors.Is(err, ErrNotFound) {
		t.Fatalf("неизвестный negotiation_id: %v", err)
	}
}

// Ответ не пришёл никогда — согласование освобождается по сроку.
func TestPendingNegotiationExpires(t *testing.T) {
	a, _ := newExec(t, func(c *Config) { c.EstablishTimeout = time.Second })
	_, pb := newExec(t, nil)
	neg, _, err := a.Offer(context.Background(), pb.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, n := a.Counts(); n != 1 {
		t.Fatalf("согласований %d", n)
	}
	time.Sleep(1500 * time.Millisecond)
	if _, n := a.Counts(); n != 0 {
		t.Fatal("согласование пережило свой срок")
	}
	if _, err := a.Complete(context.Background(), neg, []byte("late")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("опоздавший answer: %v", err)
	}
}

func TestArgumentAndStateErrors(t *testing.T) {
	a, _ := newExec(t, nil)
	ctx := context.Background()
	if _, _, err := a.Offer(ctx, nil, nil); !errors.Is(err, ErrBadArgument) {
		t.Fatal(err)
	}
	if _, _, err := a.Accept(ctx, []byte("k"), nil, nil); !errors.Is(err, ErrBadArgument) {
		t.Fatal(err)
	}
	if err := a.Send("nope", []byte("x")); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := a.Send("nope", make([]byte, MaxPayload+1)); !errors.Is(err, ErrPayloadTooBig) {
		t.Fatal(err)
	}
	if _, _, err := a.Accept(ctx, []byte("k"), []byte("garbage"), nil); err == nil {
		t.Fatal("негодный offer принят")
	}
	if s, n := a.Counts(); s != 0 || n != 0 {
		t.Fatalf("после отказов остались ресурсы: %d/%d", s, n)
	}
}

func TestSessionLimit(t *testing.T) {
	a, _ := newExec(t, nil)
	_, pb := newExec(t, nil)
	for i := 0; i < MaxSessions; i++ {
		if _, _, err := a.Offer(context.Background(), pb.Public, nil); err != nil {
			t.Fatalf("offer %d: %v", i, err)
		}
	}
	if _, _, err := a.Offer(context.Background(), pb.Public, nil); !errors.Is(err, ErrTooMany) {
		t.Fatalf("сверх предела: %v", err)
	}
}

// Одновременные сессии: несколько пар между одними и теми же
// исполнителями, сообщения не перепутываются.
func TestConcurrentSessionsAreIndependent(t *testing.T) {
	a, pa := newExec(t, nil)
	b, pb := newExec(t, nil)
	const n = 3
	var sas, sbs [n]string
	for i := 0; i < n; i++ {
		sas[i], sbs[i] = pair(t, a, b, pa, pb, nil, nil)
	}
	for i := 0; i < n; i++ {
		waitEvent(t, a, EventEstablished, sas[i], 20*time.Second)
		waitEvent(t, b, EventEstablished, sbs[i], 20*time.Second)
	}
	for i := 0; i < n; i++ {
		if err := a.Send(sas[i], []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	got := map[string]string{}
	for i := 0; i < n; i++ {
		got[sbs[i]] = string(waitEvent(t, b, EventMessage, sbs[i], 10*time.Second).Payload)
	}
	for i := 0; i < n; i++ {
		if got[sbs[i]] != fmt.Sprintf("m%d", i) {
			t.Fatalf("сессия %d получила %q", i, got[sbs[i]])
		}
	}
}

// ── TURN relay с краткоживущими учётными данными ───────────────────────

// startRESTTURN — локальный TURN-стенд с проверкой TURN REST API (та же
// схема, что coturn use-auth-secret): username "<expiry>:<user>",
// credential base64(HMAC-SHA1(secret, username)). Не coturn и не
// продакшн-инфраструктура — стенд теста.
func startRESTTURN(t *testing.T, secret string) (string, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := turn.NewServer(turn.ServerConfig{
		Realm:       "photon.test",
		AuthHandler: turn.LongTermTURNRESTAuthHandler(secret, nil),
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn: pc,
			RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{
				RelayAddress: net.ParseIP("127.0.0.1"),
				Address:      "127.0.0.1",
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := func() { _ = srv.Close() }
	t.Cleanup(stop)
	return "turn:" + pc.LocalAddr().String() + "?transport=udp", stop
}

func restCreds(t *testing.T, url, secret, user string, ttl time.Duration) *TURN {
	t.Helper()
	u, p, err := turn.GenerateLongTermTURNRESTCredentials(secret, user, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return &TURN{URLs: []string{url}, Username: u, Credential: p}
}

// forceRelay — только тест: на loopback прямой путь есть всегда, и без
// исключения host-кандидатов relay не выбрался бы. Учётные данные при
// этом идут ровно тем путём, что в продакшне — из payload действия.
func forceRelay(c *Config) {
	c.newAdapter = func(pc pionadapter.Config) (ports.P2P, error) {
		pc.ForceRelay = true
		return pionadapter.New(pc)
	}
}

func TestRelayPathWithShortLivedCredentials(t *testing.T) {
	const secret = "test-only-secret"
	url, _ := startRESTTURN(t, secret)
	a, pa := newExec(t, forceRelay)
	b, pb := newExec(t, forceRelay)
	sa, sb := pair(t, a, b, pa, pb,
		restCreds(t, url, secret, "000000001", 10*time.Minute),
		restCreds(t, url, secret, "000000002", 10*time.Minute))
	ev := waitEvent(t, a, EventEstablished, sa, 25*time.Second)
	if ev.Path != "relay" {
		t.Fatalf("путь %q, ожидался relay", ev.Path)
	}
	waitEvent(t, b, EventEstablished, sb, 25*time.Second)
	if err := a.Send(sa, []byte("через relay")); err != nil {
		t.Fatal(err)
	}
	if got := waitEvent(t, b, EventMessage, sb, 10*time.Second); string(got.Payload) != "через relay" {
		t.Fatalf("получено %q", got.Payload)
	}
}

// Истёкшие учётные данные TURN не дают relay: краткоживущие действительно
// краткоживущие.
func TestExpiredTURNCredentialsGiveNoRelay(t *testing.T) {
	const secret = "test-only-secret"
	url, _ := startRESTTURN(t, secret)
	a, pa := newExec(t, func(c *Config) { forceRelay(c); c.EstablishTimeout = 5 * time.Second })
	_, _ = pa, a
	expired := restCreds(t, url, secret, "000000001", -time.Minute)
	_, pb := newExec(t, nil)
	if _, _, err := a.Offer(context.Background(), pb.Public, expired); err == nil {
		t.Fatal("offer с истёкшими учётными данными TURN собран: relay-кандидат получен по просроченному username")
	}
}

// Сеть пропала (TURN-сервер, через который идёт единственный путь, исчез):
// обе стороны получают failed, а не висят.
func TestNetworkFailureClosesWithFailed(t *testing.T) {
	const secret = "test-only-secret"
	url, stop := startRESTTURN(t, secret)
	a, pa := newExec(t, forceRelay)
	b, pb := newExec(t, forceRelay)
	sa, sb := pair(t, a, b, pa, pb,
		restCreds(t, url, secret, "000000001", 10*time.Minute),
		restCreds(t, url, secret, "000000002", 10*time.Minute))
	waitEvent(t, a, EventEstablished, sa, 25*time.Second)
	waitEvent(t, b, EventEstablished, sb, 25*time.Second)
	stop()
	if ev := waitEvent(t, a, EventClosed, sa, 20*time.Second); ev.Reason != ReasonFailed {
		t.Fatalf("A: %q, ожидался failed", ev.Reason)
	}
	if ev := waitEvent(t, b, EventClosed, sb, 20*time.Second); ev.Reason != ReasonFailed {
		t.Fatalf("B: %q, ожидался failed", ev.Reason)
	}
}

// ── классификация исходов на подставном адаптере ───────────────────────

type fakeStream struct {
	r io.Reader
	w bytes.Buffer
}

func (s *fakeStream) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s *fakeStream) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *fakeStream) Close() error                { return nil }

type fakeSession struct {
	peer ports.PublicKey
	st   *fakeStream
}

func (s *fakeSession) OpenStream(context.Context) (ports.Stream, error)        { return s.st, nil }
func (s *fakeSession) OpenUniStream(context.Context) (ports.SendStream, error) { return nil, io.EOF }
func (s *fakeSession) AcceptStream(context.Context) (ports.Stream, error)      { return s.st, nil }
func (s *fakeSession) AcceptUniStream(context.Context) (ports.RecvStream, error) {
	return nil, io.EOF
}
func (s *fakeSession) RemoteEndpoint() ports.Endpoint { return "fake" }
func (s *fakeSession) Close() error                   { return nil }
func (s *fakeSession) Peer() ports.PublicKey          { return s.peer }

type fakeP2P struct{ sess *fakeSession }

func (f fakeP2P) CreateOffer(context.Context, ports.PublicKey) ([]byte, error) {
	return []byte("o"), nil
}
func (f fakeP2P) AcceptOffer(context.Context, ports.PublicKey, []byte) ([]byte, ports.Session, error) {
	return []byte("a"), f.sess, nil
}
func (f fakeP2P) CompleteSession(context.Context, ports.PublicKey, []byte) (ports.Session, error) {
	return f.sess, nil
}
func (f fakeP2P) Close() error { return nil }

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// Потеря собеседника (adapters/pion отдаёт ErrSessionFailed) — failed;
// штатный разрыв (io.EOF) — remote. Смешение было бы ложью плагину.
func TestCloseReasonClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"штатный разрыв", io.EOF, ReasonRemote},
		{"потеря собеседника", fmt.Errorf("pion: peer lost: %w", ports.ErrSessionFailed), ReasonFailed},
		{"обрыв посреди кадра", io.ErrUnexpectedEOF, ReasonFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := &fakeSession{peer: ports.PublicKey("peer"), st: &fakeStream{r: errReader{tc.err}}}
			e, _ := newExec(t, func(c *Config) {
				c.newAdapter = func(pionadapter.Config) (ports.P2P, error) { return fakeP2P{sess: fs}, nil }
			})
			sid, _, err := e.Accept(context.Background(), []byte("peer"), []byte("offer"), nil)
			if err != nil {
				t.Fatal(err)
			}
			if ev := waitEvent(t, e, EventClosed, sid, 5*time.Second); ev.Reason != tc.want {
				t.Fatalf("причина %q, ожидалась %q", ev.Reason, tc.want)
			}
		})
	}
}

// Переполненная очередь не теряет сообщения молча: сессия закрывается с
// slow_consumer.
func TestSlowConsumerClosesSession(t *testing.T) {
	var frames bytes.Buffer
	for i := 0; i < MaxQueuedEvents+10; i++ {
		frames.Write([]byte{0, 0, 0, 1, 'x'})
	}
	fs := &fakeSession{peer: ports.PublicKey("peer"), st: &fakeStream{r: &frames}}
	e, _ := newExec(t, func(c *Config) {
		c.newAdapter = func(pionadapter.Config) (ports.P2P, error) { return fakeP2P{sess: fs}, nil }
	})
	sid, _, err := e.Accept(context.Background(), []byte("peer"), []byte("offer"), nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := e.Counts(); s == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	var reason string
	msgs := 0
	for _, ev := range drain(e) {
		switch ev.Type {
		case EventMessage:
			msgs++
		case EventClosed:
			if ev.SessionID == sid {
				reason = ev.Reason
			}
		}
	}
	if reason != ReasonSlowConsumer {
		t.Fatalf("причина %q, ожидалась slow_consumer", reason)
	}
	if msgs >= MaxQueuedEvents+10 {
		t.Fatal("переполнения не было — тест ничего не проверил")
	}
}

func drain(e *Executor) []Event {
	all := seen[e]
	seen[e] = nil
	for {
		evs := e.Poll(MaxPoll)
		if len(evs) == 0 {
			return all
		}
		all = append(all, evs...)
	}
}

// Ресурсы: циклы установления/закрытия не оставляют горутин.
func TestCyclesDoNotLeak(t *testing.T) {
	a, pa := newExec(t, nil)
	b, pb := newExec(t, nil)
	cycle := func() {
		sa, sb := pair(t, a, b, pa, pb, nil, nil)
		waitEvent(t, a, EventEstablished, sa, 20*time.Second)
		waitEvent(t, b, EventEstablished, sb, 20*time.Second)
		_ = a.Close(sa)
		waitEvent(t, b, EventClosed, sb, 20*time.Second)
	}
	cycle()
	time.Sleep(500 * time.Millisecond)
	base := runtime.NumGoroutine()
	for i := 0; i < 4; i++ {
		cycle()
	}
	deadline := time.Now().Add(15 * time.Second)
	for runtime.NumGoroutine() > base+2 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if g := runtime.NumGoroutine(); g > base+2 {
		t.Fatalf("горутин %d при базе %d — ресурсы сессий удерживаются", g, base)
	}
	if s, n := a.Counts(); s != 0 || n != 0 {
		t.Fatalf("реестр A не пуст: %d/%d", s, n)
	}
	if s, n := b.Counts(); s != 0 || n != 0 {
		t.Fatalf("реестр B не пуст: %d/%d", s, n)
	}
}

func TestShutdownClosesEverything(t *testing.T) {
	a, pa := newExec(t, nil)
	b, pb := newExec(t, nil)
	sa, sb := pair(t, a, b, pa, pb, nil, nil)
	waitEvent(t, a, EventEstablished, sa, 20*time.Second)
	_, _, _ = a.Offer(context.Background(), pb.Public, nil)
	a.Shutdown()
	if s, n := a.Counts(); s != 0 || n != 0 {
		t.Fatalf("после Shutdown: %d/%d", s, n)
	}
	waitEvent(t, b, EventClosed, sb, 20*time.Second)
	if _, _, err := a.Offer(context.Background(), pb.Public, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Offer после Shutdown: %v", err)
	}
}

// Ловушка ADR-044 §6.1 на границе Core: контекст локального действия
// отменяется сразу после ответа хосту. Согласование обязано пережить его —
// иначе answer, пришедший через Bridge, находил бы его уже снятым. Найдено
// первым же прогоном через SendAction (complete давал negotiation_failed).
func TestNegotiationOutlivesCallContext(t *testing.T) {
	a, pa := newExec(t, nil)
	b, pb := newExec(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	neg, offer, err := a.Offer(ctx, pb.Public, nil)
	cancel() // вызов вернулся — его контекст отменён
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	actx, acancel := context.WithCancel(context.Background())
	sb, answer, err := b.Accept(actx, pa.Public, offer, nil)
	acancel()
	if err != nil {
		t.Fatal(err)
	}
	cctx, ccancel := context.WithCancel(context.Background())
	sa, err := a.Complete(cctx, neg, answer)
	ccancel()
	if err != nil {
		t.Fatalf("complete после отмены контекста offer: %v", err)
	}
	waitEvent(t, a, EventEstablished, sa, 20*time.Second)
	waitEvent(t, b, EventEstablished, sb, 20*time.Second)
}
