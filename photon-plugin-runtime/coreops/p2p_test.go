package coreops

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/wiloris/photon/photon-core/adapters/aesgcm"
	bboltadapter "github.com/wiloris/photon/photon-core/adapters/bbolt"
	ed25519adapter "github.com/wiloris/photon/photon-core/adapters/ed25519"
	"github.com/wiloris/photon/photon-core/p2pmsg"
	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-plugin-runtime/ipc"
)

// ── фейковый ports.P2P ──────────────────────────────────────────────
//
// Сети здесь нет намеренно: условие 5 проверяет ИСПОЛНИТЕЛЯ — владение
// handle, сериализацию, реестр, события, — а не транспорт. Транспорт уже
// доказан ADR-075 на двух настоящих процессах, и повторять это здесь значило
// бы проверять чужое свойство. Двухпроцессный E2E — условие 6.

// pipeStream — ports.Stream поверх двух труб.
type pipeStream struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func (p *pipeStream) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p *pipeStream) Write(b []byte) (int, error) { return p.w.Write(b) }

// Close рвёт ОБА конца.
//
// Настоящий ports.Session.Close закрывает ассоциацию, и заблокированное
// чтение перестаёт блокироваться. Первая редакция закрывала только пишущую
// половину, и насос висел в Recv до потолка теста — дефект фейка, а не кода:
// он имитировал half-close там, где происходит разрыв.
func (p *pipeStream) Close() error {
	_ = p.r.Close()
	return p.w.Close()
}

// chunkedStream дробит каждую запись надвое с точкой переключения между
// половинами.
//
// Нужен потому, что io.Pipe.Write атомарен сам по себе и потому СКРЫВАЕТ
// отсутствие сериализации: мутация «снять sendMu» на нём выживала. Контракт
// ports.Stream атомарности не обещает — в adapters/tcp кадр уходит
// несколькими записями, — и проверять надо контракт, а не удобство фейка.
type chunkedStream struct{ inner ports.Stream }

func (c *chunkedStream) Read(b []byte) (int, error) { return c.inner.Read(b) }
func (c *chunkedStream) Close() error               { return c.inner.Close() }
func (c *chunkedStream) Write(b []byte) (int, error) {
	if len(b) < 2 {
		return c.inner.Write(b)
	}
	half := len(b) / 2
	n1, err := c.inner.Write(b[:half])
	if err != nil {
		return n1, err
	}
	runtime.Gosched()
	n2, err := c.inner.Write(b[half:])
	return n1 + n2, err
}

func streamPair() (*pipeStream, *pipeStream) {
	ar, bw := io.Pipe()
	br, aw := io.Pipe()
	return &pipeStream{r: ar, w: aw}, &pipeStream{r: br, w: bw}
}

type fakeSession struct {
	peer ports.PublicKey

	mu        sync.Mutex
	stream    ports.Stream
	streamErr error
	released  chan struct{} // закрывается, когда поток можно отдать
	closed    bool
}

func newFakeSession(peer ports.PublicKey, st ports.Stream) *fakeSession {
	s := &fakeSession{peer: peer, stream: st, released: make(chan struct{})}
	close(s.released) // по умолчанию установление мгновенно
	return s
}

// newPendingSession — сессия, которая ещё не установилась.
func newPendingSession(peer ports.PublicKey) *fakeSession {
	return &fakeSession{peer: peer, released: make(chan struct{})}
}

func (s *fakeSession) release(st ports.Stream, err error) {
	s.mu.Lock()
	s.stream, s.streamErr = st, err
	s.mu.Unlock()
	close(s.released)
}

func (s *fakeSession) await(ctx context.Context) (ports.Stream, error) {
	select {
	case <-s.released:
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.streamErr != nil {
			return nil, s.streamErr
		}
		if s.closed {
			return nil, ports.ErrSessionFailed
		}
		return s.stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *fakeSession) OpenStream(ctx context.Context) (ports.Stream, error) { return s.await(ctx) }
func (s *fakeSession) AcceptStream(ctx context.Context) (ports.Stream, error) {
	return s.await(ctx)
}
func (s *fakeSession) OpenUniStream(ctx context.Context) (ports.SendStream, error) {
	return nil, errors.New("not used")
}
func (s *fakeSession) AcceptUniStream(ctx context.Context) (ports.RecvStream, error) {
	return nil, errors.New("not used")
}
func (s *fakeSession) RemoteEndpoint() ports.Endpoint { return "fake" }
func (s *fakeSession) Peer() ports.PublicKey {
	<-s.released
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	return s.peer
}
func (s *fakeSession) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	st := s.stream
	s.mu.Unlock()
	if st != nil {
		_ = st.Close()
	}
	return nil
}

type fakeP2P struct {
	mu sync.Mutex

	offer       []byte
	answer      []byte
	nextAccept  *fakeSession
	nextDone    *fakeSession
	offerErr    error
	acceptErr   error
	completeErr error

	created   int
	accepted  int
	completed int
}

var _ ports.P2P = (*fakeP2P)(nil)

func (f *fakeP2P) CreateOffer(ctx context.Context, peer ports.PublicKey) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created++
	if f.offerErr != nil {
		return nil, f.offerErr
	}
	return f.offer, nil
}

func (f *fakeP2P) AcceptOffer(ctx context.Context, peer ports.PublicKey, offer []byte) ([]byte, ports.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accepted++
	if f.acceptErr != nil {
		return nil, nil, f.acceptErr
	}
	return f.answer, f.nextAccept, nil
}

func (f *fakeP2P) CompleteSession(ctx context.Context, peer ports.PublicKey, answer []byte) (ports.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completed++
	if f.completeErr != nil {
		return nil, f.completeErr
	}
	return f.nextDone, nil
}

func (f *fakeP2P) Close() error { return nil }

// ── сбор событий ────────────────────────────────────────────────────

type event struct {
	pluginID string
	typ      string
	payload  []byte
}

type eventSink struct {
	mu   sync.Mutex
	got  []event
	full bool // очередь «переполнена» насовсем
	// failNext — сколько ближайших событий отвергнуть.
	//
	// Настоящая очередь IPC дренируется непрерывно (eventLoop пишет в
	// сокет), поэтому «полна» — состояние ВРЕМЕННОЕ. Модель «полна
	// навсегда» проверяла бы не тот сценарий.
	failNext int
	ch       chan event
}

func newSink() *eventSink { return &eventSink{ch: make(chan event, 256)} }

func (s *eventSink) emit(pluginID, typ string, payload []byte) error {
	s.mu.Lock()
	if s.full || s.failNext > 0 {
		if s.failNext > 0 {
			s.failNext--
		}
		s.mu.Unlock()
		// Ровно та ошибка, которую отдаёт ipc.Session.SendEvent при
		// переполнении очереди.
		return errors.New("ipc: event queue full, dropping " + typ)
	}
	e := event{pluginID, typ, payload}
	s.got = append(s.got, e)
	s.mu.Unlock()
	select {
	case s.ch <- e:
	default:
	}
	return nil
}

func (s *eventSink) waitFor(t *testing.T, typ string) event {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case e := <-s.ch:
			if e.typ == typ {
				return e
			}
		case <-deadline:
			s.mu.Lock()
			got := s.got
			s.mu.Unlock()
			t.Fatalf("событие %q не пришло за 5 с; получено: %v", typ, summarize(got))
		}
	}
}

func summarize(evs []event) []string {
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.typ)
	}
	return out
}

// ── обвязка ─────────────────────────────────────────────────────────
//
// Используются call/mustCall/opCode из coreops_test.go: своя копия разошлась
// бы с ними молча.

// p2pConfig — та же композиция, что у newHandler, плюс P2P.
func p2pConfig(t *testing.T, p2p ports.P2P, sink *eventSink) Config {
	t.Helper()
	st, err := bboltadapter.New(filepath.Join(t.TempDir(), "p2p.db"), []bboltadapter.Migration{{
		Version: 1,
		Up: func(tx *bolt.Tx) error {
			_, createErr := tx.CreateBucketIfNotExists([]byte(testNS))
			return createErr
		},
	}})
	if err != nil {
		t.Fatalf("bbolt.New: %v", err)
	}
	t.Cleanup(func() { st.Close(context.Background()) })

	id := ed25519adapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	cr := aesgcm.New()
	master, err := cr.GenerateKey(context.Background())
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if err := st.Migrate(context.Background(), 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	cfg := Config{
		Storage: st, StorageNamespace: testNS, Identity: id,
		DeviceKey: pair.Private, Crypto: cr,
		DeriveKey: func(ctx context.Context, domain string) (ports.SymmetricKey, error) {
			return cr.DeriveKey(ctx, master, domain)
		},
		Log: func(string, string, string) {},
		P2P: p2p,
	}
	if sink != nil {
		cfg.EmitEvent = sink.emit
	}
	return cfg
}

func newP2PHandler(t *testing.T, p2p ports.P2P, sink *eventSink) *Handler {
	t.Helper()
	h, err := New(p2pConfig(t, p2p, sink))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return h
}

func callInto(t *testing.T, h *Handler, pluginID, op string, req any, out any) {
	t.Helper()
	raw := mustCall(t, h, pluginID, op, req)
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s: разбор ответа: %v", op, err)
		}
	}
}

func requireCode(t *testing.T, err error, want ipc.ErrorCode, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: ошибки нет, ожидался %s", what, want)
	}
	if got := opCode(t, err); got != want {
		t.Fatalf("%s: код %s, ожидался %s", what, got, want)
	}
}

// ── P2P не сконфигурирован ──────────────────────────────────────────

// ADR-076 §7.6: композиция без P2P нормальна, все шесть операций отвечают
// UNSUPPORTED — как неизвестная операция, а не падением и не молчанием.
func TestP2PNilGivesUnsupportedForAllSixOperations(t *testing.T) {
	h, err := New(p2pConfig(t, nil, nil))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ops := []string{
		"p2p.create_offer", "p2p.accept_offer", "p2p.complete",
		"p2p.peer", "p2p.send", "p2p.close",
	}
	for _, op := range ops {
		_, err := call(t, h, "plug-a", op, map[string]any{"session_id": "x"})
		requireCode(t, err, ipc.ErrUnsupported, op)
	}
}

// EmitEvent обязателен вместе с P2P: без него принятые кадры некуда девать.
func TestP2PWithoutEmitEventIsRejectedAtConstruction(t *testing.T) {
	cfg := p2pConfig(t, &fakeP2P{}, nil)
	if _, err := New(cfg); err == nil {
		t.Fatal("New принял P2P без EmitEvent — канал стал бы односторонним молча")
	}
}

// Инвариант ADR-076 §7.7 проверяется при конструировании, а не оставлен на
// внимательность композиции.
func TestEstablishTimeoutMustBeStrictlyLessThanRequestTimeout(t *testing.T) {
	for _, d := range []time.Duration{ipc.DefaultRequestTimeout, ipc.DefaultRequestTimeout + time.Second} {
		cfg := p2pConfig(t, &fakeP2P{}, newSink())
		cfg.P2PEstablishTimeout = d
		_, err := New(cfg)
		if err == nil {
			t.Fatalf("New принял P2PEstablishTimeout=%s при RequestTimeout=%s", d, ipc.DefaultRequestTimeout)
		}
		if !strings.Contains(err.Error(), "strictly less") {
			t.Fatalf("ошибка не называет инвариант: %v", err)
		}
	}
}

func TestEstablishTimeoutDefaultSatisfiesInvariant(t *testing.T) {
	if defaultP2PEstablishTimeout >= ipc.DefaultRequestTimeout {
		t.Fatalf("умолчание %s не меньше %s — инвариант ADR-076 §7.7 нарушен",
			defaultP2PEstablishTimeout, ipc.DefaultRequestTimeout)
	}
}

// ── согласование ────────────────────────────────────────────────────

// Полный путь обеих ролей: create_offer → accept_offer → complete.
func TestNegotiationThreeSteps(t *testing.T) {
	aStream, bStream := streamPair()
	f := &fakeP2P{
		offer:      []byte("OFFER"),
		answer:     []byte("ANSWER"),
		nextAccept: newFakeSession(ports.PublicKey("peer-a"), bStream),
		nextDone:   newFakeSession(ports.PublicKey("peer-b"), aStream),
	}
	sink := newSink()
	h := newP2PHandler(t, f, sink)

	var off p2pCreateOfferResponse
	callInto(t, h, "plug-a", "p2p.create_offer",
		p2pCreateOfferRequest{PeerPubKey: []byte("peer-b")}, &off)
	if off.NegotiationID == "" || string(off.Offer) != "OFFER" {
		t.Fatalf("create_offer вернул %+v", off)
	}

	var acc p2pAcceptOfferResponse
	callInto(t, h, "plug-b", "p2p.accept_offer",
		p2pAcceptOfferRequest{PeerPubKey: []byte("peer-a"), Offer: off.Offer}, &acc)
	if acc.SessionID == "" || string(acc.Answer) != "ANSWER" {
		t.Fatalf("accept_offer вернул %+v", acc)
	}

	var done p2pCompleteResponse
	callInto(t, h, "plug-a", "p2p.complete",
		p2pCompleteRequest{NegotiationID: off.NegotiationID, Answer: acc.Answer}, &done)
	if done.SessionID == "" {
		t.Fatal("complete не вернул session_id")
	}
	if done.SessionID == acc.SessionID {
		t.Fatal("две стороны получили один handle — handle обязан быть локальным")
	}
}

// Согласование потребляется РОВНО ОДИН РАЗ (ADR-044 §6).
func TestRepeatedCompleteIsNotFound(t *testing.T) {
	aStream, _ := streamPair()
	f := &fakeP2P{offer: []byte("O"), nextDone: newFakeSession(ports.PublicKey("p"), aStream)}
	h := newP2PHandler(t, f, newSink())

	var off p2pCreateOfferResponse
	callInto(t, h, "plug-a", "p2p.create_offer", p2pCreateOfferRequest{PeerPubKey: []byte("p")}, &off)
	callInto(t, h, "plug-a", "p2p.complete",
		p2pCompleteRequest{NegotiationID: off.NegotiationID, Answer: []byte("A")}, &p2pCompleteResponse{})

	_, err := call(t, h, "plug-a", "p2p.complete",
		p2pCompleteRequest{NegotiationID: off.NegotiationID, Answer: []byte("A")})
	requireCode(t, err, ipc.ErrNotFound, "повторный complete")
}

// Чужое согласование неотличимо от несуществующего.
func TestNegotiationOfAnotherPluginIsNotFound(t *testing.T) {
	f := &fakeP2P{offer: []byte("O")}
	h := newP2PHandler(t, f, newSink())

	var off p2pCreateOfferResponse
	callInto(t, h, "plug-a", "p2p.create_offer", p2pCreateOfferRequest{PeerPubKey: []byte("p")}, &off)

	_, err := call(t, h, "plug-b", "p2p.complete",
		p2pCompleteRequest{NegotiationID: off.NegotiationID, Answer: []byte("A")})
	requireCode(t, err, ipc.ErrNotFound, "чужое согласование")
}

// ── владение handle ─────────────────────────────────────────────────

// remote — сторона собеседника поверх второго конца трубы.
func remote(st ports.Stream) *p2pmsg.Conversation { return p2pmsg.New(st) }

// established поднимает готовую сессию и отдаёт handle, сторону собеседника
// и обработчик.
func established(t *testing.T, sink *eventSink) (*Handler, string, *p2pmsg.Conversation, *fakeSession) {
	t.Helper()
	local, far := streamPair()
	sess := newFakeSession(ports.PublicKey("peer"), local)
	f := &fakeP2P{answer: []byte("A"), nextAccept: sess}
	h := newP2PHandler(t, f, sink)

	var acc p2pAcceptOfferResponse
	callInto(t, h, "plug-a", "p2p.accept_offer",
		p2pAcceptOfferRequest{PeerPubKey: []byte("peer"), Offer: []byte("O")}, &acc)
	return h, acc.SessionID, remote(far), sess
}

// Чужой и неизвестный handle неотличимы: иначе перебор дал бы оракул о
// сессиях соседей (ADR-076 §7.2).
func TestForeignAndUnknownHandleAreBothNotFound(t *testing.T) {
	h, id, _, _ := established(t, newSink())

	for _, tc := range []struct{ name, plugin, handle string }{
		{"чужой handle", "plug-b", id},
		{"неизвестный handle", "plug-a", "deadbeefdeadbeefdeadbeefdeadbeef"},
		{"чужой и неизвестный", "plug-b", "00000000000000000000000000000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range []string{"p2p.peer", "p2p.close"} {
				_, err := call(t, h, tc.plugin, op, p2pSessionRequest{SessionID: tc.handle})
				requireCode(t, err, ipc.ErrNotFound, op)
			}
			_, err := call(t, h, tc.plugin, "p2p.send",
				p2pSendRequest{SessionID: tc.handle, Payload: []byte("x")})
			requireCode(t, err, ipc.ErrNotFound, "p2p.send")
		})
	}
}

// Владелец свой handle видит — иначе предыдущий тест ничего не доказывал бы.
func TestOwnerSeesOwnSession(t *testing.T) {
	h, id, _, _ := established(t, newSink())
	var pr p2pPeerResponse
	callInto(t, h, "plug-a", "p2p.peer", p2pSessionRequest{SessionID: id}, &pr)
	if string(pr.PeerPubKey) != "peer" {
		t.Fatalf("peer %q", pr.PeerPubKey)
	}
}

// ── peer до и после установления ────────────────────────────────────

// p2p.peer — post-establishment query (ADR-076 §7.1): до установления он
// БЛОКИРУЕТ, а не отвечает «пока нет».
func TestPeerBlocksUntilEstablished(t *testing.T) {
	local, _ := streamPair()
	sess := newPendingSession(ports.PublicKey("peer"))
	f := &fakeP2P{answer: []byte("A"), nextAccept: sess}
	h := newP2PHandler(t, f, newSink())

	var acc p2pAcceptOfferResponse
	callInto(t, h, "plug-a", "p2p.accept_offer",
		p2pAcceptOfferRequest{PeerPubKey: []byte("peer"), Offer: []byte("O")}, &acc)

	done := make(chan []byte, 1)
	go func() {
		raw, err := call(t, h, "plug-a", "p2p.peer", p2pSessionRequest{SessionID: acc.SessionID})
		if err != nil {
			done <- nil
			return
		}
		done <- raw
	}()

	select {
	case <-done:
		t.Fatal("p2p.peer вернулся до установления — он обязан блокировать")
	case <-time.After(150 * time.Millisecond):
	}

	sess.release(local, nil) // установление состоялось
	select {
	case raw := <-done:
		if raw == nil {
			t.Fatal("p2p.peer отказал после установления")
		}
		var pr p2pPeerResponse
		if err := json.Unmarshal(raw, &pr); err != nil {
			t.Fatal(err)
		}
		if string(pr.PeerPubKey) != "peer" {
			t.Fatalf("peer %q", pr.PeerPubKey)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("p2p.peer не вернулся после установления")
	}
}

// Срок ожидания выводится из дедлайна ЗАПРОСА — инвариант ADR-076 §7.7
// держится при любом RequestTimeout, а не только при умолчании.
func TestPeerHonoursRequestDeadlineNotOnlyItsOwnCap(t *testing.T) {
	sess := newPendingSession(ports.PublicKey("peer"))
	f := &fakeP2P{answer: []byte("A"), nextAccept: sess}
	h := newP2PHandler(t, f, newSink())

	var acc p2pAcceptOfferResponse
	callInto(t, h, "plug-a", "p2p.accept_offer",
		p2pAcceptOfferRequest{PeerPubKey: []byte("peer"), Offer: []byte("O")}, &acc)

	// Дедлайн запроса НАМНОГО короче P2PEstablishTimeout (20 с).
	const requestDeadline = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), requestDeadline)
	defer cancel()
	payload, _ := json.Marshal(p2pSessionRequest{SessionID: acc.SessionID})

	start := time.Now()
	_, err := h.Handle(ctx, ipc.CallerInfo{PluginID: "plug-a"}, "p2p.peer", payload)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("p2p.peer вернул успех для неустановившейся сессии")
	}
	// Причина обязана быть со стороны P2P, а не дедлайном IPC.
	requireCode(t, err, ipc.ErrInternal, "p2p.peer до установления")

	// Суть инварианта ADR-076 §7.7: ответ обязан прийти СТРОГО РАНЬШЕ
	// дедлайна запроса, а не в гонке с ним. Просто «уложились в дедлайн»
	// проверкой не является — этого добивается и родительский контекст сам
	// по себе; проверять надо ЗАПАС.
	latest := requestDeadline - establishMargin/2
	if elapsed > latest {
		t.Fatalf("ожидание %s при дедлайне запроса %s: запас %s не соблюдён — "+
			"ответ гоняется с дедлайном IPC вместо того, чтобы его опережать",
			elapsed, requestDeadline, establishMargin)
	}
}

// ── отправка ────────────────────────────────────────────────────────

func TestSendDeliversFrameToPeer(t *testing.T) {
	h, id, far, _ := established(t, newSink())
	want := []byte("hello from plugin")

	go func() {
		_, _ = call(t, h, "plug-a", "p2p.send", p2pSendRequest{SessionID: id, Payload: want})
	}()

	got, err := far.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("получено %q, отправлено %q", got, want)
	}
}

// Нагрузки: пустая, обычная, крупная и ровно предел p2pmsg.
func TestSendPayloadSizes(t *testing.T) {
	sizes := []int{0, 1, 1024, 64 << 10, 256 << 10, p2pmsg.MaxMessageSize}
	h, id, far, _ := established(t, newSink())

	for _, n := range sizes {
		want := make([]byte, n)
		for i := range want {
			want[i] = byte(i*7 + 1)
		}
		errCh := make(chan error, 1)
		go func() {
			_, err := call(t, h, "plug-a", "p2p.send", p2pSendRequest{SessionID: id, Payload: want})
			errCh <- err
		}()
		got, err := far.Recv()
		if err != nil {
			t.Fatalf("размер %d: Recv: %v", n, err)
		}
		if sendErr := <-errCh; sendErr != nil {
			t.Fatalf("размер %d: send: %v", n, sendErr)
		}
		if len(got) != n {
			t.Fatalf("размер %d: получено %d байт", n, len(got))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("размер %d: байт %d искажён", n, i)
			}
		}
	}
}

// Сверх предела p2pmsg — отказ, а не молчаливое усечение. Предел ADR-075 не
// менялся.
func TestSendOverMaxIsRejected(t *testing.T) {
	h, id, _, _ := established(t, newSink())
	_, err := call(t, h, "plug-a", "p2p.send",
		p2pSendRequest{SessionID: id, Payload: make([]byte, p2pmsg.MaxMessageSize+1)})
	requireCode(t, err, ipc.ErrInvalidRequest, "send сверх предела")
}

// Параллельные send не повреждают кадрирование (ADR-076 §3.10).
//
// Без сериализации два одновременных Send перемешали бы байты двух кадров, и
// получатель увидел бы негодную длину — молчаливая порча.
func TestConcurrentSendsDoNotCorruptFraming(t *testing.T) {
	const senders = 8
	const each = 25

	local, farRaw := streamPair()
	sess := newFakeSession(ports.PublicKey("peer"), &chunkedStream{inner: local})
	h := newP2PHandler(t, &fakeP2P{answer: []byte("A"), nextAccept: sess}, newSink())
	var acc p2pAcceptOfferResponse
	callInto(t, h, "plug-a", "p2p.accept_offer",
		p2pAcceptOfferRequest{PeerPubKey: []byte("peer"), Offer: []byte("O")}, &acc)
	id, far := acc.SessionID, remote(farRaw)

	payloads := make([][]byte, senders)
	for i := range payloads {
		p := make([]byte, 4096)
		for j := range p {
			p[j] = byte(i + 1)
		}
		payloads[i] = p
	}

	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for k := 0; k < each; k++ {
				if _, err := call(t, h, "plug-a", "p2p.send",
					p2pSendRequest{SessionID: id, Payload: payloads[i]}); err != nil {
					t.Errorf("send: %v", err)
					return
				}
			}
		}(i)
	}

	for n := 0; n < senders*each; n++ {
		got, err := far.Recv()
		if err != nil {
			t.Fatalf("кадр %d: %v — кадрирование повреждено", n, err)
		}
		if len(got) != 4096 {
			t.Fatalf("кадр %d: длина %d вместо 4096", n, len(got))
		}
		// Каждый кадр обязан быть ОДНОРОДНЫМ: смешение двух отправителей
		// внутри одного кадра — прямой признак потерянной сериализации.
		first := got[0]
		if first == 0 || int(first) > senders {
			t.Fatalf("кадр %d: негодный маркер %d", n, first)
		}
		for _, b := range got {
			if b != first {
				t.Fatalf("кадр %d смешал отправителей %d и %d", n, first, b)
			}
		}
	}
	wg.Wait()
}

// ── приём ───────────────────────────────────────────────────────────

func TestIncomingFrameBecomesP2PMessageEvent(t *testing.T) {
	sink := newSink()
	h, id, far, _ := established(t, sink)
	_ = h

	want := []byte{0x00, 0xff, 'x', 0x00}
	if err := far.Send(want); err != nil {
		t.Fatalf("Send: %v", err)
	}

	ev := sink.waitFor(t, eventMessage)
	var me p2pMessageEvent
	if err := json.Unmarshal(ev.payload, &me); err != nil {
		t.Fatal(err)
	}
	if me.SessionID != id {
		t.Fatalf("session_id %q, ожидался %q", me.SessionID, id)
	}
	if string(me.Payload) != string(want) {
		t.Fatalf("payload % x, отправлено % x", me.Payload, want)
	}
	if ev.pluginID != "plug-a" {
		t.Fatalf("событие адресовано %q", ev.pluginID)
	}
}

// ── закрытие ────────────────────────────────────────────────────────

func TestCloseEmitsClosedEventWithLocalReason(t *testing.T) {
	sink := newSink()
	h, id, _, _ := established(t, sink)

	mustCall(t, h, "plug-a", "p2p.close", p2pSessionRequest{SessionID: id})

	ev := sink.waitFor(t, eventClosed)
	var ce p2pClosedEvent
	if err := json.Unmarshal(ev.payload, &ce); err != nil {
		t.Fatal(err)
	}
	if ce.SessionID != id || ce.Reason != reasonLocal {
		t.Fatalf("p2p.closed = %+v, ожидалось {%s %s}", ce, id, reasonLocal)
	}
}

// Собеседник ушёл — причина remote.
func TestRemoteCloseGivesRemoteReason(t *testing.T) {
	sink := newSink()
	_, id, far, _ := established(t, sink)

	_ = far.Close() // half-close: получатель увидит конец потока

	ev := sink.waitFor(t, eventClosed)
	var ce p2pClosedEvent
	if err := json.Unmarshal(ev.payload, &ce); err != nil {
		t.Fatal(err)
	}
	if ce.SessionID != id || ce.Reason != reasonRemote {
		t.Fatalf("p2p.closed = %+v, ожидалось reason=%s", ce, reasonRemote)
	}
}

// После закрытия handle снят с учёта, и все три операции дают NOT_FOUND —
// так же, как для чужого (ADR-076 §7.2, §3.8).
func TestOperationsAfterCloseAreNotFound(t *testing.T) {
	h, id, _, _ := established(t, newSink())
	mustCall(t, h, "plug-a", "p2p.close", p2pSessionRequest{SessionID: id})

	for _, op := range []string{"p2p.peer", "p2p.close"} {
		_, err := call(t, h, "plug-a", op, p2pSessionRequest{SessionID: id})
		requireCode(t, err, ipc.ErrNotFound, op+" после закрытия")
	}
	_, err := call(t, h, "plug-a", "p2p.send", p2pSendRequest{SessionID: id, Payload: []byte("x")})
	requireCode(t, err, ipc.ErrNotFound, "p2p.send после закрытия")
}

// Все p2p.message сессии приходят РАНЬШЕ её p2p.closed (ADR-076 §3.10).
func TestMessagesPrecedeClosedForTheSameSession(t *testing.T) {
	sink := newSink()
	h, id, far, _ := established(t, sink)

	for i := 0; i < 20; i++ {
		if err := far.Send([]byte{byte(i)}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	// Дать насосу разобрать очередь, затем закрыть.
	for i := 0; i < 20; i++ {
		sink.waitFor(t, eventMessage)
	}
	mustCall(t, h, "plug-a", "p2p.close", p2pSessionRequest{SessionID: id})
	sink.waitFor(t, eventClosed)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	seenClosed := false
	for _, e := range sink.got {
		if e.typ == eventClosed {
			seenClosed = true
			continue
		}
		if seenClosed && e.typ == eventMessage {
			t.Fatal("p2p.message пришло ПОСЛЕ p2p.closed той же сессии")
		}
	}
	if !seenClosed {
		t.Fatal("p2p.closed не пришло")
	}
}

// ── переполнение очереди событий ────────────────────────────────────

// Молчаливая потеря сообщения заменена громкой потерей сессии
// (ADR-076 §7.3).
func TestEventQueueOverflowClosesSessionWithSlowConsumer(t *testing.T) {
	sink := newSink()
	_, _, far, _ := established(t, sink)

	// Очередь «переполняется» на первом же сообщении.
	sink.mu.Lock()
	sink.full = true
	sink.mu.Unlock()

	if err := far.Send([]byte("overflow me")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// p2p.closed отправить тоже не удастся — очередь полна. Наблюдаем
	// последствие: сессия закрыта, её конец потока разорван.
	deadline := time.After(5 * time.Second)
	for {
		if _, err := far.Recv(); err != nil {
			return // поток разорван — сессия закрыта
		}
		select {
		case <-deadline:
			t.Fatal("сессия не закрыта после переполнения очереди")
		default:
		}
	}
}

// Когда очередь освободилась к моменту закрытия, причина видна явно.
func TestOverflowReasonIsSlowConsumerWhenClosedEventFits(t *testing.T) {
	sink := newSink()
	_, id, far, _ := established(t, sink)

	if err := far.Send([]byte("first")); err != nil {
		t.Fatal(err)
	}
	sink.waitFor(t, eventMessage)

	// Ровно одно следующее событие не влезает — дальше очередь снова
	// принимает, как и настоящая, которую непрерывно дренирует eventLoop.
	sink.mu.Lock()
	sink.failNext = 1
	sink.mu.Unlock()

	if err := far.Send([]byte("second")); err != nil {
		t.Fatal(err)
	}

	ev := sink.waitFor(t, eventClosed)
	var ce p2pClosedEvent
	if err := json.Unmarshal(ev.payload, &ce); err != nil {
		t.Fatal(err)
	}
	if ce.SessionID != id || ce.Reason != reasonSlowConsumer {
		t.Fatalf("p2p.closed = %+v, ожидалось reason=%s", ce, reasonSlowConsumer)
	}
}

// ── жизненный цикл плагина ──────────────────────────────────────────

// Сессия не переживает плагина: иначе утечка сокетов и ICE-агентов, а
// собеседник ждёт ответа от того, кого нет (ADR-076 §3.8).
func TestPluginStopClosesItsSessionsOnly(t *testing.T) {
	sink := newSink()
	localA, _ := streamPair()
	localB, _ := streamPair()
	sessA := newFakeSession(ports.PublicKey("pa"), localA)
	sessB := newFakeSession(ports.PublicKey("pb"), localB)

	f := &fakeP2P{answer: []byte("A")}
	h := newP2PHandler(t, f, sink)

	f.mu.Lock()
	f.nextAccept = sessA
	f.mu.Unlock()
	var accA p2pAcceptOfferResponse
	callInto(t, h, "plug-a", "p2p.accept_offer",
		p2pAcceptOfferRequest{PeerPubKey: []byte("pa"), Offer: []byte("O")}, &accA)

	f.mu.Lock()
	f.nextAccept = sessB
	f.mu.Unlock()
	var accB p2pAcceptOfferResponse
	callInto(t, h, "plug-b", "p2p.accept_offer",
		p2pAcceptOfferRequest{PeerPubKey: []byte("pb"), Offer: []byte("O")}, &accB)

	h.ClosePluginSessions("plug-a")

	_, err := call(t, h, "plug-a", "p2p.peer", p2pSessionRequest{SessionID: accA.SessionID})
	requireCode(t, err, ipc.ErrNotFound, "сессия остановленного плагина")

	// Чужая сессия не тронута — иначе остановка одного плагина рвала бы
	// связь другому.
	var pr p2pPeerResponse
	callInto(t, h, "plug-b", "p2p.peer", p2pSessionRequest{SessionID: accB.SessionID}, &pr)
	if string(pr.PeerPubKey) != "pb" {
		t.Fatalf("сессия соседа пострадала: peer %q", pr.PeerPubKey)
	}
}

func TestPluginStopEmitsPluginStoppedReason(t *testing.T) {
	sink := newSink()
	h, id, _, _ := established(t, sink)
	h.ClosePluginSessions("plug-a")

	ev := sink.waitFor(t, eventClosed)
	var ce p2pClosedEvent
	if err := json.Unmarshal(ev.payload, &ce); err != nil {
		t.Fatal(err)
	}
	if ce.SessionID != id || ce.Reason != reasonPluginStopped {
		t.Fatalf("p2p.closed = %+v, ожидалось reason=%s", ce, reasonPluginStopped)
	}
}

// Согласование, не дошедшее до сессии, тоже не переживает плагина.
func TestPluginStopDropsPendingNegotiations(t *testing.T) {
	f := &fakeP2P{offer: []byte("O")}
	h := newP2PHandler(t, f, newSink())

	var off p2pCreateOfferResponse
	callInto(t, h, "plug-a", "p2p.create_offer",
		p2pCreateOfferRequest{PeerPubKey: []byte("p")}, &off)

	h.ClosePluginSessions("plug-a")

	_, err := call(t, h, "plug-a", "p2p.complete",
		p2pCompleteRequest{NegotiationID: off.NegotiationID, Answer: []byte("A")})
	requireCode(t, err, ipc.ErrNotFound, "согласование после остановки")
}

// ── провал установления ─────────────────────────────────────────────

func TestEstablishmentFailureGivesFailedReason(t *testing.T) {
	sink := newSink()
	sess := newPendingSession(ports.PublicKey("peer"))
	f := &fakeP2P{answer: []byte("A"), nextAccept: sess}
	h := newP2PHandler(t, f, sink)

	var acc p2pAcceptOfferResponse
	callInto(t, h, "plug-a", "p2p.accept_offer",
		p2pAcceptOfferRequest{PeerPubKey: []byte("peer"), Offer: []byte("O")}, &acc)

	sess.release(nil, ports.ErrSessionFailed)

	ev := sink.waitFor(t, eventClosed)
	var ce p2pClosedEvent
	if err := json.Unmarshal(ev.payload, &ce); err != nil {
		t.Fatal(err)
	}
	if ce.Reason != reasonFailed {
		t.Fatalf("reason %q, ожидался %s", ce.Reason, reasonFailed)
	}
	_ = h
	_ = io.EOF
}

// ── измерение: 1 MiB через границу IPC ──────────────────────────────

// ADR-076 §7.8 перенёс этот вопрос в реализацию. Здесь он ИЗМЕРЯЕТСЯ, а не
// оценивается: событие p2p.message с нагрузкой 1 MiB кодируется настоящим
// кодеком IPC и разбирается обратно.
func TestOneMiBPayloadFitsThroughIPCFrame(t *testing.T) {
	payload := make([]byte, p2pmsg.MaxMessageSize)
	for i := range payload {
		payload[i] = byte(i*13 + 5)
	}
	body, err := json.Marshal(p2pMessageEvent{
		SessionID: "0123456789abcdef0123456789abcdef",
		Payload:   payload,
	})
	if err != nil {
		t.Fatal(err)
	}

	var buf bytesBuffer
	msg := &ipc.Message{Type: ipc.MsgEvent, EventType: eventMessage, Payload: body}
	if err := ipc.Encode(&buf, msg); err != nil {
		t.Fatalf("кадр IPC с нагрузкой 1 MiB не кодируется: %v", err)
	}
	wire := buf.Len()

	got, err := ipc.Decode(&buf)
	if err != nil {
		t.Fatalf("кадр IPC с нагрузкой 1 MiB не разбирается: %v", err)
	}
	var back p2pMessageEvent
	if err := json.Unmarshal(got.Payload, &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Payload) != len(payload) {
		t.Fatalf("после прохода через IPC %d байт вместо %d", len(back.Payload), len(payload))
	}
	for i := range back.Payload {
		if back.Payload[i] != payload[i] {
			t.Fatalf("байт %d искажён при проходе через IPC", i)
		}
	}

	t.Logf("нагрузка p2pmsg 1 MiB через границу IPC: "+
		"сырых %d Б → JSON(base64) %d Б → кадр IPC %d Б (×%.3f от сырых)",
		len(payload), len(body), wire, float64(wire)/float64(len(payload)))
}

// bytesBuffer — минимальный io.ReadWriter, чтобы не тянуть bytes в импорты
// поверх уже используемых.
type bytesBuffer struct {
	b   []byte
	pos int
}

func (b *bytesBuffer) Write(p []byte) (int, error) { b.b = append(b.b, p...); return len(p), nil }
func (b *bytesBuffer) Read(p []byte) (int, error) {
	if b.pos >= len(b.b) {
		return 0, io.EOF
	}
	n := copy(p, b.b[b.pos:])
	b.pos += n
	return n, nil
}
func (b *bytesBuffer) Len() int { return len(b.b) }

var _ = strings.Contains
