// Package p2phost — P2P-исполнитель Photon Core для хоста приложения
// (ADR-116 §3): согласование, обмен и закрытие P2P-сессий поверх
// ports.P2P (adapters/pion) и p2pmsg. Доступен хосту через локальные
// действия диспетчера 860–865.
//
// Это не второй транспорт: одна ICE/DTLS/SCTP-ассоциация на сессию, один
// bidi-поток, кадры p2pmsg (ADR-075). Наружу не проходит ни один тип Pion,
// ни ports.Session, ни ports.Stream — только непрозрачные строки handle и
// байты нагрузки (тот же принцип, что ADR-076 §3.2).
//
// События отдаются опросом (Poll), а не колбэком ADR-045: колбэк вызывает
// ровно один поток — насос подписки Bridge, — и на этом стоят
// реентерабельный Photon_Destroy и определение потока колбэка. Второй
// источник вызовов колбэка из горутин P2P эти гарантии сломал бы
// (ADR-116 §3.2, уточнение при реализации).
package legacy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	pionadapter "github.com/wiloris/photon/photon-core/adapters/pion"
	"github.com/wiloris/photon/photon-core/p2pmsg"
	"github.com/wiloris/photon/photon-core/ports"
)

// Причины закрытия — закрытый перечень ADR-116 §3.2 (+ slow_consumer — тот
// же смысл, что ADR-076 §3.4: потребитель не забирает события).
const (
	ReasonLocal        = "local"
	ReasonRemote       = "remote"
	ReasonFailed       = "failed"
	ReasonSlowConsumer = "slow_consumer"
)

// Типы событий.
const (
	EventEstablished = "P2P_ESTABLISHED"
	EventMessage     = "P2P_MESSAGE"
	EventClosed      = "P2P_CLOSED"
)

var (
	ErrNotFound      = errors.New("p2phost: not found")
	ErrNotReady      = errors.New("p2phost: session not established")
	ErrTooMany       = errors.New("p2phost: too many sessions")
	ErrBadArgument   = errors.New("p2phost: bad argument")
	ErrClosed        = errors.New("p2phost: executor closed")
	ErrPayloadTooBig = errors.New("p2phost: payload too large")
)

const (
	// MaxSessions — предел одновременных сессий и согласований инстанса:
	// каждая держит ICE-агент, сокеты и горутины.
	MaxSessions = 8
	// MaxPayload — предел одного сообщения. Меньше p2pmsg.MaxMessageSize:
	// событие кодируется в JSON с base64 и проходит C ABI одним буфером.
	MaxPayload = 64 * 1024
	// MaxQueuedEvents — предел очереди событий. Переполнение не теряет
	// сообщения молча: сессия, чьё сообщение не поместилось, закрывается
	// с reason slow_consumer.
	MaxQueuedEvents = 1024
	// MaxPoll — предел событий за один опрос.
	MaxPoll = 64
)

// TURN — краткоживущие учётные данные одного согласования (ADR-116 §4.6).
type TURN struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username"`
	Credential string   `json:"credential"`
}

// Config — композиция исполнителя.
type Config struct {
	Identity ports.Identity
	Self     ports.IdentityPair

	EstablishTimeout time.Duration
	PeerLossTimeout  time.Duration

	// newAdapter строит адаптер на одно согласование (свои учётные данные
	// TURN у каждого). Только тесты подменяют — для виртуальной сети.
	newAdapter func(pionadapter.Config) (ports.P2P, error)
}

// Event — событие для хоста.
type Event struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Payload   []byte `json:"payload,omitempty"`
	Reason    string `json:"reason,omitempty"`
	// Path — тип выбранной пары ICE, для диагностики: "direct" или "relay".
	Path string `json:"path,omitempty"`
}

type negotiation struct {
	adapter ports.P2P
	peer    ports.PublicKey
	timer   *time.Timer
	// cancel снимает контекст согласования. Он живёт до исхода, а не до
	// возврата из вызова: сторож адаптера освобождает согласование по
	// отмене контекста CreateOffer, а контекст локального действия
	// отменяется сразу после ответа хосту (ловушка ADR-044 §6.1).
	cancel context.CancelFunc
}

type session struct {
	id        string
	initiator bool
	adapter   ports.P2P
	sess      ports.Session
	ctx       context.Context
	cancel    context.CancelFunc

	ready chan struct{} // закрыт: conv готов либо установление провалилось
	conv  *p2pmsg.Conversation

	sendMu sync.Mutex

	mu     sync.Mutex
	closed bool
}

// Executor — реестр согласований и сессий одного инстанса Core.
type Executor struct {
	cfg Config

	mu           sync.Mutex
	closed       bool
	negotiations map[string]*negotiation
	sessions     map[string]*session
	events       []Event
	wg           sync.WaitGroup
}

// New проверяет композицию.
func New(cfg Config) (*Executor, error) {
	if cfg.Identity == nil || len(cfg.Self.Public) == 0 || len(cfg.Self.Private) == 0 {
		return nil, fmt.Errorf("%w: identity and device key pair are required", ErrBadArgument)
	}
	if cfg.EstablishTimeout <= 0 {
		cfg.EstablishTimeout = 20 * time.Second
	}
	if cfg.newAdapter == nil {
		cfg.newAdapter = func(c pionadapter.Config) (ports.P2P, error) { return pionadapter.New(c) }
	}
	return &Executor{cfg: cfg, negotiations: map[string]*negotiation{}, sessions: map[string]*session{}}, nil
}

func newHandle() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (e *Executor) adapterFor(turn *TURN) (ports.P2P, error) {
	c := pionadapter.Config{
		Identity:         e.cfg.Identity,
		Self:             e.cfg.Self,
		EstablishTimeout: e.cfg.EstablishTimeout,
		PeerLossTimeout:  e.cfg.PeerLossTimeout,
	}
	if turn != nil {
		c.STUNTURN, c.TURNUser, c.TURNPass = turn.URLs, turn.Username, turn.Credential
	}
	return e.cfg.newAdapter(c)
}

// reserve проверяет предел и закрытость под замком.
func (e *Executor) reserveLocked() error {
	if e.closed {
		return ErrClosed
	}
	if len(e.negotiations)+len(e.sessions) >= MaxSessions {
		return ErrTooMany
	}
	return nil
}

// Offer — шаг инициатора: offer для собеседника с ожидаемым ключом peer.
func (e *Executor) Offer(_ context.Context, peer ports.PublicKey, turn *TURN) (string, []byte, error) {
	if len(peer) == 0 {
		return "", nil, fmt.Errorf("%w: peer public key is required", ErrBadArgument)
	}
	e.mu.Lock()
	if err := e.reserveLocked(); err != nil {
		e.mu.Unlock()
		return "", nil, err
	}
	e.mu.Unlock()

	a, err := e.adapterFor(turn)
	if err != nil {
		return "", nil, err
	}
	// Срок — тот же, что у таймера согласования ниже: и сбор кандидатов, и
	// ожидание answer ограничены, но не возвратом из вызова.
	nctx, ncancel := context.WithTimeout(context.Background(), e.cfg.EstablishTimeout)
	offer, err := a.CreateOffer(nctx, peer)
	if err != nil {
		ncancel()
		_ = a.Close()
		return "", nil, err
	}
	id, err := newHandle()
	if err != nil {
		ncancel()
		_ = a.Close()
		return "", nil, err
	}
	n := &negotiation{adapter: a, peer: peer, cancel: ncancel}
	e.mu.Lock()
	if err := e.reserveLocked(); err != nil {
		e.mu.Unlock()
		ncancel()
		_ = a.Close()
		return "", nil, err
	}
	e.negotiations[id] = n
	// Незавершённое согласование не живёт вечно: ответ может не прийти
	// никогда (ADR-076 §3.6, тот же довод).
	n.timer = time.AfterFunc(e.cfg.EstablishTimeout, func() { e.dropNegotiation(id) })
	e.mu.Unlock()
	return id, offer, nil
}

func (e *Executor) dropNegotiation(id string) {
	e.mu.Lock()
	n := e.negotiations[id]
	delete(e.negotiations, id)
	e.mu.Unlock()
	if n != nil {
		n.cancel()
		_ = n.adapter.Close()
	}
}

// Accept — шаг отвечающего: answer и сессия, которая устанавливается фоном.
func (e *Executor) Accept(ctx context.Context, peer ports.PublicKey, offer []byte, turn *TURN) (string, []byte, error) {
	if len(peer) == 0 || len(offer) == 0 {
		return "", nil, fmt.Errorf("%w: peer public key and offer are required", ErrBadArgument)
	}
	e.mu.Lock()
	if err := e.reserveLocked(); err != nil {
		e.mu.Unlock()
		return "", nil, err
	}
	e.mu.Unlock()

	a, err := e.adapterFor(turn)
	if err != nil {
		return "", nil, err
	}
	// Контекст согласования живёт до исхода, а не до возврата из вызова:
	// ловушка ADR-044 §6.1.
	sctx, cancel := context.WithCancel(context.Background())
	answer, ps, err := a.AcceptOffer(sctx, peer, offer)
	if err != nil {
		cancel()
		_ = a.Close()
		return "", nil, err
	}
	id, err := e.track(a, ps, sctx, cancel, false)
	if err != nil {
		return "", nil, err
	}
	return id, answer, nil
}

// Complete — шаг инициатора по answer собеседника. negotiation_id
// потребляется ровно один раз (ADR-044 §6).
func (e *Executor) Complete(ctx context.Context, negotiationID string, answer []byte) (string, error) {
	if len(answer) == 0 {
		return "", fmt.Errorf("%w: answer is required", ErrBadArgument)
	}
	e.mu.Lock()
	n := e.negotiations[negotiationID]
	delete(e.negotiations, negotiationID)
	e.mu.Unlock()
	if n == nil {
		return "", ErrNotFound
	}
	n.timer.Stop()
	sctx, cancel := context.WithCancel(context.Background())
	ps, err := n.adapter.CompleteSession(sctx, n.peer, answer)
	// Согласование забрано адаптером (или отвергнуто) — его контекст больше
	// ничего не держит.
	n.cancel()
	if err != nil {
		cancel()
		_ = n.adapter.Close()
		return "", err
	}
	return e.track(n.adapter, ps, sctx, cancel, true)
}

func (e *Executor) track(a ports.P2P, ps ports.Session, ctx context.Context, cancel context.CancelFunc, initiator bool) (string, error) {
	id, err := newHandle()
	if err != nil {
		cancel()
		_ = ps.Close()
		_ = a.Close()
		return "", err
	}
	s := &session{id: id, initiator: initiator, adapter: a, sess: ps, ctx: ctx, cancel: cancel, ready: make(chan struct{})}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		cancel()
		_ = ps.Close()
		_ = a.Close()
		return "", ErrClosed
	}
	e.sessions[id] = s
	e.wg.Add(1)
	e.mu.Unlock()
	go e.run(s)
	return id, nil
}

// run — жизнь сессии: установление, поток, приём до исхода.
func (e *Executor) run(s *session) {
	defer e.wg.Done()
	// Peer() ждёт установления; срок ограничен EstablishTimeout адаптера.
	if s.sess.Peer() == nil {
		close(s.ready)
		e.finish(s, ReasonFailed)
		return
	}
	var st ports.Stream
	var err error
	if s.initiator {
		st, err = s.sess.OpenStream(s.ctx)
	} else {
		st, err = s.sess.AcceptStream(s.ctx)
	}
	if err != nil {
		close(s.ready)
		e.finish(s, ReasonFailed)
		return
	}
	conv := p2pmsg.New(st)
	if s.initiator {
		// Пустой кадр-«привет»: AcceptStream у собеседника возвращается
		// только после первых байтов потока, а разговор может начать
		// отвечающий. Пустое сообщение p2pmsg допустимо (ADR-075) и
		// наружу не отдаётся.
		if err := conv.Send(nil); err != nil {
			close(s.ready)
			e.finish(s, ReasonFailed)
			return
		}
	}
	s.conv = conv
	close(s.ready)
	e.push(Event{Type: EventEstablished, SessionID: s.id, Path: pathOf(s.sess.RemoteEndpoint())})

	first := !s.initiator
	for {
		msg, err := s.conv.Recv()
		if err != nil {
			// io.EOF — собеседник закрыл штатно; потеря собеседника
			// приходит как ports.ErrSessionFailed (adapters/pion), не EOF.
			if errors.Is(err, io.EOF) {
				e.finish(s, ReasonRemote)
			} else {
				e.finish(s, ReasonFailed)
			}
			return
		}
		if first {
			// Кадр-«привет» инициатора.
			first = false
			if len(msg) == 0 {
				continue
			}
		}
		if !e.pushMessage(s, msg) {
			return
		}
	}
}

// pathOf — «direct» или «relay» по выбранной паре ICE.
func pathOf(ep ports.Endpoint) string {
	if strings.Contains(string(ep), " relay ") {
		return "relay"
	}
	return "direct"
}

func (e *Executor) push(ev Event) {
	e.mu.Lock()
	e.events = append(e.events, ev)
	e.mu.Unlock()
}

// pushMessage кладёт сообщение; при переполнении закрывает сессию
// (slow_consumer) — молча терять нельзя.
func (e *Executor) pushMessage(s *session, msg []byte) bool {
	e.mu.Lock()
	full := len(e.events) >= MaxQueuedEvents
	if !full {
		e.events = append(e.events, Event{Type: EventMessage, SessionID: s.id, Payload: msg})
	}
	e.mu.Unlock()
	if full {
		e.finish(s, ReasonSlowConsumer)
		return false
	}
	return true
}

// finish закрывает сессию и сообщает причину ровно один раз; первая
// причина побеждает. Событие закрытия кладётся сверх предела очереди: их
// не больше, чем сессий.
func (e *Executor) finish(s *session, reason string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	s.cancel()
	_ = s.sess.Close()
	_ = s.adapter.Close()
	e.mu.Lock()
	delete(e.sessions, s.id)
	e.events = append(e.events, Event{Type: EventClosed, SessionID: s.id, Reason: reason})
	e.mu.Unlock()
}

func (e *Executor) lookup(id string) (*session, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sessions[id]
	return s, ok
}

// Send отправляет сообщение по установленной сессии.
func (e *Executor) Send(id string, payload []byte) error {
	if len(payload) > MaxPayload {
		return ErrPayloadTooBig
	}
	s, ok := e.lookup(id)
	if !ok {
		return ErrNotFound
	}
	select {
	case <-s.ready:
	default:
		return ErrNotReady
	}
	if s.conv == nil {
		return ErrNotReady
	}
	// p2pmsg.Conversation не потокобезопасен для параллельных Send.
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if err := s.conv.Send(payload); err != nil {
		e.finish(s, ReasonFailed)
		return fmt.Errorf("%w: %v", ports.ErrSessionFailed, err)
	}
	return nil
}

// Close закрывает свою сессию либо незавершённое согласование.
func (e *Executor) Close(id string) error {
	if s, ok := e.lookup(id); ok {
		e.finish(s, ReasonLocal)
		return nil
	}
	e.mu.Lock()
	_, isNeg := e.negotiations[id]
	e.mu.Unlock()
	if isNeg {
		e.dropNegotiation(id)
		return nil
	}
	return ErrNotFound
}

// Poll забирает до max событий в порядке появления.
func (e *Executor) Poll(max int) []Event {
	if max <= 0 || max > MaxPoll {
		max = MaxPoll
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	n := max
	if n > len(e.events) {
		n = len(e.events)
	}
	out := append([]Event(nil), e.events[:n]...)
	e.events = append(e.events[:0], e.events[n:]...)
	return out
}

// Shutdown закрывает всё и ждёт горутины сессий.
func (e *Executor) Shutdown() {
	e.mu.Lock()
	e.closed = true
	sessions := make([]*session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	negs := make([]string, 0, len(e.negotiations))
	for id := range e.negotiations {
		negs = append(negs, id)
	}
	e.mu.Unlock()
	for _, s := range sessions {
		e.finish(s, ReasonLocal)
	}
	for _, id := range negs {
		e.dropNegotiation(id)
	}
	e.wg.Wait()
}

// Counts — число живых сессий и согласований (для проверок утечек).
func (e *Executor) Counts() (sessions, negotiations int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.sessions), len(e.negotiations)
}

// MarshalEvents кодирует события для конверта локального действия.
func MarshalEvents(evs []Event) ([]byte, error) {
	if evs == nil {
		evs = []Event{}
	}
	return json.Marshal(map[string]any{"events": evs})
}
