// Package p2phost — единый P2P-исполнитель Photon: согласование, обмен и
// закрытие P2P-сессий поверх ports.P2P (adapters/pion) и p2pmsg. У него два
// потребителя, и оба говорят каждый на своём, прежнем проводе:
//
//   - хост приложения через локальные действия диспетчера 860–868
//     (ADR-116 §3): адаптер на согласование, «привет» в потоке по умолчанию,
//     служебные потоки, события опросом (Poll);
//   - Photon Plugin Runtime через coreops (ADR-076): общий адаптер
//     композиции, без «привета» и без служебных потоков, события — в
//     Config.Sink для владельца-плагина.
//
// Это не второй транспорт: одна ICE/DTLS/SCTP-ассоциация на сессию, кадры
// p2pmsg (ADR-075). Наружу не проходит ни один тип Pion, ни ports.Session,
// ни ports.Stream — только непрозрачные строки handle и байты нагрузки
// (ADR-076 §3.2).
//
// Опрос, а не колбэк ADR-045, для хоста приложения: колбэк вызывает ровно
// один поток — насос подписки Bridge, — и на этом стоят реентерабельный
// Photon_Destroy и определение потока колбэка (ADR-116 §3.2, уточнение при
// реализации). Sink — для композиций, у которых такого ограничения нет.
package p2phost

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
// же смысл, что ADR-076 §3.4: потребитель не забирает события). Причину
// закрытия владельцем (например, plugin_stopped ADR-076 §3.8) задаёт сам
// потребитель в CloseOwner.
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
	// MaxSessions — предел одновременных сессий и согласований инстанса по
	// умолчанию: каждая держит ICE-агент, сокеты и горутины.
	MaxSessions = 8
	// MaxPayload — предел одного сообщения по умолчанию. Меньше
	// p2pmsg.MaxMessageSize: событие кодируется в JSON с base64 и проходит C
	// ABI одним буфером.
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

// Config — композиция исполнителя. Нулевые значения флагов дают поведение
// хоста приложения (ADR-116), каким оно было до введения потребителей.
type Config struct {
	Identity ports.Identity
	Self     ports.IdentityPair

	EstablishTimeout time.Duration
	PeerLossTimeout  time.Duration
	// AnnounceWait — сколько OpenStream ждёт объявления служебных потоков
	// собеседником; не дождался — ErrPeerUnsupported (старый узел).
	AnnounceWait time.Duration
	// StreamOpenTimeout — срок ответа собеседника на открытие потока.
	StreamOpenTimeout time.Duration

	// Adapter — общий адаптер композиции (coreops, ADR-076). Задан —
	// Identity/Self не нужны, адаптер на согласование не строится, TURN на
	// согласование не принимается, и исполнитель этот адаптер НЕ закрывает:
	// им владеет композиция.
	Adapter ports.P2P
	// NoHello — поток по умолчанию без кадра-«привета»: провод ADR-076, где
	// инициатор не пишет пустой кадр, а отвечающий доставляет каждый кадр,
	// включая пустой первый. Нулевое значение — провод ADR-116.
	NoHello bool
	// NoStreams — без служебных потоков: ни объявления, ни приёма.
	NoStreams bool
	// PayloadLimit — предел одного сообщения; 0 — MaxPayload.
	PayloadLimit int
	// SessionLimit — предел сессий и согласований; 0 — MaxSessions,
	// отрицательное — без предела (ADR-076 §7, решение 9).
	SessionLimit int
	// Sink — доставка событий потребителю вместо очереди Poll. Вызывается под
	// замком исполнителя: не блокирует и не вызывает исполнитель. false на
	// сообщении — потребитель не принял, сессия (или поток) закрывается
	// slow_consumer; на прочих событиях результат не учитывается.
	Sink func(owner string, ev Event) bool

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
	// StreamID и Service — у событий служебных потоков (streams.go).
	StreamID string `json:"stream_id,omitempty"`
	Service  string `json:"service,omitempty"`
	// Owner — владелец сессии; наружу (JSON 865) не выходит.
	Owner string `json:"-"`
}

type negotiation struct {
	owner   string
	adapter ports.P2P
	shared  bool // адаптер композиции: не закрывать
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
	owner     string
	initiator bool
	adapter   ports.P2P
	shared    bool
	sess      ports.Session
	ctx       context.Context
	cancel    context.CancelFunc

	ready chan struct{} // закрыт: conv готов либо установление провалилось
	conv  *p2pmsg.Conversation

	sendMu sync.Mutex

	mu     sync.Mutex
	closed bool
	reason string // первая причина побеждает

	// peerStreams закрывается, когда собеседник объявил служебные потоки;
	// peerVersion — действующая версия конверта (streams.go).
	peerStreams chan struct{}
	peerOnce    sync.Once
	peerVersion int
	// streams — число служебных потоков на учёте; под Executor.mu.
	streams int
}

// Executor — реестр согласований и сессий одного инстанса.
type Executor struct {
	cfg Config

	mu           sync.Mutex
	closed       bool
	negotiations map[string]*negotiation
	sessions     map[string]*session
	streams      map[string]*svcStream
	events       []Event
	wg           sync.WaitGroup
}

// New проверяет композицию.
func New(cfg Config) (*Executor, error) {
	if cfg.Adapter == nil && (cfg.Identity == nil || len(cfg.Self.Public) == 0 || len(cfg.Self.Private) == 0) {
		return nil, fmt.Errorf("%w: identity and device key pair are required", ErrBadArgument)
	}
	if cfg.EstablishTimeout <= 0 {
		cfg.EstablishTimeout = 20 * time.Second
	}
	if cfg.AnnounceWait <= 0 {
		cfg.AnnounceWait = 3 * time.Second
	}
	if cfg.StreamOpenTimeout <= 0 {
		cfg.StreamOpenTimeout = 5 * time.Second
	}
	if cfg.PayloadLimit <= 0 {
		cfg.PayloadLimit = MaxPayload
	}
	if cfg.SessionLimit == 0 {
		cfg.SessionLimit = MaxSessions
	}
	if cfg.newAdapter == nil {
		cfg.newAdapter = func(c pionadapter.Config) (ports.P2P, error) { return pionadapter.New(c) }
	}
	return &Executor{cfg: cfg, negotiations: map[string]*negotiation{}, sessions: map[string]*session{}, streams: map[string]*svcStream{}}, nil
}

func newHandle() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// adapterFor — адаптер согласования и признак «общий, не закрывать».
func (e *Executor) adapterFor(turn *TURN) (ports.P2P, bool, error) {
	if e.cfg.Adapter != nil {
		if turn != nil {
			return nil, false, fmt.Errorf("%w: TURN per negotiation needs an adapter per negotiation", ErrBadArgument)
		}
		return e.cfg.Adapter, true, nil
	}
	c := pionadapter.Config{
		Identity:         e.cfg.Identity,
		Self:             e.cfg.Self,
		EstablishTimeout: e.cfg.EstablishTimeout,
		PeerLossTimeout:  e.cfg.PeerLossTimeout,
	}
	if turn != nil {
		c.STUNTURN, c.TURNUser, c.TURNPass = turn.URLs, turn.Username, turn.Credential
	}
	a, err := e.cfg.newAdapter(c)
	return a, false, err
}

func closeAdapter(a ports.P2P, shared bool) {
	if !shared {
		_ = a.Close()
	}
}

// reserve проверяет предел и закрытость под замком.
func (e *Executor) reserveLocked() error {
	if e.closed {
		return ErrClosed
	}
	if e.cfg.SessionLimit > 0 && len(e.negotiations)+len(e.sessions) >= e.cfg.SessionLimit {
		return ErrTooMany
	}
	return nil
}

// Offer — шаг инициатора хоста приложения (владелец по умолчанию).
func (e *Executor) Offer(ctx context.Context, peer ports.PublicKey, turn *TURN) (string, []byte, error) {
	return e.OfferAs(ctx, "", peer, turn)
}

// OfferAs — шаг инициатора: offer для собеседника с ожидаемым ключом peer;
// согласование принадлежит owner.
func (e *Executor) OfferAs(_ context.Context, owner string, peer ports.PublicKey, turn *TURN) (string, []byte, error) {
	if len(peer) == 0 {
		return "", nil, fmt.Errorf("%w: peer public key is required", ErrBadArgument)
	}
	e.mu.Lock()
	if err := e.reserveLocked(); err != nil {
		e.mu.Unlock()
		return "", nil, err
	}
	e.mu.Unlock()

	a, shared, err := e.adapterFor(turn)
	if err != nil {
		return "", nil, err
	}
	// Срок — тот же, что у таймера согласования ниже: и сбор кандидатов, и
	// ожидание answer ограничены, но не возвратом из вызова.
	nctx, ncancel := context.WithTimeout(context.Background(), e.cfg.EstablishTimeout)
	offer, err := a.CreateOffer(nctx, peer)
	if err != nil {
		ncancel()
		closeAdapter(a, shared)
		return "", nil, err
	}
	id, err := newHandle()
	if err != nil {
		ncancel()
		closeAdapter(a, shared)
		return "", nil, err
	}
	n := &negotiation{owner: owner, adapter: a, shared: shared, peer: peer, cancel: ncancel}
	e.mu.Lock()
	if err := e.reserveLocked(); err != nil {
		e.mu.Unlock()
		ncancel()
		closeAdapter(a, shared)
		return "", nil, err
	}
	e.negotiations[id] = n
	// Незавершённое согласование не живёт вечно: ответ может не прийти
	// никогда (ADR-076 §3.6).
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
		n.timer.Stop()
		n.cancel()
		closeAdapter(n.adapter, n.shared)
	}
}

// Accept — шаг отвечающего хоста приложения (владелец по умолчанию).
func (e *Executor) Accept(ctx context.Context, peer ports.PublicKey, offer []byte, turn *TURN) (string, []byte, error) {
	return e.AcceptAs(ctx, "", peer, offer, turn)
}

// AcceptAs — шаг отвечающего: answer и сессия владельца owner, которая
// устанавливается фоном.
func (e *Executor) AcceptAs(ctx context.Context, owner string, peer ports.PublicKey, offer []byte, turn *TURN) (string, []byte, error) {
	if len(peer) == 0 || len(offer) == 0 {
		return "", nil, fmt.Errorf("%w: peer public key and offer are required", ErrBadArgument)
	}
	e.mu.Lock()
	if err := e.reserveLocked(); err != nil {
		e.mu.Unlock()
		return "", nil, err
	}
	e.mu.Unlock()

	a, shared, err := e.adapterFor(turn)
	if err != nil {
		return "", nil, err
	}
	// Контекст согласования живёт до исхода, а не до возврата из вызова:
	// ловушка ADR-044 §6.1.
	sctx, cancel := context.WithCancel(context.Background())
	answer, ps, err := a.AcceptOffer(sctx, peer, offer)
	if err != nil {
		cancel()
		closeAdapter(a, shared)
		return "", nil, err
	}
	id, err := e.track(owner, a, shared, ps, sctx, cancel, false)
	if err != nil {
		return "", nil, err
	}
	return id, answer, nil
}

// Complete — шаг инициатора хоста приложения (владелец по умолчанию).
func (e *Executor) Complete(ctx context.Context, negotiationID string, answer []byte) (string, error) {
	return e.CompleteAs(ctx, "", negotiationID, answer)
}

// CompleteAs — шаг инициатора по answer собеседника. negotiation_id
// потребляется ровно один раз (ADR-044 §6) и только своим владельцем:
// чужое и неизвестное согласование неотличимы (ADR-076 §3.7).
func (e *Executor) CompleteAs(ctx context.Context, owner, negotiationID string, answer []byte) (string, error) {
	if len(answer) == 0 {
		return "", fmt.Errorf("%w: answer is required", ErrBadArgument)
	}
	e.mu.Lock()
	n := e.negotiations[negotiationID]
	if n == nil || n.owner != owner {
		e.mu.Unlock()
		return "", ErrNotFound
	}
	delete(e.negotiations, negotiationID)
	e.mu.Unlock()
	n.timer.Stop()
	sctx, cancel := context.WithCancel(context.Background())
	ps, err := n.adapter.CompleteSession(sctx, n.peer, answer)
	// Согласование забрано адаптером (или отвергнуто) — его контекст больше
	// ничего не держит.
	n.cancel()
	if err != nil {
		cancel()
		closeAdapter(n.adapter, n.shared)
		return "", err
	}
	return e.track(owner, n.adapter, n.shared, ps, sctx, cancel, true)
}

func (e *Executor) track(owner string, a ports.P2P, shared bool, ps ports.Session, ctx context.Context, cancel context.CancelFunc, initiator bool) (string, error) {
	id, err := newHandle()
	if err != nil {
		cancel()
		_ = ps.Close()
		closeAdapter(a, shared)
		return "", err
	}
	s := &session{id: id, owner: owner, initiator: initiator, adapter: a, shared: shared, sess: ps, ctx: ctx, cancel: cancel,
		ready: make(chan struct{}), peerStreams: make(chan struct{})}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		cancel()
		_ = ps.Close()
		closeAdapter(a, shared)
		return "", ErrClosed
	}
	e.sessions[id] = s
	e.wg.Add(1)
	e.mu.Unlock()
	go e.run(s)
	return id, nil
}

// run — жизнь сессии: установление, поток по умолчанию, приём до исхода.
// Единственный отправитель P2P_MESSAGE и P2P_CLOSED сессии: поэтому все её
// сообщения приходят раньше её закрытия (ADR-076 §3.10) по построению.
func (e *Executor) run(s *session) {
	defer e.wg.Done()
	defer e.emitClosed(s)
	// Поток — первым: и открытие, и приём ждут установления (ADR-044 §1) и
	// подчиняются контексту сессии, который снимает закрытие. Peer() после
	// этого не ждёт: сессия уже установлена либо отказала.
	var st ports.Stream
	var err error
	if s.initiator {
		st, err = s.sess.OpenStream(s.ctx)
	} else {
		st, err = s.sess.AcceptStream(s.ctx)
	}
	if err != nil || s.sess.Peer() == nil {
		close(s.ready)
		e.finish(s, ReasonFailed)
		return
	}
	conv := p2pmsg.New(st)
	if s.initiator && !e.cfg.NoHello {
		// Пустой кадр-«привет» — часть провода хоста приложения (ADR-116):
		// отвечающий пропускает ровно первое пустое сообщение. Отвечающему
		// он для AcceptStream не нужен (признак направления адаптер пишет
		// при открытии), но узлы этого провода его ждут — убрать нельзя.
		if err := conv.Send(nil); err != nil {
			close(s.ready)
			e.finish(s, ReasonFailed)
			return
		}
	}
	s.conv = conv
	close(s.ready)
	e.pushIfAlive(s, Event{Type: EventEstablished, SessionID: s.id, Path: pathOf(s.sess.RemoteEndpoint())}, false)
	if !e.cfg.NoStreams {
		// Объявление служебных потоков — только теперь: инициатор уже
		// записал «привет», отвечающий уже занял поток по умолчанию
		// (streams.go).
		e.startStreams(s)
	}

	first := !s.initiator && !e.cfg.NoHello
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

// deliverLocked отдаёт событие потребителю: в Sink либо в очередь. bounded
// — событие подчиняется пределу очереди; false — не принято.
func (e *Executor) deliverLocked(ev Event, bounded bool) bool {
	if e.cfg.Sink != nil {
		ok := e.cfg.Sink(ev.Owner, ev)
		return ok || !bounded
	}
	if bounded && len(e.events) >= MaxQueuedEvents {
		return false
	}
	e.events = append(e.events, ev)
	return true
}

// pushMessage отдаёт сообщение потока по умолчанию; не принято — сессия
// закрывается (slow_consumer): молча терять нельзя.
func (e *Executor) pushMessage(s *session, msg []byte) bool {
	ok := e.pushIfAlive(s, Event{Type: EventMessage, SessionID: s.id, Payload: msg}, true)
	if !ok {
		e.finish(s, ReasonSlowConsumer)
	}
	return ok
}

// finish закрывает сессию; первая причина побеждает. Событие P2P_CLOSED
// отправляет run, выходя (emitClosed), — после всех сообщений сессии.
func (e *Executor) finish(s *session, reason string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.reason = reason
	s.mu.Unlock()
	s.cancel()
	_ = s.sess.Close()
	closeAdapter(s.adapter, s.shared)
	e.mu.Lock()
	delete(e.sessions, s.id)
	e.dropStreamsLocked(s)
	e.mu.Unlock()
}

// emitClosed — единственное P2P_CLOSED сессии, сверх предела очереди: их не
// больше, чем сессий.
func (e *Executor) emitClosed(s *session) {
	e.finish(s, ReasonFailed) // no-op, если причина уже есть
	s.mu.Lock()
	reason := s.reason
	s.mu.Unlock()
	e.mu.Lock()
	e.deliverLocked(Event{Type: EventClosed, SessionID: s.id, Reason: reason, Owner: s.owner}, false)
	e.mu.Unlock()
}

// lookup — сессия своего владельца; чужая неотличима от несуществующей.
func (e *Executor) lookup(owner, id string) (*session, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sessions[id]
	if !ok || s.owner != owner {
		return nil, false
	}
	return s, true
}

// Send отправляет сообщение по сессии хоста приложения.
func (e *Executor) Send(id string, payload []byte) error { return e.SendAs("", id, payload) }

// SendAs отправляет сообщение по установленной сессии владельца.
func (e *Executor) SendAs(owner, id string, payload []byte) error {
	if len(payload) > e.cfg.PayloadLimit {
		return ErrPayloadTooBig
	}
	s, ok := e.lookup(owner, id)
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

// Await ждёт установления сессии владельца либо её отказа. ErrNotFound —
// нет такой своей сессии; ErrNotReady — установление провалилось.
func (e *Executor) Await(ctx context.Context, owner, id string) error {
	s, ok := e.lookup(owner, id)
	if !ok {
		return ErrNotFound
	}
	select {
	case <-s.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.conv == nil {
		return ErrNotReady
	}
	return nil
}

// Peer — подтверждённый handshake'ом ключ собеседника (ADR-041) или nil,
// пока сессия не установлена.
func (e *Executor) Peer(owner, id string) (ports.PublicKey, error) {
	s, ok := e.lookup(owner, id)
	if !ok {
		return nil, ErrNotFound
	}
	select {
	case <-s.ready:
	default:
		return nil, ErrNotReady
	}
	if s.conv == nil {
		return nil, ErrNotReady
	}
	return s.sess.Peer(), nil
}

// Close закрывает сессию либо незавершённое согласование хоста приложения.
func (e *Executor) Close(id string) error {
	if s, ok := e.lookup("", id); ok {
		e.finish(s, ReasonLocal)
		return nil
	}
	e.mu.Lock()
	n, isNeg := e.negotiations[id]
	e.mu.Unlock()
	if isNeg && n.owner == "" {
		e.dropNegotiation(id)
		return nil
	}
	return ErrNotFound
}

// CloseSession закрывает только сессию владельца (не согласование).
func (e *Executor) CloseSession(owner, id string) error {
	s, ok := e.lookup(owner, id)
	if !ok {
		return ErrNotFound
	}
	e.finish(s, ReasonLocal)
	return nil
}

// CloseOwner закрывает все сессии владельца с причиной reason и снимает
// его согласования — конец жизненного цикла потребителя (ADR-076 §3.8).
func (e *Executor) CloseOwner(owner, reason string) {
	e.mu.Lock()
	var sessions []*session
	for _, s := range e.sessions {
		if s.owner == owner {
			sessions = append(sessions, s)
		}
	}
	var negs []string
	for id, n := range e.negotiations {
		if n.owner == owner {
			negs = append(negs, id)
		}
	}
	e.mu.Unlock()
	for _, s := range sessions {
		e.finish(s, reason)
	}
	for _, id := range negs {
		e.dropNegotiation(id)
	}
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
