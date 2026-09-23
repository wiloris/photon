package coreops

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/wiloris/photon-core/p2pmsg"
	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/ipc"
)

// Исполнитель домена p2p.* (ADR-076, условие 5 из шести).
//
// Плагину не проходит наружу ни один тип Photon: ни ports.Session, ни
// ports.Stream, ни типы Pion. Только непрозрачные строки handle и
// непрозрачные байты нагрузки (ADR-076 §3.2).
//
// ports.P2P и p2pmsg используются КАК ЕСТЬ: ни один из них этим файлом не
// меняется (ADR-020, ADR-075).

// Причины закрытия — закрытый перечень ADR-076 §3.4. Свободного текста нет:
// решения плагин принимает по этому значению.
const (
	reasonLocal         = "local"
	reasonRemote        = "remote"
	reasonFailed        = "failed"
	reasonPluginStopped = "plugin_stopped"
	reasonSlowConsumer  = "slow_consumer"
)

const (
	eventMessage = "p2p.message"
	eventClosed  = "p2p.closed"
)

// defaultP2PEstablishTimeout — потолок ожидания установления на стороне
// coreops.
//
// Строго меньше ipc.DefaultRequestTimeout — инвариант ADR-076 §7.7. Смысл
// инварианта: ожидание обязано кончиться ПРИЧИНОЙ СО СТОРОНЫ P2P, а не
// дедлайном IPC, иначе плагин не отличит «собеседник не дошёл до
// established» от «истёк срок запроса» (то же, что ловил Phase 13.5).
const defaultP2PEstablishTimeout = 20 * time.Second

// establishMargin — запас, на который ожидание установления заканчивается
// РАНЬШЕ дедлайна запроса.
//
// Потолка из конфигурации мало: RequestTimeout задаёт композиция, и он может
// оказаться меньше потолка. Поэтому фактический срок выводится из контекста
// запроса, который дедлайн IPC уже несёт (ipc/session.go), минус этот запас.
// Тогда инвариант держится при ЛЮБОМ RequestTimeout, а не только при
// умолчании.
const establishMargin = 500 * time.Millisecond

type p2pRole int

const (
	roleInitiator p2pRole = iota // create_offer → complete: поток открывает
	roleResponder                // accept_offer: поток принимает
)

// p2pSession — запись реестра. Владелец — runtime (ADR-076 §3.8).
type p2pSession struct {
	id       string
	pluginID string
	role     p2pRole
	sess     ports.Session

	// ctx живёт ДОЛЬШЕ любого запроса и отменяется только закрытием.
	//
	// Ловушка ADR-044 §6.1 обезврежена именно здесь: согласование
	// продолжается после возврата из вызова, и привязка его к контексту
	// одного IPC-запроса убивала бы сессию сразу после ответа плагину.
	ctx    context.Context
	cancel context.CancelFunc

	// ready закрывается, когда conv готов либо установление провалилось.
	// Закрытие канала — точка синхронизации для conv и convErr.
	ready   chan struct{}
	conv    *p2pmsg.Conversation
	convErr error

	// sendMu сериализует p2p.send (ADR-076 §3.10).
	//
	// p2pmsg.Conversation не потокобезопасен для параллельных Send, а IPC
	// допускает десятки запросов в полёте. Без этого мьютекса два
	// одновременных send перемешали бы байты двух кадров — молчаливая
	// порча, худший вид отказа.
	sendMu sync.Mutex

	mu     sync.Mutex
	reason string // причина закрытия; первая победила
	closed bool
}

// negotiation — начатое create_offer и ещё не завершённое согласование.
type negotiation struct {
	pluginID string
	peer     ports.PublicKey
	ctx      context.Context
	cancel   context.CancelFunc
}

// p2pRegistry — реестр runtime. Состояние живёт здесь, у плагина только
// строка (ADR-076 §3.8).
type p2pRegistry struct {
	mu           sync.Mutex
	sessions     map[string]*p2pSession
	negotiations map[string]*negotiation
}

func newP2PRegistry() *p2pRegistry {
	return &p2pRegistry{
		sessions:     make(map[string]*p2pSession),
		negotiations: make(map[string]*negotiation),
	}
}

// lookup возвращает сессию ТОЛЬКО владельцу.
//
// Чужой, неизвестный и уже закрытый handle неотличимы, и это решение
// ADR-076 §7.2: FORBIDDEN подтверждал бы существование сессии, то есть давал
// бы перебором оракул о соседях. Цена названа там же — плагин не отличает
// свою закрытую сессию от чужой живой.
func (r *p2pRegistry) lookup(pluginID, id string) (*p2pSession, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ps, ok := r.sessions[id]
	if !ok || ps.pluginID != pluginID {
		return nil, false
	}
	return ps, true
}

func (r *p2pRegistry) put(ps *p2pSession) {
	r.mu.Lock()
	r.sessions[ps.id] = ps
	r.mu.Unlock()
}

func (r *p2pRegistry) drop(id string) {
	r.mu.Lock()
	delete(r.sessions, id)
	r.mu.Unlock()
}

// takeNegotiation изымает согласование: потребляется РОВНО ОДИН РАЗ.
// Повторный complete — ошибка, а не вторая сессия и не та же самая
// (ADR-044 §6, ADR-076 §3.6).
func (r *p2pRegistry) takeNegotiation(pluginID, id string) (*negotiation, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.negotiations[id]
	if !ok || n.pluginID != pluginID {
		return nil, false
	}
	delete(r.negotiations, id)
	return n, true
}

func (r *p2pRegistry) putNegotiation(id string, n *negotiation) {
	r.mu.Lock()
	r.negotiations[id] = n
	r.mu.Unlock()
}

// sessionsOf — все сессии плагина; используется при остановке.
func (r *p2pRegistry) sessionsOf(pluginID string) []*p2pSession {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*p2pSession
	for _, ps := range r.sessions {
		if ps.pluginID == pluginID {
			out = append(out, ps)
		}
	}
	return out
}

// ── handle ──────────────────────────────────────────────────────────

func newHandle() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// ── формы запросов и ответов (sdk-boundary.md §3) ────────────────────

type p2pCreateOfferRequest struct {
	PeerPubKey []byte `json:"peer_pubkey"`
}
type p2pCreateOfferResponse struct {
	NegotiationID string `json:"negotiation_id"`
	Offer         []byte `json:"offer"`
}
type p2pAcceptOfferRequest struct {
	PeerPubKey []byte `json:"peer_pubkey"`
	Offer      []byte `json:"offer"`
}
type p2pAcceptOfferResponse struct {
	SessionID string `json:"session_id"`
	Answer    []byte `json:"answer"`
}
type p2pCompleteRequest struct {
	NegotiationID string `json:"negotiation_id"`
	Answer        []byte `json:"answer"`
}
type p2pCompleteResponse struct {
	SessionID string `json:"session_id"`
}
type p2pSessionRequest struct {
	SessionID string `json:"session_id"`
}
type p2pPeerResponse struct {
	PeerPubKey []byte `json:"peer_pubkey"`
}
type p2pSendRequest struct {
	SessionID string `json:"session_id"`
	Payload   []byte `json:"payload"`
}
type p2pMessageEvent struct {
	SessionID string `json:"session_id"`
	Payload   []byte `json:"payload"`
}
type p2pClosedEvent struct {
	SessionID string `json:"session_id"`
	Reason    string `json:"reason"`
}

// ── общее ───────────────────────────────────────────────────────────

var errP2PUnsupported = ipc.NewOpError(ipc.ErrUnsupported, "p2p is not available in this composition")

// errNotFound — единственный ответ на чужой, неизвестный и закрытый handle.
func errNotFound() error {
	return ipc.NewOpError(ipc.ErrNotFound, "no such session")
}

func (h *Handler) p2pAvailable() bool { return h.cfg.P2P != nil }

// establishDeadline — срок ожидания установления для ОДНОГО запроса.
//
// Инвариант ADR-076 §7.7 обеспечивается здесь и не зависит от того, что
// композиция задала адаптеру: срок выводится из дедлайна запроса минус
// запас, и дополнительно ограничен потолком конфигурации.
func (h *Handler) establishDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	cap := h.cfg.P2PEstablishTimeout
	if cap <= 0 {
		cap = defaultP2PEstablishTimeout
	}
	if d, ok := ctx.Deadline(); ok {
		if room := time.Until(d) - establishMargin; room < cap {
			cap = room
		}
	}
	if cap <= 0 {
		cap = time.Millisecond // срок уже истёк: ответить сразу, не ждать
	}
	return context.WithTimeout(ctx, cap)
}

// awaitConversation ждёт готовности разговора.
//
// Это же — ожидание установления: поток открывается только после
// established (ADR-044 §1), поэтому отдельного признака готовности не нужно,
// и события p2p.established не существует (ADR-076 §7.1).
func (ps *p2pSession) awaitConversation(ctx context.Context) (*p2pmsg.Conversation, error) {
	select {
	case <-ps.ready:
		if ps.convErr != nil {
			return nil, ps.convErr
		}
		return ps.conv, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// markClosing фиксирует причину закрытия. Первая побеждает: причина, по
// которой сессия закрылась, не должна подменяться следствием.
func (ps *p2pSession) markClosing(reason string) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.closed {
		return false
	}
	ps.closed = true
	ps.reason = reason
	return true
}

func (ps *p2pSession) closingReason() string {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.reason
}

func (ps *p2pSession) isClosed() bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.closed
}

// closeSession снимает сессию с учёта и рвёт канал.
//
// Событие p2p.closed отсюда НЕ отправляется: единственный отправитель
// событий сессии — её насос (см. pump). Так гарантируется порядок ADR-076
// §3.10: все p2p.message приходят раньше p2p.closed этой же сессии.
func (h *Handler) closeSession(ps *p2pSession, reason string) {
	if !ps.markClosing(reason) {
		return
	}
	h.p2p.drop(ps.id)
	_ = ps.sess.Close()
	ps.cancel()
}

func (h *Handler) emit(pluginID, eventType string, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return h.cfg.EmitEvent(pluginID, eventType, payload)
}

// pump — единственный отправитель событий сессии.
//
// Сначала дожидается потока (то есть установления), затем превращает каждый
// принятый кадр в p2p.message. Выходя, отправляет p2p.closed — поэтому
// порядок «все сообщения раньше закрытия» держится по построению, а не по
// договорённости.
func (h *Handler) pump(ps *p2pSession) {
	var st ports.Stream
	var err error
	if ps.role == roleInitiator {
		st, err = ps.sess.OpenStream(ps.ctx)
	} else {
		st, err = ps.sess.AcceptStream(ps.ctx)
	}
	if err != nil {
		ps.convErr = fmt.Errorf("p2p: establishment failed: %w", err)
		close(ps.ready)
		h.finish(ps, reasonFailed)
		return
	}
	ps.conv = p2pmsg.New(st)
	close(ps.ready)

	for {
		msg, rerr := ps.conv.Recv()
		if rerr != nil {
			// io.EOF — собеседник закрыл сессию либо завершил отправку;
			// на уровне потока они неразличимы, и это измерено
			// (ADR-075 §6.1), а не предположено.
			reason := reasonFailed
			if errors.Is(rerr, io.EOF) {
				reason = reasonRemote
			}
			h.finish(ps, reason)
			return
		}
		if eerr := h.emit(ps.pluginID, eventMessage, p2pMessageEvent{
			SessionID: ps.id, Payload: msg,
		}); eerr != nil {
			// Очередь событий отбрасывает при переполнении
			// (ipc/session.go, threat model T9). Молчаливая потеря
			// сообщения неотличима от неотправленного и обнаруживается
			// только сверкой, которой в этом контракте нет, — поэтому
			// теряется СЕССИЯ, и громко (ADR-076 §7.3).
			h.finish(ps, reasonSlowConsumer)
			return
		}
	}
}

// finish закрывает сессию и отправляет единственное p2p.closed.
func (h *Handler) finish(ps *p2pSession, reason string) {
	h.closeSession(ps, reason) // причина уже могла быть выставлена закрывающим
	// Отправка может не удаться — например, когда закрытие вызвано именно
	// переполнением очереди либо остановкой плагина. Это не повод
	// удерживать ресурсы: плагин узнает о состоянии по NOT_FOUND от
	// следующей операции.
	_ = h.emit(ps.pluginID, eventClosed, p2pClosedEvent{
		SessionID: ps.id, Reason: ps.closingReason(),
	})
}

// ClosePluginSessions закрывает все сессии плагина.
//
// Вызывается композицией при завершении жизненного цикла плагина —
// штатная остановка, аварийное завершение, крах процесса (ADR-076 §3.8).
// Сессия, пережившая плагина, была бы утечкой сокетов и ICE-агентов, а
// собеседник ждал бы ответа от того, кого уже нет.
func (h *Handler) ClosePluginSessions(pluginID string) {
	for _, ps := range h.p2p.sessionsOf(pluginID) {
		h.closeSession(ps, reasonPluginStopped)
	}
	h.p2p.mu.Lock()
	for id, n := range h.p2p.negotiations {
		if n.pluginID == pluginID {
			n.cancel()
			delete(h.p2p.negotiations, id)
		}
	}
	h.p2p.mu.Unlock()
}

// ── операции ────────────────────────────────────────────────────────

func (h *Handler) p2pCreateOffer(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	if !h.p2pAvailable() {
		return nil, errP2PUnsupported
	}
	var req p2pCreateOfferRequest
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	if len(req.PeerPubKey) == 0 {
		return nil, ipc.NewOpError(ipc.ErrInvalidRequest, "peer_pubkey is required")
	}

	// Контекст согласования НЕ привязан к запросу: между create_offer и
	// complete стоит чужой канал доставки (ADR-042), и ответ может не
	// прийти никогда. Привязка к ctx запроса убила бы согласование сразу
	// после ответа плагину — ловушка ADR-044 §6.1.
	negCtx, cancel := context.WithTimeout(context.Background(), h.p2pCap())

	offer, err := h.cfg.P2P.CreateOffer(negCtx, ports.PublicKey(req.PeerPubKey))
	if err != nil {
		cancel()
		return nil, ipc.NewOpError(ipc.ErrInternal, "create offer failed")
	}
	id, err := newHandle()
	if err != nil {
		cancel()
		return nil, ipc.NewOpError(ipc.ErrInternal, "handle generation failed")
	}
	h.p2p.putNegotiation(id, &negotiation{
		pluginID: caller.PluginID,
		peer:     ports.PublicKey(req.PeerPubKey),
		ctx:      negCtx,
		cancel:   cancel,
	})
	return json.Marshal(p2pCreateOfferResponse{NegotiationID: id, Offer: offer})
}

func (h *Handler) p2pAcceptOffer(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	if !h.p2pAvailable() {
		return nil, errP2PUnsupported
	}
	var req p2pAcceptOfferRequest
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	if len(req.PeerPubKey) == 0 || len(req.Offer) == 0 {
		return nil, ipc.NewOpError(ipc.ErrInvalidRequest, "peer_pubkey and offer are required")
	}

	sessCtx, cancel := context.WithCancel(context.Background())
	answer, sess, err := h.cfg.P2P.AcceptOffer(sessCtx, ports.PublicKey(req.PeerPubKey), req.Offer)
	if err != nil {
		cancel()
		return nil, ipc.NewOpError(ipc.ErrInvalidRequest, "offer rejected")
	}
	ps, err := h.registerSession(caller.PluginID, sess, roleResponder, sessCtx, cancel)
	if err != nil {
		cancel()
		_ = sess.Close()
		return nil, err
	}
	return json.Marshal(p2pAcceptOfferResponse{SessionID: ps.id, Answer: answer})
}

func (h *Handler) p2pComplete(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	if !h.p2pAvailable() {
		return nil, errP2PUnsupported
	}
	var req p2pCompleteRequest
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	neg, ok := h.p2p.takeNegotiation(caller.PluginID, req.NegotiationID)
	if !ok {
		// Чужое и неизвестное согласование неотличимы — то же правило,
		// что для сессий (ADR-076 §7.2). Повторный complete попадает
		// сюда же: согласование потребляется ровно один раз.
		return nil, ipc.NewOpError(ipc.ErrNotFound, "no such negotiation")
	}
	sess, err := h.cfg.P2P.CompleteSession(neg.ctx, neg.peer, req.Answer)
	if err != nil {
		neg.cancel()
		return nil, ipc.NewOpError(ipc.ErrInvalidRequest, "answer rejected")
	}
	ps, err := h.registerSession(caller.PluginID, sess, roleInitiator, neg.ctx, neg.cancel)
	if err != nil {
		neg.cancel()
		_ = sess.Close()
		return nil, err
	}
	return json.Marshal(p2pCompleteResponse{SessionID: ps.id})
}

func (h *Handler) p2pPeer(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	if !h.p2pAvailable() {
		return nil, errP2PUnsupported
	}
	ps, err := h.sessionFromPayload(caller, payload)
	if err != nil {
		return nil, err
	}

	// post-establishment query: блокирует до established либо
	// терминального исхода (ADR-076 §7.1). Срок — §7.7.
	wctx, cancel := h.establishDeadline(ctx)
	defer cancel()
	if _, err := ps.awaitConversation(wctx); err != nil {
		return nil, ipc.NewOpError(ipc.ErrInternal, "session is not established")
	}
	peer := ps.sess.Peer()
	if peer == nil {
		return nil, ipc.NewOpError(ipc.ErrInternal, "peer is not confirmed")
	}
	return json.Marshal(p2pPeerResponse{PeerPubKey: peer})
}

func (h *Handler) p2pSend(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	if !h.p2pAvailable() {
		return nil, errP2PUnsupported
	}
	var req p2pSendRequest
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	ps, ok := h.p2p.lookup(caller.PluginID, req.SessionID)
	if !ok {
		return nil, errNotFound()
	}

	wctx, cancel := h.establishDeadline(ctx)
	defer cancel()
	conv, err := ps.awaitConversation(wctx)
	if err != nil {
		return nil, ipc.NewOpError(ipc.ErrInternal, "session is not established")
	}

	// Сериализация по сессии — ADR-076 §3.10. Порядок кадров определяется
	// моментом ПРИНЯТИЯ runtime, а не порядком вызовов плагина: при
	// конкурентных запросах второй порядок платформе неизвестен.
	ps.sendMu.Lock()
	serr := conv.Send(req.Payload)
	ps.sendMu.Unlock()
	if serr != nil {
		if errors.Is(serr, p2pmsg.ErrMessageTooLarge) {
			return nil, ipc.NewOpError(ipc.ErrInvalidRequest, "payload exceeds p2p message limit")
		}
		// Отказ записи терминален для разговора: сессию закрываем, а
		// p2p.closed отправит насос.
		h.closeSession(ps, reasonFailed)
		return nil, ipc.NewOpError(ipc.ErrInternal, "send failed")
	}
	return []byte("{}"), nil
}

func (h *Handler) p2pClose(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	if !h.p2pAvailable() {
		return nil, errP2PUnsupported
	}
	ps, err := h.sessionFromPayload(caller, payload)
	if err != nil {
		return nil, err
	}
	// Причина фиксируется здесь, событие отправит насос — единственный
	// отправитель событий сессии.
	h.closeSession(ps, reasonLocal)
	return []byte("{}"), nil
}

// ── вспомогательное ─────────────────────────────────────────────────

func (h *Handler) p2pCap() time.Duration {
	if h.cfg.P2PEstablishTimeout > 0 {
		return h.cfg.P2PEstablishTimeout
	}
	return defaultP2PEstablishTimeout
}

func (h *Handler) sessionFromPayload(caller ipc.CallerInfo, payload []byte) (*p2pSession, error) {
	var req p2pSessionRequest
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	ps, ok := h.p2p.lookup(caller.PluginID, req.SessionID)
	if !ok {
		return nil, errNotFound()
	}
	return ps, nil
}

func (h *Handler) registerSession(pluginID string, sess ports.Session, role p2pRole,
	ctx context.Context, cancel context.CancelFunc) (*p2pSession, error) {
	id, err := newHandle()
	if err != nil {
		return nil, ipc.NewOpError(ipc.ErrInternal, "handle generation failed")
	}
	ps := &p2pSession{
		id: id, pluginID: pluginID, role: role, sess: sess,
		ctx: ctx, cancel: cancel, ready: make(chan struct{}),
	}
	h.p2p.put(ps)
	go h.pump(ps)
	return ps, nil
}
