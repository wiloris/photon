package coreops

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/wiloris/photon-core/p2pmsg"
	"github.com/wiloris/photon-core/services/p2phost"
	"github.com/wiloris/photon-plugin-runtime/ipc"
)

// Исполнитель домена p2p.* (ADR-076, условие 5 из шести).
//
// Сессии и согласования ведёт единый P2P-исполнитель Core (services/p2phost)
// — тот же, что у хоста приложения (ADR-116); собственного реестра здесь
// больше нет. Отсюда — только граница ADR-076: владелец = PluginID, формы
// запросов и событий, коды ошибок IPC, срок ожидания установления §7.7.
//
// Провод не меняется: исполнитель собран без кадра-«привета» и без
// служебных потоков (NoHello, NoStreams) — ровно так, как coreops говорил до
// переноса. Плагину не проходит ни один тип Photon (ADR-076 §3.2).

// Причины закрытия — закрытый перечень ADR-076 §3.4. Свободного текста нет:
// решения плагин принимает по этому значению. local, remote, failed и
// slow_consumer приходят из исполнителя; plugin_stopped задаёт runtime.
const (
	reasonLocal         = p2phost.ReasonLocal
	reasonRemote        = p2phost.ReasonRemote
	reasonFailed        = p2phost.ReasonFailed
	reasonPluginStopped = "plugin_stopped"
	reasonSlowConsumer  = p2phost.ReasonSlowConsumer
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

// newP2PExecutor собирает единый исполнитель под контракт ADR-076.
func newP2PExecutor(cfg Config) (*p2phost.Executor, error) {
	emit := cfg.EmitEvent
	return p2phost.New(p2phost.Config{
		Adapter:          cfg.P2P, // адаптером владеет композиция
		EstablishTimeout: cfg.P2PEstablishTimeout,
		NoHello:          true,
		NoStreams:        true,
		// ADR-076 §7, решение 8: предел ADR-075 (1 MiB) сохраняется.
		PayloadLimit: p2pmsg.MaxMessageSize,
		// ADR-076 §7, решение 9: лимит сессий не вводится.
		SessionLimit: -1,
		// Очередь событий IPC отбрасывает при переполнении; отказ на
		// сообщении исполнитель превращает в slow_consumer (ADR-076 §3.11).
		// Отправка p2p.closed может не удаться (переполнение, плагин уже
		// остановлен) — это не повод удерживать ресурсы: плагин узнает о
		// состоянии по NOT_FOUND следующей операции.
		Sink: func(pluginID string, ev p2phost.Event) bool {
			var typ string
			var v any
			switch ev.Type {
			case p2phost.EventMessage:
				typ, v = eventMessage, p2pMessageEvent{SessionID: ev.SessionID, Payload: ev.Payload}
			case p2phost.EventClosed:
				typ, v = eventClosed, p2pClosedEvent{SessionID: ev.SessionID, Reason: ev.Reason}
			default:
				// p2p.established нет (ADR-076 §7, решение 1): о готовности
				// плагин узнаёт из p2p.peer.
				return true
			}
			payload, err := json.Marshal(v)
			if err != nil {
				return false
			}
			return emit(pluginID, typ, payload) == nil
		},
	})
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

// errNotFound — единственный ответ на чужой, неизвестный и закрытый handle
// (ADR-076 §3.7).
func errNotFound() error {
	return ipc.NewOpError(ipc.ErrNotFound, "no such session")
}

func (h *Handler) p2pAvailable() bool { return h.p2p != nil }

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

// awaitEstablished ждёт установления своей сессии: post-establishment
// query ADR-076 §3.9 — блокирует до established либо терминального исхода.
func (h *Handler) awaitEstablished(ctx context.Context, pluginID, sessionID string) error {
	wctx, cancel := h.establishDeadline(ctx)
	defer cancel()
	err := h.p2p.Await(wctx, pluginID, sessionID)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, p2phost.ErrNotFound):
		return errNotFound()
	default:
		return ipc.NewOpError(ipc.ErrInternal, "session is not established")
	}
}

// ClosePluginSessions закрывает все сессии плагина и снимает его
// согласования.
//
// Вызывается композицией при завершении жизненного цикла плагина —
// штатная остановка, аварийное завершение, крах процесса (ADR-076 §3.8).
// Сессия, пережившая плагина, была бы утечкой сокетов и ICE-агентов, а
// собеседник ждал бы ответа от того, кого уже нет. p2p.closed с
// plugin_stopped уходит, если канал событий ещё жив.
func (h *Handler) ClosePluginSessions(pluginID string) {
	if h.p2p == nil {
		return
	}
	h.p2p.CloseOwner(pluginID, reasonPluginStopped)
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
	// Контекст согласования исполнитель держит сам, не привязывая его к
	// запросу: между create_offer и complete стоит чужой канал доставки
	// (ADR-042) — ловушка ADR-044 §6.1.
	id, offer, err := h.p2p.OfferAs(ctx, caller.PluginID, req.PeerPubKey, nil)
	if err != nil {
		return nil, ipc.NewOpError(ipc.ErrInternal, "create offer failed")
	}
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
	id, answer, err := h.p2p.AcceptAs(ctx, caller.PluginID, req.PeerPubKey, req.Offer, nil)
	if err != nil {
		return nil, ipc.NewOpError(ipc.ErrInvalidRequest, "offer rejected")
	}
	return json.Marshal(p2pAcceptOfferResponse{SessionID: id, Answer: answer})
}

func (h *Handler) p2pComplete(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	if !h.p2pAvailable() {
		return nil, errP2PUnsupported
	}
	var req p2pCompleteRequest
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	id, err := h.p2p.CompleteAs(ctx, caller.PluginID, req.NegotiationID, req.Answer)
	switch {
	case err == nil:
		return json.Marshal(p2pCompleteResponse{SessionID: id})
	case errors.Is(err, p2phost.ErrNotFound):
		// Чужое и неизвестное согласование неотличимы — то же правило,
		// что для сессий (ADR-076 §7.2). Повторный complete попадает
		// сюда же: согласование потребляется ровно один раз.
		return nil, ipc.NewOpError(ipc.ErrNotFound, "no such negotiation")
	default:
		return nil, ipc.NewOpError(ipc.ErrInvalidRequest, "answer rejected")
	}
}

func (h *Handler) p2pPeer(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	if !h.p2pAvailable() {
		return nil, errP2PUnsupported
	}
	var req p2pSessionRequest
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	if err := h.awaitEstablished(ctx, caller.PluginID, req.SessionID); err != nil {
		return nil, err
	}
	peer, err := h.p2p.Peer(caller.PluginID, req.SessionID)
	if errors.Is(err, p2phost.ErrNotFound) {
		return nil, errNotFound()
	}
	if err != nil || peer == nil {
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
	if err := h.awaitEstablished(ctx, caller.PluginID, req.SessionID); err != nil {
		return nil, err
	}
	// Сериализация по сессии — в исполнителе (ADR-076 §3.10): порядок кадров
	// определяется моментом ПРИНЯТИЯ, а не порядком вызовов плагина.
	err := h.p2p.SendAs(caller.PluginID, req.SessionID, req.Payload)
	switch {
	case err == nil:
		return []byte("{}"), nil
	case errors.Is(err, p2phost.ErrPayloadTooBig):
		return nil, ipc.NewOpError(ipc.ErrInvalidRequest, "payload exceeds p2p message limit")
	case errors.Is(err, p2phost.ErrNotFound):
		return nil, errNotFound()
	default:
		// Отказ записи терминален для разговора: исполнитель закрыл сессию,
		// p2p.closed придёт после всех её p2p.message.
		return nil, ipc.NewOpError(ipc.ErrInternal, "send failed")
	}
}

func (h *Handler) p2pClose(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	if !h.p2pAvailable() {
		return nil, errP2PUnsupported
	}
	var req p2pSessionRequest
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	if err := h.p2p.CloseSession(caller.PluginID, req.SessionID); err != nil {
		return nil, errNotFound()
	}
	return []byte("{}"), nil
}
