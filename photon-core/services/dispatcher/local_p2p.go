package dispatcher

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	edadapter "github.com/wiloris/photon-core/adapters/ed25519"
	"github.com/wiloris/photon-core/ports"
	photonpb "github.com/wiloris/photon-core/protobuf"
	"github.com/wiloris/photon-core/services/p2phost"
	"github.com/wiloris/photon-core/services/session"
)

// Действия 860–865: P2P-исполнитель хоста (ADR-116 §3); 866–868 —
// служебные потоки сессии (проект записи к ADR-075 §2.3, не принят).
// Контракт — photon-core/docs/local-actions.md.

const (
	actionP2POffer    = int32(photonpb.Action_ACTION_P2P_OFFER)
	actionP2PAccept   = int32(photonpb.Action_ACTION_P2P_ACCEPT)
	actionP2PComplete = int32(photonpb.Action_ACTION_P2P_COMPLETE)
	actionP2PSend     = int32(photonpb.Action_ACTION_P2P_SEND)
	actionP2PClose    = int32(photonpb.Action_ACTION_P2P_CLOSE)
	actionP2PEvents   = int32(photonpb.Action_ACTION_P2P_EVENTS)

	actionP2PStreamOpen  = int32(photonpb.Action_ACTION_P2P_STREAM_OPEN)
	actionP2PStreamSend  = int32(photonpb.Action_ACTION_P2P_STREAM_SEND)
	actionP2PStreamClose = int32(photonpb.Action_ACTION_P2P_STREAM_CLOSE)
)

// Вердикты P2P-действий (ADR-038: исход вызова — PHOTON_OK, вердикт — в
// конверте). Ошибка вызова — только негодный payload.
const (
	p2pNotConfigured = "p2p_not_configured"
	p2pNotFound      = "not_found"
	p2pNotReady      = "not_ready"
	p2pTooMany       = "too_many_sessions"
	p2pTooLarge      = "payload_too_large"
	p2pFailed        = "negotiation_failed"

	p2pPeerUnsupported = "peer_unsupported"
	p2pStreamRefused   = "stream_refused"
	p2pTooManyStreams  = "too_many_streams"
	p2pInvalidService  = "invalid_service"
	// p2pStreamFailed — поток отказал при отправке или открытии; сессия
	// при этом может жить (у 863 тот же исход называется negotiation_failed
	// — прежний вердикт не меняется).
	p2pStreamFailed = "failed"
)

// p2pStreamFailure — p2pFailure для действий потоков.
func p2pStreamFailure(err error) ([]byte, error) {
	if errors.Is(err, ports.ErrSessionFailed) || errors.Is(err, context.DeadlineExceeded) {
		return p2pVerdict(p2pStreamFailed)
	}
	return p2pFailure(err)
}

// newP2PExecutor читает существующий device.key и строит исполнитель.
func newP2PExecutor(cfg P2PConfig) (*p2phost.Executor, error) {
	priv, err := readDeviceKey(cfg.DeviceKeyPath)
	if err != nil {
		return nil, fmt.Errorf("dispatcher: p2p: %w", err)
	}
	id := edadapter.New()
	pub, err := id.PublicKeyOf(context.Background(), ports.PrivateKey(priv))
	if err != nil {
		return nil, fmt.Errorf("dispatcher: p2p: device key: %w", err)
	}
	return p2phost.New(p2phost.Config{
		Identity:         id,
		Self:             ports.IdentityPair{Public: pub, Private: ports.PrivateKey(priv)},
		PeerLossTimeout:  cfg.PeerLossTimeout,
		EstablishTimeout: cfg.EstablishTimeout,
	})
}

// readDeviceKey — тот же формат и те же требования, что у хоста плагинов
// (ADR-086): base64, права не шире 0600. Отсутствующий файл — отказ:
// идентичность устройства создаёт ENGRAM при инициализации, не Core.
func readDeviceKey(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return nil, fmt.Errorf("%s доступен группе или остальным (%o)", filepath.Base(path), perm)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("%s не разбирается как base64: %w", filepath.Base(path), err)
	}
	return key, nil
}

type p2pTURN = p2phost.TURN

type p2pOfferReq struct {
	PeerPublicKey []byte   `json:"peer_public_key"`
	TURN          *p2pTURN `json:"turn"`
}

type p2pAcceptReq struct {
	PeerPublicKey []byte   `json:"peer_public_key"`
	Offer         []byte   `json:"offer"`
	TURN          *p2pTURN `json:"turn"`
}

type p2pCompleteReq struct {
	NegotiationID string `json:"negotiation_id"`
	Answer        []byte `json:"answer"`
}

type p2pSessionReq struct {
	SessionID string `json:"session_id"`
	Payload   []byte `json:"payload"`
}

type p2pStreamOpenReq struct {
	SessionID string `json:"session_id"`
	Service   string `json:"service"`
}

type p2pStreamReq struct {
	StreamID string `json:"stream_id"`
	Payload  []byte `json:"payload"`
}

type p2pEventsReq struct {
	Max int `json:"max"`
}

func p2pVerdict(code string) ([]byte, error) {
	return envelope(session.Response{OK: false, Error: code})
}

// p2pFailure переводит ошибку исполнителя в вердикт. Отказ согласования
// (негодный блоб, сбой ICE при сборе кандидатов) — тоже вердикт: вызов
// исполнен, операция не удалась.
func p2pFailure(err error) ([]byte, error) {
	switch {
	case errors.Is(err, p2phost.ErrNotFound):
		return p2pVerdict(p2pNotFound)
	case errors.Is(err, p2phost.ErrNotReady):
		return p2pVerdict(p2pNotReady)
	case errors.Is(err, p2phost.ErrTooMany):
		return p2pVerdict(p2pTooMany)
	case errors.Is(err, p2phost.ErrPayloadTooBig):
		return p2pVerdict(p2pTooLarge)
	case errors.Is(err, p2phost.ErrPeerUnsupported):
		return p2pVerdict(p2pPeerUnsupported)
	case errors.Is(err, p2phost.ErrStreamRefused):
		return p2pVerdict(p2pStreamRefused)
	case errors.Is(err, p2phost.ErrTooManyStreams):
		return p2pVerdict(p2pTooManyStreams)
	case errors.Is(err, p2phost.ErrInvalidService):
		return p2pVerdict(p2pInvalidService)
	case errors.Is(err, p2phost.ErrBadArgument):
		return nil, fmt.Errorf("%w: %v", session.ErrInvalidPayload, err)
	}
	return p2pVerdict(p2pFailed)
}

func decodeP2P(payload []byte, v any) error {
	if err := json.Unmarshal(payload, v); err != nil {
		return fmt.Errorf("%w: %v", session.ErrInvalidPayload, err)
	}
	return nil
}

// p2pAction исполняет 860–868. Ключи, блобы и payload — base64 в JSON
// ([]byte кодируется encoding/json именно так).
func (d *Dispatcher) p2pAction(ctx context.Context, action int32, payload []byte) ([]byte, error) {
	exec := d.p2p
	if exec == nil {
		return p2pVerdict(p2pNotConfigured)
	}
	switch action {
	case actionP2POffer:
		var r p2pOfferReq
		if err := decodeP2P(payload, &r); err != nil {
			return nil, err
		}
		id, offer, err := exec.Offer(ctx, ports.PublicKey(r.PeerPublicKey), r.TURN)
		if err != nil {
			return p2pFailure(err)
		}
		return okEnvelope(struct {
			NegotiationID string `json:"negotiation_id"`
			Offer         []byte `json:"offer"`
		}{id, offer})
	case actionP2PAccept:
		var r p2pAcceptReq
		if err := decodeP2P(payload, &r); err != nil {
			return nil, err
		}
		id, answer, err := exec.Accept(ctx, ports.PublicKey(r.PeerPublicKey), r.Offer, r.TURN)
		if err != nil {
			return p2pFailure(err)
		}
		return okEnvelope(struct {
			SessionID string `json:"session_id"`
			Answer    []byte `json:"answer"`
		}{id, answer})
	case actionP2PComplete:
		var r p2pCompleteReq
		if err := decodeP2P(payload, &r); err != nil {
			return nil, err
		}
		id, err := exec.Complete(ctx, r.NegotiationID, r.Answer)
		if err != nil {
			return p2pFailure(err)
		}
		return okEnvelope(struct {
			SessionID string `json:"session_id"`
		}{id})
	case actionP2PSend:
		var r p2pSessionReq
		if err := decodeP2P(payload, &r); err != nil {
			return nil, err
		}
		if err := exec.Send(r.SessionID, r.Payload); err != nil {
			return p2pFailure(err)
		}
		return okEnvelope(struct{}{})
	case actionP2PClose:
		var r p2pSessionReq
		if err := decodeP2P(payload, &r); err != nil {
			return nil, err
		}
		if err := exec.Close(r.SessionID); err != nil {
			return p2pFailure(err)
		}
		return okEnvelope(struct{}{})
	case actionP2PEvents:
		var r p2pEventsReq
		if len(payload) > 0 {
			if err := decodeP2P(payload, &r); err != nil {
				return nil, err
			}
		}
		evs := exec.Poll(r.Max)
		if evs == nil {
			evs = []p2phost.Event{}
		}
		return okEnvelope(struct {
			Events []p2phost.Event `json:"events"`
		}{evs})
	case actionP2PStreamOpen:
		var r p2pStreamOpenReq
		if err := decodeP2P(payload, &r); err != nil {
			return nil, err
		}
		id, err := exec.OpenStream(ctx, r.SessionID, r.Service)
		if err != nil {
			return p2pStreamFailure(err)
		}
		return okEnvelope(struct {
			StreamID string `json:"stream_id"`
		}{id})
	case actionP2PStreamSend:
		var r p2pStreamReq
		if err := decodeP2P(payload, &r); err != nil {
			return nil, err
		}
		if err := exec.StreamSend(r.StreamID, r.Payload); err != nil {
			return p2pStreamFailure(err)
		}
		return okEnvelope(struct{}{})
	case actionP2PStreamClose:
		var r p2pStreamReq
		if err := decodeP2P(payload, &r); err != nil {
			return nil, err
		}
		if err := exec.StreamClose(r.StreamID); err != nil {
			return p2pFailure(err)
		}
		return okEnvelope(struct{}{})
	}
	return nil, fmt.Errorf("%w: %d", session.ErrUnknownAction, action)
}
