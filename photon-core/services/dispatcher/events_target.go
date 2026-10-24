package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net"

	"github.com/wiloris/photon/photon-core/ports"
	photonpb "github.com/wiloris/photon/photon-core/protobuf"
	"github.com/wiloris/photon/photon-core/services/session"
)

// Цель подписки (ADR-123 §2.3; photon-quic-bridge/docs/conversation-push.md §4).
//
// Инстанс ENGRAM создаётся до входа и живёт весь процесс (ADR-094 §5.1), а
// сессия появляется, меняется и отзывается позже. Секция bridge в
// Photon_Create этого не выражает — её токен фиксируется при создании, —
// поэтому endpoint и токен подписки задаёт хост действием 814. Насос и
// подписка остаются единственными на инстанс (event-delivery.md §5); цель
// с Photon_Create — лишь начальное значение той же цели.
//
// Action-соединения 814 не создаёт: запросы хоста остаются его делом
// (ADR-094 §6.1).

const actionLocalEventsTarget = int32(photonpb.Action_ACTION_LOCAL_EVENTS_TARGET)

// subTarget — куда подписываться. Пустой endpoint — цели нет.
type subTarget struct {
	endpoint ports.Endpoint
	token    string
}

func (t subTarget) set() bool { return t.endpoint != "" }

// eventsTargetRequest — payload_json действия 814.
type eventsTargetRequest struct {
	Endpoint     string `json:"endpoint"`
	SessionToken string `json:"session_token"`
}

// eventsTarget — действие 814: задать, заменить или снять цель подписки.
//
// Оба поля или ни одного: endpoint без токена дал бы подписку, которую сервер
// отвергнет на каждом витке восстановления, а токен без адреса — ничего.
// Токен в ответ не попадает.
func (d *Dispatcher) eventsTarget(payload []byte) ([]byte, error) {
	var req eventsTargetRequest
	if err := decodeRequest(payload, &req); err != nil {
		return nil, err
	}
	switch {
	case req.Endpoint == "" && req.SessionToken == "":
		d.setTarget(subTarget{})
		return targetResponse("cleared")
	case req.Endpoint == "" || req.SessionToken == "":
		return nil, fmt.Errorf("%w: endpoint and session_token are set together", session.ErrInvalidPayload)
	}
	if _, _, err := net.SplitHostPort(req.Endpoint); err != nil {
		return nil, fmt.Errorf("%w: endpoint must be host:port", session.ErrInvalidPayload)
	}
	if err := d.setTarget(subTarget{endpoint: ports.Endpoint(req.Endpoint), token: req.SessionToken}); err != nil {
		return nil, err
	}
	return targetResponse("set")
}

func targetResponse(state string) ([]byte, error) {
	data, err := json.Marshal(struct {
		Target string `json:"target"`
	}{Target: state})
	if err != nil {
		return nil, fmt.Errorf("dispatcher: encode target result: %w", err)
	}
	return envelope(session.Response{OK: true, Data: data})
}

// setTarget меняет цель под d.mu.
//
// Текущая подписка и её пауза восстановления прерываются отменой контекста
// поколения: насос сразу подключается к новой цели без паузы. Ждать, пока
// насос это сделает, setTarget не может: 814 вправе прийти из колбэка
// события, то есть с потока самого насоса. Поэтому одно событие, уже
// принятое старой подпиской, может быть доставлено и после возврата —
// событие разговора лишь повод перечитать состояние (ADR-123 §2.4).
func (d *Dispatcher) setTarget(t subTarget) error {
	// Медиасоединение принадлежит прежней цели: закрывается после снятия
	// блокировки (defer выполняется после Unlock ниже).
	var media *session.Session
	defer func() {
		if media != nil {
			_ = media.Close()
		}
	}()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	if t.set() && d.subTr == nil && d.tr == nil {
		// Транспорт создаётся лениво и сети не касается; закрывается в Close
		// после выхода насоса.
		if d.cfg.Transport != nil {
			d.subTr = d.cfg.Transport
		} else {
			d.subTr, d.ownSubTr = newQUICTransport(d.cfg), true
		}
	}
	d.target = t
	d.targetGen++
	media, d.mediaSess = d.mediaSess, nil
	if d.subCancel != nil {
		d.subCancel()
		d.subCancel = nil
	}
	select {
	case d.targetWake <- struct{}{}:
	default:
	}
	if t.set() && !d.pumpStarted && d.sink != nil {
		d.startPumpLocked(d.sink, d.onStop)
	}
	return nil
}

// nextTarget ждёт цель и открывает для неё поколение — контекст, который
// отменит следующая смена цели. ok=false — инстанс закрывается.
func (d *Dispatcher) nextTarget() (t subTarget, gen uint64, gctx context.Context, ok bool) {
	for {
		d.mu.Lock()
		if d.ctx.Err() != nil {
			d.mu.Unlock()
			return subTarget{}, 0, nil, false
		}
		if d.target.set() {
			if d.subCancel != nil {
				d.subCancel()
			}
			c, cancel := context.WithCancel(d.ctx)
			d.subCancel = cancel
			t, gen = d.target, d.targetGen
			d.mu.Unlock()
			return t, gen, c, true
		}
		wake := d.targetWake
		d.mu.Unlock()
		select {
		case <-wake:
		case <-d.ctx.Done():
			return subTarget{}, 0, nil, false
		}
	}
}

// subTransport — транспорт подписки: общий с action, если он есть.
func (d *Dispatcher) subTransport() ports.Transport {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tr != nil {
		return d.tr
	}
	return d.subTr
}
