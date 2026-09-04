package main

// PushRouter — авторизация push, ADR-040.
//
// Требование:
//
//	audience(event) = { u ∈ subscribers : read_u(C_event) }
//
// Предикат тот же, что у запроса (ADR-039 §4): вопрос «кому можно видеть этот
// объект» одинаков для ответа на вызов и для события. Реализован ВАРИАНТ A —
// аудитория считается на каждое событие. Он заведомо корректен и не заводит
// собственного состояния о правах; кэш членства (вариант B) и push без тела
// (вариант C) выбираются только после измерения, см. критерий возврата в
// ADR-040 §2.
//
// Broadcast без адресата больше не используется: событие, для которого
// аудиторию вычислить не удалось, — дефект, а не повод разослать всем.
// Измерено, к чему приводило обратное: подписчик получал посты из чужого
// приватного канала и чужой личной переписки целиком.

// pushSink — куда уходит событие после того, как аудитория посчитана.
// Интерфейс, а не *PushRegistry, чтобы маршрутизацию можно было проверить без
// QUIC-соединений.
type pushSink interface {
	subscribers() []string
	sendTo(userID string, event PushEvent)
}

type PushRouter struct {
	plugin *Plugin
	authz  *authz
	sink   pushSink
}

func NewPushRouter(p *Plugin, registry *PushRegistry) *PushRouter {
	return &PushRouter{plugin: p, authz: newAuthz(p.API), sink: registry}
}

func (r *PushRouter) Route(event PushEvent) {
	// Событие с известным адресатом (status_change): аудитория задана по
	// построению, канала у события нет.
	if event.UserID != "" {
		r.sink.sendTo(event.UserID, event)
		return
	}
	if event.ChannelID == "" {
		r.log("[Photon][Push] Событие без канала не рассылается", event)
		return
	}
	for _, userID := range r.sink.subscribers() {
		if r.authz.canReadChannel(userID, event.ChannelID) {
			r.sink.sendTo(userID, event)
		}
	}
}

func (r *PushRouter) log(msg string, event PushEvent) {
	if r.plugin == nil || r.plugin.API == nil {
		return
	}
	r.plugin.API.LogWarn(msg, "event", event.Event)
}

// canReadChannel — read(C) для произвольного получателя. Отдельный вход в тот
// же предикат: второе определение видимости разошлось бы с первым.
func (z *authz) canReadChannel(userID, channelID string) bool {
	ch, err := z.channelByID(channelID)
	if err != nil {
		return false
	}
	return readC()(z, userID, objectRef{channel: ch})
}
