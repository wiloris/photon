package main

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Push разговоров ENGRAM против НАСТОЯЩЕГО PostgreSQL (ADR-123 §2.2,
// docs/conversation-push.md §3, §6). PHOTON_DM_PG_DSN. Контрольная половина
// обязательна, как в push_router_test: «не получил» без «получил» неотличимо
// от выключенного push.

type pushRecorder struct {
	mu     sync.Mutex
	events []PushEvent
}

func (p *pushRecorder) record(e PushEvent) {
	p.mu.Lock()
	p.events = append(p.events, e)
	p.mu.Unlock()
}

// to — адресаты событий с данным поводом, по порядку возрастания.
func (p *pushRecorder) to(t *testing.T, convID, reason string) []string {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, e := range p.events {
		require.Equal(t, PushEventConversationActivity, e.Event)
		require.Empty(t, e.UserID, "user_id верхнего уровня не заполняется (ADR-081 §2.5)")
		require.Empty(t, e.ChannelID, "событие разговора не маршрутизируется по каналу")
		var d convActivityData
		require.NoError(t, json.Unmarshal(e.Data, &d))
		if d.ConversationID == convID && d.Reason == reason {
			out = append(out, e.To)
		}
	}
	sort.Strings(out)
	return out
}

func (p *pushRecorder) reset() {
	p.mu.Lock()
	p.events = nil
	p.mu.Unlock()
}

func withPush(r *groupRig) *pushRecorder {
	rec := &pushRecorder{}
	r.svc.push = rec.record
	r.svc.runPush = func(f func()) { f() }
	return rec
}

func sorted(ids ...string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

// Новое сообщение группы: событие каждому активному участнику, кроме автора;
// посторонний не получает ничего.
func TestConvPushMessageGoesToMembersExceptAuthor(t *testing.T) {
	r := newGroupRig(t, 2000)
	rec := withPush(r)
	a, b, c := r.id.person(pidA), r.id.person(pidB), r.id.person(pidC)
	outsider := r.id.person(pidD)
	id := r.group(t, a, pidB, pidC)
	rec.reset()

	r.mustSend(t, b, id, newRequestID(), "секрет")
	got := rec.to(t, id, convPushReasonMessage)
	require.Equal(t, sorted(a, c), got)
	require.NotContains(t, got, b, "автор о своём сообщении не уведомляется")
	require.NotContains(t, got, outsider)
}

// В событии нет содержимого: ни текста, ни автора, ни позиции; адресат на
// провод не выходит.
func TestConvPushEventHasNoBodyOnWire(t *testing.T) {
	r := newGroupRig(t, 2000)
	rec := withPush(r)
	a, b := r.id.person(pidA), r.id.person(pidB)
	id := r.group(t, a, pidB)
	rec.reset()
	m := r.mustSend(t, b, id, newRequestID(), "очень личный текст")

	rec.mu.Lock()
	require.Len(t, rec.events, 1)
	ev := rec.events[0]
	rec.mu.Unlock()
	require.Equal(t, a, ev.To)
	wire, err := json.Marshal(ev)
	require.NoError(t, err)
	require.JSONEq(t, `{"event":"conversation_activity","data":{"conversation_id":"`+id+`","reason":"message"}}`, string(wire))
	for _, leak := range []string{"очень личный", pidB, b, a, m.ID} {
		require.NotContains(t, string(wire), leak)
	}
}

// Ушедший участник событий разговора больше не получает.
func TestConvPushSkipsLeftMember(t *testing.T) {
	r := newGroupRig(t, 2000)
	rec := withPush(r)
	a, b, c := r.id.person(pidA), r.id.person(pidB), r.id.person(pidC)
	id := r.group(t, a, pidB, pidC)
	r.mustDo(t, c, ActionConversationLeave, reqOf(id))
	rec.reset()

	r.mustSend(t, b, id, newRequestID(), "после выхода")
	require.Equal(t, []string{a}, rec.to(t, id, convPushReasonMessage))
}

// Заблокировавший автора событий о его сообщениях не получает (ADR-111);
// остальные получают.
func TestConvPushSkipsMemberWhoBlockedAuthor(t *testing.T) {
	r := newGroupRig(t, 2000)
	rec := withPush(r)
	a, b, c := r.id.person(pidA), r.id.person(pidB), r.id.person(pidC)
	id := r.group(t, a, pidB, pidC)
	r.blockOK(t, a, pidB)
	rec.reset()

	r.mustSend(t, b, id, newRequestID(), "от заблокированного")
	require.Equal(t, []string{c}, rec.to(t, id, convPushReasonMessage))
}

// PhotonID, который не переводится в пользователя Mattermost, — без доставки
// этому получателю (fail-closed), остальным — как обычно.
func TestConvPushSkipsUnresolvableRecipient(t *testing.T) {
	r := newGroupRig(t, 2000)
	rec := withPush(r)
	a, b := r.id.person(pidA), r.id.person(pidB)
	r.id.person(pidC)
	id := r.group(t, a, pidB, pidC)
	r.id.mu.Lock()
	delete(r.id.recs, pidC)
	r.id.mu.Unlock()
	rec.reset()

	r.mustSend(t, b, id, newRequestID(), "x")
	require.Equal(t, []string{a}, rec.to(t, id, convPushReasonMessage))
}

// Ошибка базы при подсчёте аудитории — не отправляется никому.
func TestConvPushAudienceFailsClosed(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Empty(t, r.svc.messageAudience(ctx, id, pidA))
	require.Len(t, r.svc.messageAudience(context.Background(), id, pidA), 1, "контроль: без ошибки аудитория есть")
}

// Повтор 930 тем же ключом сообщение не повторяет — и событие тоже.
func TestConvPushNotRepeatedOnRetry(t *testing.T) {
	r := newGroupRig(t, 2000)
	rec := withPush(r)
	a, b := r.id.person(pidA), r.id.person(pidB)
	id := r.group(t, a, pidB)
	rec.reset()
	cid := newRequestID()
	first := r.mustSend(t, b, id, cid, "один раз")
	again := r.mustSend(t, b, id, cid, "один раз")
	require.Equal(t, first.ID, again.ID)
	require.Equal(t, []string{a}, rec.to(t, id, convPushReasonMessage), "повтор не дал второго события")
}

// Отказанный 930 события не даёт.
func TestConvPushNoEventOnRefusedSend(t *testing.T) {
	r := newGroupRig(t, 2000)
	rec := withPush(r)
	a := r.id.person(pidA)
	outsider := r.id.person(pidB)
	id := r.group(t, a)
	rec.reset()
	resp := r.send(t, outsider, id, newRequestID(), "мимо")
	require.False(t, resp.OK)
	require.Empty(t, rec.events)
}

// Личный разговор: событие только собеседнику.
func TestConvPushDirectGoesToPeer(t *testing.T) {
	r := newGroupRig(t, 2000)
	rec := withPush(r)
	a, b := r.id.person(pidA), r.id.person(pidB)
	r.id.person(pidC)
	id := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID
	rec.reset()
	r.mustSend(t, a, id, newRequestID(), "привет")
	require.Equal(t, []string{b}, rec.to(t, id, convPushReasonMessage))
}

// 908: событие только самому читателю (его другим устройствам) и только если
// позиция сдвинулась; остальные участники о чужом прочтении не узнают.
func TestConvPushReadGoesToReaderOnly(t *testing.T) {
	r := newGroupRig(t, 2000)
	rec := withPush(r)
	a, b, c := r.id.person(pidA), r.id.person(pidB), r.id.person(pidC)
	id := r.group(t, a, pidB, pidC)
	m := r.mustSend(t, b, id, newRequestID(), "прочти")
	rec.reset()

	r.mustMarkRead(t, a, id, m)
	require.Equal(t, []string{a}, rec.to(t, id, convPushReasonRead))
	require.NotContains(t, rec.to(t, id, convPushReasonRead), c)

	rec.reset()
	r.mustMarkRead(t, a, id, m)
	require.Empty(t, rec.to(t, id, convPushReasonRead), "та же отметка повторно — без события")
}

// Без настроенного push сервис работает как прежде.
func TestConvPushDisabledIsNoop(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	r.mustSend(t, a, id, newRequestID(), "без push")
}
