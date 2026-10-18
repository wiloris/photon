package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// ADR-040. Измерено baseline'ом 2026-09-04: подписчик B получал пост из
// приватного канала A и из личной переписки A целиком, с телом. Контрольная
// половина обязательна — «B не получил» без «A получил» неотличимо от мёртвой
// подписки.

type recordingSink struct{ sent map[string][]PushEvent }

func newRecordingSink() *recordingSink { return &recordingSink{sent: map[string][]PushEvent{}} }

func (s *recordingSink) subscribers() []string {
	out := make([]string, 0, len(s.sent))
	for u := range s.sent {
		out = append(out, u)
	}
	return out
}

func (s *recordingSink) sendTo(userID string, e PushEvent) {
	s.sent[userID] = append(s.sent[userID], e)
}

func newRouterFixture(subs ...string) (*PushRouter, *recordingSink) {
	sink := newRecordingSink()
	for _, u := range subs {
		sink.sent[u] = nil
	}
	return &PushRouter{authz: newFixture(), sink: sink}, sink
}

func TestPushNotDeliveredWithoutReadAccess(t *testing.T) {
	r, sink := newRouterFixture(uA, uB)
	r.Route(PushEvent{Event: PushEventPosted, ChannelID: cPriv, Data: json.RawMessage(`{"message":"secret"}`)})

	require.Len(t, sink.sent[uA], 1, "A состоит в приватном канале — своё событие получает")
	require.Empty(t, sink.sent[uB], "B доступа к каналу не имеет — событие не доставляется")
}

func TestPushDeliveredToEveryoneWhoCanRead(t *testing.T) {
	r, sink := newRouterFixture(uA, uB)
	r.Route(PushEvent{Event: PushEventPosted, ChannelID: cPub, Data: json.RawMessage(`{"message":"hi"}`)})
	require.Len(t, sink.sent[uA], 1)
	require.Len(t, sink.sent[uB], 1)
}

// Публичный канал, в котором B не состоит: read(C) выполняется по team-праву,
// значит событие доставляется. Правило одно и то же для запроса и для события.
func TestPushFollowsPublicFallback(t *testing.T) {
	r, sink := newRouterFixture(uB)
	r.Route(PushEvent{Event: PushEventPosted, ChannelID: cPub2, Data: json.RawMessage(`{}`)})
	require.Len(t, sink.sent[uB], 1)
}

// Fail-closed: событие без разрешимого канала не рассылается никому.
// Прежний Broadcast в этом случае уходил КАЖДОМУ подписчику.
func TestPushWithoutChannelIsDropped(t *testing.T) {
	r, sink := newRouterFixture(uA, uB)
	r.Route(PushEvent{Event: PushEventPosted, Data: json.RawMessage(`{}`)})
	require.Empty(t, sink.sent[uA])
	require.Empty(t, sink.sent[uB])

	r2, sink2 := newRouterFixture(uA, uB)
	r2.Route(PushEvent{Event: PushEventPosted, ChannelID: "no-such-channel", Data: json.RawMessage(`{}`)})
	require.Empty(t, sink2.sent[uA])
	require.Empty(t, sink2.sent[uB])
}

// Событие, адресованное конкретному пользователю (status_change), уходит
// только ему: адресат известен по построению, канала у события нет.
func TestPushAddressedEventGoesToAddresseeOnly(t *testing.T) {
	r, sink := newRouterFixture(uA, uB)
	r.Route(PushEvent{Event: PushEventStatusChange, UserID: uB, Data: json.RawMessage(`{}`)})
	require.Empty(t, sink.sent[uA])
	require.Len(t, sink.sent[uB], 1)
}

// Маршрутный идентификатор канала не должен попадать на провод: форма push
// для варианта A не меняется (ADR-040 §2).
func TestPushChannelIDIsNotSerialized(t *testing.T) {
	b, err := json.Marshal(PushEvent{Event: PushEventPosted, ChannelID: cPriv, Data: json.RawMessage(`{}`)})
	require.NoError(t, err)
	require.NotContains(t, string(b), cPriv)
	require.NotContains(t, string(b), "channel_id")
}

// Событие разговора ENGRAM (ADR-123 §2.2): адресат посчитан convService, и
// маршрутизатор отдаёт его ровно ему, мимо read(C) и без перебора подписчиков.
func TestPushConversationEventGoesToComputedAddresseeOnly(t *testing.T) {
	r, sink := newRouterFixture(uA, uB)
	r.Route(convActivityEvent(uB, "conv-1", convPushReasonMessage))
	require.Empty(t, sink.sent[uA])
	require.Len(t, sink.sent[uB], 1)
}

// Адресат события разговора на провод не выходит: клиенту ENGRAM mm_user_id
// не выдаётся (ADR-081 §2.5).
func TestPushConversationAddresseeIsNotSerialized(t *testing.T) {
	b, err := json.Marshal(convActivityEvent(uB, "conv-1", convPushReasonRead))
	require.NoError(t, err)
	require.NotContains(t, string(b), uB)
	require.JSONEq(t, `{"event":"conversation_activity","data":{"conversation_id":"conv-1","reason":"read"}}`, string(b))
}
