package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin/plugintest"
	"github.com/stretchr/testify/require"
	photonpb "github.com/wiloris/photon-core/protobuf"
)

// conversationCodesV1 — коды v1 ровно по таблице ADR-099 §2.2.
var conversationCodesV1 = []int{900, 901, 902, 903, 904, 910, 911, 912, 920, 921, 930, 931}

// conversationCodesV2 — коды Messaging v2 (ADR-104–107, контракт §1 M1): в срезе M1
// принимаются, но поведения нет — отвечают temporarily_unavailable.
var conversationCodesV2 = []int{905, 906, 907, 913, 914, 915, 916, 940}

// conversationCodesAccepted — полный принятый набор сотни 900.
func conversationCodesAccepted() []int {
	all := append(append([]int{}, conversationCodesV1...), conversationCodesV2...)
	sort.Ints(all)
	return all
}

// TestConversationCodesMatchADR099 — enum Action содержит ровно коды v1
// ADR-099 §2.2 и ничего сверх них в сотне 900; RESOURCE_CONVERSATION = 8
// (ADR-099 §2.1). Ловит и лишний код, и пропущенный.
func TestConversationCodesMatchADR099(t *testing.T) {
	var got []int
	for code, name := range photonpb.Action_name {
		if code >= 900 && code <= 999 {
			require.True(t, strings.HasPrefix(name, "ACTION_CONVERSATION_"),
				"код %d (%s) в сотне 900 не из ConversationResource", code, name)
			got = append(got, int(code))
		}
	}
	sort.Ints(got)
	require.Equal(t, conversationCodesAccepted(), got)
	require.Equal(t, int32(8), int32(photonpb.Resource_RESOURCE_CONVERSATION))
}

// TestConversationActionsRouteToConversationHandler — каждый код v1 проходит
// авторизацию по правилу «аутентифицирован» (ADR-099 §2.4) и доходит до
// обработчика сотни 900. Пока таблиц Bridge нет, ответ — temporarily_unavailable
// (ADR-100 §2.6), а не ложный успех.
func TestConversationActionsRouteToConversationHandler(t *testing.T) {
	d, _ := newDeniedDispatcher(t)
	for _, code := range conversationCodesAccepted() {
		resp := d.Dispatch(uB, HeaderFrame{Action: code}, json.RawMessage(`{}`))
		require.False(t, resp.OK, "код %d", code)
		require.Equal(t, errCodeTemporarilyUnavailable, resp.Error, "код %d", code)
		require.Nil(t, resp.Data, "код %d", code)
	}
}

// TestConversationReservedCodesStayUnknown — резерв ADR-099 §2.3 — место, а не
// обещание: такие коды не доходят до обработчика и отвечают своей ошибкой
// неизвестного действия, отличимой от отказа авторизации (ADR-039 §8).
func TestConversationReservedCodesStayUnknown(t *testing.T) {
	d, _ := newDeniedDispatcher(t)
	for _, code := range []int{908, 909, 917, 919, 922, 929, 932, 941, 950, 999} {
		resp := d.Dispatch(uB, HeaderFrame{Action: code}, json.RawMessage(`{}`))
		require.False(t, resp.OK, "код %d", code)
		require.Equal(t, fmt.Sprintf("unknown action: %d", code), resp.Error, "код %d", code)
	}
}

// TestMessagingV2NewCodesAreStubs — M1: каждый новый код 9xx авторизуется и отвечает
// temporarily_unavailable без данных.
func TestMessagingV2NewCodesAreStubs(t *testing.T) {
	d, _ := newDeniedDispatcher(t)
	for _, code := range conversationCodesV2 {
		resp := d.Dispatch(uB, HeaderFrame{Action: code}, json.RawMessage(`{}`))
		require.False(t, resp.OK, "код %d", code)
		require.Equal(t, errCodeTemporarilyUnavailable, resp.Error, "код %d", code)
		require.Nil(t, resp.Data, "код %d", code)
	}
}

// TestAuthz219HasRuleOf218 — ADR-105 / контракт §3: 219 — тот же объект и то же решение, что у 218,
// во всех ситуациях стенда DM (попарное сравнение, как TestDMv2AuthzMirrorsV1).
func TestAuthz219HasRuleOf218(t *testing.T) {
	require.Equal(t, rules[ActionListDirectMessagesV2].object, rules[ActionListDirectMessagesBefore].object)

	z, api, pidA, _ := dmFixture(t)
	check := func(subject string, payload map[string]string) {
		t.Helper()
		e218 := z.authorize(subject, ActionListDirectMessagesV2, pl(t, payload))
		e219 := z.authorize(subject, ActionListDirectMessagesBefore, pl(t, payload))
		require.Equal(t, e218, e219, "218 и 219 разошлись для %s: %v против %v", subject, e218, e219)
	}
	const nobody = "user-without-grants"
	for _, subject := range []string{uB, nobody} {
		for _, pid := range []string{pidA, "bad", "000000001"} {
			check(subject, map[string]string{"with_photon_id": pid})
		}
	}
	chID := "dm-ab"
	api.channels[chID] = &model.Channel{Id: chID, Name: model.GetDMNameFromIds(uA, uB), Type: model.ChannelTypeDirect}
	api.byName["/"+model.GetDMNameFromIds(uA, uB)] = chID
	check(uB, map[string]string{"with_photon_id": pidA})
	require.ErrorIs(t, z.authorize(uB, ActionListDirectMessagesBefore, pl(t, map[string]string{"with_photon_id": pidA})), errForbidden)
	api.grant(uB, "channel", chID, model.PermissionReadChannelContent)
	check(uB, map[string]string{"with_photon_id": pidA})
	require.NoError(t, z.authorize(uB, ActionListDirectMessagesBefore, pl(t, map[string]string{"with_photon_id": pidA})))
}

// TestAuthz219ReachesHandler — после авторизации 219 доходит до настоящего
// обработчика (срез M3), а не до заглушки M1: запрос без `before` отвечает
// invalid_request, а не temporarily_unavailable.
func TestAuthz219ReachesHandler(t *testing.T) {
	p := &Plugin{}
	p.SetAPI(&plugintest.API{})
	h := &UserHandler{plugin: p}
	resp := h.Handle(uB, ActionListDirectMessagesBefore, pl(t, map[string]string{"with_photon_id": pidA}))
	require.False(t, resp.OK)
	require.Equal(t, errCodeInvalidRequest, resp.Error)
	require.Nil(t, resp.Data)
}
