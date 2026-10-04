package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	photonpb "github.com/wiloris/photon-core/protobuf"
)

// conversationCodesV1 — коды v1 ровно по таблице ADR-099 §2.2.
var conversationCodesV1 = []int{900, 901, 902, 903, 904, 910, 911, 912, 920, 921, 930, 931}

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
	require.Equal(t, conversationCodesV1, got)
	require.Equal(t, int32(8), int32(photonpb.Resource_RESOURCE_CONVERSATION))
}

// TestConversationActionsRouteToConversationHandler — каждый код v1 проходит
// авторизацию по правилу «аутентифицирован» (ADR-099 §2.4) и доходит до
// обработчика сотни 900. Пока таблиц Bridge нет, ответ — temporarily_unavailable
// (ADR-100 §2.6), а не ложный успех.
func TestConversationActionsRouteToConversationHandler(t *testing.T) {
	d, _ := newDeniedDispatcher(t)
	for _, code := range conversationCodesV1 {
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
	for _, code := range []int{905, 909, 913, 919, 922, 929, 932, 950, 999} {
		resp := d.Dispatch(uB, HeaderFrame{Action: code}, json.RawMessage(`{}`))
		require.False(t, resp.OK, "код %d", code)
		require.Equal(t, fmt.Sprintf("unknown action: %d", code), resp.Error, "код %d", code)
	}
}
