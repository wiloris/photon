package main

import (
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
)

// ADR-092 ред. 3 §2.7: 217/218 — те же объект и права, что 213/214.
// Проверяется попарным сравнением в каждой ситуации, а не отдельными
// ожиданиями: расхождение правил v1 и v2 роняет тест, даже если оба
// «правдоподобны».
func TestDMv2AuthzMirrorsV1(t *testing.T) {
	type pair struct{ v1, v2 int }
	send := pair{ActionSendDirectMessage, ActionSendDirectMessageV2}
	list := pair{ActionListDirectMessages, ActionListDirectMessagesV2}

	check := func(t *testing.T, z *authz, subject string, p pair, payload map[string]string) {
		t.Helper()
		e1 := z.authorize(subject, p.v1, pl(t, payload))
		e2 := z.authorize(subject, p.v2, pl(t, payload))
		require.Equal(t, e1, e2, "v1 %d и v2 %d разошлись для %s: %v против %v", p.v1, p.v2, subject, e1, e2)
	}

	z, api, pidA, _ := dmFixture(t)
	const nobody = "user-without-grants"
	for _, subject := range []string{uB, nobody} {
		for _, pid := range []string{pidA, "bad", "000000001"} {
			check(t, z, subject, send, map[string]string{"to_photon_id": pid})
			check(t, z, subject, list, map[string]string{"with_photon_id": pid})
		}
	}

	chID := "dm-ab"
	api.channels[chID] = &model.Channel{Id: chID, Name: model.GetDMNameFromIds(uA, uB), Type: model.ChannelTypeDirect}
	api.byName["/"+model.GetDMNameFromIds(uA, uB)] = chID
	check(t, z, uB, send, map[string]string{"to_photon_id": pidA})
	check(t, z, uB, list, map[string]string{"with_photon_id": pidA})
	require.ErrorIs(t, z.authorize(uB, ActionSendDirectMessageV2, pl(t, map[string]string{"to_photon_id": pidA})), errForbidden)

	api.grant(uB, "channel", chID, model.PermissionCreatePost, model.PermissionReadChannelContent)
	check(t, z, uB, send, map[string]string{"to_photon_id": pidA})
	check(t, z, uB, list, map[string]string{"with_photon_id": pidA})
	require.NoError(t, z.authorize(uB, ActionListDirectMessagesV2, pl(t, map[string]string{"with_photon_id": pidA})))

	z.accounts = failingFinder{}
	check(t, z, uB, send, map[string]string{"to_photon_id": pidA})
	require.ErrorIs(t, z.authorize(uB, ActionListDirectMessagesV2, pl(t, map[string]string{"with_photon_id": pidA})), errForbidden)
}
