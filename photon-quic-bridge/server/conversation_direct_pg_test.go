package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// M2: 905 OPEN_DIRECT и 902 с last_item против НАСТОЯЩЕГО PostgreSQL
// (ADR-104; messaging-v2-contract.md §2). PHOTON_DM_PG_DSN.

func (r *groupRig) openDirect(t *testing.T, callerMM, peer string) Response {
	t.Helper()
	return r.svc.dispatch(context.Background(), callerMM, ActionConversationOpenDirect,
		pl(t, map[string]any{"request_id": newRequestID(), "peer_photon_id": peer}))
}

func (r *groupRig) mustOpenDirect(t *testing.T, callerMM, peer string) ConvOpenDirectResponse {
	t.Helper()
	resp := r.openDirect(t, callerMM, peer)
	require.True(t, resp.OK, "905: %+v", resp)
	var out ConvOpenDirectResponse
	require.NoError(t, json.Unmarshal(resp.Data.(json.RawMessage), &out))
	return out
}

func (r *groupRig) listAll(t *testing.T, callerMM string) []ConvListItem {
	t.Helper()
	resp := r.svc.listConversations(context.Background(), callerMM, pl(t, map[string]any{}))
	require.True(t, resp.OK, "902: %+v", resp)
	return resp.Data.(ConvListResponse).Conversations
}

func TestConvDirectOpenCreatesAndRepeats(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)

	first := r.mustOpenDirect(t, a, pidB)
	require.True(t, first.Created)
	c := first.Conversation
	require.Equal(t, "direct", c.Kind)
	require.Equal(t, "active", c.State)
	require.Equal(t, "", c.Title)
	require.Equal(t, "full", c.HistoryPolicy)
	require.Equal(t, int64(1), c.ConversationVersion)
	require.Equal(t, 2, c.MemberCount)
	require.Equal(t, pidA, c.CreatedBy)
	require.Equal(t, "member", c.Me.Role)
	require.Equal(t, []string{"messages.send", "messages.read"}, c.Me.Permissions)

	// События: created + member_added пира; оба участника member.
	var kinds []string
	rows, err := r.db.Query(`SELECT event->>'event', state FROM engram_event WHERE conversation_id = $1 ORDER BY version, seq`, c.ConversationID)
	require.NoError(t, err)
	for rows.Next() {
		var k, st string
		require.NoError(t, rows.Scan(&k, &st))
		require.Equal(t, "written", st)
		kinds = append(kinds, k)
	}
	rows.Close()
	require.Equal(t, []string{"created", "member_added"}, kinds)
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM engram_member_period WHERE conversation_id = $1 AND role = 'member' AND state = 'active'`, c.ConversationID))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_direct_pair WHERE conversation_id = $1 AND low_pid = $2 AND high_pid = $3`, c.ConversationID, pidA, pidB))

	// Повтор и встречный вызов — тот же разговор, created:false.
	again := r.mustOpenDirect(t, a, pidB)
	require.False(t, again.Created)
	require.Equal(t, c.ConversationID, again.Conversation.ConversationID)
	opposite := r.mustOpenDirect(t, b, pidA)
	require.False(t, opposite.Created)
	require.Equal(t, c.ConversationID, opposite.Conversation.ConversationID)
	require.Equal(t, "member", opposite.Conversation.Me.Role)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_conversation WHERE kind = 'direct'`))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_direct_pair`))
}

func TestConvDirectParallelOppositeOpensOneConversation(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)

	const n = 8
	var wg sync.WaitGroup
	res := make([]Response, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			caller, peer := a, pidB
			if i%2 == 1 {
				caller, peer = b, pidA
			}
			res[i] = r.svc.dispatch(context.Background(), caller, ActionConversationOpenDirect,
				pl(t, map[string]any{"request_id": newRequestID(), "peer_photon_id": peer}))
		}(i)
	}
	wg.Wait()
	ids := map[string]bool{}
	created := 0
	for _, resp := range res {
		require.True(t, resp.OK, "%+v", resp)
		var out ConvOpenDirectResponse
		require.NoError(t, json.Unmarshal(resp.Data.(json.RawMessage), &out))
		ids[out.Conversation.ConversationID] = true
		if out.Created {
			created++
		}
		require.Equal(t, "active", out.Conversation.State)
		require.Equal(t, 2, out.Conversation.MemberCount)
	}
	require.Len(t, ids, 1, "один разговор на пару")
	require.Equal(t, 1, created, "created:true ровно у одного вызова")
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_conversation`))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_direct_pair`))
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM engram_event`))
}

func TestConvDirectValidation(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	deact := r.id.person(pidC)
	r.id.deactivate(deact)

	open := func(caller string, payload map[string]any) Response {
		return r.svc.dispatch(context.Background(), caller, ActionConversationOpenDirect, pl(t, payload))
	}
	req := func(peer string) map[string]any {
		return map[string]any{"request_id": newRequestID(), "peer_photon_id": peer}
	}
	require.Equal(t, errInvalidPhotonID, open(a, req("abc")).Error)
	require.Equal(t, errInvalidPhotonID, open(a, req("")).Error)
	require.Equal(t, convErrInvalidRequest, open(a, req(pidA)).Error, "свой PhotonID")
	require.Equal(t, errPhotonIDNotFound, open(a, req("999999999")).Error, "неизвестный")
	require.Equal(t, errPhotonIDNotFound, open(a, req(pidC)).Error, "деактивированный")
	require.Equal(t, convErrInvalidRequest, open(a, map[string]any{"peer_photon_id": pidB}).Error, "нет request_id")
	require.Equal(t, convErrInvalidRequest, open(a, map[string]any{"request_id": "x", "peer_photon_id": pidB}).Error)
	extra := req(pidB)
	extra["kind"] = "direct"
	require.Equal(t, convErrInvalidRequest, open(a, extra).Error, "лишнее поле")
	require.Equal(t, convErrForbidden, open(r.id.plainUser(), req(pidB)).Error, "вызывающий без PhotonID")
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_conversation`), "отказы ничего не создают")
	_ = b

	// 900 с kind=direct создаёт только 905.
	bad := createReq(newRequestID(), "d", pidB)
	bad["kind"] = "direct"
	require.Equal(t, convErrInvalidRequest, r.create(t, a, bad).Error)
}

func TestConvDirectForbidsGroupChanges(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)
	convID := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID
	ver := r.version(t, convID)

	for _, caller := range []string{a, b} {
		for _, tc := range []struct {
			action  int
			payload map[string]any
		}{
			{ActionConversationUpdate, reqOf(convID, "title", "x")},
			{ActionConversationArchive, reqOf(convID)},
			{ActionConversationMembersAdd, reqOf(convID, "photon_ids", []string{pidC})},
			{ActionConversationMemberRemove, reqOf(convID, "photon_id", pidB)},
			{ActionConversationLeave, reqOf(convID)},
			{ActionConversationRoleSet, reqOf(convID, "photon_id", pidB, "role", "admin")},
			{ActionConversationOwnerTransfer, reqOf(convID, "photon_id", pidB)},
		} {
			resp := r.do(t, caller, tc.action, tc.payload)
			require.False(t, resp.OK, "%d", tc.action)
			require.Equal(t, convErrInvalidRequest, resp.Error, "действие %d", tc.action)
		}
	}
	require.Equal(t, ver, r.version(t, convID), "версия не менялась")
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM engram_member_period WHERE conversation_id = $1 AND state = 'active'`, convID))

	// Доступ проверяется раньше: посторонний — conversation_not_found.
	outsider := r.id.person(pidD)
	resp := r.do(t, outsider, ActionConversationLeave, reqOf(convID))
	require.Equal(t, convErrConversationNF, resp.Error)
}

func TestConvDirectGetSendSync(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)
	convID := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID

	// 901 — оба видят, состав из двух member.
	for _, caller := range []string{a, b} {
		snap := r.do(t, caller, ActionConversationGet, map[string]any{"conversation_id": convID})
		require.True(t, snap.OK)
		s := snap.Data.(ConvSnapshotResponse)
		require.Equal(t, "direct", s.Conversation.Kind)
		require.Len(t, s.Members, 2)
		for _, m := range s.Members {
			require.Equal(t, "member", m.Role)
		}
	}
	// 930: оба отправляют; 931: обе стороны видят всю ленту.
	m1 := r.mustSend(t, a, convID, newRequestID(), "привет")
	m2 := r.mustSend(t, b, convID, newRequestID(), "ответ")
	require.Greater(t, m2.CreatedAt, m1.CreatedAt)
	for _, caller := range []string{a, b} {
		items := r.fullPass(t, caller, convID, 100)
		require.Equal(t, []string{"event:created", "event:member_added", "msg:привет", "msg:ответ"}, names(items), "видят всю ленту (full)")
	}
	// Посторонний — conversation_not_found.
	outsider := r.id.person(pidD)
	require.Equal(t, convErrConversationNF, r.send(t, outsider, convID, newRequestID(), "x").Error)
	require.Equal(t, convErrConversationNF, r.syncRaw(t, outsider, map[string]any{"conversation_id": convID}).Error)
}

func TestConv902MixedKindsLastItemAndPeer(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)

	dm := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID
	grp := r.group(t, a, pidC)
	other := r.mustOpenDirect(t, a, pidC).Conversation.ConversationID
	m := r.mustSend(t, b, dm, newRequestID(), "последнее")

	byID := func(items []ConvListItem) map[string]ConvListItem {
		out := map[string]ConvListItem{}
		for _, it := range items {
			out[it.ConversationID] = it
		}
		return out
	}
	la := byID(r.listAll(t, a))
	require.Len(t, la, 3)
	require.Equal(t, "direct", la[dm].Kind)
	require.Equal(t, pidB, la[dm].PeerPhotonID)
	require.Equal(t, pidC, la[other].PeerPhotonID)
	require.Equal(t, "group", la[grp].Kind)
	require.Empty(t, la[grp].PeerPhotonID)
	require.Equal(t, "member", la[dm].MyRole)
	last, ok := la[dm].LastItem.(ConvMessageItem)
	require.True(t, ok, "%T", la[dm].LastItem)
	require.Equal(t, m.ID, last.ID)
	require.Equal(t, "последнее", last.Text)
	require.Equal(t, pidB, last.AuthorPhotonID)
	// У группы последний элемент — событие member_added.
	ev, ok := la[grp].LastItem.(ConvEventItem)
	require.True(t, ok)
	require.Equal(t, "member_added", ev.Event)

	// Пир видит своего собеседника.
	lb := byID(r.listAll(t, b))
	require.Len(t, lb, 1)
	require.Equal(t, pidA, lb[dm].PeerPhotonID)

	// JSON: last_item присутствует всегда, peer_photon_id — только у direct.
	raw, err := json.Marshal(la[grp])
	require.NoError(t, err)
	var grpJSON map[string]any
	require.NoError(t, json.Unmarshal(raw, &grpJSON))
	require.NotContains(t, grpJSON, "peer_photon_id")
	require.Contains(t, grpJSON, "last_item")

	// Нет видимых элементов → null.
	_, err = r.db.Exec(`UPDATE engram_conversation SET history_policy = 'since_join' WHERE conversation_id = $1`, dm)
	require.NoError(t, err)
	_, err = r.db.Exec(`UPDATE engram_member_period SET open_at = open_at + 100000000 WHERE conversation_id = $1`, dm)
	require.NoError(t, err)
	l2 := byID(r.listAll(t, a))
	require.Nil(t, l2[dm].LastItem)
	raw, err = json.Marshal(l2[dm])
	require.NoError(t, err)
	require.Contains(t, string(raw), `"last_item":null`)
}

func TestConv902LastItemRespectsSinceJoin(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	convID := r.group(t, a, pidB) // since_join
	r.mustSend(t, a, convID, newRequestID(), "до входа C")
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(convID, "photon_ids", []string{pidC}))
	// C вошёл: видит только своё открывающее событие, не «до входа C».
	lc := r.listAll(t, c)
	require.Len(t, lc, 1)
	ev, ok := lc[0].LastItem.(ConvEventItem)
	require.True(t, ok, "%T", lc[0].LastItem)
	require.Equal(t, "member_added", ev.Event)
	require.Equal(t, pidC, ev.TargetPhotonID)

	// C выходит; закрыт период → в списке нет. После повторного входа
	// видимы оба периода, но не сообщения между ними.
	r.mustDo(t, c, ActionConversationLeave, reqOf(convID))
	r.mustSend(t, a, convID, newRequestID(), "пока C нет")
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(convID, "photon_ids", []string{pidC}))
	r.mustSend(t, b, convID, newRequestID(), "после возврата")
	lc = r.listAll(t, c)
	require.Len(t, lc, 1)
	msg, ok := lc[0].LastItem.(ConvMessageItem)
	require.True(t, ok)
	require.Equal(t, "после возврата", msg.Text)

	// Сдвигаем границу периода C за последнее сообщение: оно не видно, а
	// последний видимый — не сообщение «пока C нет» и не «до входа C».
	la := r.listAll(t, a)
	require.Equal(t, "после возврата", la[0].LastItem.(ConvMessageItem).Text)
	items := r.fullPass(t, c, convID, 100)
	require.NotContains(t, names(items), "msg:пока C нет")
	require.NotContains(t, names(items), "msg:до входа C")
}
