package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// M4: 906 UNARCHIVE, 907 MEMBER_STATE_SET и порядок/скрытие в 902 против
// НАСТОЯЩЕГО PostgreSQL (ADR-106; messaging-v2-contract.md §4). PHOTON_DM_PG_DSN.

func (r *groupRig) memberState(t *testing.T, caller string, payload map[string]any) Response {
	t.Helper()
	return r.do(t, caller, ActionConversationMemberStateSet, payload)
}

func (r *groupRig) mustMemberState(t *testing.T, caller string, payload map[string]any) ConvMemberState {
	t.Helper()
	resp := r.memberState(t, caller, payload)
	require.True(t, resp.OK, "907: %+v", resp)
	return resp.Data.(ConvMemberStateResponse).MemberState
}

func (r *groupRig) listOpts(t *testing.T, caller string, payload map[string]any) ConvListResponse {
	t.Helper()
	resp := r.do(t, caller, ActionConversationList, payload)
	require.True(t, resp.OK, "902: %+v", resp)
	return resp.Data.(ConvListResponse)
}

func listIDs(items []ConvListItem) []string {
	out := []string{}
	for _, it := range items {
		out = append(out, it.ConversationID)
	}
	return out
}

func (r *groupRig) eventNames(t *testing.T, caller, convID string) []string {
	t.Helper()
	var out []string
	cursor := ""
	for {
		page := r.syncPage(t, caller, convID, cursor, 0)
		for _, it := range page.Items {
			if ev, ok := it.(ConvEventItem); ok {
				out = append(out, ev.Event)
			}
		}
		if page.AtEnd || page.NextCursor == "" {
			return out
		}
		cursor = page.NextCursor
	}
}

func snapshotState(t *testing.T, resp Response) (string, int64) {
	t.Helper()
	require.True(t, resp.OK, "%+v", resp)
	var out struct {
		Conversation struct {
			State   string `json:"state"`
			Version int64  `json:"conversation_version"`
		} `json:"conversation"`
	}
	require.NoError(t, json.Unmarshal(resp.Data.(json.RawMessage), &out))
	return out.Conversation.State, out.Conversation.Version
}

func TestLifecycleUnarchive906(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB, pidC)
	// admin — не владелец
	r.mustDo(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidB, "role", "admin"))
	r.mustDo(t, a, ActionConversationArchive, reqOf(id))
	archivedVer := r.version(t, id)

	// право conversation.archive — только владелец
	for _, who := range []string{b, c} {
		resp := r.do(t, who, ActionConversationUnarchive, reqOf(id))
		require.False(t, resp.OK)
		require.Equal(t, convErrForbidden, resp.Error)
	}
	require.Equal(t, archivedVer, r.version(t, id))

	// посторонний — conversation_not_found
	outsider := r.id.person(pidD)
	require.Equal(t, convErrConversationNF, r.do(t, outsider, ActionConversationUnarchive, reqOf(id)).Error)

	// expected_version сверяется
	resp := r.do(t, a, ActionConversationUnarchive, reqOf(id, "expected_version", archivedVer-1))
	require.Equal(t, convErrVersionConflict, resp.Error)

	req := reqOf(id, "expected_version", archivedVer)
	resp = r.do(t, a, ActionConversationUnarchive, req)
	state, ver := snapshotState(t, resp)
	require.Equal(t, "active", state)
	require.Equal(t, archivedVer+1, ver)
	require.Equal(t, archivedVer+1, r.version(t, id))

	// повтор с тем же ключом — тот же ответ, версия не растёт
	again := r.do(t, a, ActionConversationUnarchive, req)
	require.JSONEq(t, string(resp.Data.(json.RawMessage)), string(again.Data.(json.RawMessage)))
	require.Equal(t, archivedVer+1, r.version(t, id))

	// событие unarchived видно в 931 и последним
	names := r.eventNames(t, b, id)
	require.Equal(t, "unarchived", names[len(names)-1])
	require.Equal(t, 1, countOf(names, "unarchived"))

	// после разархивирования 930 снова работает
	r.mustSend(t, b, id, newRequestID(), "hello")
}

func countOf(xs []string, s string) int {
	n := 0
	for _, x := range xs {
		if x == s {
			n++
		}
	}
	return n
}

func TestLifecycleUnarchiveNotArchivedIsNoChange(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	ver := r.version(t, id)
	events := r.eventNames(t, a, id)

	resp := r.do(t, a, ActionConversationUnarchive, reqOf(id))
	state, v := snapshotState(t, resp)
	require.Equal(t, "active", state)
	require.Equal(t, ver, v)
	require.Equal(t, ver, r.version(t, id))
	require.Equal(t, events, r.eventNames(t, a, id), "события не добавились")
}

func TestLifecycleUnarchiveDirectIsNoChange(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID
	ver := r.version(t, id)
	for _, who := range []string{a, b} {
		resp := r.do(t, who, ActionConversationUnarchive, reqOf(id))
		state, v := snapshotState(t, resp)
		require.Equal(t, "active", state)
		require.Equal(t, ver, v)
	}
	require.Equal(t, ver, r.version(t, id))
	// остальные коды по-прежнему отказывают direct
	require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationArchive, reqOf(id)).Error)
}

func TestLifecycleOtherCodesStillRefuseArchived(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	r.id.person(pidC)
	id := r.group(t, a, pidB)
	r.mustDo(t, a, ActionConversationArchive, reqOf(id))

	for _, tc := range []struct {
		action  int
		payload map[string]any
	}{
		{ActionConversationArchive, reqOf(id)},
		{ActionConversationUpdate, reqOf(id, "title", "x")},
		{ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC})},
		{ActionConversationMemberRemove, reqOf(id, "photon_id", pidB)},
		{ActionConversationRoleSet, reqOf(id, "photon_id", pidB, "role", "admin")},
		{ActionConversationOwnerTransfer, reqOf(id, "photon_id", pidB)},
	} {
		resp := r.do(t, a, tc.action, tc.payload)
		require.False(t, resp.OK, "%d", tc.action)
		require.Equal(t, convErrArchived, resp.Error, "действие %d", tc.action)
	}
}

func TestLifecycleMemberState907(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	ver := r.version(t, id)
	events := r.eventNames(t, b, id)
	reqKeys := r.count(t, `SELECT count(*) FROM engram_request`)

	// хотя бы одно поле
	require.Equal(t, convErrInvalidRequest, r.memberState(t, a, map[string]any{"conversation_id": id}).Error)
	require.Equal(t, convErrInvalidRequest, r.memberState(t, a, map[string]any{"conversation_id": id, "muted_until": -1}).Error)
	require.Equal(t, convErrInvalidRequest, r.memberState(t, a, map[string]any{"conversation_id": id, "hidden": nil}).Error)
	require.Equal(t, convErrInvalidRequest, r.memberState(t, a, map[string]any{"conversation_id": id, "pinned": "yes"}).Error)
	require.Equal(t, convErrConversationNF, r.memberState(t, r.id.person(pidD), map[string]any{"conversation_id": id, "hidden": true}).Error)

	req := map[string]any{"conversation_id": id, "hidden": true, "pinned": true, "muted_until": 4102444800000}
	st := r.mustMemberState(t, a, req)
	require.True(t, st.Hidden)
	require.NotNil(t, st.PinnedAt)
	require.NotNil(t, st.MutedUntil)
	require.Equal(t, int64(4102444800000), *st.MutedUntil)

	// идемпотентно: повтор — тот же итог, pinned_at сохраняется
	again := r.mustMemberState(t, a, req)
	require.Equal(t, st, again)

	// не меняет разговор, не порождает событий и ключей
	require.Equal(t, ver, r.version(t, id))
	require.Equal(t, events, r.eventNames(t, b, id))
	require.Equal(t, reqKeys, r.count(t, `SELECT count(*) FROM engram_request`))

	// другой участник состояния не видит
	for _, it := range r.listAll(t, b) {
		require.Equal(t, ConvMemberState{}, it.MemberState)
	}
	require.Equal(t, 0, len(r.listAll(t, a)), "для a разговор скрыт")

	// null снимает mute, false снимает hidden и pinned
	st = r.mustMemberState(t, a, map[string]any{"conversation_id": id, "muted_until": nil, "hidden": false, "pinned": false})
	require.Equal(t, ConvMemberState{}, st)
}

func TestLifecycleMemberState907Direct(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID
	ver := r.version(t, id)
	st := r.mustMemberState(t, a, map[string]any{"conversation_id": id, "pinned": true})
	require.NotNil(t, st.PinnedAt)
	require.Equal(t, ver, r.version(t, id))
	items := r.listAll(t, a)
	require.Len(t, items, 1)
	require.NotNil(t, items[0].MemberState.PinnedAt)
	require.Nil(t, r.listAll(t, b)[0].MemberState.PinnedAt)
}

func TestLifecycleMemberState907ArchivedAccess(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB, pidC)
	// (a) B удалён, пока разговор активен: и до, и после архивации — no_access
	r.mustDo(t, a, ActionConversationMemberRemove, reqOf(id, "photon_id", pidB))
	require.Equal(t, convErrNoAccess, r.memberState(t, b, map[string]any{"conversation_id": id, "hidden": true}).Error)
	r.mustDo(t, a, ActionConversationArchive, reqOf(id))
	require.Equal(t, convErrNoAccess, r.memberState(t, b, map[string]any{"conversation_id": id, "hidden": true}).Error)
	// (b) C был active в момент архивации — 907 проходит, 902 показывает состояние
	st := r.mustMemberState(t, c, map[string]any{"conversation_id": id, "hidden": true})
	require.True(t, st.Hidden)
	items := r.listOpts(t, c, map[string]any{"include_hidden": true}).Conversations
	require.Len(t, items, 1)
	require.True(t, items[0].MemberState.Hidden)
}

func TestLifecycleHiddenReturnsAfterNewMessage(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	r.mustSend(t, b, id, newRequestID(), "one")
	time.Sleep(5 * time.Millisecond)

	r.mustMemberState(t, a, map[string]any{"conversation_id": id, "hidden": true})
	require.Empty(t, r.listAll(t, a))
	hiddenList := r.listOpts(t, a, map[string]any{"include_hidden": true}).Conversations
	require.Len(t, hiddenList, 1)
	require.True(t, hiddenList[0].MemberState.Hidden)

	// новое сообщение от другого участника → возвращается; hidden = действующее
	time.Sleep(5 * time.Millisecond)
	r.mustSend(t, b, id, newRequestID(), "two")
	items := r.listAll(t, a)
	require.Len(t, items, 1)
	require.False(t, items[0].MemberState.Hidden)
	// сохранённый флаг в 907 остаётся true
	st := r.mustMemberState(t, a, map[string]any{"conversation_id": id, "pinned": false})
	require.True(t, st.Hidden)
}

func TestLifecycleHiddenStaysWhenMuted(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	future := time.Now().Add(time.Hour).UnixMilli()
	r.mustMemberState(t, a, map[string]any{"conversation_id": id, "hidden": true, "muted_until": future})
	time.Sleep(5 * time.Millisecond)
	r.mustSend(t, b, id, newRequestID(), "ping")
	require.Empty(t, r.listAll(t, a), "заглушённый скрытый разговор не возвращается")
	require.Len(t, r.listOpts(t, a, map[string]any{"include_hidden": true}).Conversations, 1)

	// mute в прошлом — правило не действует
	r.mustMemberState(t, a, map[string]any{"conversation_id": id, "muted_until": 1})
	time.Sleep(5 * time.Millisecond)
	r.mustSend(t, b, id, newRequestID(), "pong")
	require.Len(t, r.listAll(t, a), 1)
}

func TestLifecyclePinnedOrder(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	var ids []string
	for i := 0; i < 4; i++ {
		ids = append(ids, r.group(t, a))
		time.Sleep(3 * time.Millisecond)
	}
	// без закреплений: свежие первыми
	require.Equal(t, []string{ids[3], ids[2], ids[1], ids[0]}, listIDs(r.listAll(t, a)))

	r.mustMemberState(t, a, map[string]any{"conversation_id": ids[0], "pinned": true})
	time.Sleep(3 * time.Millisecond)
	r.mustMemberState(t, a, map[string]any{"conversation_id": ids[1], "pinned": true})
	// закреплённые первыми по pinned_at убыв., затем last_activity_at
	require.Equal(t, []string{ids[1], ids[0], ids[3], ids[2]}, listIDs(r.listAll(t, a)))

	// курсор идёт по незакреплённым; закреплённые только на первой странице
	p1 := r.listOpts(t, a, map[string]any{"limit": 1})
	require.Equal(t, []string{ids[1], ids[0], ids[3]}, listIDs(p1.Conversations))
	require.NotEmpty(t, p1.NextCursor)
	p2 := r.listOpts(t, a, map[string]any{"limit": 1, "cursor": p1.NextCursor})
	require.Equal(t, []string{ids[2]}, listIDs(p2.Conversations))
	require.Empty(t, p2.NextCursor)

	// снятие закрепления возвращает обычный порядок
	r.mustMemberState(t, a, map[string]any{"conversation_id": ids[1], "pinned": false})
	r.mustMemberState(t, a, map[string]any{"conversation_id": ids[0], "pinned": false})
	require.Equal(t, []string{ids[3], ids[2], ids[1], ids[0]}, listIDs(r.listAll(t, a)))
}

func TestLifecyclePinLimit(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	var ids []string
	for i := 0; i < 7; i++ {
		ids = append(ids, r.group(t, a))
	}
	for i := 0; i < 5; i++ {
		r.mustMemberState(t, a, map[string]any{"conversation_id": ids[i], "pinned": true})
	}
	resp := r.memberState(t, a, map[string]any{"conversation_id": ids[5], "pinned": true})
	require.False(t, resp.OK)
	require.Equal(t, convErrInvalidRequest, resp.Error)

	// повторное закрепление уже закреплённого — не считается за «другого»
	st := r.mustMemberState(t, a, map[string]any{"conversation_id": ids[0], "pinned": true})
	require.NotNil(t, st.PinnedAt)
	// другие поля шестого разговора менять можно
	r.mustMemberState(t, a, map[string]any{"conversation_id": ids[5], "hidden": true})
	// после снятия — можно
	r.mustMemberState(t, a, map[string]any{"conversation_id": ids[0], "pinned": false})
	r.mustMemberState(t, a, map[string]any{"conversation_id": ids[5], "pinned": true})
	// предел — на участника, не на разговор
	b := r.id.person(pidB)
	other := r.mustCreate(t, b, createReq(newRequestID(), "g", pidA)).Conversation.ConversationID
	r.mustMemberState(t, b, map[string]any{"conversation_id": other, "pinned": true})
}

func TestLifecycleParallelPinsNeverExceedFive(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	var ids []string
	for i := 0; i < 12; i++ {
		ids = append(ids, r.group(t, a))
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			resp := r.svc.dispatch(context.Background(), a, ActionConversationMemberStateSet,
				pl(t, map[string]any{"conversation_id": id, "pinned": true}))
			if resp.OK {
				mu.Lock()
				ok++
				mu.Unlock()
			} else {
				require.Equal(t, convErrInvalidRequest, resp.Error)
			}
		}(id)
	}
	wg.Wait()
	require.Equal(t, 5, ok)
	require.Equal(t, 5, r.count(t, `SELECT count(*) FROM engram_member_state WHERE pinned_at IS NOT NULL`))
}

func TestLifecycleListWithoutStateIsUnchanged(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	items := r.listAll(t, a)
	require.Len(t, items, 1)
	require.Equal(t, id, items[0].ConversationID)
	require.Equal(t, ConvMemberState{}, items[0].MemberState)
	raw, err := json.Marshal(items[0])
	require.NoError(t, err)
	require.Contains(t, string(raw), `"member_state":{"hidden":false,"pinned_at":null,"muted_until":null}`)
}
