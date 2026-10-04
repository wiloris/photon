package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Изменения 903–921 против НАСТОЯЩЕГО PostgreSQL (ADR-099 §8, §10, §11;
// conversation-storage.md §6.2, §10: C2, C2b, C4, C5). PHOTON_DM_PG_DSN.

func (r *groupRig) do(t *testing.T, callerMM string, action int, payload map[string]any) Response {
	t.Helper()
	return r.svc.dispatch(context.Background(), callerMM, action, pl(t, payload))
}

func (r *groupRig) mustDo(t *testing.T, callerMM string, action int, payload map[string]any) json.RawMessage {
	t.Helper()
	resp := r.do(t, callerMM, action, payload)
	require.True(t, resp.OK, "%d: %+v", action, resp)
	b, ok := resp.Data.(json.RawMessage)
	require.True(t, ok)
	return b
}

func (r *groupRig) version(t *testing.T, convID string) int64 {
	t.Helper()
	var v int64
	require.NoError(t, r.db.QueryRow(`SELECT version FROM engram_conversation WHERE conversation_id = $1`, convID).Scan(&v))
	return v
}

// group — разговор: a — owner, остальные — member.
func (r *groupRig) group(t *testing.T, ownerMM string, members ...string) string {
	t.Helper()
	return r.mustCreate(t, ownerMM, createReq(newRequestID(), "g", members...)).Conversation.ConversationID
}

func reqOf(convID string, kv ...any) map[string]any {
	m := map[string]any{"request_id": newRequestID(), "conversation_id": convID}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1]
	}
	return m
}

// C2 — 64 параллельных 903: версия 65, события с версиями 2..65.
func TestConvC2ParallelUpdates(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	var wg sync.WaitGroup
	var mu sync.Mutex
	unavailable := 0
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := reqOf(id, "title", "t"+string(rune('A'+i%26))+string(rune('a'+i/26)))
			for {
				resp := r.do(t, a, ActionConversationUpdate, req)
				if resp.OK || resp.Error != convErrTemporarilyUnavail {
					return
				}
				mu.Lock()
				unavailable++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	require.Equal(t, int64(65), r.version(t, id))
	// Изменения сериализуются блокировкой (§10.4), а не отказами: без неё
	// версию спасает лишь PK engram_event, ценой повторов.
	require.Zero(t, unavailable, "ни один вызов не отвергнут — сериализация ожиданием, а не конфликтом")
	rows, err := r.db.Query(`SELECT version FROM engram_event WHERE conversation_id = $1 AND event->>'event' = 'metadata_changed' ORDER BY version`, id)
	require.NoError(t, err)
	defer rows.Close()
	want := int64(2)
	for rows.Next() {
		var v int64
		require.NoError(t, rows.Scan(&v))
		require.Equal(t, want, v)
		want++
	}
	require.Equal(t, int64(66), want, "64 события с версиями 2..65")
}

// C2b — 64 параллельных 903 с одним expected_version: 1 успех, 63 конфликта.
func TestConvC2bExpectedVersion(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	var mu sync.Mutex
	ok, conflicts := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := reqOf(id, "expected_version", 1, "title", "v"+string(rune('A'+i%26))+string(rune('a'+i/26)))
			for {
				resp := r.do(t, a, ActionConversationUpdate, req)
				if resp.Error == convErrTemporarilyUnavail {
					continue
				}
				mu.Lock()
				if resp.OK {
					ok++
				} else if resp.Error == convErrVersionConflict {
					require.Equal(t, map[string]int64{"conversation_version": 2}, resp.Data)
					conflicts++
				}
				mu.Unlock()
				return
			}
		}(i)
	}
	wg.Wait()
	require.Equal(t, 1, ok)
	require.Equal(t, 63, conflicts)
}

// C4 — 16 параллельных 910 одного участника: один период, версия +1.
func TestConvC4ParallelAddSameMember(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := reqOf(id, "photon_ids", []string{pidB})
			for r.do(t, a, ActionConversationMembersAdd, req).Error == convErrTemporarilyUnavail {
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2`, id, pidB))
	require.Equal(t, int64(2), r.version(t, id))
}

// C5 — add → remove → add: no_access → active; 2 периода; новый период
// открывает его member_added.
func TestConvC5AddRemoveAdd(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a)
	get := func(caller string) Response {
		return r.svc.getConversation(context.Background(), caller, pl(t, map[string]string{"conversation_id": id}))
	}
	var add ConvMembersAddResponse
	require.NoError(t, json.Unmarshal(r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB, pidB, pidA})), &add))
	require.Equal(t, []string{pidB}, add.Added)
	require.Equal(t, []string{pidA}, add.Already)
	require.True(t, get(b).OK)

	var rm ConvChangedResponse
	require.NoError(t, json.Unmarshal(r.mustDo(t, a, ActionConversationMemberRemove, reqOf(id, "photon_id", pidB)), &rm))
	require.True(t, rm.Changed)
	require.Equal(t, convErrNoAccess, get(b).Error)
	require.Equal(t, convErrConversationNF, get(c).Error, "никогда не участвовавший")

	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB}))
	resp := get(b)
	require.True(t, resp.OK)
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2`, id, pidB))
	var openAt, addedAt int64
	require.NoError(t, r.db.QueryRow(`SELECT open_at FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2 AND period_no = 2`, id, pidB).Scan(&openAt))
	require.NoError(t, r.db.QueryRow(`SELECT at FROM engram_event WHERE conversation_id = $1 AND event->>'event' = 'member_added' ORDER BY version DESC LIMIT 1`, id).Scan(&addedAt))
	require.Equal(t, addedAt, openAt)
	require.Equal(t, openAt, resp.Data.(ConvSnapshotResponse).Conversation.Me.JoinedAt)
	// закрытие первого периода: позиция и id поста member_removed
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_member_period p JOIN engram_event e
ON e.conversation_id = p.conversation_id AND e.at = p.close_at AND e.post_id = p.close_post_id
WHERE p.conversation_id = $1 AND p.photon_id = $2 AND p.state = 'removed'`, id, pidB))
}

// 903 без изменения: без события и без роста версии; повтор — тот же ответ;
// тот же ключ с другим содержанием — idempotency_conflict.
func TestConvUpdateNoChangeAndIdempotency(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	req := reqOf(id, "title", "g")
	first := r.mustDo(t, a, ActionConversationUpdate, req)
	require.Equal(t, int64(1), r.version(t, id))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1`, id))
	require.JSONEq(t, string(first), string(r.mustDo(t, a, ActionConversationUpdate, req)))
	req["title"] = "другое"
	require.Equal(t, convErrIdempotency, r.do(t, a, ActionConversationUpdate, req).Error)
	require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationUpdate, reqOf(id, "title", " ")).Error)
}

// 904 и §8.1: архив запрещает изменения; повтор 904 отвечает тем же.
func TestConvArchive(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	require.Equal(t, convErrForbidden, r.do(t, b, ActionConversationArchive, reqOf(id)).Error, "у member нет conversation.archive")
	req := reqOf(id)
	first := r.mustDo(t, a, ActionConversationArchive, req)
	require.Contains(t, string(first), `"state":"archived"`)
	require.JSONEq(t, string(first), string(r.mustDo(t, a, ActionConversationArchive, req)), "повтор после архивации")
	for _, tc := range []struct {
		action  int
		payload map[string]any
	}{
		{ActionConversationUpdate, reqOf(id, "title", "x")},
		{ActionConversationArchive, reqOf(id)},
		{ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC})},
		{ActionConversationLeave, reqOf(id)},
	} {
		require.Equal(t, convErrArchived, r.do(t, a, tc.action, tc.payload).Error, "%d", tc.action)
	}
	require.True(t, r.svc.getConversation(context.Background(), b, pl(t, map[string]string{"conversation_id": id})).OK, "901 в архиве разрешён")
}

// 911 и правило «роль ниже роли вызывающего» (ADR-099 §7.3).
func TestConvRemoveRules(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)
	r.id.person(pidD)
	id := r.group(t, a, pidB, pidC, pidD)
	r.mustDo(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidB, "role", "admin"))
	r.mustDo(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidC, "role", "admin"))

	require.Equal(t, convErrForbidden, r.do(t, b, ActionConversationMemberRemove, reqOf(id, "photon_id", pidC)).Error, "admin не удаляет admin")
	require.Equal(t, convErrForbidden, r.do(t, b, ActionConversationMemberRemove, reqOf(id, "photon_id", pidA)).Error, "owner не удаляется")
	var rm ConvChangedResponse
	require.NoError(t, json.Unmarshal(r.mustDo(t, b, ActionConversationMemberRemove, reqOf(id, "photon_id", pidD)), &rm))
	require.True(t, rm.Changed, "admin удаляет member")
	require.NoError(t, json.Unmarshal(r.mustDo(t, b, ActionConversationMemberRemove, reqOf(id, "photon_id", pidD)), &rm))
	require.False(t, rm.Changed, "не в active — changed:false")
	require.Equal(t, rm.ConversationVersion, r.version(t, id))
	require.Equal(t, errInvalidPhotonID, r.do(t, a, ActionConversationMemberRemove, reqOf(id, "photon_id", "bad")).Error)
	require.NoError(t, json.Unmarshal(r.mustDo(t, a, ActionConversationMemberRemove, reqOf(id, "photon_id", pidC)), &rm))
	require.True(t, rm.Changed, "owner удаляет admin")
}

// 912: выход; владелец при других — owner_transfer_required; единственный
// владелец — member_left и archived одной версией.
func TestConvLeave(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	require.Equal(t, convErrOwnerTransferRequired, r.do(t, a, ActionConversationLeave, reqOf(id)).Error)
	r.mustDo(t, b, ActionConversationLeave, reqOf(id))
	require.Equal(t, convErrNoAccess, r.do(t, b, ActionConversationLeave, reqOf(id)).Error)

	var v ConvVersionResponse
	require.NoError(t, json.Unmarshal(r.mustDo(t, a, ActionConversationLeave, reqOf(id)), &v))
	var state string
	require.NoError(t, r.db.QueryRow(`SELECT state FROM engram_conversation WHERE conversation_id = $1`, id).Scan(&state))
	require.Equal(t, "archived", state)
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND version = $2`, id, v.ConversationVersion), "member_left и archived одной версией")
}

// 920 и 921.
func TestConvRolesAndOwnerTransfer(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)
	id := r.group(t, a, pidB)

	require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidB, "role", "owner")).Error)
	require.Equal(t, convErrForbidden, r.do(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidA, "role", "member")).Error, "owner не затрагивается")
	var ch ConvChangedResponse
	require.NoError(t, json.Unmarshal(r.mustDo(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidB, "role", "admin")), &ch))
	require.True(t, ch.Changed)
	require.NoError(t, json.Unmarshal(r.mustDo(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidB, "role", "admin")), &ch))
	require.False(t, ch.Changed, "та же роль")
	require.Equal(t, convErrForbidden, r.do(t, b, ActionConversationRoleSet, reqOf(id, "photon_id", pidB, "role", "member")).Error, "у admin нет roles.manage")
	resp := r.do(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidB, "role", "member", "expected_version", 1))
	require.Equal(t, convErrVersionConflict, resp.Error)

	require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationOwnerTransfer, reqOf(id, "photon_id", pidC)).Error, "цель не в active")
	var v ConvVersionResponse
	require.NoError(t, json.Unmarshal(r.mustDo(t, a, ActionConversationOwnerTransfer, reqOf(id, "photon_id", pidB)), &v))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2 AND role = 'owner' AND state = 'active'`, id, pidB))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2 AND role = 'admin' AND state = 'active'`, id, pidA))
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND version = $2 AND event->>'event' = 'role_changed'`, id, v.ConversationVersion))
	require.Equal(t, convErrForbidden, r.do(t, a, ActionConversationOwnerTransfer, reqOf(id, "photon_id", pidA)).Error, "прежний владелец больше не owner")
}

// Позиции событий — из H, а не из часов (ADR-101 §5.1): часы отстали, а
// позиция всё равно больше последней выданной. Без H событие встало бы
// раньше уже записанных.
func TestConvChangePositionsFromHWhenClockLags(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a)
	var h int64
	require.NoError(t, r.db.QueryRow(`SELECT position_h FROM engram_conversation WHERE conversation_id = $1`, id).Scan(&h))
	r.svc.now = func() int64 { return 1000 } // часы далеко в прошлом
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB}))
	var at int64
	require.NoError(t, r.db.QueryRow(`SELECT at FROM engram_event WHERE conversation_id = $1 AND version = 2`, id).Scan(&at))
	require.Equal(t, h+1, at)
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_event e1 JOIN engram_event e2
ON e1.conversation_id = e2.conversation_id AND (e1.version, e1.seq) < (e2.version, e2.seq) AND e1.at >= e2.at
WHERE e1.conversation_id = $1`, id), "I3: позиции событий растут вместе с (version, seq)")
}

// Повтор выполненного запроса — результат первого исполнения, независимо от
// текущего доступа (ADR-099 §10.2).
func TestConvRepeatIgnoresCurrentAccess(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	req := reqOf(id)
	first := r.mustDo(t, b, ActionConversationLeave, req)
	require.JSONEq(t, string(first), string(r.mustDo(t, b, ActionConversationLeave, req)), "повтор выхода — тот же ответ, а не no_access")
}

// Изменение зафиксировано, запись события не удалась: ответа нет; повтор
// дописывает событие и возвращает сохранённый ответ.
func TestConvRepeatFlushesPendingEvents(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a)
	req := reqOf(id, "photon_ids", []string{pidB})
	r.mm.mu.Lock()
	r.mm.postFailOnCall = r.mm.postCalls + 1
	r.mm.mu.Unlock()
	require.Equal(t, convErrTemporarilyUnavail, r.do(t, a, ActionConversationMembersAdd, req).Error)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND state = 'pending'`, id))
	r.mustDo(t, a, ActionConversationMembersAdd, req)
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND state = 'pending'`, id))
	require.Equal(t, int64(2), r.version(t, id), "повтор не применил изменение второй раз")
}

// Доступ: незнакомому — not_found, бывшему — no_access (ADR-099 §11).
func TestConvChangeAccess(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB)
	require.Equal(t, convErrConversationNF, r.do(t, c, ActionConversationUpdate, reqOf(id, "title", "x")).Error)
	require.Equal(t, convErrConversationNF, r.do(t, r.id.plainUser(), ActionConversationUpdate, reqOf(id, "title", "x")).Error)
	require.Equal(t, convErrForbidden, r.do(t, b, ActionConversationUpdate, reqOf(id, "title", "x")).Error)
	r.mustDo(t, a, ActionConversationMemberRemove, reqOf(id, "photon_id", pidB))
	require.Equal(t, convErrNoAccess, r.do(t, b, ActionConversationUpdate, reqOf(id, "title", "x")).Error)
	require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationMemberRemove, reqOf(id, "photon_id", pidB, "expected_version", 1)).Error, "у 911 нет expected_version")
}
