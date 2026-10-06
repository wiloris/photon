package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 913–916 против НАСТОЯЩЕГО PostgreSQL (contract §5; ADR-107 этап 1).

type inviteResp struct {
	Invite ConvInvite `json:"invite"`
}

func (r *groupRig) createInviteOK(t *testing.T, mm, convID string, kv ...any) ConvInvite {
	t.Helper()
	var out inviteResp
	require.NoError(t, json.Unmarshal(r.mustDo(t, mm, ActionConversationInviteCreate, reqOf(convID, kv...)), &out))
	require.NotEmpty(t, out.Invite.Token)
	return out.Invite
}

func (r *groupRig) join(t *testing.T, mm, token string) Response {
	t.Helper()
	return r.do(t, mm, ActionConversationJoinByInvite, map[string]any{"request_id": newRequestID(), "token": token})
}

func (r *groupRig) joinOK(t *testing.T, mm, token string) ConvSnapshotResponse {
	t.Helper()
	resp := r.join(t, mm, token)
	require.True(t, resp.OK, "916: %+v", resp)
	var snap ConvSnapshotResponse
	require.NoError(t, json.Unmarshal(resp.Data.(json.RawMessage), &snap))
	return snap
}

func (r *groupRig) events(t *testing.T, convID, name string) int {
	return r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND event->>'event' = $2`, convID, name)
}

// Гонка: два вступающих при max_uses=1 — ровно один.
func TestInviteParallelJoinMaxUsesOne(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	iv := r.createInviteOK(t, a, id, "max_uses", 1)
	const n = 8
	users := make([]string, n)
	for i := range users {
		users[i] = r.id.person(fmt.Sprintf("0000002%02d", i))
	}
	res := make([]Response, n)
	var wg sync.WaitGroup
	for i := range users {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				res[i] = r.join(t, users[i], iv.Token)
				if res[i].Error != convErrTemporarilyUnavail {
					return
				}
			}
		}(i)
	}
	wg.Wait()
	ok, invalid := 0, 0
	for _, x := range res {
		switch {
		case x.OK:
			ok++
		case x.Error == convErrInviteInvalid:
			invalid++
		}
	}
	require.Equal(t, 1, ok)
	require.Equal(t, n-1, invalid)
	require.Equal(t, 1, r.events(t, id, "member_joined"))
	require.Equal(t, 1, r.count(t, `SELECT uses FROM engram_invite WHERE invite_id = $1`, iv.InviteID))
	require.Equal(t, 2, r.count(t, `SELECT member_count FROM engram_conversation WHERE conversation_id = $1`, id))
}

// Все причины недействительности — один код.
func TestInviteInvalidReasonsOneCode(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a)

	require.Equal(t, convErrInviteInvalid, r.join(t, b, "no-such-token").Error)

	revoked := r.createInviteOK(t, a, id)
	r.mustDo(t, a, ActionConversationInviteRevoke, map[string]any{"request_id": newRequestID(), "invite_id": revoked.InviteID})
	require.Equal(t, convErrInviteInvalid, r.join(t, b, revoked.Token).Error)

	expired := r.createInviteOK(t, a, id, "expires_at", time.Now().Add(time.Hour).UnixMilli())
	_, err := r.db.Exec(`UPDATE engram_invite SET expires_at = $2 WHERE invite_id = $1`, expired.InviteID, time.Now().Add(-time.Minute).UnixMilli())
	require.NoError(t, err)
	require.Equal(t, convErrInviteInvalid, r.join(t, b, expired.Token).Error)

	used := r.createInviteOK(t, a, id, "max_uses", 1)
	r.joinOK(t, r.id.person(pidC), used.Token)
	require.Equal(t, convErrInviteInvalid, r.join(t, b, used.Token).Error)

	require.Equal(t, 1, r.events(t, id, "member_joined"))
}

// Токен не попадает в журнал; в таблице только хэш.
func TestInviteTokenNeverLoggedOnlyHashStored(t *testing.T) {
	r := newGroupRig(t, 2000)
	var mu sync.Mutex
	var logs []string
	r.svc.logger = func(msg string, kv ...any) {
		mu.Lock()
		defer mu.Unlock()
		logs = append(logs, msg+fmt.Sprint(kv...))
	}
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a)
	iv := r.createInviteOK(t, a, id)
	// Сбойный вызов: отменённый контекст заставляет 916 журналировать ошибку.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp := r.svc.dispatch(ctx, b, ActionConversationJoinByInvite, pl(t, map[string]any{"request_id": newRequestID(), "token": iv.Token}))
	require.Equal(t, convErrTemporarilyUnavail, resp.Error)
	r.joinOK(t, b, iv.Token)
	require.NotEmpty(t, logs, "сбойный вызов обязан журналировать — иначе проверка ничего не измеряет")
	for _, l := range logs {
		require.NotContains(t, l, iv.Token)
	}
	var stored []byte
	require.NoError(t, r.db.QueryRow(`SELECT token_hash FROM engram_invite WHERE invite_id = $1`, iv.InviteID).Scan(&stored))
	require.Equal(t, inviteTokenHash(iv.Token), stored)
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_invite WHERE convert_from(token_hash, 'SQL_ASCII') = $1`, iv.Token))
	// 913 хранит токен в ответе ключа по замыслу; всё остальное — нет.
	require.Zero(t, r.count(t, `SELECT count(*) FROM engram_event e WHERE e::text LIKE $1`, "%"+iv.Token+"%"))
	require.Zero(t, r.count(t, `SELECT count(*) FROM engram_invite i WHERE i::text LIKE $1`, "%"+iv.Token+"%"))
	require.Zero(t, r.count(t, `SELECT count(*) FROM engram_request q WHERE q.action = $2 AND q::text LIKE $1`, "%"+iv.Token+"%", ActionConversationJoinByInvite))
}

// since_join: вступивший не видит прежнего; видит с member_joined.
func TestInviteSinceJoinVisibility(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a)
	require.True(t, r.send(t, a, id, newRequestID(), "before").OK)
	iv := r.createInviteOK(t, a, id)
	r.joinOK(t, b, iv.Token)
	require.True(t, r.send(t, a, id, newRequestID(), "after").OK)
	page := r.syncPage(t, b, id, "", 0)
	var texts []string
	var kinds []string
	for _, it := range page.Items {
		switch v := it.(type) {
		case ConvMessageItem:
			texts = append(texts, v.Text)
		case ConvEventItem:
			kinds = append(kinds, v.Event)
			if v.Event == "member_joined" {
				require.Equal(t, iv.InviteID, v.InviteID)
				require.Equal(t, pidB, v.ActorPhotonID)
			}
		}
	}
	require.Equal(t, []string{"after"}, texts)
	require.Equal(t, []string{"member_joined"}, kinds)
}

func TestInviteCreateRepeatSameTokenAndListHasNoTokens(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	req := reqOf(id, "max_uses", 3)
	var x, y inviteResp
	require.NoError(t, json.Unmarshal(r.mustDo(t, a, ActionConversationInviteCreate, req), &x))
	require.NoError(t, json.Unmarshal(r.mustDo(t, a, ActionConversationInviteCreate, req), &y))
	require.Equal(t, x.Invite.Token, y.Invite.Token)
	require.Equal(t, x.Invite.InviteID, y.Invite.InviteID)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_invite WHERE conversation_id = $1`, id))
	require.Equal(t, int64(1), int64(r.version(t, id)), "создание приглашения не меняет версию")
	require.Zero(t, r.events(t, id, "member_joined"))

	other := r.createInviteOK(t, a, id)
	resp := r.do(t, a, ActionConversationInviteList, map[string]any{"conversation_id": id})
	require.True(t, resp.OK)
	b, err := json.Marshal(resp.Data)
	require.NoError(t, err)
	require.NotContains(t, string(b), "token")
	require.NotContains(t, string(b), x.Invite.Token)
	require.NotContains(t, string(b), other.Token)
	var list struct {
		Invites []ConvInvite `json:"invites"`
	}
	require.NoError(t, json.Unmarshal(b, &list))
	require.Len(t, list.Invites, 2)

	// отозванное и исчерпанное в списке не показываются
	r.mustDo(t, a, ActionConversationInviteRevoke, map[string]any{"request_id": newRequestID(), "invite_id": other.InviteID})
	_, err = r.db.Exec(`UPDATE engram_invite SET uses = 3 WHERE invite_id = $1`, x.Invite.InviteID)
	require.NoError(t, err)
	resp = r.do(t, a, ActionConversationInviteList, map[string]any{"conversation_id": id})
	b, _ = json.Marshal(resp.Data)
	require.JSONEq(t, `{"invites":[]}`, string(b))

	// повтор отзыва — тот же ответ
	rv := map[string]any{"request_id": newRequestID(), "invite_id": other.InviteID}
	require.JSONEq(t, string(r.mustDo(t, a, ActionConversationInviteRevoke, rv)), string(r.mustDo(t, a, ActionConversationInviteRevoke, rv)))
}

func TestInviteRequireMembersAdd(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB) // b — member: нет members.add
	iv := r.createInviteOK(t, a, id)
	require.Equal(t, convErrForbidden, r.do(t, b, ActionConversationInviteCreate, reqOf(id)).Error)
	require.Equal(t, convErrForbidden, r.do(t, b, ActionConversationInviteRevoke, map[string]any{"request_id": newRequestID(), "invite_id": iv.InviteID}).Error)
	require.Equal(t, convErrForbidden, r.do(t, b, ActionConversationInviteList, map[string]any{"conversation_id": id}).Error)
	// не участник: conversation_not_found (в т.ч. чужое приглашение)
	out := r.id.person(pidC)
	require.Equal(t, convErrConversationNF, r.do(t, out, ActionConversationInviteCreate, reqOf(id)).Error)
	require.Equal(t, convErrConversationNF, r.do(t, out, ActionConversationInviteRevoke, map[string]any{"request_id": newRequestID(), "invite_id": iv.InviteID}).Error)
	require.Equal(t, convErrConversationNF, r.do(t, out, ActionConversationInviteList, map[string]any{"conversation_id": id}).Error)
	require.Equal(t, convErrConversationNF, r.do(t, a, ActionConversationInviteRevoke, map[string]any{"request_id": newRequestID(), "invite_id": "unknown"}).Error)
	// содержание
	require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationInviteCreate, reqOf(id, "max_uses", 0)).Error)
	require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationInviteCreate, reqOf(id, "expires_at", time.Now().Add(-time.Hour).UnixMilli())).Error)
	// после права admin — можно
	r.mustDo(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidB, "role", "admin"))
	r.createInviteOK(t, b, id)
}

func TestInviteJoinArchivedAndAlreadyMember(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	iv := r.createInviteOK(t, a, id, "max_uses", 5)
	v := r.version(t, id)
	// уже участник: снимок, без события, uses не растёт
	snap := r.joinOK(t, b, iv.Token)
	require.Equal(t, id, snap.Conversation.ConversationID)
	require.Equal(t, v, r.version(t, id))
	require.Zero(t, r.events(t, id, "member_joined"))
	require.Zero(t, r.count(t, `SELECT uses FROM engram_invite WHERE invite_id = $1`, iv.InviteID))

	r.mustDo(t, a, ActionConversationArchive, reqOf(id))
	c := r.id.person(pidC)
	require.Equal(t, convErrArchived, r.join(t, c, iv.Token).Error)
	require.Zero(t, r.count(t, `SELECT uses FROM engram_invite WHERE invite_id = $1`, iv.InviteID))
}

func TestInviteJoinRepeatSameRequestAnswersStored(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a)
	iv := r.createInviteOK(t, a, id, "max_uses", 1)
	req := map[string]any{"request_id": newRequestID(), "token": iv.Token}
	first := r.do(t, b, ActionConversationJoinByInvite, req)
	require.True(t, first.OK)
	again := r.do(t, b, ActionConversationJoinByInvite, req) // приглашение уже исчерпано
	require.True(t, again.OK)
	require.Equal(t, 1, r.events(t, id, "member_joined"))
	req2 := map[string]any{"request_id": req["request_id"], "token": "other"}
	require.Equal(t, convErrIdempotency, r.do(t, b, ActionConversationJoinByInvite, req2).Error)
}

func TestInviteJoinRateLimit(t *testing.T) {
	r := newGroupRig(t, 2000)
	b := r.id.person(pidB)
	for i := 0; i < joinAttemptsPerWindow; i++ {
		require.Equal(t, convErrInviteInvalid, r.join(t, b, "bad").Error, i)
	}
	require.Equal(t, convErrTemporarilyUnavail, r.join(t, b, "bad").Error)
	// другой пользователь не затронут
	require.Equal(t, convErrInviteInvalid, r.join(t, r.id.person(pidC), "bad").Error)
}

func TestInviteOnDirectIsInvalidRequest(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	resp := r.do(t, a, ActionConversationOpenDirect, map[string]any{"request_id": newRequestID(), "peer_photon_id": pidB})
	require.True(t, resp.OK, "%+v", resp)
	var snap ConvOpenDirectResponse
	require.NoError(t, json.Unmarshal(resp.Data.(json.RawMessage), &snap))
	id := snap.Conversation.ConversationID
	require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationInviteCreate, reqOf(id)).Error)
	require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationInviteList, map[string]any{"conversation_id": id}).Error)
}
