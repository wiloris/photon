package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// 919 CONVERSATION_MEMBER_BAN против НАСТОЯЩЕГО PostgreSQL (ADR-111 §2.3;
// contract §5b.4).

func banReq(convID, pid string, ban bool) map[string]any {
	return reqOf(convID, "photon_id", pid, "ban", ban)
}

func (r *groupRig) banState(t *testing.T, convID, pid string) (periods int, lastState string, banned bool) {
	t.Helper()
	periods = r.count(t, `SELECT count(*) FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2`, convID, pid)
	if periods > 0 {
		require.NoError(t, r.db.QueryRow(`SELECT state FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2
ORDER BY period_no DESC LIMIT 1`, convID, pid).Scan(&lastState))
	}
	banned = r.count(t, `SELECT count(*) FROM engram_conversation_ban WHERE conversation_id = $1 AND photon_id = $2`, convID, pid) == 1
	return
}

func TestBan919ClosesPeriodAsBanned(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)
	id := r.group(t, a, pidB, pidC)
	before := r.snapshot(t, a, id)

	resp := r.do(t, a, ActionConversationMemberBan, banReq(id, pidB, true))
	require.True(t, resp.OK, "919: %+v", resp)
	snap := r.snapshot(t, a, id)
	require.Equal(t, before.ConversationVersion+1, snap.ConversationVersion)
	require.Equal(t, before.MemberCount-1, snap.MemberCount)

	periods, state, banned := r.banState(t, id, pidB)
	require.Equal(t, 1, periods)
	require.Equal(t, "banned", state)
	require.True(t, banned)
	var closedVersion, closeAt int64
	require.NoError(t, r.db.QueryRow(`SELECT closed_version, close_at FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2`, id, pidB).
		Scan(&closedVersion, &closeAt))
	require.Equal(t, snap.ConversationVersion, closedVersion)
	require.NotZero(t, closeAt)

	// Событие member_banned с actor/target, версия события — версия изменения.
	var actor, target string
	var evVersion int64
	require.NoError(t, r.db.QueryRow(`SELECT event->>'actor_photon_id', event->>'target_photon_id', version FROM engram_event
WHERE conversation_id = $1 AND event->>'event' = 'member_banned'`, id).Scan(&actor, &target, &evVersion))
	require.Equal(t, pidA, actor)
	require.Equal(t, pidB, target)
	require.Equal(t, snap.ConversationVersion, evVersion)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND event->>'event' = 'member_banned' AND state = 'written'`, id))
	feed := names(r.fullPass(t, a, id, 20))
	require.Equal(t, "event:member_banned", feed[len(feed)-1])

	// Забаненный доступа к разговору больше не имеет.
	require.Equal(t, convErrNoAccess, r.do(t, b, ActionConversationGet, map[string]any{"conversation_id": id}).Error)
	require.Equal(t, convErrNoAccess, r.send(t, b, id, newRequestID(), "x").Error)
}

func TestBan919RejoinRejected(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)
	priv := r.group(t, a, pidB)
	pub := r.publicGroup(t, a, "pub")
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(pub, "photon_ids", []string{pidB}))
	privInvite := r.createInviteOK(t, a, priv)
	pubInvite := r.createInviteOK(t, a, pub)

	r.mustDo(t, a, ActionConversationMemberBan, banReq(priv, pidB, true))
	r.mustDo(t, a, ActionConversationMemberBan, banReq(pub, pidB, true))

	// 916: по ссылке — тот же код, что у прочих причин.
	require.Equal(t, convErrInviteInvalid, r.join(t, b, privInvite.Token).Error)
	require.Equal(t, convErrInviteInvalid, r.join(t, b, pubInvite.Token).Error)
	// 918: каталог — как закрытый разговор.
	require.Equal(t, convErrConversationNF, r.joinPublic(t, b, pub).Error)
	// 910: администратор знает о бане.
	require.Equal(t, convErrForbidden, r.do(t, a, ActionConversationMembersAdd, reqOf(priv, "photon_ids", []string{pidB})).Error)
	require.Equal(t, convErrForbidden, r.do(t, a, ActionConversationMembersAdd, reqOf(priv, "photon_ids", []string{pidC, pidB})).Error)
	periods, state, _ := r.banState(t, priv, pidB)
	require.Equal(t, 1, periods, "новых периодов нет")
	require.Equal(t, "banned", state)
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2`, priv, pidC),
		"910 отвергнут целиком")
	// Приглашение не израсходовано отказом.
	require.Equal(t, 0, r.count(t, `SELECT sum(uses) FROM engram_invite WHERE conversation_id = $1`, priv))
}

func TestBan919UnbanDoesNotReadd(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	inv := r.createInviteOK(t, a, id)
	r.mustDo(t, a, ActionConversationMemberBan, banReq(id, pidB, true))
	banned := r.snapshot(t, a, id)

	require.True(t, r.do(t, a, ActionConversationMemberBan, banReq(id, pidB, false)).OK)
	after := r.snapshot(t, a, id)
	require.Equal(t, banned.ConversationVersion, after.ConversationVersion, "снятие бана версию не меняет")
	require.Equal(t, banned.MemberCount, after.MemberCount)
	require.Equal(t, 1, r.events(t, id, "member_banned"))
	periods, state, isBanned := r.banState(t, id, pidB)
	require.Equal(t, 1, periods)
	require.Equal(t, "banned", state, "период остаётся закрытым")
	require.False(t, isBanned)
	require.Equal(t, convErrNoAccess, r.do(t, b, ActionConversationGet, map[string]any{"conversation_id": id}).Error)
	require.Equal(t, convErrNoAccess, r.send(t, b, id, newRequestID(), "x").Error)
	for _, m := range r.snapshotMembers(t, a, id) {
		require.NotEqual(t, pidB, m.PhotonID)
	}

	// После снятия бана вернуться можно обычными путями — уже новым периодом.
	require.True(t, r.join(t, b, inv.Token).OK)
	periods, state, _ = r.banState(t, id, pidB)
	require.Equal(t, 2, periods)
	require.Equal(t, "active", state)
}

func (r *groupRig) snapshotMembers(t *testing.T, caller, convID string) []ConvMember {
	t.Helper()
	resp := r.do(t, caller, ActionConversationGet, map[string]any{"conversation_id": convID})
	require.True(t, resp.OK)
	return resp.Data.(ConvSnapshotResponse).Members
}

// Бан не участника: пишется только запись, без события и версии; повторный
// бан того же — то же; вход закрыт.
func TestBan919NonMember(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.publicGroup(t, a, "pub")
	v0 := r.version(t, id)
	for i := 0; i < 2; i++ {
		require.True(t, r.do(t, a, ActionConversationMemberBan, banReq(id, pidB, true)).OK)
	}
	require.Equal(t, v0, r.version(t, id))
	require.Equal(t, 0, r.events(t, id, "member_banned"))
	periods, _, banned := r.banState(t, id, pidB)
	require.Equal(t, 0, periods)
	require.True(t, banned)
	require.Equal(t, convErrConversationNF, r.joinPublic(t, b, id).Error)
	// Бан не зарегистрированного вообще PhotonID допустим (цель — только строка).
	require.True(t, r.do(t, a, ActionConversationMemberBan, banReq(id, "100000999", true)).OK)
}

// «Роль ниже» (ADR-099 §7.3): admin не банит owner и admin; owner не
// банится никем; member права не имеет.
func TestBan919RoleBelow(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)
	d := r.id.person(pidD)
	id := r.group(t, a, pidB, pidC, pidD)
	r.mustDo(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidB, "role", "admin"))
	r.mustDo(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidC, "role", "admin"))
	ban := func(caller, target string) Response {
		return r.do(t, caller, ActionConversationMemberBan, banReq(id, target, true))
	}
	require.Equal(t, convErrForbidden, ban(b, pidA).Error, "admin → owner")
	require.Equal(t, convErrForbidden, ban(b, pidC).Error, "admin → admin")
	require.Equal(t, convErrForbidden, ban(a, pidA).Error, "owner не банится")
	require.Equal(t, convErrForbidden, ban(d, pidC).Error, "member не имеет members.remove")
	for _, pid := range []string{pidA, pidB, pidC} {
		_, state, banned := r.banState(t, id, pid)
		require.Equal(t, "active", state)
		require.False(t, banned, "отказ не пишет бан: %s", pid)
	}
	require.True(t, ban(b, pidD).OK, "admin → member")
	require.True(t, ban(a, pidB).OK, "owner → admin")
}

func TestBan919IdempotencyKey(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	req := banReq(id, pidB, true)
	first := r.mustDo(t, a, ActionConversationMemberBan, req)
	v := r.version(t, id)
	again := r.mustDo(t, a, ActionConversationMemberBan, req)
	require.JSONEq(t, string(first), string(again), "повтор — сохранённый ответ")
	require.Equal(t, v, r.version(t, id), "повтор версию не растит")
	require.Equal(t, 1, r.events(t, id, "member_banned"))
	// Тот же ключ с другим содержанием.
	other := map[string]any{"request_id": req["request_id"], "conversation_id": id, "photon_id": pidB, "ban": false}
	require.Equal(t, convErrIdempotency, r.do(t, a, ActionConversationMemberBan, other).Error)
}

func TestBan919ValidationAndDirect(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	bad := func(m map[string]any) string { return r.do(t, a, ActionConversationMemberBan, m).Error }
	require.Equal(t, convErrInvalidRequest, bad(reqOf(id, "photon_id", pidB)), "нет ban")
	require.Equal(t, convErrInvalidRequest, bad(reqOf(id, "photon_id", pidB, "ban", "yes")))
	require.Equal(t, convErrInvalidRequest, bad(map[string]any{"request_id": "x", "conversation_id": id, "photon_id": pidB, "ban": true}))
	require.Equal(t, convErrInvalidRequest, bad(reqOf(id, "ban", true)), "нет photon_id")
	require.Equal(t, convErrInvalidRequest, bad(reqOf(id, "photon_id", pidB, "ban", true, "expected_version", 1)))
	require.Equal(t, errInvalidPhotonID, bad(banReq(id, "abc", true)))
	require.Equal(t, convErrConversationNF, bad(banReq("no-such", pidB, true)))

	dm := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID
	require.Equal(t, convErrInvalidRequest, bad(banReq(dm, pidB, true)), "direct")
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_conversation_ban`))
}

// since_join забаненного закрыт на позиции события бана: после снятия бана и
// возвращения он видит новый период, но не то, что было между.
func TestBan919SinceJoinClosedAtBan(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	s := func(text string) { r.mustSend(t, a, id, newRequestID(), text) }
	s("x1")
	r.mustDo(t, a, ActionConversationMemberBan, banReq(id, pidB, true))
	s("x2")
	r.mustDo(t, a, ActionConversationMemberBan, banReq(id, pidB, false))
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB}))
	s("x3")

	require.Equal(t, []string{"event:created", "event:member_added", "msg:x1", "event:member_banned", "event:member_added", "msg:x3"},
		names(r.fullPass(t, b, id, 20)), "x2 вне периодов забаненного")
	require.Contains(t, names(r.fullPass(t, a, id, 20)), "msg:x2")
}
