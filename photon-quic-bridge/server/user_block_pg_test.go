package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// 220–222 USER_BLOCK / USER_UNBLOCK / USER_BLOCK_LIST и их последствия против
// НАСТОЯЩЕГО PostgreSQL (ADR-111 §2.1–2.2, Q1, Q2; contract §5b.4).

func (r *groupRig) block(t *testing.T, caller string, action int, payload map[string]any) Response {
	t.Helper()
	return r.do(t, caller, action, payload)
}

func (r *groupRig) blockOK(t *testing.T, caller, target string) {
	t.Helper()
	resp := r.block(t, caller, ActionUserBlock, map[string]any{"photon_id": target})
	require.True(t, resp.OK, "220: %+v", resp)
	require.Equal(t, ConvUserBlockResponse{Blocked: true}, resp.Data)
}

func (r *groupRig) blockList(t *testing.T, caller string) []string {
	t.Helper()
	resp := r.block(t, caller, ActionUserBlockList, map[string]any{})
	require.True(t, resp.OK, "222: %+v", resp)
	return resp.Data.(ConvUserBlockListResponse).Blocked
}

func TestBlock220Validation(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	plain := r.id.plainUser()
	for _, action := range []int{ActionUserBlock, ActionUserUnblock} {
		require.Equal(t, errInvalidPhotonID, r.block(t, a, action, map[string]any{"photon_id": "abc"}).Error, "формат, %d", action)
		require.Equal(t, errInvalidPhotonID, r.block(t, a, action, map[string]any{}).Error, "нет поля, %d", action)
		require.Equal(t, errInvalidPhotonID, r.block(t, a, action, map[string]any{"photon_id": pidB, "x": 1}).Error, "лишнее поле, %d", action)
		require.Equal(t, convErrInvalidRequest, r.block(t, a, action, map[string]any{"photon_id": pidA}).Error, "свой PhotonID, %d", action)
		require.Equal(t, errPhotonIDNotFound, r.block(t, a, action, map[string]any{"photon_id": "100000999"}).Error, "незарегистрированный, %d", action)
		require.Equal(t, convErrForbidden, r.block(t, plain, action, map[string]any{"photon_id": pidB}).Error, "вызывающий без PhotonID, %d", action)
	}
	require.Equal(t, convErrInvalidRequest, r.block(t, a, ActionUserBlockList, map[string]any{"x": 1}).Error)
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_user_block`), "отказы ничего не пишут")
}

func TestBlock220IdempotentAndListIsPersonal(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)

	require.Equal(t, []string{}, r.blockList(t, a), "пустой список — [], не null")
	r.blockOK(t, a, pidB)
	r.blockOK(t, a, pidB) // повтор без ключа
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_user_block`))
	r.blockOK(t, a, pidC)
	require.Equal(t, []string{pidB, pidC}, r.blockList(t, a), "в порядке постановки")
	require.Equal(t, []string{}, r.blockList(t, b), "блокировка видна только блокирующему")

	for i := 0; i < 2; i++ {
		resp := r.block(t, a, ActionUserUnblock, map[string]any{"photon_id": pidB})
		require.True(t, resp.OK, "221: %+v", resp)
		require.Equal(t, ConvUserBlockResponse{Blocked: false}, resp.Data)
	}
	require.Equal(t, []string{pidC}, r.blockList(t, a))
	// Снятие несуществующей блокировки — тоже успех.
	require.True(t, r.block(t, b, ActionUserUnblock, map[string]any{"photon_id": pidA}).OK)
}

// Через UserHandler: сотня 2xx доходит до сервиса разговоров; без сервиса —
// temporarily_unavailable, как 9xx.
func TestBlockUserHandlerRoutesToService(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	h := &UserHandler{conv: r.svc}
	resp := h.Handle(a, ActionUserBlock, pl(t, map[string]any{"photon_id": pidB}))
	require.True(t, resp.OK, "%+v", resp)
	resp = h.Handle(a, ActionUserBlockList, pl(t, map[string]any{}))
	require.True(t, resp.OK)
	require.Equal(t, []string{pidB}, resp.Data.(ConvUserBlockListResponse).Blocked)
	for _, action := range []int{ActionUserBlock, ActionUserUnblock, ActionUserBlockList} {
		require.Equal(t, errCodeTemporarilyUnavailable, (&UserHandler{}).Handle(a, action, pl(t, map[string]any{})).Error)
	}
}

// Блокировка двусторонне закрывает direct (§2.2); B не отличает её от
// отсутствия разговора; A получает blocked; история видна обоим.
func TestBlockClosesDirectBothWays(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	conv := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID
	r.mustSend(t, b, conv, newRequestID(), "до блокировки")

	r.blockOK(t, a, pidB)

	// B → conversation_not_found, неотличимо от несуществующего разговора.
	got := r.send(t, b, conv, newRequestID(), "после")
	require.False(t, got.OK)
	require.Equal(t, convErrConversationNF, got.Error)
	missing := r.send(t, b, "no-such-conversation", newRequestID(), "после")
	require.Equal(t, missing.Error, got.Error, "блокировка неотличима от отсутствия разговора")
	require.Equal(t, missing.Data, got.Data)
	// A, заблокировавший сам, получает отдельный код.
	require.Equal(t, convErrBlocked, r.send(t, a, conv, newRequestID(), "а я?").Error)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND item_kind = 'message'`, conv),
		"после блокировки не появилось ни одной позиции")

	// История видна обоим как была (931).
	require.Equal(t, []string{"event:created", "event:member_added", "msg:до блокировки"}, names(r.fullPass(t, a, conv, 20)))
	require.Equal(t, []string{"event:created", "event:member_added", "msg:до блокировки"}, names(r.fullPass(t, b, conv, 20)))

	// 905 от B — conversation_not_found при существующей паре.
	require.Equal(t, convErrConversationNF, r.openDirect(t, b, pidA).Error)

	// Снятие блокировки возвращает отправку обоим.
	require.True(t, r.block(t, a, ActionUserUnblock, map[string]any{"photon_id": pidB}).OK)
	r.mustSend(t, b, conv, newRequestID(), "снова")
	r.mustSend(t, a, conv, newRequestID(), "и я")
}

// Взаимная блокировка не раскрывает причину тому, кого заблокировали первым.
func TestBlockMutualDoesNotDiscloseToBlocked(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	conv := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID
	r.blockOK(t, a, pidB)
	r.blockOK(t, b, pidA)
	require.Equal(t, convErrConversationNF, r.send(t, a, conv, newRequestID(), "x").Error)
	require.Equal(t, convErrConversationNF, r.send(t, b, conv, newRequestID(), "x").Error)
}

// 905 от B при блокировке A → conversation_not_found и без существующей пары;
// пара и разговор не создаются.
func TestBlock905WithoutPair(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.blockOK(t, a, pidB)
	resp := r.openDirect(t, b, pidA)
	require.False(t, resp.OK)
	require.Equal(t, convErrConversationNF, resp.Error)
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_direct_pair`))
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_conversation`))
	// Блокировка направленная: A открывает с B (A знает, что заблокировал).
	require.True(t, r.openDirect(t, a, pidB).OK)
}

// 910 по заблокировавшему неотличим от несуществующего (§2.2, ADR-111 §4.1).
func TestBlock910AddingBlockerLooksNonexistent(t *testing.T) {
	r := newGroupRig(t, 2000)
	c := r.id.person(pidC)
	b := r.id.person(pidB)
	a := r.id.person(pidA)
	r.id.person(pidD)
	id := r.group(t, c, pidB)
	r.mustDo(t, c, ActionConversationRoleSet, reqOf(id, "photon_id", pidB, "role", "admin"))

	r.blockOK(t, a, pidB)
	blocked := r.do(t, b, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidD, pidA}))
	missing := r.do(t, b, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidD, "100000999"}))
	require.False(t, blocked.OK)
	require.Equal(t, errPhotonIDNotFound, blocked.Error)
	require.Equal(t, missing.Error, blocked.Error)
	require.Equal(t, missing.Data, blocked.Data)
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_member_period WHERE conversation_id = $1 AND photon_id IN ($2, $3)`, id, pidA, pidD),
		"отказ целиком: pidD тоже не добавлен")
	require.Equal(t, int64(2), r.version(t, id)) // 900 + 920

	// Не заблокировавший вызывающего (владелец) добавляет A без помех.
	r.mustDo(t, c, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidA}))
}

// Q1: в общих группах сообщения заблокированного видны блокирующему —
// 931, 940 и 941 их не фильтруют; отправка в группе не меняется.
func TestBlockGroupMessagesStayVisible(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	r.mustSend(t, b, id, newRequestID(), "до")
	r.blockOK(t, a, pidB)
	last := r.mustSend(t, b, id, newRequestID(), "после блокировки")
	r.mustSend(t, a, id, newRequestID(), "ответ")

	pass := r.fullPass(t, a, id, 20)
	require.Equal(t, []string{"event:created", "event:member_added", "msg:до", "msg:после блокировки", "msg:ответ"}, names(pass))
	hist := r.histBefore(t, a, id, pass[len(pass)-1], 0)
	require.Contains(t, names(hist.Items), "msg:после блокировки")
	require.Equal(t, []string{"после блокировки"}, hitTexts(t, r.search(t, a, "query", "после")))
	require.Equal(t, last.ID, hitIDs(r.search(t, a, "query", "после"))[0])
}
