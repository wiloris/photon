package main

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// M11: 934 REACTION_SET против НАСТОЯЩЕГО PostgreSQL (ADR-109 §2.3; решение
// владельца 2026-10-06; messaging-v2-contract.md §5b.5). PHOTON_DM_PG_DSN.

// Набор владельца — по кодовым точкам, независимо от константы Bridge.
var ownerReactions = []string{
	"\U0001F44D", "❤️", "\U0001F602", "\U0001F62E", "\U0001F622", "\U0001F621", "\U0001F389", "\U0001F44E",
}

func reactReq(convID, itemID, reaction string, on bool) map[string]any {
	return map[string]any{"conversation_id": convID, "item_id": itemID, "reaction": reaction, "on": on}
}

func (r *groupRig) react(t *testing.T, caller, convID, itemID, reaction string, on bool) Response {
	t.Helper()
	return r.do(t, caller, ActionConversationReactionSet, reactReq(convID, itemID, reaction, on))
}

func (r *groupRig) mustReact(t *testing.T, caller, convID, itemID, reaction string, on bool) bool {
	t.Helper()
	resp := r.react(t, caller, convID, itemID, reaction, on)
	require.True(t, resp.OK, "934: %+v", resp)
	var out ConvReactionResponse
	require.NoError(t, json.Unmarshal(resp.Data.(json.RawMessage), &out))
	return out.Changed
}

func reactionEvents(items []any) []ConvEventItem { return eventsOf(items, "reaction_changed") }

// Таблица Bridge равна таблице владельца: ровно восемь, в порядке владельца.
func TestM11ReactionTableIsOwnersSet(t *testing.T) {
	require.Equal(t, ownerReactions, convReactionSet[:])
}

// Каждая из восьми принимается; прочие — invalid_request и ничего не пишут.
func TestM11AllowedSetOnly(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "m")

	for _, e := range ownerReactions {
		require.True(t, r.mustReact(t, a, id, m.ID, e, true), "%q", e)
	}
	require.Equal(t, 8, r.count(t, `SELECT count(*) FROM engram_reaction WHERE item_id = $1`, m.ID))
	for _, bad := range []string{
		"❤",                    // ❤ без U+FE0F
		"\U0001F44D\U0001F3FD", // 👍🏽
		"\U0001F44F",           // 👏 — девятый
		"❤️️", "❤️ ", " \U0001F44D", "like", "", "\U0001F44D\U0001F44D",
	} {
		resp := r.react(t, a, id, m.ID, bad, true)
		require.False(t, resp.OK, "%q", bad)
		require.Equal(t, convErrInvalidRequest, resp.Error, "%q", bad)
		require.Equal(t, convErrInvalidRequest, r.react(t, a, id, m.ID, bad, false).Error, "%q", bad)
	}
	require.Equal(t, 8, r.count(t, `SELECT count(*) FROM engram_reaction WHERE item_id = $1`, m.ID))
	require.Len(t, reactionEvents(r.fullPass(t, a, id, 50)), 8)
}

// Форма запроса: нет on, лишнее поле, пустые id — invalid_request.
func TestM11RequestShape(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "m")
	for _, bad := range []map[string]any{
		{"conversation_id": id, "item_id": m.ID, "reaction": ownerReactions[0]},                                           // нет on
		{"conversation_id": id, "item_id": m.ID, "reaction": ownerReactions[0], "on": true, "request_id": newRequestID()}, // лишнее поле
		{"conversation_id": id, "item_id": "", "reaction": ownerReactions[0], "on": true},
		{"conversation_id": "", "item_id": m.ID, "reaction": ownerReactions[0], "on": true},
		{"conversation_id": id, "item_id": m.ID, "reaction": ownerReactions[0], "on": "yes"},
	} {
		require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationReactionSet, bad).Error, "%v", bad)
	}
}

// Включить/выключить — два события; повтор — без события; события несут
// target_id, reaction, on (в том числе on=false), actor; версия разговора
// не меняется; ключ идемпотентности не пишется.
func TestM11SetUnsetAndRepeat(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "m")
	v0 := r.version(t, id)
	heart := ownerReactions[1]

	require.True(t, r.mustReact(t, b, id, m.ID, heart, true))
	require.False(t, r.mustReact(t, b, id, m.ID, heart, true), "повтор — тот же итог")
	require.Len(t, reactionEvents(r.fullPass(t, a, id, 50)), 1, "повтор не даёт события")
	require.True(t, r.mustReact(t, b, id, m.ID, heart, false))
	require.False(t, r.mustReact(t, b, id, m.ID, heart, false), "снять снятое — без события")
	require.False(t, r.mustReact(t, b, id, m.ID, ownerReactions[0], false), "снять не поставленное — без события")

	for _, who := range []string{a, b} {
		evs := reactionEvents(r.fullPass(t, who, id, 50))
		require.Len(t, evs, 2)
		require.Less(t, evs[0].CreatedAt, evs[1].CreatedAt)
		require.Equal(t, m.ID, evs[0].TargetID)
		require.Equal(t, heart, evs[0].Reaction)
		require.Equal(t, pidB, evs[0].ActorPhotonID)
		require.NotNil(t, evs[0].On)
		require.True(t, *evs[0].On)
		require.NotNil(t, evs[1].On)
		require.False(t, *evs[1].On, "on=false входит в событие")
		require.Equal(t, v0, evs[0].ConversationVersion)
	}
	raw, _ := json.Marshal(reactionEvents(r.fullPass(t, a, id, 50))[1])
	require.Contains(t, string(raw), `"on":false`)
	require.Equal(t, v0, r.version(t, id), "реакция версию разговора не меняет")
	require.Zero(t, r.count(t, `SELECT count(*) FROM engram_request WHERE action = $1`, ActionConversationReactionSet), "без ключа")
	require.Zero(t, r.count(t, `SELECT count(*) FROM engram_reaction WHERE item_id = $1`, m.ID))
}

// Реакции разных участников и разных эмодзи независимы.
func TestM11IndependentPerParticipantAndEmoji(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "m")
	up := ownerReactions[0]
	require.True(t, r.mustReact(t, a, id, m.ID, up, true))
	require.True(t, r.mustReact(t, b, id, m.ID, up, true))
	require.True(t, r.mustReact(t, b, id, m.ID, ownerReactions[7], true))
	require.True(t, r.mustReact(t, a, id, m.ID, up, false))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_reaction WHERE item_id = $1 AND photon_id = $2 AND reaction = $3`, m.ID, pidB, up))
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM engram_reaction WHERE item_id = $1`, m.ID))
	require.Len(t, reactionEvents(r.fullPass(t, a, id, 50)), 4)
}

// Гонка двух одинаковых 934 одного участника — одно событие, один changed.
func TestM11ParallelIdenticalOneEvent(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "m")
	const n = 8
	var wg sync.WaitGroup
	changed := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			changed[i] = r.mustReact(t, a, id, m.ID, ownerReactions[2], true)
		}(i)
	}
	wg.Wait()
	cnt := 0
	for _, c := range changed {
		if c {
			cnt++
		}
	}
	require.Equal(t, 1, cnt, "ровно один вызов изменил состояние")
	require.Len(t, reactionEvents(r.fullPass(t, a, id, 50)), 1)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_reaction WHERE item_id = $1`, m.ID))
}

// Удалённое сообщение — message_deleted; элемент другого разговора, событие,
// несуществующий — invalid_request. Удаление сообщения реакций не выдаёт.
func TestM11TargetRules(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	id2 := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "тело")
	gone := r.mustSend(t, a, id, newRequestID(), "удалю")
	other := r.mustSend(t, a, id2, newRequestID(), "в другом")
	up := ownerReactions[0]

	require.True(t, r.mustReact(t, a, id, gone.ID, up, true))
	r.mustDel(t, a, id, gone.ID)
	require.Equal(t, convErrMessageDeleted, r.react(t, a, id, gone.ID, up, true).Error)
	require.Equal(t, convErrMessageDeleted, r.react(t, a, id, gone.ID, up, false).Error)

	require.Equal(t, convErrInvalidRequest, r.react(t, a, id, other.ID, up, true).Error, "элемент другого разговора")
	require.Equal(t, convErrInvalidRequest, r.react(t, a, id, "нет-такого", up, true).Error)
	var evPost string
	require.NoError(t, r.db.QueryRow(`SELECT post_id FROM engram_position WHERE conversation_id = $1 AND item_kind = 'event' ORDER BY pos LIMIT 1`, id).Scan(&evPost))
	require.Equal(t, convErrInvalidRequest, r.react(t, a, id, evPost, up, true).Error, "событие — не сообщение")

	// Удалённое сообщение в ленте — надгробие без полей реакций.
	all, _ := json.Marshal(r.fullPass(t, a, id, 50))
	require.NotContains(t, string(all), `"text":"удалю"`)
	require.True(t, r.mustReact(t, a, id, m.ID, up, true), "живое сообщение реагирует")
}

// Порядок проверок ADR-099 §11: доступ → архив → право; чужой разговор.
func TestM11AccessAndArchive(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "m")
	up := ownerReactions[0]

	require.Equal(t, convErrConversationNF, r.react(t, c, id, m.ID, up, true).Error, "не участник")
	require.Equal(t, convErrConversationNF, r.react(t, a, "нет-разговора", m.ID, up, true).Error)
	r.mustDo(t, b, ActionConversationLeave, reqOf(id))
	require.Equal(t, convErrNoAccess, r.react(t, b, id, m.ID, up, true).Error)
	require.True(t, r.mustReact(t, a, id, m.ID, up, true))
	r.mustDo(t, a, ActionConversationArchive, reqOf(id))
	require.Equal(t, convErrArchived, r.react(t, a, id, m.ID, up, false).Error)
	require.Equal(t, convErrArchived, r.react(t, a, id, m.ID, up, true).Error)
}

// direct — так же, как группа.
func TestM11DirectBehavesLikeGroup(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID
	m := r.mustSend(t, a, id, newRequestID(), "от A")
	require.True(t, r.mustReact(t, b, id, m.ID, ownerReactions[5], true))
	require.False(t, r.mustReact(t, b, id, m.ID, ownerReactions[5], true))
	require.True(t, r.mustReact(t, b, id, m.ID, ownerReactions[5], false))
	require.Len(t, reactionEvents(r.fullPass(t, a, id, 50)), 2)
}

// since_join: событие о сообщении вне периода вызывающего не выдаётся; сам
// B реагировать на невидимое не может.
func TestM11SinceJoinHidesEventAboutInvisible(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a)
	old := r.mustSend(t, a, id, newRequestID(), "до вступления B")
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB}))
	fresh := r.mustSend(t, a, id, newRequestID(), "после вступления B")

	require.True(t, r.mustReact(t, a, id, old.ID, ownerReactions[0], true))
	require.True(t, r.mustReact(t, a, id, fresh.ID, ownerReactions[0], true))

	forB := reactionEvents(r.fullPass(t, b, id, 50))
	require.Len(t, forB, 1)
	require.Equal(t, fresh.ID, forB[0].TargetID)
	raw, _ := json.Marshal(r.fullPass(t, b, id, 50))
	require.NotContains(t, string(raw), old.ID)
	require.Len(t, reactionEvents(r.fullPass(t, a, id, 50)), 2, "автору видно всё")
	page, _ := json.Marshal(r.histBefore(t, b, id, r.fullPass(t, b, id, 50)[len(r.fullPass(t, b, id, 50))-1], 50))
	require.NotContains(t, string(page), old.ID)
	require.Equal(t, convErrInvalidRequest, r.react(t, b, id, old.ID, ownerReactions[0], true).Error)
}
