package main

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// M7: 908 MARK_READ и read_up_to/unread_count в 902 против НАСТОЯЩЕГО
// PostgreSQL (ADR-108; messaging-v2-contract.md §5b.1). PHOTON_DM_PG_DSN.

func (r *groupRig) markRead(t *testing.T, caller, convID string, at int64, id string) Response {
	t.Helper()
	return r.do(t, caller, ActionConversationMarkRead, map[string]any{
		"conversation_id": convID, "up_to": map[string]any{"created_at": at, "id": id}})
}

func (r *groupRig) mustMarkRead(t *testing.T, caller, convID string, m ConvMessage) ConvMarkReadResponse {
	t.Helper()
	resp := r.markRead(t, caller, convID, m.CreatedAt, m.ID)
	require.True(t, resp.OK, "908: %+v", resp)
	return resp.Data.(ConvMarkReadResponse)
}

func (r *groupRig) listItem(t *testing.T, caller, convID string) ConvListItem {
	t.Helper()
	for _, it := range r.listAll(t, caller) {
		if it.ConversationID == convID {
			return it
		}
	}
	t.Fatalf("разговора %s нет в 902", convID)
	return ConvListItem{}
}

func (r *groupRig) sendN(t *testing.T, from, convID, prefix string, n int) []ConvMessage {
	t.Helper()
	out := make([]ConvMessage, n)
	for i := range out {
		out[i] = r.mustSend(t, from, convID, newRequestID(), prefix)
	}
	return out
}

// Позиция только растёт: меньшая отметка не уменьшает, повтор даёт тот же
// ответ; отправка своего сообщения позицию не двигает.
func TestM7MarkReadMonotonic(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	ms := r.sendN(t, b, id, "от B", 3)

	got := r.mustMarkRead(t, a, id, ms[1])
	require.Equal(t, &dmWirePosition{CreatedAt: ms[1].CreatedAt, ID: ms[1].ID}, got.ReadUpTo)
	require.Equal(t, 1, got.UnreadCount)

	lower := r.mustMarkRead(t, a, id, ms[0])
	require.Equal(t, got, lower, "меньшая отметка не уменьшает позицию")
	require.Equal(t, got, r.mustMarkRead(t, a, id, ms[1]), "повтор — тот же ответ")

	r.mustSend(t, a, id, newRequestID(), "своё")
	it := r.listItem(t, a, id)
	require.Equal(t, got.ReadUpTo, it.ReadUpTo, "930 позицию не двигает")
	require.Equal(t, 1, it.UnreadCount, "своё сообщение не считается")

	up := r.mustMarkRead(t, a, id, ms[2])
	require.Equal(t, ms[2].ID, up.ReadUpTo.ID)
	require.Equal(t, 0, up.UnreadCount)
}

// Параллельные 908 с разными отметками дают максимум.
func TestM7MarkReadParallelKeepsMaximum(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	ms := r.sendN(t, b, id, "x", 24)
	var wg sync.WaitGroup
	for i := range ms {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := r.markRead(t, a, id, ms[i].CreatedAt, ms[i].ID)
			if !resp.OK {
				t.Errorf("908 #%d: %+v", i, resp)
			}
		}(i)
	}
	wg.Wait()
	last := ms[len(ms)-1]
	var at int64
	var rid string
	require.NoError(t, r.db.QueryRow(`SELECT read_created_at, read_id FROM engram_read_state
WHERE conversation_id = $1`, id).Scan(&at, &rid))
	require.Equal(t, last.CreatedAt, at)
	require.Equal(t, last.ID, rid)
	require.Equal(t, 0, r.listItem(t, a, id).UnreadCount)
}

// up_to новее последнего видимого элемента обрезается до него — в том числе
// до события.
func TestM7MarkReadClipsToLastVisibleItem(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)
	id := r.group(t, a, pidB)
	ms := r.sendN(t, b, id, "x", 2)

	far := r.markRead(t, a, id, ms[1].CreatedAt+1_000_000, "zzzz")
	require.True(t, far.OK, "%+v", far)
	got := far.Data.(ConvMarkReadResponse)
	require.Equal(t, &dmWirePosition{CreatedAt: ms[1].CreatedAt, ID: ms[1].ID}, got.ReadUpTo, "обрезано до последнего сообщения")
	require.Equal(t, 0, got.UnreadCount)

	// Последний видимый элемент — событие: обрезка до него.
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
	pg := r.syncPage(t, a, id, "", 0)
	ev, ok := pg.Items[len(pg.Items)-1].(ConvEventItem)
	require.True(t, ok)
	got = r.markRead(t, a, id, ev.CreatedAt+5_000_000, "zzzz").Data.(ConvMarkReadResponse)
	require.Equal(t, &dmWirePosition{CreatedAt: ev.CreatedAt, ID: ev.ID}, got.ReadUpTo, "обрезано до события")
}

// Счётчик: только чужие опубликованные сообщения; события, свои и надгробия
// не считаются; без отметки считается всё видимое.
func TestM7UnreadCountsOnlyOthersPublishedMessages(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)
	id := r.group(t, a, pidB)

	require.Equal(t, 0, r.listItem(t, a, id).UnreadCount, "события не считаются")
	require.Nil(t, r.listItem(t, a, id).ReadUpTo, "отметки нет — null")
	bs := r.sendN(t, b, id, "от B", 3)
	r.sendN(t, a, id, "от A", 2)
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
	require.Equal(t, 3, r.listItem(t, a, id).UnreadCount, "без отметки — все чужие, без своих и событий")
	require.Equal(t, 2, r.listItem(t, b, id).UnreadCount, "у B чужие — два сообщения A")

	// Надгробие: резерв отменён, пост остался — сообщение не считается.
	_, err := r.db.Exec(`UPDATE engram_position SET state = 'aborted' WHERE conversation_id = $1 AND pos = $2`, id, bs[2].CreatedAt)
	require.NoError(t, err)
	require.Equal(t, 2, r.listItem(t, a, id).UnreadCount, "надгробие не считается")
}

// since_join: сообщения до вступления и в разрыве между периодами не
// считаются; full — считаются все.
func TestM7UnreadRespectsPeriods(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB)
	r.sendN(t, b, id, "до вступления", 2)
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
	r.sendN(t, b, id, "период 1", 2)
	require.Equal(t, 2, r.listItem(t, c, id).UnreadCount)
	r.mustDo(t, c, ActionConversationLeave, reqOf(id))
	r.sendN(t, b, id, "разрыв", 3)
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
	r.sendN(t, b, id, "период 2", 1)
	require.Equal(t, 3, r.listItem(t, c, id).UnreadCount, "2 + 1; до вступления и разрыв не видны")
	// 908 с отметкой в первом периоде считает те же периоды.
	first := r.syncPage(t, c, id, "", 0).Items
	var firstMsg ConvMessageItem
	for _, it := range first {
		if m, ok := it.(ConvMessageItem); ok {
			firstMsg = m
			break
		}
	}
	got := r.mustMarkRead(t, c, id, ConvMessage{ID: firstMsg.ID, CreatedAt: firstMsg.CreatedAt})
	require.Equal(t, 2, got.UnreadCount, "после первого сообщения периода 1: одно из периода 1 и одно из периода 2")

	// full: вступивший позже видит и считает всю историю.
	full := r.mustCreate(t, a, map[string]any{"request_id": newRequestID(), "kind": "group", "title": "full",
		"history_policy": "full", "member_photon_ids": []string{pidB}}).Conversation.ConversationID
	r.sendN(t, b, full, "старое", 4)
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(full, "photon_ids", []string{pidC}))
	require.Equal(t, 4, r.listItem(t, c, full).UnreadCount)
}

// Предел 999 (ADR-108 Q3): 1001 непрочитанное → 999 в 902 и в 908.
func TestM7UnreadCap999(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	ms := r.sendN(t, b, id, "m", 1001)
	require.Equal(t, 999, r.listItem(t, a, id).UnreadCount)
	got := r.mustMarkRead(t, a, id, ms[0])
	require.Equal(t, 999, got.UnreadCount, "1000 после отметки — всё равно 999")
	got = r.mustMarkRead(t, a, id, ms[1])
	require.Equal(t, 999, got.UnreadCount, "999 непрочитанных — ровно 999")
	got = r.mustMarkRead(t, a, id, ms[2])
	require.Equal(t, 998, got.UnreadCount)
}

// Отметка общая для аккаунта (второе устройство видит её в 902) и личная:
// другой участник её не видит.
func TestM7ReadStateSharedAcrossDevicesAndPrivate(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	ms := r.sendN(t, b, id, "x", 2)
	r.mustMarkRead(t, a, id, ms[0])
	// «Второе устройство» — другой вызов того же аккаунта: хранится по PhotonID.
	it := r.listItem(t, a, id)
	require.Equal(t, &dmWirePosition{CreatedAt: ms[0].CreatedAt, ID: ms[0].ID}, it.ReadUpTo)
	require.Equal(t, 1, it.UnreadCount)
	other := r.listItem(t, b, id)
	require.Nil(t, other.ReadUpTo, "позиция другого участника не видна")
	raw, err := json.Marshal(other)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"read_up_to":null`)
	require.Contains(t, string(raw), `"unread_count":`)
}

// direct — так же (ADR-104).
func TestM7DirectHasReadState(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	d := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID
	ms := r.sendN(t, b, d, "привет", 2)
	require.Equal(t, 2, r.listItem(t, a, d).UnreadCount)
	got := r.mustMarkRead(t, a, d, ms[0])
	require.Equal(t, 1, got.UnreadCount)
	require.Equal(t, 1, r.listItem(t, a, d).UnreadCount)
}

// Доступ как у 931: бывший участник — no_access, чужой — conversation_not_found,
// участник архивного разговора отмечает; содержание — после доступа.
func TestM7MarkReadAccess(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB)
	ms := r.sendN(t, b, id, "x", 2)

	require.Equal(t, convErrConversationNF, r.markRead(t, c, id, ms[0].CreatedAt, ms[0].ID).Error)
	require.Equal(t, convErrConversationNF, r.markRead(t, a, "нет-такого", 1, "abc").Error)
	// Содержание проверяется после доступа: у чужого и мусорный up_to — not_found.
	require.Equal(t, convErrConversationNF, r.do(t, c, ActionConversationMarkRead, map[string]any{"conversation_id": id}).Error)

	for _, bad := range []map[string]any{
		{"conversation_id": id},
		{"conversation_id": id, "up_to": nil},
		{"conversation_id": id, "up_to": map[string]any{"created_at": 0, "id": ms[0].ID}},
		{"conversation_id": id, "up_to": map[string]any{"created_at": ms[0].CreatedAt, "id": "ID с пробелом"}},
		{"conversation_id": id, "up_to": map[string]any{"created_at": ms[0].CreatedAt, "id": ms[0].ID}, "extra": 1},
		{"up_to": map[string]any{"created_at": ms[0].CreatedAt, "id": ms[0].ID}},
	} {
		require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationMarkRead, bad).Error, "%v", bad)
	}

	r.mustDo(t, a, ActionConversationArchive, reqOf(id))
	resp := r.markRead(t, b, id, ms[1].CreatedAt, ms[1].ID)
	require.True(t, resp.OK, "участник архивного разговора: %+v", resp)
	require.Equal(t, 0, resp.Data.(ConvMarkReadResponse).UnreadCount)
	r.mustDo(t, a, ActionConversationUnarchive, reqOf(id))

	r.mustDo(t, b, ActionConversationLeave, reqOf(id))
	require.Equal(t, convErrNoAccess, r.markRead(t, b, id, ms[1].CreatedAt, ms[1].ID).Error, "бывший участник")
}

// Разговор без видимых вызывающему элементов: read_up_to null, счёт 0,
// строка не пишется.
func TestM7MarkReadNoVisibleItems(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	_, err := r.db.Exec(`UPDATE posts SET deleteat = 1 WHERE channelid = $1`, r.carrier(t, id))
	require.NoError(t, err)
	resp := r.markRead(t, a, id, 5, "abc")
	require.True(t, resp.OK, "%+v", resp)
	got := resp.Data.(ConvMarkReadResponse)
	require.Nil(t, got.ReadUpTo)
	require.Equal(t, 0, got.UnreadCount)
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_read_state`))
	raw, _ := json.Marshal(resp.Data)
	require.JSONEq(t, `{"read_up_to":null,"unread_count":0}`, string(raw))
}

// 902: порядок и курсор от позиции прочтения не зависят.
func TestM7ListOrderUnchanged(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	ids := []string{r.group(t, a, pidB), r.group(t, a, pidB), r.group(t, a, pidB)}
	before := listIDs(r.listAll(t, a))
	ms := r.sendN(t, b, ids[0], "x", 1)
	r.mustMarkRead(t, a, ids[0], ms[0])
	after := listIDs(r.listAll(t, a))
	require.ElementsMatch(t, before, after)
	require.Equal(t, ids[0], after[0], "свежая активность — первым; 908 порядок не меняет")
}
