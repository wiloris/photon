package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// 940 CONVERSATION_HISTORY_BEFORE против НАСТОЯЩЕГО PostgreSQL (ADR-105 §2,
// implementation contract messaging-v2 §3). PHOTON_DM_PG_DSN.

func (r *groupRig) histRaw(t *testing.T, callerMM string, req map[string]any) Response {
	t.Helper()
	return r.svc.dispatch(context.Background(), callerMM, ActionConversationHistoryBefore, pl(t, req))
}

// histBefore — 940 от позиции элемента anchor.
func (r *groupRig) histBefore(t *testing.T, callerMM, convID string, anchor any, limit int) ConvHistoryResponse {
	t.Helper()
	req := map[string]any{"conversation_id": convID,
		"before": map[string]any{"created_at": itemAt(anchor), "id": itemID(anchor)}}
	if limit > 0 {
		req["limit"] = limit
	}
	resp := r.histRaw(t, callerMM, req)
	require.True(t, resp.OK, "940: %+v", resp)
	return resp.Data.(ConvHistoryResponse)
}

func TestConv940Contract(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	cid := newRequestID()
	for _, txt := range []string{"m1", "m2", "m3", "m4"} {
		if txt == "m2" {
			r.mustSend(t, a, id, cid, txt)
			continue
		}
		r.mustSend(t, a, id, newRequestID(), txt)
	}
	all := r.fullPass(t, a, id, 60) // created, member_added, m1..m4
	require.Len(t, all, 6)

	// Для каждого якоря: строго старшее, в порядке ленты; якорь не входит.
	for i := range all {
		got := r.histBefore(t, a, id, all[i], 0)
		require.Equal(t, names(all[:i]), names(got.Items), "якорь %d", i)
		require.True(t, got.AtStart, "якорь %d: лимит 60 больше ленты", i)
	}

	// at_start на точной границе limit: ровно limit элементов старше — start
	// достигнут; на один меньше — нет (проба limit+1).
	anchor := all[5] // старше него 5 элементов
	exact := r.histBefore(t, a, id, anchor, 5)
	require.Equal(t, names(all[:5]), names(exact.Items))
	require.True(t, exact.AtStart, "ровно limit элементов старше якоря — начало достигнуто")
	less := r.histBefore(t, a, id, anchor, 4)
	require.Equal(t, names(all[1:5]), names(less.Items), "ближайшие к якорю, в порядке ленты")
	require.False(t, less.AtStart)

	// Шаг за шагом назад до начала: без дублей и пропусков.
	var walked []any
	cur := all[5]
	for i := 0; i < 10; i++ {
		pg := r.histBefore(t, a, id, cur, 2)
		walked = append(append([]any{}, pg.Items...), walked...)
		if pg.AtStart {
			break
		}
		cur = pg.Items[0]
	}
	require.Equal(t, names(all[:5]), names(walked))

	// Якорь старше всей ленты: пусто, at_start.
	empty := r.histRaw(t, a, map[string]any{"conversation_id": id, "before": map[string]any{"created_at": 1, "id": "zz"}})
	require.True(t, empty.OK)
	require.Empty(t, empty.Data.(ConvHistoryResponse).Items)
	require.True(t, empty.Data.(ConvHistoryResponse).AtStart)

	// client_msg_id — только у своих.
	r.id.person(pidC)
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
	last := r.fullPass(t, a, id, 60)
	own := r.histBefore(t, a, id, last[len(last)-1], 0)
	var m2 ConvMessageItem
	for _, it := range own.Items {
		if m, ok := it.(ConvMessageItem); ok && m.Text == "m2" {
			m2 = m
		}
	}
	require.Equal(t, cid, m2.ClientMsgID)
	bView := r.histBefore(t, r.id.person(pidB), id, last[len(last)-1], 0)
	for _, it := range bView.Items {
		if m, ok := it.(ConvMessageItem); ok {
			require.Empty(t, m.ClientMsgID, "чужие сообщения без client_msg_id")
		}
	}

	// Лимит.
	for _, lim := range []int{0, -1, 201} {
		require.Equal(t, convErrInvalidLimit, r.histRaw(t, a, map[string]any{"conversation_id": id,
			"before": map[string]any{"created_at": itemAt(all[5]), "id": itemID(all[5])}, "limit": lim}).Error, "limit %d", lim)
	}
	require.True(t, r.histRaw(t, a, map[string]any{"conversation_id": id,
		"before": map[string]any{"created_at": itemAt(all[5]), "id": itemID(all[5])}, "limit": 200}).OK)

	// Содержание запроса.
	for name, req := range map[string]map[string]any{
		"нет before":    {"conversation_id": id},
		"нулевое время": {"conversation_id": id, "before": map[string]any{"created_at": 0, "id": "ab"}},
		"плохой id":     {"conversation_id": id, "before": map[string]any{"created_at": 5, "id": "A B"}},
		"лишнее поле":   {"conversation_id": id, "before": map[string]any{"created_at": 5, "id": "ab"}, "cursor": "x"},
		"нет разговора": {"before": map[string]any{"created_at": 5, "id": "ab"}},
	} {
		require.Equal(t, convErrInvalidRequest, r.histRaw(t, a, req).Error, name)
	}

	// Доступ раньше содержания (ADR-099 §11).
	dMM := r.id.person(pidD)
	bad := map[string]any{"conversation_id": id}
	require.Equal(t, convErrConversationNF, r.histRaw(t, dMM, bad).Error, "не участник")
	require.Equal(t, convErrConversationNF, r.histRaw(t, dMM, map[string]any{"conversation_id": id,
		"before": map[string]any{"created_at": itemAt(all[5]), "id": itemID(all[5])}}).Error)
	require.Equal(t, convErrConversationNF, r.histRaw(t, a, map[string]any{"conversation_id": "no-such",
		"before": map[string]any{"created_at": 5, "id": "ab"}}).Error)
	require.Equal(t, convErrConversationNF, r.histRaw(t, r.id.plainUser(), map[string]any{"conversation_id": id,
		"before": map[string]any{"created_at": 5, "id": "ab"}}).Error, "без PhotonID")
}

// Позиция якоря: пост, схлопнутый на позицию якоря, не выдаётся; строго
// «createat <» (ADR-099 §9.4: позиции уникальны, посты на позицию схлопнуты).
func TestConv940AnchorPositionExcluded(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	r.mustSend(t, a, id, newRequestID(), "m1")
	r.mustSend(t, a, id, newRequestID(), "m2")
	all := r.fullPass(t, a, id, 60)
	m2 := all[len(all)-1]
	// Второй пост на той же позиции: схлопывается в один элемент.
	pgInsert(t, r.db, "0dup", r.carrier(t, id), "someone", itemAt(m2), "dup", "")
	got := r.histBefore(t, a, id, m2, 0)
	require.Equal(t, names(all[:len(all)-1]), names(got.Items), "ни якорь, ни схлопнутый на его позиции пост не выдаются")
	for _, it := range got.Items {
		require.NotEqual(t, "0dup", itemID(it))
		require.Less(t, itemAt(it), itemAt(m2))
	}
}

// since_join: разрывы между периодами не раскрываются ни якорем внутри
// разрыва, ни якорем до первого периода (ADR-099 §9.4, ADR-105 §2.4).
func TestConv940SinceJoinGaps(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB)
	s := func(text string) { r.mustSend(t, a, id, newRequestID(), text) }
	s("x1")
	s("x2")
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
	s("x3")
	r.mustDo(t, c, ActionConversationLeave, reqOf(id))
	s("x4")
	r.mustDo(t, a, ActionConversationUpdate, reqOf(id, "title", "между периодами"))
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
	s("x5")

	cAll := r.fullPass(t, c, id, 60)
	require.Equal(t, []string{"event:member_added", "msg:x3", "event:member_left", "event:member_added", "msg:x5"}, names(cAll))
	aAll := r.fullPass(t, a, id, 60)
	byName := func(n string) any {
		for _, it := range aAll {
			if itemName(it) == n {
				return it
			}
		}
		t.Fatalf("нет элемента %s", n)
		return nil
	}

	// Якорь — последний элемент: всё видимое, разрыв не раскрыт.
	got := r.histBefore(t, c, id, cAll[4], 0)
	require.Equal(t, names(cAll[:4]), names(got.Items))
	require.True(t, got.AtStart)

	// Якорь внутри разрыва (x4 виден только A): только периоды вызывающего.
	inGap := r.histBefore(t, c, id, byName("msg:x4"), 0)
	require.Equal(t, []string{"event:member_added", "msg:x3", "event:member_left"}, names(inGap.Items))
	require.True(t, inGap.AtStart)
	// Якорь до первого периода (x2): ничего, начало достигнуто.
	before := r.histBefore(t, c, id, byName("msg:x2"), 0)
	require.Empty(t, before.Items)
	require.True(t, before.AtStart)

	// Малый limit через разрыв: ближайшие к якорю видимые, без разрыва.
	pg := r.histBefore(t, c, id, cAll[4], 2)
	require.Equal(t, []string{"event:member_left", "event:member_added"}, names(pg.Items))
	require.False(t, pg.AtStart)
	pg2 := r.histBefore(t, c, id, pg.Items[0], 2)
	require.Equal(t, []string{"event:member_added", "msg:x3"}, names(pg2.Items))
	require.True(t, pg2.AtStart, "ровно limit видимых старше — начало периодов достигнуто")

	// A (весь период) видит и x4.
	require.Contains(t, names(r.histBefore(t, a, id, aAll[len(aAll)-1], 0).Items), "msg:x4")
}

// Архив (ADR-099 §8.1): участник архивного разговора читает назад (период
// остаётся active), удалённый до архива — нет (access = callerAccess, как 931).
func TestConv940Archive(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB, pidC)
	r.mustSend(t, a, id, newRequestID(), "x1")
	r.mustDo(t, a, ActionConversationMemberRemove, reqOf(id, "photon_id", pidB))
	r.mustSend(t, a, id, newRequestID(), "x2")
	r.mustDo(t, a, ActionConversationArchive, reqOf(id))
	aAll := r.fullPass(t, a, id, 60)
	anchor := aAll[len(aAll)-1]
	require.Equal(t, "event:archived", itemName(anchor))
	req := map[string]any{"conversation_id": id, "before": map[string]any{"created_at": itemAt(anchor), "id": itemID(anchor)}}

	got := r.histBefore(t, c, id, anchor, 0)
	require.Equal(t, names(aAll[:len(aAll)-1]), names(got.Items))
	require.True(t, got.AtStart)
	require.Equal(t, convErrNoAccess, r.histRaw(t, b, req).Error, "удалён до архива")
	require.Equal(t, convErrConversationNF, r.histRaw(t, r.id.person(pidD), req).Error)
}
