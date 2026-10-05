package main

import (
	"fmt"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
)

// 219 LIST_DIRECT_MESSAGES_BEFORE против НАСТОЯЩЕГО PostgreSQL (ADR-105 §2,
// contract §3; порядок ADR-092 §2.1: CreateAt ASC, Id DESC).

func (r *pgRig) beforeRaw(t *testing.T, req map[string]any) Response {
	t.Helper()
	return listDirectMessagesBefore(r.deps(), r.b.mmID, pl(t, req))
}

func (r *pgRig) before(t *testing.T, at int64, id string, limit int) DirectMessagesBefore {
	t.Helper()
	req := map[string]any{"with_photon_id": r.a.photonID, "before": map[string]any{"created_at": at, "id": id}}
	if limit > 0 {
		req["limit"] = limit
	}
	resp := r.beforeRaw(t, req)
	require.True(t, resp.OK, "219: %+v", resp)
	return resp.Data.(DirectMessagesBefore)
}

func msgIDs(ms []DirectMessageV2) []string {
	out := []string{}
	for _, m := range ms {
		out = append(out, m.ID)
	}
	return out
}

// Порядок ленты: p1(100), q3 q2 q1 (200, id по убыванию), p9(300).
func seedTies(t *testing.T, r *pgRig) {
	pgInsert(t, r.api.db, "p1", r.ch, r.a.mmID, 100, "p1", "")
	for _, id := range []string{"q1", "q2", "q3"} {
		pgInsert(t, r.api.db, id, r.ch, r.a.mmID, 200, id, "")
	}
	pgInsert(t, r.api.db, "p9", r.ch, r.b.mmID, 300, "p9", "")
}

func TestPG219AnchorBoundary(t *testing.T) {
	r := newPGRig(t, pgSchema(t, true))
	seedTies(t, r)
	// Равный created_at, разные id: строго старше (T, id) — CreateAt < T или
	// (CreateAt = T и Id > id).
	require.Equal(t, []string{"p1", "q3", "q2", "q1"}, msgIDs(r.before(t, 300, "p9", 0).Items))
	require.Equal(t, []string{"p1", "q3", "q2"}, msgIDs(r.before(t, 200, "q1", 0).Items))
	require.Equal(t, []string{"p1", "q3"}, msgIDs(r.before(t, 200, "q2", 0).Items))
	require.Equal(t, []string{"p1"}, msgIDs(r.before(t, 200, "q3", 0).Items))
	// Якорь с несуществующим id внутри миллисекунды — по той же формуле.
	require.Equal(t, []string{"p1", "q3"}, msgIDs(r.before(t, 200, "q25", 0).Items))
	got := r.before(t, 100, "p1", 0)
	require.Empty(t, got.Items)
	require.True(t, got.AtStart)
}

func TestPG219AtStartAtExactLimit(t *testing.T) {
	r := newPGRig(t, pgSchema(t, true))
	seedTies(t, r)
	exact := r.before(t, 300, "p9", 4)
	require.Equal(t, []string{"p1", "q3", "q2", "q1"}, msgIDs(exact.Items))
	require.True(t, exact.AtStart, "ровно limit старше якоря — начало достигнуто")
	less := r.before(t, 300, "p9", 3)
	require.Equal(t, []string{"q3", "q2", "q1"}, msgIDs(less.Items), "ближайшие к якорю, от старых к новым")
	require.False(t, less.AtStart)
	// Шаг за шагом назад: без дублей и пропусков.
	var walked []string
	at, id := int64(300), "p9"
	for i := 0; i < 10; i++ {
		pg := r.before(t, at, id, 2)
		walked = append(msgIDs(pg.Items), walked...)
		if pg.AtStart {
			break
		}
		at, id = pg.Items[0].CreatedAt, pg.Items[0].ID
	}
	require.Equal(t, []string{"p1", "q3", "q2", "q1"}, walked)
}

func TestPG219LimitsAndInput(t *testing.T) {
	r := newPGRig(t, pgSchema(t, true))
	for i := 0; i < 65; i++ {
		pgInsert(t, r.api.db, fmt.Sprintf("m%03d", i), r.ch, r.a.mmID, int64(1000+i), "x", "")
	}
	def := r.before(t, 5000, "zz", 0)
	require.Len(t, def.Items, 60, "по умолчанию 60")
	require.False(t, def.AtStart)
	require.Equal(t, "m064", def.Items[59].ID)
	require.Len(t, r.before(t, 5000, "zz", 200).Items, 65)

	anchor := map[string]any{"created_at": 5000, "id": "zz"}
	for _, lim := range []int{0, -1, 201} {
		resp := r.beforeRaw(t, map[string]any{"with_photon_id": r.a.photonID, "before": anchor, "limit": lim})
		require.Equal(t, errInvalidLimit, resp.Error, "limit %d", lim)
	}
	require.Equal(t, errCodeInvalidRequest, r.beforeRaw(t, map[string]any{"with_photon_id": r.a.photonID}).Error)
	require.Equal(t, errCodeInvalidRequest, r.beforeRaw(t, map[string]any{"with_photon_id": r.a.photonID,
		"before": map[string]any{"created_at": 0, "id": "ab"}}).Error)
	require.Equal(t, errCodeInvalidRequest, r.beforeRaw(t, map[string]any{"with_photon_id": r.a.photonID,
		"before": map[string]any{"created_at": 5, "id": "A-B"}}).Error)
	require.Equal(t, errCodeInvalidRequest, r.beforeRaw(t, map[string]any{"with_photon_id": r.a.photonID,
		"before": anchor, "cursor": "x"}).Error)
	require.Equal(t, errInvalidPhotonID, r.beforeRaw(t, map[string]any{"with_photon_id": "bad", "before": anchor}).Error)
}

func TestPG219FormAuthorsAndNoChannel(t *testing.T) {
	r := newPGRig(t, pgSchema(t, true))
	pgInsert(t, r.api.db, "a1", r.ch, r.a.mmID, 100, "от a", "cid-a")
	pgInsert(t, r.api.db, "b1", r.ch, r.b.mmID, 200, "от b", "cid-b")
	pgInsert(t, r.api.db, "x1", r.ch, "someoneelse", 150, "чужой", "")
	got := r.before(t, 300, "zz", 0)
	require.Equal(t, []DirectMessageV2{
		{ID: "a1", AuthorPhotonID: r.a.photonID, Text: "от a", CreatedAt: 100},
		{ID: "b1", AuthorPhotonID: r.b.photonID, Text: "от b", CreatedAt: 200, ClientMsgID: "cid-b"},
	}, got.Items, "посты третьих не выдаются; client_msg_id — только у своих")
	require.True(t, got.AtStart)

	// Пара без канала: канал не создаётся, ответ пуст и at_start.
	c := r.party(t, "c")
	resp := listDirectMessagesBefore(r.deps(), r.b.mmID, pl(t, map[string]any{"with_photon_id": c.photonID,
		"before": map[string]any{"created_at": 300, "id": "zz"}}))
	require.True(t, resp.OK, "%+v", resp)
	require.Empty(t, resp.Data.(DirectMessagesBefore).Items)
	require.True(t, resp.Data.(DirectMessagesBefore).AtStart)

	// Хранилище недоступно.
	d := r.deps()
	d.store = nil
	require.Equal(t, errSyncUnavailable, listDirectMessagesBefore(d, r.b.mmID, pl(t, map[string]any{"with_photon_id": r.a.photonID,
		"before": map[string]any{"created_at": 300, "id": "zz"}})).Error)
}

// ADR-092 §2.3 + ADR-105 §1.3: поздний пост внутри окна 218 назад может быть
// пропущен (якорь выше него), но следующий проход 218 вперёд его вернёт —
// объединение страниц потерь не имеет.
func TestPG219LatePostFoundByForwardPass(t *testing.T) {
	r := newPGRig(t, pgSchema(t, true))
	now := model.GetMillis()
	pgInsert(t, r.api.db, "a1", r.ch, r.a.mmID, now-200_000, "a1", "")
	pgInsert(t, r.api.db, "a2", r.ch, r.a.mmID, now-30_000, "a2", "")
	pgInsert(t, r.api.db, "a3", r.ch, r.a.mmID, now, "a3", "")

	first := r.list(t, "", 2) // newest: a2, a3
	require.Equal(t, []string{"a2", "a3"}, msgIDs(first.Messages))

	// Фиксируется позже, с CreateAt внутри окна (< W = 86 с), выше якоря a2.
	pgInsert(t, r.api.db, "y", r.ch, r.a.mmID, now-20_000, "y", "")

	back := r.before(t, now-30_000, "a2", 0)
	require.Equal(t, []string{"a1"}, msgIDs(back.Items), "219 от якоря ниже позднего поста его не видит")
	require.True(t, back.AtStart)

	fwd, _ := r.syncPass(t, first.NextCursor, 60)
	require.True(t, set(fwd)["y"], "проход 218 вперёд вернул поздний пост: %v", fwd)

	union := set(append(append(msgIDs(back.Items), msgIDs(first.Messages)...), fwd...))
	for _, id := range []string{"a1", "a2", "a3", "y"} {
		require.True(t, union[id], "потерян %s", id)
	}
}
