package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// 917/918 и visibility против НАСТОЯЩЕГО PostgreSQL (contract §5a; ADR-107 этап 2).

func (r *groupRig) publicGroup(t *testing.T, ownerMM, title string, extra ...any) string {
	t.Helper()
	req := createReq(newRequestID(), title)
	delete(req, "history_policy") // для public — по умолчанию full
	req["visibility"] = "public"
	for i := 0; i+1 < len(extra); i += 2 {
		req[extra[i].(string)] = extra[i+1]
	}
	return r.mustCreate(t, ownerMM, req).Conversation.ConversationID
}

func (r *groupRig) dir(t *testing.T, caller string, req map[string]any) Response {
	t.Helper()
	return r.do(t, caller, ActionConversationDirectorySearch, req)
}

func (r *groupRig) dirOK(t *testing.T, caller string, req map[string]any) ConvDirResponse {
	t.Helper()
	resp := r.dir(t, caller, req)
	require.True(t, resp.OK, "917: %+v", resp)
	return resp.Data.(ConvDirResponse)
}

func dirIDs(items []ConvDirItem) []string {
	out := []string{}
	for _, it := range items {
		out = append(out, it.ConversationID)
	}
	return out
}

func (r *groupRig) joinPublic(t *testing.T, caller, convID string) Response {
	t.Helper()
	return r.do(t, caller, ActionConversationJoinPublic, map[string]any{"request_id": newRequestID(), "conversation_id": convID})
}

// 900: public без history_policy — full; private — обязательно; значение visibility проверяется.
func TestPublic900VisibilityAndDefaultPolicy(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)

	req := createReq(newRequestID(), "pub")
	delete(req, "history_policy")
	req["visibility"] = "public"
	snap := r.mustCreate(t, a, req).Conversation
	require.Equal(t, "public", snap.Visibility)
	require.Equal(t, "full", snap.HistoryPolicy)

	// public с явным since_join — уважается.
	id2 := r.publicGroup(t, a, "pub2", "history_policy", "since_join")
	require.Equal(t, "since_join", r.snapshot(t, a, id2).HistoryPolicy)

	// private без history_policy — по-прежнему invalid_request.
	bad := createReq(newRequestID(), "x")
	delete(bad, "history_policy")
	require.Equal(t, convErrInvalidRequest, r.create(t, a, bad).Error)
	bad["visibility"] = "private"
	require.Equal(t, convErrInvalidRequest, r.create(t, a, bad).Error)
	// Неизвестное значение.
	bad = createReq(newRequestID(), "x")
	bad["visibility"] = "secret"
	require.Equal(t, convErrInvalidRequest, r.create(t, a, bad).Error)

	// Без visibility — private.
	priv := r.mustCreate(t, a, createReq(newRequestID(), "p")).Conversation
	require.Equal(t, "private", priv.Visibility)
}

func (r *groupRig) snapshot(t *testing.T, caller, convID string) ConvSnapshot {
	t.Helper()
	resp := r.do(t, caller, ActionConversationGet, map[string]any{"conversation_id": convID})
	require.True(t, resp.OK, "901: %+v", resp)
	return resp.Data.(ConvSnapshotResponse).Conversation
}

// Хэш ключа 900 без visibility — прежний (§10.2): повтор запроса, выполненного до M6, совпадает.
func TestPublic900HashUnchangedWithoutVisibility(t *testing.T) {
	p, ok := parseCreate(pl(t, createReq(newRequestID(), "legacy", pidB)))
	require.True(t, ok)
	legacy, _ := json.Marshal(map[string]any{
		"kind": "group", "title": "legacy", "history_policy": "since_join", "member_photon_ids": []string{pidB},
	})
	want := sha256.Sum256(legacy)
	require.Equal(t, want[:], canonicalCreateHash(p))

	withVis := createReq(newRequestID(), "legacy", pidB)
	withVis["visibility"] = "private"
	p2, ok := parseCreate(pl(t, withVis))
	require.True(t, ok)
	require.NotEqual(t, want[:], canonicalCreateHash(p2), "visibility входит в хэш, когда передано")
}

// 903: visibility меняется под conversation.update; metadata_changed с полем; версия +1.
func TestPublic903ChangesVisibility(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	require.Equal(t, "private", r.snapshot(t, a, id).Visibility)
	v0 := r.version(t, id)

	require.Equal(t, convErrForbidden, r.do(t, b, ActionConversationUpdate, reqOf(id, "visibility", "public")).Error)
	require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationUpdate, reqOf(id, "visibility", "weird")).Error)
	require.Equal(t, v0, r.version(t, id))

	r.mustDo(t, a, ActionConversationUpdate, reqOf(id, "visibility", "public"))
	require.Equal(t, v0+1, r.version(t, id))
	require.Equal(t, "public", r.snapshot(t, b, id).Visibility)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND event->>'event' = 'metadata_changed' AND event->>'visibility' = 'public'`, id))

	// Тот же visibility — нет изменения.
	r.mustDo(t, a, ActionConversationUpdate, reqOf(id, "visibility", "public"))
	require.Equal(t, v0+1, r.version(t, id))

	// В ленте у участника поле видно.
	var found bool
	for _, it := range r.syncPage(t, b, id, "", 0).Items {
		if ev, ok := it.(ConvEventItem); ok && ev.Event == "metadata_changed" {
			require.Equal(t, "public", ev.Visibility)
			found = true
		}
	}
	require.True(t, found)

	// В списке 902 поле есть.
	for _, it := range r.listOpts(t, b, map[string]any{}).Conversations {
		if it.ConversationID == id {
			require.Equal(t, "public", it.Visibility)
		}
	}

	// direct — invalid_request.
	d := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID
	require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationUpdate, reqOf(d, "visibility", "public")).Error)
	require.Equal(t, "private", r.snapshot(t, a, d).Visibility)
}

// 917: только public и не архивные; регистр; без участия; курсор без дублей; валидация.
func TestPublic917Directory(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	outsider := r.id.person(pidD)

	pub1 := r.publicGroup(t, a, "Garden Club")
	pub2 := r.publicGroup(t, a, "garden party")
	pub3 := r.publicGroup(t, a, "Chess")
	priv := r.mustCreate(t, a, createReq(newRequestID(), "Garden secret")).Conversation.ConversationID
	arch := r.publicGroup(t, a, "Garden archive")
	r.mustDo(t, a, ActionConversationArchive, reqOf(arch))
	_ = priv

	got := r.dirOK(t, outsider, map[string]any{"query": "GARDEN"})
	ids := dirIDs(got.Conversations)
	sort.Strings(ids)
	want := []string{pub1, pub2}
	sort.Strings(want)
	require.Equal(t, want, ids, "private и архивные не возвращаются")
	for _, it := range got.Conversations {
		require.Equal(t, 1, it.MemberCount)
	}
	b, _ := json.Marshal(got)
	require.NotContains(t, string(b), "members")
	require.NotContains(t, string(b), pidA, "состав не раскрывается")

	require.Equal(t, []string{pub3}, dirIDs(r.dirOK(t, outsider, map[string]any{"query": " chess "}).Conversations))
	require.Empty(t, r.dirOK(t, outsider, map[string]any{"query": "secret"}).Conversations)

	// Валидация.
	require.Equal(t, convErrInvalidRequest, r.dir(t, a, map[string]any{"query": "   "}).Error)
	require.Equal(t, convErrInvalidRequest, r.dir(t, a, map[string]any{}).Error)
	long := make([]byte, 65)
	for i := range long {
		long[i] = 'a'
	}
	require.Equal(t, convErrInvalidRequest, r.dir(t, a, map[string]any{"query": string(long)}).Error)
	require.Equal(t, convErrInvalidLimit, r.dir(t, a, map[string]any{"query": "g", "limit": 0}).Error)
	require.Equal(t, convErrInvalidLimit, r.dir(t, a, map[string]any{"query": "g", "limit": 51}).Error)
	require.Equal(t, convErrInvalidCursor, r.dir(t, a, map[string]any{"query": "g", "cursor": "!!"}).Error)
}

func TestPublic917CursorNoDuplicates(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	const n = 7
	want := map[string]bool{}
	for i := 0; i < n; i++ {
		want[r.publicGroup(t, a, fmt.Sprintf("room %d", i))] = true
	}
	// Одинаковый created_at у части строк — проверка тай-брейка по conversation_id.
	_, err := r.db.Exec(`UPDATE engram_conversation SET created_at = 1000 WHERE title IN ('room 1','room 2','room 3')`)
	require.NoError(t, err)

	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		req := map[string]any{"query": "room", "limit": 3}
		if cursor != "" {
			req["cursor"] = cursor
		}
		page := r.dirOK(t, a, req)
		pages++
		for _, id := range dirIDs(page.Conversations) {
			require.False(t, seen[id], "дубль %s", id)
			seen[id] = true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	require.Equal(t, want, seen)
	require.Equal(t, 3, pages)
}

// 918: вступление, since_join, повтор, уже участник; гонка.
func TestPublic918Join(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.publicGroup(t, a, "open", "history_policy", "since_join")
	require.True(t, r.send(t, a, id, newRequestID(), "before").OK)

	req := map[string]any{"request_id": newRequestID(), "conversation_id": id}
	first := r.mustDo(t, b, ActionConversationJoinPublic, req)
	var snap ConvSnapshotResponse
	require.NoError(t, json.Unmarshal(first, &snap))
	require.Equal(t, "member", snap.Conversation.Me.Role)
	require.Equal(t, 2, snap.Conversation.MemberCount)
	require.Equal(t, "public", snap.Conversation.Visibility)
	v1 := r.version(t, id)

	// Повтор с тем же request_id — тот же ответ, без нового события.
	require.JSONEq(t, string(first), string(r.mustDo(t, b, ActionConversationJoinPublic, req)))
	require.Equal(t, v1, r.version(t, id))
	// Тот же request_id с другим разговором — idempotency_conflict.
	other := r.publicGroup(t, a, "other")
	require.Equal(t, convErrIdempotency, r.do(t, b, ActionConversationJoinPublic, map[string]any{"request_id": req["request_id"], "conversation_id": other}).Error)
	// Уже участник (новый request_id) — снимок без события и без версии.
	r.mustDo(t, b, ActionConversationJoinPublic, map[string]any{"request_id": newRequestID(), "conversation_id": id})
	require.Equal(t, v1, r.version(t, id))
	require.Equal(t, 1, r.events(t, id, "member_joined"))

	// since_join: прошлое скрыто, member_joined без invite_id.
	require.True(t, r.send(t, a, id, newRequestID(), "after").OK)
	var texts []string
	for _, it := range r.syncPage(t, b, id, "", 0).Items {
		switch v := it.(type) {
		case ConvMessageItem:
			texts = append(texts, v.Text)
		case ConvEventItem:
			if v.Event == "member_joined" {
				require.Empty(t, v.InviteID)
				require.Equal(t, pidB, v.ActorPhotonID)
			}
		}
	}
	require.Equal(t, []string{"after"}, texts)
}

func TestPublic918FullHistoryForJoiner(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.publicGroup(t, a, "open") // history_policy опущен — full
	require.True(t, r.send(t, a, id, newRequestID(), "old").OK)
	r.mustDo(t, b, ActionConversationJoinPublic, map[string]any{"request_id": newRequestID(), "conversation_id": id})
	var texts []string
	for _, it := range r.fullPass(t, b, id, 60) {
		if m, ok := it.(ConvMessageItem); ok {
			texts = append(texts, m.Text)
		}
	}
	require.Equal(t, []string{"old"}, texts)
}

// 918 на private неотличим от несуществующего; архив; закрытие после вступления.
func TestPublic918PrivateIndistinguishableFromMissing(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	priv := r.group(t, a)
	missing := r.joinPublic(t, b, "no-such-conversation")
	onPrivate := r.joinPublic(t, b, priv)
	require.Equal(t, convErrConversationNF, missing.Error)
	require.Equal(t, missing, onPrivate)
	require.Zero(t, r.events(t, priv, "member_joined"))

	// creating-разговор тоже не виден.
	arch := r.publicGroup(t, a, "arch")
	r.mustDo(t, a, ActionConversationArchive, reqOf(arch))
	require.Equal(t, convErrArchived, r.joinPublic(t, b, arch).Error)

	// Стал private — снова not_found.
	pub := r.publicGroup(t, a, "flip")
	r.mustDo(t, a, ActionConversationUpdate, reqOf(pub, "visibility", "private"))
	require.Equal(t, convErrConversationNF, r.joinPublic(t, b, pub).Error)
}

// Гонка двух 918: два разных периода, member_count +2.
func TestPublic918ParallelJoinTwoPeriods(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.publicGroup(t, a, "race")
	users := []string{r.id.person(pidB), r.id.person(pidC)}
	var wg sync.WaitGroup
	res := make([]Response, len(users))
	for i := range users {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				res[i] = r.joinPublic(t, users[i], id)
				if res[i].Error != convErrTemporarilyUnavail {
					return
				}
			}
		}(i)
	}
	wg.Wait()
	for _, x := range res {
		require.True(t, x.OK, "%+v", x)
	}
	require.Equal(t, 3, r.count(t, `SELECT member_count FROM engram_conversation WHERE conversation_id = $1`, id))
	require.Equal(t, 3, r.count(t, `SELECT count(*) FROM engram_member_period WHERE conversation_id = $1 AND state = 'active'`, id))
	require.Equal(t, 2, r.events(t, id, "member_joined"))
	require.Equal(t, int64(3), r.version(t, id), "создание 1 + два вступления")
}

// Выход и повторное вступление — новый период.
func TestPublic918RejoinOpensNewPeriod(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.publicGroup(t, a, "again")
	r.mustDo(t, b, ActionConversationJoinPublic, map[string]any{"request_id": newRequestID(), "conversation_id": id})
	r.mustDo(t, b, ActionConversationLeave, reqOf(id))
	r.mustDo(t, b, ActionConversationJoinPublic, map[string]any{"request_id": newRequestID(), "conversation_id": id})
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2`, id, pidB))
}
