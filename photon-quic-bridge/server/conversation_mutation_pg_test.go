package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// M8: 932 MESSAGE_EDIT, 933 MESSAGE_DELETE, reply_to и forwarded_from в 930
// против НАСТОЯЩЕГО PostgreSQL (ADR-109 без реакций; messaging-v2-contract.md
// §5b.2). PHOTON_DM_PG_DSN.

func editReq(convID, itemID, text string) map[string]any {
	return reqOf(convID, "item_id", itemID, "text", text)
}

func delReq(convID, itemID string) map[string]any {
	return reqOf(convID, "item_id", itemID)
}

func (r *groupRig) edit(t *testing.T, caller, convID, itemID, text string) Response {
	t.Helper()
	return r.do(t, caller, ActionConversationMessageEdit, editReq(convID, itemID, text))
}

func (r *groupRig) del(t *testing.T, caller, convID, itemID string) Response {
	t.Helper()
	return r.do(t, caller, ActionConversationMessageDelete, delReq(convID, itemID))
}

func mutOf(t *testing.T, resp Response) ConvEventItem {
	t.Helper()
	require.True(t, resp.OK, "мутация: %+v", resp)
	return resp.Data.(ConvMutationResponse).Event
}

func (r *groupRig) mustEdit(t *testing.T, caller, convID, itemID, text string) ConvEventItem {
	t.Helper()
	return mutOf(t, r.edit(t, caller, convID, itemID, text))
}

func (r *groupRig) mustDel(t *testing.T, caller, convID, itemID string) ConvEventItem {
	t.Helper()
	return mutOf(t, r.del(t, caller, convID, itemID))
}

func (r *groupRig) sendWith(t *testing.T, caller, convID, text string, extra map[string]any) Response {
	t.Helper()
	req := sendReq(convID, newRequestID(), text)
	for k, v := range extra {
		req[k] = v
	}
	return r.do(t, caller, ActionConversationSend, req)
}

// eventsOf — события ленты вида name, в порядке ленты.
func eventsOf(items []any, name string) []ConvEventItem {
	var out []ConvEventItem
	for _, it := range items {
		if e, ok := it.(ConvEventItem); ok && e.Event == name {
			out = append(out, e)
		}
	}
	return out
}

func (r *groupRig) postLive(t *testing.T, postID string) bool {
	t.Helper()
	return r.count(t, `SELECT count(*) FROM posts WHERE id = $1 AND deleteat = 0`, postID) == 1
}

// Свой текст править можно: событие на новой позиции, версия разговора не
// растёт, ответ — событие как элемент ленты с id поста; 931 отдаёт то же
// событие, исходное сообщение сохраняет исходный текст.
func TestM8EditByAuthor(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "исходный")
	v0 := r.version(t, id)

	ev := r.mustEdit(t, a, id, m.ID, "правка")
	require.Equal(t, "message_edited", ev.Event)
	require.Equal(t, m.ID, ev.TargetID)
	require.Equal(t, "правка", ev.Text)
	require.Equal(t, 1, ev.EditVersion)
	require.Equal(t, pidA, ev.ActorPhotonID)
	require.NotEmpty(t, ev.ID)
	require.Greater(t, ev.CreatedAt, m.CreatedAt)
	require.Equal(t, v0, ev.ConversationVersion, "событие несёт текущую версию")
	require.Equal(t, v0, r.version(t, id), "событие элемента версию разговора не меняет (ADR-099 §10.1)")

	for _, who := range []string{a, b} {
		items := r.fullPass(t, who, id, 50)
		got := eventsOf(items, "message_edited")
		require.Len(t, got, 1)
		require.Equal(t, ev, got[0], "931 отдаёт то же событие, что вернул 932")
		var orig ConvMessageItem
		for _, it := range items {
			if mi, ok := it.(ConvMessageItem); ok && mi.ID == m.ID {
				orig = mi
			}
		}
		require.Equal(t, "исходный", orig.Text, "пост-носитель правкой не меняется")
	}
	require.Equal(t, 1, r.count(t, `SELECT edit_version FROM engram_position WHERE conversation_id = $1 AND post_id = $2`, id, m.ID))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_item_edit WHERE conversation_id = $1 AND item_id = $2 AND text = 'правка' AND edit_version = 1`, id, m.ID))
}

// Чужое сообщение править нельзя — даже owner и admin: правка только авторская.
func TestM8EditByNonAuthorForbidden(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB, pidC)
	r.mustDo(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidC, "role", "admin"))
	m := r.mustSend(t, b, id, newRequestID(), "от B")
	events := r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1`, id)

	for name, who := range map[string]string{"owner": a, "admin": c} {
		require.Equal(t, convErrForbidden, r.edit(t, who, id, m.ID, "чужая правка").Error, name)
	}
	require.Equal(t, events, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1`, id), "отказ событий не пишет")
	require.Zero(t, r.count(t, `SELECT count(*) FROM engram_item_edit WHERE conversation_id = $1`, id))
}

// Удаление: автор — своё; чужое — только messages.delete_any (owner, admin);
// member чужое удалить не может.
func TestM8DeletePermissions(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	d := r.id.person(pidD)
	id := r.group(t, a, pidB, pidC, pidD)
	r.mustDo(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidC, "role", "admin"))

	require.True(t, convHasPermission("owner", "messages.delete_any"))
	require.True(t, convHasPermission("admin", "messages.delete_any"))
	require.False(t, convHasPermission("member", "messages.delete_any"))

	mB := r.mustSend(t, b, id, newRequestID(), "B1")
	// member D чужое — нет.
	require.Equal(t, convErrForbidden, r.del(t, d, id, mB.ID).Error)
	require.True(t, r.postLive(t, mB.ID), "отказ ничего не удаляет")
	// автор — своё.
	ev := r.mustDel(t, b, id, mB.ID)
	require.Equal(t, "message_deleted", ev.Event)
	require.Equal(t, mB.ID, ev.TargetID)
	require.False(t, r.postLive(t, mB.ID), "пост-носитель удалён в Mattermost")
	require.Equal(t, 1, r.mm.deleteCalls)

	// owner и admin — чужое.
	mB2 := r.mustSend(t, b, id, newRequestID(), "B2")
	r.mustDel(t, a, id, mB2.ID)
	mB3 := r.mustSend(t, b, id, newRequestID(), "B3")
	r.mustDel(t, c, id, mB3.ID)
	// member B чужое owner'а — нет.
	mA := r.mustSend(t, a, id, newRequestID(), "A1")
	require.Equal(t, convErrForbidden, r.del(t, b, id, mA.ID).Error)
	require.True(t, r.postLive(t, mA.ID))
}

// Повтор ключа 932 и 933: тот же ответ, ни нового события, ни новой версии
// edit_version; тот же ключ с другим содержанием — idempotency_conflict.
func TestM8KeyReplay(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "m")

	req := editReq(id, m.ID, "e1")
	first := r.do(t, a, ActionConversationMessageEdit, req)
	again := r.do(t, a, ActionConversationMessageEdit, req)
	require.Equal(t, mutOf(t, first), mutOf(t, again))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND event->>'event' = 'message_edited'`, id))
	require.Equal(t, 1, r.count(t, `SELECT edit_version FROM engram_position WHERE conversation_id = $1 AND post_id = $2`, id, m.ID))
	other := editReq(id, m.ID, "e1")
	other["request_id"] = req["request_id"]
	other["text"] = "другой текст"
	require.Equal(t, convErrIdempotency, r.do(t, a, ActionConversationMessageEdit, other).Error)

	dreq := delReq(id, m.ID)
	d1 := r.do(t, a, ActionConversationMessageDelete, dreq)
	d2 := r.do(t, a, ActionConversationMessageDelete, dreq)
	require.Equal(t, mutOf(t, d1), mutOf(t, d2))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND event->>'event' = 'message_deleted'`, id))
	// Повтор по уже удалённому другим ключом — message_deleted.
	require.Equal(t, convErrMessageDeleted, r.del(t, a, id, m.ID).Error)
	// Ключ 932 и ключ 933 — разные пространства: тот же request_id не конфликтует.
	m2 := r.mustSend(t, a, id, newRequestID(), "m2")
	shared := newRequestID()
	e := editReq(id, m2.ID, "x")
	e["request_id"] = shared
	d := delReq(id, m2.ID)
	d["request_id"] = shared
	require.True(t, r.do(t, a, ActionConversationMessageEdit, e).OK)
	require.True(t, r.do(t, a, ActionConversationMessageDelete, d).OK)
}

// Правка удалённого — message_deleted (ADR-109 §2.7); и после правки удаление
// работает.
func TestM8EditOfDeletedIsMessageDeleted(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "m")
	r.mustEdit(t, a, id, m.ID, "правка до удаления")
	r.mustDel(t, a, id, m.ID)

	resp := r.edit(t, a, id, m.ID, "после удаления")
	require.False(t, resp.OK)
	require.Equal(t, convErrMessageDeleted, resp.Error)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND event->>'event' = 'message_edited'`, id))
	// Чужая правка удалённого — тоже message_deleted: у надгробия автора нет.
	require.Equal(t, convErrMessageDeleted, r.edit(t, b, id, m.ID, "x").Error)
}

// Две параллельные правки одного сообщения: обе события на разных позициях,
// edit_version 1 и 2, версия разговора не меняется, ключи хранения событий
// не совпали; последняя по позиции событий правка — текущий текст.
func TestM8ConcurrentEdits(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "m")
	v0 := r.version(t, id)

	var wg sync.WaitGroup
	evs := make([]ConvEventItem, 2)
	for i := range evs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := r.edit(t, a, id, m.ID, []string{"первая", "вторая"}[i])
			evs[i] = mutOf(t, resp)
		}(i)
	}
	wg.Wait()

	sort.Slice(evs, func(i, j int) bool { return evs[i].CreatedAt < evs[j].CreatedAt })
	require.Less(t, evs[0].CreatedAt, evs[1].CreatedAt)
	require.Equal(t, 1, evs[0].EditVersion)
	require.Equal(t, 2, evs[1].EditVersion)
	require.Equal(t, v0, r.version(t, id))
	got := eventsOf(r.fullPass(t, a, id, 50), "message_edited")
	require.Len(t, got, 2)
	require.Equal(t, evs[0], got[0])
	require.Equal(t, evs[1], got[1])
	var cur string
	require.NoError(t, r.db.QueryRow(`SELECT text FROM engram_item_edit WHERE conversation_id = $1 AND item_id = $2`, id, m.ID).Scan(&cur))
	require.Equal(t, evs[1].Text, cur, "последняя по позиции события правка — текущая")
	// Два события одной версии — разные seq и разные ключи позиций.
	require.Equal(t, 2, r.count(t, `SELECT count(DISTINCT seq) FROM engram_event WHERE conversation_id = $1 AND version = $2 AND event->>'event' = 'message_edited'`, id, v0))
	require.Zero(t, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND state <> 'written'`, id))
}

// Много событий элементов подряд при одной версии не сталкиваются на
// (version, seq) и на ключе позиции.
func TestM8ManyItemEventsSameVersion(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	v0 := r.version(t, id)
	for i := 0; i < 5; i++ {
		m := r.mustSend(t, a, id, newRequestID(), "m")
		r.mustEdit(t, a, id, m.ID, "e")
		r.mustDel(t, a, id, m.ID)
	}
	require.Equal(t, v0, r.version(t, id))
	require.Equal(t, 10, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND version = $2 AND seq >= 2`, id, v0))
	require.Zero(t, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND state <> 'written'`, id))
}

// 931, 940 и 902 отдают удалённое сообщение надгробием на его позиции: без
// автора, текста, ответа и пересылки; форма — ровно {kind, id, created_at,
// deleted}.
func TestM8TombstoneIn931And940And902(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	m1 := r.mustSend(t, a, id, newRequestID(), "до")
	secret := "секретный текст"
	m2 := r.mustSend(t, b, id, newRequestID(), secret)
	m3 := r.mustSend(t, a, id, newRequestID(), "после")
	r.mustDel(t, b, id, m2.ID)

	wantJSON, _ := json.Marshal(map[string]any{"kind": "message", "id": m2.ID, "created_at": m2.CreatedAt, "deleted": true})
	for _, who := range []string{a, b} {
		items := r.fullPass(t, who, id, 50)
		var seen bool
		for i, it := range items {
			if itemID(it) != m2.ID {
				continue
			}
			seen = true
			got, _ := json.Marshal(it)
			require.JSONEq(t, string(wantJSON), string(got), "931: надгробие")
			require.NotContains(t, string(got), secret)
			// на своей позиции: между m1 и m3
			require.Equal(t, m1.ID, itemID(items[i-1]))
			require.Equal(t, m3.ID, itemID(items[i+1]))
		}
		require.True(t, seen, "931: надгробие есть в ленте")
		// Первичная синхронизация (самые новые элементы) — то же.
		first := r.syncPage(t, who, id, "", 50)
		for _, it := range first.Items {
			if itemID(it) == m2.ID {
				got, _ := json.Marshal(it)
				require.JSONEq(t, string(wantJSON), string(got))
			}
		}
	}
	// 940: страница назад от m3 содержит надгробие.
	anchor := ConvMessageItem{ID: m3.ID, CreatedAt: m3.CreatedAt}
	page := r.histBefore(t, a, id, anchor, 10)
	var seen bool
	for _, it := range page.Items {
		if itemID(it) == m2.ID {
			seen = true
			got, _ := json.Marshal(it)
			require.JSONEq(t, string(wantJSON), string(got), "940: надгробие")
		}
	}
	require.True(t, seen, "940: надгробие есть в истории")
	// Текст не выдаётся нигде.
	all, _ := json.Marshal(r.fullPass(t, a, id, 50))
	require.NotContains(t, string(all), secret)
}

// 902 last_item — надгробие, когда событие удаления ещё не записано (позиция
// события — открытый резерв и держит барьер), а отметка удаления в БД уже
// есть: «отметка → событие → удаление поста» (contract §5b.2). Повтор того же
// ключа дописывает событие и удаляет пост.
func TestM8TombstoneWhenEventStillPending(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, b, id, newRequestID(), "будет удалено")

	r.mm.mu.Lock()
	r.mm.postFailOnCall = r.mm.postCalls + 1 // CreatePost поста события
	r.mm.mu.Unlock()
	req := delReq(id, m.ID)
	resp := r.do(t, b, ActionConversationMessageDelete, req)
	require.False(t, resp.OK)
	require.Equal(t, convErrTemporarilyUnavail, resp.Error)
	require.True(t, r.postLive(t, m.ID), "пост ещё не удалён: событие не записано")
	require.Equal(t, 0, r.mm.deleteCalls)

	li := r.listItem(t, a, id).LastItem
	require.IsType(t, ConvTombstoneItem{}, li, "902 last_item: надгробие")
	require.Equal(t, m.ID, li.(ConvTombstoneItem).ID)
	items := r.fullPass(t, a, id, 50)
	require.IsType(t, ConvTombstoneItem{}, items[len(items)-1], "931: надгробие, хотя пост ещё жив — из deleted_at")
	var dup int
	for _, it := range items {
		if itemID(it) == m.ID {
			dup++
		}
	}
	require.Equal(t, 1, dup, "один элемент на позицию: живой пост и надгробие не двоятся")

	// Повтор — дописывает событие, удаляет пост, ответ как у первого вызова.
	ev := mutOf(t, r.do(t, b, ActionConversationMessageDelete, req))
	require.Equal(t, "message_deleted", ev.Event)
	require.False(t, r.postLive(t, m.ID))
	require.Len(t, eventsOf(r.fullPass(t, a, id, 50), "message_deleted"), 1)
}

// Сбой DeletePost: элемент уже надгробие, ответ успешен, повтор ключа
// пробует удаление снова.
func TestM8DeletePostFailureKeepsTombstone(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "m")
	r.mm.mu.Lock()
	r.mm.deletePostFail = 1
	r.mm.mu.Unlock()
	req := delReq(id, m.ID)
	ev := mutOf(t, r.do(t, a, ActionConversationMessageDelete, req))
	require.True(t, r.postLive(t, m.ID), "DeletePost отказал — пост жив")
	var tomb int
	for _, it := range r.fullPass(t, a, id, 50) {
		if ts, ok := it.(ConvTombstoneItem); ok && ts.ID == m.ID {
			tomb++
		}
		if mi, ok := it.(ConvMessageItem); ok && mi.ID == m.ID {
			t.Fatal("тело удалённого сообщения выдано")
		}
	}
	require.Equal(t, 1, tomb)
	require.Equal(t, ev, mutOf(t, r.do(t, a, ActionConversationMessageDelete, req)))
	require.False(t, r.postLive(t, m.ID), "повтор ключа доудалил пост")
}

// Удалённое сообщение не входит в счётчик непрочитанного (M7).
func TestM8DeletedNotUnread(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	ms := r.sendN(t, a, id, "m", 3)
	require.Equal(t, 3, r.listItem(t, b, id).UnreadCount)
	// Пост остаётся живым — исключает именно отметка deleted_at.
	r.mm.mu.Lock()
	r.mm.deletePostFail = 1
	r.mm.mu.Unlock()
	r.mustDel(t, a, id, ms[1].ID)
	require.True(t, r.postLive(t, ms[1].ID))
	require.Equal(t, 2, r.listItem(t, b, id).UnreadCount)
	r.mustDel(t, a, id, ms[0].ID)
	require.Equal(t, 1, r.listItem(t, b, id).UnreadCount)
	// Правка событий не считает.
	r.mustEdit(t, a, id, ms[2].ID, "правка")
	require.Equal(t, 1, r.listItem(t, b, id).UnreadCount)
}

// since_join: событие правки и удаления о сообщении вне периода вызывающего
// не выдаётся — иначе правка раскрыла бы текст до вступления.
func TestM8SinceJoinHidesEventsAboutInvisible(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a)
	old := r.mustSend(t, a, id, newRequestID(), "до вступления B")
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB}))
	fresh := r.mustSend(t, a, id, newRequestID(), "после вступления B")

	r.mustEdit(t, a, id, old.ID, "ТАЙНАЯ правка старого")
	r.mustEdit(t, a, id, fresh.ID, "видимая правка нового")
	r.mustDel(t, a, id, old.ID)

	forB, _ := json.Marshal(r.fullPass(t, b, id, 50))
	require.NotContains(t, string(forB), "ТАЙНАЯ")
	require.NotContains(t, string(forB), old.ID)
	require.Len(t, eventsOf(r.fullPass(t, b, id, 50), "message_edited"), 1)
	require.Empty(t, eventsOf(r.fullPass(t, b, id, 50), "message_deleted"))
	require.Len(t, eventsOf(r.fullPass(t, a, id, 50), "message_edited"), 2, "автору видно всё")
	require.Len(t, eventsOf(r.fullPass(t, a, id, 50), "message_deleted"), 1)
	// B не может править и удалять невидимое: элемента для него нет.
	require.Equal(t, convErrInvalidRequest, r.edit(t, b, id, old.ID, "x").Error)
	require.Equal(t, convErrInvalidRequest, r.del(t, b, id, old.ID).Error)
	// 940 от первого видимого элемента не раскрывает событий о невидимом.
	page, _ := json.Marshal(r.histBefore(t, b, id, r.fullPass(t, b, id, 50)[len(r.fullPass(t, b, id, 50))-1], 50))
	require.NotContains(t, string(page), "ТАЙНАЯ")
}

// Видимость сообщения из чужого разговора: item_id другого разговора — не
// элемент этого, invalid_request; несуществующий — так же.
func TestM8ItemOfAnotherConversationInvalid(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id1 := r.group(t, a, pidB)
	id2 := r.group(t, a, pidB)
	m := r.mustSend(t, a, id1, newRequestID(), "в первом")
	require.Equal(t, convErrInvalidRequest, r.edit(t, a, id2, m.ID, "x").Error)
	require.Equal(t, convErrInvalidRequest, r.del(t, a, id2, m.ID).Error)
	require.Equal(t, convErrInvalidRequest, r.edit(t, a, id1, "нет-такого", "x").Error)
	// Событие — не сообщение: править и удалять его нельзя.
	var evPost string
	require.NoError(t, r.db.QueryRow(`SELECT post_id FROM engram_position WHERE conversation_id = $1 AND item_kind = 'event' ORDER BY pos LIMIT 1`, id1).Scan(&evPost))
	require.Equal(t, convErrInvalidRequest, r.edit(t, a, id1, evPost, "x").Error)
	require.Equal(t, convErrInvalidRequest, r.del(t, a, id1, evPost).Error)
}

// Порядок проверок ADR-099 §11: доступ → архив → право → содержание;
// форма запроса, пределы текста и их коды — как у 930.
func TestM8CheckOrderAndTextLimits(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "m")

	// форма
	for _, bad := range []map[string]any{
		{"conversation_id": id, "item_id": m.ID, "text": "x"},                          // нет request_id
		reqOf(id, "item_id", "", "text", "x"),                                          // пустой item_id
		reqOf(id, "text", "x"),                                                         // нет item_id
		reqOf(id, "item_id", m.ID, "text", "x", "extra", 1),                            // лишнее поле
		{"request_id": "не-uuid", "conversation_id": id, "item_id": m.ID, "text": "x"}, // плохой uuid
	} {
		require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationMessageEdit, bad).Error, "%v", bad)
	}
	require.Equal(t, convErrInvalidRequest, r.do(t, a, ActionConversationMessageDelete, reqOf(id)).Error)
	// пределы текста — коды 930
	require.Equal(t, errMessageEmpty, r.edit(t, a, id, m.ID, "   ").Error)
	require.Equal(t, errMessageEmpty, r.edit(t, a, id, m.ID, "").Error)
	long := make([]rune, directMessageMaxRunes+1)
	for i := range long {
		long[i] = 'я'
	}
	require.Equal(t, errMessageTooLong, r.edit(t, a, id, m.ID, string(long)).Error)
	require.Equal(t, errMessageTooManyLinks, r.edit(t, a, id, m.ID, "http://a.ru http://b.ru http://c.ru http://d.ru http://e.ru http://f.ru http://g.ru http://h.ru").Error)
	// доступ
	require.Equal(t, convErrConversationNF, r.edit(t, a, "нет-разговора", m.ID, "x").Error)
	require.Equal(t, convErrConversationNF, r.edit(t, c, id, m.ID, "x").Error, "не участник")
	require.Equal(t, convErrConversationNF, r.del(t, c, id, m.ID).Error)
	// ушедший участник — no_access
	r.mustDo(t, b, ActionConversationLeave, reqOf(id))
	require.Equal(t, convErrNoAccess, r.edit(t, b, id, m.ID, "x").Error)
	require.Equal(t, convErrNoAccess, r.del(t, b, id, m.ID).Error)
	// архив: сообщение не править и не удалять
	r.mustDo(t, a, ActionConversationArchive, reqOf(id))
	require.Equal(t, convErrArchived, r.edit(t, a, id, m.ID, "x").Error)
	require.Equal(t, convErrArchived, r.del(t, a, id, m.ID).Error)
	// повтор уже выполненного ключа отдаётся и в архиве (§10.2 первым)
}

// Повтор выполненного ключа — первым, независимо от состояния (ADR-099
// §10.2): после архивации тот же запрос отдаёт сохранённый ответ.
func TestM8ReplayBeforeStateChecks(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "m")
	req := editReq(id, m.ID, "e")
	first := mutOf(t, r.do(t, a, ActionConversationMessageEdit, req))
	r.mustDo(t, a, ActionConversationArchive, reqOf(id))
	require.Equal(t, first, mutOf(t, r.do(t, a, ActionConversationMessageEdit, req)))
}

// direct — так же, как группа: правка своего, отказ чужого, удаление,
// надгробие, message_deleted; messages.delete_any в direct нет ни у кого.
func TestM8DirectBehavesLikeGroup(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.mustOpenDirect(t, a, pidB).Conversation.ConversationID
	mA := r.mustSend(t, a, id, newRequestID(), "от A")
	mB := r.mustSend(t, b, id, newRequestID(), "от B")

	ev := r.mustEdit(t, a, id, mA.ID, "A правит")
	require.Equal(t, 1, ev.EditVersion)
	require.Equal(t, convErrForbidden, r.edit(t, a, id, mB.ID, "чужое").Error)
	require.Equal(t, convErrForbidden, r.del(t, a, id, mB.ID).Error)
	r.mustDel(t, b, id, mB.ID)
	require.Equal(t, convErrMessageDeleted, r.edit(t, b, id, mB.ID, "x").Error)

	items := r.fullPass(t, a, id, 50)
	var tomb bool
	for _, it := range items {
		if ts, ok := it.(ConvTombstoneItem); ok && ts.ID == mB.ID {
			tomb = true
		}
	}
	require.True(t, tomb)
	require.Len(t, eventsOf(items, "message_edited"), 1)
	require.Len(t, eventsOf(items, "message_deleted"), 1)
	// ответ в direct
	resp := r.sendWith(t, b, id, "ответ", map[string]any{"reply_to": mA.ID})
	require.Equal(t, mA.ID, msgOf(t, resp).ReplyTo)
}

// 930 без новых полей — прежнее поведение: хэш ключа — SHA-256 текста, ответ
// и элемент без reply_to и forwarded_from (ни в JSON, ни в props поста).
func TestM8SendWithoutNewFieldsUnchanged(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	cid := newRequestID()
	m := r.mustSend(t, a, id, cid, "обычное")

	sum := sha256.Sum256([]byte("обычное"))
	rows := r.msgRows(t, id, cid)
	require.Len(t, rows, 1)
	require.Equal(t, hex.EncodeToString(sum[:]), rows[0].Hash, "хэш ключа без новых полей — SHA-256 текста, как до ADR-109")
	raw, _ := json.Marshal(m)
	require.NotContains(t, string(raw), "reply_to")
	require.NotContains(t, string(raw), "forwarded_from")
	require.Zero(t, r.count(t, `SELECT count(*) FROM posts WHERE props ? 'engram_reply_to' OR props ? 'engram_forwarded_from'`))
	for _, it := range r.fullPass(t, a, id, 50) {
		b, _ := json.Marshal(it)
		require.NotContains(t, string(b), "reply_to")
		require.NotContains(t, string(b), "forwarded_from")
	}
	// Повтор тем же ключом и текстом — тот же ответ (старый клиент).
	require.Equal(t, m, r.mustSend(t, a, id, cid, "обычное"))
}

// reply_to: элемент-сообщение этого разговора; в ответе 930 и в элементе
// ленты; повтор ключа возвращает то же; другой reply_to под тем же ключом —
// idempotency_conflict.
func TestM8ReplyTo(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	target := r.mustSend(t, a, id, newRequestID(), "вопрос")
	other := r.mustSend(t, a, id, newRequestID(), "другое")

	cid := newRequestID()
	req := sendReq(id, cid, "ответ")
	req["reply_to"] = target.ID
	resp := r.do(t, b, ActionConversationSend, req)
	m := msgOf(t, resp)
	require.Equal(t, target.ID, m.ReplyTo)
	require.Equal(t, m, msgOf(t, r.do(t, b, ActionConversationSend, req)), "повтор ключа")
	req2 := sendReq(id, cid, "ответ")
	req2["reply_to"] = other.ID
	require.Equal(t, convErrIdempotency, r.do(t, b, ActionConversationSend, req2).Error)
	req3 := sendReq(id, cid, "ответ") // без reply_to — другое содержание
	require.Equal(t, convErrIdempotency, r.do(t, b, ActionConversationSend, req3).Error)

	var got ConvMessageItem
	for _, it := range r.fullPass(t, a, id, 50) {
		if mi, ok := it.(ConvMessageItem); ok && mi.ID == m.ID {
			got = mi
		}
	}
	require.Equal(t, target.ID, got.ReplyTo)
	require.Nil(t, got.ForwardedFrom)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM posts WHERE id = $1 AND props->>'engram_reply_to' = $2`, m.ID, target.ID))
	// Ответ на ответ и на собственное сообщение.
	require.Equal(t, m.ID, msgOf(t, r.sendWith(t, a, id, "ещё", map[string]any{"reply_to": m.ID})).ReplyTo)
	// Ответ на сообщение с правкой допустим; на событие — нет.
	r.mustEdit(t, a, id, target.ID, "вопрос (правка)")
	require.Equal(t, target.ID, msgOf(t, r.sendWith(t, b, id, "после правки", map[string]any{"reply_to": target.ID})).ReplyTo)
}

// reply_to на элемент другого разговора, несуществующий, событие, удалённое
// сообщение, пустую строку — invalid_request; пост не создаётся, резерв не
// остаётся открытым.
func TestM8ReplyToInvalid(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id1 := r.group(t, a, pidB)
	id2 := r.group(t, a, pidB)
	foreign := r.mustSend(t, a, id2, newRequestID(), "в другом разговоре")
	deleted := r.mustSend(t, a, id1, newRequestID(), "удалю")
	r.mustDel(t, a, id1, deleted.ID)
	var evPost string
	require.NoError(t, r.db.QueryRow(`SELECT post_id FROM engram_position WHERE conversation_id = $1 AND item_kind = 'event' ORDER BY pos LIMIT 1`, id1).Scan(&evPost))

	calls := r.postCallCount()
	for name, target := range map[string]string{
		"другой разговор": foreign.ID, "нет такого": "нет-такого", "событие": evPost, "удалённое": deleted.ID, "пустая строка": "",
	} {
		resp := r.sendWith(t, a, id1, "ответ", map[string]any{"reply_to": target})
		require.False(t, resp.OK, name)
		require.Equal(t, convErrInvalidRequest, resp.Error, name)
	}
	require.Equal(t, calls, r.postCallCount(), "CreatePost не вызывался")
	require.Zero(t, r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND state = 'reserved'`, id1), "открытых резервов нет")
}

// since_join: reply_to на сообщение вне периода отправителя — invalid_request.
func TestM8ReplyToInvisibleInvalid(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a)
	old := r.mustSend(t, a, id, newRequestID(), "до B")
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB}))
	resp := r.sendWith(t, b, id, "ответ", map[string]any{"reply_to": old.ID})
	require.Equal(t, convErrInvalidRequest, resp.Error)
	require.True(t, r.sendWith(t, a, id, "а автору видно", map[string]any{"reply_to": old.ID}).OK)
}

// Повтор уже опубликованного 930 с reply_to не зависит от судьбы цели.
func TestM8ReplyReplayAfterTargetDeleted(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	target := r.mustSend(t, a, id, newRequestID(), "цель")
	cid := newRequestID()
	req := sendReq(id, cid, "ответ")
	req["reply_to"] = target.ID
	m := msgOf(t, r.do(t, a, ActionConversationSend, req))
	r.mustDel(t, a, id, target.ID)
	require.Equal(t, m, msgOf(t, r.do(t, a, ActionConversationSend, req)))
}

// forwarded_from: проверяется только форма; в элементе ленты и ответе —
// как передано; без ссылки на исходный разговор.
func TestM8ForwardedFrom(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)

	fwd := map[string]any{"author_photon_id": pidC, "created_at": 1700000000000}
	cid := newRequestID()
	req := sendReq(id, cid, "переслано")
	req["forwarded_from"] = fwd
	m := msgOf(t, r.do(t, a, ActionConversationSend, req))
	require.Equal(t, &ConvForwardedFrom{AuthorPhotonID: pidC, CreatedAt: 1700000000000}, m.ForwardedFrom)
	require.Equal(t, m, msgOf(t, r.do(t, a, ActionConversationSend, req)), "повтор ключа")
	var got ConvMessageItem
	for _, it := range r.fullPass(t, a, id, 50) {
		if mi, ok := it.(ConvMessageItem); ok && mi.ID == m.ID {
			got = mi
		}
	}
	require.Equal(t, m.ForwardedFrom, got.ForwardedFrom)
	require.Equal(t, "переслано", got.Text)
	other := sendReq(id, cid, "переслано")
	other["forwarded_from"] = map[string]any{"author_photon_id": pidD, "created_at": 1700000000000}
	require.Equal(t, convErrIdempotency, r.do(t, a, ActionConversationSend, other).Error)

	// Неверная форма — invalid_request, пост не создаётся.
	calls := r.postCallCount()
	for name, bad := range map[string]any{
		"не канонический id": map[string]any{"author_photon_id": "abc", "created_at": 1},
		"id с пробелами":     map[string]any{"author_photon_id": "100 000 001", "created_at": 1},
		"нет id":             map[string]any{"created_at": 1},
		"created_at 0":       map[string]any{"author_photon_id": pidC, "created_at": 0},
		"created_at < 0":     map[string]any{"author_photon_id": pidC, "created_at": -5},
		"нет created_at":     map[string]any{"author_photon_id": pidC},
		"лишнее поле":        map[string]any{"author_photon_id": pidC, "created_at": 1, "conversation_id": id},
		"не объект":          "100000003",
	} {
		resp := r.sendWith(t, a, id, "x", map[string]any{"forwarded_from": bad})
		require.Equal(t, convErrInvalidRequest, resp.Error, name)
	}
	require.Equal(t, calls, r.postCallCount())
	// Пересылка вместе с ответом.
	both := r.sendWith(t, a, id, "оба", map[string]any{"forwarded_from": fwd, "reply_to": m.ID})
	bm := msgOf(t, both)
	require.Equal(t, m.ID, bm.ReplyTo)
	require.NotNil(t, bm.ForwardedFrom)
}

// Удалённое сообщение с ответом и пересылкой — надгробие без этих полей.
func TestM8TombstoneHidesReplyAndForward(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	base := r.mustSend(t, a, id, newRequestID(), "база")
	m := msgOf(t, r.sendWith(t, a, id, "ответ-пересылка", map[string]any{
		"reply_to": base.ID, "forwarded_from": map[string]any{"author_photon_id": pidB, "created_at": 5}}))
	r.mustDel(t, a, id, m.ID)
	for _, it := range r.fullPass(t, a, id, 50) {
		if itemID(it) != m.ID {
			continue
		}
		b, _ := json.Marshal(it)
		require.JSONEq(t, `{"kind":"message","id":"`+m.ID+`","created_at":`+itoa(m.CreatedAt)+`,"deleted":true}`, string(b))
	}
}

func itoa(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}
