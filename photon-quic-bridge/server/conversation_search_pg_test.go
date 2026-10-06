package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// M9: 941 CONVERSATION_SEARCH и индекс текста engram_item_text против
// НАСТОЯЩЕГО PostgreSQL (ADR-110; messaging-v2-contract.md §5b.3).
// PHOTON_DM_PG_DSN.

func (r *groupRig) searchRaw(t *testing.T, caller string, req map[string]any) Response {
	t.Helper()
	return r.svc.dispatch(context.Background(), caller, ActionConversationSearch, pl(t, req))
}

func (r *groupRig) search(t *testing.T, caller string, kv ...any) ConvSearchResponse {
	t.Helper()
	req := map[string]any{}
	for i := 0; i+1 < len(kv); i += 2 {
		req[kv[i].(string)] = kv[i+1]
	}
	resp := r.searchRaw(t, caller, req)
	require.True(t, resp.OK, "941: %+v", resp)
	return resp.Data.(ConvSearchResponse)
}

// texts — тексты найденных сообщений в порядке выдачи.
func hitTexts(t *testing.T, resp ConvSearchResponse) []string {
	t.Helper()
	var out []string
	for _, h := range resp.Hits {
		m, ok := h.Item.(ConvMessageItem)
		require.True(t, ok, "элемент поиска — сообщение: %+v", h.Item)
		out = append(out, m.Text)
	}
	return out
}

func hitIDs(resp ConvSearchResponse) []string {
	var out []string
	for _, h := range resp.Hits {
		out = append(out, itemID(h.Item))
	}
	return out
}

// since_join: вне периодов не находит (до вступления и между периодами),
// внутри — находит; участник с полной историей находит всё.
func TestM9SearchSinceJoinPeriods(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB)
	s := func(text string) { r.mustSend(t, a, id, newRequestID(), text) }
	s("alpha1")
	s("alpha2")
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
	s("alpha3")
	r.mustDo(t, c, ActionConversationLeave, reqOf(id))
	s("alpha4")
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
	s("alpha5")

	require.Equal(t, []string{"alpha5", "alpha3"}, hitTexts(t, r.search(t, c, "query", "alpha")))
	require.Equal(t, []string{"alpha5", "alpha3"}, hitTexts(t, r.search(t, c, "query", "alpha", "conversation_id", id)))
	require.Equal(t, []string{"alpha5", "alpha4", "alpha3", "alpha2", "alpha1"}, hitTexts(t, r.search(t, a, "query", "alpha")))

	// full — вся лента, включая сообщения до вступления.
	req := createReq(newRequestID(), "full", pidB)
	req["history_policy"] = "full"
	fid := r.mustCreate(t, a, req).Conversation.ConversationID
	r.mustSend(t, a, fid, newRequestID(), "beta1")
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(fid, "photon_ids", []string{pidC}))
	require.Equal(t, []string{"beta1"}, hitTexts(t, r.search(t, c, "query", "beta")))
}

// Чужие разговоры не находятся; фильтр по чужому — conversation_not_found;
// бывший участник (вышел) разговор не ищет; в архиве участник ищет.
func TestM9SearchAccess(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	mine := r.group(t, a, pidB)
	other := r.group(t, c)
	r.mustSend(t, a, mine, newRequestID(), "общий секрет")
	r.mustSend(t, c, other, newRequestID(), "чужой секрет")

	require.Equal(t, []string{"общий секрет"}, hitTexts(t, r.search(t, a, "query", "секрет")))
	require.Equal(t, []string{"чужой секрет"}, hitTexts(t, r.search(t, c, "query", "секрет")))
	resp := r.searchRaw(t, a, map[string]any{"query": "секрет", "conversation_id": other})
	require.False(t, resp.OK)
	require.Equal(t, convErrConversationNF, resp.Error)
	resp = r.searchRaw(t, a, map[string]any{"query": "секрет", "conversation_id": "no-such"})
	require.Equal(t, convErrConversationNF, resp.Error)

	// Фильтр по одному разговору из нескольких.
	r.mustDo(t, c, ActionConversationMembersAdd, reqOf(other, "photon_ids", []string{pidA}))
	r.mustSend(t, a, other, newRequestID(), "второй секрет")
	require.Len(t, r.search(t, a, "query", "секрет").Hits, 2)
	only := r.search(t, a, "query", "секрет", "conversation_id", mine)
	require.Len(t, only.Hits, 1)
	require.Equal(t, mine, only.Hits[0].ConversationID)

	// Вышедший участник: разговор не ищется (доступ как у 931).
	r.mustDo(t, b, ActionConversationLeave, reqOf(mine))
	require.Empty(t, r.search(t, b, "query", "секрет").Hits)

	// Архив: участник ищет по-прежнему.
	r.mustDo(t, a, ActionConversationArchive, reqOf(mine))
	require.Len(t, r.search(t, a, "query", "общий").Hits, 1)
}

// Удалённое не находится (строка индекса удалена в той же транзакции);
// правка: старый текст не находится, новый — находится и отдаётся.
func TestM9SearchDeletedAndEdited(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	del := r.mustSend(t, a, id, newRequestID(), "удалимое слово")
	ed := r.mustSend(t, a, id, newRequestID(), "старый текст")
	r.mustSend(t, a, id, newRequestID(), "слово остаётся")
	require.Len(t, r.search(t, a, "query", "слово").Hits, 2)

	r.mustDel(t, a, id, del.ID)
	require.Zero(t, r.count(t, `SELECT count(*) FROM engram_item_text WHERE item_id = $1`, del.ID), "933 удаляет строку индекса")
	require.Equal(t, []string{"слово остаётся"}, hitTexts(t, r.search(t, a, "query", "слово")))

	r.mustEdit(t, a, id, ed.ID, "новая редакция")
	require.Empty(t, r.search(t, a, "query", "старый").Hits)
	got := r.search(t, a, "query", "редакция")
	require.Equal(t, []string{"новая редакция"}, hitTexts(t, got))
	require.Equal(t, ed.ID, itemID(got.Hits[0].Item))
	require.Equal(t, ed.CreatedAt, itemAt(got.Hits[0].Item))
	// Вторая правка заменяет первую.
	r.mustEdit(t, a, id, ed.ID, "третий вариант")
	require.Empty(t, r.search(t, a, "query", "редакция").Hits)
	require.Len(t, r.search(t, a, "query", "вариант").Hits, 1)
}

// Регистр и ё ≡ е; метасимволы LIKE — буквально; пробелы по краям обрезаются.
func TestM9SearchNormalisation(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	r.mustSend(t, a, id, newRequestID(), "Ёлка и ПРИВЕТ")
	r.mustSend(t, a, id, newRequestID(), "скидка 50% до_пятницы")
	r.mustSend(t, a, id, newRequestID(), "скидка 5000 доXпятницы")

	for _, q := range []string{"елка", "ЁЛКА", "ёлка", "привет", "Привет", "  привет  "} {
		require.Len(t, r.search(t, a, "query", q).Hits, 1, q)
	}
	require.Len(t, r.search(t, a, "query", "50%").Hits, 1, "% — буквально")
	require.Len(t, r.search(t, a, "query", "до_п").Hits, 1, "_ — буквально")
	require.Len(t, r.search(t, a, "query", "скидка").Hits, 2)
}

// Страницы назад: без дублей и пропусков, на двух разговорах, с совпадающими
// created_at невозможно (позиции уникальны в разговоре), поэтому якорь —
// пара (created_at, id).
func TestM9SearchPagination(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	g1 := r.group(t, a, pidB)
	g2 := r.group(t, a, pidB)
	var want []string
	for i := 0; i < 23; i++ {
		g := g1
		if i%3 == 0 {
			g = g2
		}
		m := r.mustSend(t, a, g, newRequestID(), fmt.Sprintf("пункт %02d", i))
		want = append([]string{m.ID}, want...) // свежие первыми
		if i%5 == 0 {
			r.mustSend(t, a, g, newRequestID(), "посторонний") // не совпадает с запросом
		}
	}
	for _, limit := range []int{1, 4, 7, 23, 50} {
		var got []string
		var before any
		for pages := 0; pages < 100; pages++ {
			req := map[string]any{"query": "пункт", "limit": limit}
			if before != nil {
				req["before"] = before
			}
			resp := r.searchRaw(t, a, req)
			require.True(t, resp.OK, "%+v", resp)
			page := resp.Data.(ConvSearchResponse)
			require.LessOrEqual(t, len(page.Hits), limit)
			got = append(got, hitIDs(page)...)
			if page.NextBefore == nil {
				break
			}
			require.Len(t, page.Hits, limit, "next_before только при неполной выдаче")
			before = map[string]any{"created_at": page.NextBefore.CreatedAt, "id": page.NextBefore.ID}
		}
		require.Equal(t, want, got, "limit %d", limit)
	}
}

// Пределы запроса и порядок проверок.
func TestM9SearchValidation(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	bad := func(req map[string]any, want string) {
		t.Helper()
		resp := r.searchRaw(t, a, req)
		require.False(t, resp.OK, "%v", req)
		require.Equal(t, want, resp.Error, "%v", req)
	}
	bad(map[string]any{}, convErrInvalidRequest)
	bad(map[string]any{"query": "а"}, convErrInvalidRequest)
	bad(map[string]any{"query": "   б  "}, convErrInvalidRequest)
	bad(map[string]any{"query": strings.Repeat("я", 65)}, convErrInvalidRequest)
	bad(map[string]any{"query": "ok", "limit": 0}, convErrInvalidLimit)
	bad(map[string]any{"query": "ok", "limit": 51}, convErrInvalidLimit)
	bad(map[string]any{"query": "ok", "bogus": 1}, convErrInvalidRequest)
	bad(map[string]any{"query": "ok", "before": map[string]any{"created_at": 0, "id": "x"}}, convErrInvalidRequest)
	bad(map[string]any{"query": "ok", "before": map[string]any{"created_at": 5, "id": "BAD ID"}}, convErrInvalidRequest)
	// Доступ раньше содержания (ADR-099 §11): чужой разговор с плохим якорем.
	bad(map[string]any{"query": "ok", "conversation_id": "nope", "before": map[string]any{"created_at": 0, "id": "x"}}, convErrConversationNF)
	require.True(t, r.searchRaw(t, a, map[string]any{"query": strings.Repeat("я", 64), "limit": 50, "conversation_id": id}).OK)
	// Нет совпадений — пустая выдача, не ошибка.
	require.Empty(t, r.search(t, a, "query", "нетакого").Hits)
}

// Сообщение, опубликованное сверкой после неопределённого исхода CreatePost,
// находится.
func TestM9SearchReconcilePublished(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	r.failNextPost(func(m *fakeConvMM, n int) { m.postCreateThenFailOnCall = n })
	require.False(t, r.send(t, a, id, newRequestID(), "потерянный ответ").OK)
	require.Empty(t, r.search(t, a, "query", "потерянный").Hits, "до сверки сообщение не опубликовано")
	res, err := r.svc.reconcileConversation(context.Background(), id, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, 1, res.Published)
	require.Equal(t, []string{"потерянный ответ"}, hitTexts(t, r.search(t, a, "query", "потерянный")))
}

// Миграция v9 заполняет индекс для уже опубликованных сообщений: с последним
// текстом правки, без удалённых.
func TestM9MigrationBackfillsPublishedMessages(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	keep := r.mustSend(t, a, id, newRequestID(), "Старое Сообщение")
	ed := r.mustSend(t, a, id, newRequestID(), "до правки")
	gone := r.mustSend(t, a, id, newRequestID(), "старое удалённое")
	r.mustEdit(t, a, id, ed.ID, "после правки старое")
	r.mustDel(t, a, id, gone.ID)

	// Состояние «до v9»: индекса нет, версия схемы 8.
	_, err := r.db.Exec(`DROP TABLE engram_item_text`)
	require.NoError(t, err)
	_, err = r.db.Exec(`UPDATE engram_schema SET version = 8`)
	require.NoError(t, err)
	require.NoError(t, migrateConversationSchema(context.Background(), r.db, 5*time.Second, 10*time.Millisecond))
	require.NoError(t, checkConversationSchema(context.Background(), r.db, "postgres"))

	got := r.search(t, a, "query", "старое")
	require.ElementsMatch(t, []string{keep.ID, ed.ID}, hitIDs(got))
	require.ElementsMatch(t, []string{"Старое Сообщение", "после правки старое"}, hitTexts(t, got))
	require.Empty(t, r.search(t, a, "query", "до правки").Hits)
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM engram_item_text WHERE conversation_id = $1`, id))
}

// Замер: 100 000 сообщений (PHOTON_SEARCH_BENCH=1), время ответа 941 в журнал.
func TestM9SearchBench100k(t *testing.T) {
	if os.Getenv("PHOTON_SEARCH_BENCH") == "" {
		t.Skip("замер 941 на 100 000 сообщений: PHOTON_SEARCH_BENCH=1")
	}
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	const n = 100000
	var channel string
	require.NoError(t, r.db.QueryRow(`SELECT carrier_channel_id FROM engram_conversation WHERE conversation_id = $1`, id).Scan(&channel))
	base := time.Now().UnixMilli() + 1_000_000
	_, err := r.db.Exec(`INSERT INTO posts (id, channelid, userid, createat, updateat, deleteat, message, type, props)
SELECT 'bp' || g, $1, 'u', $2::bigint + g, $2::bigint + g, 0, 'Сообщение номер ' || g || CASE WHEN g % 100 = 0 THEN ' needle' ELSE '' END, '', '{}'::jsonb
FROM generate_series(1, $3::int) g`, channel, base, n)
	require.NoError(t, err)
	_, err = r.db.Exec(`INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, sender, reserved_at, post_id)
SELECT $1, $2::bigint + g, 'message', 'k' || g, 'published', $4, 1, 'bp' || g FROM generate_series(1, $3::int) g`, id, base, n, pidA)
	require.NoError(t, err)
	_, err = r.db.Exec(`INSERT INTO engram_item_text (item_id, conversation_id, created_at, text_norm)
SELECT 'bp' || g, $1, $2::bigint + g, lower('Сообщение номер ' || g || CASE WHEN g % 100 = 0 THEN ' needle' ELSE '' END) FROM generate_series(1, $3::int) g`, id, base, n)
	require.NoError(t, err)
	_, err = r.db.Exec(`ANALYZE engram_item_text; ANALYZE engram_position; ANALYZE posts`)
	require.NoError(t, err)

	for _, q := range []string{"needle", "номер 99"} {
		for i := 0; i < 3; i++ {
			start := time.Now()
			resp := r.search(t, a, "query", q, "limit", 50)
			d := time.Since(start)
			require.NotEmpty(t, resp.Hits)
			t.Logf("941 %q, %d сообщений: %v, hits=%d", q, n, d, len(resp.Hits))
		}
	}
}
