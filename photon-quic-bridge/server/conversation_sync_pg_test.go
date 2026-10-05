package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
)

// 931 SYNC против НАСТОЯЩЕГО PostgreSQL (implementation contract §4, §6.2
// P1/P3/P5, §6.4 L3; ADR-099 §9, ADR-101 §5.1b, ADR-103 §3). PHOTON_DM_PG_DSN.
// Отрицательные контроли выполняют сломанный вариант оператора через
// runConvFeed (текст оператора — параметр) и обязаны дать расхождение.

// ── помощники ────────────────────────────────────────────────────────────────

func (r *groupRig) syncRaw(t *testing.T, callerMM string, req map[string]any) Response {
	t.Helper()
	return r.svc.dispatch(context.Background(), callerMM, ActionConversationSync, pl(t, req))
}

func (r *groupRig) syncPage(t *testing.T, callerMM, convID, cursor string, limit int) ConvSyncResponse {
	t.Helper()
	req := map[string]any{"conversation_id": convID}
	if cursor != "" {
		req["cursor"] = cursor
	}
	if limit > 0 {
		req["limit"] = limit
	}
	resp := r.syncRaw(t, callerMM, req)
	require.True(t, resp.OK, "931: %+v", resp)
	return resp.Data.(ConvSyncResponse)
}

func itemID(it any) string {
	switch v := it.(type) {
	case ConvMessageItem:
		return v.ID
	case ConvEventItem:
		return v.ID
	}
	return ""
}

func itemAt(it any) int64 {
	switch v := it.(type) {
	case ConvMessageItem:
		return v.CreatedAt
	case ConvEventItem:
		return v.CreatedAt
	}
	return 0
}

// itemName — короткое имя элемента для сравнений: «event:<вид>» или
// «msg:<текст>».
func itemName(it any) string {
	switch v := it.(type) {
	case ConvMessageItem:
		return "msg:" + v.Text
	case ConvEventItem:
		return "event:" + v.Event
	}
	return "?"
}

func names(items []any) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = itemName(it)
	}
	return out
}

// startCursor — курсор «начало прохода без водяного знака» (ADR-092
// §2.2.2): клиент таким не пользуется, тесту он даёт полный проход ленты
// страницами заданного размера.
func startCursor(convID string) string {
	return encodeCursor(dmCursor{V: dmCursorVersion, B: convBinding(convID)})
}

// fullPass — вся видимая лента страницами по limit, от начала.
func (r *groupRig) fullPass(t *testing.T, callerMM, convID string, limit int) []any {
	t.Helper()
	var all []any
	seen := map[string]bool{}
	cursor := startCursor(convID)
	for i := 0; i < 10000; i++ {
		pg := r.syncPage(t, callerMM, convID, cursor, limit)
		require.LessOrEqual(t, len(pg.Items), limit)
		for _, it := range pg.Items {
			if !seen[itemID(it)] {
				seen[itemID(it)] = true
				all = append(all, it)
			}
		}
		if pg.AtEnd {
			return all
		}
		cursor = pg.NextCursor
	}
	t.Fatal("проход не закончился")
	return nil
}

func (r *groupRig) carrier(t *testing.T, convID string) string {
	t.Helper()
	var ch string
	require.NoError(t, r.db.QueryRow(`SELECT carrier_channel_id FROM engram_conversation WHERE conversation_id = $1`, convID).Scan(&ch))
	return ch
}

// ── контракт формы ───────────────────────────────────────────────────────────

func TestConv931Contract(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)
	id := r.group(t, a, pidB)
	cidA, cidB := newRequestID(), newRequestID()
	m1 := r.mustSend(t, a, id, cidA, "от A")
	m2 := r.mustSend(t, b, id, cidB, "от B")

	// Первичная синхронизация: события и сообщения одной шкалой.
	pg := r.syncPage(t, a, id, "", 0)
	require.True(t, pg.AtEnd)
	require.Nil(t, pg.From, "у первичной синхронизации from нет")
	require.NotEmpty(t, pg.NextCursor)
	require.Equal(t, []string{"event:created", "event:member_added", "msg:от A", "msg:от B"}, names(pg.Items))
	created := pg.Items[0].(ConvEventItem)
	require.Equal(t, "event", created.Kind)
	require.Equal(t, pidA, created.ActorPhotonID)
	require.Equal(t, "g", created.Title)
	require.Equal(t, int64(1), created.ConversationVersion)
	added := pg.Items[1].(ConvEventItem)
	require.Equal(t, pidA, added.ActorPhotonID)
	require.Equal(t, pidB, added.TargetPhotonID)
	for i := 1; i < len(pg.Items); i++ {
		require.Less(t, itemAt(pg.Items[i-1]), itemAt(pg.Items[i]), "порядок по created_at")
	}
	own := pg.Items[2].(ConvMessageItem)
	require.Equal(t, ConvMessageItem{Kind: "message", ID: m1.ID, CreatedAt: m1.CreatedAt, AuthorPhotonID: pidA, Text: "от A", ClientMsgID: cidA}, own)
	foreign := pg.Items[3].(ConvMessageItem)
	require.Equal(t, ConvMessageItem{Kind: "message", ID: m2.ID, CreatedAt: m2.CreatedAt, AuthorPhotonID: pidB, Text: "от B"}, foreign,
		"client_msg_id — только у своих сообщений")
	pgB := r.syncPage(t, b, id, "", 0)
	require.Equal(t, "", pgB.Items[2].(ConvMessageItem).ClientMsgID)
	require.Equal(t, cidB, pgB.Items[3].(ConvMessageItem).ClientMsgID)

	// Ни одного идентификатора, типа или имени Mattermost (ADR-099 §3).
	wire, err := json.Marshal(pg)
	require.NoError(t, err)
	for _, forbidden := range []string{a, b, r.carrier(t, id), convEventPostType, `"type"`, "channel", "engram_"} {
		require.NotContains(t, string(wire), forbidden)
	}

	// Продолжение: from, at_end.
	cont := r.syncPage(t, a, id, pg.NextCursor, 0)
	require.NotNil(t, cont.From)
	require.True(t, cont.AtEnd)
	require.Equal(t, int64(pg.Items[3].(ConvMessageItem).CreatedAt), cont.From.CreatedAt, "W = 0: проход начинается с водяного знака")

	// Пустая страница — не ошибка: курсор сохраняется.
	empty := r.syncPage(t, a, id, cont.NextCursor, 0)
	require.True(t, empty.AtEnd)
	require.NotEmpty(t, empty.NextCursor)

	// Лимит и курсор.
	for _, lim := range []int{0, -1, 201} {
		require.Equal(t, convErrInvalidLimit, r.syncRaw(t, a, map[string]any{"conversation_id": id, "limit": lim}).Error, "limit %d", lim)
	}
	require.Equal(t, convErrInvalidRequest, r.syncRaw(t, a, map[string]any{"conversation_id": id, "extra": 1}).Error)
	require.Equal(t, convErrInvalidRequest, r.syncRaw(t, a, map[string]any{}).Error)
	require.Equal(t, convErrInvalidCursor, r.syncRaw(t, a, map[string]any{"conversation_id": id, "cursor": "!!!"}).Error)
	other := r.group(t, a)
	foreignCursor := r.syncPage(t, a, other, "", 0).NextCursor
	require.Equal(t, convErrInvalidCursor, r.syncRaw(t, a, map[string]any{"conversation_id": id, "cursor": foreignCursor}).Error,
		"курсор другого разговора")
	require.True(t, r.syncRaw(t, a, map[string]any{"conversation_id": other, "cursor": foreignCursor}).OK)

	// Доступ (ADR-099 §11): порядок — доступ раньше содержания курсора.
	cMM := r.id.person(pidD)
	require.Equal(t, convErrConversationNF, r.syncRaw(t, cMM, map[string]any{"conversation_id": id}).Error, "никогда не участвовал")
	require.Equal(t, convErrConversationNF, r.syncRaw(t, cMM, map[string]any{"conversation_id": id, "cursor": "!!!"}).Error)
	require.Equal(t, convErrConversationNF, r.syncRaw(t, a, map[string]any{"conversation_id": "no-such"}).Error)
	require.Equal(t, convErrConversationNF, r.syncRaw(t, r.id.plainUser(), map[string]any{"conversation_id": id}).Error, "без PhotonID")
	r.mustDo(t, a, ActionConversationMemberRemove, reqOf(id, "photon_id", pidB))
	require.Equal(t, convErrNoAccess, r.syncRaw(t, b, map[string]any{"conversation_id": id}).Error, "удалённый участник")

	// Архив: читать можно (ADR-099 §8.1).
	r.mustDo(t, a, ActionConversationArchive, reqOf(id))
	arch := r.syncPage(t, a, id, "", 0)
	require.Equal(t, "event:archived", itemName(arch.Items[len(arch.Items)-1]))
}

// Все виды событий ленты со своими полями (ADR-099 §9.3).
func TestConv931EventItems(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	r.id.person(pidC)
	id := r.group(t, a, pidB)
	r.mustDo(t, a, ActionConversationUpdate, reqOf(id, "title", "новое", "description", "описание"))
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
	r.mustDo(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidC, "role", "admin"))
	r.mustDo(t, a, ActionConversationMemberRemove, reqOf(id, "photon_id", pidC))
	r.mustDo(t, b, ActionConversationLeave, reqOf(id))
	r.mustDo(t, a, ActionConversationArchive, reqOf(id))

	items := r.fullPass(t, a, id, 3)
	require.Equal(t, []string{"event:created", "event:member_added", "event:metadata_changed", "event:member_added",
		"event:role_changed", "event:member_removed", "event:member_left", "event:archived"}, names(items))
	ev := func(i int) ConvEventItem { return items[i].(ConvEventItem) }
	require.Equal(t, ConvEventItem{Kind: "event", ID: ev(2).ID, CreatedAt: ev(2).CreatedAt, Event: "metadata_changed", ActorPhotonID: pidA,
		Title: "новое", Description: "описание", ConversationVersion: 2}, ev(2))
	require.Equal(t, pidC, ev(3).TargetPhotonID)
	require.Equal(t, "admin", ev(4).Role)
	require.Equal(t, pidC, ev(4).TargetPhotonID)
	require.Equal(t, pidA, ev(5).ActorPhotonID)
	require.Equal(t, pidC, ev(5).TargetPhotonID)
	require.Equal(t, pidB, ev(6).ActorPhotonID)
	require.Equal(t, pidA, ev(7).ActorPhotonID)
	for i := 1; i < len(items); i++ {
		require.Greater(t, ev(i).ConversationVersion, int64(0))
		require.Less(t, itemAt(items[i-1]), itemAt(items[i]))
	}
}

// ── видимая лента ────────────────────────────────────────────────────────────

func TestConv931VisibleFeed(t *testing.T) {
	t.Run("since_join", func(t *testing.T) {
		r := newGroupRig(t, 2000)
		a := r.id.person(pidA)
		r.id.person(pidB)
		c := r.id.person(pidC)
		id := r.group(t, a, pidB)
		s := func(mm, text string) { r.mustSend(t, mm, id, newRequestID(), text) }
		s(a, "x1")
		s(a, "x2")
		r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
		s(a, "x3")
		r.mustDo(t, c, ActionConversationLeave, reqOf(id))
		s(a, "x4")
		r.mustDo(t, a, ActionConversationUpdate, reqOf(id, "title", "между периодами"))
		r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
		s(a, "x5")

		want := []string{"event:member_added", "msg:x3", "event:member_left", "event:member_added", "msg:x5"}
		for _, limit := range []int{1, 2, 3, 60} {
			require.Equal(t, want, names(r.fullPass(t, c, id, limit)), "limit %d: x4 и событие между периодами не видны", limit)
		}
		// Границы периода — по позициям элементов: открывающий и закрывающий
		// входят в период.
		items := r.fullPass(t, c, id, 2)
		var open1, close1, open2 int64
		rows, err := r.db.Query(`SELECT open_at, COALESCE(close_at, 0) FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2 ORDER BY period_no`, id, pidC)
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&open1, &close1))
		require.True(t, rows.Next())
		var c2 int64
		require.NoError(t, rows.Scan(&open2, &c2))
		require.Equal(t, open1, itemAt(items[0]))
		require.Equal(t, close1, itemAt(items[2]))
		require.Equal(t, open2, itemAt(items[3]))
		// Первичная синхронизация — последние limit видимых.
		pg := r.syncPage(t, c, id, "", 2)
		require.Equal(t, []string{"event:member_added", "msg:x5"}, names(pg.Items))
		// Участник A (весь период) видит всё.
		require.Contains(t, names(r.fullPass(t, a, id, 4)), "msg:x4")
	})

	t.Run("full", func(t *testing.T) {
		r := newGroupRig(t, 2000)
		a := r.id.person(pidA)
		r.id.person(pidB)
		c := r.id.person(pidC)
		req := createReq(newRequestID(), "g", pidB)
		req["history_policy"] = "full"
		id := r.mustCreate(t, a, req).Conversation.ConversationID
		r.mustSend(t, a, id, newRequestID(), "x1")
		r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
		r.mustSend(t, a, id, newRequestID(), "x2")
		want := []string{"event:created", "event:member_added", "msg:x1", "event:member_added", "msg:x2"}
		for _, limit := range []int{1, 2, 60} {
			require.Equal(t, want, names(r.fullPass(t, c, id, limit)), "limit %d: новому участнику видна вся лента", limit)
		}
	})
}

// ── L3: лента одним оператором против перебора ──────────────────────────────

// l3Fixture — лента с группами равных CreateAt, надгробиями с поздними
// постами, удалёнными и системными постами, открытым резервом (барьер) и
// постами выше барьера.
type l3Fixture struct {
	r       *groupRig
	convID  string
	channel string
	base    int64
	barrier int64
}

func newL3Fixture(t *testing.T) *l3Fixture {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	r.id.person(pidC)
	id := r.group(t, a, pidB)
	f := &l3Fixture{r: r, convID: id, channel: r.carrier(t, id)}
	var h int64
	require.NoError(t, r.db.QueryRow(`SELECT position_h FROM engram_conversation WHERE conversation_id = $1`, id).Scan(&h))
	f.base = h + 10000
	f.barrier = f.base + 400

	pos := func(p int64, kind, key, state, sender string) {
		var s any
		if sender != "" {
			s = sender
		}
		_, err := r.db.Exec(`INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, sender, reserved_at)
VALUES ($1, $2, $3, $4, $5, $6, 1)`, id, p, kind, key, state, s)
		require.NoError(t, err)
	}
	post := func(pid string, at int64, typ string, deleteAt int64, props string) {
		_, err := r.db.Exec(`INSERT INTO posts (id, channelid, userid, createat, updateat, deleteat, message, type, props)
VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $8::jsonb)`, pid, f.channel, "u", at, deleteAt, "m"+pid, typ, props)
		require.NoError(t, err)
	}
	msgProps := func(key string) string { return fmt.Sprintf(`{"%s":"%s"}`, dmPropClientMsgID, key) }
	evProps := func(key string) string {
		return fmt.Sprintf(`{"engram_event":{"event":"member_added","actor_photon_id":"%s","target_photon_id":"%s","conversation_version":2},"engram_event_key":"%s"}`, pidA, pidB, key)
	}
	for i := 1; i <= 450; i++ {
		at := f.base + int64(i)
		key := fmt.Sprintf("k%04d", i)
		var typ, props string
		sender := []string{pidA, pidB}[i%2]
		switch {
		case i == 400:
			pos(at, "message", key, "reserved", sender) // барьер: поста нет
			continue
		case i%11 == 0:
			pos(at, "message", key, "aborted", sender)
			post(fmt.Sprintf("late%04d", i), at, "", 0, msgProps(key)) // поздний пост на надгробии
			continue
		case i%13 == 0:
			pos(at, "event", key, "published", "")
			typ, props = convEventPostType, evProps(key)
			post(fmt.Sprintf("p%04d", i), at, typ, 0, props)
		default:
			pos(at, "message", key, "published", sender)
			typ, props = "", msgProps(key)
			post(fmt.Sprintf("p%04d", i), at, typ, 0, props)
		}
		if i%7 == 0 { // дубли одного элемента: равный CreateAt, id и меньше, и больше
			post(fmt.Sprintf("o%04d", i), at, typ, 0, props)
			post(fmt.Sprintf("q%04dx", i), at, typ, 0, props)
			if i%14 == 0 {
				post(fmt.Sprintf("q%04dy", i), at, typ, 0, props)
			}
		}
		if i%17 == 0 {
			post(fmt.Sprintf("del%04d", i), at+0, "", 1, msgProps(key)) // удалённый
		}
		if i%19 == 0 {
			post(fmt.Sprintf("sys%04d", i), at, "system_join_channel", 0, "{}") // системный
		}
	}
	_, err := r.db.Exec(`UPDATE engram_conversation SET position_h = $2 WHERE conversation_id = $1`, id, f.base+450)
	require.NoError(t, err)
	// Три периода C: [base+60, base+120], [base+200, base+260], [base+330, …).
	for i, p := range []struct {
		o, c  int64
		state string
	}{{60, 120, "left"}, {200, 260, "left"}, {330, 0, "active"}} {
		var closeAt, closedV any
		if p.c != 0 {
			closeAt, closedV = f.base+p.c, 3
		}
		_, err := r.db.Exec(`INSERT INTO engram_member_period (conversation_id, photon_id, period_no, role, state, opened_version, closed_version, open_at, close_at)
VALUES ($1, $2, $3, 'member', $4, 2, $5, $6, $7)`, id, pidC, i+1, p.state, closedV, f.base+p.o, closeAt)
		require.NoError(t, err)
	}
	return f
}

// expected — перебор по правилам ADR-052 и ADR-099 §9.4, независимо от SQL.
func (f *l3Fixture) expected(t *testing.T, pid string) []int64 {
	t.Helper()
	r := f.r
	var policy string
	require.NoError(t, r.db.QueryRow(`SELECT history_policy FROM engram_conversation WHERE conversation_id = $1`, f.convID).Scan(&policy))
	aborted := map[int64]bool{}
	rows, err := r.db.Query(`SELECT pos FROM engram_position WHERE conversation_id = $1 AND state = 'aborted'`, f.convID)
	require.NoError(t, err)
	for rows.Next() {
		var p int64
		require.NoError(t, rows.Scan(&p))
		aborted[p] = true
	}
	rows.Close()
	var periods [][2]int64
	prs, err := r.db.Query(`SELECT open_at, COALESCE(close_at, 9223372036854775807) FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2 ORDER BY period_no`, f.convID, pid)
	require.NoError(t, err)
	for prs.Next() {
		var o, c int64
		require.NoError(t, prs.Scan(&o, &c))
		periods = append(periods, [2]int64{o, c})
	}
	prs.Close()
	minID := map[int64]string{}
	ps, err := r.db.Query(`SELECT id, createat FROM posts WHERE channelid = $1 AND deleteat = 0 AND type IN ('', 'custom_engram_event')`, f.channel)
	require.NoError(t, err)
	for ps.Next() {
		var id string
		var at int64
		require.NoError(t, ps.Scan(&id, &at))
		if cur, ok := minID[at]; !ok || id < cur {
			minID[at] = id
		}
	}
	ps.Close()
	var out []int64
	for at := range minID {
		if at >= f.barrier || aborted[at] {
			continue
		}
		if policy == "since_join" {
			in := false
			for _, p := range periods {
				if at >= p[0] && at <= p[1] {
					in = true
				}
			}
			if !in {
				continue
			}
		}
		out = append(out, at)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (f *l3Fixture) periods(t *testing.T, pid string) []convPeriodBounds {
	t.Helper()
	ps, err := callerPeriods(context.Background(), f.r.db, f.convID, pid)
	require.NoError(t, err)
	return ps
}

// passWith — проход от начала оператором sqlText; страницы без Go-логики
// курсора: нижняя граница — позиция последнего элемента + 1.
func (f *l3Fixture) passWith(t *testing.T, sqlText string, limit int, policyFull bool, periods []convPeriodBounds) (pages [][]int64) {
	t.Helper()
	lower := int64(0)
	if !policyFull && len(periods) > 0 {
		lower = periods[0].open
	}
	use := periods
	if policyFull {
		use = nil
	}
	for i := 0; i < 5000; i++ {
		_, rows, err := runConvFeed(context.Background(), f.r.db, sqlText, f.convID, f.channel, lower, limit, use)
		require.NoError(t, err)
		page := make([]int64, len(rows))
		for j, row := range rows {
			page[j] = row.createAt
		}
		pages = append(pages, page)
		if len(rows) < limit {
			return pages
		}
		lower = rows[len(rows)-1].createAt + 1
	}
	t.Fatal("проход не закончился")
	return nil
}

func flatten(pages [][]int64) []int64 {
	var out []int64
	for _, p := range pages {
		out = append(out, p...)
	}
	return out
}

func equalInts(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var l3Limits = []int{1, 2, 3, 5, 7, 11, 60}

// mismatches — сколько сравнений «страница оператора против перебора» не
// сошлось. Страница k перебора — expected[k·limit : (k+1)·limit].
func (f *l3Fixture) mismatches(t *testing.T, sqlText string, pid string, policyFull bool) (compared, bad int) {
	t.Helper()
	want := f.expected(t, pid)
	require.Greater(t, len(want), 80, "перебор не вырожден")
	periods := f.periods(t, pid)
	for _, limit := range l3Limits {
		pages := f.passWith(t, sqlText, limit, policyFull, periods)
		for k, page := range pages {
			lo, hi := k*limit, (k+1)*limit
			if lo > len(want) {
				lo = len(want)
			}
			if hi > len(want) {
				hi = len(want)
			}
			compared++
			if !equalInts(page, want[lo:hi]) {
				bad++
			}
		}
	}
	return compared, bad
}

func mutate(t *testing.T, s, from, to string) string {
	t.Helper()
	require.Contains(t, s, from, "мутация не применилась — тест ничего не проверяет")
	return strings.Replace(s, from, to, 1)
}

// L3 — перебор против оператора (ADR-052, ADR-099 §9.4): full и since_join,
// в том числе с несколькими периодами, страницы с границей внутри группы
// равных CreateAt. Контроли: без барьера, без фильтра надгробия, без
// схлопывания дублей, без фильтра периодов.
func TestConvL3FeedAgainstBruteForce(t *testing.T) {
	f := newL3Fixture(t)
	for _, c := range []struct {
		name string
		pid  string
		full bool
	}{{"A since_join", pidA, false}, {"C три периода", pidC, false}} {
		sqlText := convFeedSQL(feedAscend, len(f.periods(t, c.pid)))
		compared, bad := f.mismatches(t, sqlText, c.pid, c.full)
		t.Logf("L3 %s: %d сравнений, %d расхождений", c.name, compared, bad)
		require.Greater(t, compared, 100)
		require.Zero(t, bad, c.name)
	}
	// Реальный обработчик: полный проход и первичная синхронизация.
	for _, c := range []struct{ mm, pid string }{{f.r.id.recByPID(pidA), pidA}, {f.r.id.recByPID(pidC), pidC}} {
		want := f.expected(t, c.pid)
		for _, limit := range []int{3, 7, 60} { // обработчик дорог на страницу; все лимиты — на уровне оператора выше
			var got []int64
			for _, it := range f.r.fullPass(t, c.mm, f.convID, limit) {
				got = append(got, itemAt(it))
			}
			require.True(t, equalInts(want, got), "обработчик, %s, limit %d", c.pid, limit)
			prim := f.r.syncPage(t, c.mm, f.convID, "", limit)
			var gotPrim []int64
			for _, it := range prim.Items {
				gotPrim = append(gotPrim, itemAt(it))
			}
			tail := want
			if len(tail) > limit {
				tail = tail[len(tail)-limit:]
			}
			require.True(t, equalInts(tail, gotPrim), "первичная синхронизация, %s, limit %d", c.pid, limit)
		}
	}
	// full: все видят всё, что ниже барьера.
	_, err := f.r.db.Exec(`UPDATE engram_conversation SET history_policy = 'full' WHERE conversation_id = $1`, f.convID)
	require.NoError(t, err)
	for _, pid := range []string{pidA, pidC} {
		sqlText := convFeedSQL(feedAscend, 0)
		compared, bad := f.mismatches(t, sqlText, pid, true)
		t.Logf("L3 full %s: %d сравнений, %d расхождений", pid, compared, bad)
		require.Zero(t, bad)
		var got []int64
		for _, it := range f.r.fullPass(t, f.r.id.recByPID(pid), f.convID, 11) {
			got = append(got, itemAt(it))
		}
		require.True(t, equalInts(f.expected(t, pid), got))
	}
	_, err = f.r.db.Exec(`UPDATE engram_conversation SET history_policy = 'since_join' WHERE conversation_id = $1`, f.convID)
	require.NoError(t, err)

	// Отрицательные контроли: каждая поломка даёт расхождения.
	base3 := convFeedSQL(feedAscend, 3)
	base1 := convFeedSQL(feedAscend, 1)
	for _, c := range []struct {
		id, pid, sql string
	}{
		{"без барьера", pidA, mutate(t, base1, "LEAST(b.bar, $5)", "$5")},
		{"без фильтра надгробия", pidA, mutate(t, base1, "AND t.state = 'aborted'", "AND t.state = 'aborted-never'")},
		{"без схлопывания дублей", pidA, mutate(t, base1, "SELECT DISTINCT ON (p.createat) p.id", "SELECT p.id")},
		{"без фильтра периодов", pidC, mutate(t, base3, "AND (", "AND (TRUE OR ")},
	} {
		_, bad := f.mismatches(t, c.sql, c.pid, false)
		t.Logf("L3 контроль %q: %d расхождений", c.id, bad)
		require.Positive(t, bad, "контроль %q обязан дать расхождения", c.id)
	}
}

// recByPID — пользователь Mattermost по PhotonID (для тестов).
func (f *fakeConvIdentity) recByPID(pid string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.recs[pid].MMUserID
}

// ── P1: барьер и страница одним оператором ──────────────────────────────────

// feedReader — способ прочитать страницу ниже/после lower: настоящий
// оператор или сломанный вариант контроля.
type feedReader func(t *testing.T, f *p1Rig, lower int64, between func()) []convFeedRow

type p1Rig struct {
	r       *groupRig
	a, b    string
	id, ch  string
	periods []convPeriodBounds
}

func realReader(t *testing.T, f *p1Rig, lower int64, between func()) []convFeedRow {
	_, rows, err := readConvFeed(context.Background(), f.r.db, f.id, f.ch, feedAscend, lower, 200, f.periods)
	require.NoError(t, err)
	between()
	return rows
}

// noBarrierReader — контроль «без барьера».
func noBarrierReader(t *testing.T, f *p1Rig, lower int64, between func()) []convFeedRow {
	sqlText := mutate(t, convFeedSQL(feedAscend, 1), "LEAST(b.bar, $5)", "$5")
	_, rows, err := runConvFeed(context.Background(), f.r.db, sqlText, f.id, f.ch, lower, 200, f.periods)
	require.NoError(t, err)
	between()
	return rows
}

// pageThenBarrier — контроль «страница, затем барьер»: два оператора; между
// ними успевает опубликоваться застрявший резерв.
func pageThenBarrier(t *testing.T, f *p1Rig, lower int64, between func()) []convFeedRow {
	sqlText := mutate(t, convFeedSQL(feedAscend, 1), "LEAST(b.bar, $5)", "$5")
	_, rows, err := runConvFeed(context.Background(), f.r.db, sqlText, f.id, f.ch, lower, 200, f.periods)
	require.NoError(t, err)
	between()
	var bar int64
	require.NoError(t, f.r.db.QueryRow(`SELECT COALESCE(
  (SELECT min(pos) FROM engram_position WHERE conversation_id = $1 AND state = 'reserved'),
  (SELECT position_h + 1 FROM engram_conversation WHERE conversation_id = $1))`, f.id).Scan(&bar))
	var out []convFeedRow
	for _, row := range rows {
		if row.createAt < bar {
			out = append(out, row)
		}
	}
	return out
}

// p1Run: отправитель A застрял в CreatePost (резерв N), B публикует N+1;
// читатель читает; A заканчивает; читатель продолжает с курсора. Возвращает
// пропуски — опубликованные позиции ниже курсора, которых читатель не
// получил.
func p1Run(t *testing.T, read feedReader) (gaps []int64, stuckPos int64) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	f := &p1Rig{r: r, a: a, b: b, id: id, ch: r.carrier(t, id)}
	f.periods = nil // since_join, один период: границы — по позиции открытия
	ps, err := callerPeriods(context.Background(), r.db, id, pidA)
	require.NoError(t, err)
	f.periods = ps

	release := make(chan struct{})
	r.mm.mu.Lock()
	r.mm.postBeforeCreate = func(p *model.Post) {
		if p.Message == "застрявшее" {
			<-release
		}
	}
	r.mm.mu.Unlock()
	cidSlow := newRequestID()
	slow := sendAsync(r, a, id, cidSlow, "застрявшее")
	require.Eventually(t, func() bool {
		return r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND item_key = $2 AND state = 'reserved'`, id, cidSlow) == 1
	}, 5*time.Second, 5*time.Millisecond)
	stuckPos = r.msgRows(t, id, cidSlow)[0].Pos
	mB := r.mustSend(t, b, id, newRequestID(), "после застрявшего")
	require.Greater(t, mB.CreatedAt, stuckPos)

	released := false
	finishSlow := func() {
		if released {
			return
		}
		released = true
		close(release)
		require.True(t, recv(t, slow).OK)
	}
	defer finishSlow()

	got := map[int64]bool{}
	rows1 := read(t, f, f.periods[0].open, finishSlow)
	cursor := int64(0)
	for _, row := range rows1 {
		got[row.createAt] = true
		cursor = row.createAt
	}
	finishSlow()
	rows2 := read(t, f, cursor+1, func() {})
	for _, row := range rows2 {
		got[row.createAt] = true
	}
	// Опубликованные позиции ниже курсора, не полученные читателем.
	rs, err := r.db.Query(`SELECT pos FROM engram_position WHERE conversation_id = $1 AND state = 'published' AND pos <= $2 ORDER BY pos`, id, cursor)
	require.NoError(t, err)
	defer rs.Close()
	for rs.Next() {
		var p int64
		require.NoError(t, rs.Scan(&p))
		if !got[p] {
			gaps = append(gaps, p)
		}
	}
	require.NoError(t, rs.Err())
	// Читатель, дочитавший всё, получает все опубликованные позиции.
	all := read(t, f, f.periods[0].open, func() {})
	require.Equal(t, 4, len(all), "created, member_added, застрявшее, после застрявшего — полный набор")
	return gaps, stuckPos
}

func TestConvP1BarrierAndPageInOneStatement(t *testing.T) {
	gaps, _ := p1Run(t, realReader)
	require.Empty(t, gaps, "один оператор: ни одного пропуска ниже курсора")

	// Контроли: оба сломанных способа теряют застрявшую позицию.
	gaps, stuck := p1Run(t, noBarrierReader)
	require.Equal(t, []int64{stuck}, gaps, "без барьера: курсор прошёл застрявший резерв")
	gaps, stuck = p1Run(t, pageThenBarrier)
	require.Equal(t, []int64{stuck}, gaps, "страница, затем барьер: между операторами резерв опубликовался")
}

// Настоящий 931 за застрявшим резервом: барьер не пускает дальше, после
// публикации лента продолжается без пропуска.
func TestConvP1RealSyncStopsAtBarrier(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	release := make(chan struct{})
	r.mm.mu.Lock()
	r.mm.postBeforeCreate = func(p *model.Post) {
		if p.Message == "застрявшее" {
			<-release
		}
	}
	r.mm.mu.Unlock()
	cid := newRequestID()
	slow := sendAsync(r, a, id, cid, "застрявшее")
	require.Eventually(t, func() bool {
		return r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND item_key = $2 AND state = 'reserved'`, id, cid) == 1
	}, 5*time.Second, 5*time.Millisecond)
	stuck := r.msgRows(t, id, cid)[0].Pos
	r.mustSend(t, b, id, newRequestID(), "после")

	pg := r.syncPage(t, a, id, "", 0)
	require.Equal(t, []string{"event:created", "event:member_added"}, names(pg.Items), "ниже барьера только события")
	for _, it := range pg.Items {
		require.Less(t, itemAt(it), stuck)
	}
	close(release)
	require.True(t, recv(t, slow).OK)
	cont := r.syncPage(t, a, id, pg.NextCursor, 0)
	require.Equal(t, []string{"event:member_added", "msg:застрявшее", "msg:после"}, names(cont.Items),
		"водяной знак входит в проход (W = 0), застрявшая позиция не пропущена")
	require.Equal(t, stuck, itemAt(cont.Items[1]))
}

// ── P3: надгробие ────────────────────────────────────────────────────────────

// Поздний пост на отменённой позиции не выдаётся никогда: сверка отменила
// резерв, пост долетел позже (ordering вместо задержки в 3 с и 8 с).
func TestConvP3LatePostOnTombstoneNeverReturned(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)

	// (а) пост долетел после отмены сверкой.
	cid := newRequestID()
	r.mm.mu.Lock()
	r.mm.postNilOnCall = r.mm.postCalls + 1
	r.mm.mu.Unlock()
	require.False(t, r.send(t, a, id, cid, "потерянное").OK)
	p1 := r.msgRows(t, id, cid)[0].Pos
	pub, ab, err := r.svc.closeAbandonedMessages(context.Background(), id, math.MaxInt64, "")
	require.NoError(t, err)
	require.Equal(t, [2]int{0, 1}, [2]int{pub, ab})
	r.latePost(t, id, a, cid, p1)

	// (б) задержка в хуке: CreatePost начат до отмены, пост записан после.
	cid2 := newRequestID()
	gate := make(chan struct{})
	r.mm.mu.Lock()
	r.mm.postBeforeCreate = func(p *model.Post) {
		if p.Message == "в хуке" {
			<-gate
		}
	}
	r.mm.mu.Unlock()
	hooked := sendAsync(r, a, id, cid2, "в хуке")
	require.Eventually(t, func() bool {
		return r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND item_key = $2 AND state = 'reserved'`, id, cid2) == 1
	}, 5*time.Second, 5*time.Millisecond)
	p2 := r.msgRows(t, id, cid2)[0].Pos
	_, ab, err = r.svc.closeAbandonedMessages(context.Background(), id, math.MaxInt64, cid2)
	require.NoError(t, err)
	require.Equal(t, 1, ab)
	close(gate)
	require.False(t, recv(t, hooked).OK, "владелец не сообщает успех над надгробием")
	require.Equal(t, "aborted", r.stateAt(t, id, p2))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM posts WHERE createat = $1`, p2), "пост действительно долетел")

	ok := r.mustSend(t, a, id, newRequestID(), "живое")
	feed := r.fullPass(t, a, id, 2)
	require.Equal(t, []string{"event:created", "msg:живое"}, names(feed), "ни одного поста с надгробия")
	require.Equal(t, ok.ID, itemID(feed[1]))

	// Контроль: без фильтра надгробия оба поздних поста выданы.
	f := &p1Rig{r: r, id: id, ch: r.carrier(t, id)}
	ps, err := callerPeriods(context.Background(), r.db, id, pidA)
	require.NoError(t, err)
	f.periods = ps
	sqlText := mutate(t, convFeedSQL(feedAscend, 1), "AND t.state = 'aborted'", "AND t.state = 'aborted-never'")
	_, rows, err := runConvFeed(context.Background(), r.db, sqlText, id, f.ch, 0, 200, ps)
	require.NoError(t, err)
	var at []int64
	for _, row := range rows {
		at = append(at, row.createAt)
	}
	require.Contains(t, at, p1, "контроль: без фильтра надгробия поздний пост выдан")
	require.Contains(t, at, p2)
}

// Потеря X посреди записи события: пост события создан, ответ потерян,
// отметка written не поставлена. Сверка находит пост (E3) — дубля нет;
// 931 до сверки не выдаёт ничего за позицией события, после — выдаёт его
// на месте и один раз. Контроль: дубль поста на той же позиции схлопывается;
// без схлопывания выданы оба.
func TestConvP3LostXInMiddleOfEventWrite(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a)
	before := r.mustSend(t, a, id, newRequestID(), "до")
	r.mm.mu.Lock()
	r.mm.postCreateThenFailOnCall = r.mm.postCalls + 1
	r.mm.mu.Unlock()
	resp := r.do(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB}))
	require.False(t, resp.OK, "пост события записан, ответ потерян")
	var at int64
	require.NoError(t, r.db.QueryRow(`SELECT at FROM engram_event WHERE conversation_id = $1 AND state = 'pending'`, id).Scan(&at))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM posts WHERE createat = $1`, at))
	after := r.mustSend(t, a, id, newRequestID(), "после")
	require.Greater(t, after.CreatedAt, at)

	// До сверки: барьер на позиции события.
	require.Equal(t, []string{"event:created", "msg:до"}, names(r.fullPass(t, a, id, 10)))

	// Дубль поста события на той же позиции (повторная запись без E3).
	_, err := r.db.Exec(`INSERT INTO posts (id, channelid, userid, createat, updateat, deleteat, message, type, props)
SELECT $2, channelid, userid, createat, updateat, deleteat, message, type, props FROM posts WHERE createat = $1`, at, model.NewId())
	require.NoError(t, err)

	n, err := r.svc.reconcileEvents(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM posts WHERE createat = $1`, at), "E3 новых постов не создал")
	feed := r.fullPass(t, a, id, 10)
	require.Equal(t, []string{"event:created", "msg:до", "event:member_added", "msg:после"}, names(feed), "одно событие на позицию, на месте")
	require.Less(t, before.CreatedAt, itemAt(feed[2]))
	require.Equal(t, at, itemAt(feed[2]))

	ps, err := callerPeriods(context.Background(), r.db, id, pidA)
	require.NoError(t, err)
	sqlText := mutate(t, convFeedSQL(feedAscend, 1), "SELECT DISTINCT ON (p.createat) p.id", "SELECT p.id")
	_, rows, err := runConvFeed(context.Background(), r.db, sqlText, id, r.carrier(t, id), 0, 200, ps)
	require.NoError(t, err)
	require.Len(t, rows, len(feed)+1, "контроль: без схлопывания дубль выдан")
}

// ── P5: конкурентный прогон, W = 0 ──────────────────────────────────────────

// P5: отправители со сбоями, изменения и читатель одновременно. Инварианты:
// читатель никогда не получает неизвестный элемент с позицией ниже уже
// полученных (ни одного пропуска ниже курсора), ни одного поста на
// надгробии, каждая позиция один раз, порядок по created_at, в конце набор
// полученного равен набору опубликованных позиций.
func TestConvP5ConcurrentRunW0(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	rnd := rand.New(rand.NewSource(11))
	var rmu sync.Mutex
	r.mm.mu.Lock()
	r.mm.postDecide = func(*model.Post) string {
		rmu.Lock()
		defer rmu.Unlock()
		return []string{"ok", "ok", "ok", "reject", "nil", "lost"}[rnd.Intn(6)]
	}
	r.mm.mu.Unlock()

	type seen struct {
		mu    sync.Mutex
		ids   map[string]int64
		maxAt int64
		bad   []string
	}
	rd := &seen{ids: map[string]int64{}}
	consume := func(items []any) {
		rd.mu.Lock()
		defer rd.mu.Unlock()
		for _, it := range items {
			iid, at := itemID(it), itemAt(it)
			if _, ok := rd.ids[iid]; ok {
				continue
			}
			if at <= rd.maxAt {
				rd.bad = append(rd.bad, fmt.Sprintf("элемент %s на позиции %d пришёл после позиции %d", iid, at, rd.maxAt))
			}
			rd.ids[iid] = at
			rd.maxAt = at
		}
	}

	var done atomic.Bool
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		cursor := startCursor(id)
		for !done.Load() {
			resp := r.syncRaw(t, a, map[string]any{"conversation_id": id, "cursor": cursor, "limit": 7})
			if !resp.OK {
				time.Sleep(time.Millisecond)
				continue
			}
			pg := resp.Data.(ConvSyncResponse)
			consume(pg.Items)
			cursor = pg.NextCursor
			time.Sleep(time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	slots := make(chan struct{}, convTestSenders)
	type sent struct{ caller, pid, cid string }
	var keys []sent
	for i := 0; i < 60; i++ {
		if i%2 == 0 {
			keys = append(keys, sent{a, pidA, newRequestID()})
		} else {
			keys = append(keys, sent{b, pidB, newRequestID()})
		}
	}
	results := make([]ConvMessage, len(keys))
	for i, k := range keys {
		wg.Add(1)
		go func(i int, k sent) {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			raw, _ := json.Marshal(sendReq(id, k.cid, "t"+k.cid))
			for attempt := 0; attempt < 40; attempt++ {
				resp := r.svc.dispatch(context.Background(), k.caller, ActionConversationSend, raw)
				if resp.OK {
					results[i] = resp.Data.(ConvSendResponse).Message
					return
				}
				if resp.Error == errMessageInProgress {
					_, _, err := r.svc.closeAbandonedMessages(context.Background(), id, math.MaxInt64, k.cid)
					require.NoError(t, err)
				}
			}
		}(i, k)
	}
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for attempt := 0; attempt < 20; attempt++ {
				if r.do(t, a, ActionConversationUpdate, reqOf(id, "title", fmt.Sprintf("t%d", i))).OK {
					return
				}
			}
		}(i)
	}
	wg.Wait()
	done.Store(true)
	readerWG.Wait()
	_, err := r.svc.reconcileOpen(context.Background(), math.MaxInt64, 100)
	require.NoError(t, err)
	// Дочитать до конца тем же потребителем, уже после остановки читателя.
	cursor := startCursor(id)
	for i := 0; i < 500; i++ {
		pg := r.syncPage(t, a, id, cursor, 7)
		consume(pg.Items)
		cursor = pg.NextCursor
		if pg.AtEnd {
			break
		}
	}

	require.Empty(t, rd.bad, "пропуск ниже курсора или нарушение порядка")
	require.Positive(t, r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND state = 'aborted'`, id), "прогон прошёл через отмены")
	for i, k := range keys {
		require.NotZero(t, results[i].CreatedAt, "ключ %d не доставлен", i)
		r.assertDelivered(t, id, k.pid, results[i])
	}
	// Набор полученного = набор опубликованных; на надгробиях ничего.
	want := map[string]int64{}
	rows, err := r.db.Query(`SELECT post_id, pos FROM engram_position WHERE conversation_id = $1 AND state = 'published'`, id)
	require.NoError(t, err)
	for rows.Next() {
		var pid string
		var pos int64
		require.NoError(t, rows.Scan(&pid, &pos))
		want[pid] = pos
	}
	rows.Close()
	require.Equal(t, want, rd.ids)
	tomb, err := r.db.Query(`SELECT pos FROM engram_position WHERE conversation_id = $1 AND state = 'aborted'`, id)
	require.NoError(t, err)
	defer tomb.Close()
	got := map[int64]bool{}
	for _, pos := range rd.ids {
		got[pos] = true
	}
	for tomb.Next() {
		var pos int64
		require.NoError(t, tomb.Scan(&pos))
		require.False(t, got[pos], "пост на надгробии %d выдан", pos)
	}
}
