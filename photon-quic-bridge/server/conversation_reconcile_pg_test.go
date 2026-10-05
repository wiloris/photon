package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Сверка (contract §3.6, §4.5; conversation-storage.md §8; ADR-101 §5.10,
// §5.11) и планы O1/L4 (contract §6.4) против НАСТОЯЩЕГО PostgreSQL.

// failNextEventPost — следующий CreatePost (пост события) откажет, ничего
// не записав: изменение зафиксировано, событие pending.
func (r *groupRig) failNextPost(onCall func(*fakeConvMM, int)) {
	r.mm.mu.Lock()
	defer r.mm.mu.Unlock()
	onCall(r.mm, r.mm.postCalls+1)
}

func (r *groupRig) pendingAt(t *testing.T, convID string) int64 {
	t.Helper()
	var at int64
	require.NoError(t, r.db.QueryRow(`SELECT at FROM engram_event WHERE conversation_id = $1 AND state = 'pending'`, convID).Scan(&at))
	return at
}

// Сбой между фиксацией изменения и постом события: 931 не выдаёт ничего
// за позицией события, сверка пишет событие с тем же at, лента содержит его
// на месте.
func TestConvReconcilePendingEventAfterCrash(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a)
	r.mustSend(t, a, id, newRequestID(), "до")
	r.failNextPost(func(m *fakeConvMM, n int) { m.postFailOnCall = n })
	require.False(t, r.do(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB})).OK, "пост события не записан")
	at := r.pendingAt(t, id)
	require.Equal(t, "reserved", r.stateAt(t, id, at))
	require.Zero(t, r.count(t, `SELECT count(*) FROM posts WHERE createat = $1`, at))
	after := r.mustSend(t, a, id, newRequestID(), "после")
	require.Greater(t, after.CreatedAt, at)

	require.Equal(t, []string{"event:created", "msg:до"}, names(r.fullPass(t, a, id, 10)), "за позицией события — ничего")

	res, err := r.svc.reconcileConversation(context.Background(), id, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, convReconcileResult{EventsWritten: 1}, res)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM posts WHERE createat = $1 AND type = $2`, at, convEventPostType), "тот же at")
	require.Equal(t, "published", r.stateAt(t, id, at))
	feed := r.fullPass(t, a, id, 2)
	require.Equal(t, []string{"event:created", "msg:до", "event:member_added", "msg:после"}, names(feed))
	require.Equal(t, at, itemAt(feed[2]))

	// Повтор сверки ничего не меняет.
	res, err = r.svc.reconcileConversation(context.Background(), id, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, convReconcileResult{}, res)
}

// Событие записано, written не отмечено: E3 находит пост, дубля нет.
func TestConvReconcileEventWrittenNotMarked(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a)
	r.failNextPost(func(m *fakeConvMM, n int) { m.postCreateThenFailOnCall = n })
	require.False(t, r.do(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB})).OK)
	at := r.pendingAt(t, id)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM posts WHERE createat = $1`, at))
	callsBefore := r.postCallCount()

	res, err := r.svc.reconcileConversation(context.Background(), id, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, 1, res.EventsWritten)
	require.Equal(t, callsBefore, r.postCallCount(), "CreatePost не вызван: пост найден (E3)")
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM posts WHERE createat = $1`, at), "дубля нет")
	require.Equal(t, "published", r.stateAt(t, id, at))
	require.Equal(t, []string{"event:created", "event:member_added"}, names(r.fullPass(t, a, id, 10)))
}

// Брошенные резервы сообщений: условный оператор, порог, повтор 930,
// барьер.
func TestConvReconcileAbandonedMessageReservations(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	ctx := context.Background()

	// Три брошенных резерва: пост не создан; пост создан, ответ потерян;
	// плюс живой опубликованный.
	cidLost, cidNone := newRequestID(), newRequestID()
	r.failNextPost(func(m *fakeConvMM, n int) { m.postCreateThenFailOnCall = n })
	require.False(t, r.send(t, a, id, cidLost, "потерян ответ").OK)
	r.failNextPost(func(m *fakeConvMM, n int) { m.postNilOnCall = n })
	require.False(t, r.send(t, a, id, cidNone, "нет поста").OK)
	pLost, pNone := r.msgRows(t, id, cidLost)[0].Pos, r.msgRows(t, id, cidNone)[0].Pos
	live := r.mustSend(t, b, id, newRequestID(), "живое")
	require.Equal(t, errMessageInProgress, r.send(t, a, id, cidNone, "нет поста").Error, "до сверки — message_in_progress")

	// Барьер стоит на самом раннем открытом резерве.
	require.Equal(t, []string{"event:created", "event:member_added"}, names(r.fullPass(t, a, id, 10)))

	// Порог: reserved_at < abandonedBefore. Резервы моложе порога не трогаются.
	var reservedAt int64
	require.NoError(t, r.db.QueryRow(`SELECT min(reserved_at) FROM engram_position WHERE conversation_id = $1 AND state = 'reserved'`, id).Scan(&reservedAt))
	pub, ab, err := r.svc.closeAbandonedMessages(ctx, id, reservedAt, "")
	require.NoError(t, err)
	require.Equal(t, [2]int{0, 0}, [2]int{pub, ab}, "порог строгий: резерв не старше порога")
	require.Equal(t, "reserved", r.stateAt(t, id, pLost))

	// Опубликованный резерв оператором не затрагивается.
	res, err := r.svc.reconcileConversation(ctx, id, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, convReconcileResult{Published: 1, Aborted: 1}, res)
	require.Equal(t, "published", r.stateAt(t, id, pLost))
	require.Equal(t, "aborted", r.stateAt(t, id, pNone))
	require.Equal(t, "published", r.stateAt(t, id, live.CreatedAt))
	var postID string
	require.NoError(t, r.db.QueryRow(`SELECT post_id FROM engram_position WHERE conversation_id = $1 AND pos = $2`, id, pLost).Scan(&postID))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM posts WHERE id = $1 AND createat = $2`, postID, pLost))
	var lastActivity int64
	require.NoError(t, r.db.QueryRow(`SELECT last_activity_at FROM engram_conversation WHERE conversation_id = $1`, id).Scan(&lastActivity))
	require.GreaterOrEqual(t, lastActivity, live.CreatedAt)

	// Барьер сдвинулся: лента идёт дальше, надгробия нет.
	require.Equal(t, []string{"event:created", "event:member_added", "msg:потерян ответ", "msg:живое"}, names(r.fullPass(t, a, id, 10)))

	// Повтор 930: найденная позиция / новая позиция больше прежней.
	m := r.mustSend(t, a, id, cidLost, "потерян ответ")
	require.Equal(t, pLost, m.CreatedAt)
	require.Equal(t, postID, m.ID)
	m2 := r.mustSend(t, a, id, cidNone, "нет поста")
	require.Greater(t, m2.CreatedAt, pNone)
	require.Greater(t, m2.CreatedAt, live.CreatedAt)
	r.assertDelivered(t, id, pidA, m2)
	require.Equal(t, []string{"event:created", "event:member_added", "msg:потерян ответ", "msg:живое", "msg:нет поста"}, names(r.fullPass(t, a, id, 3)))
}

// Проход по всем разговорам и живость: только то, что старше порога.
func TestConvReconcileOpenAcrossConversations(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	ctx := context.Background()
	c1 := r.group(t, a)
	c2 := r.group(t, a)
	c3 := r.group(t, a)

	r.failNextPost(func(m *fakeConvMM, n int) { m.postFailOnCall = n })
	require.False(t, r.do(t, a, ActionConversationMembersAdd, reqOf(c1, "photon_ids", []string{pidB})).OK)
	cid := newRequestID()
	r.failNextPost(func(m *fakeConvMM, n int) { m.postNilOnCall = n })
	require.False(t, r.send(t, a, c2, cid, "брошено").OK)
	cidFresh := newRequestID()
	r.failNextPost(func(m *fakeConvMM, n int) { m.postNilOnCall = n })
	require.False(t, r.send(t, a, c3, cidFresh, "свежее").OK)
	_, err := r.db.Exec(`UPDATE engram_position SET reserved_at = 1000 WHERE conversation_id = $1 AND item_key = $2`, c2, cid)
	require.NoError(t, err)

	n, err := r.svc.reconcileOpen(ctx, 2000, 100)
	require.NoError(t, err)
	require.Equal(t, 2, n, "c1 — событие; c2 — брошенный резерв; c3 моложе порога")
	require.Equal(t, "aborted", r.msgRows(t, c2, cid)[0].State)
	require.Equal(t, "reserved", r.msgRows(t, c3, cidFresh)[0].State, "живой резерв моложе порога не отменяется")
	require.Zero(t, r.count(t, `SELECT count(*) FROM engram_event WHERE state = 'pending'`))

	n, err = r.svc.reconcileOpen(ctx, 2000, 100)
	require.NoError(t, err)
	require.Equal(t, 0, n, "всё старше порога закрыто; c3 моложе порога и в выборку не попадает")
}

// Дописывание событий — только под X (contract §5.1): X чужой — ожидание
// по бюджету, CreatePost не вызван.
func TestConvReconcileEventsNeedX(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a)
	r.failNextPost(func(m *fakeConvMM, n int) { m.postFailOnCall = n })
	require.False(t, r.do(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB})).OK)
	hold, err := beginHeld(t, r.db)
	require.NoError(t, err)
	_, err = hold.Exec(`SELECT pg_advisory_xact_lock($1)`, convLockKey(id))
	require.NoError(t, err)
	r.svc.lockWait = 100 * time.Millisecond
	calls := r.postCallCount()
	_, err = r.svc.reconcileConversation(context.Background(), id, math.MaxInt64)
	require.ErrorIs(t, err, errConvLockTimeout)
	require.Equal(t, calls, r.postCallCount())
	require.NoError(t, hold.Rollback())
	r.svc.lockWait = 10 * time.Second
	res, err := r.svc.reconcileConversation(context.Background(), id, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, 1, res.EventsWritten)
}

// ── планы: O1, L4 ────────────────────────────────────────────────────────────

// explainPrepared — PREPARE, шесть EXECUTE, затем EXPLAIN (ANALYZE) седьмого
// на одном соединении (спецификация §7: «после 6 EXECUTE»).
func explainPrepared(t *testing.T, r *groupRig, name, types, sqlText, args string) (string, string) {
	t.Helper()
	ctx := context.Background()
	conn, err := r.db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, fmt.Sprintf("PREPARE %s(%s) AS %s", name, types, sqlText))
	require.NoError(t, err)
	defer conn.ExecContext(ctx, "DEALLOCATE "+name)
	for i := 0; i < 6; i++ {
		rows, err := conn.QueryContext(ctx, fmt.Sprintf("EXECUTE %s(%s)", name, args))
		require.NoError(t, err)
		for rows.Next() {
		}
		require.NoError(t, rows.Err())
		rows.Close()
	}
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("EXPLAIN (ANALYZE, COSTS OFF) EXECUTE %s(%s)", name, args))
	require.NoError(t, err)
	defer rows.Close()
	var lines []string
	exec := ""
	for rows.Next() {
		var l string
		require.NoError(t, rows.Scan(&l))
		lines = append(lines, l)
		if strings.HasPrefix(l, "Execution Time:") {
			exec = strings.TrimSpace(strings.TrimPrefix(l, "Execution Time:"))
		}
	}
	require.NoError(t, rows.Err())
	return strings.Join(lines, "\n"), exec
}

// O1 — проверка владельца ключа (ADR-102 §2) по engram_position_key_all на
// разговоре со 300 000 резервов (объём спецификации §7): Index Scan по
// (conversation_id, item_key), без прохода по резервам разговора. Время
// записывается, порога нет.
func TestConvO1OwnerCheckPlan(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	_, err := r.db.Exec(`INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, sender, payload_hash, reserved_at, post_id)
SELECT $1, 1000000000000 + g, 'message', md5(g::text), 'published', CASE WHEN g % 2 = 0 THEN $2 ELSE $3 END, '\x00', 1, 'p' || g
FROM generate_series(1, 300000) g`, id, pidA, pidB)
	require.NoError(t, err)
	_, err = r.db.Exec(`ANALYZE engram_position`)
	require.NoError(t, err)

	var foreignKey string
	require.NoError(t, r.db.QueryRow(`SELECT md5('12345')`).Scan(&foreignKey))
	q := `SELECT 1 FROM engram_position WHERE conversation_id = $1 AND item_kind = 'message' AND item_key = $2 AND sender <> $3 LIMIT 1`
	for _, c := range []struct{ name, key string }{
		{"ключ чужой", "'" + foreignKey + "'"}, // g = 12345 нечётное → отправитель B
		{"ключа нет", "'нет-такого-ключа'"},
	} {
		plan, ms := explainPrepared(t, r, "o1", "text, text, text", q, fmt.Sprintf("'%s', %s, '%s'", id, c.key, pidA))
		t.Logf("O1 %s: %s\n%s", c.name, ms, plan)
		require.Contains(t, plan, "Index Scan using engram_position_key_all")
		require.Contains(t, plan, "conversation_id = ")
		require.Contains(t, plan, "item_key = ")
		require.NotContains(t, plan, "Seq Scan")
		require.NotContains(t, plan, "Bitmap")
	}
}

// L4 — план оператора 931 после 6 EXECUTE на объёме спецификации §7: носитель
// на 300 000 элементов (события каждые 500, пары равного CreateAt каждые
// 50), 198 000 носителей по 10 постов, канал двух авторов на 300 000 и
// канал на 3000 постов; Posts — около 2.58 млн строк. Генерация — SQL
// (generate_series). Тест долгий — включается PHOTON_L4=1; PHOTON_L4_SCALE
// (по умолчанию 1) уменьшает объём для быстрой пробы. Условия индекса
// (channelid, deleteat, createat), без Bitmap по каналу и полной сортировки;
// время записывается, порога нет.
func TestConvL4FeedPlan(t *testing.T) {
	if os.Getenv("PHOTON_L4") == "" {
		t.Skip("PHOTON_L4 не задан — тест объёма спецификации §7 включается явно (долгий)")
	}
	scale := 1.0
	if v := os.Getenv("PHOTON_L4_SCALE"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		require.NoError(t, err)
		scale = f
	}
	n := func(x int) int { return int(float64(x) * scale) }

	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	ch := r.carrier(t, id)
	var h int64
	require.NoError(t, r.db.QueryRow(`SELECT position_h FROM engram_conversation WHERE conversation_id = $1`, id).Scan(&h))
	base := h + 1000000
	elements := n(300000)

	exec := func(q string, args ...any) {
		_, err := r.db.Exec(q, args...)
		require.NoError(t, err)
	}
	start := time.Now()
	// Носитель: элементы на позициях base+g; пары равных CreateAt каждые 50,
	// события каждые 500.
	exec(`INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, sender, reserved_at, post_id)
SELECT $1, $2::bigint + g, CASE WHEN g % 500 = 0 THEN 'event' ELSE 'message' END, 'k' || g, 'published',
  CASE WHEN g % 500 = 0 THEN NULL ELSE $3 END, 1, 'p' || g FROM generate_series(1, $4::int) g`, id, base, pidA, elements)
	exec(`INSERT INTO posts (id, channelid, userid, createat, updateat, deleteat, message, type, props)
SELECT 'c' || lpad(g::text, 10, '0') || 'x' || n, $1, 'u', $2::bigint + g, $2::bigint + g, 0, 'm',
  CASE WHEN g % 500 = 0 THEN 'custom_engram_event' ELSE '' END, '{}'::jsonb
FROM generate_series(1, $3::int) g, generate_series(1, CASE WHEN g % 50 = 0 THEN 2 ELSE 1 END) n`, ch, base, elements)
	// Открытые резервы (живые отправители) за концом ленты: в базе их немного,
	// но не ноль — планировщик строит план по настоящей статистике.
	exec(`INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, sender, reserved_at)
SELECT $1, $2::bigint + $3::bigint + g, 'message', 'open' || g, 'reserved', $4, 1 FROM generate_series(1, 20) g`, id, base, elements, pidA)
	exec(`UPDATE engram_conversation SET position_h = $2 WHERE conversation_id = $1`, id, base+int64(elements)+20)
	// Остальные каналы: 198 000 носителей по 10 постов, канал двух авторов,
	// канал с группами по 5 равных CreateAt.
	exec(`INSERT INTO posts (id, channelid, userid, createat, updateat, deleteat, message, type, props)
SELECT 'o' || lpad(c::text, 7, '0') || lpad(p::text, 3, '0'), 'oc' || c, 'u', $1::bigint + p, $1::bigint + p, 0, 'm', '', '{}'::jsonb
FROM generate_series(1, $2::int) c, generate_series(1, 10) p`, base, n(198000))
	exec(`INSERT INTO posts (id, channelid, userid, createat, updateat, deleteat, message, type, props)
SELECT 'dm' || lpad(g::text, 9, '0'), 'dmchannel', 'u' || (g % 2), $1::bigint + g, $1::bigint + g, 0, 'm', '', '{}'::jsonb FROM generate_series(1, $2::int) g`, base, n(300000))
	exec(`INSERT INTO posts (id, channelid, userid, createat, updateat, deleteat, message, type, props)
SELECT 'g' || lpad(g::text, 6, '0'), 'groupchannel', 'u', $1::bigint + g / 5, $1::bigint + g / 5, 0, 'm', '', '{}'::jsonb FROM generate_series(1, 3000) g`, base)
	exec(`ANALYZE posts`)
	exec(`ANALYZE engram_position`)
	var total int
	require.NoError(t, r.db.QueryRow(`SELECT count(*) FROM posts`).Scan(&total))
	t.Logf("L4: объём подготовлен за %s, Posts = %d строк, носитель %d элементов", time.Since(start).Round(time.Second), total, elements)

	mid := base + int64(elements/2)
	cases := []struct {
		name, sqlText, types, args string
	}{
		{"проход, один период, limit 60", convFeedSQL(feedAscend, 1), "text, text, bigint, bigint, bigint",
			fmt.Sprintf("'%s', '%s', %d, 60, 9223372036854775807", id, ch, mid)},
		{"первичная синхронизация, limit 60", convFeedSQL(feedNewest, 1), "text, text, bigint, bigint, bigint",
			fmt.Sprintf("'%s', '%s', %d, 60, 9223372036854775807", id, ch, base)},
		{"проход, три периода, limit 60", convFeedSQL(feedAscend, 3), "text, text, bigint, bigint, bigint, bigint, bigint, bigint, bigint, bigint, bigint",
			fmt.Sprintf("'%s', '%s', %d, 60, 9223372036854775807, %d, %d, %d, %d, %d, 9223372036854775807", id, ch, base+1,
				base+1, base+1000, mid, mid+1000, base+int64(elements)-1000)},
	}
	for _, c := range cases {
		plan, ms := explainPrepared(t, r, "l4", c.types, c.sqlText, c.args)
		t.Logf("L4 %s: %s\n%s", c.name, ms, plan)
		requirePostsIndexPlan(t, c.name, plan)
	}
}

// requirePostsIndexPlan — форма плана L2/L4: страница читается по индексу
// Posts (channelid, deleteat, createat) условием индекса, без Bitmap по
// каналу и без полной сортировки. Допустимы: Incremental Sort с
// Presorted Key createat (внутри групп равного createat для DISTINCT ON) и
// внешний Sort над готовой страницей (строк не больше limit). Индекс
// барьера — engram_position_open.
func requirePostsIndexPlan(t *testing.T, name, plan string) {
	t.Helper()
	scan := regexp.MustCompile(`Index Scan( Backward)? using \S+ on posts p \(actual time=\S+ rows=(\d+)`)
	m := scan.FindStringSubmatch(plan)
	require.NotNil(t, m, "%s: нет Index Scan по posts:\n%s", name, plan)
	scanned, _ := strconv.Atoi(m[2])
	require.LessOrEqual(t, scanned, 200, "%s: индекс читает не больше страницы с запасом на равные createat", name)
	require.Contains(t, plan, "Index Cond: (((channelid)::text = $2) AND (deleteat = 0) AND (createat ", name)
	require.NotContains(t, plan, "Bitmap", name)
	require.NotContains(t, plan, "Seq Scan on posts", name)
	require.Regexp(t, `Index (Only )?Scan using engram_position_open on`, plan, name+": барьер по engram_position_open")
	lines := strings.Split(plan, "\n")
	for i, line := range lines {
		trim := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "->"))
		switch {
		case strings.HasPrefix(trim, "Incremental Sort"):
			require.Contains(t, lines[i+1]+lines[i+2], "Presorted Key: p.createat", "%s: сортировка без предсортированного ключа", name)
		case strings.HasPrefix(trim, "Sort ("):
			require.Equal(t, 0, i, "%s: полная сортировка внутри плана: %s", name, line)
			rowsOf := regexp.MustCompile(`rows=(\d+)`).FindStringSubmatch(trim)
			n, _ := strconv.Atoi(rowsOf[1])
			require.LessOrEqual(t, n, 60, "%s: внешний Sort — только над страницей", name)
		}
	}
}
