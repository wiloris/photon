package main

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
)

// Фоновая сверка (conversation_reconciler.go) против НАСТОЯЩЕГО PostgreSQL:
// незавершённый резерв 930 не держит ленту вечно, восстановление не
// создаёт дублей и не нарушает порядок (contract §3.6, §4.2, §4.5).

// allAbandoned — порог в прошлом на час вперёд: любой резерв «брошен».
// Порог давности проверяется отдельно (TestConvReconcileAbandonedMessageReservations).
func allAbandoned() time.Duration { return -time.Hour }

func reconcilerOf(svc *convService, batch int) *convReconciler {
	return &convReconciler{svc: svc, abandon: allAbandoned, batch: batch}
}

// keyPosts — постов носителя с ключом (включая скрытые надгробием).
func (r *groupRig) keyPosts(t *testing.T, convID, cid string) int {
	t.Helper()
	return r.count(t, `SELECT count(*) FROM posts p JOIN engram_conversation c ON c.carrier_channel_id = p.channelid
WHERE c.conversation_id = $1 AND p.deleteat = 0 AND p.props->>'engram_client_msg_id' = $2`, convID, cid)
}

// 1. Пост создан, итог записан: сверке нечего делать, состояние не меняется.
func TestConvReconcilerLeavesPublishedAlone(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	m := r.mustSend(t, a, id, newRequestID(), "обычное")
	calls := r.postCallCount()

	pass := reconcilerOf(r.svc, 100).run(context.Background())
	require.Equal(t, 0, pass.Selected, "открытых резервов нет — разговор в выборку не попадает")
	require.Equal(t, calls, r.postCallCount())
	r.assertDelivered(t, id, pidA, m)
}

// 2. Пост создан, Bridge подтверждения не получил: сверка находит пост и
// публикует позицию; CreatePost не вызывается; лента идёт дальше; повтор
// клиента получает ту же позицию.
func TestConvReconcilerPublishesCreatedPost(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	cid := newRequestID()
	r.failNextPost(func(m *fakeConvMM, n int) { m.postCreateThenFailOnCall = n })
	require.False(t, r.send(t, a, id, cid, "ответ потерян").OK)
	pos := r.msgRows(t, id, cid)[0].Pos
	after := r.mustSend(t, b, id, newRequestID(), "после")
	require.Equal(t, []string{"event:created", "event:member_added"}, names(r.fullPass(t, b, id, 10)),
		"до сверки барьер держит ленту у читателя")
	calls := r.postCallCount()

	pass := reconcilerOf(r.svc, 100).run(context.Background())
	require.Equal(t, 1, pass.Published)
	require.Equal(t, 0, pass.Aborted)
	require.Equal(t, calls, r.postCallCount(), "пост найден — CreatePost не вызван")
	require.Equal(t, "published", r.stateAt(t, id, pos))
	require.Equal(t, 1, r.keyPosts(t, id, cid), "дубля нет")

	feed := r.fullPass(t, b, id, 10)
	require.Equal(t, []string{"event:created", "event:member_added", "msg:ответ потерян", "msg:после"}, names(feed))
	require.Equal(t, pos, itemAt(feed[2]))
	require.Equal(t, after.CreatedAt, itemAt(feed[3]))

	m := r.mustSend(t, a, id, cid, "ответ потерян")
	require.Equal(t, pos, m.CreatedAt, "повтор — найденная позиция")
	r.assertDelivered(t, id, pidA, m)
	require.Equal(t, calls, r.postCallCount())
}

// 3. Пост не создан, исход неизвестен: надгробие; повтор клиента — новая
// позиция после всех опубликованных; пост ровно один; поздний пост на
// старой позиции в ленту не попадает.
func TestConvReconcilerAbortsMissingPost(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	cid := newRequestID()
	r.failNextPost(func(m *fakeConvMM, n int) { m.postNilOnCall = n })
	require.False(t, r.send(t, a, id, cid, "нет поста").OK)
	pos := r.msgRows(t, id, cid)[0].Pos
	other := r.mustSend(t, b, id, newRequestID(), "чужое")

	pass := reconcilerOf(r.svc, 100).run(context.Background())
	require.Equal(t, [2]int{0, 1}, [2]int{pass.Published, pass.Aborted})
	require.Equal(t, "aborted", r.stateAt(t, id, pos))
	require.Zero(t, r.keyPosts(t, id, cid))

	m := r.mustSend(t, a, id, cid, "нет поста")
	require.Greater(t, m.CreatedAt, other.CreatedAt, "новая позиция — после опубликованного")
	require.Equal(t, 1, r.keyPosts(t, id, cid), "ровно один пост ключа")
	r.assertDelivered(t, id, pidA, m)

	// R1: пост, долетевший на отменённую позицию, скрыт надгробием.
	r.latePost(t, id, a, cid, pos)
	require.Equal(t, []string{"event:created", "event:member_added", "msg:чужое", "msg:нет поста"}, names(r.fullPass(t, b, id, 10)))
}

// 4. Bridge перезапущен при незавершённом резерве: новый процесс (свой
// convService, пустой учёт публикаций, тот же PostgreSQL) закрывает резерв.
func TestConvReconcilerAfterRestart(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	cidLost, cidNone := newRequestID(), newRequestID()
	r.failNextPost(func(m *fakeConvMM, n int) { m.postCreateThenFailOnCall = n })
	require.False(t, r.send(t, a, id, cidLost, "до перезапуска 1").OK)
	r.failNextPost(func(m *fakeConvMM, n int) { m.postNilOnCall = n })
	require.False(t, r.send(t, a, id, cidNone, "до перезапуска 2").OK)

	restarted := &convService{
		db: r.db, mm: r.mm, registry: r.id, users: r.id, botID: convTestBot,
		now:      func() int64 { return time.Now().UnixMilli() },
		lockWait: 10 * time.Second, pollEvery: 5 * time.Millisecond,
		logger: func(string, ...any) {}, inflight: &convInflight{},
	}
	pass := reconcilerOf(restarted, 100).run(context.Background())
	require.Equal(t, [2]int{1, 1}, [2]int{pass.Published, pass.Aborted})
	require.Equal(t, []string{"event:created", "event:member_added", "msg:до перезапуска 1"}, names(r.fullPass(t, b, id, 10)))
	m := msgOf(t, restarted.dispatch(context.Background(), a, ActionConversationSend, pl(t, sendReq(id, cidNone, "до перезапуска 2"))))
	r.assertDelivered(t, id, pidA, m)
}

// 5. Повторная сверка ничего не меняет и постов не создаёт.
func TestConvReconcilerRepeatIsNoop(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	cidLost, cidNone := newRequestID(), newRequestID()
	r.failNextPost(func(m *fakeConvMM, n int) { m.postCreateThenFailOnCall = n })
	require.False(t, r.send(t, a, id, cidLost, "x").OK)
	r.failNextPost(func(m *fakeConvMM, n int) { m.postNilOnCall = n })
	require.False(t, r.send(t, a, id, cidNone, "y").OK)
	rec := reconcilerOf(r.svc, 100)
	first := rec.run(context.Background())
	require.Equal(t, [2]int{1, 1}, [2]int{first.Published, first.Aborted})
	calls := r.postCallCount()
	before := r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND state = 'published'`, id)

	for i := 0; i < 3; i++ {
		again := rec.run(context.Background())
		require.Equal(t, convReconcilePass{}, again, "проход %d", i+2)
	}
	require.Equal(t, calls, r.postCallCount())
	require.Equal(t, before, r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND state = 'published'`, id))
	require.Equal(t, 1, r.keyPosts(t, id, cidLost))
	require.Zero(t, r.keyPosts(t, id, cidNone))
}

// 6, 7, 8. Несколько незавершённых резервов вперемешку с опубликованными:
// один проход закрывает все; порядок ленты — по позициям; дублей нет;
// читатель снова получает всё через 931, включая повторённое сообщение.
func TestConvReconcilerSeveralReservationsKeepOrder(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	m1 := r.mustSend(t, a, id, newRequestID(), "m1")
	cid1, cid2, cid3 := newRequestID(), newRequestID(), newRequestID()
	r.failNextPost(func(m *fakeConvMM, n int) { m.postNilOnCall = n })
	require.False(t, r.send(t, a, id, cid1, "r1-нет").OK)
	b1 := r.mustSend(t, b, id, newRequestID(), "b1")
	r.failNextPost(func(m *fakeConvMM, n int) { m.postCreateThenFailOnCall = n })
	require.False(t, r.send(t, a, id, cid2, "r2-есть").OK)
	b2 := r.mustSend(t, b, id, newRequestID(), "b2")
	r.failNextPost(func(m *fakeConvMM, n int) { m.postCreateThenFailOnCall = n })
	require.False(t, r.send(t, a, id, cid3, "r3-есть").OK)
	p1, p2, p3 := r.msgRows(t, id, cid1)[0].Pos, r.msgRows(t, id, cid2)[0].Pos, r.msgRows(t, id, cid3)[0].Pos
	require.True(t, m1.CreatedAt < p1 && p1 < b1.CreatedAt && b1.CreatedAt < p2 && p2 < b2.CreatedAt && b2.CreatedAt < p3)

	require.Equal(t, []string{"event:created", "event:member_added", "msg:m1"}, names(r.fullPass(t, b, id, 10)),
		"барьер — самый ранний открытый резерв")

	pass := reconcilerOf(r.svc, 100).run(context.Background())
	require.Equal(t, [2]int{2, 1}, [2]int{pass.Published, pass.Aborted})
	require.Equal(t, 1, pass.Done)

	feed := r.fullPass(t, b, id, 2)
	require.Equal(t, []string{"event:created", "event:member_added", "msg:m1", "msg:b1", "msg:r2-есть", "msg:b2", "msg:r3-есть"}, names(feed))
	for i := 1; i < len(feed); i++ {
		require.Less(t, itemAt(feed[i-1]), itemAt(feed[i]), "позиции строго возрастают")
	}

	re := r.mustSend(t, a, id, cid1, "r1-нет")
	require.Greater(t, re.CreatedAt, p3)
	for _, cid := range []string{cid1, cid2, cid3} {
		require.Equal(t, 1, r.keyPosts(t, id, cid), "один пост на ключ")
	}
	require.Equal(t, []string{"event:created", "event:member_added", "msg:m1", "msg:b1", "msg:r2-есть", "msg:b2", "msg:r3-есть", "msg:r1-нет"},
		names(r.fullPass(t, b, id, 3)))
}

// Публикация, идущая в этом процессе, не закрывается сверкой, даже если её
// резерв старше порога: «поста ещё нет» — не установленное отсутствие.
func TestConvReconcilerSkipsInFlightPublication(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	cid := newRequestID()
	entered, gate := make(chan struct{}), make(chan struct{})
	r.mm.mu.Lock()
	r.mm.postBeforeCreate = func(p *model.Post) {
		if p.Type == "" {
			close(entered)
			<-gate
		}
	}
	r.mm.mu.Unlock()
	done := sendAsync(r, a, id, cid, "в полёте")
	<-entered
	pos := r.msgRows(t, id, cid)[0].Pos
	_, err := r.db.Exec(`UPDATE engram_position SET reserved_at = 1 WHERE conversation_id = $1 AND pos = $2`, id, pos)
	require.NoError(t, err)

	pass := reconcilerOf(r.svc, 100).run(context.Background())
	require.Equal(t, 1, pass.Selected, "резерв старше порога — разговор выбран")
	require.Equal(t, [2]int{0, 0}, [2]int{pass.Published, pass.Aborted}, "публикация в полёте не тронута")
	require.Equal(t, "reserved", r.stateAt(t, id, pos))

	close(gate)
	m := msgOf(t, recv(t, done))
	r.mm.mu.Lock()
	r.mm.postBeforeCreate = nil
	r.mm.mu.Unlock()
	require.Equal(t, pos, m.CreatedAt, "владелец довёл свою публикацию")
	r.assertDelivered(t, id, pidA, m)
	require.Equal(t, []string{"event:created", "event:member_added", "msg:в полёте"}, names(r.fullPass(t, b, id, 10)))
}

// Защита публикации в полёте ограничена: зависший вызов не держит барьер
// вечно.
func TestConvInflightGuardIsBounded(t *testing.T) {
	var f convInflight
	end := f.begin("c1", 100, 1_000)
	f.begin("c2", 200, 1_000)
	require.Equal(t, []int64{100}, f.positions("c1", 1_000))
	require.Empty(t, f.positions("c1", 1_001), "старше предела — не защищена")
	end()
	require.Empty(t, f.positions("c1", 0), "снята после завершения")
	require.Equal(t, []int64{200}, f.positions("c2", 0))
	var none *convInflight
	none.begin("c", 1, 1)()
	require.Empty(t, none.positions("c", 0))
}

// Проход по кругу: разговор, который раз за разом не закрывается (здесь —
// публикация в полёте), не загораживает остальные при пакете в один
// разговор.
func TestConvReconcilerRotatesPastStuckConversation(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	ids := []string{r.group(t, a), r.group(t, a), r.group(t, a)}
	sort.Strings(ids)

	// Самый первый по порядку — с публикацией в полёте.
	cidStuck := newRequestID()
	entered, gate := make(chan struct{}), make(chan struct{})
	r.mm.mu.Lock()
	r.mm.postBeforeCreate = func(p *model.Post) {
		if p.Type == "" && p.GetProp(dmPropClientMsgID) == cidStuck {
			close(entered)
			<-gate
		}
	}
	r.mm.mu.Unlock()
	done := sendAsync(r, a, ids[0], cidStuck, "застряло")
	<-entered
	_, err := r.db.Exec(`UPDATE engram_position SET reserved_at = 1 WHERE conversation_id = $1`, ids[0])
	require.NoError(t, err)
	cids := []string{"", newRequestID(), newRequestID()}
	for _, i := range []int{1, 2} {
		r.failNextPost(func(m *fakeConvMM, n int) { m.postNilOnCall = n })
		require.False(t, r.send(t, a, ids[i], cids[i], "брошено").OK)
	}

	rec := reconcilerOf(r.svc, 1)
	for i := 0; i < 3; i++ {
		rec.run(context.Background())
	}
	require.Equal(t, "aborted", r.msgRows(t, ids[1], cids[1])[0].State)
	require.Equal(t, "aborted", r.msgRows(t, ids[2], cids[2])[0].State)
	require.Equal(t, "reserved", r.msgRows(t, ids[0], cidStuck)[0].State)

	close(gate)
	require.True(t, recv(t, done).OK)
	r.mm.mu.Lock()
	r.mm.postBeforeCreate = nil
	r.mm.mu.Unlock()
}

// Занятый X (событие не дописать) не мешает закрыть брошенный резерв
// сообщения того же разговора.
func TestConvReconcileMessagesDoNotWaitForEvents(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	cid := newRequestID()
	r.failNextPost(func(m *fakeConvMM, n int) { m.postCreateThenFailOnCall = n })
	require.False(t, r.send(t, a, id, cid, "есть пост").OK)
	r.failNextPost(func(m *fakeConvMM, n int) { m.postFailOnCall = n })
	require.False(t, r.do(t, b, ActionConversationLeave, reqOf(id)).OK, "пост события не записан")
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1 AND state = 'pending'`, id))

	held, err := beginHeld(t, r.db)
	require.NoError(t, err)
	_, err = held.Exec(`SELECT pg_advisory_xact_lock($1)`, convLockKey(id))
	require.NoError(t, err)
	quick := &convService{
		db: r.db, mm: r.mm, registry: r.id, users: r.id, botID: convTestBot,
		now:      func() int64 { return time.Now().UnixMilli() },
		lockWait: 100 * time.Millisecond, pollEvery: 5 * time.Millisecond,
		logger: func(string, ...any) {}, inflight: &convInflight{},
	}
	res, err := quick.reconcileConversation(context.Background(), id, 1<<62)
	require.Error(t, err, "событие не дописано: X занят")
	require.Equal(t, convReconcileResult{Published: 1}, res, "резерв сообщения закрыт и без X")
	require.Equal(t, "published", r.msgRows(t, id, cid)[0].State)
	require.NoError(t, held.Rollback())
}

func TestConvReconcileSettings(t *testing.T) {
	i, a := convReconcileSettings(nil)
	require.Equal(t, [2]time.Duration{time.Minute, 5 * time.Minute}, [2]time.Duration{i, a})
	i, a = convReconcileSettings(&configuration{ConvReconcileIntervalSec: 1, ConvReconcileAbandonSec: 1})
	require.Equal(t, [2]time.Duration{10 * time.Second, 2 * convCallTimeout}, [2]time.Duration{i, a}, "ниже минимума — минимум")
	i, a = convReconcileSettings(&configuration{ConvReconcileIntervalSec: 30, ConvReconcileAbandonSec: 600})
	require.Equal(t, [2]time.Duration{30 * time.Second, 10 * time.Minute}, [2]time.Duration{i, a})
}
