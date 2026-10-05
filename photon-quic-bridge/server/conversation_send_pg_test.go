package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin"
	"github.com/stretchr/testify/require"
)

// 930 SEND против НАСТОЯЩЕГО PostgreSQL (implementation contract §3, §5.2,
// §6.2: P2, P4, P6, P8 и X против S; ADR-101 §5.1, §5.3, §5.6, §5.10;
// ADR-102). PHOTON_DM_PG_DSN. Mattermost — fakeConvMM: пост пишется своим
// соединением, и каждый пост сообщения проверяется на зафиксированный
// резерв своей позиции (S10, проверка в newGroupRig).

// convTestSenders — одновременных 930 в конкурентных прогонах.
const convTestSenders = 16

func sendReq(convID, cid, text string) map[string]any {
	return map[string]any{"conversation_id": convID, "client_msg_id": cid, "text": text}
}

func (r *groupRig) send(t *testing.T, callerMM, convID, cid, text string) Response {
	t.Helper()
	return r.svc.dispatch(context.Background(), callerMM, ActionConversationSend, pl(t, sendReq(convID, cid, text)))
}

func msgOf(t *testing.T, resp Response) ConvMessage {
	t.Helper()
	require.True(t, resp.OK, "930: %+v", resp)
	return resp.Data.(ConvSendResponse).Message
}

func (r *groupRig) mustSend(t *testing.T, callerMM, convID, cid, text string) ConvMessage {
	t.Helper()
	return msgOf(t, r.send(t, callerMM, convID, cid, text))
}

func (r *groupRig) posH(t *testing.T, convID string) int64 {
	t.Helper()
	var h int64
	require.NoError(t, r.db.QueryRow(`SELECT position_h FROM engram_conversation WHERE conversation_id = $1`, convID).Scan(&h))
	return h
}

func (r *groupRig) postCallCount() int {
	r.mm.mu.Lock()
	defer r.mm.mu.Unlock()
	return r.mm.postCalls
}

// msgRow — резерв сообщения.
type msgRow struct {
	Pos    int64
	State  string
	Sender string
	PostID string
	Hash   string
}

func (r *groupRig) msgRows(t *testing.T, convID, cid string) []msgRow {
	t.Helper()
	rows, err := r.db.Query(`SELECT pos, state, sender, COALESCE(post_id, ''), encode(payload_hash, 'hex') FROM engram_position
WHERE conversation_id = $1 AND item_kind = 'message' AND item_key = $2 ORDER BY pos`, convID, cid)
	require.NoError(t, err)
	defer rows.Close()
	var out []msgRow
	for rows.Next() {
		var m msgRow
		require.NoError(t, rows.Scan(&m.Pos, &m.State, &m.Sender, &m.PostID, &m.Hash))
		out = append(out, m)
	}
	require.NoError(t, rows.Err())
	return out
}

func (r *groupRig) stateAt(t *testing.T, convID string, pos int64) string {
	t.Helper()
	var st string
	require.NoError(t, r.db.QueryRow(`SELECT state FROM engram_position WHERE conversation_id = $1 AND pos = $2`, convID, pos).Scan(&st))
	return st
}

// assertDelivered — «0 потерь при успехе» (contract §6.2 P4): ответу 930
// соответствует опубликованный резерв на позиции created_at и неудалённый
// пост носителя с CreateAt = позиция, тем же ключом и id ответа.
func (r *groupRig) assertDelivered(t *testing.T, convID, senderPID string, m ConvMessage) {
	t.Helper()
	var state, sender, key, postID string
	require.NoError(t, r.db.QueryRow(`SELECT state, sender, item_key, COALESCE(post_id, '') FROM engram_position
WHERE conversation_id = $1 AND pos = $2`, convID, m.CreatedAt).Scan(&state, &sender, &key, &postID))
	require.Equal(t, "published", state, "резерв позиции ответа")
	require.Equal(t, senderPID, sender)
	require.Equal(t, m.ClientMsgID, key)
	require.Equal(t, m.ID, postID)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM posts p JOIN engram_conversation c ON c.carrier_channel_id = p.channelid
WHERE c.conversation_id = $1 AND p.id = $2 AND p.createat = $3 AND p.deleteat = 0 AND p.type = ''
AND p.props->>'engram_client_msg_id' = $4`, convID, m.ID, m.CreatedAt, m.ClientMsgID), "пост на позиции ответа")
	require.LessOrEqual(t, r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND item_key = $2
AND state = 'published'`, convID, m.ClientMsgID), 1, "не больше одной опубликованной позиции на ключ")
}

// reconcileKey — заменитель сверки среза 6 (contract §3.6) в тестах:
// открытый резерв ключа закрывается одним условным оператором — пост с
// CreateAt = позиция и ключом найден → published, иначе → aborted.
func (r *groupRig) reconcileKey(t *testing.T, convID, cid string) {
	t.Helper()
	_, err := r.db.Exec(`UPDATE engram_position pp SET
  state = CASE WHEN f.id IS NULL THEN 'aborted' ELSE 'published' END, post_id = f.id
FROM engram_conversation c LEFT JOIN LATERAL (
  SELECT id FROM posts WHERE channelid = c.carrier_channel_id AND deleteat = 0
  AND createat = (SELECT pos FROM engram_position WHERE conversation_id = $1 AND item_key = $2 AND state = 'reserved')
  AND props->>'engram_client_msg_id' = $2 ORDER BY id LIMIT 1) f ON true
WHERE c.conversation_id = $1 AND pp.conversation_id = $1 AND pp.item_key = $2 AND pp.state = 'reserved'`, convID, cid)
	require.NoError(t, err)
}

// latePost — пост, долетевший на позицию после отмены её резерва (R1).
func (r *groupRig) latePost(t *testing.T, convID, userMM, cid string, at int64) {
	t.Helper()
	var ch string
	require.NoError(t, r.db.QueryRow(`SELECT carrier_channel_id FROM engram_conversation WHERE conversation_id = $1`, convID).Scan(&ch))
	pgInsert(t, r.db, model.NewId(), ch, userMM, at, "поздний", cid)
}

// waitLockWait — оператор, содержащий фрагмент query, ждёт блокировку
// (pg_stat_activity): точка детерминированного чередования без sleep.
func waitLockWait(t *testing.T, db *sql.DB, fragment string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM pg_stat_activity
WHERE wait_event_type = 'Lock' AND position($1 in query) > 0`, fragment).Scan(&n))
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("оператор %q так и не встал в ожидание блокировки", fragment)
}

// beginHeld — транзакция теста, удерживающая блокировку. Откат — и в
// очистке: упавшая проверка не оставляет 930 и DROP SCHEMA ждать вечно.
func beginHeld(t *testing.T, db *sql.DB) (*sql.Tx, error) {
	t.Helper()
	tx, err := db.Begin()
	if err == nil {
		t.Cleanup(func() { _ = tx.Rollback() })
	}
	return tx, err
}

// tryX — можно ли сейчас взять X(разговор) с отдельного соединения.
func tryX(t *testing.T, db *sql.DB, convID string) bool {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()
	var got bool
	require.NoError(t, tx.QueryRow(`SELECT pg_try_advisory_xact_lock($1)`, convLockKey(convID)).Scan(&got))
	return got
}

func sendAsync(r *groupRig, callerMM, convID, cid, text string) <-chan Response {
	ch := make(chan Response, 1)
	raw, _ := json.Marshal(sendReq(convID, cid, text))
	go func() { ch <- r.svc.dispatch(context.Background(), callerMM, ActionConversationSend, raw) }()
	return ch
}

func notYet(t *testing.T, ch <-chan Response, wait time.Duration, msg string) {
	t.Helper()
	select {
	case resp := <-ch:
		t.Fatalf("%s: вызов завершился раньше времени: %+v", msg, resp)
	case <-time.After(wait):
	}
}

func recv(t *testing.T, ch <-chan Response) Response {
	t.Helper()
	select {
	case resp := <-ch:
		return resp
	case <-time.After(30 * time.Second):
		t.Fatal("вызов не завершился")
	}
	return Response{}
}

// ── форма, допуск, повтор ────────────────────────────────────────────────────

// Успешная отправка (contract §3.3–§3.5, ADR-099 §9.1, ADR-103 §1): резерв
// до CreatePost, явный CreateAt = P, published с id поста, H = P,
// last_activity_at = P; ответ — ровно поля §9.1, created_at = P.
func TestConv930SendPublishes(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	h0 := r.posH(t, id)
	cid := newRequestID()
	resp := r.send(t, b, id, strings.ToUpper(cid), "привет")
	m := msgOf(t, resp)
	require.Equal(t, pidB, m.AuthorPhotonID)
	require.Equal(t, "привет", m.Text)
	require.Equal(t, cid, m.ClientMsgID, "ключ без учёта регистра UUID")
	require.Greater(t, m.CreatedAt, h0)
	require.Equal(t, m.CreatedAt, r.posH(t, id), "H := P")
	r.assertDelivered(t, id, pidB, m)
	var last int64
	require.NoError(t, r.db.QueryRow(`SELECT last_activity_at FROM engram_conversation WHERE conversation_id = $1`, id).Scan(&last))
	require.Equal(t, m.CreatedAt, last)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM posts WHERE id = $1 AND userid = $2`, m.ID, b), "автор поста — аккаунт отправителя")

	body, err := json.Marshal(resp.Data)
	require.NoError(t, err)
	var shape map[string]map[string]any
	require.NoError(t, json.Unmarshal(body, &shape))
	require.Len(t, shape, 1)
	keys := []string{}
	for k := range shape["message"] {
		keys = append(keys, k)
	}
	require.ElementsMatch(t, []string{"id", "author_photon_id", "text", "created_at", "client_msg_id"}, keys)

	// PendingPostId либо не задан, либо привязан к позиции (contract §3.4).
	r.mm.mu.Lock()
	for _, pp := range r.mm.pendingIDs {
		require.True(t, pp == "" || strings.Contains(pp, strconv.FormatInt(m.CreatedAt, 10)), "PendingPostId %q без позиции", pp)
	}
	r.mm.mu.Unlock()

	// Последовательные сообщения: позиции растут (ADR-101 §5.3).
	m2 := r.mustSend(t, a, id, newRequestID(), "второе")
	require.Greater(t, m2.CreatedAt, m.CreatedAt)
}

func TestConv930Validation(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	calls := r.postCallCount()
	for name, tc := range map[string]struct {
		payload map[string]any
		code    string
	}{
		"unknown field":   {map[string]any{"conversation_id": id, "client_msg_id": newRequestID(), "text": "x", "extra": 1}, convErrInvalidRequest},
		"no client_msg":   {map[string]any{"conversation_id": id, "text": "x"}, convErrInvalidRequest},
		"bad client_msg":  {sendReq(id, "not-a-uuid", "x"), convErrInvalidRequest},
		"no conversation": {map[string]any{"client_msg_id": newRequestID(), "text": "x"}, convErrInvalidRequest},
		"empty":           {sendReq(id, newRequestID(), "  \n"), errMessageEmpty},
		"too long":        {sendReq(id, newRequestID(), strings.Repeat("я", directMessageMaxRunes+1)), errMessageTooLong},
		"too many links":  {sendReq(id, newRequestID(), strings.Repeat("https://e.x ", dmMaxLinks+1)), errMessageTooManyLinks},
	} {
		resp := r.svc.dispatch(context.Background(), a, ActionConversationSend, pl(t, tc.payload))
		require.Equal(t, tc.code, resp.Error, name)
	}
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_position WHERE item_kind = 'message'`))
	require.Equal(t, calls, r.postCallCount())
	require.True(t, r.send(t, a, id, newRequestID(), strings.Repeat("я", directMessageMaxRunes)).OK, "предел включительно")
}

// Доступ → состояние → право (ADR-099 §11, §8.1; contract §3.1 п. 2).
func TestConv930Access(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB)
	calls := r.postCallCount()
	require.Equal(t, convErrConversationNF, r.send(t, c, id, newRequestID(), "x").Error, "никогда не участвовавший")
	require.Equal(t, convErrConversationNF, r.send(t, r.id.plainUser(), id, newRequestID(), "x").Error, "без PhotonID")
	require.Equal(t, convErrConversationNF, r.send(t, a, "нет-такого", newRequestID(), "x").Error)

	// Право проверяется по таблице ролей (ADR-099 §7.3): у member его
	// временно нет.
	saved := convPermissions["member"]
	convPermissions["member"] = []string{"messages.read"}
	resp := r.send(t, b, id, newRequestID(), "x")
	convPermissions["member"] = saved
	require.Equal(t, convErrForbidden, resp.Error)

	r.mustDo(t, a, ActionConversationMemberRemove, reqOf(id, "photon_id", pidB))
	require.Equal(t, convErrNoAccess, r.send(t, b, id, newRequestID(), "x").Error)
	r.mustDo(t, a, ActionConversationArchive, reqOf(id))
	require.Equal(t, convErrArchived, r.send(t, a, id, newRequestID(), "x").Error)
	require.Equal(t, convErrNoAccess, r.send(t, b, id, newRequestID(), "x").Error, "участие раньше состояния")
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_position WHERE item_kind = 'message'`))
	require.Equal(t, calls+2, r.postCallCount(), "CreatePost — только события 911 и 904")
}

// Повтор (ADR-101 §5.6, I6): тот же ключ и текст — тот же ответ без нового
// поста; другой текст — idempotency_conflict.
func TestConv930IdempotentRetry(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	cid := newRequestID()
	first := r.mustSend(t, a, id, cid, "текст")
	calls := r.postCallCount()
	h := r.posH(t, id)
	require.Equal(t, first, r.mustSend(t, a, id, cid, "текст"))
	require.Equal(t, first, r.mustSend(t, a, id, strings.ToUpper(cid), "текст"))
	require.Equal(t, convErrIdempotency, r.send(t, a, id, cid, "другой").Error)
	require.Equal(t, calls, r.postCallCount(), "повтор не вызывает CreatePost")
	require.Equal(t, h, r.posH(t, id), "повтор не выдаёт позиций")
	require.Len(t, r.msgRows(t, id, cid), 1)
}

// ── исходы CreatePost (contract §3.4–§3.6) ───────────────────────────────────

// Неопределённый исход — обрыв RPC (nil, nil) или потерянный ответ (пост
// записан, R1): резерв остаётся reserved, успех не сообщается, повтор —
// message_in_progress без CreatePost и без новой позиции.
func TestConv930UncertainOutcomeKeepsReservation(t *testing.T) {
	for _, mode := range []string{"rpc-nil", "lost-response"} {
		t.Run(mode, func(t *testing.T) {
			r := newGroupRig(t, 2000)
			a := r.id.person(pidA)
			id := r.group(t, a)
			r.mm.mu.Lock()
			if mode == "rpc-nil" {
				r.mm.postNilOnCall = r.mm.postCalls + 1
			} else {
				r.mm.postCreateThenFailOnCall = r.mm.postCalls + 1
			}
			r.mm.mu.Unlock()
			cid := newRequestID()
			require.Equal(t, convErrTemporarilyUnavail, r.send(t, a, id, cid, "x").Error)
			rows := r.msgRows(t, id, cid)
			require.Len(t, rows, 1)
			require.Equal(t, "reserved", rows[0].State, "неопределённость не отменяет резерв")
			calls, h := r.postCallCount(), r.posH(t, id)
			require.Equal(t, errMessageInProgress, r.send(t, a, id, cid, "x").Error)
			require.Equal(t, calls, r.postCallCount())
			require.Equal(t, h, r.posH(t, id))
			require.Equal(t, "reserved", r.msgRows(t, id, cid)[0].State)
		})
	}
}

// Однозначный отказ (ошибка 9.11.18 до записи поста, createPostRejected) —
// reserved → aborted, успех не сообщается; повтор — новая позиция больше
// прежней. Отказ по длине — message_too_long, как 217.
func TestConv930ConfirmedFailureAborts(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	for _, tc := range []struct {
		errID, where string
		status       int
		code         string
	}{
		{"", "", 0, convErrTemporarilyUnavail},
		{mmPostTooLongErrorID, "Post.IsValid", 0, errMessageTooLong},
		{plugin.DismissPostError, "createPost", 0, convErrTemporarilyUnavail},
		{mmChannelNotFoundErrorID, "CreatePostMissingChannel", http.StatusNotFound, convErrTemporarilyUnavail},
		{mmMissingAccountErrorID, "CreatePost", http.StatusNotFound, convErrTemporarilyUnavail},
	} {
		r.mm.mu.Lock()
		r.mm.postRejectOnCall, r.mm.postRejectID = r.mm.postCalls+1, tc.errID
		r.mm.postRejectWhere, r.mm.postRejectStatus = tc.where, tc.status
		r.mm.mu.Unlock()
		cid := newRequestID()
		require.Equal(t, tc.code, r.send(t, a, id, cid, "x").Error)
		rows := r.msgRows(t, id, cid)
		require.Len(t, rows, 1)
		require.Equal(t, "aborted", rows[0].State)
		m := r.mustSend(t, a, id, cid, "x")
		require.Greater(t, m.CreatedAt, rows[0].Pos)
		r.assertDelivered(t, id, pidA, m)
		require.Equal(t, "aborted", r.stateAt(t, id, rows[0].Pos), "надгробие постоянно")
	}
}

// 4xx — не подтверждение, что пост не создан (contract §3.4). 9.11.18:
// после Post().Save App.CreatePost вызывает SanitizePostMetadataForUser, и
// GetChannel канала из permalink в тексте отвечает 404
// app.channel.get.existing.app_error — пост уже записан. Такой исход и 4xx
// вне закрытого списка — неопределённость: резерв остаётся reserved,
// повтор — message_in_progress без CreatePost. Контроль — «любой 4xx —
// aborted»: резерв записанного поста отменён, пост лежит на надгробии.
func TestConv930Unconfirmed4xxKeepsReservation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		written bool
		err     *model.AppError
	}{
		{"post-save-permalink-404", true, &model.AppError{Id: mmChannelNotFoundErrorID, Where: "GetChannel", StatusCode: http.StatusNotFound}},
		{"post-save-other-4xx", true, &model.AppError{Id: "api.context.permissions.app_error", Where: "SanitizePostMetadataForUser", StatusCode: http.StatusForbidden}},
		{"unlisted-4xx", false, &model.AppError{Id: "api.post.create_post.root_id.app_error", Where: "createPost", StatusCode: http.StatusBadRequest}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newGroupRig(t, 2000)
			a := r.id.person(pidA)
			id := r.group(t, a)
			r.mm.mu.Lock()
			if tc.written {
				r.mm.postCreateThenFailOnCall, r.mm.postLostErr = r.mm.postCalls+1, tc.err
			} else {
				r.mm.postRejectOnCall = r.mm.postCalls + 1
				r.mm.postRejectID, r.mm.postRejectWhere, r.mm.postRejectStatus = tc.err.Id, tc.err.Where, tc.err.StatusCode
			}
			r.mm.mu.Unlock()
			cid := newRequestID()
			require.Equal(t, convErrTemporarilyUnavail, r.send(t, a, id, cid, "x").Error)
			rows := r.msgRows(t, id, cid)
			require.Len(t, rows, 1)
			require.Equal(t, "reserved", rows[0].State, "неподтверждённый отказ не отменяет резерв")
			calls, h := r.postCallCount(), r.posH(t, id)
			require.Equal(t, errMessageInProgress, r.send(t, a, id, cid, "x").Error)
			require.Equal(t, calls, r.postCallCount())
			require.Equal(t, h, r.posH(t, id))
			if tc.written {
				require.Equal(t, 1, r.count(t, `SELECT count(*) FROM posts p JOIN engram_conversation c ON c.carrier_channel_id = p.channelid
WHERE c.conversation_id = $1 AND p.createat = $2 AND p.props->>'engram_client_msg_id' = $3`, id, rows[0].Pos, cid),
					"пост записан на позиции открытого резерва — его найдёт сверка")
			}
		})
	}
}

// CreateAt возвращённого поста не равен P (ADR-101 §5.6) — резерв
// отменяется, успех не сообщается; повтор — новая позиция.
func TestConv930CreateAtMismatchAborts(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	r.mm.mu.Lock()
	r.mm.postShiftCreateAt = 7
	r.mm.mu.Unlock()
	cid := newRequestID()
	resp := r.send(t, a, id, cid, "x")
	require.False(t, resp.OK, "%+v", resp)
	rows := r.msgRows(t, id, cid)
	require.Len(t, rows, 1)
	require.Equal(t, "aborted", rows[0].State)
	r.mm.mu.Lock()
	r.mm.postShiftCreateAt = 0
	r.mm.mu.Unlock()
	m := r.mustSend(t, a, id, cid, "x")
	require.Greater(t, m.CreatedAt, rows[0].Pos)
	r.assertDelivered(t, id, pidA, m)
}

// Резерв отменён сверкой, пока CreatePost был в полёте: переход →
// published не проходит, успех не сообщается, надгробие не меняется
// (ADR-101 §5.10, contract §3.5).
func TestConv930NoPublishOverAbortedReservation(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	cid := newRequestID()
	r.mm.mu.Lock()
	r.mm.postAfterCreate = func(p *model.Post) {
		if p.Type == "" {
			_, err := r.db.Exec(`UPDATE engram_position SET state = 'aborted' WHERE conversation_id = $1 AND pos = $2 AND state = 'reserved'`, id, p.CreateAt)
			require.NoError(t, err)
		}
	}
	r.mm.mu.Unlock()
	resp := r.send(t, a, id, cid, "x")
	require.False(t, resp.OK, "%+v", resp)
	rows := r.msgRows(t, id, cid)
	require.Len(t, rows, 1)
	require.Equal(t, "aborted", rows[0].State)
	require.Empty(t, rows[0].PostID)
	r.mm.mu.Lock()
	r.mm.postAfterCreate = nil
	r.mm.mu.Unlock()
	m := r.mustSend(t, a, id, cid, "x")
	require.Greater(t, m.CreatedAt, rows[0].Pos)
	r.assertDelivered(t, id, pidA, m)
}

// ── P2: порядок фиксации резервов (contract §6.2, S7–S9) ─────────────────────

// Пока транзакция резерва 930 не зафиксирована, никто не выдаёт следующую
// позицию разговора: вторая транзакция резерва ждёт строку H. Контроль —
// выдача позиции вне транзакции резерва (как nextval): вторая проходит.
// Попутно: S(разговор) держится и в момент резерва — X взять нельзя.
func TestConvP2ReserveHoldsHUntilCommit(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	h0 := r.posH(t, id)

	// SHARE не мешает чтениям, но останавливает INSERT резерва — 930 стоит
	// внутри транзакции резерва, после UPDATE строки H.
	hold, err := beginHeld(t, r.db)
	require.NoError(t, err)
	_, err = hold.Exec(`LOCK TABLE engram_position IN SHARE MODE`)
	require.NoError(t, err)
	done := sendAsync(r, a, id, newRequestID(), "x")
	waitLockWait(t, r.db, "INSERT INTO engram_position")

	probe, err := beginHeld(t, r.db)
	require.NoError(t, err)
	_, err = probe.Exec(`SET LOCAL lock_timeout = '300ms'`)
	require.NoError(t, err)
	_, err = probe.Exec(`UPDATE engram_conversation SET position_h = GREATEST($2, position_h + 1) WHERE conversation_id = $1`, id, time.Now().UnixMilli())
	require.Error(t, err, "вторая выдача позиции обязана ждать фиксации первой (S8)")
	require.NoError(t, probe.Rollback())
	require.False(t, tryX(t, r.db, id), "S держится до фиксации резерва (ADR-101 §5.3)")

	require.NoError(t, hold.Rollback())
	m := msgOf(t, recv(t, done))
	require.Greater(t, m.CreatedAt, h0)
	require.Equal(t, m.CreatedAt, r.posH(t, id))
	require.True(t, tryX(t, r.db, id), "после ответа S снята")
}

// Позиции конкурентных отправителей уникальны и равны H по завершении
// (I4); ни одной пропущенной выдачи между резервами нет.
func TestConvP2ConcurrentSendersUniquePositions(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB, pidC)
	r.svc.now = func() int64 { return 1000 } // часы стоят: позиции только из H
	h0 := r.posH(t, id)
	callers := []string{a, b, c}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var got []int64
	// 930 держит до трёх соединений (блокировка отправки, S, резерв);
	// предел max_connections временного PostgreSQL — 100.
	slots := make(chan struct{}, convTestSenders)
	for i := 0; i < 48; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			raw, _ := json.Marshal(sendReq(id, newRequestID(), fmt.Sprint("m", i)))
			resp := r.svc.dispatch(context.Background(), callers[i%3], ActionConversationSend, raw)
			mu.Lock()
			defer mu.Unlock()
			if !resp.OK {
				t.Errorf("отправка %d: %+v", i, resp)
				return
			}
			got = append(got, resp.Data.(ConvSendResponse).Message.CreatedAt)
		}(i)
	}
	wg.Wait()
	require.Len(t, got, 48)
	seen := map[int64]bool{}
	for _, p := range got {
		require.False(t, seen[p], "позиция %d выдана дважды", p)
		seen[p] = true
		require.True(t, p > h0 && p <= h0+48, "позиция %d вне H0+1..H0+48", p)
	}
	require.Equal(t, h0+48, r.posH(t, id))
}

// ── P4: повтор после отмены (contract §6.2, ADR-101 §5.12 п. 2) ──────────────

func TestConvP4RetryAfterAbort(t *testing.T) {
	setup := func(t *testing.T) (*groupRig, string, string) {
		r := newGroupRig(t, 2000)
		a := r.id.person(pidA)
		return r, a, r.group(t, a)
	}
	failNext := func(r *groupRig, mode string) {
		r.mm.mu.Lock()
		defer r.mm.mu.Unlock()
		switch mode {
		case "nil":
			r.mm.postNilOnCall = r.mm.postCalls + 1
		case "lost":
			r.mm.postCreateThenFailOnCall = r.mm.postCalls + 1
		}
	}

	// 1. Повтор до позднего поста: новая позиция; поздний пост на
	// надгробии потом не находится повтором.
	t.Run("before-late-post", func(t *testing.T) {
		r, a, id := setup(t)
		cid := newRequestID()
		failNext(r, "nil")
		require.False(t, r.send(t, a, id, cid, "x").OK)
		p1 := r.msgRows(t, id, cid)[0].Pos
		r.reconcileKey(t, id, cid)
		require.Equal(t, "aborted", r.stateAt(t, id, p1))
		m := r.mustSend(t, a, id, cid, "x")
		require.Greater(t, m.CreatedAt, p1)
		r.latePost(t, id, a, cid, p1)
		require.Equal(t, m, r.mustSend(t, a, id, cid, "x"))
		r.assertDelivered(t, id, pidA, m)
	})
	// 2. Повтор после позднего поста: пост на позиции с надгробием не
	// считается найденным (ADR-101 §5.6).
	t.Run("after-late-post", func(t *testing.T) {
		r, a, id := setup(t)
		cid := newRequestID()
		failNext(r, "nil")
		require.False(t, r.send(t, a, id, cid, "x").OK)
		p1 := r.msgRows(t, id, cid)[0].Pos
		r.reconcileKey(t, id, cid)
		r.latePost(t, id, a, cid, p1)
		m := r.mustSend(t, a, id, cid, "x")
		require.Greater(t, m.CreatedAt, p1)
		r.assertDelivered(t, id, pidA, m)
		require.Equal(t, "aborted", r.stateAt(t, id, p1))
	})
	// 3. Повтор после публикации сверкой: та же позиция и пост.
	t.Run("after-reconcile-published", func(t *testing.T) {
		r, a, id := setup(t)
		cid := newRequestID()
		failNext(r, "lost")
		require.False(t, r.send(t, a, id, cid, "x").OK)
		p1 := r.msgRows(t, id, cid)[0].Pos
		r.reconcileKey(t, id, cid)
		require.Equal(t, "published", r.stateAt(t, id, p1))
		m := r.mustSend(t, a, id, cid, "x")
		require.Equal(t, p1, m.CreatedAt)
		r.assertDelivered(t, id, pidA, m)
	})
	// 4. Повтор при открытом резерве — message_in_progress.
	t.Run("open-reservation", func(t *testing.T) {
		r, a, id := setup(t)
		cid := newRequestID()
		failNext(r, "nil")
		require.False(t, r.send(t, a, id, cid, "x").OK)
		require.Equal(t, errMessageInProgress, r.send(t, a, id, cid, "x").Error)
	})
	// 5. 32 одновременных повтора: один резерв, один пост; каждый ответ —
	// та же позиция либо message_in_progress.
	t.Run("32-concurrent", func(t *testing.T) {
		r, a, id := setup(t)
		cid := newRequestID()
		var wg sync.WaitGroup
		resps := make([]Response, 32)
		raw, _ := json.Marshal(sendReq(id, cid, "x"))
		for i := range resps {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				resps[i] = r.svc.dispatch(context.Background(), a, ActionConversationSend, raw)
			}(i)
		}
		wg.Wait()
		var first *ConvMessage
		for _, resp := range resps {
			if !resp.OK {
				require.Equal(t, errMessageInProgress, resp.Error)
				continue
			}
			m := resp.Data.(ConvSendResponse).Message
			if first == nil {
				first = &m
			}
			require.Equal(t, *first, m)
		}
		require.NotNil(t, first)
		require.Len(t, r.msgRows(t, id, cid), 1)
		require.Equal(t, 1, r.count(t, `SELECT count(*) FROM posts WHERE props->>'engram_client_msg_id' = $1`, cid))
		r.assertDelivered(t, id, pidA, *first)
	})
	// 6. Кэш PendingPostId (30 с): прежняя попытка создала пост, резерв
	// отменён; повтор в пределах кэша доходит без потери — пост на новой
	// позиции. Контроль: PendingPostId = ключ без сверки CreateAt —
	// «успех» с постом на надгробии.
	t.Run("pending-post-id-cache", func(t *testing.T) {
		r, a, id := setup(t)
		cid := newRequestID()
		failNext(r, "lost")
		require.False(t, r.send(t, a, id, cid, "x").OK)
		p1 := r.msgRows(t, id, cid)[0].Pos
		_, err := r.db.Exec(`UPDATE engram_position SET state = 'aborted' WHERE conversation_id = $1 AND pos = $2`, id, p1)
		require.NoError(t, err) // сверка отменила резерв до того, как пост стал виден
		m := r.mustSend(t, a, id, cid, "x")
		require.Greater(t, m.CreatedAt, p1)
		r.assertDelivered(t, id, pidA, m)
		r.mm.mu.Lock()
		for _, pp := range r.mm.pendingIDs {
			require.True(t, pp == "" || strings.Contains(pp, strconv.FormatInt(m.CreatedAt, 10)) || strings.Contains(pp, strconv.FormatInt(p1, 10)), "PendingPostId %q без позиции", pp)
		}
		r.mm.mu.Unlock()
	})
	// 7. Повтор, пока CreatePost в полёте: ждёт блокировку отправки и
	// получает message_in_progress; после — найденную позицию.
	t.Run("create-post-in-flight", func(t *testing.T) {
		r, a, id := setup(t)
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
		done := sendAsync(r, a, id, cid, "x")
		<-entered
		// Блокировка отправки ADR-092 §2.5.1 (contract §3.1 п. 4) держится,
		// пока CreatePost в полёте.
		probe, err := beginHeld(t, r.db)
		require.NoError(t, err)
		var got bool
		require.NoError(t, probe.QueryRow(`SELECT pg_try_advisory_xact_lock($1)`, convSendLockKey(pidA, cid)).Scan(&got))
		require.NoError(t, probe.Rollback())
		require.False(t, got, "блокировка отправки держится до конца вызова")
		quick := *r.svc
		quick.lockWait = 200 * time.Millisecond
		raw, _ := json.Marshal(sendReq(id, cid, "x"))
		require.Equal(t, errMessageInProgress, quick.dispatch(context.Background(), a, ActionConversationSend, raw).Error)
		close(gate)
		m := msgOf(t, recv(t, done))
		r.mm.mu.Lock()
		r.mm.postBeforeCreate = nil
		r.mm.mu.Unlock()
		require.Equal(t, m, r.mustSend(t, a, id, cid, "x"))
		r.assertDelivered(t, id, pidA, m)
	})
}

// Конкурентный прогон со сбоями всех видов и сверкой-заменителем: каждый
// ключ доставлен ровно одной опубликованной позицией, каждый «успех»
// подтверждён постом, надгробия не переходят (contract §6.2 P4).
func TestConvP4ConcurrentWithFailures(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	rnd := rand.New(rand.NewSource(7))
	var rmu sync.Mutex
	r.mm.mu.Lock()
	r.mm.postDecide = func(*model.Post) string {
		rmu.Lock()
		defer rmu.Unlock()
		return []string{"ok", "ok", "ok", "reject", "nil", "lost", "shift"}[rnd.Intn(7)]
	}
	r.mm.mu.Unlock()
	type sent struct {
		caller, pid, cid string
	}
	var keys []sent
	for i := 0; i < 60; i++ {
		if i%2 == 0 {
			keys = append(keys, sent{a, pidA, newRequestID()})
		} else {
			keys = append(keys, sent{b, pidB, newRequestID()})
		}
	}
	var wg sync.WaitGroup
	results := make([]ConvMessage, len(keys))
	slots := make(chan struct{}, convTestSenders)
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
					r.reconcileKey(t, id, k.cid)
				}
			}
		}(i, k)
	}
	wg.Wait()
	require.Positive(t, r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND item_kind = 'message' AND state = 'aborted'`, id),
		"прогон действительно прошёл через отмены")
	for i, k := range keys {
		require.NotZero(t, results[i].CreatedAt, "ключ %d не доставлен", i)
		r.assertDelivered(t, id, k.pid, results[i])
		published := 0
		for _, row := range r.msgRows(t, id, k.cid) {
			require.Equal(t, k.pid, row.Sender)
			if row.State == "published" {
				published++
				require.Equal(t, results[i].CreatedAt, row.Pos)
			}
		}
		require.Equal(t, 1, published)
	}
}

// ── P6: ADR-102, чужой client_msg_id (contract §6.2) ─────────────────────────

func TestConvP6ForeignClientMsgID(t *testing.T) {
	for _, state := range []string{"reserved", "published", "aborted"} {
		t.Run(state, func(t *testing.T) {
			r := newGroupRig(t, 2000)
			a := r.id.person(pidA)
			b := r.id.person(pidB)
			id := r.group(t, a, pidB)
			cid := newRequestID()
			r.mm.mu.Lock()
			switch state {
			case "reserved":
				r.mm.postNilOnCall = r.mm.postCalls + 1
			case "aborted":
				r.mm.postRejectOnCall = r.mm.postCalls + 1
			}
			r.mm.mu.Unlock()
			resp := r.send(t, a, id, cid, "от A")
			require.Equal(t, state == "published", resp.OK)
			before := r.msgRows(t, id, cid)
			require.Len(t, before, 1)
			require.Equal(t, state, before[0].State)
			calls, h := r.postCallCount(), r.posH(t, id)

			require.Equal(t, convErrIdempotency, r.send(t, b, id, cid, "от B").Error)
			require.Equal(t, convErrIdempotency, r.send(t, b, id, cid, "от A").Error, "тот же текст — всё равно чужой ключ")
			require.Equal(t, before, r.msgRows(t, id, cid), "резервы A не изменены")
			require.Equal(t, calls, r.postCallCount(), "CreatePost не вызван")
			require.Equal(t, h, r.posH(t, id), "позиция не выдана")

			// Повтор владельца работает по ADR-101 §5.6.
			switch state {
			case "reserved":
				require.Equal(t, errMessageInProgress, r.send(t, a, id, cid, "от A").Error)
			case "published":
				require.Equal(t, resp.Data.(ConvSendResponse).Message, r.mustSend(t, a, id, cid, "от A"))
			case "aborted":
				m := r.mustSend(t, a, id, cid, "от A")
				require.Greater(t, m.CreatedAt, before[0].Pos)
				r.assertDelivered(t, id, pidA, m)
			}
			require.Equal(t, convErrIdempotency, r.send(t, b, id, cid, "от B").Error)
		})
	}
}

// Одновременная пара A/B без резервов: один резерв, второй —
// idempotency_conflict (ADR-102 §2 п. 4).
func TestConvP6ConcurrentPair(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	for i := 0; i < 20; i++ {
		cid := newRequestID()
		ca := sendAsync(r, a, id, cid, "a")
		cb := sendAsync(r, b, id, cid, "b")
		ra, rb := recv(t, ca), recv(t, cb)
		require.True(t, ra.OK != rb.OK, "ровно один успех: %+v %+v", ra, rb)
		winner, loser := ra, rb
		pid := pidA
		if rb.OK {
			winner, loser, pid = rb, ra, pidB
		}
		require.Equal(t, convErrIdempotency, loser.Error)
		rows := r.msgRows(t, id, cid)
		require.Len(t, rows, 1)
		require.Equal(t, pid, rows[0].Sender)
		r.assertDelivered(t, id, pid, winner.Data.(ConvSendResponse).Message)
	}
}

// Чужой резерв появился и был отменён между проверкой §3.2 и резервом:
// проверка после блокировки строки H всё равно его видит, ключ не
// захватывается (ADR-102 §2, §3).
func TestConvP6ForeignTombstoneAppearsBeforeReserve(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	cid := newRequestID()
	hold, err := beginHeld(t, r.db)
	require.NoError(t, err)
	_, err = hold.Exec(`SELECT 1 FROM engram_conversation WHERE conversation_id = $1 FOR NO KEY UPDATE`, id)
	require.NoError(t, err)
	done := sendAsync(r, b, id, cid, "b")
	waitLockWait(t, r.db, "UPDATE engram_conversation SET position_h")
	// Резерв A и его отмена — как если бы они прошли, пока B ждал.
	_, err = r.db.Exec(`INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, sender, payload_hash, reserved_at)
VALUES ($1, 1, 'message', $2, 'aborted', $3, '\x00', 1)`, id, cid, pidA)
	require.NoError(t, err)
	require.NoError(t, hold.Rollback())
	require.Equal(t, convErrIdempotency, recv(t, done).Error)
	rows := r.msgRows(t, id, cid)
	require.Len(t, rows, 1)
	require.Equal(t, pidA, rows[0].Sender)
}

// interleaveQuerier — выполняет после первого запроса between: так
// конкурентная фиксация встаёт ровно между проверкой владельца ключа и
// поиском повтора в lookupSendKey — детерминированно, без расписания.
type interleaveQuerier struct {
	convQuerier
	calls   int
	between func()
}

func (q *interleaveQuerier) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	row := q.convQuerier.QueryRowContext(ctx, query, args...)
	q.calls++
	if q.calls == 1 {
		q.between()
	}
	return row
}

// Чужой резерв того же ключа зафиксирован между проверкой владельца и
// поиском повтора (lookupSendKey — два оператора): он не считается своим
// повтором (не message_in_progress), резерв отвергает ключ под строкой H, а
// следующий проход §3.2 даёт idempotency_conflict (ADR-102 §2). Это
// детерминированная форма гонки TestConvP6ConcurrentPair. Контроль — поиск
// повтора без sender = $3: message_in_progress.
func TestConvP6ForeignReservationBetweenOwnerCheckAndRetryLookup(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	ctx := context.Background()
	for _, state := range []string{"reserved", "published"} {
		t.Run(state, func(t *testing.T) {
			cid := newRequestID()
			hash := []byte("hash-b")
			q := &interleaveQuerier{convQuerier: r.db, between: func() {
				// Резерв A, как его делает reserveMessage: позиция из H.
				var pos int64
				require.NoError(t, r.db.QueryRow(`UPDATE engram_conversation SET position_h = position_h + 1
WHERE conversation_id = $1 RETURNING position_h`, id).Scan(&pos))
				postID := sql.NullString{String: "post-a", Valid: state == "published"}
				_, err := r.db.Exec(`INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, sender, payload_hash, reserved_at, post_id)
VALUES ($1, $2, 'message', $3, $4, $5, '\x00', 1, $6)`, id, pos, cid, state, pidA, postID)
				require.NoError(t, err)
			}}
			slot, code, done, err := lookupSendKey(ctx, q, id, pidB, cid, hash)
			require.NoError(t, err)
			require.Equal(t, 2, q.calls, "между операторами вставлен чужой резерв")
			require.Empty(t, code, "чужой резерв — не свой повтор")
			require.False(t, done)
			require.False(t, slot.found)

			// Шаги цикла admitAndReserve: резерв отвергает ключ, повтор §3.2.
			_, again, err := r.svc.reserveMessage(ctx, id, pidB, cid, hash)
			require.NoError(t, err)
			require.True(t, again, "резерв под строкой H видит чужой ключ")
			_, code, _, err = lookupSendKey(ctx, r.db, id, pidB, cid, hash)
			require.NoError(t, err)
			require.Equal(t, convErrIdempotency, code)

			rows := r.msgRows(t, id, cid)
			require.Len(t, rows, 1)
			require.Equal(t, pidA, rows[0].Sender)
			require.Equal(t, state, rows[0].State, "чужой резерв не изменён")
			require.Equal(t, convErrIdempotency, r.send(t, b, id, cid, "b").Error)
		})
	}
}

// ── P8: I1/I2 со смещёнными часами (contract §6.2) ───────────────────────────

// Сообщения внутри периодов отправителя при часах, расходящихся с
// позициями событий: позиция сообщения — из H, а не из часов Mattermost.
// Контроль — CreateAt = 0 (время Mattermost) или позиция по часам Bridge.
func TestConvP8MessagesInsidePeriodsWithSkewedClocks(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a)
	clock := func() int64 { return time.Now().UnixMilli() }
	r.mm.mu.Lock()
	r.mm.mmNow = clock
	r.mm.mu.Unlock()

	// I1: изменение прошло на узле, чьи часы спешат на 10 минут; новый
	// участник сразу пишет с узла с верными часами.
	r.svc.now = func() int64 { return clock() + 10*60*1000 }
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB}))
	var openAt int64
	require.NoError(t, r.db.QueryRow(`SELECT open_at FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2 AND state = 'active'`, id, pidB).Scan(&openAt))
	r.svc.now = clock
	m1 := r.mustSend(t, b, id, newRequestID(), "сразу после добавления")
	require.Greater(t, m1.CreatedAt, openAt, "I1: сообщение позже своего member_added")
	r.assertDelivered(t, id, pidB, m1)

	// I2: сообщение завершилось на спешащем узле до удаления, удаление — на
	// узле с верными часами: member_removed всё равно позже.
	r.svc.now = func() int64 { return clock() + 20*60*1000 }
	m2 := r.mustSend(t, b, id, newRequestID(), "до удаления")
	r.svc.now = clock
	r.mustDo(t, a, ActionConversationMemberRemove, reqOf(id, "photon_id", pidB))
	var closeAt int64
	require.NoError(t, r.db.QueryRow(`SELECT close_at FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2 AND state = 'removed'`, id, pidB).Scan(&closeAt))
	require.Less(t, m2.CreatedAt, closeAt, "I2: сообщение раньше member_removed")
	require.Greater(t, m2.CreatedAt, openAt)
	require.Equal(t, convErrNoAccess, r.send(t, b, id, newRequestID(), "после").Error)

	// Ни один пост сообщения не стоит вне позиции своего резерва.
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM posts p JOIN engram_conversation c ON c.carrier_channel_id = p.channelid
WHERE c.conversation_id = $1 AND p.type = '' AND NOT EXISTS (SELECT 1 FROM engram_position pp
  WHERE pp.conversation_id = c.conversation_id AND pp.pos = p.createat AND pp.item_key = p.props->>'engram_client_msg_id')`, id))
}

// ── X против S (contract §3.1 п. 3, §5.2; ADR-101 §5.3) ──────────────────────

// S берётся до проверки доступа: пока 930 проверяет участие, X не взять.
// Контроль — S после проверки или без S.
func TestConvXvsSSharedHeldDuringAdmission(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	id := r.group(t, a)
	hold, err := beginHeld(t, r.db)
	require.NoError(t, err)
	_, err = hold.Exec(`LOCK TABLE engram_member_period IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)
	done := sendAsync(r, a, id, newRequestID(), "x")
	waitLockWait(t, r.db, "FROM engram_member_period")
	require.False(t, tryX(t, r.db, id), "S держится уже на проверке доступа")
	require.NoError(t, hold.Rollback())
	require.True(t, recv(t, done).OK)
}

// Изменение под X начато раньше 930: 930 ждёт X и после фиксации удаления
// видит no_access — сообщение не встаёт после member_removed. Контроль —
// без S: 930 проходит мимо незафиксированного удаления и резервирует.
func TestConvXvsSSendWaitsForChangeUnderX(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	x, err := beginHeld(t, r.db)
	require.NoError(t, err)
	_, err = x.Exec(`SELECT pg_advisory_xact_lock($1)`, convLockKey(id))
	require.NoError(t, err)
	_, err = x.Exec(`UPDATE engram_member_period SET state = 'removed' WHERE conversation_id = $1 AND photon_id = $2 AND state = 'active'`, id, pidB)
	require.NoError(t, err)
	done := sendAsync(r, b, id, newRequestID(), "x")
	notYet(t, done, 300*time.Millisecond, "930 при чужой X")
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND item_kind = 'message'`, id))
	require.NoError(t, x.Commit())
	require.Equal(t, convErrNoAccess, recv(t, done).Error)
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND item_kind = 'message'`, id))
}

// 930 держит S внутри транзакции резерва: настоящий 911 не начинается до
// её фиксации, и member_removed встаёт после сообщения (I5).
func TestConvXvsSRemoveWaitsForSendReservation(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	hold, err := beginHeld(t, r.db)
	require.NoError(t, err)
	_, err = hold.Exec(`LOCK TABLE engram_position IN SHARE MODE`)
	require.NoError(t, err)
	done := sendAsync(r, b, id, newRequestID(), "x")
	waitLockWait(t, r.db, "INSERT INTO engram_position")
	removed := make(chan Response, 1)
	go func() {
		removed <- r.svc.dispatch(context.Background(), a, ActionConversationMemberRemove, pl(t, reqOf(id, "photon_id", pidB)))
	}()
	select {
	case resp := <-removed:
		t.Fatalf("911 прошёл, пока 930 держит S: %+v", resp)
	case <-time.After(300 * time.Millisecond):
	}
	require.False(t, tryX(t, r.db, id))
	require.NoError(t, hold.Rollback())
	m := msgOf(t, recv(t, done))
	require.True(t, recv(t, removed).OK)
	var closeAt int64
	require.NoError(t, r.db.QueryRow(`SELECT close_at FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2 AND state = 'removed'`, id, pidB).Scan(&closeAt))
	require.Less(t, m.CreatedAt, closeAt)
}

// Удаление участника параллельно с его 930: сообщение либо до
// member_removed, либо no_access (contract §5.2).
func TestConvXvsSConcurrentRemoveAndSend(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	for i := 0; i < 15; i++ {
		id := r.group(t, a, pidB)
		sendDone := sendAsync(r, b, id, newRequestID(), "x")
		rm := r.svc.dispatch(context.Background(), a, ActionConversationMemberRemove, pl(t, reqOf(id, "photon_id", pidB)))
		require.True(t, rm.OK, "%+v", rm)
		resp := recv(t, sendDone)
		var closeAt int64
		require.NoError(t, r.db.QueryRow(`SELECT close_at FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2 AND state = 'removed'`, id, pidB).Scan(&closeAt))
		if resp.OK {
			require.Less(t, resp.Data.(ConvSendResponse).Message.CreatedAt, closeAt)
		} else {
			require.Equal(t, convErrNoAccess, resp.Error)
			require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND item_kind = 'message'`, id))
		}
	}
}

// ── опережение позиции над часами (ADR-101 §5.4, §9) ─────────────────────────

type convLogCapture struct {
	mu   sync.Mutex
	recs []map[string]any
}

func (c *convLogCapture) log(msg string, kv ...any) {
	if msg != convPositionLeadLog {
		return
	}
	rec := map[string]any{}
	for i := 0; i+1 < len(kv); i += 2 {
		rec[kv[i].(string)] = kv[i+1]
	}
	c.mu.Lock()
	c.recs = append(c.recs, rec)
	c.mu.Unlock()
}

func (c *convLogCapture) take() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.recs
	c.recs = nil
	return out
}

// Позиция, опередившая часы Bridge (P = H + 1 > часы), журналируется одной
// записью на выдачу — у 930 и у изменений 903–921; при часах впереди H
// записей нет. Формула позиции не меняется. Контроль — убрать вызов
// observePositionLead из 930 или из транзакции изменения: записи нет.
func TestConvPositionLeadLogged(t *testing.T) {
	r := newGroupRig(t, 2000)
	logs := &convLogCapture{}
	r.svc.logger = logs.log
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a)
	clock := func() int64 { return time.Now().UnixMilli() }

	m := r.mustSend(t, a, id, newRequestID(), "часы впереди H")
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidB}))
	require.Empty(t, logs.take(), "часы не отстают от H — опережения нет")

	// Часы Bridge отстали на минуту (шаг назад, другой узел).
	const lag = 60 * 1000
	lagged := clock() - lag
	r.svc.now = func() int64 { return lagged }

	h := r.posH(t, id)
	m = r.mustSend(t, a, id, newRequestID(), "часы позади H")
	require.Equal(t, h+1, m.CreatedAt, "формула не изменилась: P = H + 1")
	recs := logs.take()
	require.Len(t, recs, 1)
	require.Equal(t, "930", recs[0]["op"])
	require.Equal(t, id, recs[0]["conversation_id"])
	require.Equal(t, m.CreatedAt, recs[0]["pos"])
	require.Equal(t, lagged, recs[0]["clock"])
	require.Equal(t, m.CreatedAt-lagged, recs[0]["lead_ms"])
	require.Greater(t, recs[0]["lead_ms"].(int64), int64(lag/2))

	h = r.posH(t, id)
	r.mustDo(t, a, ActionConversationMemberRemove, reqOf(id, "photon_id", pidB))
	recs = logs.take()
	require.Len(t, recs, 1, "одна запись на выдачу изменения")
	require.Equal(t, strconv.Itoa(ActionConversationMemberRemove), recs[0]["op"])
	require.Equal(t, h+1, recs[0]["pos"])
	require.Equal(t, h+1-lagged, recs[0]["lead_ms"])

	// Изменение без событий (920 с целью не в active — changed:false)
	// позиций не выдаёт — записи нет.
	r.mustDo(t, a, ActionConversationRoleSet, reqOf(id, "photon_id", pidB, "role", "admin"))
	require.Empty(t, logs.take())
}

// 900: финализация выдаёт позиции created/member_added по той же формуле;
// при H впереди часов — запись с op "900". У нового разговора H = 0, и
// вызовом 900 этого не достичь, поэтому финализация зовётся напрямую на
// строке creating с H впереди часов. Контроль — убрать вызов из
// finalizeCreate.
func TestConvPositionLeadLoggedOnCreate(t *testing.T) {
	r := newGroupRig(t, 2000)
	logs := &convLogCapture{}
	r.svc.logger = logs.log
	a := r.id.person(pidA)
	src := r.group(t, a)
	require.Empty(t, logs.take(), "900 при H = 0 опережения не даёт")

	now := time.Now().UnixMilli()
	r.svc.now = func() int64 { return now }
	h := now + 60*1000
	id := "lead-" + newRequestID()
	_, err := r.db.Exec(`INSERT INTO engram_conversation (conversation_id, kind, state, title, description,
history_policy, version, created_at, created_by, member_count, last_activity_at, position_h, carrier_state, carrier_team,
carrier_name, reserved_at)
SELECT $2, kind, 'creating', title, description, history_policy, 0, created_at, created_by, 0, 0, $3, 'reserved',
carrier_team, carrier_name || '-lead', reserved_at FROM engram_conversation WHERE conversation_id = $1`, src, id, h)
	require.NoError(t, err)
	require.NoError(t, r.svc.finalizeCreate(context.Background(), id, pidA, nil))
	recs := logs.take()
	require.Len(t, recs, 1)
	require.Equal(t, "900", recs[0]["op"])
	require.Equal(t, h+1, recs[0]["pos"])
	require.Equal(t, now, recs[0]["clock"])
	require.Equal(t, h+1-now, recs[0]["lead_ms"])
}

// createPostRejected — закрытый список ошибок 9.11.18 до записи поста.
// 4xx вне списка и тот же Id с другим Where — неопределённость. Без
// PostgreSQL.
func TestCreatePostRejectedClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  *model.AppError
		want bool
	}{
		{"nil", nil, false},
		{"is_valid.msg", &model.AppError{Id: mmPostTooLongErrorID, Where: "Post.IsValid", StatusCode: 400}, true},
		{"is_valid.other", &model.AppError{Id: "model.post.is_valid.props.app_error", Where: "Post.IsValid", StatusCode: 400}, true},
		{"hook-reject", &model.AppError{Id: "Post rejected by plugin. spam", Where: "createPost", StatusCode: 400}, true},
		{"hook-dismiss", &model.AppError{Id: plugin.DismissPostError, Where: "createPost", StatusCode: 400}, true},
		{"carrier-missing", &model.AppError{Id: mmChannelNotFoundErrorID, Where: "CreatePostMissingChannel", StatusCode: 404}, true},
		{"missing-account", &model.AppError{Id: mmMissingAccountErrorID, Where: "CreatePost", StatusCode: 404}, true},
		{"permalink-404-after-save", &model.AppError{Id: mmChannelNotFoundErrorID, Where: "GetChannel", StatusCode: 404}, false},
		{"unlisted-400", &model.AppError{Id: "api.post.create_post.root_id.app_error", Where: "createPost", StatusCode: 400}, false},
		{"unlisted-403", &model.AppError{Id: "api.context.permissions.app_error", StatusCode: 403}, false},
		{"listed-id-5xx", &model.AppError{Id: mmPostTooLongErrorID, Where: "Post.IsValid", StatusCode: 500}, false},
		{"store-500", &model.AppError{Id: "app.post.save.app_error", Where: "CreatePost", StatusCode: 500}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, createPostRejected(tc.err))
		})
	}
}
