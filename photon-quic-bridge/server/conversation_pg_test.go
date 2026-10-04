package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// 900/901/902 и распределитель служебных команд против НАСТОЯЩЕГО PostgreSQL
// (conversation-storage.md §6.0, §6.1, §6.5, §10: C1–C3, K1–K5). Включаются
// PHOTON_DM_PG_DSN, как dm_sql_pg_test.go. Mattermost — fakeConvMM (F5, F6).

const (
	pidA = "100000001"
	pidB = "100000002"
	pidC = "100000003"
	pidD = "100000004"
)

func (r *groupRig) create(t *testing.T, callerMM string, req map[string]any) Response {
	t.Helper()
	return r.svc.createConversation(context.Background(), callerMM, pl(t, req))
}

func (r *groupRig) mustCreate(t *testing.T, callerMM string, req map[string]any) ConvSnapshotResponse {
	t.Helper()
	resp := r.create(t, callerMM, req)
	require.True(t, resp.OK, "900: %+v", resp)
	var snap ConvSnapshotResponse
	require.NoError(t, json.Unmarshal(resp.Data.(json.RawMessage), &snap))
	return snap
}

func (r *groupRig) count(t *testing.T, q string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, r.db.QueryRow(q, args...).Scan(&n))
	return n
}

// ── 900 ──────────────────────────────────────────────────────────────────────

func TestConv900CreatesConversation(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	r.id.person(pidC)
	snap := r.mustCreate(t, a, createReq(newRequestID(), "Группа", pidB, pidC, pidB, pidA))

	c := snap.Conversation
	require.Equal(t, "group", c.Kind)
	require.Equal(t, "active", c.State)
	require.Equal(t, "since_join", c.HistoryPolicy)
	require.Equal(t, int64(1), c.ConversationVersion)
	require.Equal(t, 3, c.MemberCount, "повторы и сам создатель игнорируются")
	require.Equal(t, pidA, c.CreatedBy)
	require.Equal(t, "owner", c.Me.Role)
	require.Equal(t, convPermissions["owner"], c.Me.Permissions)
	require.Len(t, snap.Members, 3)

	// События: created первым, позиции подряд из H, H = последняя позиция.
	rows, err := r.db.Query(`SELECT seq, at, state, event->>'event' FROM engram_event
WHERE conversation_id = $1 ORDER BY version, seq`, c.ConversationID)
	require.NoError(t, err)
	var ats []int64
	var kinds []string
	for rows.Next() {
		var seq int
		var at int64
		var st, kind string
		require.NoError(t, rows.Scan(&seq, &at, &st, &kind))
		require.Equal(t, "written", st)
		ats = append(ats, at)
		kinds = append(kinds, kind)
	}
	rows.Close()
	require.Equal(t, []string{"created", "member_added", "member_added"}, kinds)
	require.Equal(t, ats[0]+1, ats[1])
	require.Equal(t, ats[0]+2, ats[2])
	var h int64
	require.NoError(t, r.db.QueryRow(`SELECT position_h FROM engram_conversation WHERE conversation_id = $1`, c.ConversationID).Scan(&h))
	require.Equal(t, ats[2], h)

	// Начальные участники: открывающий элемент — created (ADR-099 §9.4).
	for _, m := range snap.Members {
		require.Equal(t, ats[0], m.JoinedAt, "joined_at %s", m.PhotonID)
	}
	require.Equal(t, ats[0], c.Me.JoinedAt)

	// Резервы событий опубликованы; посты — с явным CreateAt = позиция.
	require.Equal(t, 3, r.count(t, `SELECT count(*) FROM engram_position WHERE conversation_id = $1 AND state = 'published' AND item_kind = 'event'`, c.ConversationID))
	require.Equal(t, 3, r.count(t, `SELECT count(*) FROM posts p JOIN engram_event e ON e.post_id = p.id
WHERE e.conversation_id = $1 AND p.createat = e.at AND p.type = 'custom_engram_event'
AND p.props->>'engram_event_key' = e.conversation_id || ':' || e.version || ':' || e.seq`, c.ConversationID))
	require.Equal(t, 3, r.count(t, `SELECT count(*) FROM engram_member_period WHERE conversation_id = $1 AND open_post_id IS NOT NULL`, c.ConversationID))

	// Носитель привязан; ключ done; служебная команда с ботом.
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_conversation WHERE conversation_id = $1 AND carrier_state = 'bound'`, c.ConversationID))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_request WHERE state = 'done'`))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-1' AND state = 'created'`))
	team := r.mm.teams["engram-carriers-1"]
	require.True(t, r.mm.teamMembers[team.Id][convTestBot], "бот — участник служебной команды")
	require.Equal(t, 1, r.mm.channelsNamed(carrierChannelPrefix+c.ConversationID))
}

func TestConv900Validation(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	base := func() map[string]any { return createReq(newRequestID(), "T", pidB) }
	cases := []struct {
		name string
		mod  func(m map[string]any)
		want string
	}{
		{"неизвестное поле", func(m map[string]any) { m["x"] = 1 }, convErrInvalidRequest},
		{"kind не group", func(m map[string]any) { m["kind"] = "direct" }, convErrInvalidRequest},
		{"пустой title", func(m map[string]any) { m["title"] = "  " }, convErrInvalidRequest},
		{"нет member_photon_ids", func(m map[string]any) { delete(m, "member_photon_ids") }, convErrInvalidRequest},
		{"неверная политика", func(m map[string]any) { m["history_policy"] = "all" }, convErrInvalidRequest},
		{"request_id не UUID", func(m map[string]any) { m["request_id"] = "abc" }, convErrInvalidRequest},
		{"неканонический PhotonID", func(m map[string]any) { m["member_photon_ids"] = []string{"000 000 001"} }, errInvalidPhotonID},
		{"незарегистрированный", func(m map[string]any) { m["member_photon_ids"] = []string{pidD} }, errPhotonIDNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := base()
			tc.mod(m)
			resp := r.create(t, a, m)
			require.False(t, resp.OK)
			require.Equal(t, tc.want, resp.Error)
		})
	}
	r.id.deactivate(b)
	resp := r.create(t, a, base())
	require.Equal(t, errPhotonIDNotFound, resp.Error, "деактивированный участник")
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_conversation`), "отказы не создают разговоров")

	resp = r.create(t, r.id.plainUser(), base())
	require.Equal(t, convErrForbidden, resp.Error, "без PhotonID создать разговор нельзя")
	resp = r.create(t, "missing-user", base())
	require.Equal(t, convErrTemporarilyUnavail, resp.Error, "сбой GetUser — недоступность, не отказ по праву")
}

// C1 — 32 параллельных 900 с одним ключом.
func TestConvC1ParallelSameKey(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	r.id.person(pidC)
	req := createReq(newRequestID(), "C1", pidB, pidC)
	var wg sync.WaitGroup
	ids := make([]string, 32)
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := r.create(t, a, req)
			if !resp.OK {
				ids[i] = "ERR:" + resp.Error
				return
			}
			var snap ConvSnapshotResponse
			_ = json.Unmarshal(resp.Data.(json.RawMessage), &snap)
			ids[i] = snap.Conversation.ConversationID
		}(i)
	}
	wg.Wait()
	// Исполнители, не дождавшиеся X в бюджет, отвечают temporarily_unavailable;
	// повтор доводит их до того же разговора.
	for i := range ids {
		if len(ids[i]) > 4 && ids[i][:4] == "ERR:" {
			require.Equal(t, "ERR:"+convErrTemporarilyUnavail, ids[i])
			ids[i] = r.mustCreate(t, a, req).Conversation.ConversationID
		}
	}
	for _, id := range ids {
		require.Equal(t, ids[0], id, "один разговор на ключ")
	}
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_conversation`))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_request`))
	require.Equal(t, 3, r.count(t, `SELECT count(*) FROM engram_member_period`))
	require.Equal(t, 3, r.count(t, `SELECT count(*) FROM engram_event`))
	require.Equal(t, 1, r.mm.channelsNamed(carrierChannelPrefix+ids[0]))
	require.Equal(t, 3, r.count(t, `SELECT count(*) FROM posts WHERE type = 'custom_engram_event'`), "посты-события без дублей")
}

// C1b — тот же ключ, другое содержание.
func TestConvC1bSameKeyOtherContent(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	rid := newRequestID()
	r.mustCreate(t, a, createReq(rid, "Один", pidB))
	resp := r.create(t, a, createReq(rid, "Другой", pidB))
	require.Equal(t, convErrIdempotency, resp.Error)
	// Тот же ключ с тем же содержанием (порядок и повторы списка не важны).
	again := r.mustCreate(t, a, createReq(rid, "Один", pidB, pidB))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_conversation`))
	require.Equal(t, "Один", again.Conversation.Title)
}

// C3a — сбой после CreateChannel до привязки, повтор 900.
func TestConvC3aRetryAfterChannelCreated(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	req := createReq(newRequestID(), "C3a", pidB)
	r.mm.chanCreateThenFail = 1
	resp := r.create(t, a, req)
	require.Equal(t, convErrTemporarilyUnavail, resp.Error)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_conversation WHERE state = 'creating'`))

	snap := r.mustCreate(t, a, req)
	require.Equal(t, "active", snap.Conversation.State)
	require.Equal(t, 1, r.mm.channelsNamed(carrierChannelPrefix+snap.Conversation.ConversationID), "один канал")
}

// C3b — команда заполнена до создания канала: резерв переезжает.
func TestConvC3bTeamFullBeforeChannel(t *testing.T) {
	r := newGroupRig(t, 10)
	a := r.id.person(pidA)
	r.id.person(pidB)
	r.mustCreate(t, a, createReq(newRequestID(), "первая", pidB)) // создаёт engram-carriers-1
	r.mm.fillTeam("engram-carriers-1")                            // каналы вне Bridge

	snap := r.mustCreate(t, a, createReq(newRequestID(), "вторая", pidB))
	var team string
	require.NoError(t, r.db.QueryRow(`SELECT carrier_team FROM engram_conversation WHERE conversation_id = $1`, snap.Conversation.ConversationID).Scan(&team))
	require.Equal(t, "engram-carriers-2", team, "резерв переехал")
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-1' AND state = 'full'`))
	t2 := r.mm.teams["engram-carriers-2"]
	_, inNew := r.mm.channels[t2.Id+"/"+carrierChannelPrefix+snap.Conversation.ConversationID]
	require.True(t, inNew)
	require.Equal(t, 1, r.mm.channelsNamed(carrierChannelPrefix+snap.Conversation.ConversationID), "канал только в новой команде")
}

// C3c — канал создан, затем команда заполнилась; повтор получает ошибку
// лимита, находит канал и привязывает его (F5: лимит раньше уникальности).
func TestConvC3cLimitAfterChannelCreated(t *testing.T) {
	r := newGroupRig(t, 10)
	a := r.id.person(pidA)
	r.id.person(pidB)
	req := createReq(newRequestID(), "C3c", pidB)
	r.mm.chanCreateThenFail = 1
	require.Equal(t, convErrTemporarilyUnavail, r.create(t, a, req).Error)
	r.mm.fillTeam("engram-carriers-1")

	snap := r.mustCreate(t, a, req)
	require.Equal(t, 1, r.mm.channelsNamed(carrierChannelPrefix+snap.Conversation.ConversationID), "один канал, сироты нет")
	var team string
	require.NoError(t, r.db.QueryRow(`SELECT carrier_team FROM engram_conversation WHERE conversation_id = $1`, snap.Conversation.ConversationID).Scan(&team))
	require.Equal(t, "engram-carriers-1", team, "одна ошибка лимита не переносит резерв, если канал найден")
}

// C3d / K1–K3 — привязка условна, повторная привязка другого канала —
// расхождение, а не перезапись.
func TestConvK1K3BindingIsConditional(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	snap := r.mustCreate(t, a, createReq(newRequestID(), "K", pidB))
	id := snap.Conversation.ConversationID
	var bound string
	require.NoError(t, r.db.QueryRow(`SELECT carrier_channel_id FROM engram_conversation WHERE conversation_id = $1`, id).Scan(&bound))

	got, err := r.svc.bindCarrier(context.Background(), id, bound)
	require.NoError(t, err)
	require.Equal(t, bound, got, "тот же канал — продолжить")
	_, err = r.svc.bindCarrier(context.Background(), id, "otherchannel")
	require.ErrorIs(t, err, errConvUnavailable, "другой канал — расхождение")

	other := r.mustCreate(t, a, createReq(newRequestID(), "K2", pidB))
	_, err = r.db.Exec(`UPDATE engram_conversation SET carrier_channel_id = $2 WHERE conversation_id = $1`, other.Conversation.ConversationID, bound)
	require.Error(t, err, "K2: один канал — носитель двух разговоров")
	_, err = r.db.Exec(`UPDATE engram_conversation SET carrier_channel_id = NULL WHERE conversation_id = $1`, id)
	require.Error(t, err, "K3: снять канал у bound нельзя")
}

// K5 — смена команды при параллельных исполнителях, шаги 2–5 под X: один канал.
func TestConvK5ParallelCompleteUnderX(t *testing.T) {
	r := newGroupRig(t, 10)
	a := r.id.person(pidA)
	r.id.person(pidB)
	r.mustCreate(t, a, createReq(newRequestID(), "первая", pidB))
	req := createReq(newRequestID(), "K5", pidB)
	p, ok := parseCreate(pl(t, req))
	require.True(t, ok)
	convID, _, code, err := r.svc.reserveConversation(context.Background(), pidA, p, canonicalCreateHash(p))
	require.NoError(t, err)
	require.Empty(t, code)
	r.mm.fillTeam("engram-carriers-1")

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = r.svc.completeCreate(context.Background(), a, pidA, convID, p, []string{pidB})
		}(i)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			require.ErrorIs(t, e, errConvLockTimeout)
		}
	}
	require.Equal(t, 1, r.mm.channelsNamed(carrierChannelPrefix+convID), "один канал")
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM engram_event WHERE conversation_id = $1`, convID))
}

// Сбой записи события после T2: повтор дописывает с теми же позициями, без
// дубля постов (E1–E4).
func TestConvEventWriteResumesWithSamePositions(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	r.id.person(pidC)
	req := createReq(newRequestID(), "E", pidB, pidC)
	r.mm.postFailOnCall = 2
	require.Equal(t, convErrTemporarilyUnavail, r.create(t, a, req).Error)
	before := map[int]int64{}
	rows, _ := r.db.Query(`SELECT seq, at FROM engram_event`)
	for rows.Next() {
		var s int
		var at int64
		_ = rows.Scan(&s, &at)
		before[s] = at
	}
	rows.Close()
	require.Len(t, before, 3)
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM engram_position WHERE state = 'reserved'`), "незаписанные события держат резервы")

	snap := r.mustCreate(t, a, req)
	rows, _ = r.db.Query(`SELECT seq, at FROM engram_event`)
	for rows.Next() {
		var s int
		var at int64
		_ = rows.Scan(&s, &at)
		require.Equal(t, before[s], at, "позиция не переназначается")
	}
	rows.Close()
	require.Equal(t, 3, r.count(t, `SELECT count(*) FROM posts WHERE channelid = (SELECT carrier_channel_id FROM engram_conversation WHERE conversation_id = $1)`, snap.Conversation.ConversationID), "дублей постов нет")
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_position WHERE state = 'reserved'`))
}

// E3 — пост события записан, ответ потерян: повтор находит пост точечным
// поиском (носитель, CreateAt, ключ) и не пишет второй.
func TestConvEventLostResponseFoundByE3(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	req := createReq(newRequestID(), "E3", pidB)
	r.mm.postCreateThenFailOnCall = 2
	require.Equal(t, convErrTemporarilyUnavail, r.create(t, a, req).Error)
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM posts WHERE type = 'custom_engram_event'`), "второй пост записан, ответ потерян")
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_event WHERE state = 'pending'`))

	r.mustCreate(t, a, req)
	require.Equal(t, 2, r.count(t, `SELECT count(*) FROM posts WHERE type = 'custom_engram_event'`), "дубля нет")
	require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_event e WHERE e.post_id IS DISTINCT FROM
(SELECT p.id FROM posts p WHERE p.createat = e.at AND p.props->>'engram_event_key' = e.conversation_id || ':' || e.version || ':' || e.seq)`), "событие связано с найденным постом")
}

// ── Распределитель §6.0 ──────────────────────────────────────────────────────

// Существовавшая команда с зарезервированным именем не принимается (P1).
func TestConvAllocatorSkipsPreexistingTeam(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	r.mm.mu.Lock()
	foreign := r.mm.addTeamLocked("engram-carriers-1")
	r.mm.mu.Unlock()

	r.mustCreate(t, a, createReq(newRequestID(), "g", pidB))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-1' AND state = 'full' AND team_id IS NULL AND create_attempted_at IS NULL`))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-2' AND state = 'created'`))
	require.Empty(t, r.mm.teamMembers[foreign.Id], "бот в чужую команду не добавлен")
}

// Конфликт имени на первом вызове CreateTeam — команда уже существует, не
// принимается.
func TestConvAllocatorConflictOnFirstCallNotAdopted(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	r.mm.teamAppearsForeign = 1
	r.mustCreate(t, a, createReq(newRequestID(), "g", pidB))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-1' AND state = 'full' AND team_id IS NULL`))
	require.Empty(t, r.mm.teamMembers[r.mm.teams["engram-carriers-1"].Id])
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-2' AND state = 'created'`))
}

// Неопределённый CreateTeam, создавший команду: повтор находит её и НЕ
// принимает (§6.0.5); пустая команда остаётся, берётся следующий N.
func TestConvAllocatorUncertainCreateLeavesEmptyTeam(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	req := createReq(newRequestID(), "g", pidB)
	r.mm.teamCreateThenFail = 1
	require.Equal(t, convErrTemporarilyUnavail, r.create(t, a, req).Error)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE state = 'reserved' AND create_attempted_at IS NOT NULL`))

	r.mustCreate(t, a, req)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-1' AND state = 'full' AND team_id IS NULL AND create_attempted_at IS NOT NULL`), "учёт брошенной попытки")
	require.Empty(t, r.mm.teamMembers[r.mm.teams["engram-carriers-1"].Id], "найденная команда не принята, бота в ней нет")
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-2' AND state = 'created'`))
}

// Неопределённый CreateTeam, не создавший команду: повтор создаёт то же имя.
func TestConvAllocatorUncertainNoCreateRetriesSameName(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	r.id.person(pidB)
	req := createReq(newRequestID(), "g", pidB)
	r.mm.teamFailNoCreate = 1
	require.Equal(t, convErrTemporarilyUnavail, r.create(t, a, req).Error)
	r.mustCreate(t, a, req)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team`))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-1' AND state = 'created'`))
}

// Сбой P3 после успешного CreateTeam: team_id уже в строке, повтор
// продолжает свою команду, а не ищет её по имени (§6.0.4).
func TestConvAllocatorBotJoinFailureContinuesOwnTeam(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	req := createReq(newRequestID(), "g")
	r.mm.memberFail = 1
	require.Equal(t, convErrTemporarilyUnavail, r.create(t, a, req).Error)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-1' AND state = 'reserved' AND team_id IS NOT NULL`))

	r.mustCreate(t, a, req)
	require.Equal(t, 1, r.mm.createTeamCalls, "вторая команда не создавалась")
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team`))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-1' AND state = 'created'`))
	require.True(t, r.mm.teamMembers[r.mm.teams["engram-carriers-1"].Id][convTestBot])
}

// Своя команда исчезла до P3: reserved → full с её team_id, следующий N.
func TestConvAllocatorOwnTeamDisappeared(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	req := createReq(newRequestID(), "g")
	r.mm.memberFail = 1
	require.Equal(t, convErrTemporarilyUnavail, r.create(t, a, req).Error)
	r.mm.dropTeam("engram-carriers-1")

	r.mustCreate(t, a, req)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-1' AND state = 'full' AND team_id IS NOT NULL`))
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-2' AND state = 'created'`))
}

func TestConvAllocatorCapacity(t *testing.T) {
	t.Run("MaxChannelsPerTeam ≤ 2 — без команд", func(t *testing.T) {
		r := newGroupRig(t, 2)
		a := r.id.person(pidA)
		require.Equal(t, convErrTemporarilyUnavail, r.create(t, a, createReq(newRequestID(), "g")).Error)
		require.Equal(t, 0, r.mm.createTeamCalls)
		require.Equal(t, 0, r.count(t, `SELECT count(*) FROM engram_service_team`))
	})
	t.Run("ёмкость = MaxChannelsPerTeam − 2", func(t *testing.T) {
		r := newGroupRig(t, 4) // capacity 2
		a := r.id.person(pidA)
		for i := 0; i < 3; i++ {
			r.mustCreate(t, a, createReq(newRequestID(), "g"))
		}
		require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-1' AND state = 'full' AND carriers = 2`))
		require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE team_name = 'engram-carriers-2' AND state = 'created' AND carriers = 1`))
	})
	t.Run("предел снят", func(t *testing.T) {
		r := newGroupRig(t, -1)
		a := r.id.person(pidA)
		for i := 0; i < 3; i++ {
			r.mustCreate(t, a, createReq(newRequestID(), "g"))
		}
		require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE state = 'created' AND carriers = 3`))
	})
}

// Параллельные исполнители создают одну команду (последовательный распределитель).
func TestConvAllocatorParallelCreatesOneTeam(t *testing.T) {
	r := newGroupRig(t, 2000)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = r.svc.ensureCarrierTeam(context.Background()) }()
	}
	wg.Wait()
	require.Equal(t, 1, r.mm.createTeamCalls)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_service_team WHERE state = 'created'`))
}

// Нет бота — 900 недоступен (§6.0.7), даже когда команда с местом есть;
// повтор выполненного 900, 901 и 902 работают.
func TestConvNoBotOnly900Unavailable(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	req := createReq(newRequestID(), "g")
	snap := r.mustCreate(t, a, req)
	r.svc.botID = ""
	require.Equal(t, convErrTemporarilyUnavail, r.create(t, a, createReq(newRequestID(), "g2")).Error)
	require.True(t, r.create(t, a, req).OK, "повтор выполненного запроса")
	require.True(t, r.svc.getConversation(context.Background(), a, pl(t, map[string]string{"conversation_id": snap.Conversation.ConversationID})).OK)
	require.True(t, r.svc.listConversations(context.Background(), a, pl(t, map[string]any{})).OK)
	require.Equal(t, 1, r.count(t, `SELECT count(*) FROM engram_conversation`))
}

// ── 901 ──────────────────────────────────────────────────────────────────────

func TestConv901Access(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	snap := r.mustCreate(t, a, createReq(newRequestID(), "g", pidB))
	id := snap.Conversation.ConversationID
	get := func(caller, convID string) Response {
		return r.svc.getConversation(context.Background(), caller, pl(t, map[string]string{"conversation_id": convID}))
	}

	resp := get(b, id)
	require.True(t, resp.OK)
	got := resp.Data.(ConvSnapshotResponse)
	require.Equal(t, "member", got.Conversation.Me.Role)
	require.Equal(t, convPermissions["member"], got.Conversation.Me.Permissions)
	require.Len(t, got.Members, 2)

	require.Equal(t, convErrConversationNF, get(c, id).Error, "незнакомому — not_found")
	require.Equal(t, convErrConversationNF, get(a, "nonexistent").Error)
	require.Equal(t, convErrConversationNF, get(r.id.plainUser(), id).Error, "без PhotonID")
	require.Equal(t, convErrInvalidRequest, r.svc.getConversation(context.Background(), a, pl(t, map[string]any{"conversation_id": id, "x": 1})).Error)

	_, err := r.db.Exec(`UPDATE engram_member_period SET state = 'left' WHERE conversation_id = $1 AND photon_id = $2`, id, pidB)
	require.NoError(t, err)
	require.Equal(t, convErrNoAccess, get(b, id).Error, "бывшему — no_access")

	_, err = r.db.Exec(`UPDATE engram_conversation SET state = 'archived' WHERE conversation_id = $1`, id)
	require.NoError(t, err)
	require.True(t, get(a, id).OK, "архивный — участнику, бывшему в active на момент архивации")

	_, err = r.db.Exec(`UPDATE engram_conversation SET state = 'creating' WHERE conversation_id = $1`, id)
	require.NoError(t, err)
	require.Equal(t, convErrConversationNF, get(a, id).Error, "creating не виден никому (S5)")
}

// ── 902 ──────────────────────────────────────────────────────────────────────

func TestConv902ListOrderAndPaging(t *testing.T) {
	r := newGroupRig(t, 2000)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, r.mustCreate(t, a, createReq(newRequestID(), "g", pidB)).Conversation.ConversationID)
	}
	// Свежая активность — первой: поднимаем ids[1].
	_, err := r.db.Exec(`UPDATE engram_conversation SET last_activity_at = last_activity_at + 1000000 WHERE conversation_id = $1`, ids[1])
	require.NoError(t, err)
	list := func(caller string, payload map[string]any) Response {
		return r.svc.listConversations(context.Background(), caller, pl(t, payload))
	}
	resp := list(b, map[string]any{})
	require.True(t, resp.OK)
	all := resp.Data.(ConvListResponse)
	require.Len(t, all.Conversations, 5)
	require.Equal(t, ids[1], all.Conversations[0].ConversationID)
	require.Equal(t, "member", all.Conversations[0].MyRole)
	require.Empty(t, all.NextCursor)

	var seen []string
	cursor := ""
	for i := 0; i < 10; i++ {
		p := map[string]any{"limit": 2}
		if cursor != "" {
			p["cursor"] = cursor
		}
		page := list(b, p).Data.(ConvListResponse)
		for _, it := range page.Conversations {
			seen = append(seen, it.ConversationID)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	var want []string
	for _, it := range all.Conversations {
		want = append(want, it.ConversationID)
	}
	require.Equal(t, want, seen, "постранично — тот же порядок без пропусков и повторов")

	require.Equal(t, convErrInvalidLimit, list(b, map[string]any{"limit": 0}).Error)
	require.Equal(t, convErrInvalidLimit, list(b, map[string]any{"limit": 201}).Error)
	require.Equal(t, convErrInvalidCursor, list(b, map[string]any{"cursor": "!!"}).Error)
	require.Equal(t, convErrInvalidRequest, list(b, map[string]any{"x": 1}).Error)
	require.Empty(t, list(r.id.plainUser(), map[string]any{}).Data.(ConvListResponse).Conversations)

	_, err = r.db.Exec(`UPDATE engram_member_period SET state = 'removed' WHERE conversation_id = $1 AND photon_id = $2`, ids[0], pidB)
	require.NoError(t, err)
	_, err = r.db.Exec(`UPDATE engram_conversation SET state = 'archived' WHERE conversation_id = $1`, ids[2])
	require.NoError(t, err)
	_, err = r.db.Exec(`UPDATE engram_conversation SET state = 'creating' WHERE conversation_id = $1`, ids[3])
	require.NoError(t, err)
	got := list(b, map[string]any{}).Data.(ConvListResponse)
	var gotIDs []string
	for _, it := range got.Conversations {
		gotIDs = append(gotIDs, it.ConversationID)
	}
	require.NotContains(t, gotIDs, ids[0], "не в периоде active")
	require.Contains(t, gotIDs, ids[2], "архивные включаются")
	require.NotContains(t, gotIDs, ids[3], "creating не виден")
	_ = a
}
