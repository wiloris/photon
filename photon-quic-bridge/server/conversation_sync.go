package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

// 931 SYNC — ADR-099 §9.2–§9.4 с поправкой ADR-103 §3; ADR-101 §5.1b, §5.5,
// §5.11; implementation contract §4; conversation-storage.md §6.4, §7.2.
//
// Под S: существование и участие, периоды вызывающего. Затем ОДИН оператор
// SQL через master: барьер (наименьшая открытая позиция разговора, иначе
// H + 1) и страница видимой ленты ниже барьера — в одном снимке. Барьер и
// страница разными операторами, соединениями или с реплики запрещены
// (ADR-101 §5.1b, контроль стенда AP-1 «страница, затем барьер»). W = 0:
// окна нет. 931 события не дописывает и pending не проверяет.

type convSyncPayload struct {
	ConversationID string  `json:"conversation_id"`
	Cursor         *string `json:"cursor"`
	Limit          *int    `json:"limit"`
}

// ConvMessageItem и ConvEventItem — элемент ленты (ADR-099 §9.3). Ни одного
// идентификатора, типа или имени Mattermost: id — непрозрачный id поста,
// как у 930.
type ConvMessageItem struct {
	Kind           string `json:"kind"`
	ID             string `json:"id"`
	CreatedAt      int64  `json:"created_at"`
	AuthorPhotonID string `json:"author_photon_id"`
	Text           string `json:"text"`
	ClientMsgID    string `json:"client_msg_id,omitempty"` // только у своих
}

type ConvEventItem struct {
	Kind                string `json:"kind"`
	ID                  string `json:"id"`
	CreatedAt           int64  `json:"created_at"`
	Event               string `json:"event"`
	ActorPhotonID       string `json:"actor_photon_id"`
	TargetPhotonID      string `json:"target_photon_id,omitempty"`
	Role                string `json:"role,omitempty"`
	Title               string `json:"title,omitempty"`
	Description         string `json:"description,omitempty"`
	InviteID            string `json:"invite_id,omitempty"`
	Visibility          string `json:"visibility,omitempty"`
	ConversationVersion int64  `json:"conversation_version"`
}

// ConvSyncResponse — ответ 931 (ADR-099 §9.2).
type ConvSyncResponse struct {
	Items      []any           `json:"items"`
	NextCursor string          `json:"next_cursor"`
	From       *dmWirePosition `json:"from,omitempty"`
	AtEnd      bool            `json:"at_end"`
}

// convBinding — привязка курсора к разговору. Формат курсора — dmCursor
// (прямые разбор и кодирование ADR-092 §2.2.5), привязка другая: курсор
// 218 к 931 и курсор одного разговора к другому дают invalid_cursor.
func convBinding(convID string) string {
	h := sha256.Sum256([]byte("photon:conv:" + convID))
	return hex.EncodeToString(h[:8])
}

// convPeriodBounds — границы периода вызывающего по позициям элементов
// ленты: открывающий и закрывающий элементы входят в период (ADR-099
// §9.4). Позиции уникальны (S10), поэтому «по (created_at, id)» сводится к
// created_at.
type convPeriodBounds struct {
	open  int64
	close int64 // math.MaxInt64 — период не закрыт
}

type convFeedMode int

const (
	feedNewest convFeedMode = iota // первичная синхронизация: последние limit, по убыванию
	feedAscend                     // проход: createat >= нижняя граница, по возрастанию
	feedBefore                     // история назад (940): createat < якорь, по убыванию; текст как у feedNewest
)

// convFeedSQL — оператор §7.2 (ADR-101): барьер — подзапрос того же
// оператора (наименьшая открытая позиция записана как ORDER BY pos LIMIT 1 —
// то же значение, что min(pos), но планировщик берёт engram_position_open);
// верхняя граница диапазона — min(барьер, закрытие); посты на
// позициях с надгробием отфильтрованы; на позицию — один пост (наименьший
// id). Параметры: $1 разговор, $2 носитель, $3 нижняя граница createat
// (включительно), $4 limit, $5 верхняя граница (исключительно), далее пары
// (открытие, закрытие) периодов — только при нескольких периодах since_join:
// диапазон по индексу ограничен крайними границами, пропуски между
// периодами отсекаются фильтром.
// Первая колонка — барьер, он возвращается и при пустой странице.
func convFeedSQL(mode convFeedMode, periods int) string {
	order := "ASC"
	if mode == feedNewest || mode == feedBefore {
		order = "DESC"
	}
	var filter string
	if periods > 1 {
		parts := make([]string, periods)
		for i := range parts {
			parts[i] = fmt.Sprintf("(p.createat >= $%d AND p.createat <= $%d)", 6+2*i, 7+2*i)
		}
		filter = " AND (" + strings.Join(parts, " OR ") + ")"
	}
	return `WITH b AS (
  SELECT COALESCE(
    (SELECT pos FROM engram_position WHERE conversation_id = $1 AND state = 'reserved' ORDER BY pos LIMIT 1),
    (SELECT position_h + 1 FROM engram_conversation WHERE conversation_id = $1)) AS bar)
SELECT b.bar, x.id, x.createat, x.type, x.message, x.event, r.sender, r.item_key
FROM b
LEFT JOIN LATERAL (
  SELECT DISTINCT ON (p.createat) p.id, p.createat, p.type, p.message, (p.props->'engram_event')::text AS event
  FROM posts p
  WHERE p.channelid = $2 AND p.deleteat = 0 AND p.type IN ('', 'custom_engram_event')
    AND p.createat >= $3 AND p.createat < LEAST(b.bar, $5)` + filter + `
    AND NOT EXISTS (SELECT 1 FROM engram_position t
                    WHERE t.conversation_id = $1 AND t.pos = p.createat AND t.state = 'aborted')
  ORDER BY p.createat ` + order + `, p.id ASC
  LIMIT $4) x ON true
LEFT JOIN engram_position r ON r.conversation_id = $1 AND r.pos = x.createat
ORDER BY x.createat ` + order
}

// convFeedRow — строка страницы до сборки элемента.
type convFeedRow struct {
	id, typ, message, event, sender, itemKey string
	createAt                                 int64
}

// readConvFeed выполняет оператор §7.2 и возвращает барьер и страницу в
// порядке запроса.
func readConvFeed(ctx context.Context, q convQuerier, convID, channelID string, mode convFeedMode,
	lower int64, limit int, periods []convPeriodBounds) (int64, []convFeedRow, error) {
	return runConvFeed(ctx, q, convFeedSQL(mode, len(periods)), convID, channelID, lower, limit, periods)
}

// runConvFeed — выполнение оператора convFeedSQL; текст оператора параметр,
// чтобы тесты могли выполнить сломанный вариант (отрицательные контроли).
func runConvFeed(ctx context.Context, q convQuerier, sqlText, convID, channelID string,
	lower int64, limit int, periods []convPeriodBounds) (int64, []convFeedRow, error) {
	return runConvFeedBelow(ctx, q, sqlText, convID, channelID, lower, math.MaxInt64, limit, periods)
}

// runConvFeedBelow — runConvFeed с дополнительной верхней границей below
// (исключительно): верхняя граница оператора — min(below, закрытие периода + 1).
func runConvFeedBelow(ctx context.Context, q convQuerier, sqlText, convID, channelID string,
	lower, below int64, limit int, periods []convPeriodBounds) (int64, []convFeedRow, error) {
	upper := below
	if n := len(periods); n > 0 && periods[n-1].close != math.MaxInt64 && periods[n-1].close+1 < upper {
		upper = periods[n-1].close + 1
	}
	args := []any{convID, channelID, lower, limit, upper}
	if len(periods) > 1 {
		for _, p := range periods {
			args = append(args, p.open, p.close)
		}
	}
	rows, err := q.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	var bar int64
	var out []convFeedRow
	for rows.Next() {
		var b int64
		var id, typ, msg, ev, sender, key sql.NullString
		var at sql.NullInt64
		if err := rows.Scan(&b, &id, &at, &typ, &msg, &ev, &sender, &key); err != nil {
			return 0, nil, err
		}
		bar = b
		if !id.Valid {
			continue // пустая страница: LEFT JOIN вернул только барьер
		}
		out = append(out, convFeedRow{id: id.String, createAt: at.Int64, typ: typ.String, message: msg.String,
			event: ev.String, sender: sender.String, itemKey: key.String})
	}
	return bar, out, rows.Err()
}

// callerPeriods — периоды вызывающего по порядку (ADR-099 §9.4).
func callerPeriods(ctx context.Context, q convQuerier, convID, pid string) ([]convPeriodBounds, error) {
	rows, err := q.QueryContext(ctx, `SELECT open_at, close_at FROM engram_member_period
WHERE conversation_id = $1 AND photon_id = $2 ORDER BY period_no`, convID, pid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []convPeriodBounds
	for rows.Next() {
		var o, c sql.NullInt64
		if err := rows.Scan(&o, &c); err != nil {
			return nil, err
		}
		if !o.Valid {
			return nil, errors.New("conversation: у периода нет позиции открывающего элемента")
		}
		p := convPeriodBounds{open: o.Int64, close: math.MaxInt64}
		if c.Valid {
			p.close = c.Int64
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// syncConversation — 931.
func (s *convService) syncConversation(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convSyncPayload
	if !decodeStrict(raw, &p) || p.ConversationID == "" {
		return convFail(convErrInvalidRequest)
	}
	limit := convListDefaultLimit
	if p.Limit != nil {
		limit = *p.Limit
		if limit < 1 || limit > convListMaxLimit {
			return convFail(convErrInvalidLimit)
		}
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("931 caller", err)
	}
	if !found {
		return convFail(convErrConversationNF)
	}

	// S: доступ и периоды не меняются, пока читается лента (ADR-101 §5.3).
	releaseS, err := s.holdSharedLock(ctx, convLockKey(p.ConversationID))
	if err != nil {
		return s.convFailFor("931 lock", err)
	}
	defer releaseS()

	code, err := callerAccess(ctx, s.db, p.ConversationID, pid)
	if err != nil {
		return s.convFailFor("931 access", err)
	}
	if code != "" {
		return convFail(code)
	}
	// Содержание запроса — после доступа (ADR-099 §11).
	binding := convBinding(p.ConversationID)
	var cur *dmCursor
	if p.Cursor != nil && *p.Cursor != "" {
		c, ok := decodeCursor(*p.Cursor, binding)
		if !ok {
			return convFail(convErrInvalidCursor)
		}
		cur = c
	}

	var policy string
	var channelID sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT history_policy, carrier_channel_id FROM engram_conversation
WHERE conversation_id = $1`, p.ConversationID).Scan(&policy, &channelID); err != nil {
		return s.convFailFor("931 conversation", err)
	}
	if !channelID.Valid {
		return s.convFailFor("931 carrier", errors.New("conversation: у активного разговора нет носителя"))
	}
	// Видимая лента (ADR-099 §9.4): full — вся; since_join — периоды.
	var periods []convPeriodBounds
	visibleFrom := int64(0)
	if policy == "since_join" {
		periods, err = callerPeriods(ctx, s.db, p.ConversationID, pid)
		if err != nil || len(periods) == 0 {
			if err == nil {
				err = errors.New("conversation: нет периодов вызывающего")
			}
			return s.convFailFor("931 periods", err)
		}
		visibleFrom = periods[0].open
	}
	maxInt := func(a, b int64) int64 {
		if a > b {
			return a
		}
		return b
	}

	resp := ConvSyncResponse{Items: []any{}}
	next := dmCursor{V: dmCursorVersion, B: binding}
	var (
		bar  int64
		rows []convFeedRow
	)
	switch {
	case cur == nil:
		// Первичная синхронизация (ADR-092 §2.2.1): последние limit видимых
		// элементов ниже барьера, блок newest; водяной знак — самый новый.
		bar, rows, err = readConvFeed(ctx, s.db, p.ConversationID, channelID.String, feedNewest, visibleFrom, limit, periods)
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
		resp.AtEnd = true
		if err == nil && len(rows) > 0 {
			next.W = &dmWire{T: rows[len(rows)-1].createAt, ID: rows[len(rows)-1].id}
		}
	case cur.P == nil:
		// Начало прохода (§2.2.2), W = 0: L = водяной знак.
		lower := int64(0)
		if cur.W != nil {
			lower = cur.W.T
		}
		bar, rows, err = readConvFeed(ctx, s.db, p.ConversationID, channelID.String, feedAscend, maxInt(lower, visibleFrom), limit, periods)
		resp.From = &dmWirePosition{CreatedAt: lower, ID: ""}
		next.W = cur.W
	default:
		// Продолжение (§2.2.3): строго новее P. Позиции уникальны и посты на
		// позицию схлопнуты, поэтому «новее» — createat > P.
		bar, rows, err = readConvFeed(ctx, s.db, p.ConversationID, channelID.String, feedAscend, maxInt(cur.P.T+1, visibleFrom), limit, periods)
		resp.From = &dmWirePosition{CreatedAt: cur.P.T, ID: cur.P.ID}
		next.W = cur.W
	}
	if err != nil {
		return s.convFailFor("931 feed", err)
	}
	_ = bar // барьер уже учтён в самом операторе: выше него строк нет

	for _, r := range rows {
		it, ok := convItemOf(r, pid)
		if !ok {
			s.logger("[Photon][Conv] 931 skipped post without reservation or event body", "post_pos", r.createAt)
			continue
		}
		resp.Items = append(resp.Items, it)
	}

	if cur != nil {
		// Конец прохода и next_cursor (§2.2.4).
		resp.AtEnd = len(rows) < limit
		if len(rows) > 0 {
			last := dmPos{CreateAt: rows[len(rows)-1].createAt, ID: rows[len(rows)-1].id}
			if next.W == nil || last.newerThan(next.W.pos()) {
				next.W = wireOf(last)
			}
			if !resp.AtEnd {
				next.P = wireOf(last)
			}
		}
	}
	resp.NextCursor = encodeCursor(next)
	return Response{OK: true, Data: resp}
}

// convItemOf собирает элемент ленты (ADR-099 §9.3). false — пост без
// резерва автора или без тела события: такой элемент не выдаётся.
func convItemOf(r convFeedRow, callerPID string) (any, bool) {
	switch r.typ {
	case "":
		if r.sender == "" {
			return nil, false
		}
		it := ConvMessageItem{Kind: "message", ID: r.id, CreatedAt: r.createAt, AuthorPhotonID: r.sender, Text: r.message}
		if r.sender == callerPID {
			it.ClientMsgID = r.itemKey
		}
		return it, true
	case convEventPostType:
		var ev convEventBody
		if r.event == "" || json.Unmarshal([]byte(r.event), &ev) != nil || ev.Event == "" {
			return nil, false
		}
		return ConvEventItem{Kind: "event", ID: r.id, CreatedAt: r.createAt, Event: ev.Event, ActorPhotonID: ev.ActorPhotonID,
			TargetPhotonID: ev.TargetPhotonID, Role: ev.Role, Title: ev.Title, Description: ev.Description, InviteID: ev.InviteID, Visibility: ev.Visibility,
			ConversationVersion: ev.Version}, true
	}
	return nil, false
}
