package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

// 908 CONVERSATION_MARK_READ — позиция прочтения участника (ADR-108 §2,
// contract messaging-v2 §5b.1). Личное состояние, как 907: разговор не
// меняется — без X-блокировки, события, версии и ключа идемпотентности.
// Доступ — как у 931 (S-блокировка, callerAccess: участник в периоде active,
// в том числе в архивном разговоре, ADR-099 §8.1).

const (
	// convUnreadCap — предел счётчика (ADR-108 Q3): клиент показывает «999+».
	convUnreadCap = 999
	// convUnreadProbe — предел строк запроса: на одну больше предела ответа.
	convUnreadProbe = convUnreadCap + 1
)

type convMarkReadPayload struct {
	ConversationID string          `json:"conversation_id"`
	UpTo           *dmWirePosition `json:"up_to"`
}

// ConvMarkReadResponse — ответ 908. ReadUpTo == nil — у вызывающего нет
// видимых элементов (отметке не на что опираться).
type ConvMarkReadResponse struct {
	ReadUpTo    *dmWirePosition `json:"read_up_to"`
	UnreadCount int             `json:"unread_count"`
}

// posAfter — (aAt, aID) строго новее (bAt, bID); id сравниваются побайтово,
// как COLLATE "C" в запросах.
func posAfter(aAt int64, aID string, bAt int64, bID string) bool {
	return aAt > bAt || (aAt == bAt && aID > bID)
}

// convVisibility — видимая вызывающему часть ленты (ADR-099 §9.4): нижняя
// граница и периоды при since_join. Те же правила, что у 931 и 902.
func convVisibility(ctx context.Context, q convQuerier, convID, policy, pid string) (periods []convPeriodBounds, visibleFrom int64, ok bool, err error) {
	if policy != "since_join" {
		return nil, 0, true, nil
	}
	periods, err = callerPeriods(ctx, q, convID, pid)
	if err != nil || len(periods) == 0 {
		return nil, 0, false, err
	}
	return periods, periods[0].open, true, nil
}

// convUnreadSQL — счёт непрочитанного (ADR-108 §2.3) тем же оператором
// выбора ленты, что у 931 (convFeedSQL): барьер, надгробия, один пост на
// позицию, периоды since_join. Параметры: $1 разговор, $2 носитель, $3
// нижняя граница createat (включительно), $4 вызывающий, $5 верхняя граница
// (исключительно), $6 и $7 — позиция прочтения, далее пары (открытие,
// закрытие) периодов при нескольких периодах. LIMIT — предел стоимости.
func convUnreadSQL(periods int) string {
	var filter string
	if periods > 1 {
		parts := make([]string, periods)
		for i := range parts {
			parts[i] = fmt.Sprintf("(p.createat >= $%d AND p.createat <= $%d)", 8+2*i, 9+2*i)
		}
		filter = " AND (" + strings.Join(parts, " OR ") + ")"
	}
	return `WITH b AS (
  SELECT COALESCE(
    (SELECT pos FROM engram_position WHERE conversation_id = $1 AND state = 'reserved' ORDER BY pos LIMIT 1),
    (SELECT position_h + 1 FROM engram_conversation WHERE conversation_id = $1)) AS bar)
SELECT count(*) FROM (
  SELECT 1
  FROM (
    SELECT DISTINCT ON (p.createat) p.id, p.createat, p.type
    FROM posts p, b
    WHERE p.channelid = $2 AND p.deleteat = 0 AND p.type IN ('', 'custom_engram_event')
      AND p.createat >= $3 AND p.createat < LEAST(b.bar, $5)` + filter + `
      AND NOT EXISTS (SELECT 1 FROM engram_position t
                      WHERE t.conversation_id = $1 AND t.pos = p.createat AND t.state = 'aborted')
    ORDER BY p.createat, p.id) x
  JOIN engram_position r ON r.conversation_id = $1 AND r.pos = x.createat
  WHERE x.type = '' AND r.item_kind = 'message' AND r.state = 'published'
    AND r.sender IS NOT NULL AND r.sender <> '' AND r.sender <> $4
    AND (x.createat > $6 OR (x.createat = $6 AND x.id COLLATE "C" > $7))
  LIMIT ` + fmt.Sprint(convUnreadProbe) + `) y`
}

// unreadCount — число непрочитанных вызывающим сообщений, не больше
// convUnreadCap. read — позиция прочтения; nil — отметки нет, считается всё
// видимое. Один оператор SQL, без прохода по ленте в Go.
func (s *convService) unreadCount(ctx context.Context, q convQuerier, convID, channelID, policy, pid string, read *dmWirePosition) (int, error) {
	if channelID == "" {
		return 0, nil
	}
	periods, visibleFrom, ok, err := convVisibility(ctx, q, convID, policy, pid)
	if err != nil || !ok {
		return 0, err
	}
	var rc int64
	var rid string
	if read != nil {
		rc, rid = read.CreatedAt, read.ID
	}
	lower := visibleFrom
	if rc > lower {
		lower = rc
	}
	upper := int64(math.MaxInt64)
	if n := len(periods); n > 0 && periods[n-1].close != math.MaxInt64 && periods[n-1].close+1 < upper {
		upper = periods[n-1].close + 1
	}
	args := []any{convID, channelID, lower, pid, upper, rc, rid}
	if len(periods) > 1 {
		for _, p := range periods {
			args = append(args, p.open, p.close)
		}
	}
	var n int
	if err := q.QueryRowContext(ctx, convUnreadSQL(len(periods)), args...).Scan(&n); err != nil {
		return 0, err
	}
	if n > convUnreadCap {
		n = convUnreadCap
	}
	return n, nil
}

// markRead — 908.
func (s *convService) markRead(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convMarkReadPayload
	if !decodeStrict(raw, &p) || p.ConversationID == "" {
		return convFail(convErrInvalidRequest)
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("908 caller", err)
	}
	if !found {
		return convFail(convErrConversationNF)
	}

	// S: доступ и периоды не меняются, пока вычисляется отметка (ADR-101 §5.3).
	releaseS, err := s.holdSharedLock(ctx, convLockKey(p.ConversationID))
	if err != nil {
		return s.convFailFor("908 lock", err)
	}
	defer releaseS()

	code, err := callerAccess(ctx, s.db, p.ConversationID, pid)
	if err != nil {
		return s.convFailFor("908 access", err)
	}
	if code != "" {
		return convFail(code)
	}
	// Содержание запроса — после доступа (ADR-099 §11).
	if p.UpTo == nil || p.UpTo.CreatedAt <= 0 || !postIDRe.MatchString(p.UpTo.ID) {
		return convFail(convErrInvalidRequest)
	}

	var policy string
	var channelID sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT history_policy, carrier_channel_id FROM engram_conversation
WHERE conversation_id = $1`, p.ConversationID).Scan(&policy, &channelID); err != nil {
		return s.convFailFor("908 conversation", err)
	}
	if !channelID.Valid {
		return s.convFailFor("908 carrier", errors.New("conversation: у активного разговора нет носителя"))
	}
	periods, visibleFrom, ok, err := convVisibility(ctx, s.db, p.ConversationID, policy, pid)
	if err != nil || !ok {
		if err == nil {
			err = errors.New("conversation: нет периодов вызывающего")
		}
		return s.convFailFor("908 periods", err)
	}
	// Последний видимый элемент (событие или сообщение): отметка новее него
	// обрезается до него.
	_, rows, err := readConvFeed(ctx, s.db, p.ConversationID, channelID.String, feedNewest, visibleFrom, 1, periods)
	if err != nil {
		return s.convFailFor("908 last item", err)
	}
	if len(rows) == 0 {
		return Response{OK: true, Data: ConvMarkReadResponse{}}
	}
	upTo := *p.UpTo
	if last := rows[0]; posAfter(upTo.CreatedAt, upTo.ID, last.createAt, last.id) {
		upTo = dmWirePosition{CreatedAt: last.createAt, ID: last.id}
	}

	// Позиция только растёт: строка обновляется, лишь если новая позиция
	// больше хранимой; гонка двух 908 не уменьшает её.
	if _, err := s.db.ExecContext(ctx, `INSERT INTO engram_read_state (conversation_id, photon_id, read_created_at, read_id, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (conversation_id, photon_id) DO UPDATE
SET read_created_at = EXCLUDED.read_created_at, read_id = EXCLUDED.read_id, updated_at = EXCLUDED.updated_at
WHERE EXCLUDED.read_created_at > engram_read_state.read_created_at
   OR (EXCLUDED.read_created_at = engram_read_state.read_created_at
       AND EXCLUDED.read_id COLLATE "C" > engram_read_state.read_id COLLATE "C")`,
		p.ConversationID, pid, upTo.CreatedAt, upTo.ID, s.now()); err != nil {
		return s.convFailFor("908 write", err)
	}
	var stored dmWirePosition
	if err := s.db.QueryRowContext(ctx, `SELECT read_created_at, read_id FROM engram_read_state
WHERE conversation_id = $1 AND photon_id = $2`, p.ConversationID, pid).Scan(&stored.CreatedAt, &stored.ID); err != nil {
		return s.convFailFor("908 read", err)
	}
	n, err := s.unreadCount(ctx, s.db, p.ConversationID, channelID.String, policy, pid, &stored)
	if err != nil {
		return s.convFailFor("908 unread", err)
	}
	return Response{OK: true, Data: ConvMarkReadResponse{ReadUpTo: &stored, UnreadCount: n}}
}
