package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// 940 CONVERSATION_HISTORY_BEFORE — ADR-105 §2.2–§2.4, implementation
// contract messaging-v2 §3. Близнец 931: тот же доступ (S-блокировка,
// callerAccess с правилом архива §8.1), те же периоды since_join, тот же
// оператор §7.2 (барьер, надгробия, один пост на позицию), другое
// направление. Барьер не нужен (ADR-105 §1.4): выданное ниже барьера уже
// окончательно, а оператор и так не отдаёт ничего выше него.

type convHistoryPayload struct {
	ConversationID string          `json:"conversation_id"`
	Before         *dmWirePosition `json:"before"`
	Limit          *int            `json:"limit"`
}

// ConvHistoryResponse — ответ 940: элементы строго старше якоря в порядке
// ленты; курсора нет (ADR-105 §2.3).
type ConvHistoryResponse struct {
	Items   []any `json:"items"`
	AtStart bool  `json:"at_start"`
}

// historyBefore — 940.
func (s *convService) historyBefore(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convHistoryPayload
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
		return s.convFailFor("940 caller", err)
	}
	if !found {
		return convFail(convErrConversationNF)
	}

	releaseS, err := s.holdSharedLock(ctx, convLockKey(p.ConversationID))
	if err != nil {
		return s.convFailFor("940 lock", err)
	}
	defer releaseS()

	code, err := callerAccess(ctx, s.db, p.ConversationID, pid)
	if err != nil {
		return s.convFailFor("940 access", err)
	}
	if code != "" {
		return convFail(code)
	}
	// Содержание запроса — после доступа (ADR-099 §11).
	if p.Before == nil || p.Before.CreatedAt <= 0 || !postIDRe.MatchString(p.Before.ID) {
		return convFail(convErrInvalidRequest)
	}

	var policy string
	var channelID sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT history_policy, carrier_channel_id FROM engram_conversation
WHERE conversation_id = $1`, p.ConversationID).Scan(&policy, &channelID); err != nil {
		return s.convFailFor("940 conversation", err)
	}
	if !channelID.Valid {
		return s.convFailFor("940 carrier", errors.New("conversation: у активного разговора нет носителя"))
	}
	var periods []convPeriodBounds
	visibleFrom := int64(0)
	if policy == "since_join" {
		periods, err = callerPeriods(ctx, s.db, p.ConversationID, pid)
		if err != nil || len(periods) == 0 {
			if err == nil {
				err = errors.New("conversation: нет периодов вызывающего")
			}
			return s.convFailFor("940 periods", err)
		}
		visibleFrom = periods[0].open
	}

	// Позиции уникальны в разговоре, посты на позицию схлопнуты (S10), поэтому
	// «строго старше (created_at, id)» — это createat < before.created_at: id
	// различает посты одной позиции, а такой пост на позиции якоря — тот же
	// элемент и не выдаётся. limit+1 — проба для at_start.
	_, rows, err := runConvFeedBelow(ctx, s.db, convFeedSQL(feedBefore, len(periods)), p.ConversationID,
		channelID.String, visibleFrom, p.Before.CreatedAt, limit+1, periods)
	if err != nil {
		return s.convFailFor("940 feed", err)
	}
	atStart := len(rows) <= limit
	if !atStart {
		rows = rows[:limit] // строки по убыванию: лишняя — самая старая
	}
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	resp := ConvHistoryResponse{Items: []any{}, AtStart: atStart}
	for _, r := range rows {
		it, ok := convItemOf(r, pid)
		if !ok {
			s.logger("[Photon][Conv] 940 skipped post without reservation or event body", "post_pos", r.createAt)
			continue
		}
		resp.Items = append(resp.Items, it)
	}
	return Response{OK: true, Data: resp}
}
