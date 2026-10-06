package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

// Публичные разговоры — ADR-107 этап 2, contract §5a: 917 и 918.

const (
	convDirMaxQueryRunes = 64
	convDirDefaultLimit  = 20
	convDirMaxLimit      = 50
)

// ── 917 DIRECTORY_SEARCH ─────────────────────────────────────────────────────

type convDirPayload struct {
	Query  string  `json:"query"`
	Cursor *string `json:"cursor"`
	Limit  *int    `json:"limit"`
}

// ConvDirItem — запись каталога: без состава и без ленты (ADR-107 §2.5).
type ConvDirItem struct {
	ConversationID string `json:"conversation_id"`
	Title          string `json:"title"`
	Description    string `json:"description"`
	MemberCount    int    `json:"member_count"`
}

type ConvDirResponse struct {
	Conversations []ConvDirItem `json:"conversations"`
	NextCursor    string        `json:"next_cursor,omitempty"`
}

// directorySearch — 917: public и не архивные; подстрока названия без учёта
// регистра (Q4); порядок (created_at DESC, conversation_id), курсор — keyset
// (created_at, conversation_id). Участие вызывающего не требуется.
func (s *convService) directorySearch(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convDirPayload
	if !decodeStrict(raw, &p) {
		return convFail(convErrInvalidRequest)
	}
	q := strings.TrimSpace(p.Query)
	if n := utf8.RuneCountInString(q); n < 1 || n > convDirMaxQueryRunes {
		return convFail(convErrInvalidRequest)
	}
	limit := convDirDefaultLimit
	if p.Limit != nil {
		limit = *p.Limit
		if limit < 1 || limit > convDirMaxLimit {
			return convFail(convErrInvalidLimit)
		}
	}
	var after *convListCursor
	if p.Cursor != nil {
		c, ok := decodeListCursor(*p.Cursor)
		if !ok {
			return convFail(convErrInvalidCursor)
		}
		after = &c
	}
	query := `SELECT conversation_id, title, description, member_count, created_at FROM engram_conversation
WHERE visibility = 'public' AND state = 'active' AND strpos(lower(title), lower($1)) > 0`
	args := []any{q, limit + 1}
	if after != nil {
		query += ` AND (created_at < $3 OR (created_at = $3 AND conversation_id > $4))`
		args = append(args, after.A, after.C)
	}
	query += ` ORDER BY created_at DESC, conversation_id LIMIT $2`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return s.convFailFor("917", err)
	}
	defer rows.Close()
	out := ConvDirResponse{Conversations: []ConvDirItem{}}
	var created []int64
	for rows.Next() {
		var it ConvDirItem
		var at int64
		if err := rows.Scan(&it.ConversationID, &it.Title, &it.Description, &it.MemberCount, &at); err != nil {
			return s.convFailFor("917 scan", err)
		}
		out.Conversations = append(out.Conversations, it)
		created = append(created, at)
	}
	if err := rows.Err(); err != nil {
		return s.convFailFor("917 rows", err)
	}
	if len(out.Conversations) > limit {
		out.Conversations = out.Conversations[:limit]
		out.NextCursor = encodeListCursor(convListCursor{A: created[limit-1], C: out.Conversations[limit-1].ConversationID})
	}
	return Response{OK: true, Data: out}
}

// ── 918 JOIN_PUBLIC ──────────────────────────────────────────────────────────

type convJoinPublicPayload struct {
	RequestID      string `json:"request_id"`
	ConversationID string `json:"conversation_id"`
}

// joinPublic — 918: как 916, но для public разговора. private, creating и
// несуществующий неразличимы — conversation_not_found (ADR-107 §2.6).
func (s *convService) joinPublic(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convJoinPublicPayload
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) || p.ConversationID == "" {
		return convFail(convErrInvalidRequest)
	}
	// Предел общий с 916 — до любой работы с БД.
	if !s.joinLimiter().allow(callerID, s.now()) {
		return convFail(convErrTemporarilyUnavail)
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("918 caller", err)
	}
	if !found {
		return convFail(convErrConversationNF)
	}
	reqHash := convHash(map[string]any{"conversation_id": p.ConversationID})

	// Повтор по §10.2 — первым.
	if data, code, done, err := s.joinStoredKey(ctx, s.db, ActionConversationJoinPublic, pid, p.RequestID, reqHash); err != nil {
		return s.convFailFor("918 key", err)
	} else if code != "" {
		return convFail(code)
	} else if done {
		return Response{OK: true, Data: data}
	}

	var visibility string
	err = s.db.QueryRowContext(ctx, `SELECT visibility FROM engram_conversation WHERE conversation_id = $1`, p.ConversationID).Scan(&visibility)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && visibility != "public") {
		return convFail(convErrConversationNF)
	}
	if err != nil {
		return s.convFailFor("918 lookup", err)
	}
	release, err := s.holdLock(ctx, convLockKey(p.ConversationID))
	if err != nil {
		return s.convFailFor("918 lock", err)
	}
	defer release()

	data, failCode, err := s.joinPublicTx(ctx, pid, p.RequestID, p.ConversationID, reqHash)
	if err != nil {
		return s.convFailFor("918", err)
	}
	if failCode != "" {
		return convFail(failCode)
	}
	if err := s.flushEvents(ctx, p.ConversationID, map[string]string{pid: callerID}); err != nil {
		return s.convFailFor("918 events", err)
	}
	return Response{OK: true, Data: data}
}

func (s *convService) joinPublicTx(ctx context.Context, pid, requestID, convID string, reqHash []byte) (json.RawMessage, string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, "", err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `INSERT INTO engram_request (photon_id, action, request_id, payload_hash, conversation_id, state, created_at)
VALUES ($1, $2, $3, $4, NULL, 'pending', $5) ON CONFLICT DO NOTHING`, pid, ActionConversationJoinPublic, requestID, reqHash, s.now())
	if err != nil {
		return nil, "", err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		data, code, done, err := s.joinStoredKey(ctx, tx, ActionConversationJoinPublic, pid, requestID, reqHash)
		if err != nil || code != "" {
			return nil, code, err
		}
		if !done {
			return nil, "", errConvUnavailable
		}
		return data, "", nil
	}

	// Видимость и состояние — повторно под X разговора и FOR UPDATE:
	// 903 мог сделать разговор закрытым после предварительного чтения.
	c := &convChange{tx: tx, convID: convID, callerPID: pid}
	var state, visibility string
	if err := tx.QueryRowContext(ctx, `SELECT state, kind, version, member_count, position_h, visibility FROM engram_conversation
WHERE conversation_id = $1 FOR UPDATE`, convID).Scan(&state, &c.kind, &c.version, &c.memberCount, &c.h, &visibility); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, convErrConversationNF, nil
		}
		return nil, "", err
	}
	if state == "creating" || visibility != "public" {
		return nil, convErrConversationNF, nil
	}
	// Забаненный не входит через каталог (ADR-111 §2.3); код тот же, что у
	// закрытого разговора (ADR-107 §2.6).
	if banned, err := conversationBanned(ctx, tx, convID, pid); err != nil {
		return nil, "", err
	} else if banned {
		return nil, convErrConversationNF, nil
	}
	if state == "archived" {
		return nil, convErrArchived, nil
	}
	role, err := activePeriod(ctx, tx, convID, pid)
	if err != nil {
		return nil, "", err
	}
	var clock, firstAt int64
	if role == "" {
		c.nextVersion = c.version + 1
		clock = s.now()
		c.nextPos = clock
		if c.h+1 > c.nextPos {
			c.nextPos = c.h + 1
		}
		at := c.addEvent(convEventBody{Event: "member_joined", ActorPhotonID: pid})
		if _, err := tx.ExecContext(ctx, `INSERT INTO engram_member_period (conversation_id, photon_id, period_no, role,
state, opened_version, open_at)
SELECT $1, $2, COALESCE(max(period_no), 0) + 1, 'member', 'active', $3, $4
FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2`, convID, pid, c.nextVersion, at); err != nil {
			return nil, "", err
		}
		c.memberCount++
		if err := s.persistChange(ctx, c); err != nil {
			return nil, "", err
		}
		firstAt = c.ats[0]
	}
	snap, err := snapshotFor(ctx, tx, convID, pid)
	if err != nil {
		return nil, "", err
	}
	body, err := json.Marshal(map[string]any{"conversation": snap.Conversation})
	if err != nil {
		return nil, "", err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE engram_request SET state = 'done', response = $4, conversation_id = $5
WHERE photon_id = $1 AND action = $2 AND request_id = $3`, pid, ActionConversationJoinPublic, requestID, body, convID); err != nil {
		return nil, "", err
	}
	if err := tx.Commit(); err != nil {
		return nil, "", err
	}
	if firstAt != 0 {
		s.observePositionLead("918", convID, firstAt, clock)
	}
	return body, "", nil
}
