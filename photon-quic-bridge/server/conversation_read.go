package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
)

// 901 GET и 902 LIST — ADR-099 §5, §6, §11; conversation-storage.md §6.5, §7.3.

type convGetPayload struct {
	ConversationID string `json:"conversation_id"`
}

// getConversation — 901: снимок и полный состав (единственный источник
// состава, ADR-099 §5).
func (s *convService) getConversation(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convGetPayload
	if !decodeStrict(raw, &p) || p.ConversationID == "" {
		return convFail(convErrInvalidRequest)
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("901 caller", err)
	}
	if !found {
		// Без PhotonID пользователь ни в одном разговоре не участвовал.
		return convFail(convErrConversationNF)
	}
	code, err := callerAccess(ctx, s.db, p.ConversationID, pid)
	if err != nil {
		return s.convFailFor("901 access", err)
	}
	if code != "" {
		return convFail(code)
	}
	snap, err := snapshotFor(ctx, s.db, p.ConversationID, pid)
	if err != nil {
		return s.convFailFor("901 snapshot", err)
	}
	return Response{OK: true, Data: snap}
}

type convListPayload struct {
	Cursor *string `json:"cursor"`
	Limit  *int    `json:"limit"`
}

// ConvListItem — краткое описание без состава (ADR-099 §6).
type ConvListItem struct {
	ConversationID      string `json:"conversation_id"`
	Kind                string `json:"kind"`
	Title               string `json:"title"`
	State               string `json:"state"`
	ConversationVersion int64  `json:"conversation_version"`
	MemberCount         int    `json:"member_count"`
	MyRole              string `json:"my_role"`
	LastActivityAt      int64  `json:"last_activity_at"`
}

type ConvListResponse struct {
	Conversations []ConvListItem `json:"conversations"`
	NextCursor    string         `json:"next_cursor,omitempty"`
}

// convListCursor — позиция keyset (last_activity_at, conversation_id);
// непрозрачна для клиента (ADR-099 §6).
type convListCursor struct {
	A int64  `json:"a"`
	C string `json:"c"`
}

func encodeListCursor(c convListCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeListCursor(s string) (convListCursor, bool) {
	var c convListCursor
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || json.Unmarshal(b, &c) != nil || c.C == "" {
		return c, false
	}
	return c, true
}

const (
	convListDefaultLimit = 60
	convListMaxLimit     = 200
)

// listConversations — 902: группы, где вызывающий в периоде active, включая
// архивные; свежие первыми (ADR-099 §6). Форма запроса — спецификация §7.3.
func (s *convService) listConversations(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convListPayload
	if !decodeStrict(raw, &p) {
		return convFail(convErrInvalidRequest)
	}
	limit := convListDefaultLimit
	if p.Limit != nil {
		limit = *p.Limit
		if limit < 1 || limit > convListMaxLimit {
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
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("902 caller", err)
	}
	out := ConvListResponse{Conversations: []ConvListItem{}}
	if !found {
		return Response{OK: true, Data: out}
	}

	q := `SELECT c.conversation_id, c.title, c.state, c.version, c.member_count, m.role, c.last_activity_at
FROM engram_member_period m
CROSS JOIN LATERAL (SELECT * FROM engram_conversation c
                    WHERE c.conversation_id = m.conversation_id LIMIT 1) c
WHERE m.photon_id = $1 AND m.state = 'active' AND c.state IN ('active','archived')`
	args := []any{pid}
	if after != nil {
		q += ` AND (c.last_activity_at, c.conversation_id) < ($3, $4)`
		args = append(args, limit+1, after.A, after.C)
	} else {
		args = append(args, limit+1)
	}
	q += ` ORDER BY c.last_activity_at DESC, c.conversation_id DESC LIMIT $2`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return s.convFailFor("902 list", err)
	}
	defer rows.Close()
	for rows.Next() {
		it := ConvListItem{Kind: "group"}
		if err := rows.Scan(&it.ConversationID, &it.Title, &it.State, &it.ConversationVersion,
			&it.MemberCount, &it.MyRole, &it.LastActivityAt); err != nil {
			return s.convFailFor("902 scan", err)
		}
		out.Conversations = append(out.Conversations, it)
	}
	if err := rows.Err(); err != nil {
		return s.convFailFor("902 rows", err)
	}
	if len(out.Conversations) > limit {
		out.Conversations = out.Conversations[:limit]
		last := out.Conversations[limit-1]
		out.NextCursor = encodeListCursor(convListCursor{A: last.LastActivityAt, C: last.ConversationID})
	}
	return Response{OK: true, Data: out}
}
