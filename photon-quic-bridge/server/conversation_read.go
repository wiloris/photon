package main

import (
	"context"
	"database/sql"
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
	// IncludeHidden — вернуть и скрытые разговоры (ADR-106 §2.4).
	IncludeHidden bool `json:"include_hidden"`
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
	// PeerPhotonID — второй участник; только у direct (ADR-104 §3.1).
	PeerPhotonID string `json:"peer_photon_id,omitempty"`
	// LastItem — последний видимый вызывающему элемент ленты (ADR-099 §9.3,
	// видимость как у 931); null, если видимых элементов нет.
	LastItem any `json:"last_item"`
	// MemberState — личное состояние вызывающего; hidden — действующее по
	// правилу чтения (contract §4), не сохранённый флаг.
	MemberState ConvMemberState `json:"member_state"`
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

	// Скрыт, если hidden_at задан и (после него не было активности ИЛИ разговор
	// заглушён): contract §4, «возврат скрытого» — правило чтения.
	now := s.now()
	const hiddenExpr = `(ms.hidden_at IS NOT NULL AND (c.last_activity_at <= ms.hidden_at OR COALESCE(ms.muted_until > $2, false)))`
	base := `SELECT c.conversation_id, c.kind, c.title, c.state, c.version, c.member_count, m.role, c.last_activity_at,
       c.history_policy, c.carrier_channel_id, ` + hiddenExpr + `, ms.pinned_at, ms.muted_until
FROM engram_member_period m
CROSS JOIN LATERAL (SELECT * FROM engram_conversation c
                    WHERE c.conversation_id = m.conversation_id LIMIT 1) c
LEFT JOIN engram_member_state ms ON ms.conversation_id = m.conversation_id AND ms.photon_id = m.photon_id
WHERE m.photon_id = $1 AND m.state = 'active' AND c.state IN ('active','archived')
  AND ($3 OR NOT ` + hiddenExpr + `)`
	type listMeta struct{ policy, channelID string }
	var metas []listMeta
	collect := func(q string, args ...any) error {
		rows, err := s.db.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var it ConvListItem
			var policy string
			var channelID sql.NullString
			var pinned, muted sql.NullInt64
			if err := rows.Scan(&it.ConversationID, &it.Kind, &it.Title, &it.State, &it.ConversationVersion,
				&it.MemberCount, &it.MyRole, &it.LastActivityAt, &policy, &channelID,
				&it.MemberState.Hidden, &pinned, &muted); err != nil {
				return err
			}
			if pinned.Valid {
				it.MemberState.PinnedAt = &pinned.Int64
			}
			if muted.Valid {
				it.MemberState.MutedUntil = &muted.Int64
			}
			out.Conversations = append(out.Conversations, it)
			metas = append(metas, listMeta{policy, channelID.String})
		}
		return rows.Err()
	}
	// Закреплённые (≤ 5) — первой страницей целиком; курсор идёт по
	// незакреплённым (contract §4, «реализация»).
	if after == nil {
		if err := collect(base+` AND ms.pinned_at IS NOT NULL ORDER BY ms.pinned_at DESC, c.conversation_id DESC`,
			pid, now, p.IncludeHidden); err != nil {
			return s.convFailFor("902 pinned", err)
		}
	}
	nPinned := len(out.Conversations)
	q := base + ` AND ms.pinned_at IS NULL`
	args := []any{pid, now, p.IncludeHidden, limit + 1}
	if after != nil {
		q += ` AND (c.last_activity_at, c.conversation_id) < ($5, $6)`
		args = append(args, after.A, after.C)
	}
	q += ` ORDER BY c.last_activity_at DESC, c.conversation_id DESC LIMIT $4`
	if err := collect(q, args...); err != nil {
		return s.convFailFor("902 list", err)
	}
	if len(out.Conversations)-nPinned > limit {
		out.Conversations = out.Conversations[:nPinned+limit]
		last := out.Conversations[nPinned+limit-1]
		out.NextCursor = encodeListCursor(convListCursor{A: last.LastActivityAt, C: last.ConversationID})
	}
	for i := range out.Conversations {
		it := &out.Conversations[i]
		if it.Kind == "direct" {
			var peer string
			if err := s.db.QueryRowContext(ctx, `SELECT photon_id FROM engram_member_period
WHERE conversation_id = $1 AND state = 'active' AND photon_id <> $2 ORDER BY photon_id LIMIT 1`, it.ConversationID, pid).Scan(&peer); err != nil {
				return s.convFailFor("902 peer", err)
			}
			it.PeerPhotonID = peer
		}
		item, err := s.lastVisibleItem(ctx, it.ConversationID, metas[i].channelID, metas[i].policy, pid)
		if err != nil {
			return s.convFailFor("902 last_item", err)
		}
		it.LastItem = item
	}
	return Response{OK: true, Data: out}
}

// lastVisibleItem — самый новый элемент ленты, видимый вызывающему: те же
// периоды и тот же оператор, что у 931 (ADR-099 §9.4). nil — видимых элементов
// нет. Читается без S-блокировки: список не обещает снимка, а элемент —
// самый новый ниже барьера на момент запроса.
func (s *convService) lastVisibleItem(ctx context.Context, convID, channelID, policy, pid string) (any, error) {
	if channelID == "" {
		return nil, nil
	}
	var periods []convPeriodBounds
	visibleFrom := int64(0)
	if policy == "since_join" {
		var err error
		periods, err = callerPeriods(ctx, s.db, convID, pid)
		if err != nil {
			return nil, err
		}
		if len(periods) == 0 {
			return nil, nil
		}
		visibleFrom = periods[0].open
	}
	_, rows, err := readConvFeed(ctx, s.db, convID, channelID, feedNewest, visibleFrom, 1, periods)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	it, ok := convItemOf(rows[0], pid)
	if !ok {
		return nil, nil
	}
	return it, nil
}
