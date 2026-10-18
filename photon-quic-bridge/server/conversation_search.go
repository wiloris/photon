package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

// 941 CONVERSATION_SEARCH — ADR-110, contract messaging-v2 §5b.3. Поиск по
// индексу текста Bridge (engram_item_text, Q2): подстрока без учёта регистра,
// ё ≡ е; доступ — как 931/940 (разговоры, где вызывающий в периоде active;
// since_join — только позиции его периодов; без надгробий).

const (
	convSearchMinRunes     = 2
	convSearchMaxRunes     = 64
	convSearchDefaultLimit = 20
	convSearchMaxLimit     = 50
	convBackfillBatch      = 500
)

// normalizeSearchText — нормализация текста индекса и запроса: нижний
// регистр, ё → е (как локальный поиск на устройстве).
func normalizeSearchText(s string) string {
	return strings.ReplaceAll(strings.ToLower(s), "ё", "е")
}

// likeEscape экранирует метасимволы LIKE (ESCAPE '\').
func likeEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

type convSearchPayload struct {
	Query          string          `json:"query"`
	ConversationID string          `json:"conversation_id"`
	Before         *dmWirePosition `json:"before"`
	Limit          *int            `json:"limit"`
	Features       []string        `json:"features"` // ADR-124 M1, contract §8.8
}

// ConvSearchHit — одно найденное сообщение.
type ConvSearchHit struct {
	ConversationID string `json:"conversation_id"`
	Item           any    `json:"item"`
}

// ConvSearchResponse — ответ 941.
type ConvSearchResponse struct {
	Hits       []ConvSearchHit `json:"hits"`
	NextBefore *dmWirePosition `json:"next_before,omitempty"`
}

// convSearchSQL — один запрос: разговоры вызывающего ($1), необязательный
// фильтр ($2, ” — все), образец ($3), якорь ($4 created_at, $5 id; $4 = 0 —
// без якоря), limit ($6). Периоды since_join — EXISTS по периодам вызывающего.
const convSearchSQL = `SELECT t.conversation_id, t.item_id, t.created_at, pos.sender, pos.item_key,
       COALESCE(e.text, p.message), (p.props->>'engram_reply_to'), (p.props->'engram_forwarded_from')::text,
       (p.props->'photon_media')::text
FROM engram_member_period mp
JOIN engram_conversation c ON c.conversation_id = mp.conversation_id AND c.state <> 'creating'
JOIN engram_item_text t ON t.conversation_id = mp.conversation_id
JOIN engram_position pos ON pos.conversation_id = t.conversation_id AND pos.pos = t.created_at
JOIN posts p ON p.id = pos.post_id AND p.deleteat = 0
LEFT JOIN engram_item_edit e ON e.conversation_id = t.conversation_id AND e.item_id = t.item_id
WHERE mp.photon_id = $1 AND mp.state = 'active'
  AND ($2 = '' OR mp.conversation_id = $2)
  AND t.text_norm LIKE $3 ESCAPE '\'
  AND ($4::bigint = 0 OR t.created_at < $4::bigint OR (t.created_at = $4::bigint AND t.item_id COLLATE "C" < $5::text))
  AND pos.item_kind = 'message' AND pos.state = 'published' AND pos.deleted_at IS NULL AND pos.post_id = t.item_id
  AND (c.history_policy = 'full' OR EXISTS (
        SELECT 1 FROM engram_member_period pp
        WHERE pp.conversation_id = t.conversation_id AND pp.photon_id = $1 AND pp.open_at IS NOT NULL
          AND t.created_at >= pp.open_at AND t.created_at <= COALESCE(pp.close_at, 9223372036854775807)))
ORDER BY t.created_at DESC, t.item_id COLLATE "C" DESC
LIMIT $6`

// searchMessages — 941.
func (s *convService) searchMessages(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convSearchPayload
	if !decodeStrict(raw, &p) {
		return convFail(convErrInvalidRequest)
	}
	query := strings.TrimSpace(p.Query)
	if n := utf8.RuneCountInString(query); n < convSearchMinRunes || n > convSearchMaxRunes {
		return convFail(convErrInvalidRequest)
	}
	limit := convSearchDefaultLimit
	if p.Limit != nil {
		limit = *p.Limit
		if limit < 1 || limit > convSearchMaxLimit {
			return convFail(convErrInvalidLimit)
		}
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("941 caller", err)
	}
	if !found {
		if p.ConversationID != "" {
			return convFail(convErrConversationNF)
		}
		return Response{OK: true, Data: ConvSearchResponse{Hits: []ConvSearchHit{}}}
	}
	if p.ConversationID != "" {
		code, err := callerAccess(ctx, s.db, p.ConversationID, pid)
		if err != nil {
			return s.convFailFor("941 access", err)
		}
		if code != "" {
			return convFail(code)
		}
	}
	// Содержание запроса — после доступа (ADR-099 §11).
	var beforeAt int64
	var beforeID string
	if p.Before != nil {
		if p.Before.CreatedAt <= 0 || !postIDRe.MatchString(p.Before.ID) {
			return convFail(convErrInvalidRequest)
		}
		beforeAt, beforeID = p.Before.CreatedAt, p.Before.ID
	}

	pattern := "%" + likeEscape(normalizeSearchText(query)) + "%"
	rows, err := s.db.QueryContext(ctx, convSearchSQL, pid, p.ConversationID, pattern, beforeAt, beforeID, limit+1)
	if err != nil {
		return s.convFailFor("941 query", err)
	}
	defer rows.Close()
	type hit struct {
		convID string
		row    convFeedRow
	}
	var found2 []hit
	for rows.Next() {
		var convID, id string
		var at int64
		var sender, key, msg, reply, fwd, media sql.NullString
		if err := rows.Scan(&convID, &id, &at, &sender, &key, &msg, &reply, &fwd, &media); err != nil {
			return s.convFailFor("941 scan", err)
		}
		found2 = append(found2, hit{convID, convFeedRow{id: id, createAt: at, sender: sender.String, itemKey: key.String,
			message: msg.String, replyTo: reply.String, forwardedFrom: fwd.String, media: media.String}})
	}
	if err := rows.Err(); err != nil {
		return s.convFailFor("941 rows", err)
	}
	more := len(found2) > limit
	if more {
		found2 = found2[:limit]
	}
	resp := ConvSearchResponse{Hits: []ConvSearchHit{}}
	for _, h := range found2 {
		it, ok := convItemOf(h.row, pid, nil, hasFeature(p.Features, convFeatureAttachments))
		if !ok {
			s.logger("[Photon][Conv] 941 skipped post without reservation", "post_pos", h.row.createAt)
			continue
		}
		resp.Hits = append(resp.Hits, ConvSearchHit{ConversationID: h.convID, Item: it})
	}
	if more && len(found2) > 0 {
		last := found2[len(found2)-1].row
		resp.NextBefore = &dmWirePosition{CreatedAt: last.createAt, ID: last.id}
	}
	return Response{OK: true, Data: resp}
}

// upsertItemText записывает (или заменяет) строку индекса текста сообщения.
func upsertItemText(ctx context.Context, q interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}, convID, itemID string, createdAt int64, text string) error {
	_, err := q.ExecContext(ctx, `INSERT INTO engram_item_text (item_id, conversation_id, created_at, text_norm)
VALUES ($1, $2, $3, $4)
ON CONFLICT (item_id) DO UPDATE SET text_norm = EXCLUDED.text_norm`, itemID, convID, createdAt, normalizeSearchText(text))
	return err
}

// backfillItemText — Go-шаг миграции v9: уже опубликованные, не удалённые
// сообщения (S1-чтение постов-носителей, те же таблицы, что у ленты); текст —
// последняя правка, если есть. Порциями по ключу (conversation_id, pos).
func backfillItemText(ctx context.Context, tx *sql.Tx) error {
	// Без таблицы posts публиковать нечего; её отсутствие — расхождение, о
	// котором сообщает checkConversationSchema, а не миграция.
	var reg sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('posts')::text`).Scan(&reg); err != nil {
		return err
	}
	if !reg.Valid {
		return nil
	}
	type rec struct {
		conv, id, text string
		pos            int64
	}
	lastConv, lastPos := "", int64(-1)
	for {
		rows, err := tx.QueryContext(ctx, `SELECT pos.conversation_id, pos.pos, pos.post_id, COALESCE(e.text, p.message)
FROM engram_position pos
JOIN posts p ON p.id = pos.post_id AND p.deleteat = 0
LEFT JOIN engram_item_edit e ON e.conversation_id = pos.conversation_id AND e.item_id = pos.post_id
WHERE pos.item_kind = 'message' AND pos.state = 'published' AND pos.deleted_at IS NULL AND pos.post_id IS NOT NULL
  AND (pos.conversation_id, pos.pos) > ($1, $2)
ORDER BY pos.conversation_id, pos.pos LIMIT $3`, lastConv, lastPos, convBackfillBatch)
		if err != nil {
			return err
		}
		var batch []rec
		for rows.Next() {
			var r rec
			if err := rows.Scan(&r.conv, &r.pos, &r.id, &r.text); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, r := range batch {
			if err := upsertItemText(ctx, tx, r.conv, r.id, r.pos, r.text); err != nil {
				return err
			}
		}
		if len(batch) < convBackfillBatch {
			return nil
		}
		last := batch[len(batch)-1]
		lastConv, lastPos = last.conv, last.pos
	}
}
