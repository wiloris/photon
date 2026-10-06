package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

// 932 MESSAGE_EDIT и 933 MESSAGE_DELETE — ADR-109 §2.1–2.2, §2.7; contract
// messaging-v2 §5b.2. Мутация — событие ленты: на новой позиции, версия
// разговора не растёт (ADR-099 §10.1). Исполняются общим runChange: ключ
// §10.2 первым, X(разговор), проверки ADR-099 §11 (доступ → архив → право →
// элемент → авторство), запись события — E2–E4.

type convEditPayload struct {
	RequestID      string `json:"request_id"`
	ConversationID string `json:"conversation_id"`
	ItemID         string `json:"item_id"`
	Text           string `json:"text"`
}

type convDeletePayload struct {
	RequestID      string `json:"request_id"`
	ConversationID string `json:"conversation_id"`
	ItemID         string `json:"item_id"`
}

// ConvMutationResponse — ответ 932 и 933: событие мутации как элемент ленты.
type ConvMutationResponse struct {
	Event ConvEventItem `json:"event"`
}

// convTarget — сообщение, на которое указывает item_id.
type convTarget struct {
	pos         int64
	sender      string
	deleted     bool
	editVersion int
}

// lookupVisibleMessage — опубликованное сообщение разговора с id поста
// itemID, видимое pid по правилам ленты (ADR-099 §9.4). nil — такого
// элемента нет либо он не видим: оба случая неразличимы для вызывающего.
func lookupVisibleMessage(ctx context.Context, q convQuerier, convID, pid, itemID string) (*convTarget, error) {
	var t convTarget
	err := q.QueryRowContext(ctx, `SELECT pos, COALESCE(sender, ''), deleted_at IS NOT NULL, edit_version FROM engram_position
WHERE conversation_id = $1 AND item_kind = 'message' AND state = 'published' AND post_id = $2`, convID, itemID).
		Scan(&t.pos, &t.sender, &t.deleted, &t.editVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var policy string
	if err := q.QueryRowContext(ctx, `SELECT history_policy FROM engram_conversation WHERE conversation_id = $1`, convID).Scan(&policy); err != nil {
		return nil, err
	}
	periods, _, ok, err := convVisibility(ctx, q, convID, policy, pid)
	if err != nil {
		return nil, err
	}
	if !ok || (len(periods) > 0 && !inPeriods(periods, t.pos)) {
		return nil, nil
	}
	return &t, nil
}

// mutationEvent — элемент ответа по только что добавленному событию. id поста
// события появляется после E2–E4 и подставляется после записи.
func mutationEvent(c *convChange, at int64) ConvEventItem {
	ev := c.events[len(c.events)-1]
	return ConvEventItem{Kind: "event", CreatedAt: at, Event: ev.Event, ActorPhotonID: ev.ActorPhotonID,
		TargetID: ev.TargetID, Text: ev.Text, EditVersion: ev.EditVersion, ConversationVersion: ev.Version}
}

// finishMutation — после runChange: id поста события берётся по его позиции
// (событие записано E2–E4 до ответа; повтор ключа получает тот же id).
func (s *convService) finishMutation(ctx context.Context, convID string, resp Response, what string) Response {
	if !resp.OK {
		return resp
	}
	raw, ok := resp.Data.(json.RawMessage)
	if !ok {
		return s.convFailFor(what+" response", errors.New("conversation: ответ мутации не сохранён как JSON"))
	}
	var out ConvMutationResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return s.convFailFor(what+" response", err)
	}
	var postID sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT post_id FROM engram_position WHERE conversation_id = $1 AND pos = $2`,
		convID, out.Event.CreatedAt).Scan(&postID); err != nil {
		return s.convFailFor(what+" event id", err)
	}
	if !postID.Valid {
		return s.convFailFor(what+" event id", errors.New("conversation: событие мутации без поста"))
	}
	out.Event.ID = postID.String
	return Response{OK: true, Data: out}
}

// editMessage — 932.
func (s *convService) editMessage(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convEditPayload
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) || p.ConversationID == "" || p.ItemID == "" {
		return convFail(convErrInvalidRequest)
	}
	// Те же пределы текста, что у 930 (ADR-109 §2.1).
	if strings.TrimSpace(p.Text) == "" {
		return convFail(errMessageEmpty)
	}
	if utf8.RuneCountInString(p.Text) > directMessageMaxRunes {
		return convFail(errMessageTooLong)
	}
	if countLinks(p.Text) > dmMaxLinks {
		return convFail(errMessageTooManyLinks)
	}
	spec := convChangeSpec{action: ActionConversationMessageEdit, requestID: p.RequestID, conversationID: p.ConversationID,
		hash:       convHash(map[string]any{"conversation_id": p.ConversationID, "item_id": p.ItemID, "text": p.Text}),
		permission: "messages.send", allowDirect: true, itemEvent: true}
	resp := s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
		t, err := lookupVisibleMessage(ctx, c.tx, c.convID, c.callerPID, p.ItemID)
		if err != nil {
			return nil, false, "", err
		}
		if t == nil {
			return nil, false, convErrInvalidRequest, nil
		}
		if t.deleted {
			return nil, false, convErrMessageDeleted, nil
		}
		if t.sender != c.callerPID {
			return nil, false, convErrForbidden, nil
		}
		version := t.editVersion + 1
		if _, err := c.tx.ExecContext(ctx, `UPDATE engram_position SET edit_version = $3 WHERE conversation_id = $1 AND pos = $2`,
			c.convID, t.pos, version); err != nil {
			return nil, false, "", err
		}
		if _, err := c.tx.ExecContext(ctx, `INSERT INTO engram_item_edit (conversation_id, item_id, text, edit_version, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (conversation_id, item_id) DO UPDATE SET text = EXCLUDED.text, edit_version = EXCLUDED.edit_version, updated_at = EXCLUDED.updated_at`,
			c.convID, p.ItemID, p.Text, version, s.now()); err != nil {
			return nil, false, "", err
		}
		at := c.addEvent(convEventBody{Event: "message_edited", ActorPhotonID: c.callerPID, TargetID: p.ItemID, TargetPos: t.pos,
			Text: p.Text, EditVersion: version})
		return ConvMutationResponse{Event: mutationEvent(c, at)}, true, "", nil
	})
	return s.finishMutation(ctx, p.ConversationID, resp, "932")
}

// deleteMessage — 933.
func (s *convService) deleteMessage(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convDeletePayload
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) || p.ConversationID == "" || p.ItemID == "" {
		return convFail(convErrInvalidRequest)
	}
	spec := convChangeSpec{action: ActionConversationMessageDelete, requestID: p.RequestID, conversationID: p.ConversationID,
		hash:        convHash(map[string]any{"conversation_id": p.ConversationID, "item_id": p.ItemID}),
		allowDirect: true, itemEvent: true}
	resp := s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
		t, err := lookupVisibleMessage(ctx, c.tx, c.convID, c.callerPID, p.ItemID)
		if err != nil {
			return nil, false, "", err
		}
		if t == nil {
			return nil, false, convErrInvalidRequest, nil
		}
		if t.deleted {
			return nil, false, convErrMessageDeleted, nil
		}
		// Автор — своё; иначе право messages.delete_any (ADR-109 §2.2).
		if t.sender != c.callerPID && !convHasPermission(c.callerRole, "messages.delete_any") {
			return nil, false, convErrForbidden, nil
		}
		if _, err := c.tx.ExecContext(ctx, `UPDATE engram_position SET deleted_at = $3 WHERE conversation_id = $1 AND pos = $2`,
			c.convID, t.pos, s.now()); err != nil {
			return nil, false, "", err
		}
		at := c.addEvent(convEventBody{Event: "message_deleted", ActorPhotonID: c.callerPID, TargetID: p.ItemID, TargetPos: t.pos})
		return ConvMutationResponse{Event: mutationEvent(c, at)}, true, "", nil
	})
	if resp.OK {
		s.deleteCarrierPost(ctx, p.ItemID)
	}
	return s.finishMutation(ctx, p.ConversationID, resp, "933")
}

// deleteCarrierPost — последний шаг 933: «отметка в БД → событие → удаление
// поста» (contract §5b.2). Сбой не меняет исход: элемент уже надгробие по
// deleted_at, и повтор ключа пробует удаление снова — пост, ещё не
// удалённый, удаляется.
func (s *convService) deleteCarrierPost(ctx context.Context, postID string) {
	var live bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM posts WHERE id = $1 AND deleteat = 0)`, postID).Scan(&live); err != nil {
		s.logger("[Photon][Conv] 933 post lookup failed", "error", err.Error())
		return
	}
	if !live {
		return
	}
	if appErr := s.mm.DeletePost(postID); appErr != nil {
		s.logger("[Photon][Conv] 933 DeletePost failed, item stays a tombstone", "error", appErr.Error())
	}
}
