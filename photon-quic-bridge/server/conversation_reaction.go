package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// 934 REACTION_SET — реакция участника на сообщение (ADR-109 §2.3; решение
// владельца 2026-10-06 по Q3; contract messaging-v2 §5b.5). Множество
// (элемент, участник, реакция): установка идемпотентна без ключа. Исполняется
// общим runChange как 932, но без записи в engram_request (keyless): X
// разговора сериализует параллельные повторы, второй видит уже изменённое
// состояние и отвечает changed:false без события.

// convReactionSet — допустимые реакции: ровно восемь последовательностей
// кодовых точек владельца (порядок владельца). Сравнение побайтовое, без
// нормализации: ❤ без U+FE0F и модификаторы цвета кожи не входят.
var convReactionSet = [...]string{
	"\U0001F44D", // 👍
	"❤️",         // ❤️
	"\U0001F602", // 😂
	"\U0001F62E", // 😮
	"\U0001F622", // 😢
	"\U0001F621", // 😡
	"\U0001F389", // 🎉
	"\U0001F44E", // 👎
}

func convReactionAllowed(r string) bool {
	for _, a := range convReactionSet {
		if r == a {
			return true
		}
	}
	return false
}

type convReactionPayload struct {
	ConversationID string `json:"conversation_id"`
	ItemID         string `json:"item_id"`
	Reaction       string `json:"reaction"`
	On             *bool  `json:"on"`
}

// ConvReactionResponse — ответ 934.
type ConvReactionResponse struct {
	Changed bool `json:"changed"`
}

func (s *convService) setReaction(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convReactionPayload
	if !decodeStrict(raw, &p) || p.ConversationID == "" || p.ItemID == "" || p.On == nil || !convReactionAllowed(p.Reaction) {
		return convFail(convErrInvalidRequest)
	}
	on := *p.On
	spec := convChangeSpec{action: ActionConversationReactionSet, conversationID: p.ConversationID,
		permission: "messages.send", allowDirect: true, itemEvent: true, keyless: true}
	return s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
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
		var one int
		err = c.tx.QueryRowContext(ctx, `SELECT 1 FROM engram_reaction WHERE item_id = $1 AND photon_id = $2 AND reaction = $3`,
			p.ItemID, c.callerPID, p.Reaction).Scan(&one)
		exists := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, false, "", err
		}
		if exists == on {
			return ConvReactionResponse{Changed: false}, false, "", nil
		}
		if on {
			_, err = c.tx.ExecContext(ctx, `INSERT INTO engram_reaction (item_id, photon_id, reaction, created_at) VALUES ($1, $2, $3, $4)`,
				p.ItemID, c.callerPID, p.Reaction, s.now())
		} else {
			_, err = c.tx.ExecContext(ctx, `DELETE FROM engram_reaction WHERE item_id = $1 AND photon_id = $2 AND reaction = $3`,
				p.ItemID, c.callerPID, p.Reaction)
		}
		if err != nil {
			return nil, false, "", err
		}
		c.addEvent(convEventBody{Event: "reaction_changed", ActorPhotonID: c.callerPID, TargetID: p.ItemID, TargetPos: t.pos,
			Reaction: p.Reaction, On: &on})
		return ConvReactionResponse{Changed: true}, true, "", nil
	})
}
