package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// 919 CONVERSATION_MEMBER_BAN — бан PhotonID в разговоре и снятие бана
// (ADR-111 §2.3; contract messaging-v2 §5b.4). Исполняется через общий
// runChange: ключ §10.2, X(разговор), проверки ADR-099 §11, право
// members.remove и правило «роль ниже» (ADR-099 §7.3), как у 911.

type convBanPayload struct {
	RequestID      string `json:"request_id"`
	ConversationID string `json:"conversation_id"`
	PhotonID       string `json:"photon_id"`
	Ban            *bool  `json:"ban"`
}

// conversationBanned — есть ли PhotonID в списке банов разговора.
func conversationBanned(ctx context.Context, q convQuerier, convID, pid string) (bool, error) {
	var one int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM engram_conversation_ban WHERE conversation_id = $1 AND photon_id = $2`, convID, pid).Scan(&one)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

func (s *convService) banMember(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convBanPayload
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) || p.ConversationID == "" || p.PhotonID == "" || p.Ban == nil {
		return convFail(convErrInvalidRequest)
	}
	ban := *p.Ban
	spec := convChangeSpec{action: ActionConversationMemberBan, requestID: p.RequestID, conversationID: p.ConversationID,
		hash: convHash(map[string]any{"conversation_id": p.ConversationID, "photon_id": p.PhotonID, "ban": ban}), permission: "members.remove"}
	return s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
		if !isCanonicalPhotonID(p.PhotonID) {
			return nil, false, errInvalidPhotonID, nil
		}
		if !ban {
			// Снятие бана — без события и без версии; в разговор не
			// возвращает (ADR-111 §2.3).
			if _, err := c.tx.ExecContext(ctx, `DELETE FROM engram_conversation_ban WHERE conversation_id = $1 AND photon_id = $2`,
				c.convID, p.PhotonID); err != nil {
				return nil, false, "", err
			}
			return convSnapshotData{}, false, "", nil
		}
		role, err := activePeriod(ctx, c.tx, c.convID, p.PhotonID)
		if err != nil {
			return nil, false, "", err
		}
		// Активного периода нет — роль не сравнивается, пишется только запрос бана.
		if role != "" {
			// owner не банится; банить можно только роль ниже своей (ADR-099 §7.3).
			if role == "owner" || convRoleRank[role] >= convRoleRank[c.callerRole] {
				return nil, false, convErrForbidden, nil
			}
		}
		if _, err := c.tx.ExecContext(ctx, `INSERT INTO engram_conversation_ban (conversation_id, photon_id, banned_at, banned_by)
VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, c.convID, p.PhotonID, s.now(), c.callerPID); err != nil {
			return nil, false, "", err
		}
		if role == "" {
			return convSnapshotData{}, false, "", nil
		}
		at := c.addEvent(convEventBody{Event: "member_banned", ActorPhotonID: c.callerPID, TargetPhotonID: p.PhotonID})
		if err := closePeriod(ctx, c, p.PhotonID, "banned", at); err != nil {
			return nil, false, "", err
		}
		c.memberCount--
		return convSnapshotData{}, true, "", nil
	})
}
