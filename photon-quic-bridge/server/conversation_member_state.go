package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// 907 MEMBER_STATE_SET — личное состояние участника (ADR-106 §2.2–2.3,
// contract §4). Не меняет разговор: без X-блокировки, события, версии и
// ключа идемпотентности — установка по содержанию, повтор даёт тот же итог.

// convMaxPinned — предел закреплённых одного участника (ADR-106 Q4).
const convMaxPinned = 5

type convMemberStatePayload struct {
	ConversationID string          `json:"conversation_id"`
	Hidden         json.RawMessage `json:"hidden"`
	Pinned         json.RawMessage `json:"pinned"`
	MutedUntil     json.RawMessage `json:"muted_until"`
}

// ConvMemberState — личное состояние в ответе 907 и в ListItem 902.
type ConvMemberState struct {
	Hidden     bool   `json:"hidden"`
	PinnedAt   *int64 `json:"pinned_at"`
	MutedUntil *int64 `json:"muted_until"`
}

type ConvMemberStateResponse struct {
	MemberState ConvMemberState `json:"member_state"`
}

func parseOptBool(raw json.RawMessage) (set, val, ok bool) {
	if raw == nil {
		return false, false, true
	}
	if err := json.Unmarshal(raw, &val); err != nil || string(raw) == "null" {
		return false, false, false
	}
	return true, val, true
}

// convPinLockKey — сериализация закреплений одного участника: два параллельных
// pin не должны дать шесть.
func convPinLockKey(pid string) int64 { return advisoryKey("photon:pin:" + pid) }

func (s *convService) setMemberState(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convMemberStatePayload
	if !decodeStrict(raw, &p) || p.ConversationID == "" {
		return convFail(convErrInvalidRequest)
	}
	hSet, hVal, ok1 := parseOptBool(p.Hidden)
	pSet, pVal, ok2 := parseOptBool(p.Pinned)
	if !ok1 || !ok2 {
		return convFail(convErrInvalidRequest)
	}
	mSet, mClear := p.MutedUntil != nil, false
	var mVal int64
	if mSet {
		if string(p.MutedUntil) == "null" {
			mClear = true
		} else if err := json.Unmarshal(p.MutedUntil, &mVal); err != nil || mVal < 0 {
			return convFail(convErrInvalidRequest)
		}
	}
	if !hSet && !pSet && !mSet {
		return convFail(convErrInvalidRequest)
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("907 caller", err)
	}
	if !found {
		return convFail(convErrConversationNF)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return s.convFailFor("907 begin", err)
	}
	defer tx.Rollback()

	code, err := callerAccess(ctx, tx, p.ConversationID, pid)
	if err != nil {
		return s.convFailFor("907 access", err)
	}
	if code != "" {
		return convFail(code)
	}
	if pSet && pVal {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, convPinLockKey(pid)); err != nil {
			return s.convFailFor("907 pin lock", err)
		}
	}
	now := s.now()
	var st ConvMemberState
	var hiddenAt sql.NullInt64
	var pinnedAt, muted sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT hidden_at, pinned_at, muted_until FROM engram_member_state
WHERE conversation_id = $1 AND photon_id = $2 FOR UPDATE`, p.ConversationID, pid).Scan(&hiddenAt, &pinnedAt, &muted)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return s.convFailFor("907 read", err)
	}
	if hSet {
		hiddenAt = sql.NullInt64{Int64: now, Valid: hVal}
	}
	if pSet {
		switch {
		case !pVal:
			pinnedAt = sql.NullInt64{}
		case !pinnedAt.Valid:
			var others int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM engram_member_state
WHERE photon_id = $1 AND pinned_at IS NOT NULL AND conversation_id <> $2`, pid, p.ConversationID).Scan(&others); err != nil {
				return s.convFailFor("907 pin count", err)
			}
			if others >= convMaxPinned {
				return convFail(convErrInvalidRequest)
			}
			pinnedAt = sql.NullInt64{Int64: now, Valid: true}
		}
	}
	if mSet {
		muted = sql.NullInt64{Int64: mVal, Valid: !mClear}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO engram_member_state (conversation_id, photon_id, hidden_at, pinned_at, muted_until, updated_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (conversation_id, photon_id) DO UPDATE
SET hidden_at = $3, pinned_at = $4, muted_until = $5, updated_at = $6`,
		p.ConversationID, pid, hiddenAt, pinnedAt, muted, now); err != nil {
		return s.convFailFor("907 write", err)
	}
	if err := tx.Commit(); err != nil {
		return s.convFailFor("907 commit", err)
	}
	st.Hidden = hiddenAt.Valid
	if pinnedAt.Valid {
		st.PinnedAt = &pinnedAt.Int64
	}
	if muted.Valid {
		st.MutedUntil = &muted.Int64
	}
	return Response{OK: true, Data: ConvMemberStateResponse{MemberState: st}}
}
