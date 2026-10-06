package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// Приглашения по ссылке — ADR-107 этап 1, contract §5: 913–916.

const convErrInviteInvalid = "invite_invalid"

// Предел попыток 916: 20 в минуту на пользователя, в памяти процесса
// (contract §5, эксплуатационный параметр ADR-107 §3). В ADR-099 §12 кода
// rate_limited нет — ответ temporarily_unavailable.
const (
	joinAttemptsPerWindow = 20
	joinAttemptWindowMs   = 60_000
)

type joinLimiter struct {
	mu       sync.Mutex
	attempts map[string][]int64
}

// allow учитывает попытку; false — предел исчерпан.
func (l *joinLimiter) allow(user string, now int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.attempts == nil {
		l.attempts = map[string][]int64{}
	}
	kept := l.attempts[user][:0]
	for _, t := range l.attempts[user] {
		if now-t < joinAttemptWindowMs {
			kept = append(kept, t)
		}
	}
	if len(kept) >= joinAttemptsPerWindow {
		l.attempts[user] = kept
		return false
	}
	l.attempts[user] = append(kept, now)
	return true
}

var joinLimiterInit sync.Mutex

// joinLimiter — предел попыток сервиса; лениво, без sync-значений в convService
// (тесты копируют структуру).
func (s *convService) joinLimiter() *joinLimiter {
	joinLimiterInit.Lock()
	defer joinLimiterInit.Unlock()
	if s.joinRL == nil {
		s.joinRL = &joinLimiter{}
	}
	return s.joinRL
}

func inviteTokenHash(token string) []byte {
	h := sha256.Sum256([]byte(token))
	return h[:]
}

func newInviteToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func newInviteID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	h := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32]), nil
}

// ConvInvite — приглашение. Token есть только в ответе 913.
type ConvInvite struct {
	InviteID  string `json:"invite_id"`
	Token     string `json:"token,omitempty"`
	ExpiresAt *int64 `json:"expires_at"`
	MaxUses   *int   `json:"max_uses"`
	Uses      int    `json:"uses"`
}

// ── 913 INVITE_CREATE ────────────────────────────────────────────────────────

type convInviteCreatePayload struct {
	RequestID      string `json:"request_id"`
	ConversationID string `json:"conversation_id"`
	ExpiresAt      *int64 `json:"expires_at"`
	MaxUses        *int   `json:"max_uses"`
}

func (s *convService) createInvite(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convInviteCreatePayload
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) || p.ConversationID == "" {
		return convFail(convErrInvalidRequest)
	}
	canon := map[string]any{"conversation_id": p.ConversationID}
	if p.ExpiresAt != nil {
		canon["expires_at"] = *p.ExpiresAt
	}
	if p.MaxUses != nil {
		canon["max_uses"] = *p.MaxUses
	}
	spec := convChangeSpec{action: ActionConversationInviteCreate, requestID: p.RequestID, conversationID: p.ConversationID,
		hash: convHash(canon), permission: "members.add"}
	return s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
		// Содержание — после права (ADR-099 §11).
		now := s.now()
		if (p.MaxUses != nil && *p.MaxUses < 1) || (p.ExpiresAt != nil && *p.ExpiresAt <= now) {
			return nil, false, convErrInvalidRequest, nil
		}
		token, err := newInviteToken()
		if err != nil {
			return nil, false, "", err
		}
		id, err := newInviteID()
		if err != nil {
			return nil, false, "", err
		}
		if _, err := c.tx.ExecContext(ctx, `INSERT INTO engram_invite (invite_id, conversation_id, token_hash, created_by, created_at, expires_at, max_uses)
VALUES ($1, $2, $3, $4, $5, $6, $7)`, id, c.convID, inviteTokenHash(token), c.callerPID, now, p.ExpiresAt, p.MaxUses); err != nil {
			return nil, false, "", err
		}
		// Состав разговора не меняется: без события и версии.
		return map[string]any{"invite": ConvInvite{InviteID: id, Token: token, ExpiresAt: p.ExpiresAt, MaxUses: p.MaxUses}}, false, "", nil
	})
}

// ── 914 INVITE_REVOKE ────────────────────────────────────────────────────────

type convInviteRevokePayload struct {
	RequestID string `json:"request_id"`
	InviteID  string `json:"invite_id"`
}

func (s *convService) revokeInvite(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convInviteRevokePayload
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) || p.InviteID == "" {
		return convFail(convErrInvalidRequest)
	}
	// Разговор для блокировки — по самому приглашению.
	var convID string
	err := s.db.QueryRowContext(ctx, `SELECT conversation_id FROM engram_invite WHERE invite_id = $1`, p.InviteID).Scan(&convID)
	if errors.Is(err, sql.ErrNoRows) {
		return convFail(convErrConversationNF)
	}
	if err != nil {
		return s.convFailFor("914 lookup", err)
	}
	spec := convChangeSpec{action: ActionConversationInviteRevoke, requestID: p.RequestID, conversationID: convID,
		hash: convHash(map[string]any{"invite_id": p.InviteID}), permission: "members.add"}
	return s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
		if _, err := c.tx.ExecContext(ctx, `UPDATE engram_invite SET revoked_at = $2 WHERE invite_id = $1 AND revoked_at IS NULL`, p.InviteID, s.now()); err != nil {
			return nil, false, "", err
		}
		return map[string]any{"invite_id": p.InviteID, "revoked": true}, false, "", nil
	})
}

// ── 915 INVITE_LIST ──────────────────────────────────────────────────────────

type convInviteListPayload struct {
	ConversationID string `json:"conversation_id"`
}

func (s *convService) listInvites(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convInviteListPayload
	if !decodeStrict(raw, &p) || p.ConversationID == "" {
		return convFail(convErrInvalidRequest)
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("915 caller", err)
	}
	if !found {
		return convFail(convErrConversationNF)
	}
	releaseS, err := s.holdSharedLock(ctx, convLockKey(p.ConversationID))
	if err != nil {
		return s.convFailFor("915 lock", err)
	}
	defer releaseS()
	code, err := callerAccess(ctx, s.db, p.ConversationID, pid)
	if err != nil {
		return s.convFailFor("915 access", err)
	}
	if code != "" {
		return convFail(code)
	}
	var role, kind string
	if err := s.db.QueryRowContext(ctx, `SELECT p.role, c.kind FROM engram_member_period p
JOIN engram_conversation c ON c.conversation_id = p.conversation_id
WHERE p.conversation_id = $1 AND p.photon_id = $2 AND p.state = 'active'`, p.ConversationID, pid).Scan(&role, &kind); err != nil {
		return s.convFailFor("915 role", err)
	}
	if kind == "direct" {
		return convFail(convErrInvalidRequest)
	}
	if !convHasPermission(role, "members.add") {
		return convFail(convErrForbidden)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT invite_id, expires_at, max_uses, uses FROM engram_invite
WHERE conversation_id = $1 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > $2)
AND (max_uses IS NULL OR uses < max_uses) ORDER BY created_at, invite_id`, p.ConversationID, s.now())
	if err != nil {
		return s.convFailFor("915 list", err)
	}
	defer rows.Close()
	out := []ConvInvite{}
	for rows.Next() {
		var iv ConvInvite
		var exp sql.NullInt64
		var mu sql.NullInt64
		if err := rows.Scan(&iv.InviteID, &exp, &mu, &iv.Uses); err != nil {
			return s.convFailFor("915 scan", err)
		}
		if exp.Valid {
			v := exp.Int64
			iv.ExpiresAt = &v
		}
		if mu.Valid {
			v := int(mu.Int64)
			iv.MaxUses = &v
		}
		out = append(out, iv)
	}
	if err := rows.Err(); err != nil {
		return s.convFailFor("915 rows", err)
	}
	return Response{OK: true, Data: map[string]any{"invites": out}}
}

// ── 916 JOIN_BY_INVITE ───────────────────────────────────────────────────────

type convJoinPayload struct {
	RequestID string `json:"request_id"`
	Token     string `json:"token"`
}

func (s *convService) joinByInvite(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convJoinPayload
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) || p.Token == "" {
		return convFail(convErrInvalidRequest)
	}
	// Предел — до любой работы с БД.
	if !s.joinLimiter().allow(callerID, s.now()) {
		return convFail(convErrTemporarilyUnavail)
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("916 caller", err)
	}
	if !found {
		return convFail(convErrConversationNF)
	}
	hash := inviteTokenHash(p.Token)
	reqHash := convHash(map[string]any{"token_hash": hex.EncodeToString(hash)})

	// Повтор по §10.2 — первым: выполненный запрос отвечает сохранённым,
	// даже если приглашение уже отозвано или исчерпано.
	if data, code, done, err := s.joinStoredKey(ctx, s.db, pid, p.RequestID, reqHash); err != nil {
		return s.convFailFor("916 key", err)
	} else if code != "" {
		return convFail(code)
	} else if done {
		return Response{OK: true, Data: data}
	}

	var convID string
	err = s.db.QueryRowContext(ctx, `SELECT conversation_id FROM engram_invite WHERE token_hash = $1`, hash).Scan(&convID)
	if errors.Is(err, sql.ErrNoRows) {
		return convFail(convErrInviteInvalid)
	}
	if err != nil {
		return s.convFailFor("916 lookup", err)
	}
	release, err := s.holdLock(ctx, convLockKey(convID))
	if err != nil {
		return s.convFailFor("916 lock", err)
	}
	defer release()

	data, failCode, err := s.joinTx(ctx, pid, p.RequestID, reqHash, hash)
	if err != nil {
		return s.convFailFor("916", err)
	}
	if failCode != "" {
		return convFail(failCode)
	}
	if err := s.flushEvents(ctx, convID, map[string]string{pid: callerID}); err != nil {
		return s.convFailFor("916 events", err)
	}
	return Response{OK: true, Data: data}
}

// joinStoredKey читает ключ §10.2: done — сохранённый ответ; code —
// idempotency_conflict; ни того ни другого — ключа нет.
func (s *convService) joinStoredKey(ctx context.Context, q convQuerier, pid, requestID string, reqHash []byte) (json.RawMessage, string, bool, error) {
	var gotHash, response []byte
	var state string
	err := q.QueryRowContext(ctx, `SELECT payload_hash, state, response FROM engram_request
WHERE photon_id = $1 AND action = $2 AND request_id = $3`, pid, ActionConversationJoinByInvite, requestID).Scan(&gotHash, &state, &response)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", false, nil
	}
	if err != nil {
		return nil, "", false, err
	}
	if string(gotHash) != string(reqHash) {
		return nil, convErrIdempotency, false, nil
	}
	if state != "done" {
		return nil, "", false, fmt.Errorf("%w: ключ в состоянии %s", errConvUnavailable, state)
	}
	return json.RawMessage(response), "", true, nil
}

func (s *convService) joinTx(ctx context.Context, pid, requestID string, reqHash, tokenHash []byte) (json.RawMessage, string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, "", err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `INSERT INTO engram_request (photon_id, action, request_id, payload_hash, conversation_id, state, created_at)
VALUES ($1, $2, $3, $4, NULL, 'pending', $5) ON CONFLICT DO NOTHING`, pid, ActionConversationJoinByInvite, requestID, reqHash, s.now())
	if err != nil {
		return nil, "", err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		data, code, done, err := s.joinStoredKey(ctx, tx, pid, requestID, reqHash)
		if err != nil || code != "" {
			return nil, code, err
		}
		if !done {
			return nil, "", errConvUnavailable
		}
		return data, "", nil
	}

	// Повторное чтение приглашения под X разговора и FOR UPDATE.
	var inviteID, convID string
	var expires, maxUses sql.NullInt64
	var uses int
	var revoked sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT invite_id, conversation_id, expires_at, max_uses, uses, revoked_at FROM engram_invite
WHERE token_hash = $1 FOR UPDATE`, tokenHash).Scan(&inviteID, &convID, &expires, &maxUses, &uses, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, convErrInviteInvalid, nil
	}
	if err != nil {
		return nil, "", err
	}
	if revoked.Valid || (expires.Valid && expires.Int64 <= s.now()) || (maxUses.Valid && int64(uses) >= maxUses.Int64) {
		return nil, convErrInviteInvalid, nil
	}

	c := &convChange{tx: tx, convID: convID, callerPID: pid}
	var state string
	if err := tx.QueryRowContext(ctx, `SELECT state, kind, version, member_count, position_h FROM engram_conversation
WHERE conversation_id = $1 FOR UPDATE`, convID).Scan(&state, &c.kind, &c.version, &c.memberCount, &c.h); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, convErrInviteInvalid, nil
		}
		return nil, "", err
	}
	if state == "creating" {
		return nil, convErrInviteInvalid, nil
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
		at := c.addEvent(convEventBody{Event: "member_joined", ActorPhotonID: pid, InviteID: inviteID})
		// Новый период открывает его member_joined (ADR-099 §9.4).
		if _, err := tx.ExecContext(ctx, `INSERT INTO engram_member_period (conversation_id, photon_id, period_no, role,
state, opened_version, open_at)
SELECT $1, $2, COALESCE(max(period_no), 0) + 1, 'member', 'active', $3, $4
FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2`, convID, pid, c.nextVersion, at); err != nil {
			return nil, "", err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE engram_invite SET uses = uses + 1 WHERE invite_id = $1`, inviteID); err != nil {
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
WHERE photon_id = $1 AND action = $2 AND request_id = $3`, pid, ActionConversationJoinByInvite, requestID, body, convID); err != nil {
		return nil, "", err
	}
	if err := tx.Commit(); err != nil {
		return nil, "", err
	}
	if firstAt != 0 {
		s.observePositionLead("916", convID, firstAt, clock)
	}
	return body, "", nil
}
