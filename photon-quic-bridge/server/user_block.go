package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// Личные блокировки пользователей, действия 220–222 (ADR-111 §2.1–2.2;
// contract messaging-v2 §5b.4). Таблица пар «блокирующий → заблокированный»
// лежит в той же базе, что и таблицы разговоров (схема v10); блокировка видна
// только блокирующему, а заблокированный отличить её от отсутствия разговора
// не может.

// convErrBlocked — ответ блокирующему на 930 в direct (ADR-111 Q2): он знает,
// что заблокировал сам, поэтому код отличим от conversation_not_found.
const convErrBlocked = "blocked"

type convUserBlockPayload struct {
	PhotonID string `json:"photon_id"`
}

// ConvUserBlockResponse — ответ 220/221.
type ConvUserBlockResponse struct {
	Blocked bool `json:"blocked"`
}

// ConvUserBlockListResponse — ответ 222.
type ConvUserBlockListResponse struct {
	Blocked []string `json:"blocked"`
}

// setUserBlock — 220 (block=true) и 221 (block=false). Идемпотентны без
// ключа: повтор ставит или снимает то же состояние.
func (s *convService) setUserBlock(ctx context.Context, callerID string, raw json.RawMessage, block bool) Response {
	var p convUserBlockPayload
	if !decodeStrict(raw, &p) || !isCanonicalPhotonID(p.PhotonID) {
		return convFail(errInvalidPhotonID)
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("220/221 caller", err)
	}
	if !found {
		return convFail(convErrForbidden) // как 905: без PhotonID в ENGRAM участвовать нельзя
	}
	if p.PhotonID == pid {
		return convFail(convErrInvalidRequest)
	}
	// Как 212: незарегистрированный и деактивированный — photon_id_not_found.
	if _, code, err := s.resolveExisting([]string{p.PhotonID}); err != nil {
		return s.convFailFor("220/221 target", err)
	} else if code != "" {
		return convFail(code)
	}
	if block {
		_, err = s.db.ExecContext(ctx, `INSERT INTO engram_user_block (blocker_pid, blocked_pid, created_at)
VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, pid, p.PhotonID, s.now())
	} else {
		_, err = s.db.ExecContext(ctx, `DELETE FROM engram_user_block WHERE blocker_pid = $1 AND blocked_pid = $2`, pid, p.PhotonID)
	}
	if err != nil {
		return s.convFailFor("220/221 write", err)
	}
	return Response{OK: true, Data: ConvUserBlockResponse{Blocked: block}}
}

// listUserBlocks — 222: PhotonID, заблокированные вызывающим, в порядке
// постановки блокировки.
func (s *convService) listUserBlocks(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p struct{}
	if !decodeStrict(raw, &p) {
		return convFail(convErrInvalidRequest)
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("222 caller", err)
	}
	if !found {
		return convFail(convErrForbidden)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT blocked_pid FROM engram_user_block WHERE blocker_pid = $1
ORDER BY created_at, blocked_pid`, pid)
	if err != nil {
		return s.convFailFor("222 read", err)
	}
	defer rows.Close()
	out := ConvUserBlockListResponse{Blocked: []string{}}
	for rows.Next() {
		var b string
		if err := rows.Scan(&b); err != nil {
			return s.convFailFor("222 scan", err)
		}
		out.Blocked = append(out.Blocked, b)
	}
	if err := rows.Err(); err != nil {
		return s.convFailFor("222 rows", err)
	}
	return Response{OK: true, Data: out}
}

// userBlocked — есть ли блокировка blocker → blocked.
func userBlocked(ctx context.Context, q convQuerier, blocker, blocked string) (bool, error) {
	var one int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM engram_user_block WHERE blocker_pid = $1 AND blocked_pid = $2`, blocker, blocked).Scan(&one)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

// directBlockState — для direct-разговора: заблокировал ли собеседник
// вызывающего (peerBlockedMe) и заблокировал ли вызывающий собеседника
// (iBlockedPeer). Не direct (нет строки пары) — обе false.
func directBlockState(ctx context.Context, q convQuerier, convID, pid string) (peerBlockedMe, iBlockedPeer bool, err error) {
	err = q.QueryRowContext(ctx, `SELECT
  EXISTS (SELECT 1 FROM engram_user_block b WHERE b.blocker_pid = p.peer AND b.blocked_pid = $2),
  EXISTS (SELECT 1 FROM engram_user_block b WHERE b.blocker_pid = $2 AND b.blocked_pid = p.peer)
FROM (SELECT CASE WHEN low_pid = $2 THEN high_pid ELSE low_pid END AS peer
      FROM engram_direct_pair WHERE conversation_id = $1) p`, convID, pid).Scan(&peerBlockedMe, &iBlockedPeer)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	return
}
