package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
)

// 905 OPEN_DIRECT — ADR-104 §3.2; messaging-v2-contract.md §2.

type convOpenDirectPayload struct {
	RequestID    string `json:"request_id"`
	PeerPhotonID string `json:"peer_photon_id"`
}

// ConvOpenDirectResponse — ответ 905 (ADR-104 §3.2).
type ConvOpenDirectResponse struct {
	Conversation ConvSnapshot `json:"conversation"`
	Created      bool         `json:"created"`
}

// directPairKey — пара в каноническом порядке: low < high.
func directPairKey(a, b string) (low, high string) {
	if a < b {
		return a, b
	}
	return b, a
}

func directPairLockKey(low, high string) int64 {
	return advisoryKey("photon:direct:" + low + ":" + high)
}

// openDirect — 905. Идемпотентность — по паре, а не по request_id: повтор и
// встречный вызов дают один conversation_id (contract §2). Поиск и вставка
// пары — под транзакционной advisory-блокировкой ключа пары, вставка пары — в
// одной транзакции со строкой разговора.
func (s *convService) openDirect(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convOpenDirectPayload
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) {
		return convFail(convErrInvalidRequest)
	}
	if !isCanonicalPhotonID(p.PeerPhotonID) {
		return convFail(errInvalidPhotonID)
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("905 caller", err)
	}
	if !found {
		return convFail(convErrForbidden) // как 900: без PhotonID в ENGRAM участвовать нельзя
	}
	if p.PeerPhotonID == pid {
		return convFail(convErrInvalidRequest)
	}
	if _, code, err := s.resolveExisting([]string{p.PeerPhotonID}); err != nil {
		return s.convFailFor("905 peer", err)
	} else if code != "" {
		return convFail(code)
	}
	if s.botID == "" {
		return s.convFailFor("905", errConvNoBot)
	}

	convID, created, err := s.reserveDirect(ctx, pid, p)
	if err != nil {
		return s.convFailFor("905 T1", err)
	}

	// Завершение — как у 900, повторно входимо: разговор мог остаться
	// creating после сбоя первого вызова (в том числе чужого).
	var createdBy, lowPID, highPID string
	if err := s.db.QueryRowContext(ctx, `SELECT c.created_by, d.low_pid, d.high_pid FROM engram_conversation c
JOIN engram_direct_pair d ON d.conversation_id = c.conversation_id WHERE c.conversation_id = $1`, convID).
		Scan(&createdBy, &lowPID, &highPID); err != nil {
		return s.convFailFor("905 pair", err)
	}
	other := lowPID
	if createdBy == lowPID {
		other = highPID
	}
	if err := s.completeCarrier(ctx, callerID, createdBy, convID, []string{other}); err != nil {
		return s.convFailFor("905", err)
	}
	snap, err := snapshotFor(ctx, s.db, convID, pid)
	if err != nil {
		return s.convFailFor("905 snapshot", err)
	}
	body, err := json.Marshal(ConvOpenDirectResponse{Conversation: snap.Conversation, Created: created})
	if err != nil {
		return s.convFailFor("905", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE engram_request SET state = 'done', response = $4
WHERE photon_id = $1 AND action = $2 AND request_id = $3`, pid, ActionConversationOpenDirect, p.RequestID, body); err != nil {
		return s.convFailFor("905 T3", err)
	}
	return Response{OK: true, Data: json.RawMessage(body)}
}

// reserveDirect — T1: под блокировкой пары найти разговор или зарезервировать
// новый вместе со строкой пары. created — разговор создан этим вызовом.
func (s *convService) reserveDirect(ctx context.Context, pid string, p convOpenDirectPayload) (string, bool, error) {
	for attempt := 0; attempt < convMoveCarrierMax; attempt++ {
		if err := s.ensureCarrierTeam(ctx); err != nil {
			return "", false, err
		}
		capacity, unlimited, err := s.carrierCapacity()
		if err != nil {
			return "", false, err
		}
		convID, created, err := s.reserveDirectTx(ctx, pid, p, capacity, unlimited)
		if err != nil {
			return "", false, err
		}
		if convID != "" {
			return convID, created, nil
		}
		// Команда с местом ушла между ensure и транзакцией — ещё раз.
	}
	return "", false, fmt.Errorf("%w: нет служебной команды с местом", errConvUnavailable)
}

func (s *convService) reserveDirectTx(ctx context.Context, pid string, p convOpenDirectPayload, capacity int64, unlimited bool) (string, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()

	low, high := directPairKey(pid, p.PeerPhotonID)
	if err := lockTxAdvisory(ctx, tx, directPairLockKey(low, high), s.lockWait, s.pollEvery); err != nil {
		return "", false, err
	}
	now := s.now()
	hash := convHash(map[string]any{"peer_photon_id": p.PeerPhotonID})

	var convID string
	err = tx.QueryRowContext(ctx, `SELECT conversation_id FROM engram_direct_pair WHERE low_pid = $1 AND high_pid = $2`, low, high).Scan(&convID)
	switch {
	case err == nil:
		// request_id записывается (ADR-099 §10.2), на результат не влияет.
		if err := insertDirectRequest(ctx, tx, pid, p.RequestID, hash, convID, now); err != nil {
			return "", false, err
		}
		return convID, false, tx.Commit()
	case !errors.Is(err, sql.ErrNoRows):
		return "", false, err
	}

	team, err := pickCarrierTeam(ctx, tx, capacity, unlimited)
	if err != nil || team == "" {
		return "", false, err
	}
	if err := takeCarrierSlot(ctx, tx, team, capacity, unlimited); err != nil {
		return "", false, err
	}
	convID = model.NewId()
	if _, err := tx.ExecContext(ctx, `INSERT INTO engram_conversation (conversation_id, kind, state, title, description,
history_policy, version, created_at, created_by, member_count, last_activity_at, carrier_state, carrier_team,
carrier_name, reserved_at) VALUES ($1, 'direct', 'creating', '', '', 'full', 0, $2, $3, 0, 0, 'reserved', $4, $5, $2)`,
		convID, now, pid, team, carrierChannelPrefix+convID); err != nil {
		return "", false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO engram_direct_pair (low_pid, high_pid, conversation_id) VALUES ($1, $2, $3)`,
		low, high, convID); err != nil {
		return "", false, err
	}
	if err := insertDirectRequest(ctx, tx, pid, p.RequestID, hash, convID, now); err != nil {
		return "", false, err
	}
	return convID, true, tx.Commit()
}

func insertDirectRequest(ctx context.Context, tx *sql.Tx, pid, requestID string, hash []byte, convID string, now int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO engram_request (photon_id, action, request_id, payload_hash, conversation_id, state, created_at)
VALUES ($1, $2, $3, $4, $5, 'pending', $6) ON CONFLICT DO NOTHING`, pid, ActionConversationOpenDirect, requestID, hash, convID, now)
	return err
}

// lockTxAdvisory — транзакционная advisory-блокировка внутри уже открытой
// транзакции, попыткой с опросом в пределах бюджета (как holdAdvisory).
func lockTxAdvisory(ctx context.Context, tx *sql.Tx, key int64, wait, pollEvery time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		var got bool
		if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock($1)`, key).Scan(&got); err != nil {
			return err
		}
		if got {
			return nil
		}
		if time.Now().After(deadline) {
			return errConvLockTimeout
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollEvery):
		}
	}
}
