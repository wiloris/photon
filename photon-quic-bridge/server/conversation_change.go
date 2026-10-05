package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// Изменения 903, 904, 910, 911, 912, 920, 921 — ADR-099 §8, §10;
// conversation-storage.md §6.2. Под X(разговор), одной транзакцией: ключ
// идемпотентности, разговор FOR UPDATE, проверки §11, expected_version,
// изменение, version + 1, события с позициями из H и их резервы
// (ADR-101 §5.1, §5.2), ключ done. Затем E2–E4 и снятие X; ответ — после
// записи событий.

const convErrVersionConflict = "version_conflict"
const convErrOwnerTransferRequired = "owner_transfer_required"
const convErrArchived = "conversation_archived"

// convChange — состояние разговора для функции изменения.
type convChange struct {
	tx          *sql.Tx
	convID      string
	callerPID   string
	callerRole  string
	version     int64 // текущая
	nextVersion int64 // версия этого изменения
	memberCount int
	h           int64 // H на входе
	nextPos     int64 // следующая позиция для события
	events      []convEventBody
	ats         []int64
}

// addEvent — событие изменения: позиция из H (P, P+1, … в порядке событий).
func (c *convChange) addEvent(ev convEventBody) int64 {
	ev.Version = c.nextVersion
	at := c.nextPos
	c.nextPos++
	c.events = append(c.events, ev)
	c.ats = append(c.ats, at)
	return at
}

// convChangeFn — само изменение. changed=false — «нет изменения»: без события
// и без роста версии, ключ всё равно записывается (§6.2). data строится
// функцией и сохраняется в ключе.
type convChangeFn func(ctx context.Context, c *convChange) (data any, changed bool, code string, err error)

type convChangeSpec struct {
	action          int
	requestID       string
	conversationID  string
	hash            []byte
	permission      string // "" — право не нужно (912)
	expectedVersion *int64
}

var convRoleRank = map[string]int{"owner": 3, "admin": 2, "member": 1}

// runChange — общий исполнитель изменений.
func (s *convService) runChange(ctx context.Context, callerID string, spec convChangeSpec, fn convChangeFn) Response {
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("change caller", err)
	}
	if !found {
		return convFail(convErrConversationNF)
	}
	release, err := s.holdLock(ctx, convLockKey(spec.conversationID))
	if err != nil {
		return s.convFailFor("change lock", err)
	}
	defer release()

	data, failCode, failData, err := s.changeTx(ctx, pid, spec, fn)
	if err != nil {
		return s.convFailFor(fmt.Sprintf("%d", spec.action), err)
	}
	if failCode != "" {
		return Response{OK: false, Error: failCode, Data: failData}
	}
	// E2–E4 под X; ответ — после записи (§6.2). Повтор выполненного запроса
	// тоже дописывает то, что осталось незаписанным после сбоя.
	if err := s.flushEvents(ctx, spec.conversationID, map[string]string{pid: callerID}); err != nil {
		return s.convFailFor("change events", err)
	}
	return Response{OK: true, Data: data}
}

// changeTx — транзакция изменения. Возвращает сохранённый ответ (data),
// код отказа (с данными для version_conflict) или ошибку.
func (s *convService) changeTx(ctx context.Context, pid string, spec convChangeSpec, fn convChangeFn) (json.RawMessage, string, any, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, "", nil, err
	}
	defer tx.Rollback()

	// Повтор по §10.2 — первым, независимо от состояния и доступа.
	res, err := tx.ExecContext(ctx, `INSERT INTO engram_request (photon_id, action, request_id, payload_hash, conversation_id, state, created_at)
VALUES ($1, $2, $3, $4, $5, 'pending', $6) ON CONFLICT DO NOTHING`, pid, spec.action, spec.requestID, spec.hash, spec.conversationID, s.now())
	if err != nil {
		return nil, "", nil, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		var gotHash, response []byte
		var state string
		if err := tx.QueryRowContext(ctx, `SELECT payload_hash, state, response FROM engram_request
WHERE photon_id = $1 AND action = $2 AND request_id = $3`, pid, spec.action, spec.requestID).Scan(&gotHash, &state, &response); err != nil {
			return nil, "", nil, err
		}
		if string(gotHash) != string(spec.hash) {
			return nil, convErrIdempotency, nil, nil
		}
		if state != "done" {
			return nil, "", nil, fmt.Errorf("%w: ключ в состоянии %s", errConvUnavailable, state)
		}
		return json.RawMessage(response), "", nil, nil
	}

	c := &convChange{tx: tx, convID: spec.conversationID, callerPID: pid}
	var state string
	err = tx.QueryRowContext(ctx, `SELECT state, version, member_count, position_h FROM engram_conversation
WHERE conversation_id = $1 FOR UPDATE`, spec.conversationID).Scan(&state, &c.version, &c.memberCount, &c.h)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && state == "creating") {
		return nil, convErrConversationNF, nil, nil
	}
	if err != nil {
		return nil, "", nil, err
	}
	// Существование и участие → состояние → право (ADR-099 §11).
	var periodState string
	err = tx.QueryRowContext(ctx, `SELECT state, role FROM engram_member_period
WHERE conversation_id = $1 AND photon_id = $2 ORDER BY period_no DESC LIMIT 1`, spec.conversationID, pid).Scan(&periodState, &c.callerRole)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, convErrConversationNF, nil, nil
	}
	if err != nil {
		return nil, "", nil, err
	}
	if periodState != "active" {
		return nil, convErrNoAccess, nil, nil
	}
	if state == "archived" {
		return nil, convErrArchived, nil, nil
	}
	if spec.permission != "" && !convHasPermission(c.callerRole, spec.permission) {
		return nil, convErrForbidden, nil, nil
	}
	if spec.expectedVersion != nil && *spec.expectedVersion != c.version {
		return nil, convErrVersionConflict, map[string]int64{"conversation_version": c.version}, nil
	}

	c.nextVersion = c.version + 1
	// P = max(часы Bridge, H + 1) (ADR-101 §5.1).
	clock := s.now()
	c.nextPos = clock
	if c.h+1 > c.nextPos {
		c.nextPos = c.h + 1
	}

	data, changed, code, err := fn(ctx, c)
	if err != nil {
		return nil, "", nil, err
	}
	if code != "" {
		return nil, code, nil, nil
	}
	if changed {
		for i, ev := range c.events {
			body, err := json.Marshal(ev)
			if err != nil {
				return nil, "", nil, err
			}
			key := fmt.Sprintf("%s:%d:%d", c.convID, c.nextVersion, i)
			if _, err := tx.ExecContext(ctx, `INSERT INTO engram_event (conversation_id, version, seq, event, at, state)
VALUES ($1, $2, $3, $4, $5, 'pending')`, c.convID, c.nextVersion, i, body, c.ats[i]); err != nil {
				return nil, "", nil, err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, reserved_at)
VALUES ($1, $2, 'event', $3, 'reserved', $4)`, c.convID, c.ats[i], key, s.now()); err != nil {
				return nil, "", nil, err
			}
		}
		newH := c.h
		if len(c.ats) > 0 {
			newH = c.ats[len(c.ats)-1]
		}
		if _, err := tx.ExecContext(ctx, `UPDATE engram_conversation SET version = $2, position_h = $3, member_count = $4
WHERE conversation_id = $1`, c.convID, c.nextVersion, newH, c.memberCount); err != nil {
			return nil, "", nil, err
		}
	}
	// Снимок для 903/904 строится внутри транзакции — после изменения.
	if _, ok := data.(convSnapshotData); ok {
		snap, err := snapshotFor(ctx, tx, c.convID, pid)
		if err != nil {
			return nil, "", nil, err
		}
		data = map[string]any{"conversation": snap.Conversation}
	}
	body, err := json.Marshal(data)
	if err != nil {
		return nil, "", nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE engram_request SET state = 'done', response = $4
WHERE photon_id = $1 AND action = $2 AND request_id = $3`, pid, spec.action, spec.requestID, body); err != nil {
		return nil, "", nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, "", nil, err
	}
	if changed && len(c.ats) > 0 {
		s.observePositionLead(fmt.Sprint(spec.action), c.convID, c.ats[0], clock)
	}
	return body, "", nil, nil
}

// convSnapshotData — маркер: ответ операции — {conversation: Snapshot}.
type convSnapshotData struct{}

func convHasPermission(role, perm string) bool {
	for _, p := range convPermissions[role] {
		if p == perm {
			return true
		}
	}
	return false
}

// flushEvents — незаписанные события разговора (E2–E4), если носитель
// привязан.
func (s *convService) flushEvents(ctx context.Context, convID string, actorMM map[string]string) error {
	var channelID sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT carrier_channel_id FROM engram_conversation WHERE conversation_id = $1`, convID).Scan(&channelID)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !channelID.Valid) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.writePendingEvents(ctx, convID, channelID.String, actorMM)
}

// convHash — каноническое содержание (ADR-099 §10.2): JSON с отсортированными
// ключами; списки PhotonID приводятся к множеству вызывающим.
func convHash(canon map[string]any) []byte {
	b, _ := json.Marshal(canon)
	h := sha256.Sum256(b)
	return h[:]
}

func photonSet(ids []string) []string {
	m := map[string]bool{}
	for _, id := range ids {
		m[id] = true
	}
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// activePeriod — роль участника в периоде active; "" — не в active.
func activePeriod(ctx context.Context, tx *sql.Tx, convID, pid string) (string, error) {
	var role string
	err := tx.QueryRowContext(ctx, `SELECT role FROM engram_member_period
WHERE conversation_id = $1 AND photon_id = $2 AND state = 'active'`, convID, pid).Scan(&role)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return role, err
}

// closePeriod — закрытие активного периода (left/removed) с позицией
// закрывающего события.
func closePeriod(ctx context.Context, c *convChange, pid, state string, at int64) error {
	_, err := c.tx.ExecContext(ctx, `UPDATE engram_member_period SET state = $3, closed_version = $4, close_at = $5
WHERE conversation_id = $1 AND photon_id = $2 AND state = 'active'`, c.convID, pid, state, c.nextVersion, at)
	return err
}

// ── 903 UPDATE ───────────────────────────────────────────────────────────────

type convUpdatePayload struct {
	RequestID       string  `json:"request_id"`
	ConversationID  string  `json:"conversation_id"`
	ExpectedVersion *int64  `json:"expected_version"`
	Title           *string `json:"title"`
	Description     *string `json:"description"`
}

func (s *convService) updateConversation(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convUpdatePayload
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) || p.ConversationID == "" {
		return convFail(convErrInvalidRequest)
	}
	if p.Title != nil && (strings.TrimSpace(*p.Title) == "" || utf8.RuneCountInString(*p.Title) > convTitleMaxRunes) {
		return convFail(convErrInvalidRequest)
	}
	if p.Description != nil && utf8.RuneCountInString(*p.Description) > convDescriptionMaxRunes {
		return convFail(convErrInvalidRequest)
	}
	canon := map[string]any{"conversation_id": p.ConversationID}
	if p.ExpectedVersion != nil {
		canon["expected_version"] = *p.ExpectedVersion
	}
	if p.Title != nil {
		canon["title"] = *p.Title
	}
	if p.Description != nil {
		canon["description"] = *p.Description
	}
	spec := convChangeSpec{action: ActionConversationUpdate, requestID: p.RequestID, conversationID: p.ConversationID,
		hash: convHash(canon), permission: "conversation.update", expectedVersion: p.ExpectedVersion}
	return s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
		var title, desc string
		if err := c.tx.QueryRowContext(ctx, `SELECT title, description FROM engram_conversation WHERE conversation_id = $1`, c.convID).Scan(&title, &desc); err != nil {
			return nil, false, "", err
		}
		ev := convEventBody{Event: "metadata_changed", ActorPhotonID: c.callerPID}
		changed := false
		if p.Title != nil && *p.Title != title {
			title, ev.Title, changed = *p.Title, *p.Title, true
		}
		if p.Description != nil && *p.Description != desc {
			desc, ev.Description, changed = *p.Description, *p.Description, true
		}
		if !changed {
			return convSnapshotData{}, false, "", nil
		}
		if _, err := c.tx.ExecContext(ctx, `UPDATE engram_conversation SET title = $2, description = $3 WHERE conversation_id = $1`, c.convID, title, desc); err != nil {
			return nil, false, "", err
		}
		c.addEvent(ev)
		return convSnapshotData{}, true, "", nil
	})
}

// ── 904 ARCHIVE ──────────────────────────────────────────────────────────────

type convRequestOnly struct {
	RequestID      string `json:"request_id"`
	ConversationID string `json:"conversation_id"`
}

func (s *convService) archiveConversation(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convRequestOnly
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) || p.ConversationID == "" {
		return convFail(convErrInvalidRequest)
	}
	spec := convChangeSpec{action: ActionConversationArchive, requestID: p.RequestID, conversationID: p.ConversationID,
		hash: convHash(map[string]any{"conversation_id": p.ConversationID}), permission: "conversation.archive"}
	return s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
		if _, err := c.tx.ExecContext(ctx, `UPDATE engram_conversation SET state = 'archived' WHERE conversation_id = $1`, c.convID); err != nil {
			return nil, false, "", err
		}
		c.addEvent(convEventBody{Event: "archived", ActorPhotonID: c.callerPID})
		return convSnapshotData{}, true, "", nil
	})
}

// ── 910 MEMBERS_ADD ──────────────────────────────────────────────────────────

type convMembersAddPayload struct {
	RequestID      string    `json:"request_id"`
	ConversationID string    `json:"conversation_id"`
	PhotonIDs      *[]string `json:"photon_ids"`
}

type ConvMembersAddResponse struct {
	Added               []string `json:"added"`
	Already             []string `json:"already"`
	ConversationVersion int64    `json:"conversation_version"`
}

func (s *convService) addMembers(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convMembersAddPayload
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) || p.ConversationID == "" || p.PhotonIDs == nil || len(*p.PhotonIDs) == 0 {
		return convFail(convErrInvalidRequest)
	}
	spec := convChangeSpec{action: ActionConversationMembersAdd, requestID: p.RequestID, conversationID: p.ConversationID,
		hash: convHash(map[string]any{"conversation_id": p.ConversationID, "photon_ids": photonSet(*p.PhotonIDs)}), permission: "members.add"}
	return s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
		// Содержание — после права (ADR-099 §11).
		ids, code, err := s.resolveExisting(*p.PhotonIDs)
		if err != nil || code != "" {
			return nil, false, code, err
		}
		out := ConvMembersAddResponse{Added: []string{}, Already: []string{}}
		for _, id := range ids {
			role, err := activePeriod(ctx, c.tx, c.convID, id)
			if err != nil {
				return nil, false, "", err
			}
			if role != "" {
				out.Already = append(out.Already, id)
				continue
			}
			at := c.addEvent(convEventBody{Event: "member_added", ActorPhotonID: c.callerPID, TargetPhotonID: id})
			// Новый период открывает его member_added (ADR-099 §9.4).
			if _, err := c.tx.ExecContext(ctx, `INSERT INTO engram_member_period (conversation_id, photon_id, period_no, role,
state, opened_version, open_at)
SELECT $1, $2, COALESCE(max(period_no), 0) + 1, 'member', 'active', $3, $4
FROM engram_member_period WHERE conversation_id = $1 AND photon_id = $2`, c.convID, id, c.nextVersion, at); err != nil {
				return nil, false, "", err
			}
			c.memberCount++
			out.Added = append(out.Added, id)
		}
		if len(out.Added) == 0 {
			out.ConversationVersion = c.version
			return out, false, "", nil
		}
		out.ConversationVersion = c.nextVersion
		return out, true, "", nil
	})
}

// resolveExisting — PhotonID в порядке первого появления без повторов;
// каждый — зарегистрированный активный пользователь ENGRAM.
func (s *convService) resolveExisting(ids []string) ([]string, string, error) {
	for _, id := range ids {
		if !isCanonicalPhotonID(id) {
			return nil, errInvalidPhotonID, nil
		}
	}
	seen := map[string]bool{}
	var out []string
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		rec, found, err := s.registry.FindByPhotonID(id)
		if err != nil {
			return nil, "", err
		}
		if !found {
			return nil, errPhotonIDNotFound, nil
		}
		u, appErr := s.users.GetUser(rec.MMUserID)
		if appErr != nil {
			if appErr.StatusCode == 404 {
				return nil, errPhotonIDNotFound, nil
			}
			return nil, "", appErr
		}
		if u.DeleteAt != 0 {
			return nil, errPhotonIDNotFound, nil
		}
		out = append(out, id)
	}
	return out, "", nil
}

// ── 911 MEMBER_REMOVE ────────────────────────────────────────────────────────

type convTargetPayload struct {
	RequestID       string `json:"request_id"`
	ConversationID  string `json:"conversation_id"`
	ExpectedVersion *int64 `json:"expected_version"`
	PhotonID        string `json:"photon_id"`
	Role            string `json:"role"`
}

type ConvChangedResponse struct {
	Changed             bool  `json:"changed"`
	ConversationVersion int64 `json:"conversation_version"`
}

type ConvVersionResponse struct {
	ConversationVersion int64 `json:"conversation_version"`
}

// parseTarget — строгий разбор; withVersion/withRole — допустимые поля
// операции (лишнее поле — invalid_request).
func parseTarget(raw json.RawMessage, withVersion, withRole bool) (convTargetPayload, bool) {
	var p convTargetPayload
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) || p.ConversationID == "" || p.PhotonID == "" {
		return p, false
	}
	if !withVersion && p.ExpectedVersion != nil {
		return p, false
	}
	if withRole != (p.Role != "") {
		return p, false
	}
	return p, true
}

func (s *convService) removeMember(ctx context.Context, callerID string, raw json.RawMessage) Response {
	p, ok := parseTarget(raw, false, false)
	if !ok {
		return convFail(convErrInvalidRequest)
	}
	spec := convChangeSpec{action: ActionConversationMemberRemove, requestID: p.RequestID, conversationID: p.ConversationID,
		hash: convHash(map[string]any{"conversation_id": p.ConversationID, "photon_id": p.PhotonID}), permission: "members.remove"}
	return s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
		if !isCanonicalPhotonID(p.PhotonID) {
			return nil, false, errInvalidPhotonID, nil
		}
		role, err := activePeriod(ctx, c.tx, c.convID, p.PhotonID)
		if err != nil {
			return nil, false, "", err
		}
		if role == "" {
			return ConvChangedResponse{Changed: false, ConversationVersion: c.version}, false, "", nil
		}
		// owner не удаляется; удалить можно только роль ниже своей (ADR-099 §7.3).
		if role == "owner" || convRoleRank[role] >= convRoleRank[c.callerRole] {
			return nil, false, convErrForbidden, nil
		}
		at := c.addEvent(convEventBody{Event: "member_removed", ActorPhotonID: c.callerPID, TargetPhotonID: p.PhotonID})
		if err := closePeriod(ctx, c, p.PhotonID, "removed", at); err != nil {
			return nil, false, "", err
		}
		c.memberCount--
		return ConvChangedResponse{Changed: true, ConversationVersion: c.nextVersion}, true, "", nil
	})
}

// ── 912 LEAVE ────────────────────────────────────────────────────────────────

func (s *convService) leaveConversation(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convRequestOnly
	if !decodeStrict(raw, &p) || !uuidRe.MatchString(p.RequestID) || p.ConversationID == "" {
		return convFail(convErrInvalidRequest)
	}
	spec := convChangeSpec{action: ActionConversationLeave, requestID: p.RequestID, conversationID: p.ConversationID,
		hash: convHash(map[string]any{"conversation_id": p.ConversationID})}
	return s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
		if c.callerRole == "owner" {
			if c.memberCount > 1 {
				return nil, false, convErrOwnerTransferRequired, nil
			}
			// Единственный участник-владелец: выход и архив одной операцией
			// (ADR-099 §8): member_left и archived.
			at := c.addEvent(convEventBody{Event: "member_left", ActorPhotonID: c.callerPID})
			if err := closePeriod(ctx, c, c.callerPID, "left", at); err != nil {
				return nil, false, "", err
			}
			c.addEvent(convEventBody{Event: "archived", ActorPhotonID: c.callerPID})
			if _, err := c.tx.ExecContext(ctx, `UPDATE engram_conversation SET state = 'archived' WHERE conversation_id = $1`, c.convID); err != nil {
				return nil, false, "", err
			}
			c.memberCount--
			return ConvVersionResponse{ConversationVersion: c.nextVersion}, true, "", nil
		}
		at := c.addEvent(convEventBody{Event: "member_left", ActorPhotonID: c.callerPID})
		if err := closePeriod(ctx, c, c.callerPID, "left", at); err != nil {
			return nil, false, "", err
		}
		c.memberCount--
		return ConvVersionResponse{ConversationVersion: c.nextVersion}, true, "", nil
	})
}

// ── 920 ROLE_SET ─────────────────────────────────────────────────────────────

func (s *convService) setRole(ctx context.Context, callerID string, raw json.RawMessage) Response {
	p, ok := parseTarget(raw, true, true)
	// roles.manage назначает только admin и member (ADR-099 §7.2).
	if !ok || (p.Role != "admin" && p.Role != "member") {
		return convFail(convErrInvalidRequest)
	}
	canon := map[string]any{"conversation_id": p.ConversationID, "photon_id": p.PhotonID, "role": p.Role}
	if p.ExpectedVersion != nil {
		canon["expected_version"] = *p.ExpectedVersion
	}
	spec := convChangeSpec{action: ActionConversationRoleSet, requestID: p.RequestID, conversationID: p.ConversationID,
		hash: convHash(canon), permission: "roles.manage", expectedVersion: p.ExpectedVersion}
	return s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
		if !isCanonicalPhotonID(p.PhotonID) {
			return nil, false, errInvalidPhotonID, nil
		}
		role, err := activePeriod(ctx, c.tx, c.convID, p.PhotonID)
		if err != nil {
			return nil, false, "", err
		}
		if role == "owner" { // roles.manage никогда не затрагивает owner (ADR-099 §7.2)
			return nil, false, convErrForbidden, nil
		}
		if role == "" || role == p.Role {
			return ConvChangedResponse{Changed: false, ConversationVersion: c.version}, false, "", nil
		}
		if _, err := c.tx.ExecContext(ctx, `UPDATE engram_member_period SET role = $3
WHERE conversation_id = $1 AND photon_id = $2 AND state = 'active'`, c.convID, p.PhotonID, p.Role); err != nil {
			return nil, false, "", err
		}
		c.addEvent(convEventBody{Event: "role_changed", ActorPhotonID: c.callerPID, TargetPhotonID: p.PhotonID, Role: p.Role})
		return ConvChangedResponse{Changed: true, ConversationVersion: c.nextVersion}, true, "", nil
	})
}

// ── 921 OWNER_TRANSFER ───────────────────────────────────────────────────────

func (s *convService) transferOwner(ctx context.Context, callerID string, raw json.RawMessage) Response {
	p, ok := parseTarget(raw, true, false)
	if !ok {
		return convFail(convErrInvalidRequest)
	}
	canon := map[string]any{"conversation_id": p.ConversationID, "photon_id": p.PhotonID}
	if p.ExpectedVersion != nil {
		canon["expected_version"] = *p.ExpectedVersion
	}
	spec := convChangeSpec{action: ActionConversationOwnerTransfer, requestID: p.RequestID, conversationID: p.ConversationID,
		hash: convHash(canon), permission: "owner.transfer", expectedVersion: p.ExpectedVersion}
	return s.runChange(ctx, callerID, spec, func(ctx context.Context, c *convChange) (any, bool, string, error) {
		if !isCanonicalPhotonID(p.PhotonID) {
			return nil, false, errInvalidPhotonID, nil
		}
		if p.PhotonID == c.callerPID {
			return ConvVersionResponse{ConversationVersion: c.version}, false, "", nil
		}
		role, err := activePeriod(ctx, c.tx, c.convID, p.PhotonID)
		if err != nil {
			return nil, false, "", err
		}
		if role == "" { // цель должна быть в периоде active (ADR-099 §8)
			return nil, false, convErrInvalidRequest, nil
		}
		if _, err := c.tx.ExecContext(ctx, `UPDATE engram_member_period SET role = CASE WHEN photon_id = $2 THEN 'owner' ELSE 'admin' END
WHERE conversation_id = $1 AND state = 'active' AND photon_id IN ($2, $3)`, c.convID, p.PhotonID, c.callerPID); err != nil {
			return nil, false, "", err
		}
		// Два role_changed с одной версией (ADR-099 §9.3): новый владелец,
		// затем прежний — admin.
		c.addEvent(convEventBody{Event: "role_changed", ActorPhotonID: c.callerPID, TargetPhotonID: p.PhotonID, Role: "owner"})
		c.addEvent(convEventBody{Event: "role_changed", ActorPhotonID: c.callerPID, TargetPhotonID: c.callerPID, Role: "admin"})
		return ConvVersionResponse{ConversationVersion: c.nextVersion}, true, "", nil
	})
}
