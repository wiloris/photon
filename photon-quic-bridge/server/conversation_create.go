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

	"github.com/mattermost/mattermost/server/public/model"
)

// 900 CREATE — ADR-099 §4, conversation-storage.md §6.1.

// Пределы длины задаёт реализация (ADR-099 §4) и сообщает invalid_request.
const (
	convTitleMaxRunes       = 256
	convDescriptionMaxRunes = 4096
	// convMoveCarrierMax — сколько раз один вызов переносит резерв носителя
	// в другую команду (§6.1 шаг 2), прежде чем ответить temporarily_unavailable.
	convMoveCarrierMax = 4
)

type convCreatePayload struct {
	RequestID       string    `json:"request_id"`
	Kind            string    `json:"kind"`
	Title           string    `json:"title"`
	Description     *string   `json:"description"`
	HistoryPolicy   string    `json:"history_policy"`
	MemberPhotonIDs *[]string `json:"member_photon_ids"`
}

// canonicalCreateHash — каноническое содержание (ADR-099 §10.2): порядок
// полей не важен, список PhotonID — множество, отсутствующее необязательное
// поле — отсутствует.
func canonicalCreateHash(p convCreatePayload) []byte {
	ids := map[string]bool{}
	for _, id := range *p.MemberPhotonIDs {
		ids[id] = true
	}
	set := make([]string, 0, len(ids))
	for id := range ids {
		set = append(set, id)
	}
	sort.Strings(set)
	canon := map[string]any{
		"kind":              p.Kind,
		"title":             p.Title,
		"history_policy":    p.HistoryPolicy,
		"member_photon_ids": set,
	}
	if p.Description != nil {
		canon["description"] = *p.Description
	}
	b, _ := json.Marshal(canon) // ключи map сериализуются отсортированными
	h := sha256.Sum256(b)
	return h[:]
}

// parseCreate — строгий разбор и проверка формы (ADR-099 §3, §4). Отказ —
// invalid_request; проверка PhotonID участников — отдельно (resolveMembers).
func parseCreate(raw json.RawMessage) (convCreatePayload, bool) {
	var p convCreatePayload
	if !decodeStrict(raw, &p) {
		return p, false
	}
	if !uuidRe.MatchString(p.RequestID) || p.Kind != "group" || p.MemberPhotonIDs == nil {
		return p, false
	}
	if strings.TrimSpace(p.Title) == "" || utf8.RuneCountInString(p.Title) > convTitleMaxRunes {
		return p, false
	}
	if p.Description != nil && utf8.RuneCountInString(*p.Description) > convDescriptionMaxRunes {
		return p, false
	}
	if p.HistoryPolicy != "full" && p.HistoryPolicy != "since_join" {
		return p, false
	}
	return p, true
}

// resolveMembers — начальные участники кроме создателя, без повторов, в
// порядке первого появления (ADR-099 §4). Код отказа — invalid_photon_id /
// photon_id_not_found; err — сбой реестра или Mattermost.
func (s *convService) resolveMembers(ids []string, creatorPID string) ([]string, string, error) {
	all, code, err := s.resolveExisting(ids)
	if err != nil || code != "" {
		return nil, code, err
	}
	out := []string{}
	for _, id := range all {
		if id != creatorPID {
			out = append(out, id)
		}
	}
	return out, "", nil
}

// createConversation — 900.
func (s *convService) createConversation(ctx context.Context, callerID string, raw json.RawMessage) Response {
	p, ok := parseCreate(raw)
	if !ok {
		return convFail(convErrInvalidRequest)
	}
	creatorPID, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("900 caller", err)
	}
	if !found {
		return convFail(convErrForbidden) // без PhotonID в ENGRAM участвовать нельзя
	}
	hash := canonicalCreateHash(p)

	// Повтор по §10.2 — первым (ADR-099 §11).
	convID, resp, code, err := s.lookupCreateKey(ctx, creatorPID, p.RequestID, hash)
	if err != nil {
		return s.convFailFor("900 key", err)
	}
	if code != "" {
		return convFail(code)
	}
	if resp != nil {
		return Response{OK: true, Data: resp}
	}
	// Нет бота — 900 недоступен (§6.0.7); повтор выполненного запроса выше
	// возвращает свой результат и без бота.
	if s.botID == "" {
		return s.convFailFor("900", errConvNoBot)
	}

	members, code, err := s.resolveMembers(*p.MemberPhotonIDs, creatorPID)
	if err != nil {
		return s.convFailFor("900 members", err)
	}
	if code != "" {
		return convFail(code)
	}

	if convID == "" {
		// T1 — резерв: ключ, команда, разговор creating с резервом носителя.
		convID, resp, code, err = s.reserveConversation(ctx, creatorPID, p, hash)
		if err != nil {
			return s.convFailFor("900 T1", err)
		}
		if code != "" {
			return convFail(code)
		}
		if resp != nil {
			return Response{OK: true, Data: resp}
		}
	}

	out, err := s.completeCreate(ctx, callerID, creatorPID, convID, p, members)
	if err != nil {
		return s.convFailFor("900", err)
	}
	return Response{OK: true, Data: out}
}

// lookupCreateKey — ключ (вызывающий, 900, request_id). Нет ключа — всё
// пустое. Другое содержание — idempotency_conflict; done — сохранённый ответ;
// pending — conversation_id для продолжения.
func (s *convService) lookupCreateKey(ctx context.Context, pid, requestID string, hash []byte) (string, json.RawMessage, string, error) {
	var gotHash []byte
	var convID sql.NullString
	var state string
	var response []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload_hash, conversation_id, state, response FROM engram_request
WHERE photon_id = $1 AND action = $2 AND request_id = $3`, pid, ActionConversationCreate, requestID).
		Scan(&gotHash, &convID, &state, &response)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, "", nil
	}
	if err != nil {
		return "", nil, "", err
	}
	if string(gotHash) != string(hash) {
		return "", nil, convErrIdempotency, nil
	}
	if state == "done" {
		return convID.String, json.RawMessage(response), "", nil
	}
	return convID.String, nil, "", nil
}

// reserveConversation — T1 (§6.1). При гонке за ключ проигравший получает
// состояние победителя через lookupCreateKey.
func (s *convService) reserveConversation(ctx context.Context, pid string, p convCreatePayload, hash []byte) (string, json.RawMessage, string, error) {
	for attempt := 0; attempt < convMoveCarrierMax; attempt++ {
		if err := s.ensureCarrierTeam(ctx); err != nil {
			return "", nil, "", err
		}
		capacity, unlimited, err := s.carrierCapacity()
		if err != nil {
			return "", nil, "", err
		}
		convID, inserted, err := s.reserveTx(ctx, pid, p, hash, capacity, unlimited)
		if err != nil {
			return "", nil, "", err
		}
		if !inserted {
			// Ключ появился параллельно: продолжить по нему.
			return s.lookupCreateKey(ctx, pid, p.RequestID, hash)
		}
		if convID != "" {
			return convID, nil, "", nil
		}
		// Команда с местом ушла между ensure и транзакцией — ещё раз.
	}
	return "", nil, "", fmt.Errorf("%w: нет служебной команды с местом", errConvUnavailable)
}

func (s *convService) reserveTx(ctx context.Context, pid string, p convCreatePayload, hash []byte, capacity int64, unlimited bool) (convID string, inserted bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	now := s.now()
	res, err := tx.ExecContext(ctx, `INSERT INTO engram_request (photon_id, action, request_id, payload_hash, state, created_at)
VALUES ($1, $2, $3, $4, 'pending', $5) ON CONFLICT DO NOTHING`, pid, ActionConversationCreate, p.RequestID, hash, now)
	if err != nil {
		return "", false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return "", false, nil
	}
	team, err := pickCarrierTeam(ctx, tx, capacity, unlimited)
	if err != nil || team == "" {
		return "", true, err
	}
	if err := takeCarrierSlot(ctx, tx, team, capacity, unlimited); err != nil {
		return "", true, err
	}
	convID = model.NewId()
	desc := ""
	if p.Description != nil {
		desc = *p.Description
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO engram_conversation (conversation_id, kind, state, title, description,
history_policy, version, created_at, created_by, member_count, last_activity_at, carrier_state, carrier_team,
carrier_name, reserved_at) VALUES ($1, 'group', 'creating', $2, $3, $4, 0, $5, $6, 0, 0, 'reserved', $7, $8, $5)`,
		convID, p.Title, desc, p.HistoryPolicy, now, pid, team, carrierChannelPrefix+convID); err != nil {
		return "", true, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE engram_request SET conversation_id = $4
WHERE photon_id = $1 AND action = $2 AND request_id = $3`, pid, ActionConversationCreate, p.RequestID, convID); err != nil {
		return "", true, err
	}
	return convID, true, tx.Commit()
}

// takeCarrierSlot — carriers + 1; достигнут предел — created → full (§6.0.3).
func takeCarrierSlot(ctx context.Context, tx *sql.Tx, team string, capacity int64, unlimited bool) error {
	var carriers int64
	if err := tx.QueryRowContext(ctx, `UPDATE engram_service_team SET carriers = carriers + 1
WHERE team_name = $1 RETURNING carriers`, team).Scan(&carriers); err != nil {
		return err
	}
	if !unlimited && carriers >= capacity {
		_, err := tx.ExecContext(ctx, `UPDATE engram_service_team SET state = 'full'
WHERE team_name = $1 AND state = 'created'`, team)
		return err
	}
	return nil
}

// completeCreate — шаги 2–6 §6.1 под X(разговор): носитель, привязка, T2,
// события, T3. Повтор продолжает с первого незавершённого шага.
func (s *convService) completeCreate(ctx context.Context, callerID, creatorPID, convID string, p convCreatePayload, members []string) (json.RawMessage, error) {
	if err := s.completeCarrier(ctx, callerID, creatorPID, convID, members); err != nil {
		return nil, err
	}

	// T3 — ключ done; ответ только после записи событий (ADR-099 §4).
	snap, err := snapshotFor(ctx, s.db, convID, creatorPID)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(snap)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE engram_request SET state = 'done', response = $4
WHERE photon_id = $1 AND action = $2 AND request_id = $3`, creatorPID, ActionConversationCreate, p.RequestID, body); err != nil {
		return nil, err
	}
	return body, nil
}

// completeCarrier — шаги 2–5 §6.1 под X(разговор): носитель, привязка, T2 и
// запись событий. Общий для 900 и 905.
func (s *convService) completeCarrier(ctx context.Context, callerID, creatorPID, convID string, members []string) error {
	release, err := s.holdLock(ctx, convLockKey(convID))
	if err != nil {
		return err
	}
	defer release()
	channelID, err := s.ensureCarrierChannel(ctx, convID)
	if err != nil {
		return err
	}
	if err := s.finalizeCreate(ctx, convID, creatorPID, members); err != nil {
		return err
	}
	return s.writePendingEvents(ctx, convID, channelID, map[string]string{creatorPID: callerID})
}

// ensureCarrierChannel — шаги 2–3 §6.1: канал-носитель и привязка.
func (s *convService) ensureCarrierChannel(ctx context.Context, convID string) (string, error) {
	for move := 0; move <= convMoveCarrierMax; move++ {
		var state, team, name string
		var channelID sql.NullString
		var teamID sql.NullString
		if err := s.db.QueryRowContext(ctx, `SELECT c.carrier_state, c.carrier_team, c.carrier_name, c.carrier_channel_id, t.team_id
FROM engram_conversation c JOIN engram_service_team t ON t.team_name = c.carrier_team
WHERE c.conversation_id = $1`, convID).Scan(&state, &team, &name, &channelID, &teamID); err != nil {
			return "", err
		}
		if state == "bound" {
			return channelID.String, nil
		}
		if !teamID.Valid {
			return "", fmt.Errorf("%w: у команды %s нет team_id", errConvUnavailable, team)
		}
		ch, appErr := s.mm.CreateChannel(&model.Channel{
			TeamId:      teamID.String,
			Name:        name,
			DisplayName: "ENGRAM",
			Type:        model.ChannelTypePrivate,
		})
		switch {
		case appErr == nil && ch != nil:
		case appErr != nil && appErr.Id == mmErrChannelExists:
			// F5: конфликт (Name, TeamId) возвращает существующий канал.
			if ch == nil {
				ch, appErr = s.mm.GetChannelByName(teamID.String, name, true)
				if appErr != nil {
					return "", fmt.Errorf("%w: GetChannelByName %s: %v", errConvUnavailable, name, appErr)
				}
			}
		case appErr != nil && appErr.Id == mmErrChannelLimit:
			// F5: лимит проверяется раньше уникальности — сначала искать канал.
			found, getErr := s.mm.GetChannelByName(teamID.String, name, true)
			if getErr == nil && found != nil {
				ch = found
				break
			}
			if getErr != nil && getErr.StatusCode != 404 {
				return "", fmt.Errorf("%w: GetChannelByName %s: %v", errConvUnavailable, name, getErr)
			}
			if err := s.markTeamFull(ctx, team); err != nil {
				return "", err
			}
			if err := s.moveCarrierReserve(ctx, convID); err != nil {
				return "", err
			}
			continue
		default:
			return "", fmt.Errorf("%w: CreateChannel %s: %v", errConvUnavailable, name, appErr)
		}
		return s.bindCarrier(ctx, convID, ch.Id)
	}
	return "", fmt.Errorf("%w: носитель не размещён за %d переносов", errConvUnavailable, convMoveCarrierMax)
}

// moveCarrierReserve — резерв переезжает в другую команду, только пока
// carrier_state = 'reserved' (§6.1 шаг 2, S3).
func (s *convService) moveCarrierReserve(ctx context.Context, convID string) error {
	if err := s.ensureCarrierTeam(ctx); err != nil {
		return err
	}
	capacity, unlimited, err := s.carrierCapacity()
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	team, err := pickCarrierTeam(ctx, tx, capacity, unlimited)
	if err != nil {
		return err
	}
	if team == "" {
		return fmt.Errorf("%w: нет служебной команды с местом", errConvUnavailable)
	}
	res, err := tx.ExecContext(ctx, `UPDATE engram_conversation SET carrier_team = $2
WHERE conversation_id = $1 AND carrier_state = 'reserved'`, convID, team)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		if err := takeCarrierSlot(ctx, tx, team, capacity, unlimited); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// bindCarrier — шаг 3 §6.1: условная привязка (K1–K3).
func (s *convService) bindCarrier(ctx context.Context, convID, channelID string) (string, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE engram_conversation SET carrier_state = 'bound', carrier_channel_id = $2
WHERE conversation_id = $1 AND carrier_state = 'reserved'`, convID, channelID)
	if err != nil {
		return "", err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return channelID, nil
	}
	var bound sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT carrier_channel_id FROM engram_conversation WHERE conversation_id = $1`, convID).Scan(&bound); err != nil {
		return "", err
	}
	if bound.String == channelID {
		return channelID, nil
	}
	// Под X гонка исключена (K5): расхождение — внешнее вмешательство.
	s.logger("[Photon][Conv] carrier binding mismatch", "conversation_id", convID)
	return "", fmt.Errorf("%w: носитель привязан к другому каналу", errConvUnavailable)
}

// convEventBody — поля элемента-события (ADR-099 §9.3) без id и created_at:
// их дают пост и позиция.
type convEventBody struct {
	Event          string `json:"event"`
	ActorPhotonID  string `json:"actor_photon_id"`
	TargetPhotonID string `json:"target_photon_id,omitempty"`
	Role           string `json:"role,omitempty"`
	Title          string `json:"title,omitempty"`
	Description    string `json:"description,omitempty"`
	InviteID       string `json:"invite_id,omitempty"`
	Version        int64  `json:"conversation_version"`
}

// finalizeCreate — T2 (§6.1 шаг 4): если разговор ещё creating — периоды,
// версия 1, события created и member_added с позициями из H и их резервы
// (E1, ADR-101 §5.1, §5.2). Уже active — ничего (повтор).
func (s *convService) finalizeCreate(ctx context.Context, convID, creatorPID string, members []string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var state, kind, title, desc string
	var h int64
	if err := tx.QueryRowContext(ctx, `SELECT state, kind, title, description, position_h FROM engram_conversation
WHERE conversation_id = $1 FOR UPDATE`, convID).Scan(&state, &kind, &title, &desc, &h); err != nil {
		return err
	}
	if state != "creating" {
		return tx.Commit()
	}
	events := []convEventBody{{Event: "created", ActorPhotonID: creatorPID, Title: title, Description: desc, Version: 1}}
	for _, m := range members {
		events = append(events, convEventBody{Event: "member_added", ActorPhotonID: creatorPID, TargetPhotonID: m, Version: 1})
	}
	// Позиции P..P+k−1 из H (ADR-101 §5.1, §5.2): P = max(часы Bridge, H+1).
	clock := s.now()
	p := clock
	if h+1 > p {
		p = h + 1
	}
	for seq, ev := range events {
		at := p + int64(seq)
		body, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%s:%d:%d", convID, 1, seq)
		if _, err := tx.ExecContext(ctx, `INSERT INTO engram_event (conversation_id, version, seq, event, at, state)
VALUES ($1, 1, $2, $3, $4, 'pending')`, convID, seq, body, at); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, reserved_at)
VALUES ($1, $2, 'event', $3, 'reserved', $4)`, convID, at, key, s.now()); err != nil {
			return err
		}
	}
	// Открывающий элемент начальных участников — created (ADR-099 §9.4);
	// позиция зафиксирована здесь и больше не меняется (ADR-101 §5.2), id
	// поста добавляет E4.
	all := append([]string{creatorPID}, members...)
	for i, pid := range all {
		role := "member"
		if i == 0 && kind == "group" {
			role = "owner" // в direct оба участника member (ADR-104 §3.1)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO engram_member_period (conversation_id, photon_id, period_no, role,
state, opened_version, open_at) VALUES ($1, $2, 1, $3, 'active', 1, $4)`, convID, pid, role, p); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE engram_conversation SET state = 'active', version = 1, member_count = $2,
position_h = $3 WHERE conversation_id = $1`, convID, len(all), p+int64(len(events))-1); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.observePositionLead("900", convID, p, clock)
	return nil
}

// writePendingEvents — E2–E4 (§4): каждое незаписанное событие разговора —
// пост с явным CreateAt = at; перед CreatePost точечный поиск (E3), после —
// транзакция written + резерв published. actorMM — PhotonID → пользователь
// Mattermost для известных вызывающему авторов; остальных ищет реестр.
func (s *convService) writePendingEvents(ctx context.Context, convID, channelID string, actorMM map[string]string) error {
	rows, err := s.db.QueryContext(ctx, `SELECT version, seq, event, at FROM engram_event
WHERE conversation_id = $1 AND state = 'pending' ORDER BY version, seq`, convID)
	if err != nil {
		return err
	}
	type pending struct {
		version int64
		seq     int
		body    []byte
		at      int64
	}
	var list []pending
	for rows.Next() {
		var e pending
		if err := rows.Scan(&e.version, &e.seq, &e.body, &e.at); err != nil {
			rows.Close()
			return err
		}
		list = append(list, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, e := range list {
		key := fmt.Sprintf("%s:%d:%d", convID, e.version, e.seq)
		var ev convEventBody
		if err := json.Unmarshal(e.body, &ev); err != nil {
			return err
		}
		postID, err := s.findEventPost(ctx, channelID, e.at, key)
		if err != nil {
			return err
		}
		if postID == "" {
			mmID, err := s.mmUserOf(ev.ActorPhotonID, actorMM)
			if err != nil {
				return err
			}
			var fields map[string]any
			if err := json.Unmarshal(e.body, &fields); err != nil {
				return err
			}
			post := &model.Post{ChannelId: channelID, UserId: mmID, CreateAt: e.at, Type: convEventPostType}
			post.AddProp(propEngramEvent, fields)
			post.AddProp(propEngramEventKey, key)
			created, appErr := s.mm.CreatePost(post)
			if appErr != nil {
				return fmt.Errorf("%w: CreatePost события %s: %v", errConvUnavailable, key, appErr)
			}
			postID = created.Id
		}
		if err := s.markEventWritten(ctx, convID, e.version, e.seq, e.at, key, postID, ev); err != nil {
			return err
		}
	}
	return nil
}

// findEventPost — E3: точечный поиск по (носитель, CreateAt, ключ).
func (s *convService) findEventPost(ctx context.Context, channelID string, at int64, key string) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM posts WHERE channelid = $1 AND deleteat = 0 AND createat = $2
AND props->>'engram_event_key' = $3 ORDER BY id LIMIT 1`, channelID, at, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
}

// markEventWritten — E4: событие written, резерв published, у периода,
// который событие открывает, — id поста; last_activity_at.
func (s *convService) markEventWritten(ctx context.Context, convID string, version int64, seq int, at int64, key, postID string, ev convEventBody) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE engram_event SET state = 'written', post_id = $4
WHERE conversation_id = $1 AND version = $2 AND seq = $3 AND state = 'pending'`, convID, version, seq, postID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE engram_position SET state = 'published', post_id = $3
WHERE conversation_id = $1 AND pos = $2 AND state = 'reserved'`, convID, at, postID); err != nil {
		return err
	}
	// Периоды, которые событие открывает или закрывает, узнаются по позиции:
	// её поставила транзакция изменения (E1, E4).
	if _, err := tx.ExecContext(ctx, `UPDATE engram_member_period SET open_post_id = $2
WHERE conversation_id = $1 AND open_at = $3 AND open_post_id IS NULL`, convID, postID, at); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE engram_member_period SET close_post_id = $2
WHERE conversation_id = $1 AND close_at = $3 AND close_post_id IS NULL`, convID, postID, at); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE engram_conversation SET last_activity_at = GREATEST(last_activity_at, $2)
WHERE conversation_id = $1`, convID, at); err != nil {
		return err
	}
	return tx.Commit()
}

// mmUserOf — пользователь Mattermost автора события.
func (s *convService) mmUserOf(photonID string, known map[string]string) (string, error) {
	if id, ok := known[photonID]; ok {
		return id, nil
	}
	rec, found, err := s.registry.FindByPhotonID(photonID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("%w: нет аккаунта Mattermost для автора события", errConvUnavailable)
	}
	return rec.MMUserID, nil
}
