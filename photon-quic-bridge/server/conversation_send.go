package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin"
)

// 930 SEND — ADR-099 §9.1 с поправкой ADR-103 §2; ADR-101 §5.1, §5.1a,
// §5.3, §5.6, §5.10; ADR-102 §2; implementation contract §3;
// conversation-storage.md §6.3.
//
// Последовательность:
//  1. строгий разбор и пределы текста 217 (contract §3.1 п. 1);
//  2. блокировка отправки photon:conv-send:<sender>:<client_msg_id>
//     (ADR-092 §2.5.1, contract §3.1 п. 4) — до конца вызова;
//  3. S(разговор): доступ → состояние → право по таблицам Bridge, владелец
//     ключа (ADR-102), повтор (ADR-101 §5.6), резерв P = max(часы Bridge,
//     H + 1) одной транзакцией; S отпускается только после её COMMIT
//     (ADR-101 §5.3, contract §3.1 п. 3);
//  4. CreatePost с явным CreateAt = P, сверка CreateAt (contract §3.4);
//  5. условный переход reserved → published и last_activity_at (§3.5).
// Неопределённый исход CreatePost резерв не трогает: он остаётся reserved и
// закрывается сверкой (contract §3.6) — она вне этого файла.

type convSendPayload struct {
	ConversationID string `json:"conversation_id"`
	Text           string `json:"text"`
	ClientMsgID    string `json:"client_msg_id"`
	// ADR-109 §2.4–2.5: необязательные ответ и пересылка. Отсутствуют —
	// прежнее поведение байт в байт, хэш ключа не меняется.
	ReplyTo       *string                `json:"reply_to"`
	ForwardedFrom *convForwardedFromWire `json:"forwarded_from"`
	// ADR-124 M1 (contract §8.7): необязательные вложения — загруженные 703
	// объекты отправителя в этом разговоре. Отсутствуют — прежнее поведение.
	Attachments *[]convAttachmentWire `json:"attachments"`
}

type convForwardedFromWire struct {
	AuthorPhotonID string `json:"author_photon_id"`
	CreatedAt      int64  `json:"created_at"`
}

// convSendHash — хэш содержания 930 для ключа §10.2. Без reply_to и
// forwarded_from — SHA-256 текста, как до ADR-109 (ключи, созданные прежними
// клиентами, остаются повторяемыми); переданные поля входят в хэш, и только
// они.
func convSendHash(p convSendPayload) []byte {
	if p.ReplyTo == nil && p.ForwardedFrom == nil && p.Attachments == nil {
		sum := sha256.Sum256([]byte(p.Text))
		return sum[:]
	}
	canon := map[string]any{"text": p.Text}
	if p.ReplyTo != nil {
		canon["reply_to"] = *p.ReplyTo
	}
	if p.ForwardedFrom != nil {
		canon["forwarded_from"] = map[string]any{"author_photon_id": p.ForwardedFrom.AuthorPhotonID, "created_at": p.ForwardedFrom.CreatedAt}
	}
	if p.Attachments != nil {
		// Упорядоченный список media_id: другой набор под тем же ключом —
		// idempotency_conflict (contract §8.7).
		ids := make([]string, len(*p.Attachments))
		for i, a := range *p.Attachments {
			ids[i] = a.MediaID
		}
		canon["attachments"] = ids
	}
	return convHash(canon)
}

// ConvMessage — сообщение ответа 930 (ADR-099 §9.1). id — непрозрачный id
// поста носителя, как у 217 (ADR-082 §3.2, ADR-092 §2.1); created_at —
// позиция резерва (ADR-103 §1).
type ConvMessage struct {
	ID             string `json:"id"`
	AuthorPhotonID string `json:"author_photon_id"`
	Text           string `json:"text"`
	CreatedAt      int64  `json:"created_at"`
	ClientMsgID    string `json:"client_msg_id"`
	// ADR-109: как в элементе ленты; у обычного сообщения их нет.
	ReplyTo       string             `json:"reply_to,omitempty"`
	ForwardedFrom *ConvForwardedFrom `json:"forwarded_from,omitempty"`
	// ADR-124 M1: вложения сообщения в порядке запроса.
	Attachments []ConvAttachment `json:"attachments,omitempty"`
}

type ConvSendResponse struct {
	Message ConvMessage `json:"message"`
}

// convSendReserveAttempts — сколько раз 930 повторяет contract §3.2 после
// того, как резерв не прошёл из-за чужого или своего резерва того же ключа,
// появившегося между проверкой и резервом (contract §3.3, ADR-102 §2 п. 4).
const convSendReserveAttempts = 3

func convSendLockKey(senderPID, clientMsgID string) int64 {
	return advisoryKey("photon:conv-send:" + senderPID + ":" + clientMsgID)
}

// convSendSlot — итог допуска и резерва: либо найденный опубликованный
// резерв своего ключа (повтор, I6), либо новый резерв на позиции pos.
type convSendSlot struct {
	channelID string
	pos       int64
	postID    string // только у найденного опубликованного
	found     bool
}

func (s *convService) sendMessage(ctx context.Context, callerID string, raw json.RawMessage) Response {
	var p convSendPayload
	if !decodeStrict(raw, &p) || p.ConversationID == "" || !uuidRe.MatchString(p.ClientMsgID) {
		return convFail(convErrInvalidRequest)
	}
	// Регистр UUID не различает ключ — как 217.
	cid := strings.ToLower(p.ClientMsgID)
	mediaIDs, ok := attachmentIDs(p.Attachments)
	if !ok {
		return convFail(convErrInvalidRequest)
	}
	// Пересылка с вложениями в v1 запрещена (ADR-124 §2.1).
	if mediaIDs != nil && p.ForwardedFrom != nil {
		return convFail(convErrInvalidRequest)
	}
	// Пустой текст допустим только при вложениях (contract §8.7).
	if strings.TrimSpace(p.Text) == "" && mediaIDs == nil {
		return convFail(errMessageEmpty)
	}
	if utf8.RuneCountInString(p.Text) > directMessageMaxRunes {
		return convFail(errMessageTooLong)
	}
	if countLinks(p.Text) > dmMaxLinks {
		return convFail(errMessageTooManyLinks)
	}
	// Форма ответа и пересылки (ADR-109 §2.4–2.5): только форма; ссылка
	// reply_to проверяется после доступа, под S.
	if p.ReplyTo != nil && *p.ReplyTo == "" {
		return convFail(convErrInvalidRequest)
	}
	if f := p.ForwardedFrom; f != nil && (!isCanonicalPhotonID(f.AuthorPhotonID) || f.CreatedAt <= 0) {
		return convFail(convErrInvalidRequest)
	}
	pid, _, found, err := ownPhotonID(s.registry, s.users, callerID)
	if err != nil {
		return s.convFailFor("930 caller", err)
	}
	if !found {
		return convFail(convErrConversationNF)
	}

	// Блокировка отправки: одновременный повтор той же пары (отправитель,
	// client_msg_id) ещё идёт — message_in_progress (ADR-092 §2.5.1, ADR-099
	// §9.1). Держится до конца вызова, включая CreatePost.
	releaseSend, err := s.holdLock(ctx, convSendLockKey(pid, cid))
	if errors.Is(err, errConvLockTimeout) {
		return convFail(errMessageInProgress)
	}
	if err != nil {
		return s.convFailFor("930 send lock", err)
	}
	defer releaseSend()

	slot, code, err := s.admitAndReserve(ctx, p.ConversationID, pid, cid, convSendHash(p), p.ReplyTo, mediaIDs)
	if err != nil {
		return s.convFailFor("930 reserve", err)
	}
	if code != "" {
		return convFail(code)
	}
	msg := ConvMessage{AuthorPhotonID: pid, Text: p.Text, CreatedAt: slot.pos, ClientMsgID: cid}
	extra := map[string]any{}
	if p.ReplyTo != nil {
		msg.ReplyTo = *p.ReplyTo
		extra[propEngramReplyTo] = *p.ReplyTo
	}
	if f := p.ForwardedFrom; f != nil {
		msg.ForwardedFrom = &ConvForwardedFrom{AuthorPhotonID: f.AuthorPhotonID, CreatedAt: f.CreatedAt}
		extra[propEngramForwardedFrom] = map[string]any{"author_photon_id": f.AuthorPhotonID, "created_at": f.CreatedAt}
	}
	if mediaIDs != nil {
		atts, err := loadAttachments(ctx, s.db, p.ConversationID, slot.pos)
		if err != nil {
			if !slot.found {
				// Резерв уже держит привязку: без проекции пост не публикуется,
				// резерв закроет сверка (contract §3.6).
				s.logger("[Photon][Conv] 930 attachments read failed, reservation stays open", "pos", slot.pos)
			}
			return s.convFailFor("930 attachments", err)
		}
		msg.Attachments = atts
		proj, err := projectionOf(atts)
		if err != nil {
			return s.convFailFor("930 attachments", err)
		}
		extra[propPhotonMedia] = proj
	}
	if slot.found {
		msg.ID = slot.postID
		return Response{OK: true, Data: ConvSendResponse{Message: msg}}
	}
	postID, code := s.publishMessage(ctx, callerID, p.ConversationID, cid, p.Text, slot, extra)
	if code != "" {
		return convFail(code)
	}
	// Событие — только после перехода reserved → published (publishMessage
	// сообщает успех лишь после него; ADR-123 §2.2). Повтор тем же ключом
	// (slot.found выше) сюда не доходит и события не повторяет.
	s.pushMessage(p.ConversationID, pid)
	msg.ID = postID
	return Response{OK: true, Data: ConvSendResponse{Message: msg}}
}

// admitAndReserve — шаги под S(разговор) (contract §3.1–§3.3). S берётся
// попыткой с бюджетом (иначе temporarily_unavailable) и отпускается при
// выходе из функции — после COMMIT транзакции резерва, никак не раньше:
// иначе изменение, зафиксированное между проверкой права и резервом,
// встало бы раньше сообщения (ADR-101 §5.3).
func (s *convService) admitAndReserve(ctx context.Context, convID, pid, cid string, hash []byte, replyTo *string, mediaIDs []string) (convSendSlot, string, error) {
	releaseS, err := s.holdSharedLock(ctx, convLockKey(convID))
	if err != nil {
		return convSendSlot{}, "", err
	}
	defer releaseS()

	channelID, code, err := sendAdmission(ctx, s.db, convID, pid)
	if err != nil || code != "" {
		return convSendSlot{}, code, err
	}
	replyChecked := false
	for attempt := 0; attempt < convSendReserveAttempts; attempt++ {
		slot, code, done, err := lookupSendKey(ctx, s.db, convID, pid, cid, hash)
		if err != nil || code != "" || done {
			slot.channelID = channelID
			return slot, code, err
		}
		// Ответ (ADR-109 §2.4): элемент-сообщение этого разговора, видимый
		// автору и не удалённый. После поиска повтора: повтор уже
		// выполненного запроса не зависит от судьбы целевого сообщения.
		if replyTo != nil && !replyChecked {
			t, err := lookupVisibleMessage(ctx, s.db, convID, pid, *replyTo)
			if err != nil {
				return convSendSlot{}, "", err
			}
			if t == nil || t.deleted {
				return convSendSlot{}, convErrInvalidRequest, nil
			}
			replyChecked = true
		}
		pos, again, code, err := s.reserveMessage(ctx, convID, pid, cid, hash, mediaIDs)
		if err != nil {
			return convSendSlot{}, "", err
		}
		if code != "" {
			return convSendSlot{}, code, nil
		}
		if !again {
			return convSendSlot{channelID: channelID, pos: pos}, "", nil
		}
	}
	return convSendSlot{}, "", errors.New("conversation: резерв 930 не прошёл за отведённые попытки")
}

// sendAdmission — существование и участие → состояние → право (ADR-099
// §11, §8.1), только по таблицам Bridge. Возвращает носитель разговора.
func sendAdmission(ctx context.Context, q convQuerier, convID, pid string) (string, string, error) {
	var state string
	var channelID sql.NullString
	err := q.QueryRowContext(ctx, `SELECT state, carrier_channel_id FROM engram_conversation
WHERE conversation_id = $1`, convID).Scan(&state, &channelID)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && state == "creating") {
		return "", convErrConversationNF, nil
	}
	if err != nil {
		return "", "", err
	}
	var periodState, role string
	err = q.QueryRowContext(ctx, `SELECT state, role FROM engram_member_period
WHERE conversation_id = $1 AND photon_id = $2 ORDER BY period_no DESC LIMIT 1`, convID, pid).Scan(&periodState, &role)
	if errors.Is(err, sql.ErrNoRows) {
		return "", convErrConversationNF, nil
	}
	if err != nil {
		return "", "", err
	}
	if periodState != "active" {
		return "", convErrNoAccess, nil
	}
	// Блокировка в direct (ADR-111 §2.2, Q2): заблокированному — как
	// недоступный разговор, без раскрытия причины; блокирующему — blocked.
	// Первым — «собеседник заблокировал меня»: при взаимной блокировке
	// причина не раскрывается.
	peerBlockedMe, iBlockedPeer, err := directBlockState(ctx, q, convID, pid)
	if err != nil {
		return "", "", err
	}
	if peerBlockedMe {
		return "", convErrConversationNF, nil
	}
	if iBlockedPeer {
		return "", convErrBlocked, nil
	}
	if state == "archived" {
		return "", convErrArchived, nil
	}
	if !convHasPermission(role, "messages.send") {
		return "", convErrForbidden, nil
	}
	if !channelID.Valid {
		// active без привязанного носителя не бывает (S3, §6.1).
		return "", "", errors.New("conversation: у активного разговора нет носителя")
	}
	return channelID.String, "", nil
}

// lookupSendKey — contract §3.2. Сначала владелец ключа (ADR-102 §2): резерв
// сообщения другого отправителя в ЛЮБОМ состоянии, включая надгробие, —
// idempotency_conflict, ничего не меняется. Затем повтор среди своих
// резервов не в состоянии aborted (ADR-101 §5.6). done=true — ответ готов
// (найденная позиция); done=false и пустой code — нужен новый резерв.
func lookupSendKey(ctx context.Context, q convQuerier, convID, pid, cid string, hash []byte) (convSendSlot, string, bool, error) {
	var one int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM engram_position
WHERE conversation_id = $1 AND item_kind = 'message' AND item_key = $2 AND sender <> $3 LIMIT 1`, convID, cid, pid).Scan(&one)
	if err == nil {
		return convSendSlot{}, convErrIdempotency, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return convSendSlot{}, "", false, err
	}
	var state, postID string
	var gotHash []byte
	var pos int64
	// Только свои резервы: чужой, зафиксированный после проверки владельца
	// (два оператора — два снимка READ COMMITTED), повтором не считается.
	// Его увидит проверка ключа под блокировкой строки H в reserveMessage, и
	// следующий проход §3.2 ответит idempotency_conflict (ADR-102 §2).
	err = q.QueryRowContext(ctx, `SELECT state, pos, payload_hash, COALESCE(post_id, '') FROM engram_position
WHERE conversation_id = $1 AND item_kind = 'message' AND item_key = $2 AND sender = $3 AND state <> 'aborted'`, convID, cid, pid).
		Scan(&state, &pos, &gotHash, &postID)
	if errors.Is(err, sql.ErrNoRows) {
		// Нет вовсе или только свои надгробия: пост на позиции с надгробием
		// найденным не считается — новый резерв.
		return convSendSlot{}, "", false, nil
	}
	if err != nil {
		return convSendSlot{}, "", false, err
	}
	switch state {
	case "published":
		if string(gotHash) != string(hash) {
			return convSendSlot{}, convErrIdempotency, false, nil
		}
		return convSendSlot{pos: pos, postID: postID, found: true}, "", true, nil
	case "reserved":
		return convSendSlot{}, errMessageInProgress, false, nil
	}
	return convSendSlot{}, "", false, errors.New("conversation: резерв в неизвестном состоянии " + state)
}

// reserveMessage — contract §3.3: одна транзакция. UPDATE строки разговора
// выдаёт P = max(часы Bridge, H + 1) и держит блокировку строки до COMMIT
// (S7, S8): резервы разговора фиксируются в порядке позиций. После неё
// ключ проверяется ещё раз — все резервы разговора, зафиксированные до
// этого места, видны (READ COMMITTED, новый снимок на оператор), а новые
// ждут строку; так чужой резерв, появившийся и даже отменённый после
// проверки §3.2, не даёт второму отправителю занять ключ (ADR-102 §2).
// again=true — резерв не сделан, повторить §3.2.
func (s *convService) reserveMessage(ctx context.Context, convID, pid, cid string, hash []byte, mediaIDs []string) (int64, bool, string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, "", err
	}
	defer tx.Rollback()
	var pos int64
	clock := s.now()
	if err := tx.QueryRowContext(ctx, `UPDATE engram_conversation SET position_h = GREATEST($2, position_h + 1)
WHERE conversation_id = $1 RETURNING position_h`, convID, clock).Scan(&pos); err != nil {
		return 0, false, "", err
	}
	var taken bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM engram_position
WHERE conversation_id = $1 AND item_kind = 'message' AND item_key = $2 AND (sender <> $3 OR state <> 'aborted'))`,
		convID, cid, pid).Scan(&taken); err != nil {
		return 0, false, "", err
	}
	if taken {
		return 0, true, "", nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO engram_position
  (conversation_id, pos, item_kind, item_key, state, sender, payload_hash, reserved_at)
VALUES ($1, $2, 'message', $3, 'reserved', $4, $5, $6)`, convID, pos, cid, pid, hash, s.now()); err != nil {
		// Нарушение engram_position_key_live — повторить §3.2 (contract
		// §3.3). Текст ошибки через RPC-драйвер приходит строкой, поэтому
		// причину не разбираем: неустранимая ошибка исчерпает попытки.
		s.logger("[Photon][Conv] 930 reserve insert failed", "error", err.Error())
		return 0, true, "", nil
	}
	// Привязка вложений — в той же транзакции, что резерв (contract §8.7):
	// либо позиция и все привязки, либо ничего.
	if mediaIDs != nil {
		ok, err := bindAttachments(ctx, tx, convID, pid, pos, mediaIDs)
		if err != nil {
			return 0, false, "", err
		}
		if !ok {
			return 0, false, convErrForbidden, nil
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, false, "", err
	}
	s.observePositionLead("930", convID, pos, clock)
	return pos, false, "", nil
}

// publishMessage — contract §3.4–§3.5. Возвращает id поста или код отказа;
// успех сообщается только после условного перехода reserved → published.
func (s *convService) publishMessage(ctx context.Context, callerID, convID, cid, text string, slot convSendSlot, extra map[string]any) (string, string) {
	// PendingPostId не задаётся (contract §3.4: либо не задаётся, либо
	// ключ + позиция): дедупликация Mattermost по нему не сверяет CreateAt.
	// Пока публикация идёт в этом процессе, сверка позицию не закрывает:
	// отсутствие поста сейчас ещё не значит, что его не будет (contract §3.6).
	defer s.inflight.begin(convID, slot.pos, s.now())()
	post := &model.Post{ChannelId: slot.channelID, UserId: callerID, Message: text, CreateAt: slot.pos}
	post.AddProp(dmPropClientMsgID, cid)
	for k, v := range extra {
		post.AddProp(k, v)
	}
	created, appErr := s.mm.CreatePost(post)
	switch {
	case createPostRejected(appErr):
		// Однозначный отказ: Mattermost отверг пост до записи — пост не
		// создан. reserved → aborted.
		s.logger("[Photon][Conv] 930 CreatePost rejected", "error", appErr.Error())
		s.abortMessage(ctx, convID, slot.pos)
		if appErr.Id == mmPostTooLongErrorID {
			return "", errMessageTooLong
		}
		return "", convErrTemporarilyUnavail
	case appErr != nil || created == nil:
		// Неопределённый исход: внутренняя ошибка сервера, ошибка после
		// записи поста (в том числе 4xx — см. createPostRejected), обрыв
		// RPC (клиент RPC плагина возвращает nil, nil). Пост мог быть
		// создан (R1) — резерв остаётся reserved и держит барьер до сверки
		// (contract §3.6). Успех не сообщается.
		errText := "nil post"
		if appErr != nil {
			errText = appErr.Error()
		}
		s.logger("[Photon][Conv] 930 CreatePost outcome unknown, reservation stays open", "pos", slot.pos, "error", errText)
		return "", convErrTemporarilyUnavail
	case created.CreateAt != slot.pos:
		// Пост не на выданной позиции (ADR-101 §5.6) — свой резерв
		// отменяется, успех не сообщается.
		s.logger("[Photon][Conv] 930 CreateAt mismatch", "pos", slot.pos, "create_at", created.CreateAt)
		s.abortMessage(ctx, convID, slot.pos)
		return "", convErrTemporarilyUnavail
	}

	ok, err := s.markMessagePublished(ctx, convID, slot.pos, created.Id, text)
	if err != nil {
		s.logger("[Photon][Conv] 930 publish failed", "error", err.Error())
		return "", convErrTemporarilyUnavail
	}
	if !ok {
		// Резерв уже отменён сверкой: успех не сообщается, повтор получит
		// новую позицию (ADR-101 §5.10).
		return "", convErrTemporarilyUnavail
	}
	return created.Id, ""
}

// Ошибки CreatePost Mattermost 9.11.18, которые подтверждают, что пост НЕ
// создан (contract §3.4). Путь плагина: PluginAPI.CreatePost →
// App.CreatePostMissingChannel → App.CreatePost (app/post.go) →
// SqlPostStore.SaveMultiple (sqlstore/post_store.go).
const (
	mmPostInvalidErrorPrefix  = "model.post.is_valid."      // Post.IsValid в SaveMultiple до INSERT
	mmPluginRejectErrorPrefix = "Post rejected by plugin. " // отказ хука MessageWillBePosted
	mmChannelNotFoundErrorID  = "app.channel.get.existing.app_error"
	mmMissingAccountErrorID   = "app.user.missing_account.const"
)

// createPostRejected — CreatePost отверг пост ДО записи: однозначная
// подтверждённая ошибка создания (contract §3.4) — только тогда резерв
// отменяется. Статус 4xx сам по себе этого не доказывает: после
// Post().Save App.CreatePost ещё вызывает SanitizePostMetadataForUser, и
// GetChannel канала из permalink в тексте сообщения отвечает 404
// app.channel.get.existing.app_error (Where "GetChannel") — пост при этом
// уже записан. Тот же Id до записи даёт только CreatePostMissingChannel,
// поэтому для него сверяется и Where (поле доходит через RPC плагина).
// Список закрыт: любая другая ошибка, включая прочие 4xx, —
// неопределённость (contract §3.6).
func createPostRejected(e *model.AppError) bool {
	if e == nil || e.StatusCode < http.StatusBadRequest || e.StatusCode >= http.StatusInternalServerError {
		return false
	}
	switch {
	case strings.HasPrefix(e.Id, mmPostInvalidErrorPrefix):
		return true
	case strings.HasPrefix(e.Id, mmPluginRejectErrorPrefix), e.Id == plugin.DismissPostError:
		return true
	case e.Id == mmChannelNotFoundErrorID:
		return e.Where == "CreatePostMissingChannel"
	case e.Id == mmMissingAccountErrorID:
		return e.Where == "CreatePost"
	}
	return false
}

// abortMessage — условный переход своего резерва reserved → aborted
// (S11). Сбой оставляет резерв reserved — его закроет сверка.
func (s *convService) abortMessage(ctx context.Context, convID string, pos int64) {
	if _, err := s.db.ExecContext(ctx, `UPDATE engram_position SET state = 'aborted'
WHERE conversation_id = $1 AND pos = $2 AND state = 'reserved'`, convID, pos); err != nil {
		s.logger("[Photon][Conv] 930 abort failed", "error", err.Error())
	}
}

// markMessagePublished — contract §3.5: условный переход и
// last_activity_at одной транзакцией. false — резерв уже не reserved.
func (s *convService) markMessagePublished(ctx context.Context, convID string, pos int64, postID, text string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE engram_position SET state = 'published', post_id = $3
WHERE conversation_id = $1 AND pos = $2 AND state = 'reserved'`, convID, pos, postID)
	if err != nil {
		return false, err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE engram_conversation SET last_activity_at = GREATEST(last_activity_at, $2)
WHERE conversation_id = $1`, convID, pos); err != nil {
		return false, err
	}
	// Индекс текста поиска (ADR-110): в той же транзакции, что и видимость.
	if err := upsertItemText(ctx, tx, convID, postID, pos, text); err != nil {
		return false, err
	}
	return true, tx.Commit()
}
