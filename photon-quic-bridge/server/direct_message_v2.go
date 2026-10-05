package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mattermost/mattermost/server/public/model"
)

// Личные сообщения v2 — ADR-092 ред. 3: 217 SendDirectMessageV2 (§2.5),
// 218 ListDirectMessagesV2 (§2.2). 213/214 не меняются.
//
// Разделение: здесь — контракт (проверки входа, курсор, окно, форма
// ответа); SQL — только в dm_sql.go.

const (
	errMessageTooManyLinks = "message_too_many_links"
	errIdempotencyConflict = "idempotency_conflict"
	errMessageInProgress   = "message_in_progress"
	// errSyncUnavailable — v2 выключен: схема Mattermost не прошла проверку
	// при старте либо БД недоступна (§5). 213/214 при этом работают.
	errSyncUnavailable = "sync_unavailable"

	// dmMaxLinks — N_max: URL в тексте 217 (§2.5.1). Константа контракта;
	// меняется только редакцией ADR вместе с W.
	dmMaxLinks = 5
	// dmDBCommitMax — T_db_max, допущение о задержке записи (§2.3).
	dmDBCommitMax = time.Second
	// dmWindowMargin — Δ, эксплуатационный запас поверх W_min (§2.3).
	dmWindowMargin = 30 * time.Second
	// dmIdempotencyTTL — сколько назад искать свой пост по client_msg_id.
	// Обязан быть не меньше срока автоповторов outbox (§2.5.1).
	dmIdempotencyTTL = 30 * 24 * time.Hour
	// dmSendLockWait — ожидание блокировки повтора той же операции; дольше
	// — message_in_progress (временный отказ, клиент повторит).
	dmSendLockWait = 10 * time.Second

	dmCursorVersion = 1
)

// dmWindow — W = W_min + Δ, W_min = F_max × T_link + T_db_max,
// F_max = N_max + LinkMetadataMaxImages + 1 (§2.3). T_link берётся из
// конфигурации Mattermost при каждом вызове: смена настройки учитывается
// без перезапуска плагина.
func dmWindow(tLink time.Duration) time.Duration {
	fMax := dmMaxLinks + model.LinkMetadataMaxImages + 1
	return time.Duration(fMax)*tLink + dmDBCommitMax + dmWindowMargin
}

// linkTokens — консервативный счёт URL (§2.5.1): каждое http://, https://,
// www. без учёта регистра и каждое «![» (изображение markdown, в том числе
// с относительным адресом). Не меньше, чем насчитает предпросмотр
// Mattermost; лишнее засчитанное — в сторону отказа, не потери.
var linkTokens = regexp.MustCompile(`(?i)https?://|www\.`)

func countLinks(text string) int {
	return len(linkTokens.FindAllStringIndex(text, -1)) + strings.Count(text, "![")
}

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// directMessageV2Deps — зависимости v2 поверх зависимостей 213/214.
type directMessageV2Deps struct {
	directMessageDeps
	store    dmStore // nil — v2 выключен, причина в storeErr
	storeErr error
	// linkTimeout — LinkMetadataTimeoutMilliseconds Mattermost.
	linkTimeout func() time.Duration
	now         func() time.Time
	// lockWait — ожидание блокировки повтора; ноль — dmSendLockWait.
	lockWait time.Duration
}

func (d directMessageV2Deps) available() bool { return d.store != nil }

// ── 217 ──────────────────────────────────────────────────────────────

type sendDirectMessageV2Payload struct {
	ToPhotonID  string `json:"to_photon_id"`
	Text        string `json:"text"`
	ClientMsgID string `json:"client_msg_id"`
}

// DirectMessageV2 — сообщение протокола v2 (photon.proto DirectMessageV2).
type DirectMessageV2 struct {
	ID             string `json:"id"`
	AuthorPhotonID string `json:"author_photon_id"`
	Text           string `json:"text"`
	CreatedAt      int64  `json:"created_at"`
	ClientMsgID    string `json:"client_msg_id,omitempty"`
	Deleted        bool   `json:"deleted,omitempty"`
}

// errDMConflict / errDMCreate — исходы внутри транзакции блокировки; они же
// откатывают её (транзакция держит только блокировку, откат ничего не
// теряет).
var (
	errDMConflict = errors.New("dm: same client_msg_id, different text")
	errDMCreate   = errors.New("dm: create post failed")
)

func decodeStrict(raw json.RawMessage, dst any) bool {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(dst) == nil
}

// sendDirectMessageV2 — Action 217 (ADR-092 ред. 3 §2.5.1).
func sendDirectMessageV2(d directMessageV2Deps, callerID string, raw json.RawMessage) Response {
	var p sendDirectMessageV2Payload
	if !decodeStrict(raw, &p) {
		return dmFail(errCodeInvalidRequest)
	}
	if !isCanonicalPhotonID(p.ToPhotonID) {
		return dmFail(errInvalidPhotonID)
	}
	if !uuidRe.MatchString(p.ClientMsgID) {
		return dmFail(errCodeInvalidRequest)
	}
	cid := strings.ToLower(p.ClientMsgID)

	// Текст — до обращения к Mattermost (ADR-082 §3.4, ADR-092 §2.5.1).
	if strings.TrimSpace(p.Text) == "" {
		return dmFail(errMessageEmpty)
	}
	if utf8.RuneCountInString(p.Text) > directMessageMaxRunes {
		return dmFail(errMessageTooLong)
	}
	if countLinks(p.Text) > dmMaxLinks {
		return dmFail(errMessageTooManyLinks)
	}
	if !d.available() {
		d.logger("[Photon][DMv2] 217 refused: v2 unavailable", "error", errString(d.storeErr))
		return dmFail(errSyncUnavailable)
	}

	who, code := resolveParticipants(d.directMessageDeps, callerID, p.ToPhotonID)
	if code != "" {
		return dmFail(code)
	}
	ch, appErr := d.api.GetDirectChannel(callerID, who.peerUserID)
	if appErr != nil || ch == nil {
		if appErr != nil {
			d.logger("[Photon][DMv2] GetDirectChannel failed", "error", appErr.Error())
		}
		return dmFail(errMessageFailed)
	}

	var (
		out      DirectMessageV2
		failCode string
	)
	since := d.now().Add(-dmIdempotencyTTL).UnixMilli()
	wait := d.lockWait
	if wait <= 0 {
		wait = dmSendLockWait
	}
	err := d.store.WithSendLock(context.Background(), callerID, cid, wait, func(find dmFindFunc) error {
		existing, err := find(ch.Id, callerID, cid, since)
		if err != nil {
			return err
		}
		if existing != nil {
			if existing.Message != p.Text {
				return errDMConflict
			}
			out = DirectMessageV2{
				ID: existing.ID, AuthorPhotonID: who.ownPhotonID, Text: existing.Message,
				CreatedAt: existing.CreateAt, ClientMsgID: cid, Deleted: existing.DeleteAt != 0,
			}
			return nil
		}
		post := &model.Post{UserId: callerID, ChannelId: ch.Id, Message: p.Text}
		post.AddProp(dmPropClientMsgID, cid)
		created, appErr := d.api.CreatePost(post)
		if appErr != nil {
			if appErr.Id == mmPostTooLongErrorID {
				failCode = errMessageTooLong
			} else {
				d.logger("[Photon][DMv2] CreatePost failed", "error", appErr.Error())
				failCode = errMessageFailed
			}
			return errDMCreate
		}
		out = DirectMessageV2{
			ID: created.Id, AuthorPhotonID: who.ownPhotonID, Text: created.Message,
			CreatedAt: created.CreateAt, ClientMsgID: cid,
		}
		return nil
	})
	switch {
	case err == nil:
		return Response{OK: true, Data: map[string]DirectMessageV2{"message": out}}
	case errors.Is(err, errDMConflict):
		return dmFail(errIdempotencyConflict)
	case errors.Is(err, errSendLockTimeout):
		return dmFail(errMessageInProgress)
	case errors.Is(err, errDMCreate):
		return dmFail(failCode)
	}
	d.logger("[Photon][DMv2] send lock/lookup failed", "error", err.Error())
	return dmFail(errMessageFailed)
}

// ── курсор (§2.2.5) ───────────────────────────────────────────────────

// dmCursor — состояние прохода. На проводе — base64url(JSON); клиент его не
// разбирает. Поста по курсору сервер не ищет: позиции содержат сами
// (CreateAt, PostID).
type dmCursor struct {
	V int     `json:"v"`
	B string  `json:"b"`           // привязка к разговору
	W *dmWire `json:"w,omitempty"` // водяной знак; нет — пустой разговор
	P *dmWire `json:"p,omitempty"` // позиция прохода; нет — следующий запрос начинает проход
}

type dmWire struct {
	T  int64  `json:"t"`
	ID string `json:"i"`
}

func (w *dmWire) pos() dmPos { return dmPos{CreateAt: w.T, ID: w.ID} }

func wireOf(p dmPos) *dmWire { return &dmWire{T: p.CreateAt, ID: p.ID} }

// conversationBinding — привязка курсора к паре участников. От пары, а не
// от канала: курсор первичной синхронизации выдаётся и тогда, когда канала
// ещё нет. Хэш — чтобы в курсоре не было mm_user_id (ADR-082 §3.2).
func conversationBinding(a, b string) string {
	if b < a {
		a, b = b, a
	}
	h := sha256.Sum256([]byte("photon:dm:" + a + ":" + b))
	return hex.EncodeToString(h[:8])
}

var postIDRe = regexp.MustCompile(`^[a-z0-9]{1,64}$`)

func encodeCursor(c dmCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeCursor — nil и false — курсор не годится: не разбирается, другая
// версия, чужой разговор, негодные позиции (§2.2.5).
func decodeCursor(s, binding string) (*dmCursor, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, false
	}
	var c dmCursor
	if !decodeStrict(raw, &c) {
		return nil, false
	}
	if c.V != dmCursorVersion || c.B != binding {
		return nil, false
	}
	for _, w := range []*dmWire{c.W, c.P} {
		if w != nil && (w.T < 0 || !postIDRe.MatchString(w.ID)) {
			return nil, false
		}
	}
	// Позиция прохода без водяного знака невозможна: водяной знак — самая
	// новая отданная позиция, а позиция — последняя отданная.
	if c.P != nil && c.W == nil {
		return nil, false
	}
	return &c, true
}

// ── 218 ──────────────────────────────────────────────────────────────

type listDirectMessagesV2Payload struct {
	WithPhotonID string `json:"with_photon_id"`
	Cursor       string `json:"cursor"`
	Limit        *int   `json:"limit"`
}

type dmWirePosition struct {
	CreatedAt int64  `json:"created_at"`
	ID        string `json:"id"`
}

// DirectMessagesV2 — ответ 218 (photon.proto DirectMessagesV2Response).
type DirectMessagesV2 struct {
	Messages   []DirectMessageV2 `json:"messages"`
	NextCursor string            `json:"next_cursor"`
	From       *dmWirePosition   `json:"from,omitempty"`
	AtEnd      bool              `json:"at_end"`
}

// listDirectMessagesV2 — Action 218 (ADR-092 ред. 3 §2.2).
func listDirectMessagesV2(d directMessageV2Deps, callerID string, raw json.RawMessage) Response {
	var p listDirectMessagesV2Payload
	if !decodeStrict(raw, &p) {
		return dmFail(errCodeInvalidRequest)
	}
	if !isCanonicalPhotonID(p.WithPhotonID) {
		return dmFail(errInvalidPhotonID)
	}
	limit := directMessagesDefaultN
	if p.Limit != nil {
		limit = *p.Limit
	}
	if limit < 1 || limit > directMessagesMaximumN {
		return dmFail(errInvalidLimit)
	}

	who, code := resolveParticipants(d.directMessageDeps, callerID, p.WithPhotonID)
	if code != "" {
		return dmFail(code)
	}
	binding := conversationBinding(callerID, who.peerUserID)

	var cur *dmCursor
	if p.Cursor != "" {
		c, ok := decodeCursor(p.Cursor, binding)
		if !ok {
			return dmFail(errInvalidCursor)
		}
		cur = c
	}
	if !d.available() {
		d.logger("[Photon][DMv2] 218 refused: v2 unavailable", "error", errString(d.storeErr))
		return dmFail(errSyncUnavailable)
	}

	// Поиск канала БЕЗ создания (ADR-082 §3.3): нет канала — нет постов.
	var channelID string
	ch, appErr := d.api.GetChannelByName("", model.GetDMNameFromIds(callerID, who.peerUserID), false)
	switch {
	case appErr == nil && ch != nil:
		channelID = ch.Id
	case appErr != nil && appErr.StatusCode != mmChannelNotFoundStatus:
		d.logger("[Photon][DMv2] GetChannelByName failed", "error", appErr.Error())
		return dmFail(errMessageFailed)
	}
	authors := dmAuthors{callerID, who.peerUserID}
	ctx := context.Background()

	var (
		rows []dmRow
		err  error
		resp = DirectMessagesV2{Messages: []DirectMessageV2{}}
	)
	next := dmCursor{V: dmCursorVersion, B: binding}

	switch {
	case cur == nil:
		// Первичная синхронизация (§2.2.1): последние limit, блок newest.
		if channelID != "" {
			rows, err = d.store.Newest(ctx, channelID, authors, limit)
		}
		if err == nil {
			resp.AtEnd = true
			if len(rows) > 0 {
				next.W = wireOf(dmPos{CreateAt: rows[len(rows)-1].CreateAt, ID: rows[len(rows)-1].ID})
			}
		}

	case cur.P == nil:
		// Начало прохода (§2.2.2): окно — только здесь.
		lower := int64(0)
		if cur.W != nil {
			lower = cur.W.T - dmWindow(d.linkTimeout()).Milliseconds()
			if lower < 0 {
				lower = 0
			}
		}
		if channelID != "" {
			rows, err = d.store.From(ctx, channelID, authors, lower, limit)
		}
		resp.From = &dmWirePosition{CreatedAt: lower, ID: ""}
		next.W = cur.W

	default:
		// Продолжение (§2.2.3): строго новее позиции, без окна.
		if channelID != "" {
			rows, err = d.store.After(ctx, channelID, authors, cur.P.pos(), limit)
		}
		resp.From = &dmWirePosition{CreatedAt: cur.P.T, ID: cur.P.ID}
		next.W = cur.W
	}
	if err != nil {
		d.logger("[Photon][DMv2] query failed", "error", err.Error())
		return dmFail(errMessageFailed)
	}

	for _, r := range rows {
		m := DirectMessageV2{ID: r.ID, Text: r.Message, CreatedAt: r.CreateAt}
		if r.UserID == callerID {
			m.AuthorPhotonID = who.ownPhotonID
			m.ClientMsgID = r.ClientMsgID
		} else {
			m.AuthorPhotonID = who.peerPhotonID
		}
		resp.Messages = append(resp.Messages, m)
	}

	if cur != nil {
		// Конец прохода и next_cursor (§2.2.4).
		resp.AtEnd = len(rows) < limit
		if len(rows) > 0 {
			last := dmPos{CreateAt: rows[len(rows)-1].CreateAt, ID: rows[len(rows)-1].ID}
			if next.W == nil || last.newerThan(next.W.pos()) {
				next.W = wireOf(last)
			}
			if !resp.AtEnd {
				next.P = wireOf(last)
			}
		}
	}
	resp.NextCursor = encodeCursor(next)
	return Response{OK: true, Data: resp}
}

// ── 219 ──────────────────────────────────────────────────────────────

type listDirectMessagesBeforePayload struct {
	WithPhotonID string          `json:"with_photon_id"`
	Before       *dmWirePosition `json:"before"`
	Limit        *int            `json:"limit"`
}

// DirectMessagesBefore — ответ 219: сообщения строго старше якоря в порядке
// ADR-092 §2.1; курсора нет (ADR-105 §2.3).
type DirectMessagesBefore struct {
	Items   []DirectMessageV2 `json:"items"`
	AtStart bool              `json:"at_start"`
}

// listDirectMessagesBefore — Action 219 (ADR-105, близнец 218: те же
// проверки в том же порядке, форма сообщения 218, окна нет).
func listDirectMessagesBefore(d directMessageV2Deps, callerID string, raw json.RawMessage) Response {
	var p listDirectMessagesBeforePayload
	if !decodeStrict(raw, &p) {
		return dmFail(errCodeInvalidRequest)
	}
	if !isCanonicalPhotonID(p.WithPhotonID) {
		return dmFail(errInvalidPhotonID)
	}
	limit := dmBeforeDefaultLimit
	if p.Limit != nil {
		limit = *p.Limit
	}
	if limit < 1 || limit > dmBeforeMaxLimit {
		return dmFail(errInvalidLimit)
	}
	if p.Before == nil || p.Before.CreatedAt <= 0 || !postIDRe.MatchString(p.Before.ID) {
		return dmFail(errCodeInvalidRequest)
	}

	who, code := resolveParticipants(d.directMessageDeps, callerID, p.WithPhotonID)
	if code != "" {
		return dmFail(code)
	}
	store, _ := d.store.(dmBeforeStore)
	if !d.available() || store == nil {
		d.logger("[Photon][DMv2] 219 refused: v2 unavailable", "error", errString(d.storeErr))
		return dmFail(errSyncUnavailable)
	}

	// Канал ищется БЕЗ создания: нет канала — нет постов, начало достигнуто.
	resp := DirectMessagesBefore{Items: []DirectMessageV2{}, AtStart: true}
	ch, appErr := d.api.GetChannelByName("", model.GetDMNameFromIds(callerID, who.peerUserID), false)
	switch {
	case appErr == nil && ch != nil:
	case appErr != nil && appErr.StatusCode != mmChannelNotFoundStatus:
		d.logger("[Photon][DMv2] GetChannelByName failed", "error", appErr.Error())
		return dmFail(errMessageFailed)
	default:
		return Response{OK: true, Data: resp}
	}

	// limit+1 — проба для at_start: старше выданного есть хотя бы один пост.
	rows, err := store.Before(context.Background(), ch.Id, dmAuthors{callerID, who.peerUserID},
		dmPos{CreateAt: p.Before.CreatedAt, ID: p.Before.ID}, limit+1)
	if err != nil {
		d.logger("[Photon][DMv2] 219 query failed", "error", err.Error())
		return dmFail(errMessageFailed)
	}
	if len(rows) > limit {
		resp.AtStart = false
		rows = rows[len(rows)-limit:] // строки от старых к новым: лишняя — первая
	}
	for _, r := range rows {
		m := DirectMessageV2{ID: r.ID, Text: r.Message, CreatedAt: r.CreateAt}
		if r.UserID == callerID {
			m.AuthorPhotonID = who.ownPhotonID
			m.ClientMsgID = r.ClientMsgID
		} else {
			m.AuthorPhotonID = who.peerPhotonID
		}
		resp.Items = append(resp.Items, m)
	}
	return Response{OK: true, Data: resp}
}

// 219: limit 1..200, по умолчанию 60 (ADR-105 §2.2); константы 218 не трогаем.
const (
	dmBeforeDefaultLimit = 60
	dmBeforeMaxLimit     = 200
)

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
