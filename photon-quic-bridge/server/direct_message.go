package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/mattermost/mattermost/server/public/model"
)

// Личные сообщения по PhotonID — ADR-082.
//
// Собеседник задаётся только PhotonID; сервер разрешает его в пользователя
// Mattermost (реестр, ADR-081) и в личный канал. Наружу выходят PhotonID,
// текст, время и непрозрачный id сообщения — никогда mm_user_id, id или имя
// канала (оно <mm_user_id>__<mm_user_id>), login_name, email, Subject.
//
// Авторизация — в Authorize (objDirectPeer, authz.go); здесь решений о
// правах нет.

const (
	errMessageEmpty         = "message_empty"
	errMessageTooLong       = "message_too_long"
	errInvalidCursor        = "invalid_cursor"
	errInvalidLimit         = "invalid_limit"
	errSenderHasNoPhotonID  = "sender_has_no_photon_id"
	errMessageFailed        = "message_failed"
	directMessageMaxRunes   = model.PostMessageMaxRunesV2 // 16383, ADR-082 §3.4
	directMessagesDefaultN  = 60                          // REST Mattermost PerPageDefault
	directMessagesMaximumN  = 200                         // REST Mattermost PerPageMaximum
	mmPostTooLongErrorID    = "model.post.is_valid.msg.app_error"
	mmChannelNotFoundStatus = http.StatusNotFound
)

// DirectMessage — публичное сообщение (ADR-082 §3.2).
type DirectMessage struct {
	ID             string `json:"id"`
	AuthorPhotonID string `json:"author_photon_id"`
	Text           string `json:"text"`
	CreatedAt      int64  `json:"created_at"`
}

type sendDirectMessagePayload struct {
	ToPhotonID string `json:"to_photon_id"`
	Text       string `json:"text"`
}

type listDirectMessagesPayload struct {
	WithPhotonID string `json:"with_photon_id"`
	After        string `json:"after"`
	Limit        *int   `json:"limit"`
}

// directMessageAPI — операции Mattermost, нужные личным сообщениям. Узкий
// интерфейс, а не plugin.API: он же граница для тестового дублёра.
type directMessageAPI interface {
	GetUser(userID string) (*model.User, *model.AppError)
	GetDirectChannel(userID1, userID2 string) (*model.Channel, *model.AppError)
	GetChannelByName(teamID, name string, includeDeleted bool) (*model.Channel, *model.AppError)
	CreatePost(post *model.Post) (*model.Post, *model.AppError)
	GetPost(postID string) (*model.Post, *model.AppError)
	GetPostsForChannel(channelID string, page, perPage int) (*model.PostList, *model.AppError)
	GetPostsAfter(channelID, postID string, page, perPage int) (*model.PostList, *model.AppError)
}

type directMessageDeps struct {
	registry photonAccountFinder
	api      directMessageAPI
	logger   func(msg string, keyValuePairs ...any)
}

func dmFail(code string) Response { return Response{OK: false, Error: code} }

// participants — подтверждённый собственный PhotonID вызывающего и
// собеседник по его PhotonID (ADR-082 §3.3). Пустой code — успех.
type dmParticipants struct {
	ownPhotonID  string
	peerPhotonID string
	peerUserID   string
}

func resolveParticipants(d directMessageDeps, callerID, peerPhotonID string) (dmParticipants, string) {
	if !isCanonicalPhotonID(peerPhotonID) {
		return dmParticipants{}, errInvalidPhotonID
	}
	own, _, found, err := ownPhotonID(d.registry, d.api, callerID)
	if err != nil {
		d.logger("[Photon][DM] own PhotonID lookup failed", "error", err.Error())
		return dmParticipants{}, errMessageFailed
	}
	if !found {
		return dmParticipants{}, errSenderHasNoPhotonID
	}

	rec, found, err := d.registry.FindByPhotonID(peerPhotonID)
	if err != nil {
		d.logger("[Photon][DM] registry failure", "error", err.Error())
		return dmParticipants{}, errMessageFailed
	}
	if !found {
		return dmParticipants{}, errPhotonIDNotFound
	}
	// Деактивированный или исчезнувший — тот же «не найдено», что в ADR-081.
	peer, appErr := d.api.GetUser(rec.MMUserID)
	if appErr != nil {
		if appErr.StatusCode == http.StatusNotFound {
			return dmParticipants{}, errPhotonIDNotFound
		}
		d.logger("[Photon][DM] GetUser failed", "error", appErr.Error())
		return dmParticipants{}, errMessageFailed
	}
	if peer.DeleteAt != 0 {
		return dmParticipants{}, errPhotonIDNotFound
	}
	return dmParticipants{ownPhotonID: own, peerPhotonID: peerPhotonID, peerUserID: rec.MMUserID}, ""
}

// sendDirectMessage — Action 213.
func sendDirectMessage(d directMessageDeps, callerID string, raw json.RawMessage) Response {
	var p sendDirectMessagePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return dmFail(errInvalidPhotonID)
	}
	if !isCanonicalPhotonID(p.ToPhotonID) {
		return dmFail(errInvalidPhotonID)
	}
	// Текст проверяется ДО обращения к Mattermost (ADR-082 §3.4) и
	// хранится без изменений: ни обрезки, ни нормализации.
	if strings.TrimSpace(p.Text) == "" {
		return dmFail(errMessageEmpty)
	}
	if utf8.RuneCountInString(p.Text) > directMessageMaxRunes {
		return dmFail(errMessageTooLong)
	}

	who, code := resolveParticipants(d, callerID, p.ToPhotonID)
	if code != "" {
		return dmFail(code)
	}

	// Создаётся при первом сообщении; с самим собой Mattermost его
	// допускает штатно (ADR-082 §3.5).
	ch, appErr := d.api.GetDirectChannel(callerID, who.peerUserID)
	if appErr != nil || ch == nil {
		if appErr != nil {
			d.logger("[Photon][DM] GetDirectChannel failed", "error", appErr.Error())
		}
		return dmFail(errMessageFailed)
	}

	post, appErr := d.api.CreatePost(&model.Post{UserId: callerID, ChannelId: ch.Id, Message: p.Text})
	if appErr != nil {
		// Установка с меньшим столбцом posts.message отвергнет пост по
		// длине сама — ответ тот же (ADR-082 §3.4).
		if appErr.Id == mmPostTooLongErrorID {
			return dmFail(errMessageTooLong)
		}
		d.logger("[Photon][DM] CreatePost failed", "error", appErr.Error())
		return dmFail(errMessageFailed)
	}
	return Response{OK: true, Data: map[string]DirectMessage{
		"message": {ID: post.Id, AuthorPhotonID: who.ownPhotonID, Text: post.Message, CreatedAt: post.CreateAt},
	}}
}

// listDirectMessages — Action 214.
func listDirectMessages(d directMessageDeps, callerID string, raw json.RawMessage) Response {
	var p listDirectMessagesPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return dmFail(errInvalidPhotonID)
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

	who, code := resolveParticipants(d, callerID, p.WithPhotonID)
	if code != "" {
		return dmFail(code)
	}

	// Поиск БЕЗ создания (ADR-082 §3.3): чтение не заводит канал.
	ch, appErr := d.api.GetChannelByName("", model.GetDMNameFromIds(callerID, who.peerUserID), false)
	if appErr != nil && appErr.StatusCode != mmChannelNotFoundStatus {
		d.logger("[Photon][DM] GetChannelByName failed", "error", appErr.Error())
		return dmFail(errMessageFailed)
	}
	if appErr != nil || ch == nil {
		// Канала нет — сообщений нет; курсора из него быть не может.
		if p.After != "" {
			return dmFail(errInvalidCursor)
		}
		return Response{OK: true, Data: map[string][]DirectMessage{"messages": {}}}
	}

	var list *model.PostList
	if p.After != "" {
		// Курсор обязан быть сообщением ЭТОГО разговора: GetPostsAfter на
		// неизвестный id молча отдаёт пустоту, и клиент с негодным курсором
		// иначе никогда не увидел бы новых сообщений (ADR-082 §3.6).
		cursor, appErr := d.api.GetPost(p.After)
		if appErr != nil || cursor == nil || cursor.ChannelId != ch.Id {
			return dmFail(errInvalidCursor)
		}
		list, appErr = d.api.GetPostsAfter(ch.Id, p.After, 0, limit)
		if appErr != nil {
			d.logger("[Photon][DM] GetPostsAfter failed", "error", appErr.Error())
			return dmFail(errMessageFailed)
		}
	} else {
		list, appErr = d.api.GetPostsForChannel(ch.Id, 0, limit)
		if appErr != nil {
			d.logger("[Photon][DM] GetPostsForChannel failed", "error", appErr.Error())
			return dmFail(errMessageFailed)
		}
	}
	return Response{OK: true, Data: map[string][]DirectMessage{
		"messages": publicMessages(list, callerID, who),
	}}
}

// publicMessages — посты канала → публичные сообщения по возрастанию
// времени. Автор определяется без поиска: в личном канале участников двое
// (ADR-082 §3.3). Системные посты, удалённые и посты прочих авторов не
// выдаются.
func publicMessages(list *model.PostList, callerID string, who dmParticipants) []DirectMessage {
	out := []DirectMessage{}
	if list == nil {
		return out
	}
	for _, post := range list.Posts {
		if post == nil || post.Type != "" || post.DeleteAt != 0 {
			continue
		}
		var author string
		switch post.UserId {
		case callerID:
			author = who.ownPhotonID
		case who.peerUserID:
			author = who.peerPhotonID
		default:
			continue
		}
		out = append(out, DirectMessage{ID: post.Id, AuthorPhotonID: author, Text: post.Message, CreatedAt: post.CreateAt})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt < out[j].CreatedAt
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func (h *UserHandler) directMessageDeps() directMessageDeps {
	return directMessageDeps{
		registry: NewAccountRegistry(h.plugin.API),
		api:      h.plugin.API,
		logger:   h.plugin.API.LogWarn,
	}
}
