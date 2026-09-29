package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"

	"github.com/mattermost/mattermost/server/public/model"
)

// Входящие: список существующих разговоров 1:1 — ADR-084.
//
// Источник списка, а не окончательная модель переписки: группы, общий слой
// Conversation, события и push — отдельные решения (ADR-084, «Граница
// решения»).
//
// Личные каналы вызывающего берутся штатным GetChannelsForTeamForUser с
// пустым teamId: Mattermost 9.11 тогда не фильтрует по команде и отдаёт все
// каналы участия, включая личные (у пользователей engram-* команд нет).
// Наружу — только PhotonID и данные сообщения; id и имя канала,
// mm_user_id не выходят (ADR-084 §2.4).

const (
	directConversationsDefaultN = 60  // REST Mattermost PerPageDefault
	directConversationsMaximumN = 200 // REST Mattermost PerPageMaximum
	// lastMessageWindow — сколько свежих постов канала смотреть, чтобы найти
	// последнее публичное сообщение среди системных.
	lastMessageWindow = 20
)

// DirectConversation — разговор 1:1 (ADR-084 §2.1).
type DirectConversation struct {
	PeerPhotonID string        `json:"peer_photon_id"`
	LastMessage  DirectMessage `json:"last_message"`
}

type listDirectConversationsPayload struct {
	Limit *int `json:"limit"`
}

// conversationAPI — операции Mattermost, нужные списку. Узкий интерфейс —
// граница для тестового дублёра.
type conversationAPI interface {
	GetUser(userID string) (*model.User, *model.AppError)
	GetChannelsForTeamForUser(teamID, userID string, includeDeleted bool) ([]*model.Channel, *model.AppError)
	GetPostsForChannel(channelID string, page, perPage int) (*model.PostList, *model.AppError)
}

type conversationDeps struct {
	registry photonAccountFinder
	api      conversationAPI
	logger   func(msg string, keyValuePairs ...any)
}

// listDirectConversations — Action 215.
func listDirectConversations(d conversationDeps, callerID string, raw json.RawMessage) Response {
	var p listDirectConversationsPayload
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &p); err != nil {
			return dmFail(errInvalidLimit)
		}
	}
	limit := directConversationsDefaultN
	if p.Limit != nil {
		limit = *p.Limit
	}
	if limit < 1 || limit > directConversationsMaximumN {
		return dmFail(errInvalidLimit)
	}

	own, _, found, err := ownPhotonID(d.registry, d.api, callerID)
	if err != nil {
		d.logger("[Photon][Inbox] own PhotonID lookup failed", "error", err.Error())
		return dmFail(errMessageFailed)
	}
	if !found {
		return dmFail(errSenderHasNoPhotonID)
	}

	// Пустой teamId — все каналы участия, без членства в команде (ADR-084 §1).
	channels, appErr := d.api.GetChannelsForTeamForUser("", callerID, false)
	if appErr != nil {
		d.logger("[Photon][Inbox] GetChannelsForTeamForUser failed", "error", appErr.Error())
		return dmFail(errMessageFailed)
	}
	direct := make([]*model.Channel, 0, len(channels))
	for _, ch := range channels {
		if ch != nil && ch.Type == model.ChannelTypeDirect {
			direct = append(direct, ch)
		}
	}
	sort.SliceStable(direct, func(i, j int) bool { return direct[i].LastPostAt > direct[j].LastPostAt })

	out := []DirectConversation{}
	for _, ch := range direct {
		if len(out) == limit {
			break
		}
		conv, ok, err := conversationOf(d, ch, callerID, own)
		if err != nil {
			d.logger("[Photon][Inbox] conversation lookup failed", "error", err.Error())
			return dmFail(errMessageFailed)
		}
		if ok {
			out = append(out, conv)
		}
	}
	// LastPostAt учитывает и системные посты; окончательный порядок — по
	// времени последнего публичного сообщения (ADR-084 §2.1).
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].LastMessage.CreatedAt > out[j].LastMessage.CreatedAt
	})
	return Response{OK: true, Data: map[string][]DirectConversation{"conversations": out}}
}

// conversationOf — разговор по личному каналу. ok=false — канал в список не
// входит: собеседник не подтверждён реестром (не ENGRAM-вход, поддельное
// зеркало), деактивирован либо исчез, или публичных сообщений нет.
func conversationOf(d conversationDeps, ch *model.Channel, callerID, own string) (DirectConversation, bool, error) {
	peerID := ch.GetOtherUserIdForDM(callerID)
	if peerID == "" {
		// Для личного канала с собой GetBothUsersForDM возвращает
		// ("<id>", ""), и GetOtherUserIdForDM даёт пустую строку — без этой
		// ветки разговор с собой молча выпадал бы из списка (ADR-084 §2.3).
		first, second := ch.GetBothUsersForDM()
		if first != callerID || second != "" {
			return DirectConversation{}, false, nil
		}
		peerID = callerID
	}

	// С собой — свой PhotonID (ADR-084 §2.3).
	peerPhotonID := own
	if peerID != callerID {
		pid, peer, found, err := ownPhotonID(d.registry, d.api, peerID)
		if err != nil {
			var appErr *model.AppError
			if errors.As(err, &appErr) && appErr.StatusCode == http.StatusNotFound {
				return DirectConversation{}, false, nil
			}
			return DirectConversation{}, false, err
		}
		if !found || peer == nil || peer.DeleteAt != 0 {
			return DirectConversation{}, false, nil
		}
		peerPhotonID = pid
	}

	list, appErr := d.api.GetPostsForChannel(ch.Id, 0, lastMessageWindow)
	if appErr != nil {
		return DirectConversation{}, false, appErr
	}
	msgs := publicMessages(list, callerID, dmParticipants{ownPhotonID: own, peerPhotonID: peerPhotonID, peerUserID: peerID})
	if len(msgs) == 0 {
		return DirectConversation{}, false, nil
	}
	return DirectConversation{PeerPhotonID: peerPhotonID, LastMessage: msgs[len(msgs)-1]}, true, nil
}

func (h *UserHandler) conversationDeps() conversationDeps {
	return conversationDeps{
		registry: NewAccountRegistry(h.plugin.API),
		api:      h.plugin.API,
		logger:   h.plugin.API.LogWarn,
	}
}
