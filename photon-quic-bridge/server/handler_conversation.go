package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// errCodeTemporarilyUnavailable — код ADR-099 §12: «хранилище или проверка
// схемы недоступны», повторять можно.
const errCodeTemporarilyUnavailable = "temporarily_unavailable"

// convCallTimeout — предел одного вызова 900–902, включая ожидание
// блокировок и вызовы Mattermost.
const convCallTimeout = 60 * time.Second

// ConversationHandler — сотня 900: групповые разговоры ENGRAM (ADR-099).
//
// service == nil — схема Bridge не готова (ADR-100 §2.6): каждый код v1
// отвечает temporarily_unavailable. Реализованы 900–902; остальные коды v1
// до своих срезов отвечают так же — честный отказ, а не ложный успех.
type ConversationHandler struct {
	plugin  *Plugin
	service *convService
}

func newConversationHandler(p *Plugin) *ConversationHandler {
	h := &ConversationHandler{plugin: p}
	if p.convDB != nil {
		h.service = &convService{
			db:        p.convDB,
			mm:        p.API,
			registry:  NewAccountRegistry(p.API),
			users:     p.API,
			botID:     p.convBotID,
			now:       func() int64 { return time.Now().UnixMilli() },
			lockWait:  10 * time.Second,
			pollEvery: 50 * time.Millisecond,
			logger:    p.API.LogWarn,
		}
	}
	return h
}

func (h *ConversationHandler) Handle(userID string, action int, raw json.RawMessage) Response {
	switch action {
	case ActionConversationCreate, ActionConversationGet, ActionConversationList:
		if h.service == nil {
			return Response{OK: false, Error: errCodeTemporarilyUnavailable}
		}
		ctx, cancel := context.WithTimeout(context.Background(), convCallTimeout)
		defer cancel()
		switch action {
		case ActionConversationCreate:
			return h.service.createConversation(ctx, userID, raw)
		case ActionConversationGet:
			return h.service.getConversation(ctx, userID, raw)
		default:
			return h.service.listConversations(ctx, userID, raw)
		}
	case ActionConversationUpdate, ActionConversationArchive,
		ActionConversationMembersAdd, ActionConversationMemberRemove, ActionConversationLeave,
		ActionConversationRoleSet, ActionConversationOwnerTransfer,
		ActionConversationSend, ActionConversationSync:
		return Response{OK: false, Error: errCodeTemporarilyUnavailable}
	default:
		return Response{OK: false, Error: fmt.Sprintf("ConversationResource: unknown action %d", action)}
	}
}
