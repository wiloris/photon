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

// convCallTimeout — предел одного вызова 900–930, включая ожидание
// блокировок и вызовы Mattermost.
const convCallTimeout = 60 * time.Second

// ConversationHandler — сотня 900: групповые разговоры ENGRAM (ADR-099).
//
// service == nil — схема Bridge не готова (ADR-100 §2.6): каждый код v1
// отвечает temporarily_unavailable. Реализованы 900–921, 930 и 931.
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
	case ActionConversationCreate, ActionConversationGet, ActionConversationList,
		ActionConversationUpdate, ActionConversationArchive,
		ActionConversationMembersAdd, ActionConversationMemberRemove, ActionConversationLeave,
		ActionConversationRoleSet, ActionConversationOwnerTransfer, ActionConversationSend,
		ActionConversationSync:
		if h.service == nil {
			return Response{OK: false, Error: errCodeTemporarilyUnavailable}
		}
		ctx, cancel := context.WithTimeout(context.Background(), convCallTimeout)
		defer cancel()
		return h.service.dispatch(ctx, userID, action, raw)
	default:
		return Response{OK: false, Error: fmt.Sprintf("ConversationResource: unknown action %d", action)}
	}
}

// dispatch — реализованные действия сервиса.
func (s *convService) dispatch(ctx context.Context, userID string, action int, raw json.RawMessage) Response {
	switch action {
	case ActionConversationCreate:
		return s.createConversation(ctx, userID, raw)
	case ActionConversationGet:
		return s.getConversation(ctx, userID, raw)
	case ActionConversationList:
		return s.listConversations(ctx, userID, raw)
	case ActionConversationUpdate:
		return s.updateConversation(ctx, userID, raw)
	case ActionConversationArchive:
		return s.archiveConversation(ctx, userID, raw)
	case ActionConversationMembersAdd:
		return s.addMembers(ctx, userID, raw)
	case ActionConversationMemberRemove:
		return s.removeMember(ctx, userID, raw)
	case ActionConversationLeave:
		return s.leaveConversation(ctx, userID, raw)
	case ActionConversationRoleSet:
		return s.setRole(ctx, userID, raw)
	case ActionConversationOwnerTransfer:
		return s.transferOwner(ctx, userID, raw)
	case ActionConversationSend:
		return s.sendMessage(ctx, userID, raw)
	case ActionConversationSync:
		return s.syncConversation(ctx, userID, raw)
	}
	return Response{OK: false, Error: fmt.Sprintf("ConversationResource: unknown action %d", action)}
}
