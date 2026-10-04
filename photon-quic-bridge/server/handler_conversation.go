package main

import (
	"encoding/json"
	"fmt"
)

// errCodeTemporarilyUnavailable — код ADR-099 §12: «хранилище или проверка
// схемы недоступны», повторять можно.
const errCodeTemporarilyUnavailable = "temporarily_unavailable"

// ConversationHandler — сотня 900: групповые разговоры ENGRAM (ADR-099).
//
// Пока у Bridge нет таблиц разговоров (ADR-100), каждый код v1 отвечает
// temporarily_unavailable — ровно так ADR-100 §2.6 велит отвечать при
// недоступной схеме Bridge. Это честный отказ, а не имитация работы: ни
// одна операция не притворяется выполненной, и клиент, повторяющий запрос,
// не получает ложного успеха. Обработчики появляются вместе с хранилищем
// (implementation contract 900-931, срезы 2-6).
type ConversationHandler struct {
	plugin *Plugin
}

func (h *ConversationHandler) Handle(userID string, action int, raw json.RawMessage) Response {
	switch action {
	case ActionConversationCreate, ActionConversationGet, ActionConversationList,
		ActionConversationUpdate, ActionConversationArchive,
		ActionConversationMembersAdd, ActionConversationMemberRemove, ActionConversationLeave,
		ActionConversationRoleSet, ActionConversationOwnerTransfer,
		ActionConversationSend, ActionConversationSync:
		return Response{OK: false, Error: errCodeTemporarilyUnavailable}
	default:
		return Response{OK: false, Error: fmt.Sprintf("ConversationResource: unknown action %d", action)}
	}
}
