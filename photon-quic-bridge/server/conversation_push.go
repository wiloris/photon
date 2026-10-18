package main

import (
	"context"
	"encoding/json"
	"time"
)

// Push разговоров ENGRAM — ADR-123 §2.2; docs/conversation-push.md §3.
//
// Сообщения разговоров — посты приватного канала-носителя, членами которого
// участники не являются (ADR-100). Хук MessageHasBeenPosted для них
// срабатывает, но PushRouter по read(C) отдаёт событие никому — и правильно:
// пост с текстом подписчику не нужен. Аудиторию события разговора считает
// этот файл по тем же таблицам, что разрешают чтение 931 (callerAccess):
// активный период участия.
//
// Событие без тела (ADR-040, вариант C для этого класса): только
// conversation_id и повод. Всё, что покажет уведомление, клиент берёт из
// 902 по своей сессии — то есть через обычную авторизацию чтения.
//
// Fail-closed: ошибка SQL — не отправляется никому; PhotonID, который не
// переводится в пользователя Mattermost, — не отправляется этому получателю.
// Исход 930/908 от push не зависит: сообщение уже опубликовано, push —
// ускоритель (ADR-046 §1).

const (
	convPushReasonMessage = "message"
	convPushReasonRead    = "read"

	// convPushTimeout — бюджет вычисления аудитории одного события.
	convPushTimeout = 5 * time.Second
)

type convActivityData struct {
	ConversationID string `json:"conversation_id"`
	Reason         string `json:"reason"`
}

// convAudience — получатели события «новое сообщение»: активные участники
// разговора, кроме автора и тех, кто автора заблокировал (ADR-111).
const convAudienceSQL = `SELECT m.photon_id FROM engram_member_period m
WHERE m.conversation_id = $1 AND m.state = 'active' AND m.photon_id <> $2
  AND NOT EXISTS (SELECT 1 FROM engram_user_block b
                  WHERE b.blocker_pid = m.photon_id AND b.blocked_pid = $2)
ORDER BY m.photon_id`

// pushMessage — событие после публикации 930. Вызывается только после
// условного перехода reserved → published (contract §3.5): раньше клиент,
// пришедший по событию с 902, не увидел бы сообщение за барьером резерва.
func (s *convService) pushMessage(convID, authorPID string) {
	if s.push == nil {
		return
	}
	s.spawn(func() {
		ctx, cancel := context.WithTimeout(context.Background(), convPushTimeout)
		defer cancel()
		for _, mmID := range s.messageAudience(ctx, convID, authorPID) {
			s.push(convActivityEvent(mmID, convID, convPushReasonMessage))
		}
	})
}

// pushRead — событие после 908, сдвинувшего позицию прочтения: только самому
// читателю (другим его устройствам — снять уведомление). Чужое прочтение
// остальным участникам не сообщается.
func (s *convService) pushRead(convID, readerMMID string) {
	if s.push == nil || readerMMID == "" {
		return
	}
	s.spawn(func() { s.push(convActivityEvent(readerMMID, convID, convPushReasonRead)) })
}

// messageAudience — пользователи Mattermost, которым уходит событие.
func (s *convService) messageAudience(ctx context.Context, convID, authorPID string) []string {
	rows, err := s.db.QueryContext(ctx, convAudienceSQL, convID, authorPID)
	if err != nil {
		s.logger("[Photon][Push] conversation audience failed", "conversation_id", convID, "error", err.Error())
		return nil
	}
	defer rows.Close()
	var pids []string
	for rows.Next() {
		var pid string
		if err := rows.Scan(&pid); err != nil {
			s.logger("[Photon][Push] conversation audience scan failed", "conversation_id", convID, "error", err.Error())
			return nil
		}
		pids = append(pids, pid)
	}
	if err := rows.Err(); err != nil {
		s.logger("[Photon][Push] conversation audience failed", "conversation_id", convID, "error", err.Error())
		return nil
	}
	out := make([]string, 0, len(pids))
	for _, pid := range pids {
		rec, found, err := s.registry.FindByPhotonID(pid)
		if err != nil || !found || rec.MMUserID == "" {
			continue
		}
		out = append(out, rec.MMUserID)
	}
	return out
}

func convActivityEvent(to, convID, reason string) PushEvent {
	data, _ := json.Marshal(convActivityData{ConversationID: convID, Reason: reason})
	return PushEvent{Event: PushEventConversationActivity, To: to, Data: data}
}

// spawn — вычисление аудитории вне вызова 930/908: SQL и реестр не должны
// удлинять ответ отправителю. Тесты подставляют синхронный запуск.
func (s *convService) spawn(f func()) {
	if s.runPush != nil {
		s.runPush(f)
		return
	}
	go f()
}
