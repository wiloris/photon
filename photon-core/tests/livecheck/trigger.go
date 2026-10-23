package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	quicadapter "github.com/wiloris/photon-core/adapters/quic"
	photonpb "github.com/wiloris/photon-core/protobuf"
	"github.com/wiloris/photon-core/services/session"
)

// triggerPush порождает push сам, вместо человека у клиента Mattermost.
//
// Зачем: прогон подписки, требующий «отправьте сообщение руками», не
// повторяем и не годится ни для регрессии, ни для проверки в другой среде.
// Сообщение создаётся в самопереписке (direct channel с самим собой) —
// посторонних оно не касается — и удаляется сразу после приёма push.
//
// Отдельное соединение в режиме action — не удобство, а требование
// протокола: push и action не сосуществуют на одном соединении
// (bridge-session.md §2.4).
func triggerPush(tr *quicadapter.Transport) (postID string) {
	s := newSession(tr, session.ModeAction)
	defer s.Close()

	var me struct {
		ID string `json:"id"`
	}
	if err := actionJSON(s, photonpb.Action_ACTION_GET_ME, nil, &me); err != nil {
		bad("trigger: getMe: %v", err)
		return ""
	}

	var ch struct {
		ID string `json:"id"`
	}
	payload := []byte(fmt.Sprintf(`{"target_user_id":%q}`, me.ID))
	if err := actionJSON(s, photonpb.Action_ACTION_GET_OR_CREATE_DIRECT_CHANNEL, payload, &ch); err != nil {
		bad("trigger: getOrCreateDirectChannel: %v", err)
		return ""
	}

	var post struct {
		ID string `json:"id"`
	}
	msg := fmt.Sprintf("photon livecheck %s", time.Now().Format(time.RFC3339))
	payload = []byte(fmt.Sprintf(`{"channel_id":%q,"message":%q}`, ch.ID, msg))
	if err := actionJSON(s, photonpb.Action_ACTION_CREATE_POST, payload, &post); err != nil {
		bad("trigger: createPost: %v", err)
		return ""
	}
	fmt.Printf("  📨 создано сообщение %s в самопереписке %s\n", post.ID, ch.ID)
	return post.ID
}

// cleanupPost удаляет созданное сообщение. Прогон не должен оставлять следов
// в чужой переписке — иначе повторный запуск копит мусор.
func cleanupPost(tr *quicadapter.Transport, postID string) {
	if postID == "" {
		return
	}
	s := newSession(tr, session.ModeAction)
	defer s.Close()
	if err := actionJSON(s, photonpb.Action_ACTION_DELETE_POST,
		[]byte(fmt.Sprintf(`{"post_id":%q}`, postID)), nil); err != nil {
		fmt.Printf("  ⚠️  сообщение %s удалить не удалось: %v\n", postID, err)
		return
	}
	fmt.Printf("  🧹 сообщение %s удалено\n", postID)
}

// actionJSON выполняет действие и разбирает data ответа. ok:false сервера —
// ошибка здесь, а не конверт: это вспомогательный вызов прогона, а не
// проверяемое свойство.
func actionJSON(s *session.Session, action photonpb.Action, payload []byte, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	resp, err := s.Do(ctx, int32(action), payload)
	if err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("сервер отверг действие %d: %s", action, resp.Error)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(resp.Data, out)
}
