package main

import (
	"encoding/json"
	"fmt"
)

// Разбор ленты и извлечение АВТОРИТЕТНОГО описания вложения.
//
// Зачем отдельный шаг, а не «мы же помним, что загружали»: то, что клиент
// помнит о своей загрузке, ничего не доказывает о сообщении. Единственное
// авторитетное описание вложения — props поста, каким его отдаёт сервер
// (ADR-048 §3). Поэтому скачивание идёт по media_id, взятому ИЗ ЛЕНТЫ, а не
// из ответа на загрузку.

// feedMedia — запись photon_media в props поста.
type feedMedia struct {
	MediaID string `json:"media_id"`
	SHA256  string `json:"sha256"`
	Size    int64  `json:"size"`
	Mime    string `json:"mime,omitempty"`
	Name    string `json:"name,omitempty"`
}

// postEnvelope — то немногое из поста, что нужно прогону.
type postEnvelope struct {
	ID    string                     `json:"id"`
	Props map[string]json.RawMessage `json:"props"`
}

// findPostInFeed достаёт пост из ответа getFeed (model.PostList:
// {"order":[...],"posts":{id:{...}}}).
func findPostInFeed(feed []byte, postID string) (json.RawMessage, error) {
	var list struct {
		Order []string                   `json:"order"`
		Posts map[string]json.RawMessage `json:"posts"`
	}
	if err := json.Unmarshal(feed, &list); err != nil {
		return nil, fmt.Errorf("лента не разбирается: %w", err)
	}
	if list.Posts == nil {
		return nil, fmt.Errorf("в ленте нет поля posts")
	}
	p, ok := list.Posts[postID]
	if !ok {
		return nil, fmt.Errorf("поста %s нет в ленте (в ней %d сообщений)", postID, len(list.Posts))
	}
	return p, nil
}

// mediaFromPost извлекает photon_media из props.
//
// Отсутствие props или ключа — ошибка, а не пустой список: пост с вложением,
// у которого props пусты, означает потерянное вложение, и молча вернуть ноль
// записей значило бы выдать потерю за норму.
func mediaFromPost(post json.RawMessage) ([]feedMedia, error) {
	var env postEnvelope
	if err := json.Unmarshal(post, &env); err != nil {
		return nil, fmt.Errorf("пост не разбирается: %w", err)
	}
	if env.Props == nil {
		return nil, fmt.Errorf("у поста нет props")
	}
	raw, ok := env.Props["photon_media"]
	if !ok {
		return nil, fmt.Errorf("в props нет photon_media (ключи: %v)", propKeys(env.Props))
	}
	var refs []feedMedia
	if err := json.Unmarshal(raw, &refs); err != nil {
		return nil, fmt.Errorf("photon_media не разбирается как список: %w", err)
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("photon_media пуст")
	}
	return refs, nil
}

func propKeys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
