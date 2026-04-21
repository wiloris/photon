package main

import (
	"encoding/json"
	"fmt"

	"github.com/mattermost/mattermost/server/public/model"
)

// WallHandler зеркалит WallResource из Kotlin SDK.
// Стена профиля реализована поверх приватного канала wall_<userID>.
// Это позволяет использовать стандартные posts/files API Mattermost без лишней инфраструктуры.
//
// Actions: 600=GetOrCreateWall, 601=GetWallPosts, 602=PublishWallPost
type WallHandler struct {
	plugin *Plugin
}

func (h *WallHandler) Handle(userID string, action int, raw json.RawMessage) Response {
	switch action {
	case ActionGetOrCreateWall:
		return h.getOrCreateWall(userID, raw)
	case ActionGetWallPosts:
		return h.getWallPosts(userID, raw)
	case ActionPublishWallPost:
		return h.publishWallPost(userID, raw)
	default:
		return Response{OK: false, Error: fmt.Sprintf("WallResource: unknown action %d", action)}
	}
}

// getOrCreateWall — Action 600.
// Возвращает существующий или создаёт новый приватный канал-стену для пользователя.
func (h *WallHandler) getOrCreateWall(userID string, raw json.RawMessage) Response {
	var p struct {
		TeamID string `json:"team_id"`
		UserID string `json:"user_id"` // чья стена; пусто = текущего пользователя
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][WallHandler] JSON unmarshal error (getOrCreateWall)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.TeamID == "" {
		return Response{OK: false, Error: "team_id is required"}
	}
	targetUserID := p.UserID
	if targetUserID == "" {
		targetUserID = userID
	}

	channelName := wallChannelName(targetUserID)

	// Проверяем существование канала-стены.
	ch, appErr := h.plugin.API.GetChannelByName(p.TeamID, channelName, false)
	if appErr == nil {
		h.plugin.API.LogInfo("[Photon][WallHandler] Wall channel found",
			"user_id", userID, "target_id", targetUserID, "channel_id", ch.Id)
		return Response{OK: true, Data: ch}
	}

	// Канал не существует — создаём.
	owner, appErr := h.plugin.API.GetUser(targetUserID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][WallHandler] GetUser API error (getOrCreateWall)",
			"user_id", userID, "target_id", targetUserID, "error", appErr.Error())
		return Response{OK: false, Error: "user not found: " + appErr.Error()}
	}

	displayName := fmt.Sprintf("Wall: %s %s", owner.FirstName, owner.LastName)
	if owner.FirstName == "" && owner.LastName == "" {
		displayName = "Wall: " + owner.Username
	}

	ch, appErr = h.plugin.API.CreateChannel(&model.Channel{
		TeamId:      p.TeamID,
		Name:        channelName,
		DisplayName: displayName,
		Type:        model.ChannelTypePrivate,
		Purpose:     fmt.Sprintf("Profile wall for %s", targetUserID),
		CreatorId:   targetUserID,
	})
	if appErr != nil {
		h.plugin.API.LogError("[Photon][WallHandler] CreateChannel API error (getOrCreateWall)",
			"user_id", userID, "target_id", targetUserID, "error", appErr.Error())
		return Response{OK: false, Error: "failed to create wall channel: " + appErr.Error()}
	}

	// Добавляем владельца в канал.
	if _, appErr := h.plugin.API.AddChannelMember(ch.Id, targetUserID); appErr != nil {
		h.plugin.API.LogWarn("[Photon][WallHandler] Failed to add wall owner as member",
			"channel_id", ch.Id, "target_id", targetUserID, "error", appErr.Error())
	}

	// Сохраняем маппинг userID → channelID для быстрого поиска.
	h.saveWallMapping(targetUserID, ch.Id)

	h.plugin.API.LogInfo("[Photon][WallHandler] Wall channel created",
		"user_id", userID, "target_id", targetUserID, "channel_id", ch.Id)
	return Response{OK: true, Data: ch}
}

// getWallPosts — Action 601. Посты стены с пагинацией.
func (h *WallHandler) getWallPosts(userID string, raw json.RawMessage) Response {
	var p struct {
		ChannelID string `json:"channel_id"`
		Page      int    `json:"page"`
		PerPage   int    `json:"per_page"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][WallHandler] JSON unmarshal error (getWallPosts)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" {
		return Response{OK: false, Error: "channel_id is required"}
	}
	if p.PerPage <= 0 {
		p.PerPage = 20
	}

	list, appErr := h.plugin.API.GetPostsForChannel(p.ChannelID, p.Page, p.PerPage)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][WallHandler] GetPostsForChannel API error (getWallPosts)",
			"user_id", userID, "channel_id", p.ChannelID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}

	h.plugin.API.LogInfo("[Photon][WallHandler] Wall posts fetched",
		"user_id", userID, "channel_id", p.ChannelID,
		"page", p.Page, "count", len(list.Posts))
	return Response{OK: true, Data: map[string]interface{}{
		"posts":    list,
		"has_more": len(list.Order) == p.PerPage,
	}}
}

// publishWallPost — Action 602.
// Атомарная операция: загружает файлы и создаёт пост за один вызов.
// Если загрузка хотя бы одного файла падает — пост не создаётся.
func (h *WallHandler) publishWallPost(userID string, raw json.RawMessage) Response {
	var p struct {
		ChannelID string `json:"channel_id"`
		Message   string `json:"message"`
		Files     []struct {
			Name  string `json:"name"`
			Bytes []byte `json:"bytes"`
		} `json:"files,omitempty"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][WallHandler] JSON unmarshal error (publishWallPost)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" {
		return Response{OK: false, Error: "channel_id is required"}
	}
	if p.Message == "" && len(p.Files) == 0 {
		return Response{OK: false, Error: "message or at least one file is required"}
	}

	// Загружаем файлы первыми — атомарность через ранний выход при ошибке.
	fileIDs := make([]string, 0, len(p.Files))
	for _, f := range p.Files {
		if len(f.Bytes) == 0 || f.Name == "" {
			return Response{OK: false, Error: "each file must have name and bytes"}
		}
		info, appErr := h.plugin.API.UploadFile(f.Bytes, p.ChannelID, f.Name)
		if appErr != nil {
			h.plugin.API.LogError("[Photon][WallHandler] UploadFile API error (publishWallPost)",
				"user_id", userID, "file_name", f.Name, "error", appErr.Error())
			return Response{OK: false, Error: fmt.Sprintf("failed to upload %s: %s", f.Name, appErr.Error())}
		}
		fileIDs = append(fileIDs, info.Id)
	}

	// Создаём пост с прикреплёнными файлами.
	post := &model.Post{
		UserId:    userID,
		ChannelId: p.ChannelID,
		Message:   p.Message,
		FileIds:   fileIDs,
	}
	created, appErr := h.plugin.API.CreatePost(post)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][WallHandler] CreatePost API error (publishWallPost)",
			"user_id", userID, "channel_id", p.ChannelID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}

	h.plugin.API.LogInfo("[Photon][WallHandler] Wall post published",
		"user_id", userID, "post_id", created.Id,
		"channel_id", p.ChannelID, "files", len(fileIDs))
	return Response{OK: true, Data: created}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// wallChannelName строит имя канала-стены.
// Mattermost: имя канала строчное, max 64 символа, без пробелов.
func wallChannelName(userID string) string {
	name := "wall_" + userID
	if len(name) > 64 {
		name = name[:64]
	}
	return name
}

func (h *WallHandler) saveWallMapping(userID, channelID string) {
	key := fmt.Sprintf("wall_map:%s", userID)
	data, _ := json.Marshal(map[string]string{"channel_id": channelID})
	if appErr := h.plugin.API.KVSet(key, data); appErr != nil {
		h.plugin.API.LogWarn("[Photon][WallHandler] Failed to save wall mapping",
			"user_id", userID, "channel_id", channelID, "error", appErr.Error())
	}
}