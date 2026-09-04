package main

import (
	"encoding/json"
	"fmt"

	"github.com/mattermost/mattermost/server/public/model"
)

// UserHandler зеркалит UserResource из Kotlin SDK.
// Actions: 200=GetMe, 201=GetUser, 202=GetUsers, 203=GetUserStatus,
//
//	204=GetUserProfile, 205=SearchUsers, 206=GetUsersByIds,
//	207=PatchMe, 208=SetStatus, 209=UploadAvatar, 210=GetUserAvatar
type UserHandler struct {
	plugin *Plugin
}

func (h *UserHandler) Handle(userID string, action int, raw json.RawMessage) Response {
	switch action {
	case ActionGetMe:
		return h.getMe(userID, raw)
	case ActionGetUser:
		return h.getUser(userID, raw)
	case ActionGetUsers:
		return h.getUsers(userID, raw)
	case ActionGetUserStatus:
		return h.getUserStatus(userID, raw)
	case ActionGetUserProfile:
		return h.getUserProfile(userID, raw)
	case ActionSearchUsers:
		return h.searchUsers(userID, raw)
	case ActionGetUsersByIds:
		return h.getUsersByIds(userID, raw)
	case ActionPatchMe:
		return h.patchMe(userID, raw)
	case ActionSetStatus:
		return h.setStatus(userID, raw)
	case ActionUploadAvatar:
		return h.uploadAvatar(userID, raw)
	case ActionGetUserAvatar:
		return h.getUserAvatar(userID, raw)
	default:
		return Response{OK: false, Error: fmt.Sprintf("UserResource: unknown action %d", action)}
	}
}

// getMe — Action 200.
func (h *UserHandler) getMe(userID string, _ json.RawMessage) Response {
	user, appErr := h.plugin.API.GetUser(userID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][UserHandler] GetUser API error (getMe)",
			"user_id", userID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	user = h.plugin.sanitizeUser(user, userID)
	h.plugin.API.LogInfo("[Photon][UserHandler] getMe fetched", "user_id", userID)
	return Response{OK: true, Data: user}
}

// getUser — Action 201.
func (h *UserHandler) getUser(userID string, raw json.RawMessage) Response {
	var p GetUserProfilePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][UserHandler] JSON unmarshal error (getUser)",
			"caller_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.UserID == "" {
		return Response{OK: false, Error: "user_id is required"}
	}
	user, appErr := h.plugin.API.GetUser(p.UserID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][UserHandler] GetUser API error (getUser)",
			"caller_id", userID, "target_id", p.UserID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	user = h.plugin.sanitizeUser(user, userID)
	h.plugin.API.LogInfo("[Photon][UserHandler] User fetched",
		"caller_id", userID, "target_id", p.UserID)
	return Response{OK: true, Data: user}
}

// getUsers — Action 202.
func (h *UserHandler) getUsers(userID string, raw json.RawMessage) Response {
	var p struct {
		Page    int `json:"page"`
		PerPage int `json:"per_page"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][UserHandler] JSON unmarshal error (getUsers)",
			"caller_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.PerPage <= 0 {
		p.PerPage = 50
	}
	users, appErr := h.plugin.API.GetUsers(&model.UserGetOptions{
		Page:    p.Page,
		PerPage: p.PerPage,
	})
	if appErr != nil {
		h.plugin.API.LogError("[Photon][UserHandler] GetUsers API error",
			"caller_id", userID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	users = h.plugin.sanitizeUsers(users, userID)
	h.plugin.API.LogInfo("[Photon][UserHandler] Users listed",
		"caller_id", userID, "count", len(users))
	return Response{OK: true, Data: users}
}

// getUserStatus — Action 203.
func (h *UserHandler) getUserStatus(userID string, raw json.RawMessage) Response {
	var p GetUserStatusPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][UserHandler] JSON unmarshal error (getUserStatus)",
			"caller_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	targetID := p.UserID
	if targetID == "" {
		targetID = userID
	}
	status, appErr := h.plugin.API.GetUserStatus(targetID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][UserHandler] GetUserStatus API error",
			"caller_id", userID, "target_id", targetID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][UserHandler] Status fetched",
		"caller_id", userID, "target_id", targetID, "status", status.Status)
	return Response{OK: true, Data: status}
}

// getUserProfile — Action 204.
func (h *UserHandler) getUserProfile(userID string, raw json.RawMessage) Response {
	var p GetUserProfilePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][UserHandler] JSON unmarshal error (getUserProfile)",
			"caller_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	targetID := p.UserID
	if targetID == "" {
		targetID = userID
	}
	user, appErr := h.plugin.API.GetUser(targetID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][UserHandler] GetUser API error (getUserProfile)",
			"caller_id", userID, "target_id", targetID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	user = h.plugin.sanitizeUser(user, userID)
	h.plugin.API.LogInfo("[Photon][UserHandler] Profile fetched",
		"caller_id", userID, "target_id", targetID)
	return Response{OK: true, Data: user}
}

// searchUsers — Action 205.
func (h *UserHandler) searchUsers(userID string, raw json.RawMessage) Response {
	var p SearchUsersPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][UserHandler] JSON unmarshal error (searchUsers)",
			"caller_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.Term == "" {
		return Response{OK: false, Error: "term is required"}
	}
	if p.Limit <= 0 {
		p.Limit = 20
	}
	users, appErr := h.plugin.API.SearchUsers(&model.UserSearch{
		Term:   p.Term,
		TeamId: p.InTeam,
		Limit:  p.Limit,
	})
	if appErr != nil {
		h.plugin.API.LogError("[Photon][UserHandler] SearchUsers API error",
			"caller_id", userID, "term", p.Term, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	users = h.plugin.sanitizeUsers(users, userID)
	h.plugin.API.LogInfo("[Photon][UserHandler] Users searched",
		"caller_id", userID, "term", p.Term, "results", len(users))
	return Response{OK: true, Data: users}
}

// getUsersByIds — Action 206.
// Plugin API v11 не имеет InUsers в UserGetOptions —
// получаем каждого пользователя через GetUser индивидуально.
func (h *UserHandler) getUsersByIds(userID string, raw json.RawMessage) Response {
	var p struct {
		UserIDs []string `json:"user_ids"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][UserHandler] JSON unmarshal error (getUsersByIds)",
			"caller_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if len(p.UserIDs) == 0 {
		return Response{OK: false, Error: "user_ids is required"}
	}
	users := make([]*model.User, 0, len(p.UserIDs))
	for _, uid := range p.UserIDs {
		u, appErr := h.plugin.API.GetUser(uid)
		if appErr != nil {
			h.plugin.API.LogWarn("[Photon][UserHandler] GetUser error (getUsersByIds)",
				"caller_id", userID, "target_id", uid, "error", appErr.Error())
			continue // пропускаем ненайденных, не прерываем батч
		}
		users = append(users, u)
	}
	users = h.plugin.sanitizeUsers(users, userID)
	h.plugin.API.LogInfo("[Photon][UserHandler] Users fetched by ids",
		"caller_id", userID, "requested", len(p.UserIDs), "found", len(users))
	return Response{OK: true, Data: users}
}

// patchMe — Action 207.
// Plugin API v11 не имеет PatchUser — используем UpdateUser с полным объектом.
func (h *UserHandler) patchMe(userID string, raw json.RawMessage) Response {
	var p struct {
		FirstName string            `json:"first_name,omitempty"`
		LastName  string            `json:"last_name,omitempty"`
		Nickname  string            `json:"nickname,omitempty"`
		Username  string            `json:"username,omitempty"`
		Props     map[string]string `json:"props,omitempty"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][UserHandler] JSON unmarshal error (patchMe)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	// Загружаем текущий профиль и обновляем только переданные поля.
	user, appErr := h.plugin.API.GetUser(userID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][UserHandler] GetUser API error (patchMe)",
			"user_id", userID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	if p.FirstName != "" {
		user.FirstName = p.FirstName
	}
	if p.LastName != "" {
		user.LastName = p.LastName
	}
	if p.Nickname != "" {
		user.Nickname = p.Nickname
	}
	if p.Username != "" {
		user.Username = p.Username
	}
	if p.Props != nil {
		if user.Props == nil {
			user.Props = make(model.StringMap)
		}
		for k, v := range p.Props {
			user.Props[k] = v
		}
	}
	updated, appErr := h.plugin.API.UpdateUser(user)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][UserHandler] UpdateUser API error (patchMe)",
			"user_id", userID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	updated = h.plugin.sanitizeUser(updated, userID)
	h.plugin.API.LogInfo("[Photon][UserHandler] Profile patched", "user_id", userID)
	return Response{OK: true, Data: updated}
}

// setStatus — Action 208.
// UpdateUserStatus(userID string, status string) — принимает строку, не *model.Status.
func (h *UserHandler) setStatus(userID string, raw json.RawMessage) Response {
	var p struct {
		Status string `json:"status"` // "online" | "away" | "dnd" | "offline"
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][UserHandler] JSON unmarshal error (setStatus)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.Status == "" {
		return Response{OK: false, Error: "status is required"}
	}
	updated, appErr := h.plugin.API.UpdateUserStatus(userID, p.Status)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][UserHandler] UpdateUserStatus API error",
			"user_id", userID, "status", p.Status, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][UserHandler] Status updated",
		"user_id", userID, "status", updated.Status)
	return Response{OK: true, Data: updated}
}

// uploadAvatar — Action 209.
func (h *UserHandler) uploadAvatar(userID string, raw json.RawMessage) Response {
	var p struct {
		ImageBytes []byte `json:"image_bytes"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][UserHandler] JSON unmarshal error (uploadAvatar)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if len(p.ImageBytes) == 0 {
		return Response{OK: false, Error: "image_bytes is required"}
	}
	if appErr := h.plugin.API.SetProfileImage(userID, p.ImageBytes); appErr != nil {
		h.plugin.API.LogError("[Photon][UserHandler] SetProfileImage API error",
			"user_id", userID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][UserHandler] Avatar uploaded", "user_id", userID)
	return Response{OK: true, Data: map[string]bool{"updated": true}}
}

// getUserAvatar — Action 210.
func (h *UserHandler) getUserAvatar(userID string, raw json.RawMessage) Response {
	var p struct {
		UserID string `json:"user_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][UserHandler] JSON unmarshal error (getUserAvatar)",
			"caller_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	targetID := p.UserID
	if targetID == "" {
		targetID = userID
	}
	data, appErr := h.plugin.API.GetProfileImage(targetID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][UserHandler] GetProfileImage API error",
			"caller_id", userID, "target_id", targetID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][UserHandler] Avatar fetched",
		"caller_id", userID, "target_id", targetID)
	return Response{OK: true, Data: map[string]interface{}{
		"user_id":     targetID,
		"image_bytes": data,
	}}
}
