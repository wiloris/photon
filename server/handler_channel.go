package main

import (
	"encoding/json"
	"fmt"

	"github.com/mattermost/mattermost/server/public/model"
)

// ChannelHandler зеркалит ChannelResource из Kotlin SDK.
// Actions: 300=ListChannels, 301=GetChannelMembers, 302=GetMyChannels,
//          303=GetChannel, 304=GetChannelByName, 305=CreateChannel,
//          306=CreateDirectChannel, 307=GetDirectChannels,
//          308=GetOrCreateDirectChannel, 309=AddMember, 310=RemoveMember,
//          311=UpdateChannel, 312=GetUnreadForChannel, 313=MarkAsRead,
//          314=GetChannelMember
type ChannelHandler struct {
	plugin *Plugin
}

func (h *ChannelHandler) Handle(userID string, action int, raw json.RawMessage) Response {
	switch action {
	case ActionListChannels:
		return h.listChannels(userID, raw)
	case ActionGetChannelMembers:
		return h.getChannelMembers(userID, raw)
	case ActionGetMyChannels:
		return h.getMyChannels(userID, raw)
	case ActionGetChannel:
		return h.getChannel(userID, raw)
	case ActionGetChannelByName:
		return h.getChannelByName(userID, raw)
	case ActionCreateChannel:
		return h.createChannel(userID, raw)
	case ActionCreateDirectChannel:
		return h.createDirectChannel(userID, raw)
	case ActionGetDirectChannels:
		return h.getDirectChannels(userID, raw)
	case ActionGetOrCreateDirectChannel:
		return h.createDirectChannel(userID, raw) // идемпотентно
	case ActionAddMember:
		return h.addMember(userID, raw)
	case ActionRemoveMember:
		return h.removeMember(userID, raw)
	case ActionUpdateChannel:
		return h.updateChannel(userID, raw)
	case ActionGetUnreadForChannel:
		return h.getUnreadForChannel(userID, raw)
	case ActionMarkAsRead:
		return h.markAsRead(userID, raw)
	case ActionGetChannelMember:
		return h.getChannelMember(userID, raw)
	default:
		return Response{OK: false, Error: fmt.Sprintf("ChannelResource: unknown action %d", action)}
	}
}

// listChannels — Action 300.
func (h *ChannelHandler) listChannels(userID string, raw json.RawMessage) Response {
	var p ListChannelsPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] JSON unmarshal error (listChannels)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.TeamID == "" {
		return Response{OK: false, Error: "team_id is required"}
	}
	if p.PerPage <= 0 {
		p.PerPage = 50
	}
	channels, appErr := h.plugin.API.GetChannelsForTeamForUser(p.TeamID, userID, false)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] GetChannelsForTeamForUser API error",
			"user_id", userID, "team_id", p.TeamID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	start := p.Page * p.PerPage
	if start >= len(channels) {
		return Response{OK: true, Data: map[string]interface{}{
			"channels": []*model.Channel{},
			"has_more": false,
		}}
	}
	end := start + p.PerPage
	if end > len(channels) {
		end = len(channels)
	}
	h.plugin.API.LogInfo("[Photon][ChannelHandler] Channels listed",
		"user_id", userID, "team_id", p.TeamID, "count", end-start)
	return Response{OK: true, Data: map[string]interface{}{
		"channels": channels[start:end],
		"has_more": end < len(channels),
	}}
}

// getChannelMembers — Action 301.
func (h *ChannelHandler) getChannelMembers(userID string, raw json.RawMessage) Response {
	var p GetChannelMembersPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] JSON unmarshal error (getChannelMembers)",
			"user_id", userID, "error", err.Error())
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" {
		return Response{OK: false, Error: "channel_id is required"}
	}
	if p.PerPage <= 0 {
		p.PerPage = 50
	}
	members, appErr := h.plugin.API.GetChannelMembers(p.ChannelID, p.Page, p.PerPage)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] GetChannelMembers API error",
			"user_id", userID, "channel_id", p.ChannelID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][ChannelHandler] Members fetched",
		"user_id", userID, "channel_id", p.ChannelID, "count", len(members))
	return Response{OK: true, Data: map[string]interface{}{
		"members":  members,
		"has_more": len(members) == p.PerPage,
	}}
}

// getMyChannels — Action 302.
func (h *ChannelHandler) getMyChannels(userID string, raw json.RawMessage) Response {
	var p struct {
		TeamID string `json:"team_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.TeamID == "" {
		return Response{OK: false, Error: "team_id is required"}
	}
	channels, appErr := h.plugin.API.GetChannelsForTeamForUser(p.TeamID, userID, false)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] GetChannelsForTeamForUser API error (getMyChannels)",
			"user_id", userID, "team_id", p.TeamID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][ChannelHandler] My channels fetched",
		"user_id", userID, "count", len(channels))
	return Response{OK: true, Data: channels}
}

// getChannel — Action 303.
func (h *ChannelHandler) getChannel(userID string, raw json.RawMessage) Response {
	var p struct {
		ChannelID string `json:"channel_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" {
		return Response{OK: false, Error: "channel_id is required"}
	}
	ch, appErr := h.plugin.API.GetChannel(p.ChannelID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] GetChannel API error",
			"user_id", userID, "channel_id", p.ChannelID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	return Response{OK: true, Data: ch}
}

// getChannelByName — Action 304.
func (h *ChannelHandler) getChannelByName(userID string, raw json.RawMessage) Response {
	var p struct {
		TeamID string `json:"team_id"`
		Name   string `json:"name"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.TeamID == "" || p.Name == "" {
		return Response{OK: false, Error: "team_id and name are required"}
	}
	ch, appErr := h.plugin.API.GetChannelByName(p.TeamID, p.Name, false)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] GetChannelByName API error",
			"user_id", userID, "name", p.Name, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	return Response{OK: true, Data: ch}
}

// createChannel — Action 305.
func (h *ChannelHandler) createChannel(userID string, raw json.RawMessage) Response {
	var p struct {
		TeamID      string `json:"team_id"`
		Name        string `json:"name"`
		DisplayName string `json:"display_name"`
		IsPrivate   bool   `json:"is_private"`
		Purpose     string `json:"purpose,omitempty"`
		Header      string `json:"header,omitempty"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.TeamID == "" || p.Name == "" || p.DisplayName == "" {
		return Response{OK: false, Error: "team_id, name and display_name are required"}
	}
	chType := model.ChannelTypeOpen
	if p.IsPrivate {
		chType = model.ChannelTypePrivate
	}
	ch, appErr := h.plugin.API.CreateChannel(&model.Channel{
		TeamId:      p.TeamID,
		Name:        p.Name,
		DisplayName: p.DisplayName,
		Type:        chType,
		Purpose:     p.Purpose,
		Header:      p.Header,
		CreatorId:   userID,
	})
	if appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] CreateChannel API error",
			"user_id", userID, "name", p.Name, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][ChannelHandler] Channel created",
		"user_id", userID, "channel_id", ch.Id)
	return Response{OK: true, Data: ch}
}

// createDirectChannel — Action 306 / 308.
// Plugin API v11 не имеет CreateDirectChannel —
// используем GetDirectChannel (создаёт если не существует).
func (h *ChannelHandler) createDirectChannel(userID string, raw json.RawMessage) Response {
	var p struct {
		TargetUserID string `json:"target_user_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.TargetUserID == "" {
		return Response{OK: false, Error: "target_user_id is required"}
	}
	ch, appErr := h.plugin.API.GetDirectChannel(userID, p.TargetUserID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] GetDirectChannel API error",
			"user_id", userID, "target_id", p.TargetUserID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][ChannelHandler] Direct channel ready",
		"user_id", userID, "target_id", p.TargetUserID, "channel_id", ch.Id)
	return Response{OK: true, Data: ch}
}

// getDirectChannels — Action 307.
func (h *ChannelHandler) getDirectChannels(userID string, raw json.RawMessage) Response {
	var p struct {
		TeamID string `json:"team_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.TeamID == "" {
		return Response{OK: false, Error: "team_id is required"}
	}
	all, appErr := h.plugin.API.GetChannelsForTeamForUser(p.TeamID, userID, false)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] GetChannelsForTeamForUser API error (getDirectChannels)",
			"user_id", userID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	dms := make([]*model.Channel, 0)
	for _, ch := range all {
		if ch.Type == model.ChannelTypeDirect || ch.Type == model.ChannelTypeGroup {
			dms = append(dms, ch)
		}
	}
	h.plugin.API.LogInfo("[Photon][ChannelHandler] Direct channels fetched",
		"user_id", userID, "count", len(dms))
	return Response{OK: true, Data: dms}
}

// addMember — Action 309.
func (h *ChannelHandler) addMember(userID string, raw json.RawMessage) Response {
	var p struct {
		ChannelID string `json:"channel_id"`
		UserID    string `json:"user_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" || p.UserID == "" {
		return Response{OK: false, Error: "channel_id and user_id are required"}
	}
	member, appErr := h.plugin.API.AddChannelMember(p.ChannelID, p.UserID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] AddChannelMember API error",
			"caller_id", userID, "channel_id", p.ChannelID, "target_id", p.UserID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][ChannelHandler] Member added",
		"caller_id", userID, "channel_id", p.ChannelID, "member_id", p.UserID)
	return Response{OK: true, Data: member}
}

// removeMember — Action 310.
func (h *ChannelHandler) removeMember(userID string, raw json.RawMessage) Response {
	var p struct {
		ChannelID string `json:"channel_id"`
		UserID    string `json:"user_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" || p.UserID == "" {
		return Response{OK: false, Error: "channel_id and user_id are required"}
	}
	if appErr := h.plugin.API.DeleteChannelMember(p.ChannelID, p.UserID); appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] DeleteChannelMember API error",
			"caller_id", userID, "channel_id", p.ChannelID, "target_id", p.UserID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][ChannelHandler] Member removed",
		"caller_id", userID, "channel_id", p.ChannelID, "member_id", p.UserID)
	return Response{OK: true, Data: map[string]string{"removed_user_id": p.UserID}}
}

// updateChannel — Action 311.
func (h *ChannelHandler) updateChannel(userID string, raw json.RawMessage) Response {
	var p struct {
		ChannelID   string `json:"channel_id"`
		DisplayName string `json:"display_name,omitempty"`
		Purpose     string `json:"purpose,omitempty"`
		Header      string `json:"header,omitempty"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" {
		return Response{OK: false, Error: "channel_id is required"}
	}
	ch, appErr := h.plugin.API.GetChannel(p.ChannelID)
	if appErr != nil {
		return Response{OK: false, Error: appErr.Error()}
	}
	if p.DisplayName != "" {
		ch.DisplayName = p.DisplayName
	}
	if p.Purpose != "" {
		ch.Purpose = p.Purpose
	}
	if p.Header != "" {
		ch.Header = p.Header
	}
	updated, appErr := h.plugin.API.UpdateChannel(ch)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] UpdateChannel API error",
			"user_id", userID, "channel_id", p.ChannelID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][ChannelHandler] Channel updated",
		"user_id", userID, "channel_id", updated.Id)
	return Response{OK: true, Data: updated}
}

// getUnreadForChannel — Action 312.
func (h *ChannelHandler) getUnreadForChannel(userID string, raw json.RawMessage) Response {
	var p struct {
		ChannelID string `json:"channel_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" {
		return Response{OK: false, Error: "channel_id is required"}
	}
	member, appErr := h.plugin.API.GetChannelMember(p.ChannelID, userID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] GetChannelMember API error (getUnreadForChannel)",
			"user_id", userID, "channel_id", p.ChannelID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	ch, appErr := h.plugin.API.GetChannel(p.ChannelID)
	if appErr != nil {
		return Response{OK: false, Error: appErr.Error()}
	}
	unread := ch.TotalMsgCount - member.MsgCount
	if unread < 0 {
		unread = 0
	}
	return Response{OK: true, Data: map[string]interface{}{
		"channel_id":     p.ChannelID,
		"msg_count":      member.MsgCount,
		"mention_count":  member.MentionCount,
		"unread_count":   unread,
		"last_viewed_at": member.LastViewedAt,
	}}
}

// markAsRead — Action 313.
func (h *ChannelHandler) markAsRead(userID string, raw json.RawMessage) Response {
	var p struct {
		ChannelID string `json:"channel_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" {
		return Response{OK: false, Error: "channel_id is required"}
	}
	_, appErr := h.plugin.API.GetChannelMember(p.ChannelID, userID)
	if appErr != nil {
		return Response{OK: false, Error: appErr.Error()}
	}
	h.plugin.API.LogInfo("[Photon][ChannelHandler] Mark as read acknowledged",
		"user_id", userID, "channel_id", p.ChannelID)
	return Response{OK: true, Data: map[string]interface{}{
		"channel_id": p.ChannelID,
		"read":       true,
	}}
}

// getChannelMember — Action 314.
func (h *ChannelHandler) getChannelMember(userID string, raw json.RawMessage) Response {
	var p struct {
		ChannelID string `json:"channel_id"`
		UserID    string `json:"user_id,omitempty"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return Response{OK: false, Error: "invalid payload: " + err.Error()}
	}
	if p.ChannelID == "" {
		return Response{OK: false, Error: "channel_id is required"}
	}
	targetID := p.UserID
	if targetID == "" {
		targetID = userID
	}
	member, appErr := h.plugin.API.GetChannelMember(p.ChannelID, targetID)
	if appErr != nil {
		h.plugin.API.LogError("[Photon][ChannelHandler] GetChannelMember API error",
			"caller_id", userID, "channel_id", p.ChannelID, "target_id", targetID, "error", appErr.Error())
		return Response{OK: false, Error: appErr.Error()}
	}
	return Response{OK: true, Data: member}
}