package main

import (
	"encoding/json"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin"
)

// pushEvent — единственная точка, через которую хуки отдают события.
// F7: отказ активации теперь возвращает ошибку (plugin.go), но страховка
// остаётся: хук, роняющий процесс плагина на nil, — слишком дорогая цена за
// любую будущую перестановку порядка инициализации. recover в хуках нет.
func (p *Plugin) pushEvent(event PushEvent) {
	if p.quicListener == nil {
		return
	}
	p.quicListener.Push(event)
}

// MessageHasBeenPosted — новое сообщение.
func (p *Plugin) MessageHasBeenPosted(_ *plugin.Context, post *model.Post) {
	if post.Type != "" && post.Type != model.PostTypeDefault {
		return
	}
	data, err := json.Marshal(post)
	if err != nil {
		p.API.LogError("[Photon][Hooks] Marshal error (MessageHasBeenPosted)", "error", err.Error())
		return
	}
	p.pushEvent(PushEvent{Event: PushEventPosted, ChannelID: post.ChannelId, Data: data})
}

// MessageHasBeenUpdated — сообщение отредактировано.
func (p *Plugin) MessageHasBeenUpdated(_ *plugin.Context, newPost *model.Post, _ *model.Post) {
	data, err := json.Marshal(newPost)
	if err != nil {
		p.API.LogError("[Photon][Hooks] Marshal error (MessageHasBeenUpdated)", "error", err.Error())
		return
	}
	p.pushEvent(PushEvent{Event: PushEventPostEdited, ChannelID: newPost.ChannelId, Data: data})
}

// ReactionHasBeenAdded — реакция добавлена.
func (p *Plugin) ReactionHasBeenAdded(_ *plugin.Context, reaction *model.Reaction) {
	data, err := json.Marshal(map[string]interface{}{
		"user_id":    reaction.UserId,
		"post_id":    reaction.PostId,
		"emoji_name": reaction.EmojiName,
	})
	if err != nil {
		p.API.LogError("[Photon][Hooks] Marshal error (ReactionHasBeenAdded)", "error", err.Error())
		return
	}
	p.pushEvent(PushEvent{Event: PushEventReactionAdded, ChannelID: reaction.ChannelId, Data: data})
}

// ReactionHasBeenRemoved — реакция удалена.
func (p *Plugin) ReactionHasBeenRemoved(_ *plugin.Context, reaction *model.Reaction) {
	data, err := json.Marshal(map[string]interface{}{
		"user_id":    reaction.UserId,
		"post_id":    reaction.PostId,
		"emoji_name": reaction.EmojiName,
	})
	if err != nil {
		p.API.LogError("[Photon][Hooks] Marshal error (ReactionHasBeenRemoved)", "error", err.Error())
		return
	}
	p.pushEvent(PushEvent{Event: PushEventReactionRemoved, ChannelID: reaction.ChannelId, Data: data})
}

// UserHasJoinedChannel — пользователь добавлен в канал.
func (p *Plugin) UserHasJoinedChannel(_ *plugin.Context, channelMember *model.ChannelMember, _ *model.User) {
	data, err := json.Marshal(map[string]interface{}{
		"channel_id": channelMember.ChannelId,
		"user_id":    channelMember.UserId,
	})
	if err != nil {
		p.API.LogError("[Photon][Hooks] Marshal error (UserHasJoinedChannel)", "error", err.Error())
		return
	}
	p.pushEvent(PushEvent{Event: PushEventUserAdded, ChannelID: channelMember.ChannelId, Data: data})
}

// UserHasLeftChannel — пользователь покинул канал.
func (p *Plugin) UserHasLeftChannel(_ *plugin.Context, channelMember *model.ChannelMember, _ *model.User) {
	data, err := json.Marshal(map[string]interface{}{
		"channel_id": channelMember.ChannelId,
		"user_id":    channelMember.UserId,
	})
	if err != nil {
		p.API.LogError("[Photon][Hooks] Marshal error (UserHasLeftChannel)", "error", err.Error())
		return
	}
	p.pushEvent(PushEvent{Event: PushEventUserRemoved, ChannelID: channelMember.ChannelId, Data: data})
}

// UserHasLoggedIn — пользователь вошёл.
func (p *Plugin) UserHasLoggedIn(_ *plugin.Context, user *model.User) {
	data, err := json.Marshal(map[string]interface{}{
		"user_id":  user.Id,
		"username": user.Username,
		"status":   "online",
	})
	if err != nil {
		p.API.LogError("[Photon][Hooks] Marshal error (UserHasLoggedIn)", "error", err.Error())
		return
	}
	p.pushEvent(PushEvent{Event: PushEventStatusChange, UserID: user.Id, Data: data})
}

// ChannelHasBeenCreated — новый канал создан.
func (p *Plugin) ChannelHasBeenCreated(_ *plugin.Context, channel *model.Channel) {
	data, err := json.Marshal(channel)
	if err != nil {
		p.API.LogError("[Photon][Hooks] Marshal error (ChannelHasBeenCreated)", "error", err.Error())
		return
	}
	p.pushEvent(PushEvent{Event: PushEventChannelUpdated, ChannelID: channel.Id, Data: data})
}
