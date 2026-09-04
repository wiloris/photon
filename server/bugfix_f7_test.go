package main

import (
	"encoding/json"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin"
	"github.com/stretchr/testify/require"
)

// F7 — отказ TLS оставлял quicListener == nil, а OnActivate возвращал nil.
// Плагин выглядел активированным, и первое же сообщение роняло процесс:
// все восемь хуков безусловно звали p.quicListener.Push, recover в них нет.
func TestF7_HooksSurviveNilListener(t *testing.T) {
	api := quietAPI()
	p := handlersFor(api)
	require.Nil(t, p.quicListener)

	require.NotPanics(t, func() {
		p.MessageHasBeenPosted(&plugin.Context{}, &model.Post{Id: "p", ChannelId: cPub})
		p.MessageHasBeenUpdated(&plugin.Context{}, &model.Post{Id: "p", ChannelId: cPub}, nil)
		p.ReactionHasBeenAdded(&plugin.Context{}, &model.Reaction{PostId: "p", ChannelId: cPub})
		p.ReactionHasBeenRemoved(&plugin.Context{}, &model.Reaction{PostId: "p", ChannelId: cPub})
		p.UserHasJoinedChannel(&plugin.Context{}, &model.ChannelMember{ChannelId: cPub, UserId: uA}, nil)
		p.UserHasLeftChannel(&plugin.Context{}, &model.ChannelMember{ChannelId: cPub, UserId: uA}, nil)
		p.UserHasLoggedIn(&plugin.Context{}, &model.User{Id: uA})
		p.ChannelHasBeenCreated(&plugin.Context{}, &model.Channel{Id: cPub})
	}, "хук на неактивированном listener'е не должен ронять процесс плагина")
}

// F7 — второй вход: HandleAction разыменовывал p.quicListener так же.
func TestF7_HTTPEntrySurvivesNilListener(t *testing.T) {
	api := quietAPI()
	api.On("GetSession", "sid").Return(&model.Session{Id: "sid", UserId: uB}, nil)
	p := handlersFor(api)
	p.router = p.initRouter()

	require.NotPanics(t, func() {
		w := newRecorder()
		r := newActionRequest(`{"auth":{"session_token":"sid"},"header":{"action":200},"payload":{}}`)
		p.ServeHTTP(nil, w, r)
		var resp Response
		require.NoError(t, json.NewDecoder(w.Result().Body).Decode(&resp))
		require.False(t, resp.OK)
	})
}
