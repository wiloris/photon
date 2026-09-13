package main

import (
	"encoding/json"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// B1 — createChannel не добавлял создателя в канал. Обнаружилось на откате
// мутации baseline'а: removeMember ответил «вы последний участник», хотя канал
// создавал A. К модели авторизации отношения не имеет.
func TestB1_CreateChannelAddsCreator(t *testing.T) {
	api := quietAPI()
	created := &model.Channel{Id: cPriv, TeamId: team, Type: model.ChannelTypePrivate}
	api.On("CreateChannel", mock.AnythingOfType("*model.Channel")).Return(created, nil)
	api.On("AddChannelMember", cPriv, uA).Return(&model.ChannelMember{ChannelId: cPriv, UserId: uA}, nil).Once()

	h := &ChannelHandler{plugin: handlersFor(api)}
	resp := h.Handle(uA, ActionCreateChannel,
		json.RawMessage(`{"team_id":"`+team+`","name":"n","display_name":"N","is_private":true}`))

	require.True(t, resp.OK, resp.Error)
	api.AssertExpectations(t)
}
