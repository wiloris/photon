package main

import (
	"encoding/json"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// B3 — getUserStatus падал у обоих субъектов: GetUserStatus возвращает ошибку
// для пользователя без записи статуса. Сам Mattermost на этом же месте отдаёт
// offline, потому что REST ходит через GetUserStatusesByIds (api4/status.go:35).
func TestB3_GetUserStatusFallsBackLikeMattermost(t *testing.T) {
	api := quietAPI()
	api.On("GetUserStatusesByIds", []string{uA}).
		Return([]*model.Status{{UserId: uA, Status: model.StatusOffline}}, nil)

	h := &UserHandler{plugin: handlersFor(api)}
	resp := h.Handle(uB, ActionGetUserStatus, json.RawMessage(`{"user_id":"`+uA+`"}`))

	require.True(t, resp.OK, resp.Error)
	st, ok := resp.Data.(*model.Status)
	require.True(t, ok)
	require.Equal(t, model.StatusOffline, st.Status)
	api.AssertNotCalled(t, "GetUserStatus", mock.Anything)
}

func TestB3_GetUserStatusEmptyResultIsOffline(t *testing.T) {
	api := quietAPI()
	api.On("GetUserStatusesByIds", []string{uA}).Return([]*model.Status{}, nil)

	h := &UserHandler{plugin: handlersFor(api)}
	resp := h.Handle(uB, ActionGetUserStatus, json.RawMessage(`{"user_id":"`+uA+`"}`))

	require.True(t, resp.OK, resp.Error)
	st, ok := resp.Data.(*model.Status)
	require.True(t, ok)
	require.Equal(t, model.StatusOffline, st.Status)
}
