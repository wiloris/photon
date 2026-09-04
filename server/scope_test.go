package main

import (
	"encoding/json"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin/plugintest"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Scope layer — ADR-039 §12. Это НЕ авторизация: правило доступа к action'у
// уже проверено в Dispatcher. Здесь проверяется другое — что выборка сужена
// самой платформой, потому что над невыборочным вызовом никакое правило
// доступа утечку не чинит: скоуп находится внутри запроса.

// scopeAPI — quietAPI плюс конфигурация, нужная sanitize (§10). Отдельно от
// quietAPI: тесты sanitize задают свою политику приватности, а повторная
// регистрация GetConfig в testify не переопределяет первую.
func scopeAPI() *plugintest.API {
	api := quietAPI()
	api.On("GetConfig").Return(cfgPrivacy(true, true)).Maybe()
	return api
}

// Измерено baseline'ом: searchPosts вернул B пять постов A, включая личную
// переписку и стену — каналы, на которые Mattermost отвечает B 403.
// Причина не в отсутствии проверки, а в вызове без пользователя.
func TestSearchPostsUsesUserScopedAPI(t *testing.T) {
	api := scopeAPI()
	api.On("SearchPostsInTeamForUser", team, uB, mock.AnythingOfType("model.SearchParameter")).
		Return(&model.PostSearchResults{PostList: model.NewPostList()}, nil).Once()

	h := &PostHandler{plugin: handlersFor(api)}
	resp := h.Handle(uB, ActionSearchPosts, json.RawMessage(`{"team_id":"`+team+`","query":"marker"}`))

	require.True(t, resp.OK, resp.Error)
	api.AssertExpectations(t)
	api.AssertNotCalled(t, "SearchPostsInTeam", mock.Anything, mock.Anything)
}

// Каталог пользователей: baseline показал, что обычный пользователь получает
// весь инстанс. Сужение — команды субъекта (§12), решение продуктовое и
// принято в ADR-039.
func TestGetUsersScopedToCallerTeams(t *testing.T) {
	api := scopeAPI()
	api.On("GetTeamsForUser", uB).Return([]*model.Team{{Id: team}}, nil)
	api.On("GetUsersInTeam", team, 0, mock.AnythingOfType("int")).
		Return([]*model.User{{Id: uB, Username: "b"}, {Id: uA, Username: "a"}}, nil)
	api.On("GetUsersInTeam", team, mock.MatchedBy(func(p int) bool { return p > 0 }),
		mock.AnythingOfType("int")).Return([]*model.User{}, nil).Maybe()

	h := &UserHandler{plugin: handlersFor(api)}
	resp := h.Handle(uB, ActionGetUsers, json.RawMessage(`{"page":0,"per_page":50}`))

	require.True(t, resp.OK, resp.Error)
	users, ok := resp.Data.([]*model.User)
	require.True(t, ok)
	require.Len(t, users, 2)
	api.AssertNotCalled(t, "GetUsers", mock.Anything)
}

func TestGetUsersByIdsFiltersOutsideScope(t *testing.T) {
	api := scopeAPI()
	api.On("GetTeamsForUser", uB).Return([]*model.Team{{Id: team}}, nil)
	api.On("GetUsersInTeam", team, 0, mock.AnythingOfType("int")).
		Return([]*model.User{{Id: uB, Username: "b"}}, nil)
	api.On("GetUsersInTeam", team, mock.MatchedBy(func(p int) bool { return p > 0 }),
		mock.AnythingOfType("int")).Return([]*model.User{}, nil).Maybe()
	api.On("GetUser", uB).Return(&model.User{Id: uB, Username: "b"}, nil).Maybe()

	h := &UserHandler{plugin: handlersFor(api)}
	resp := h.Handle(uB, ActionGetUsersByIds, json.RawMessage(`{"user_ids":["`+uB+`","`+uA+`"]}`))

	require.True(t, resp.OK, resp.Error)
	users, ok := resp.Data.([]*model.User)
	require.True(t, ok)
	require.Len(t, users, 1, "пользователь вне общих команд не возвращается")
	require.Equal(t, uB, users[0].Id)
	api.AssertNotCalled(t, "GetUser", uA)
}

func TestSearchUsersFiltersOutsideScope(t *testing.T) {
	api := scopeAPI()
	api.On("GetTeamsForUser", uB).Return([]*model.Team{{Id: team}}, nil)
	api.On("GetUsersInTeam", team, 0, mock.AnythingOfType("int")).
		Return([]*model.User{{Id: uB, Username: "b"}}, nil)
	api.On("GetUsersInTeam", team, mock.MatchedBy(func(p int) bool { return p > 0 }),
		mock.AnythingOfType("int")).Return([]*model.User{}, nil).Maybe()
	api.On("SearchUsers", mock.AnythingOfType("*model.UserSearch")).
		Return([]*model.User{{Id: uB, Username: "b"}, {Id: uA, Username: "a"}}, nil)

	h := &UserHandler{plugin: handlersFor(api)}
	resp := h.Handle(uB, ActionSearchUsers, json.RawMessage(`{"term":"a","limit":10}`))

	require.True(t, resp.OK, resp.Error)
	users, ok := resp.Data.([]*model.User)
	require.True(t, ok)
	require.Len(t, users, 1)
	require.Equal(t, uB, users[0].Id)
}
