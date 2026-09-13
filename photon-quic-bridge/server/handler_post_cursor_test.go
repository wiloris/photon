package main

import (
	"encoding/json"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin/plugintest"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Phase 12 — курсорная пагинация getFeed.
//
// Проверяется НЕ «вызов прошёл», а какой именно метод Plugin API выбран и с
// какими аргументами: подмена GetPostsBefore на GetPostsForChannel не меняет
// ok:true и осталась бы незамеченной при проверке по ответу.

func feedAPI(t *testing.T) (*plugintest.API, *PostHandler) {
	t.Helper()
	api := quietAPI()
	p := &Plugin{}
	p.SetAPI(api)
	return api, &PostHandler{plugin: p}
}

func emptyList() *model.PostList {
	return &model.PostList{Order: []string{}, Posts: map[string]*model.Post{}}
}

// before → GetPostsBefore с page=0: смещение внутри курсорного окна не
// используется намеренно (две системы координат сразу дали бы несогласованность).
func TestP12_FeedBeforeUsesCursorAPI(t *testing.T) {
	api, h := feedAPI(t)
	api.On("GetPostsBefore", "ch1", "cursor-post", 0, 25).Return(emptyList(), nil).Once()

	raw, _ := json.Marshal(GetFeedPayload{ChannelID: "ch1", Before: "cursor-post", Limit: 25})
	resp := h.getFeed("u1", raw)

	require.True(t, resp.OK)
	api.AssertExpectations(t) // падает, если позвали не тот метод или не с теми аргументами
	api.AssertNotCalled(t, "GetPostsForChannel", mock.Anything, mock.Anything, mock.Anything)
}

func TestP12_FeedAfterUsesCursorAPI(t *testing.T) {
	api, h := feedAPI(t)
	api.On("GetPostsAfter", "ch1", "cursor-post", 0, 10).Return(emptyList(), nil).Once()

	raw, _ := json.Marshal(GetFeedPayload{ChannelID: "ch1", After: "cursor-post", Limit: 10})
	resp := h.getFeed("u1", raw)

	require.True(t, resp.OK)
	api.AssertExpectations(t)
	api.AssertNotCalled(t, "GetPostsBefore", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// Пустой курсор — самые свежие. Обратная совместимость с матрицей Phase 14.
func TestP12_FeedWithoutCursorKeepsOldBehaviour(t *testing.T) {
	api, h := feedAPI(t)
	api.On("GetPostsForChannel", "ch1", 2, 30).Return(emptyList(), nil).Once()

	raw, _ := json.Marshal(GetFeedPayload{ChannelID: "ch1", Page: 2})
	resp := h.getFeed("u1", raw)

	require.True(t, resp.OK)
	api.AssertExpectations(t)
}

// before и after одновременно — отказ, а не молчаливый выбор одного из двух.
func TestP12_FeedRejectsBothCursors(t *testing.T) {
	api, h := feedAPI(t)

	raw, _ := json.Marshal(GetFeedPayload{ChannelID: "ch1", Before: "a", After: "b"})
	resp := h.getFeed("u1", raw)

	require.False(t, resp.OK, "две взаимоисключающие границы обязаны отвергаться")
	api.AssertNotCalled(t, "GetPostsBefore", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	api.AssertNotCalled(t, "GetPostsAfter", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	api.AssertNotCalled(t, "GetPostsForChannel", mock.Anything, mock.Anything, mock.Anything)
}

// limit имеет приоритет над per_page; при обоих нулях — умолчание 30.
func TestP12_FeedLimitPrecedence(t *testing.T) {
	t.Run("limit важнее per_page", func(t *testing.T) {
		api, h := feedAPI(t)
		api.On("GetPostsBefore", "ch1", "c", 0, 7).Return(emptyList(), nil).Once()
		raw, _ := json.Marshal(GetFeedPayload{ChannelID: "ch1", Before: "c", Limit: 7, PerPage: 99})
		require.True(t, h.getFeed("u1", raw).OK)
		api.AssertExpectations(t)
	})
	t.Run("без обоих — умолчание 30", func(t *testing.T) {
		api, h := feedAPI(t)
		api.On("GetPostsBefore", "ch1", "c", 0, 30).Return(emptyList(), nil).Once()
		raw, _ := json.Marshal(GetFeedPayload{ChannelID: "ch1", Before: "c"})
		require.True(t, h.getFeed("u1", raw).OK)
		api.AssertExpectations(t)
	})
}
