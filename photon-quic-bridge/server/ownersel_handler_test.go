package main

import (
	"encoding/json"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Тесты уровня handler'а, а не политики.
//
// TestAuthzOwnerSelectorIsSelector проверяет ТОЛЬКО таблицу правил и зелёный
// даже тогда, когда в обработчике осталась своя проверка владельца — что и
// произошло: живой прогон 0.4.0 показал, что администратор с
// delete_others_posts получает «forbidden: not the post owner», хотя политика
// его пропускает. Утверждение «решение принимает ownerSel» проверяемо только
// сквозным путём Dispatch → handler, и проверяется здесь.

func postDispatcher(t *testing.T, apiSetup func(api *mockAPI)) *Dispatcher {
	t.Helper()
	api := quietAPI()
	apiSetup(api)
	p := handlersFor(api)
	d := NewDispatcher(p)
	d.authz = newFixture() // тот же стенд, что у политики: A — админ, B — обычный
	return d
}

type mockAPI = plugintestAPI

// A с delete_others_posts обязан удалить пост B: селектор выбирает
// delete_others_posts, а не отказывает по несовпадению владельца.
func TestOwnerSel_AdminDeletesForeignPost(t *testing.T) {
	d := postDispatcher(t, func(api *mockAPI) {
		api.On("GetPost", pPubB).Return(&model.Post{Id: pPubB, UserId: uB, ChannelId: cPub}, nil)
		api.On("DeletePost", pPubB).Return(nil).Once()
	})
	resp := d.Dispatch(uA, HeaderFrame{Action: ActionDeletePost},
		json.RawMessage(`{"post_id":"`+pPubB+`"}`))
	require.True(t, resp.OK, "администратор с delete_others_posts обязан удалить чужой пост, получено: %s", resp.Error)
}

func TestOwnerSel_OwnerDeletesOwnPost(t *testing.T) {
	d := postDispatcher(t, func(api *mockAPI) {
		api.On("GetPost", pPubB).Return(&model.Post{Id: pPubB, UserId: uB, ChannelId: cPub}, nil)
		api.On("DeletePost", pPubB).Return(nil).Once()
	})
	resp := d.Dispatch(uB, HeaderFrame{Action: ActionDeletePost},
		json.RawMessage(`{"post_id":"`+pPubB+`"}`))
	require.True(t, resp.OK, resp.Error)
}

// B без delete_others_posts не удаляет чужой пост — и отказ приходит от слоя
// авторизации, единым текстом (§7.3), а не отдельной формулировкой обработчика.
func TestOwnerSel_NonOwnerWithoutPermissionDenied(t *testing.T) {
	d := postDispatcher(t, func(api *mockAPI) {
		api.On("GetPost", pPub).Return(&model.Post{Id: pPub, UserId: uA, ChannelId: cPub}, nil).Maybe()
	})
	resp := d.Dispatch(uB, HeaderFrame{Action: ActionDeletePost},
		json.RawMessage(`{"post_id":"`+pPub+`"}`))
	require.False(t, resp.OK)
	require.Equal(t, "forbidden", resp.Error,
		"текст отказа обязан совпадать с любым другим отказом авторизации")
}

func TestOwnerSel_AdminUpdatesForeignPost(t *testing.T) {
	d := postDispatcher(t, func(api *mockAPI) {
		api.On("GetPost", pPubB).Return(&model.Post{Id: pPubB, UserId: uB, ChannelId: cPub}, nil)
		api.On("UpdatePost", mock.AnythingOfType("*model.Post")).
			Return(&model.Post{Id: pPubB, UserId: uB, ChannelId: cPub, Message: "edited"}, nil).Once()
	})
	resp := d.Dispatch(uA, HeaderFrame{Action: ActionUpdatePost},
		json.RawMessage(`{"post_id":"`+pPubB+`","message":"edited"}`))
	require.True(t, resp.OK, "администратор с edit_others_posts обязан править чужой пост, получено: %s", resp.Error)
}

func TestOwnerSel_NonOwnerUpdateDenied(t *testing.T) {
	d := postDispatcher(t, func(api *mockAPI) {
		api.On("GetPost", pPub).Return(&model.Post{Id: pPub, UserId: uA, ChannelId: cPub}, nil).Maybe()
	})
	resp := d.Dispatch(uB, HeaderFrame{Action: ActionUpdatePost},
		json.RawMessage(`{"post_id":"`+pPub+`","message":"x"}`))
	require.False(t, resp.OK)
	require.Equal(t, "forbidden", resp.Error)
}
