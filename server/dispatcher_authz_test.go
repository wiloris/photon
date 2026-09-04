package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin/plugintest"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Оба входа обязаны идти через одну проверку — ADR-039 §6. Проверка не
// декоративная: /api/v1/action зарегистрирован БЕЗ middleware (api.go:15),
// и именно через него снята вся матрица 56×2 измеренного baseline'а.
// Тест на «QUIC тоже закрыт» отдельно не нужен: handleStream зовёт тот же
// Dispatch, и это единственный путь — см. quic_listener.go:439.

func newDeniedDispatcher(t *testing.T) (*Dispatcher, *plugintest.API) {
	t.Helper()
	api := &plugintest.API{}
	api.On("LogDebug", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Maybe()
	api.On("LogWarn", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Maybe()
	p := &Plugin{}
	p.SetAPI(api)
	d := NewDispatcher(p)
	d.authz = newFixture() // стенд baseline'а: B не входит в приватный канал A
	return d, api
}

func TestDispatchDeniesUnauthorized(t *testing.T) {
	d, _ := newDeniedDispatcher(t)
	resp := d.Dispatch(uB, HeaderFrame{Action: ActionGetFeed},
		json.RawMessage(`{"channel_id":"`+cPriv+`"}`))
	require.False(t, resp.OK)
	require.Equal(t, "forbidden", resp.Error)
	require.Nil(t, resp.Data, "тело не должно доходить до отвергнутого вызывающего")
}

func TestDispatchUnknownActionKeepsItsOwnError(t *testing.T) {
	d, _ := newDeniedDispatcher(t)
	resp := d.Dispatch(uB, HeaderFrame{Action: 999}, json.RawMessage(`{}`))
	require.False(t, resp.OK)
	require.Equal(t, "unknown action: 999", resp.Error,
		"§8: неизвестный action остаётся отличим от отказа авторизации")
}

// TestHTTPEntryIsAuthorized — HTTP-вход целиком: конверт → GetSession →
// Dispatch → отказ. Именно этим путём baseline снял 29 обходов.
func TestHTTPEntryIsAuthorized(t *testing.T) {
	d, api := newDeniedDispatcher(t)
	api.On("GetSession", "sid-b").Return(&model.Session{Id: "sid-b", UserId: uB}, nil)

	p := &Plugin{}
	p.SetAPI(api)
	p.router = p.initRouter()
	p.quicListener = &QuicListener{plugin: p, dispatcher: d}

	body, err := json.Marshal(map[string]any{
		"auth":    map[string]string{"session_token": "sid-b"},
		"header":  map[string]any{"action": ActionDownloadFile},
		"payload": map[string]string{"file_id": fPriv},
	})
	require.NoError(t, err)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/action", bytes.NewReader(body))
	p.ServeHTTP(nil, w, r)

	var resp Response
	require.NoError(t, json.NewDecoder(w.Result().Body).Decode(&resp))
	require.False(t, resp.OK)
	require.Equal(t, "forbidden", resp.Error)
}
