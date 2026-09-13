package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// B2 — engramDelete чужого/несуществующего ключа возвращал
// ok:true + deleted_key:<чужой ключ>. Данные не страдали (операция уходит в
// свой namespace), но ответ утверждал удаление того, чего не удалял.
func TestB2_EngramDeleteDoesNotClaimMissingKey(t *testing.T) {
	api := quietAPI()
	api.On("KVGet", mock.AnythingOfType("string")).Return(nil, nil)
	api.On("KVDelete", mock.Anything).Return(nil).Maybe()
	api.On("KVSet", mock.Anything, mock.Anything).Return(nil).Maybe()

	h := NewEngramHandler(handlersFor(api))
	resp := h.Handle(uB, ActionEngramDelete, json.RawMessage(`{"key":"someone-elses-key"}`))

	require.True(t, resp.OK, "отсутствие ключа не ошибка — иначе это оракул существования")
	data, ok := resp.Data.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, false, data["deleted"], "ответ не должен утверждать удаление")
}
