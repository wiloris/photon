package main

import (
	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin/plugintest"
	"github.com/stretchr/testify/mock"
)

// quietAPI — мок Plugin API с заглушенным логированием и без единого
// разрешённого обращения к данным: каждый тест объявляет ровно те вызовы,
// которые ожидает увидеть. Незаявленный вызов роняет тест, а не проходит.
func quietAPI() *plugintest.API {
	api := &plugintest.API{}
	for _, m := range []string{"LogDebug", "LogInfo", "LogWarn", "LogError"} {
		api.On(m, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe()
	}
	return api
}

func handlersFor(api *plugintest.API) *Plugin {
	p := &Plugin{}
	p.SetAPI(api)
	return p
}

func cfgPrivacy(showEmail, showFullName bool) *model.Config {
	c := &model.Config{}
	c.SetDefaults()
	c.PrivacySettings.ShowEmailAddress = model.NewPointer(showEmail)
	c.PrivacySettings.ShowFullName = model.NewPointer(showFullName)
	return c
}

// plugintestAPI — псевдоним, чтобы тесты не тащили импорт plugintest ради
// одного типа в сигнатуре helper'а.
type plugintestAPI = plugintest.API

// quietAPIWithErrorSink — как quietAPI, но LogError дополнительно уходит в
// канал. Порядок регистрации существен: перехват объявляется ПЕРВЫМ, иначе
// сработает общая заглушка, и Run никогда не вызовется.
func quietAPIWithErrorSink(sink chan<- string) *plugintest.API {
	api := &plugintest.API{}
	api.On("LogError", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			if len(args) == 0 {
				return
			}
			msg, ok := args.Get(0).(string)
			if !ok {
				return
			}
			select {
			case sink <- msg:
			default:
			}
		}).Maybe()
	for _, m := range []string{"LogDebug", "LogInfo", "LogWarn"} {
		api.On(m, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe()
	}
	return api
}
