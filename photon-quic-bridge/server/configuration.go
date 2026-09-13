package main

import (
	"reflect"

	"github.com/mattermost/mattermost/server/public/plugin"
)

// configuration — настройки плагина из System Console.
type configuration struct {
	// DebugMode включает подробное логирование hooks и SSE событий.
	DebugMode bool `json:"DebugMode"`

	// SSEKeepAliveSec — интервал keepalive ping в секундах.
	SSEKeepAliveSec int `json:"SSEKeepAliveSec"`

	// ClientBufferSize — размер буфера событий на одного SSE-клиента.
	ClientBufferSize int `json:"ClientBufferSize"`
}

func (c *configuration) Clone() *configuration {
	var clone = *c
	return &clone
}

func (p *Plugin) getConfiguration() *configuration {
	p.configurationLock.RLock()
	defer p.configurationLock.RUnlock()

	if p.configuration == nil {
		return &configuration{
			SSEKeepAliveSec:  25,
			ClientBufferSize: 64,
		}
	}
	return p.configuration
}

func (p *Plugin) setConfiguration(configuration *configuration) {
	p.configurationLock.Lock()
	defer p.configurationLock.Unlock()

	if configuration != nil && p.configuration == configuration {
		if reflect.ValueOf(*configuration).NumField() == 0 {
			return
		}
		panic("setConfiguration called with the existing configuration")
	}
	p.configuration = configuration
}

func (p *Plugin) OnConfigurationChange() error {
	var configuration = new(configuration)
	if err := p.API.LoadPluginConfiguration(configuration); err != nil {
		return err
	}
	p.setConfiguration(configuration)
	return nil
}

// IsValid validates the configuration — called before saving.
func (c *configuration) IsValid() error {
	_ = plugin.MattermostPlugin{}
	return nil
}