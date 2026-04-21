package main

import (
	"encoding/json"
	"fmt"
)

// Dispatcher маршрутизирует входящие экшны к соответствующим хендлерам.
// Структура повторяет иерархию Resource-классов Kotlin SDK.
type Dispatcher struct {
	plugin  *Plugin
	post    *PostHandler
	user    *UserHandler
	channel *ChannelHandler
	engram  *EngramHandler
	file    *FileHandler
	wall    *WallHandler
}

func NewDispatcher(p *Plugin) *Dispatcher {
	return &Dispatcher{
		plugin:  p,
		post:    &PostHandler{plugin: p},
		user:    &UserHandler{plugin: p},
		channel: &ChannelHandler{plugin: p},
		engram:  NewEngramHandler(p), // инициализируем через конструктор (rate limiter)
		file:    &FileHandler{plugin: p},
		wall:    &WallHandler{plugin: p},
	}
}

// Dispatch — центральный роутер.
func (d *Dispatcher) Dispatch(userID string, header HeaderFrame, rawPayload json.RawMessage) Response {
	d.plugin.API.LogDebug("[Photon] Dispatcher routing",
		"user_id", userID,
		"resource", header.Resource,
		"action", header.Action,
	)

	switch {
	case header.Action >= 100 && header.Action <= 199:
		return d.post.Handle(userID, header.Action, rawPayload)
	case header.Action >= 200 && header.Action <= 299:
		return d.user.Handle(userID, header.Action, rawPayload)
	case header.Action >= 300 && header.Action <= 399:
		return d.channel.Handle(userID, header.Action, rawPayload)
	case header.Action >= 400 && header.Action <= 499:
		return d.engram.Handle(userID, header.Action, rawPayload)
	case header.Action >= 500 && header.Action <= 599:
		return d.file.Handle(userID, header.Action, rawPayload)
	case header.Action >= 600 && header.Action <= 699:
		return d.wall.Handle(userID, header.Action, rawPayload)
	default:
		d.plugin.API.LogWarn("[Photon] Unknown action code",
			"user_id", userID,
			"action", header.Action,
			"resource", header.Resource,
		)
		return Response{OK: false, Error: fmt.Sprintf("unknown action: %d", header.Action)}
	}
}