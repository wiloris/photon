package main

import (
	"encoding/json"
	"fmt"
)

// Dispatcher маршрутизирует входящие экшны к соответствующим хендлерам.
// Структура повторяет иерархию Resource-классов Kotlin SDK.
type Dispatcher struct {
	plugin *Plugin
	// authz — единственная точка принятия решения о доступе (ADR-039 §6).
	// Стоит здесь, а не в handler'ах и не в QUIC-слое: /api/v1/action
	// зарегистрирован без middleware и ведёт сюда же, поэтому проверка
	// внутри handler'ов оставила бы HTTP-вход открытым.
	authz   *authz
	post    *PostHandler
	user    *UserHandler
	channel *ChannelHandler
	engram  *EngramHandler
	file    *FileHandler
	wall    *WallHandler
	media   *MediaHandler
	// mediaService — исполнитель медийных операций. Держится здесь, чтобы у
	// обычного пути и у медийного стрима были ОДИН экземпляр сервиса и одно
	// хранилище: два экземпляра разошлись бы по состоянию.
	mediaService *MediaService
}

// mediaSvc — доступ к сервису для медийной ветки стрима.
func (d *Dispatcher) mediaSvc() *MediaService { return d.mediaService }

func NewDispatcher(p *Plugin) *Dispatcher {
	store := NewFSMediaStore(p.API.GetConfig)
	// Оракул живости поста (ADR-056) подключается ЗДЕСЬ, в единственной точке
	// сборки: и обычный путь, и медийный стрим держат один экземпляр сервиса,
	// поэтому проверка не может оказаться включённой на одном входе и
	// выключенной на другом.
	mediaSvcInstance := NewMediaService(store).
		WithLiveness(postLivenessOracle(p.API, p.API))
	return &Dispatcher{
		plugin:       p,
		authz:        newAuthz(p.API, store),
		post:         &PostHandler{plugin: p, media: mediaSvcInstance},
		user:         &UserHandler{plugin: p},
		channel:      &ChannelHandler{plugin: p},
		engram:       NewEngramHandler(p), // инициализируем через конструктор (rate limiter)
		file:         &FileHandler{plugin: p},
		wall:         &WallHandler{plugin: p},
		media:        &MediaHandler{plugin: p, media: mediaSvcInstance},
		mediaService: mediaSvcInstance,
	}
}

// Authorize — ЕДИНСТВЕННАЯ точка принятия решения о доступе.
//
// Вынесена из Dispatch, чтобы медийный путь, который несёт двоичный хвост и
// потому не проходит через Dispatch, пользовался ТОЙ ЖЕ таблицей правил и тем
// же resolve → policy. Две точки ВЫЗОВА допустимы, две точки РЕШЕНИЯ — нет:
// именно это требование ADR-039 §6 и закрывало HTTP-вход, зарегистрированный
// без middleware.
//
// Возвращает nil, если действие разрешено, иначе готовый отказ — чтобы форма
// ответа и запись в журнал тоже не разошлись между путями.
//
// Три различимых исхода ADR-039 §8 сохранены, и порядок существен: неизвестный
// action обязан остаться отличим от отказа авторизации, а действие без правила
// не должно доходить до handler'а.
func (d *Dispatcher) Authorize(userID string, header HeaderFrame, rawPayload json.RawMessage) *Response {
	if _, known := rules[header.Action]; !known {
		d.plugin.API.LogWarn("[Photon] Unknown action code",
			"user_id", userID,
			"action", header.Action,
			"resource", header.Resource,
		)
		return &Response{OK: false, Error: fmt.Sprintf("unknown action: %d", header.Action)}
	}
	if err := d.authz.authorize(userID, header.Action, rawPayload); err != nil {
		d.plugin.API.LogWarn("[Photon] Authorization denied",
			"user_id", userID,
			"action", header.Action,
			"resource", header.Resource,
		)
		return &Response{OK: false, Error: errForbidden.Error()}
	}
	return nil
}

// Dispatch — центральный роутер.
func (d *Dispatcher) Dispatch(userID string, header HeaderFrame, rawPayload json.RawMessage) Response {
	d.plugin.API.LogDebug("[Photon] Dispatcher routing",
		"user_id", userID,
		"resource", header.Resource,
		"action", header.Action,
	)

	// Решение принимает Authorize — одна точка на все входы (ADR-039 §6).
	// Здесь только применение результата: §7.1, поведение по умолчанию есть
	// отказ.
	if deny := d.Authorize(userID, header, rawPayload); deny != nil {
		return *deny
	}

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
	case header.Action >= 700 && header.Action <= 799:
		return d.media.Handle(userID, header.Action, rawPayload)
	default:
		// Недостижимо: код без правила отсеян выше. Оставлено намеренно —
		// switch по диапазонам и таблица правил могут разойтись только через
		// ошибку, и молчаливый выход из функции был бы хуже.
		return Response{OK: false, Error: fmt.Sprintf("unknown action: %d", header.Action)}
	}
}
