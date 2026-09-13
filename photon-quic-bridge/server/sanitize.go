package main

import (
	"github.com/mattermost/mattermost/server/public/model"
)

// Field-level filtering — ADR-039 §10.
//
// Это НЕ авторизация: правило «getUser требует ViewMembers» истинно независимо
// от того, показывается ли почта. Здесь решается, какие ПОЛЯ видны, а какие
// ОБЪЕКТЫ доступны — решено в authz.go.
//
// Своей политики приватности не заводится: набор опций берётся у самого
// Mattermost (`Config.GetSanitizeOptions` над PrivacySettings), а вычищает
// поля метод модели (`User.SanitizeProfile`). До этого в четырёх местах
// вручную затирались Password/AuthData/MfaSecret, а PrivacySettings не
// читались вовсе — измерено: обычный пользователь получал адреса почты всего
// инстанса.

// sanitizeUser возвращает КОПИЮ пользователя, пригодную к отправке viewer'у.
// Копию, а не оригинал: Plugin API отдаёт объекты, которые могут быть общими,
// и зачистка на месте испортила бы их для остальных.
func (p *Plugin) sanitizeUser(u *model.User, viewerID string) *model.User {
	if u == nil {
		return nil
	}
	out := u.DeepCopy()
	if out.Id == viewerID {
		// Себе видно своё целиком; вычищаются только секреты, которые не
		// покидают сервер ни при каких настройках.
		out.Password = ""
		out.MfaSecret = ""
		out.MfaUsedTimestamps = nil
		out.AuthData = model.NewPointer("")
		return out
	}
	var opts map[string]bool
	if cfg := p.API.GetConfig(); cfg != nil {
		opts = cfg.GetSanitizeOptions()
	} else {
		// Конфигурация недоступна — показываем минимум, а не максимум.
		opts = map[string]bool{"email": false, "fullname": false}
	}
	out.SanitizeProfile(opts, false)
	return out
}

func (p *Plugin) sanitizeUsers(users []*model.User, viewerID string) []*model.User {
	out := make([]*model.User, 0, len(users))
	for _, u := range users {
		if s := p.sanitizeUser(u, viewerID); s != nil {
			out = append(out, s)
		}
	}
	return out
}
