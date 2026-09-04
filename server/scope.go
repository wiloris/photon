package main

import (
	"github.com/mattermost/mattermost/server/public/model"
)

// Scope layer — ADR-039 §12.
//
// Это НЕ авторизация и не замена ей. Решение «можно ли субъекту выполнить этот
// action» принято в authz.go до входа в handler. Здесь решается другое: какие
// строки вообще попадают в выборку.
//
// Разделение существенно, потому что над невыборочным вызовом Plugin API
// никакое правило доступа утечку не чинит — скоуп находится внутри запроса.
// Измерено baseline'ом: searchPosts через SearchPostsInTeam вернул обычному
// пользователю посты из чужой личной переписки и с чужой стены, хотя правило
// доступа к самому действию у него законное.
//
// Там, где у платформы есть вариант вызова с пользователем, берётся он.
// Там, где его нет (каталог пользователей), выборка сужается командами
// субъекта — см. ADR-039 §12 о том, почему не воспроизводится
// GetViewUsersRestrictions.

const (
	// Предохранитель на обход страниц GetUsersInTeam: инстанс с огромной
	// командой не должен превращать один вызов в бесконечный обход.
	maxScopePages = 20
	scopePageSize = 200
)

// visibleUserIDs — множество пользователей, с которыми субъект делит хотя бы
// одну команду.
//
// Это подмножество того, что считает сам Mattermost (GetViewUsersRestrictions
// добавляет ещё каналы субъекта), то есть строго уже. Осознанно: расширять
// видимость собственной логикой — заводить свою модель прав, чего ADR-039
// запрещает. Следствие названо явно: собеседник по личной переписке, не
// состоящий с субъектом ни в одной общей команде, в каталоге не появится.
func (p *Plugin) visibleUserIDs(userID string) (map[string]struct{}, *model.AppError) {
	teams, appErr := p.API.GetTeamsForUser(userID)
	if appErr != nil {
		return nil, appErr
	}
	visible := make(map[string]struct{}, 64)
	for _, t := range teams {
		for page := 0; page < maxScopePages; page++ {
			users, appErr := p.API.GetUsersInTeam(t.Id, page, scopePageSize)
			if appErr != nil {
				return nil, appErr
			}
			for _, u := range users {
				visible[u.Id] = struct{}{}
			}
			if len(users) < scopePageSize {
				break
			}
		}
	}
	// Сам субъект виден себе всегда, даже если не состоит ни в одной команде.
	visible[userID] = struct{}{}
	return visible, nil
}

// filterVisible оставляет только пользователей внутри скоупа субъекта.
func filterVisible(users []*model.User, visible map[string]struct{}) []*model.User {
	out := make([]*model.User, 0, len(users))
	for _, u := range users {
		if _, ok := visible[u.Id]; ok {
			out = append(out, u)
		}
	}
	return out
}

// paginate — страничная нарезка уже суженной выборки.
func paginate[T any](items []T, page, perPage int) ([]T, bool) {
	if perPage <= 0 {
		perPage = 50
	}
	start := page * perPage
	if start >= len(items) {
		return []T{}, false
	}
	end := start + perPage
	if end > len(items) {
		end = len(items)
	}
	return items[start:end], end < len(items)
}
