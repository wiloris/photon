package main

import (
	"time"

	"github.com/mattermost/mattermost/server/public/model"
)

// F6 — жизненный цикл сессии.
//
// Проблема, ради которой существует этот файл (ADR-039 §11). Плагинный
// `API.GetSession` — это `app.GetSessionById`: выборка строки БЕЗ проверки
// истечения и idle-timeout. REST-путь `app.GetSession(token)` обе проверки
// делает, поэтому Bridge принимал то, что REST отверг бы. Плюс QUIC захватывал
// userID один раз за соединение и дальше работал с локальной переменной: отзыв
// сессии закрывал HTTP немедленно, а открытое QUIC-соединение — никогда.
//
// Решение — ОДНА точка проверки на оба входа, тем же приёмом, каким ADR-039
// сделал единой точкой авторизацию в Dispatch: два входа не могут разойтись,
// если проверка у них физически одна.
//
// Правила воспроизводят app.GetSession (channels/app/session.go) дословно, а
// не «по смыслу»: расхождение с REST здесь и было дефектом.
type sessionAuthorizer interface {
	GetSession(sessionID string) (*model.Session, *model.AppError)
	GetConfig() *model.Config
}

// authorizeSession проверяет сессию и возвращает userID.
//
// Возвращает ошибку — значит субъекта нет: вызывающий обязан отказать, а не
// продолжать со старым userID.
func authorizeSession(api sessionAuthorizer, sessionID string) (string, *model.AppError) {
	if sessionID == "" {
		return "", model.NewAppError("authorizeSession", "photon.session.missing",
			nil, "session id is empty", 401)
	}

	session, appErr := api.GetSession(sessionID)
	if appErr != nil {
		return "", appErr
	}
	// Отозванная сессия: строки нет. GetSession может вернуть пустую структуру
	// без ошибки — пустой Id проверяется отдельно, ровно как в REST.
	if session == nil || session.Id == "" {
		return "", model.NewAppError("authorizeSession", "photon.session.invalid",
			nil, "session is nil or empty", 401)
	}

	// Истечение. Именно эту проверку GetSessionById не делает.
	if session.IsExpired() {
		return "", model.NewAppError("authorizeSession", "photon.session.expired",
			nil, "session is expired", 401)
	}

	if sessionIdleTimedOut(api.GetConfig(), session) {
		return "", model.NewAppError("authorizeSession", "photon.session.idle_timeout",
			nil, "session exceeded idle timeout", 401)
	}

	return session.UserId, nil
}

// sessionIdleTimedOut повторяет условия REST, включая ВСЕ исключения: без них
// проверка отвергала бы сессии, которые Mattermost считает живыми, — то есть
// расхождение осталось бы, только в другую сторону.
func sessionIdleTimedOut(cfg *model.Config, session *model.Session) bool {
	if cfg == nil || cfg.ServiceSettings.SessionIdleTimeoutInMinutes == nil {
		return false
	}
	timeoutMinutes := *cfg.ServiceSettings.SessionIdleTimeoutInMinutes
	if timeoutMinutes <= 0 {
		return false
	}
	if session.IsOAuth || session.IsMobileApp() {
		return false
	}
	if session.Props[model.SessionPropType] == model.SessionTypeUserAccessToken {
		return false
	}
	if cfg.ServiceSettings.ExtendSessionLengthWithActivity != nil &&
		*cfg.ServiceSettings.ExtendSessionLengthWithActivity {
		return false
	}
	timeout := int64(timeoutMinutes) * 1000 * 60
	return (model.GetMillis() - session.LastActivityAt) > timeout
}

// sessionRevalidatePeriod — как часто перепроверяется сессия подписки.
//
// Значение — компромисс, и он назван: слишком часто означает лишний запрос к
// БД на каждого подписчика, слишком редко — окно, в котором отозванная сессия
// продолжает получать события. Тридцать секунд соизмеримы с KeepAlivePeriod
// (15 с) и дают верхнюю границу окна, а не «когда-нибудь».
const sessionRevalidatePeriod = 30 * time.Second
