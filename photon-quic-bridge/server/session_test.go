package main

import (
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin/plugintest"
	"github.com/stretchr/testify/require"
)

// F6 — жизненный цикл сессии (ADR-039 §11).
//
// Дефект был двойной: плагинный GetSession не проверяет истечения (это
// GetSessionById, выборка строки), а QUIC захватывал userID один раз за
// соединение и отзыв сессии не замечал вовсе. Оба входа обязаны иметь ОДНО
// время жизни доверия — иначе через QUIC проходит то, что REST уже отверг.

func configWithIdleTimeout(minutes int) *model.Config {
	cfg := &model.Config{}
	cfg.SetDefaults()
	cfg.ServiceSettings.SessionIdleTimeoutInMinutes = model.NewPointer(minutes)
	cfg.ServiceSettings.ExtendSessionLengthWithActivity = model.NewPointer(false)
	return cfg
}

func sessionAPI(t *testing.T, s *model.Session, cfg *model.Config) *plugintest.API {
	t.Helper()
	api := &plugintest.API{}
	if s == nil {
		api.On("GetSession", "sid").Return(nil,
			model.NewAppError("GetSessionById", "app.session.get.app_error", nil, "not found", 404))
	} else {
		api.On("GetSession", "sid").Return(s, nil)
	}
	api.On("GetConfig").Return(cfg)
	return api
}

// Живая сессия проходит — иначе проверка была бы бесполезной запрещалкой.
func TestF6_ValidSessionPasses(t *testing.T) {
	api := sessionAPI(t, &model.Session{
		Id: "sid", UserId: uA, ExpiresAt: model.GetMillis() + 60_000,
		LastActivityAt: model.GetMillis(),
	}, configWithIdleTimeout(0))

	userID, appErr := authorizeSession(api, "sid")
	require.Nil(t, appErr)
	require.Equal(t, uA, userID, "живая сессия обязана давать своего пользователя")
}

// Сессия без срока (ExpiresAt = 0) действительна: так Mattermost помечает
// бессрочные. Отвергать её значило бы разойтись с REST в другую сторону.
func TestF6_SessionWithoutExpiryPasses(t *testing.T) {
	api := sessionAPI(t, &model.Session{
		Id: "sid", UserId: uA, ExpiresAt: 0, LastActivityAt: model.GetMillis(),
	}, configWithIdleTimeout(0))

	userID, appErr := authorizeSession(api, "sid")
	require.Nil(t, appErr)
	require.Equal(t, uA, userID)
}

// ЯДРО F6: истёкшая сессия отвергается. Именно её GetSessionById возвращает
// без ошибки, и именно она проходила через Bridge, будучи отвергнутой REST.
func TestF6_ExpiredSessionRejected(t *testing.T) {
	api := sessionAPI(t, &model.Session{
		Id: "sid", UserId: uA, ExpiresAt: model.GetMillis() - 1000,
		LastActivityAt: model.GetMillis(),
	}, configWithIdleTimeout(0))

	userID, appErr := authorizeSession(api, "sid")
	require.NotNil(t, appErr, "истёкшая сессия обязана быть отвергнута")
	require.Empty(t, userID, "при отказе субъекта нет — возвращать userID нельзя")
}

// Отозванная сессия: строки нет, GetSession отдаёт ошибку.
func TestF6_RevokedSessionRejected(t *testing.T) {
	api := sessionAPI(t, nil, configWithIdleTimeout(0))

	userID, appErr := authorizeSession(api, "sid")
	require.NotNil(t, appErr)
	require.Empty(t, userID)
}

// Пустой Id при отсутствии ошибки — тот же случай, что проверяет REST
// отдельной строкой (`session.Id == ""`).
func TestF6_EmptySessionIDRejected(t *testing.T) {
	api := sessionAPI(t, &model.Session{Id: "", UserId: uA}, configWithIdleTimeout(0))

	_, appErr := authorizeSession(api, "sid")
	require.NotNil(t, appErr, "сессия с пустым Id действительной не является")
}

// Пустой идентификатор отвергается до похода в API.
func TestF6_MissingSessionIDRejected(t *testing.T) {
	api := &plugintest.API{} // ни одного вызова не ожидается
	_, appErr := authorizeSession(api, "")
	require.NotNil(t, appErr)
	api.AssertNotCalled(t, "GetSession")
}

// Idle-timeout: сессия жива по ExpiresAt, но давно без активности.
func TestF6_IdleTimeoutRejected(t *testing.T) {
	api := sessionAPI(t, &model.Session{
		Id: "sid", UserId: uA, ExpiresAt: model.GetMillis() + 600_000,
		LastActivityAt: model.GetMillis() - 11*60*1000, // 11 минут назад
	}, configWithIdleTimeout(10))

	_, appErr := authorizeSession(api, "sid")
	require.NotNil(t, appErr, "простой дольше таймаута обязан отвергаться")
}

// Тот же простой, но таймаут выключен (0) — сессия действительна.
func TestF6_IdleTimeoutDisabledPasses(t *testing.T) {
	api := sessionAPI(t, &model.Session{
		Id: "sid", UserId: uA, ExpiresAt: model.GetMillis() + 600_000,
		LastActivityAt: model.GetMillis() - 11*60*1000,
	}, configWithIdleTimeout(0))

	userID, appErr := authorizeSession(api, "sid")
	require.Nil(t, appErr)
	require.Equal(t, uA, userID)
}

// Исключения REST обязаны воспроизводиться ВСЕ: иначе проверка отвергала бы
// сессии, которые Mattermost считает живыми, — расхождение осталось бы, только
// в другую сторону.
func TestF6_IdleTimeoutExceptionsMatchREST(t *testing.T) {
	idle := model.GetMillis() - 11*60*1000

	t.Run("OAuth не подпадает", func(t *testing.T) {
		api := sessionAPI(t, &model.Session{
			Id: "sid", UserId: uA, IsOAuth: true, LastActivityAt: idle,
		}, configWithIdleTimeout(10))
		_, appErr := authorizeSession(api, "sid")
		require.Nil(t, appErr)
	})

	t.Run("мобильная сессия не подпадает", func(t *testing.T) {
		api := sessionAPI(t, &model.Session{
			Id: "sid", UserId: uA, DeviceId: "dev-1", LastActivityAt: idle,
		}, configWithIdleTimeout(10))
		_, appErr := authorizeSession(api, "sid")
		require.Nil(t, appErr)
	})

	t.Run("personal access token не подпадает", func(t *testing.T) {
		api := sessionAPI(t, &model.Session{
			Id: "sid", UserId: uA, LastActivityAt: idle,
			Props: model.StringMap{model.SessionPropType: model.SessionTypeUserAccessToken},
		}, configWithIdleTimeout(10))
		_, appErr := authorizeSession(api, "sid")
		require.Nil(t, appErr)
	})

	t.Run("ExtendSessionLengthWithActivity отключает таймаут", func(t *testing.T) {
		cfg := configWithIdleTimeout(10)
		cfg.ServiceSettings.ExtendSessionLengthWithActivity = model.NewPointer(true)
		api := sessionAPI(t, &model.Session{
			Id: "sid", UserId: uA, LastActivityAt: idle,
		}, cfg)
		_, appErr := authorizeSession(api, "sid")
		require.Nil(t, appErr)
	})
}

// Истечение сильнее исключений idle-timeout: у OAuth-сессии срок тоже истекает.
func TestF6_ExpiryBeatsIdleExceptions(t *testing.T) {
	api := sessionAPI(t, &model.Session{
		Id: "sid", UserId: uA, IsOAuth: true,
		ExpiresAt: model.GetMillis() - 1, LastActivityAt: model.GetMillis(),
	}, configWithIdleTimeout(10))

	_, appErr := authorizeSession(api, "sid")
	require.NotNil(t, appErr, "истёкшая сессия отвергается независимо от типа")
}
