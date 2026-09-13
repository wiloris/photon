package main

import (
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
)

func userFixture() *model.User {
	return &model.User{
		Id: uA, Username: "wiloris", Email: "wiloris@example.com",
		FirstName: "Wil", LastName: "Oris",
		Password: "hash", MfaSecret: "secret", AuthData: model.NewPointer("authdata"),
		Roles: "system_user system_admin",
		Props: model.StringMap{"k": "v"},
	}
}

// ADR-039 §10: field-level filtering — не авторизация, но и не ручная зачистка
// трёх полей в четырёх местах. Измерено baseline'ом: getUsers отдавал обычному
// пользователю адреса почты всего инстанса.
func TestSanitizeHidesEmailWhenPrivacyForbids(t *testing.T) {
	api := quietAPI()
	api.On("GetConfig").Return(cfgPrivacy(false, false))
	p := handlersFor(api)

	got := p.sanitizeUser(userFixture(), uB)
	require.Empty(t, got.Email, "почта скрыта настройкой инстанса")
	require.Empty(t, got.FirstName)
	require.Empty(t, got.LastName)
	require.Empty(t, got.Password)
	require.Empty(t, got.MfaSecret)
}

func TestSanitizeShowsWhatPrivacyAllows(t *testing.T) {
	api := quietAPI()
	api.On("GetConfig").Return(cfgPrivacy(true, true))
	p := handlersFor(api)

	got := p.sanitizeUser(userFixture(), uB)
	require.Equal(t, "wiloris@example.com", got.Email)
	require.Equal(t, "Wil", got.FirstName)
	require.Empty(t, got.Password, "секреты вычищаются независимо от настроек")
	require.Empty(t, got.MfaSecret)
}

// Себе видно всё, что о себе положено, независимо от PrivacySettings —
// иначе getMe перестал бы показывать собственную почту.
func TestSanitizeKeepsOwnProfile(t *testing.T) {
	api := quietAPI()
	api.On("GetConfig").Return(cfgPrivacy(false, false)).Maybe()
	p := handlersFor(api)

	got := p.sanitizeUser(userFixture(), uA)
	require.Equal(t, "wiloris@example.com", got.Email)
	require.Equal(t, "Wil", got.FirstName)
	require.Empty(t, got.Password)
	require.Empty(t, got.MfaSecret)
	require.Equal(t, "", *got.AuthData)
}

// Оригинал не должен мутировать: Plugin API отдаёт объекты из кэша, и
// зачистка «на месте» испортила бы их для всех остальных.
func TestSanitizeDoesNotMutateSource(t *testing.T) {
	api := quietAPI()
	api.On("GetConfig").Return(cfgPrivacy(false, false))
	p := handlersFor(api)

	src := userFixture()
	_ = p.sanitizeUser(src, uB)
	require.Equal(t, "wiloris@example.com", src.Email, "исходный объект не тронут")
	require.Equal(t, model.StringMap{"k": "v"}, src.Props)
}

func TestSanitizeUsersHandlesNil(t *testing.T) {
	api := quietAPI()
	api.On("GetConfig").Return(cfgPrivacy(true, true)).Maybe()
	p := handlersFor(api)

	require.Nil(t, p.sanitizeUser(nil, uB))
	require.Empty(t, p.sanitizeUsers(nil, uB))
	out := p.sanitizeUsers([]*model.User{userFixture(), nil}, uB)
	require.Len(t, out, 1)
}
