package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin/plugintest"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Acceptance-условия ADR-081 §5. Реестр НАСТОЯЩИЙ (поверх fakeKV):
// проверяется путь pid: → acct: → linked, а не заглушка поиска.

type fakeProfileUsers struct {
	byID map[string]*model.User
	err  *model.AppError
}

func (f *fakeProfileUsers) GetUser(id string) (*model.User, *model.AppError) {
	if f.err != nil {
		return nil, f.err
	}
	u, ok := f.byID[id]
	if !ok {
		return nil, model.NewAppError("GetUser", "app.user.missing_account.const", nil, "", http.StatusNotFound)
	}
	return u, nil
}

// countingFinder считает обращения к реестру: негодный PhotonID обязан
// отсекаться ДО него (ADR-081 §2.4).
type countingFinder struct {
	inner photonAccountFinder
	calls int
	err   error
}

func (c *countingFinder) FindByPhotonID(id string) (AccountRecord, bool, error) {
	c.calls++
	if c.err != nil {
		return AccountRecord{}, false, c.err
	}
	return c.inner.FindByPhotonID(id)
}

type profileRig struct {
	kv       *fakeKV
	registry *AccountRegistry
	finder   *countingFinder
	users    *fakeProfileUsers
	fullName bool
}

func newProfileRig(t *testing.T) *profileRig {
	t.Helper()
	kv := newFakeKV()
	reg := NewAccountRegistry(kv)
	return &profileRig{
		kv:       kv,
		registry: reg,
		finder:   &countingFinder{inner: reg},
		users:    &fakeProfileUsers{byID: map[string]*model.User{}},
	}
}

func (r *profileRig) deps() profileDeps {
	return profileDeps{
		registry:     r.finder,
		users:        r.users,
		showFullName: func() bool { return r.fullName },
		logger:       func(string, ...any) {},
	}
}

// linked — аккаунт, прошедший весь вход: запись linked и пользователь
// Mattermost в том виде, в каком его создаёт ENGRAM-вход (ADR-080).
func (r *profileRig) linked(t *testing.T, subject, mmUserID string) (AccountRecord, *model.User) {
	t.Helper()
	acc, err := r.registry.Register("google", subject, subject+"@example.com")
	require.NoError(t, err)
	acc, err = r.registry.Link("google", subject, mmUserID)
	require.NoError(t, err)
	u := engramUser(acc, subject+"@example.com")
	u.Id = mmUserID
	r.users.byID[mmUserID] = u
	return acc, u
}

func find(t *testing.T, r *profileRig, caller, photonID string) Response {
	t.Helper()
	return findByPhotonID(r.deps(), caller, pl(t, map[string]string{"photon_id": photonID}))
}

func requireProfile(t *testing.T, resp Response, photonID, name string) {
	t.Helper()
	require.True(t, resp.OK, "ответ: %+v", resp)
	require.Equal(t, PhotonProfile{PhotonID: photonID, DisplayName: name}, resp.Data)
}

// ── свой профиль (211) ──────────────────────────────────────────────

func TestProfileOwn(t *testing.T) {
	r := newProfileRig(t)
	acc, u := r.linked(t, "sub-me", "mm-me")
	u.FirstName, u.LastName = "Ada", "Lovelace"

	// Своё полное имя видно себе и при ShowFullName = false.
	requireProfile(t, getMyPhotonProfile(r.deps(), "mm-me"), acc.PhotonID, "Ada Lovelace")
}

func TestProfileOwnWithoutPhotonID(t *testing.T) {
	r := newProfileRig(t)
	r.users.byID["mm-plain"] = &model.User{Id: "mm-plain", Username: "plain"}
	resp := getMyPhotonProfile(r.deps(), "mm-plain")
	require.Equal(t, profileFail(errPhotonIDNotFound), resp)
	require.Zero(t, r.finder.calls, "пользователь без способа входа ENGRAM не ведёт в реестр")
}

// Зеркало AuthData указывает на чужую запись — зеркалу не верим.
func TestProfileOwnMirrorPointingElsewhereIsNotTrusted(t *testing.T) {
	r := newProfileRig(t)
	other, _ := r.linked(t, "sub-other", "mm-other")
	forged := other.PhotonID
	r.users.byID["mm-forger"] = &model.User{
		Id: "mm-forger", AuthService: engramAuthService, AuthData: &forged,
	}
	require.Equal(t, profileFail(errPhotonIDNotFound), getMyPhotonProfile(r.deps(), "mm-forger"))
}

// ── поиск (212) ─────────────────────────────────────────────────────

func TestProfileFindExisting(t *testing.T) {
	r := newProfileRig(t)
	acc, u := r.linked(t, "sub-1", "mm-1")
	u.Nickname = "ada"
	r.linked(t, "sub-me", "mm-me")

	requireProfile(t, find(t, r, "mm-me", acc.PhotonID), acc.PhotonID, "ada")
}

// Всё, что не найдено, отвечает ОДИНАКОВО (ADR-081 §2.3).
func TestProfileFindNotFoundIsUniform(t *testing.T) {
	cases := map[string]func(t *testing.T, r *profileRig) string{
		"неизвестный": func(*testing.T, *profileRig) string { return "000000000" },
		"осиротевший резерв": func(t *testing.T, r *profileRig) string {
			acc, _ := r.linked(t, "sub-1", "mm-1")
			// Резерв указывает на ту же личность, но запись несёт другой
			// photon_id — ровно проигравший гонку резерв ADR-077.
			raw, err := json.Marshal(pidOwner{Provider: "google", Subject: "sub-1"})
			require.NoError(t, err)
			orphan := "999999999"
			require.NotEqual(t, acc.PhotonID, orphan)
			r.kv.put(pidKey(orphan), raw)
			return orphan
		},
		"не linked": func(t *testing.T, r *profileRig) string {
			acc, err := r.registry.Register("google", "sub-2", "x@example.com")
			require.NoError(t, err)
			return acc.PhotonID
		},
		"деактивирован": func(t *testing.T, r *profileRig) string {
			acc, u := r.linked(t, "sub-3", "mm-3")
			u.DeleteAt = 1
			return acc.PhotonID
		},
		"пользователь Mattermost исчез": func(t *testing.T, r *profileRig) string {
			acc, _ := r.linked(t, "sub-4", "mm-4")
			delete(r.users.byID, "mm-4")
			return acc.PhotonID
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			r := newProfileRig(t)
			id := setup(t, r)
			require.Equal(t, profileFail(errPhotonIDNotFound), find(t, r, "mm-me", id))
		})
	}
}

func TestProfileFindMalformed(t *testing.T) {
	r := newProfileRig(t)
	for _, bad := range []string{"", "15375918", "1537591830", "153 759 183", "15375918a", "１５３７５９１８３"} {
		require.Equal(t, profileFail(errInvalidPhotonID), find(t, r, "mm-me", bad), "ввод %q", bad)
	}
	require.Equal(t, profileFail(errInvalidPhotonID),
		findByPhotonID(r.deps(), "mm-me", json.RawMessage(`not json`)))
	require.Zero(t, r.finder.calls, "негодный PhotonID дошёл до реестра")
}

// Ведущие нули значимы: 000000000 — валидный PhotonID (ADR-018).
func TestProfileFindLeadingZerosAreCanonical(t *testing.T) {
	r := newProfileRig(t)
	require.Equal(t, profileFail(errPhotonIDNotFound), find(t, r, "mm-me", "000000000"))
	require.Equal(t, 1, r.finder.calls)
}

func TestProfileLookupFailure(t *testing.T) {
	r := newProfileRig(t)
	r.finder.err = errors.New("kv down")
	require.Equal(t, profileFail(errProfileLookupFailed), find(t, r, "mm-me", "123456789"))

	r = newProfileRig(t)
	acc, _ := r.linked(t, "sub-1", "mm-1")
	r.users.err = model.NewAppError("GetUser", "db", nil, "", http.StatusInternalServerError)
	require.Equal(t, profileFail(errProfileLookupFailed), find(t, r, "mm-me", acc.PhotonID))
}

// ── приватность (ADR-081 §2.5) ──────────────────────────────────────

func TestProfileFullNameOfOthersFollowsPrivacy(t *testing.T) {
	r := newProfileRig(t)
	acc, u := r.linked(t, "sub-1", "mm-1")
	u.FirstName, u.LastName = "Ada", "Lovelace"

	r.fullName = false
	requireProfile(t, find(t, r, "mm-me", acc.PhotonID), acc.PhotonID, "")
	r.fullName = true
	requireProfile(t, find(t, r, "mm-me", acc.PhotonID), acc.PhotonID, "Ada Lovelace")
}

func TestProfileExposesOnlyPublicFields(t *testing.T) {
	r := newProfileRig(t)
	acc, u := r.linked(t, "sub-secret", "mm-1")
	u.Email = "private@example.com"
	u.Nickname = "ada"

	for name, resp := range map[string]Response{
		"поиск": find(t, r, "mm-other", acc.PhotonID),
		"свой":  getMyPhotonProfile(r.deps(), "mm-1"),
	} {
		body, err := json.Marshal(resp)
		require.NoError(t, err)
		var envelope struct {
			Data map[string]any `json:"data"`
		}
		require.NoError(t, json.Unmarshal(body, &envelope))
		keys := make([]string, 0, len(envelope.Data))
		for k := range envelope.Data {
			keys = append(keys, k)
		}
		require.ElementsMatch(t, []string{"photon_id", "display_name"}, keys, name)
		for _, secret := range []string{"private@example.com", "sub-secret", "mm-1", acc.LoginName} {
			require.NotContains(t, string(body), secret, "%s: утёк %q", name, secret)
		}
	}
}

// ── авторизация (ADR-081 §2.1, §2.6) ────────────────────────────────

func TestProfileAuthzRules(t *testing.T) {
	z := newFixture()
	const nobody = "user-without-grants"
	payload := pl(t, map[string]string{"photon_id": "123456789"})

	require.NoError(t, z.authorize(nobody, ActionGetMyPhotonProfile, pl(t, map[string]string{})),
		"свой профиль доступен любому вошедшему — как getMe")
	require.ErrorIs(t, z.authorize(nobody, ActionFindByPhotonID, payload), errForbidden,
		"поиск без view_members")
	require.NoError(t, z.authorize(uB, ActionFindByPhotonID, payload),
		"поиск с view_members — как searchUsers")
}

// Без сессии конверт не доходит до обработчика — существующий 401.
func TestProfileHTTPWithoutSessionIsUnauthorized(t *testing.T) {
	api := &plugintest.API{}
	api.On("LogDebug", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Maybe()
	api.On("GetSession", "bogus").Return(nil,
		model.NewAppError("GetSession", "session.not_found", nil, "", http.StatusUnauthorized))

	p := &Plugin{}
	p.SetAPI(api)
	p.router = p.initRouter()
	p.quicListener = &QuicListener{plugin: p, dispatcher: NewDispatcher(p)}

	for _, action := range []int{ActionGetMyPhotonProfile, ActionFindByPhotonID} {
		for name, token := range map[string]string{"без токена": "", "чужой токен": "bogus"} {
			body, err := json.Marshal(map[string]any{
				"auth":    map[string]string{"session_token": token},
				"header":  map[string]any{"action": action, "resource": "UserResource"},
				"payload": map[string]string{"photon_id": "123456789"},
			})
			require.NoError(t, err)
			w := httptest.NewRecorder()
			p.ServeHTTP(nil, w, httptest.NewRequest(http.MethodPost, "/api/v1/action", bytes.NewReader(body)))
			require.Equal(t, http.StatusUnauthorized, w.Code, "action %d, %s", action, name)
		}
	}
}

// Реестр сам отвечает «не найдено» для неполной записи, не полагаясь на то,
// что следующий шаг (GetUser с пустым id) случайно откажет.
func TestFindByPhotonIDRequiresLinkedMatchingRecord(t *testing.T) {
	r := newProfileRig(t)

	registered, err := r.registry.Register("google", "sub-reg", "x@example.com")
	require.NoError(t, err)
	_, found, err := r.registry.FindByPhotonID(registered.PhotonID)
	require.NoError(t, err)
	require.False(t, found, "запись до linked найдена")

	linked, _ := r.linked(t, "sub-ok", "mm-ok")
	raw, err := json.Marshal(pidOwner{Provider: "google", Subject: "sub-ok"})
	require.NoError(t, err)
	r.kv.put(pidKey("999999999"), raw)
	_, found, err = r.registry.FindByPhotonID("999999999")
	require.NoError(t, err)
	require.False(t, found, "осиротевший резерв найден")

	rec, found, err := r.registry.FindByPhotonID(linked.PhotonID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "mm-ok", rec.MMUserID)
}
