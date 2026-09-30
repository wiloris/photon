package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin/plugintest"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Acceptance-условия ADR-091 §5 (часть Bridge). Реестр и чтение профиля —
// те же, что в photon_profile_test.go: имя проверяется обратным чтением
// через 211 и 212, а не только ответом 216.

// recordingUpdater пишет пользователя в тот же fakeProfileUsers, из которого
// читают 211/212, и запоминает каждый вызов.
type recordingUpdater struct {
	users *fakeProfileUsers
	calls []model.User
	err   *model.AppError
}

func (u *recordingUpdater) UpdateUser(user *model.User) (*model.User, *model.AppError) {
	u.calls = append(u.calls, *user)
	if u.err != nil {
		return nil, u.err
	}
	stored := *user
	u.users.byID[user.Id] = &stored
	out := stored
	return &out, nil
}

func (r *profileRig) withUpdater() (profileDeps, *recordingUpdater) {
	up := &recordingUpdater{users: r.users}
	d := r.deps()
	d.updater = up
	return d, up
}

func setName(t *testing.T, d profileDeps, caller string, payload any) Response {
	t.Helper()
	return setMyDisplayName(d, caller, pl(t, payload))
}

func TestDisplayNameSetAndReadBack(t *testing.T) {
	r := newProfileRig(t)
	acc, before := r.linked(t, "sub-a", "mm-a")
	d, up := r.withUpdater()

	requireProfile(t, setName(t, d, "mm-a", map[string]string{"display_name": "Ада"}), acc.PhotonID, "Ада")

	// Записан ровно один пользователь — вызывающий — и в нём изменился
	// только Nickname.
	require.Len(t, up.calls, 1)
	want := *before
	want.Nickname = "Ада"
	require.Equal(t, want, up.calls[0], "216 меняет только Nickname")

	requireProfile(t, getMyPhotonProfile(r.deps(), "mm-a"), acc.PhotonID, "Ада")
	requireProfile(t, find(t, r, "mm-other", acc.PhotonID), acc.PhotonID, "Ада")
}

func TestDisplayNameTrimmed(t *testing.T) {
	r := newProfileRig(t)
	acc, _ := r.linked(t, "sub-a", "mm-a")
	d, _ := r.withUpdater()

	requireProfile(t, setName(t, d, "mm-a", map[string]string{"display_name": " \t Ада Лавлейс \u00a0\n"}),
		acc.PhotonID, "Ада Лавлейс")
	require.Equal(t, "Ада Лавлейс", r.users.byID["mm-a"].Nickname)
}

func TestDisplayNameEmptyClears(t *testing.T) {
	r := newProfileRig(t)
	acc, u := r.linked(t, "sub-a", "mm-a")
	u.Nickname = "Ада"
	d, up := r.withUpdater()

	for _, empty := range []string{"", "   ", "\t\n"} {
		requireProfile(t, setName(t, d, "mm-a", map[string]string{"display_name": empty}), acc.PhotonID, "")
		require.Empty(t, r.users.byID["mm-a"].Nickname, "%q очищает", empty)
		requireProfile(t, getMyPhotonProfile(r.deps(), "mm-a"), acc.PhotonID, "")
		r.users.byID["mm-a"].Nickname = "Ада"
	}
	require.Len(t, up.calls, 3)
}

func TestDisplayNameLength(t *testing.T) {
	r := newProfileRig(t)
	acc, _ := r.linked(t, "sub-a", "mm-a")
	d, up := r.withUpdater()

	// Руны, не байты: 64 кириллических буквы — 128 байт, и это допустимо.
	name64 := strings.Repeat("ж", 64)
	requireProfile(t, setName(t, d, "mm-a", map[string]string{"display_name": name64}), acc.PhotonID, name64)
	// Эмодзи — одна руна; обрезка до проверки длины: пробелы по краям не
	// считаются.
	emoji64 := strings.Repeat("😀", 64)
	requireProfile(t, setName(t, d, "mm-a", map[string]string{"display_name": "  " + emoji64 + "  "}), acc.PhotonID, emoji64)
	require.Len(t, up.calls, 2)

	require.Equal(t, profileFail(errDisplayNameTooLong),
		setName(t, d, "mm-a", map[string]string{"display_name": strings.Repeat("ж", 65)}))
	require.Len(t, up.calls, 2, "отказ не пишет")
	require.Equal(t, emoji64, r.users.byID["mm-a"].Nickname, "прежнее имя на месте")
}

func TestDisplayNameRejectsControlAndInvisible(t *testing.T) {
	r := newProfileRig(t)
	r.linked(t, "sub-a", "mm-a")
	d, up := r.withUpdater()

	for name, v := range map[string]string{
		"NUL (Cc)": "Ада\x00",
		"перевод строки внутри":    "Ада\nЛавлейс",
		"табуляция внутри":         "Ада\tЛавлейс",
		"DEL (Cc)":                 "Ада\x7f",
		"C1 (Cc)":                  "Ада\u0085x",
		"RLO bidi (Cf)":            "Ада\u202eнилп",
		"нулевой ширины (Cf)":      "Ад\u200bа",
		"ZWJ (Cf)":                 "Ада\u200dЛ",
		"BOM (Cf)":                 "\ufeffАда",
		"разделитель строк (Zl)":   "Ада\u2028Л",
		"разделитель абзацев (Zp)": "Ада\u2029Л",
		"мягкий перенос (Cf)":      "Ада\u00adЛ",
		"soft hyphen только им":    "\u00ad",
		"изоляция LRI (Cf)":        "\u2066Ада\u2069",
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, profileFail(errDisplayNameInvalid),
				setName(t, d, "mm-a", map[string]string{"display_name": v}))
		})
	}
	require.Empty(t, up.calls)
}

func TestDisplayNameRejectsInvalidUTF8(t *testing.T) {
	// encoding/json подменяет битые байты на U+FFFD — через 216 невалидный
	// UTF-8 до правил имени не доходит. Проверяется сам нормализатор.
	_, code := normalizeDisplayName("Ада\xff")
	require.Equal(t, errDisplayNameInvalid, code)
}

func TestDisplayNameRejectsOnlyDigitsAndSpaces(t *testing.T) {
	r := newProfileRig(t)
	acc, _ := r.linked(t, "sub-a", "mm-a")
	d, up := r.withUpdater()

	for _, v := range []string{
		"765805407",
		"765 805 407",
		"  765 805 407  ",       // после обрезки — только цифры и пробелы
		"765\u00a0805\u3000407", // неразрывный и идеографический пробелы
		"٧٦٥٨٠٥",                // арабо-индийские цифры — тоже цифры
		"0",
	} {
		require.Equal(t, profileFail(errDisplayNameInvalid),
			setName(t, d, "mm-a", map[string]string{"display_name": v}), "%q", v)
	}
	require.Empty(t, up.calls)

	// Цифры внутри имени, пунктуация, эмодзи и любые алфавиты — законны.
	for _, v := range []string{"Агент 007", "R2-D2", "765-805-407", "#1", "🙂 42", "李小龙", "Ὀδυσσεύς", "Ada Lovelace"} {
		requireProfile(t, setName(t, d, "mm-a", map[string]string{"display_name": v}), acc.PhotonID, v)
	}
}

func TestDisplayNamePayloadIsExact(t *testing.T) {
	r := newProfileRig(t)
	r.linked(t, "sub-a", "mm-a")
	r.linked(t, "sub-b", "mm-b")
	d, up := r.withUpdater()

	for name, raw := range map[string]string{
		"нет поля":        `{}`,
		"null":            `{"display_name":null}`,
		"не строка":       `{"display_name":42}`,
		"username":        `{"display_name":"Ада","username":"root"}`,
		"first_name":      `{"display_name":"Ада","first_name":"x"}`,
		"props":           `{"display_name":"Ада","props":{"photon_id":"1"}}`,
		"email":           `{"display_name":"Ада","email":"x@example.com"}`,
		"чужой user_id":   `{"display_name":"Ада","user_id":"mm-b"}`,
		"чужой target":    `{"display_name":"Ада","target_user_id":"mm-b"}`,
		"чужой photon_id": `{"display_name":"Ада","photon_id":"123456789"}`,
		"не объект":       `"Ада"`,
		"битый JSON":      `{"display_name":`,
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, profileFail(errCodeInvalidRequest),
				setMyDisplayName(d, "mm-a", json.RawMessage(raw)))
		})
	}
	require.Empty(t, up.calls)
	require.Empty(t, r.users.byID["mm-b"].Nickname, "чужой пользователь не тронут")
}

// Субъект — только вызывающий: единственный вход, от которого зависит, чья
// запись меняется, — callerID из сессии.
func TestDisplayNameChangesOnlyCaller(t *testing.T) {
	r := newProfileRig(t)
	accA, _ := r.linked(t, "sub-a", "mm-a")
	accB, _ := r.linked(t, "sub-b", "mm-b")
	d, up := r.withUpdater()

	requireProfile(t, setName(t, d, "mm-a", map[string]string{"display_name": "Ада"}), accA.PhotonID, "Ада")
	require.Len(t, up.calls, 1)
	require.Equal(t, "mm-a", up.calls[0].Id)
	requireProfile(t, find(t, r, "mm-a", accB.PhotonID), accB.PhotonID, "")
}

func TestDisplayNameWithoutPhotonID(t *testing.T) {
	r := newProfileRig(t)
	r.users.byID["mm-plain"] = &model.User{Id: "mm-plain", Username: "plain"}
	d, up := r.withUpdater()

	require.Equal(t, profileFail(errSenderHasNoPhotonID),
		setName(t, d, "mm-plain", map[string]string{"display_name": "Ада"}))
	require.Empty(t, up.calls)
}

func TestDisplayNameValidatesBeforeLookup(t *testing.T) {
	r := newProfileRig(t)
	r.linked(t, "sub-a", "mm-a")
	d, _ := r.withUpdater()
	r.finder.calls = 0
	setName(t, d, "mm-a", map[string]string{"display_name": "123"})
	require.Zero(t, r.finder.calls, "негодное имя отсекается до реестра")
}

func TestDisplayNameStoreFailures(t *testing.T) {
	r := newProfileRig(t)
	r.linked(t, "sub-a", "mm-a")
	d, up := r.withUpdater()

	up.err = model.NewAppError("UpdateUser", "store", nil, "", http.StatusInternalServerError)
	require.Equal(t, profileFail(errProfileUpdateFailed),
		setName(t, d, "mm-a", map[string]string{"display_name": "Ада"}))

	r.finder.err = errors.New("kv down")
	require.Equal(t, profileFail(errProfileLookupFailed),
		setName(t, d, "mm-a", map[string]string{"display_name": "Ада"}))
}

func TestDisplayNameExposesOnlyPublicFields(t *testing.T) {
	r := newProfileRig(t)
	acc, u := r.linked(t, "sub-secret", "mm-1")
	u.Email = "private@example.com"
	d, _ := r.withUpdater()

	body, err := json.Marshal(setName(t, d, "mm-1", map[string]string{"display_name": "Ада"}))
	require.NoError(t, err)
	var envelope struct {
		OK   bool           `json:"ok"`
		Data map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(body, &envelope))
	require.True(t, envelope.OK)
	keys := make([]string, 0, len(envelope.Data))
	for k := range envelope.Data {
		keys = append(keys, k)
	}
	require.ElementsMatch(t, []string{"photon_id", "display_name"}, keys)
	for _, secret := range []string{"private@example.com", "sub-secret", "mm-1", acc.LoginName} {
		require.NotContains(t, string(body), secret, "утёк %q", secret)
	}
}

// ── авторизация (ADR-091 §2.1, §2.4) ────────────────────────────────

func TestDisplayNameAuthzRules(t *testing.T) {
	z := newFixture()
	const nobody = "user-without-grants"
	require.NoError(t, z.authorize(nobody, ActionSetMyDisplayName, pl(t, map[string]string{"display_name": "Ада"})),
		"своё имя доступно любому вошедшему — субъект только он сам")
}

func TestPatchMeClosedForEngramUsers(t *testing.T) {
	z := newFixture()
	f := z.api.(*fakeAuthzAPI)
	const engram, plain = "mm-engram", "mm-plain"
	f.users = map[string]*model.User{
		engram: {Id: engram, AuthService: engramAuthService},
		plain:  {Id: plain},
	}

	for name, payload := range map[string]any{
		"имя":      map[string]string{"nickname": "Ада"},
		"username": map[string]string{"username": "root"},
		"props":    map[string]any{"props": map[string]string{"photon_id": "1"}},
		"пустой":   map[string]string{},
	} {
		require.ErrorIs(t, z.authorize(engram, ActionPatchMe, pl(t, payload)), errForbidden,
			"207 для ENGRAM закрыт целиком: %s", name)
		require.NoError(t, z.authorize(plain, ActionPatchMe, pl(t, payload)),
			"207 для прочих пользователей Mattermost — как прежде: %s", name)
	}
	// Субъекты матрицы — обычные пользователи.
	require.NoError(t, z.authorize(uB, ActionPatchMe, pl(t, map[string]string{"nickname": "x"})))

	// Способ входа не узнать — отказ, а не «разрешить по умолчанию».
	f.usersFail = true
	require.ErrorIs(t, z.authorize(plain, ActionPatchMe, pl(t, map[string]string{})), errForbidden)
}

// Без сессии конверт не доходит до обработчика — существующий 401.
func TestDisplayNameHTTPWithoutSessionIsUnauthorized(t *testing.T) {
	api := &plugintest.API{}
	api.On("LogDebug", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Maybe()
	api.On("GetSession", "bogus").Return(nil,
		model.NewAppError("GetSession", "session.not_found", nil, "", http.StatusUnauthorized))

	p := &Plugin{}
	p.SetAPI(api)
	p.router = p.initRouter()
	p.quicListener = &QuicListener{plugin: p, dispatcher: NewDispatcher(p)}

	for name, token := range map[string]string{"без токена": "", "чужой токен": "bogus"} {
		body, err := json.Marshal(map[string]any{
			"auth":    map[string]string{"session_token": token},
			"header":  map[string]any{"action": ActionSetMyDisplayName, "resource": "UserResource"},
			"payload": map[string]string{"display_name": "Ада"},
		})
		require.NoError(t, err)
		w := httptest.NewRecorder()
		p.ServeHTTP(nil, w, httptest.NewRequest(http.MethodPost, "/api/v1/action", bytes.NewReader(body)))
		require.Equal(t, http.StatusUnauthorized, w.Code, name)
	}
	api.AssertNotCalled(t, "UpdateUser", mock.Anything)
}

// Сквозь UserHandler с настоящей проводкой profileDeps(): маршрут 216 и
// запись через plugin.API. Реестр — содержимое fakeKV, отданное через KVGet.
func TestDisplayNameThroughUserHandler(t *testing.T) {
	r := newProfileRig(t)
	acc, u := r.linked(t, "sub-a", "mm-a")

	api := quietAPI()
	for k, v := range r.kv.data {
		api.On("KVGet", k).Return(v, nil).Maybe()
	}
	api.On("GetUser", "mm-a").Return(u, nil)
	var written *model.User
	api.On("UpdateUser", mock.Anything).Run(func(args mock.Arguments) {
		c := *args.Get(0).(*model.User)
		written = &c
	}).Return(func(in *model.User) *model.User { c := *in; return &c }, nil)

	h := &UserHandler{plugin: handlersFor(api)}
	resp := h.Handle("mm-a", ActionSetMyDisplayName, json.RawMessage(`{"display_name":"  Ада  "}`))
	requireProfile(t, resp, acc.PhotonID, "Ада")
	require.NotNil(t, written)
	require.Equal(t, "mm-a", written.Id)
	require.Equal(t, "Ада", written.Nickname)
	require.Equal(t, u.Username, written.Username, "username не тронут")

	require.Equal(t, profileFail(errDisplayNameInvalid),
		h.Handle("mm-a", ActionSetMyDisplayName, json.RawMessage(`{"display_name":"123 456"}`)))
	api.AssertNumberOfCalls(t, "UpdateUser", 1)
}
