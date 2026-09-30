package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/wiloris/photon-core/ports"
)

// Acceptance-условия ADR-078 §4. Сети здесь нет: проверяется контракт
// обработчика, а не Google и не Mattermost.

// ── подменяемые зависимости ─────────────────────────────────────────

type fakeVerifier struct {
	ext   ports.ExternalIdentity
	err   error
	calls int
}

func (f *fakeVerifier) VerifyIDToken(context.Context, string) (ports.ExternalIdentity, error) {
	f.calls++
	if f.err != nil {
		return ports.ExternalIdentity{}, f.err
	}
	return f.ext, nil
}

type fakeUsers struct {
	byName map[string]*model.User
	nextID int

	createErr  *model.AppError
	sessionErr *model.AppError

	created  []*model.User
	sessions []*model.Session
}

func newFakeUsers() *fakeUsers {
	return &fakeUsers{byName: map[string]*model.User{}}
}

// fakeMinPasswordLength — PasswordSettings.MinimumLength стенда (и значение
// Mattermost по умолчанию).
const fakeMinPasswordLength = 8

// CreateUser повторяет проверки настоящего Mattermost 9.11, которые
// касаются способа входа, — иначе заглушка принимает пользователя, которого
// сервер отвергнет (так и случилось на живом прогоне 2026-09-29):
//
//   - users.go createUser: пароль проверяется, ТОЛЬКО если AuthService пуст;
//     «Login failed because of invalid password» — ровно этот отказ;
//   - model.User.IsValid: AuthData без AuthService и пароль вместе с
//     AuthData недопустимы;
//   - уникальный индекс users_authdata_key.
func (f *fakeUsers) CreateUser(u *model.User) (*model.User, *model.AppError) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	hasAuthData := u.AuthData != nil && *u.AuthData != ""
	if u.AuthService == "" &&
		(len(u.Password) < fakeMinPasswordLength || len(u.Password) > model.PasswordMaximumLength) {
		return nil, model.NewAppError("createUserOrGuest", "api.user.check_user_password.invalid.app_error",
			nil, "", 400)
	}
	if hasAuthData && u.AuthService == "" {
		return nil, model.NewAppError("User.IsValid", "model.user.is_valid.auth_data_type.app_error",
			nil, "", 400)
	}
	if hasAuthData && u.Password != "" {
		return nil, model.NewAppError("User.IsValid", "model.user.is_valid.auth_data_pwd.app_error",
			nil, "", 400)
	}
	if hasAuthData {
		for _, other := range f.byName {
			if other.AuthData != nil && *other.AuthData == *u.AuthData {
				return nil, model.NewAppError("CreateUser", "app.user.save.existing.app_error",
					nil, "", 400)
			}
		}
	}
	if _, exists := f.byName[u.Username]; exists {
		return nil, model.NewAppError("CreateUser", "store.sql_user.save.username_exists.app_error",
			nil, "", 400)
	}
	return f.store(u), nil
}

// seed кладёт пользователя, уже существующего в Mattermost, минуя
// проверки CreateUser: это подготовка состояния C (account-registry.md §6),
// а не вызов кода под тестом.
func (f *fakeUsers) seed(u *model.User) *model.User {
	return f.store(u)
}

// engramUser — пользователь Mattermost в том виде, в каком его создаёт
// ENGRAM-вход (ADR-080): для подготовки состояния C.
func engramUser(acc AccountRecord, email string) *model.User {
	authData := acc.PhotonID
	return &model.User{
		Username: acc.LoginName, Email: email,
		AuthService: engramAuthService, AuthData: &authData,
		Props: model.StringMap{photonIDProp: acc.PhotonID},
	}
}

func (f *fakeUsers) store(u *model.User) *model.User {
	f.nextID++
	stored := *u
	stored.Id = fmt.Sprintf("mmuser%d", f.nextID)
	f.byName[u.Username] = &stored
	f.created = append(f.created, &stored)
	return &stored
}

func (f *fakeUsers) GetUserByUsername(name string) (*model.User, *model.AppError) {
	u, ok := f.byName[name]
	if !ok {
		return nil, model.NewAppError("GetUserByUsername", "not found", nil, "", 404)
	}
	return u, nil
}

func (f *fakeUsers) CreateSession(s *model.Session) (*model.Session, *model.AppError) {
	if f.sessionErr != nil {
		return nil, f.sessionErr
	}
	out := *s
	out.Id = fmt.Sprintf("sessid%d", len(f.sessions)+1)
	out.Token = fmt.Sprintf("sesstoken%d", len(f.sessions)+1)
	f.sessions = append(f.sessions, &out)
	return &out, nil
}

type logCapture struct{ lines []string }

func (l *logCapture) log(msg string, kv ...any) {
	l.lines = append(l.lines, fmt.Sprint(append([]any{msg}, kv...)...))
}

type authRig struct {
	kv       *fakeKV
	verifier *fakeVerifier
	users    *fakeUsers
	logs     *logCapture
	deps     authDeps
}

func newAuthRig(t *testing.T) *authRig {
	t.Helper()
	kvs := newFakeKV()
	ver := &fakeVerifier{ext: ports.ExternalIdentity{
		Provider: "google", Subject: "sub-1", Email: "user@example.com",
	}}
	users := newFakeUsers()
	logs := &logCapture{}
	return &authRig{
		kv: kvs, verifier: ver, users: users, logs: logs,
		deps: authDeps{
			clientID: "client-123.apps.googleusercontent.com",
			verifier: ver,
			registry: NewAccountRegistry(kvs),
			users:    users,
			logger:   logs.log,
		},
	}
}

func (r *authRig) post(t *testing.T, body string) (int, authResponse) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "/api/v1/auth/google", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return handleGoogleAuth(context.Background(), req, r.deps)
}

func (r *authRig) login(t *testing.T) (int, authResponse) {
	t.Helper()
	return r.post(t, `{"id_token":"TOKEN"}`)
}

func requireCode(t *testing.T, status int, resp authResponse, wantStatus int, wantCode string) {
	t.Helper()
	if status != wantStatus || resp.ErrorCode != wantCode {
		t.Fatalf("получено %d/%q, ожидалось %d/%q", status, resp.ErrorCode, wantStatus, wantCode)
	}
	if resp.OK {
		t.Fatal("ok=true в отказе")
	}
}

// ── конфигурация ────────────────────────────────────────────────────

// Пустой clientID — отказ, и провайдер НЕ вызывается: иначе адаптер пропустил
// бы сверку audience и принял токен чужого приложения.
func TestAuthWithoutClientIDIsUnavailableAndDoesNotVerify(t *testing.T) {
	r := newAuthRig(t)
	r.deps.clientID = ""

	status, resp := r.login(t)
	requireCode(t, status, resp, http.StatusServiceUnavailable, errCodeNotConfigured)
	if r.verifier.calls != 0 {
		t.Fatalf("VerifyIDToken вызван %d раз при ненастроенном clientID", r.verifier.calls)
	}
	if r.kv.countPrefix("acct:") != 0 {
		t.Fatal("реестр тронут при ненастроенном clientID")
	}
}

// ── разбор запроса ──────────────────────────────────────────────────

func TestAuthMalformedRequest(t *testing.T) {
	for name, body := range map[string]string{
		"не JSON":      `{`,
		"пустой токен": `{"id_token":""}`,
		"нет поля":     `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			r := newAuthRig(t)
			status, resp := r.post(t, body)
			requireCode(t, status, resp, http.StatusBadRequest, errCodeInvalidRequest)
			if r.verifier.calls != 0 {
				t.Fatal("провайдер вызван при негодном запросе")
			}
		})
	}
}

// ── 401 против 503 ──────────────────────────────────────────────────

// Негодный токен и невозможность принять решение — РАЗНЫЕ ответы.
func TestAuthDistinguishesInvalidTokenFromUnavailableVerification(t *testing.T) {
	invalid := newAuthRig(t)
	invalid.verifier.err = ports.ErrTokenInvalid
	st1, r1 := invalid.login(t)
	requireCode(t, st1, r1, http.StatusUnauthorized, errCodeInvalidCredential)

	unavailable := newAuthRig(t)
	unavailable.verifier.err = errors.New("jwks: dial tcp: connection refused")
	st2, r2 := unavailable.login(t)
	requireCode(t, st2, r2, http.StatusServiceUnavailable, errCodeVerifyUnavailable)

	if st1 == st2 || r1.ErrorCode == r2.ErrorCode {
		t.Fatal("два класса отказа свёрнуты в один ответ")
	}
}

// ── email ───────────────────────────────────────────────────────────

// Отсутствие email прекращает flow ДО обращения к реестру: иначе R1a оставил
// бы claimed для личности, которая завершить регистрацию не может.
func TestAuthWithoutEmailStopsBeforeRegistry(t *testing.T) {
	for name, email := range map[string]string{
		"пустой":   "",
		"негодный": "not-an-email",
	} {
		t.Run(name, func(t *testing.T) {
			r := newAuthRig(t)
			r.verifier.ext.Email = email

			status, resp := r.login(t)
			requireCode(t, status, resp, http.StatusUnauthorized, errCodeEmailRequired)
			if r.kv.countPrefix("acct:") != 0 || r.kv.countPrefix("pid:") != 0 {
				t.Fatalf("реестр тронут: acct=%d pid=%d",
					r.kv.countPrefix("acct:"), r.kv.countPrefix("pid:"))
			}
			if len(r.users.created) != 0 {
				t.Fatal("создан пользователь Mattermost")
			}
		})
	}
}

// ── успешный вход ───────────────────────────────────────────────────

func TestAuthNewUserFullFlow(t *testing.T) {
	r := newAuthRig(t)
	status, resp := r.login(t)
	if status != http.StatusOK || !resp.OK {
		t.Fatalf("%d %+v", status, resp)
	}
	if len(resp.PhotonID) != photonIDDigits {
		t.Fatalf("photon_id %q", resp.PhotonID)
	}
	if resp.LoginName != LoginNameFor(resp.PhotonID) {
		t.Fatalf("login_name %q", resp.LoginName)
	}
	// Возвращается Session.Id, а не Session.Token (ADR-078 §2.5): именно его
	// принимает authorizeSession → GetSession(sessionID).
	sess := r.users.sessions[0]
	if resp.SessionToken != sess.Id {
		t.Fatalf("session_token = %q, ожидался Session.Id %q", resp.SessionToken, sess.Id)
	}
	if resp.SessionToken == sess.Token {
		t.Fatal("вернулся Session.Token — существующая проверка его не принимает")
	}
	// Маркер принадлежности проставлен.
	created := r.users.created[0]
	if got := created.Props[photonIDProp]; got != resp.PhotonID {
		t.Fatalf("Props[photon_id] = %q, ожидался %q", got, resp.PhotonID)
	}
	// Способ входа ENGRAM (ADR-080): AuthService, AuthData = photon_id, без
	// пароля. Subject провайдера в AuthData не попадает.
	if created.AuthService != engramAuthService {
		t.Fatalf("AuthService = %q, ожидался %q", created.AuthService, engramAuthService)
	}
	if created.AuthData == nil || *created.AuthData != resp.PhotonID {
		t.Fatalf("AuthData = %v, ожидался photon_id %q", created.AuthData, resp.PhotonID)
	}
	if *created.AuthData == r.verifier.ext.Subject {
		t.Fatal("AuthData = Google Subject — внешний ключ ушёл в Mattermost")
	}
	if created.Password != "" {
		t.Fatal("пользователь создан с паролем — появился вход мимо ENGRAM")
	}
	// Запись реестра связана.
	acc, found, err := r.deps.registry.Lookup("google", "sub-1")
	if err != nil || !found || acc.State != StateLinked || acc.MMUserID != sess.UserId {
		t.Fatalf("запись реестра: %+v (found=%v err=%v)", acc, found, err)
	}
}

// ── идемпотентность ─────────────────────────────────────────────────

func TestAuthRepeatedLoginIsIdempotent(t *testing.T) {
	r := newAuthRig(t)
	_, first := r.login(t)

	for i := 0; i < 5; i++ {
		status, again := r.login(t)
		if status != http.StatusOK {
			t.Fatalf("вход #%d: %d", i, status)
		}
		if again.PhotonID != first.PhotonID {
			t.Fatalf("вход #%d: photon_id %q, первый %q", i, again.PhotonID, first.PhotonID)
		}
		// Сессия КАЖДЫЙ раз новая — расходный ресурс.
		if again.SessionToken == first.SessionToken {
			t.Fatalf("вход #%d вернул ту же сессию", i)
		}
	}
	if len(r.users.created) != 1 {
		t.Fatalf("создано пользователей: %d", len(r.users.created))
	}
	if r.kv.countPrefix("pid:") != 1 {
		t.Fatalf("резервов: %d", r.kv.countPrefix("pid:"))
	}
}

// Смена email у Google не меняет личность и не ломает поиск.
func TestAuthGoogleEmailChangeDoesNotChangeIdentity(t *testing.T) {
	r := newAuthRig(t)
	_, first := r.login(t)

	r.verifier.ext.Email = "renamed@example.com"
	status, again := r.login(t)
	if status != http.StatusOK {
		t.Fatalf("после смены email: %d %+v", status, again)
	}
	if again.PhotonID != first.PhotonID {
		t.Fatalf("photon_id изменился: %q → %q", first.PhotonID, again.PhotonID)
	}
	if len(r.users.created) != 1 {
		t.Fatal("смена email породила второго пользователя")
	}
}

// ── восстановление частичных состояний ──────────────────────────────

// Прерывание после CreateUser до Link: повторный вход находит того же
// пользователя по login_name и связывает его, второго не создаёт.
func TestAuthRecoveryAfterCreateUserBeforeLink(t *testing.T) {
	r := newAuthRig(t)
	// Доводим реестр до registered и создаём пользователя вручную — ровно
	// состояние C спецификации.
	acc, err := r.deps.registry.Register("google", "sub-1", "user@example.com")
	if err != nil {
		t.Fatal(err)
	}
	r.users.seed(engramUser(acc, "user@example.com"))

	status, resp := r.login(t)
	if status != http.StatusOK {
		t.Fatalf("%d %+v", status, resp)
	}
	if resp.PhotonID != acc.PhotonID {
		t.Fatalf("photon_id %q, ожидался %q", resp.PhotonID, acc.PhotonID)
	}
	if len(r.users.created) != 1 {
		t.Fatalf("создано пользователей %d — второй вместо повторного использования", len(r.users.created))
	}
	linked, _, _ := r.deps.registry.Lookup("google", "sub-1")
	if linked.State != StateLinked {
		t.Fatalf("состояние %q", linked.State)
	}
}

// Смена email МЕЖДУ CreateUser и Link восстановлению не мешает: сверка идёт
// по маркеру, а не по адресу.
func TestAuthRecoverySurvivesEmailChangeBetweenCreateUserAndLink(t *testing.T) {
	r := newAuthRig(t)
	acc, err := r.deps.registry.Register("google", "sub-1", "old@example.com")
	if err != nil {
		t.Fatal(err)
	}
	r.users.seed(engramUser(acc, "old@example.com"))

	r.verifier.ext.Email = "new@example.com"
	status, resp := r.login(t)
	if status != http.StatusOK {
		t.Fatalf("смена email заперла восстановление: %d %+v", status, resp)
	}
	if resp.PhotonID != acc.PhotonID {
		t.Fatalf("photon_id %q", resp.PhotonID)
	}
}

// Тёзка без маркера либо с чужим — аномалия: не связываем, запись остаётся
// registered.
func TestAuthForeignNamesakeIsRejected(t *testing.T) {
	str := func(s string) *string { return &s }
	// Каждый случай портит ровно одно из трёх условий принадлежности
	// (ADR-078 §2.7 + ADR-080 §4); photon_id подставляется по месту.
	for name, spoil := range map[string]func(u *model.User, pid string){
		"без маркера":        func(u *model.User, _ string) { u.Props = model.StringMap{} },
		"чужой маркер":       func(u *model.User, _ string) { u.Props[photonIDProp] = "999999999" },
		"парольный тёзка":    func(u *model.User, _ string) { u.AuthService, u.AuthData = "", nil },
		"чужой AuthService":  func(u *model.User, _ string) { u.AuthService = model.ServiceGoogle },
		"чужой AuthData":     func(u *model.User, _ string) { u.AuthData = str("999999999") },
		"AuthData = Subject": func(u *model.User, _ string) { u.AuthData = str("sub-1") },
	} {
		t.Run(name, func(t *testing.T) {
			r := newAuthRig(t)
			acc, err := r.deps.registry.Register("google", "sub-1", "user@example.com")
			if err != nil {
				t.Fatal(err)
			}
			foreign := engramUser(acc, "someone@else.example")
			foreign.Id = "foreign-user"
			spoil(foreign, acc.PhotonID)
			r.users.byName[acc.LoginName] = foreign

			status, resp := r.login(t)
			requireCode(t, status, resp, http.StatusInternalServerError, errCodeMattermostUserError)

			after, _, _ := r.deps.registry.Lookup("google", "sub-1")
			if after.State != StateRegistered {
				t.Fatalf("состояние %q — связывание произошло", after.State)
			}
			if after.MMUserID != "" {
				t.Fatalf("связано с %q", after.MMUserID)
			}
		})
	}
}

// ── отказы нижних слоёв ─────────────────────────────────────────────

func TestAuthRegistryFailure(t *testing.T) {
	r := newAuthRig(t)
	r.kv.failCAS = kvFailure("kv down")
	status, resp := r.login(t)
	requireCode(t, status, resp, http.StatusInternalServerError, errCodeRegistryFailure)
}

func TestAuthCreateUserFailure(t *testing.T) {
	r := newAuthRig(t)
	r.users.createErr = model.NewAppError("CreateUser", "boom", nil, "", 500)
	status, resp := r.login(t)
	requireCode(t, status, resp, http.StatusInternalServerError, errCodeMattermostUserError)

	// Связи registry↔Mattermost не появилось, сессии нет.
	acc, found, err := r.deps.registry.Lookup("google", "sub-1")
	if err != nil || !found {
		t.Fatalf("запись реестра: found=%v err=%v", found, err)
	}
	if acc.State != StateRegistered || acc.MMUserID != "" {
		t.Fatalf("после отказа CreateUser: состояние %q, MMUserID %q", acc.State, acc.MMUserID)
	}
	if len(r.users.sessions) != 0 {
		t.Fatalf("выдано сессий: %d", len(r.users.sessions))
	}
}

// Отказ CreateUser, затем повторный вход, когда Mattermost снова принимает
// пользователя: тот же PhotonID, второго резерва нет, связь установлена.
// Ровно путь живого стенда 2026-09-29 после исправления ADR-080.
func TestAuthRetryAfterCreateUserFailureKeepsPhotonID(t *testing.T) {
	r := newAuthRig(t)
	r.users.createErr = model.NewAppError("CreateUser", "boom", nil, "", 500)
	r.login(t)
	before, _, _ := r.deps.registry.Lookup("google", "sub-1")

	r.users.createErr = nil
	status, resp := r.login(t)
	if status != http.StatusOK {
		t.Fatalf("%d %+v", status, resp)
	}
	if resp.PhotonID != before.PhotonID {
		t.Fatalf("photon_id %q, выделенный при первой попытке %q", resp.PhotonID, before.PhotonID)
	}
	if r.kv.countPrefix("pid:") != 1 {
		t.Fatalf("резервов: %d", r.kv.countPrefix("pid:"))
	}
	after, _, _ := r.deps.registry.Lookup("google", "sub-1")
	if after.State != StateLinked || after.MMUserID != r.users.created[0].Id {
		t.Fatalf("связь: состояние %q, MMUserID %q, пользователь %q",
			after.State, after.MMUserID, r.users.created[0].Id)
	}
	if resp.SessionToken != r.users.sessions[0].Id || r.users.sessions[0].UserId != after.MMUserID {
		t.Fatal("сессия выдана не связанному пользователю")
	}
}

func TestAuthCreateSessionFailure(t *testing.T) {
	r := newAuthRig(t)
	r.users.sessionErr = model.NewAppError("CreateSession", "boom", nil, "", 500)
	status, resp := r.login(t)
	requireCode(t, status, resp, http.StatusInternalServerError, errCodeSessionError)
}

// ── журнал не содержит секретов ─────────────────────────────────────

func TestAuthNeverLogsCredentialOrSession(t *testing.T) {
	// Прогоняем все пути, которые вообще пишут в журнал.
	rigs := []func() *authRig{
		func() *authRig { r := newAuthRig(t); r.deps.clientID = ""; return r },
		func() *authRig { r := newAuthRig(t); r.verifier.err = errors.New("jwks down"); return r },
		func() *authRig { r := newAuthRig(t); r.kv.failCAS = kvFailure("kv down"); return r },
		func() *authRig {
			r := newAuthRig(t)
			r.users.createErr = model.NewAppError("CreateUser", "boom", nil, "", 500)
			return r
		},
		func() *authRig {
			r := newAuthRig(t)
			r.users.sessionErr = model.NewAppError("CreateSession", "boom", nil, "", 500)
			return r
		},
		func() *authRig { return newAuthRig(t) }, // успешный путь
	}
	for i, mk := range rigs {
		r := mk()
		_, resp := r.post(t, `{"id_token":"SUPER-SECRET-ID-TOKEN"}`)
		joined := strings.Join(r.logs.lines, "\n")
		if strings.Contains(joined, "SUPER-SECRET-ID-TOKEN") {
			t.Fatalf("прогон %d: ID token попал в журнал:\n%s", i, joined)
		}
		if resp.SessionToken != "" && strings.Contains(joined, resp.SessionToken) {
			t.Fatalf("прогон %d: идентификатор сессии попал в журнал", i)
		}
	}
}
