package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/wiloris/photon-core/adapters/googleoidc"
	"github.com/wiloris/photon-core/ports"
)

// Server-side ENGRAM Google Authentication — реализация ADR-078.
//
//	Google ID token → VerifyIDToken → account registry → пользователь
//	Mattermost → сессия Mattermost → HTTP-ответ
//
// Реестр и его CAS-протокол не дублируются: используется реализация ADR-077
// (account_registry.go, photon-core/docs/account-registry.md §6).

// Коды ошибок — стабильные и машиночитаемые (ADR-078 §2.4).
//
// Собственная форма ответа, а не конверт Response{OK,Data,Error}: тот несёт
// только свободный текст, и клиенту достался бы разбор строки вместо кода.
const (
	errCodeInvalidRequest      = "invalid_request"
	errCodeInvalidCredential   = "invalid_google_credential"
	errCodeEmailRequired       = "google_email_required"
	errCodeNotConfigured       = "auth_not_configured"
	errCodeVerifyUnavailable   = "google_verification_unavailable"
	errCodeRegistryFailure     = "account_registry_error"
	errCodeMattermostUserError = "mattermost_user_error"
	errCodeSessionError        = "session_error"
)

// photonIDProp — маркер принадлежности пользователя Mattermost аккаунту
// ENGRAM (ADR-078 §2.7).
//
// Сверка идёт по нему, а НЕ по email: ADR-023 прямо допускает смену email у
// провайдера, и сверка по нему заперла бы сменившего адрес в состоянии
// registered навсегда. Истина о связи остаётся в Account.MattermostUserID
// (ADR-077 §2.7); здесь Props — подтверждение принадлежности, не источник.
const photonIDProp = "photon_id"

// engramAuthService — способ входа пользователя Mattermost, созданного
// ENGRAM-входом (ADR-080). AuthData = photon_id, пароля нет.
//
// Mattermost проверяет пароль у всякого пользователя с пустым AuthService
// (app/users createUser), поэтому без этого CreateUser отказывает. Непустой
// AuthService закрывает вход паролем, сброс и смену пароля, а ни один
// провайдер SSO Mattermost так не называется — пару (AuthData, service) он
// не найдёт. Google Subject сюда не попадает: он ключ внешней личности в
// реестре (ADR-023), а не атрибут пользователя Mattermost.
const engramAuthService = "engram"

type authRequest struct {
	IDToken string `json:"id_token"`
}

type authResponse struct {
	OK           bool   `json:"ok"`
	SessionToken string `json:"session_token,omitempty"`
	PhotonID     string `json:"photon_id,omitempty"`
	LoginName    string `json:"login_name,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
	Error        string `json:"error,omitempty"`
}

// idTokenVerifier — ровно то, что нужно от провайдера. *googleoidc.Adapter
// ему удовлетворяет; подмена нужна тестам, чтобы не ходить в сеть.
type idTokenVerifier interface {
	VerifyIDToken(ctx context.Context, token string) (ports.ExternalIdentity, error)
}

// userSessionAPI — операции Mattermost, лежащие ВНЕ реестра (ADR-077 §2.2:
// CreateUser и CreateSession не входят и не могут входить в транзакцию).
type userSessionAPI interface {
	CreateUser(user *model.User) (*model.User, *model.AppError)
	GetUserByUsername(name string) (*model.User, *model.AppError)
	CreateSession(session *model.Session) (*model.Session, *model.AppError)
}

// verifierCache держит адаптер, пересобирая его при смене clientID.
//
// Конфигурация может меняться в рантайме (ADR-078 §2.2), поэтому захватывать
// значение при активации нельзя. Пересборка нужна и ради кеша JWKS: строить
// адаптер на каждый запрос значило бы ходить к Google за ключами каждый раз.
type verifierCache struct {
	mu       sync.Mutex
	clientID string
	verifier idTokenVerifier
}

func (c *verifierCache) get(clientID string) idTokenVerifier {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.verifier == nil || c.clientID != clientID {
		c.clientID = clientID
		c.verifier = googleoidc.New(clientID)
	}
	return c.verifier
}

// authDeps — всё, что нужно обработчику. Собирается в HandleGoogleAuth из
// Plugin API; в тестах подменяется целиком.
type authDeps struct {
	clientID string
	verifier idTokenVerifier
	registry *AccountRegistry
	users    userSessionAPI
	logger   func(msg string, keyValuePairs ...any)
}

// HandleGoogleAuth — POST /api/v1/auth/google.
//
// Живёт в НЕаутентифицированной ветви роутера: до входа у клиента нет ни
// пользователя, ни сессии, и заголовку Mattermost-User-ID взяться неоткуда.
func (p *Plugin) HandleGoogleAuth(w http.ResponseWriter, r *http.Request) {
	clientID := p.getConfiguration().GoogleClientID
	deps := authDeps{
		clientID: clientID,
		verifier: p.googleVerifier.get(clientID),
		registry: NewAccountRegistry(p.API),
		users:    p.API,
		logger:   p.API.LogWarn,
	}
	status, body := handleGoogleAuth(r.Context(), r, deps)
	writeAuthResponse(w, status, body)
}

func writeAuthResponse(w http.ResponseWriter, status int, body authResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func fail(code, message string) (int, authResponse) {
	return statusFor(code), authResponse{OK: false, ErrorCode: code, Error: message}
}

func statusFor(code string) int {
	switch code {
	case errCodeInvalidRequest:
		return http.StatusBadRequest
	case errCodeInvalidCredential, errCodeEmailRequired:
		return http.StatusUnauthorized
	case errCodeNotConfigured, errCodeVerifyUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// handleGoogleAuth — вся логика, отделённая от http.ResponseWriter, чтобы
// тесты проверяли контракт, а не сериализацию.
func handleGoogleAuth(ctx context.Context, r *http.Request, d authDeps) (int, authResponse) {
	// clientID не настроен — отказ ДО обращения к провайдеру. Пустое значение
	// заставило бы адаптер пропустить сверку audience (googleoidc.go:136), то
	// есть принять токен, выпущенный для чужого приложения.
	if d.clientID == "" {
		d.logger("[Photon][Auth] Google clientID is not configured")
		return fail(errCodeNotConfigured, "google authentication is not configured")
	}

	var req authRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.IDToken == "" {
		return fail(errCodeInvalidRequest, "id_token is required")
	}

	ext, err := d.verifier.VerifyIDToken(ctx, req.IDToken)
	if err != nil {
		// Негодный токен и невозможность принять решение — РАЗНЫЕ ответы
		// (ADR-078 §2.4). Свернуть их значило бы сообщать «твой токен плох»,
		// когда недоступен Google.
		if errors.Is(err, ports.ErrTokenInvalid) {
			return fail(errCodeInvalidCredential, "google credential rejected")
		}
		d.logger("[Photon][Auth] Google verification unavailable", "error", err.Error())
		return fail(errCodeVerifyUnavailable, "cannot verify google credential right now")
	}

	// Email обязателен, и отказ наступает ДО первого обращения к реестру:
	// иначе R1a оставил бы запись claimed для личности, которая завершить
	// регистрацию не может (ADR-078 §2.3). Ключом email при этом не
	// становится — им остаётся (Provider, Subject).
	if !model.IsValidEmail(ext.Email) {
		return fail(errCodeEmailRequired, "google account has no usable email")
	}

	account, err := d.registry.Register(ext.Provider, ext.Subject, ext.Email)
	if err != nil {
		d.logger("[Photon][Auth] account registry failure", "error", err.Error())
		return fail(errCodeRegistryFailure, "account registry failure")
	}

	if account.MMUserID == "" {
		userID, code := ensureMattermostUser(d, account, ext.Email)
		if code != "" {
			return fail(code, "cannot resolve mattermost user")
		}
		linked, linkErr := d.registry.Link(ext.Provider, ext.Subject, userID)
		if linkErr != nil {
			d.logger("[Photon][Auth] registry link failure", "error", linkErr.Error())
			return fail(errCodeRegistryFailure, "account registry failure")
		}
		account = linked
	}

	session, appErr := d.users.CreateSession(&model.Session{UserId: account.MMUserID})
	if appErr != nil {
		d.logger("[Photon][Auth] CreateSession failed", "error", appErr.Error())
		return fail(errCodeSessionError, "cannot issue session")
	}

	// Возвращается Session.Id, а не Session.Token: существующая проверка
	// принимает именно идентификатор — authorizeSession зовёт
	// GetSession(sessionID), то есть app.GetSessionById (ADR-078 §2.5).
	// Поле конверта исторически называется session_token.
	return http.StatusOK, authResponse{
		OK:           true,
		SessionToken: session.Id,
		PhotonID:     account.PhotonID,
		LoginName:    account.LoginName,
	}
}

// ensureMattermostUser — идемпотентный шаг U (account-registry.md §6.1).
//
// Ищет по детерминированному login_name, а не по email. Возвращает пустой код
// при успехе.
//
// Пользователь создаётся без пароля, со способом входа ENGRAM (ADR-080):
// AuthData — выделенный реестром photon_id, а не Google Subject.
func ensureMattermostUser(d authDeps, account AccountRecord, email string) (string, string) {
	authData := account.PhotonID
	created, appErr := d.users.CreateUser(&model.User{
		Username:    account.LoginName,
		Email:       email,
		AuthService: engramAuthService,
		AuthData:    &authData,
		Props:       model.StringMap{photonIDProp: account.PhotonID},
	})
	if appErr == nil {
		return created.Id, ""
	}

	// Пользователь уже существует — это ожидаемое состояние C: U выполнился,
	// а Link не успел. Ищем того же по login_name.
	existing, getErr := d.users.GetUserByUsername(account.LoginName)
	if getErr != nil || existing == nil || existing.Id == "" {
		d.logger("[Photon][Auth] CreateUser failed and user not found",
			"login_name", account.LoginName, "error", appErr.Error())
		return "", errCodeMattermostUserError
	}

	// Пространство имён engram-<9 цифр> зарезервировано за реестром, но это
	// соглашение, а не запрет со стороны Mattermost. Найденный тёзка
	// принимается своим ТОЛЬКО по маркеру и способу входа ENGRAM (ADR-080 §4);
	// несовпадение — аномалия, а не повод связать: молчаливая связка отдала
	// бы чужой аккаунт.
	if existing.Props[photonIDProp] != account.PhotonID ||
		existing.AuthService != engramAuthService ||
		existing.AuthData == nil || *existing.AuthData != account.PhotonID {
		d.logger("[Photon][Auth] username collision with foreign user",
			"login_name", account.LoginName)
		return "", errCodeMattermostUserError
	}
	return existing.Id, ""
}
