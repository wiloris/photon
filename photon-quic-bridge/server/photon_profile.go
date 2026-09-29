package main

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/mattermost/mattermost/server/public/model"
)

// Профиль и поиск по PhotonID — ADR-081.
//
// Истина — реестр аккаунтов (ADR-077): pid: → acct: → linked → живой
// пользователь Mattermost. Props["photon_id"] и AuthData пользователя —
// зеркала (ADR-077 §2.7, ADR-080 §4); для своего профиля AuthData служит
// только подсказкой и подтверждается реестром.

const (
	errPhotonIDNotFound    = "photon_id_not_found"
	errInvalidPhotonID     = "invalid_photon_id"
	errProfileLookupFailed = "profile_lookup_failed"
)

// PhotonProfile — минимальный публичный профиль (ADR-081 §2.5). Email,
// Subject, mm_user_id и login_name сюда не входят НАМЕРЕННО: одна форма и
// для своего профиля, и для чужого.
type PhotonProfile struct {
	PhotonID    string `json:"photon_id"`
	DisplayName string `json:"display_name"`
}

type findByPhotonIDPayload struct {
	PhotonID string `json:"photon_id"`
}

type photonAccountFinder interface {
	FindByPhotonID(photonID string) (AccountRecord, bool, error)
}

type profileUserAPI interface {
	GetUser(userID string) (*model.User, *model.AppError)
}

type profileDeps struct {
	registry photonAccountFinder
	users    profileUserAPI
	// showFullName — PrivacySettings.ShowFullName: видно ли чужое полное
	// имя. Своё имя себе видно всегда.
	showFullName func() bool
	logger       func(msg string, keyValuePairs ...any)
}

// isCanonicalPhotonID — ровно 9 ASCII-цифр (ADR-018). «000 000 000» —
// представление клиента, сервер его не принимает (ADR-081 §2.4).
func isCanonicalPhotonID(s string) bool {
	if len(s) != photonIDDigits {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func profileFail(code string) Response { return Response{OK: false, Error: code} }

// getMyPhotonProfile — Action 211.
func getMyPhotonProfile(d profileDeps, callerID string) Response {
	user, appErr := d.users.GetUser(callerID)
	if appErr != nil {
		d.logger("[Photon][Profile] GetUser failed (own profile)", "error", appErr.Error())
		return profileFail(errProfileLookupFailed)
	}
	// Подсказка из зеркала; пользователь, созданный не ENGRAM-входом,
	// PhotonID не имеет.
	if user.AuthService != engramAuthService || user.AuthData == nil || !isCanonicalPhotonID(*user.AuthData) {
		return profileFail(errPhotonIDNotFound)
	}
	photonID := *user.AuthData

	rec, found, err := d.registry.FindByPhotonID(photonID)
	if err != nil {
		d.logger("[Photon][Profile] registry failure (own profile)", "error", err.Error())
		return profileFail(errProfileLookupFailed)
	}
	// Зеркало указывает на запись, связанную не с вызывающим, — зеркалу не
	// верим (ADR-081 §2.2).
	if !found || rec.MMUserID != callerID {
		return profileFail(errPhotonIDNotFound)
	}
	return Response{OK: true, Data: profileOf(photonID, user, true)}
}

// findByPhotonID — Action 212.
func findByPhotonID(d profileDeps, callerID string, raw json.RawMessage) Response {
	var p findByPhotonIDPayload
	if err := json.Unmarshal(raw, &p); err != nil || !isCanonicalPhotonID(p.PhotonID) {
		return profileFail(errInvalidPhotonID)
	}

	rec, found, err := d.registry.FindByPhotonID(p.PhotonID)
	if err != nil {
		d.logger("[Photon][Profile] registry failure (find)", "error", err.Error())
		return profileFail(errProfileLookupFailed)
	}
	if !found {
		return profileFail(errPhotonIDNotFound)
	}

	user, appErr := d.users.GetUser(rec.MMUserID)
	if appErr != nil {
		if appErr.StatusCode == http.StatusNotFound {
			return profileFail(errPhotonIDNotFound)
		}
		d.logger("[Photon][Profile] GetUser failed (find)", "error", appErr.Error())
		return profileFail(errProfileLookupFailed)
	}
	// Деактивированный — тот же «не найдено» (ADR-081 §2.3).
	if user.DeleteAt != 0 {
		return profileFail(errPhotonIDNotFound)
	}
	self := user.Id == callerID
	return Response{OK: true, Data: profileOf(p.PhotonID, user, self || d.showFullName())}
}

// profileOf — Nickname; если пуст — полное имя, когда его разрешено
// показывать; иначе пусто. Username (engram-<photon_id>) не подставляется:
// он ничего не добавляет к photon_id.
func profileOf(photonID string, u *model.User, fullNameVisible bool) PhotonProfile {
	name := strings.TrimSpace(u.Nickname)
	if name == "" && fullNameVisible {
		name = strings.TrimSpace(u.FirstName + " " + u.LastName)
	}
	return PhotonProfile{PhotonID: photonID, DisplayName: name}
}

func (h *UserHandler) profileDeps() profileDeps {
	api := h.plugin.API
	return profileDeps{
		registry: NewAccountRegistry(api),
		users:    api,
		showFullName: func() bool {
			// Та же политика, что sanitize.go: без конфигурации — минимум.
			cfg := api.GetConfig()
			return cfg != nil && cfg.GetSanitizeOptions()["fullname"]
		},
		logger: api.LogWarn,
	}
}
