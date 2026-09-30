package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

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

	// Своё отображаемое имя (ADR-091 §2.2).
	errDisplayNameTooLong  = "display_name_too_long"
	errDisplayNameInvalid  = "display_name_invalid"
	errProfileUpdateFailed = "profile_update_failed"

	// maxDisplayNameRunes — предел Nickname в Mattermost
	// (model.UserNicknameMaxRunes): больше сервер не сохранит.
	maxDisplayNameRunes = model.UserNicknameMaxRunes
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

// setMyDisplayNamePayload — ровно одно поле (ADR-091 §2.1). Указатель —
// чтобы отличить отсутствие поля от пустой строки (пусто — очистка).
type setMyDisplayNamePayload struct {
	DisplayName *string `json:"display_name"`
}

type photonAccountFinder interface {
	FindByPhotonID(photonID string) (AccountRecord, bool, error)
}

type profileUserAPI interface {
	GetUser(userID string) (*model.User, *model.AppError)
}

// profileUpdateAPI — запись пользователя Mattermost; нужна только 216.
type profileUpdateAPI interface {
	UpdateUser(user *model.User) (*model.User, *model.AppError)
}

type profileDeps struct {
	registry photonAccountFinder
	users    profileUserAPI
	updater  profileUpdateAPI
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

// ownPhotonID — собственный PhotonID вызывающего, подтверждённый реестром
// (ADR-081 §2.2; им же пользуется ADR-082 §3.3). found=false — у
// вызывающего PhotonID нет либо зеркало указывает не на него; err — сбой.
func ownPhotonID(registry photonAccountFinder, users profileUserAPI, callerID string) (string, *model.User, bool, error) {
	user, appErr := users.GetUser(callerID)
	if appErr != nil {
		return "", nil, false, appErr
	}
	// Подсказка из зеркала; пользователь, созданный не ENGRAM-входом,
	// PhotonID не имеет.
	if user.AuthService != engramAuthService || user.AuthData == nil || !isCanonicalPhotonID(*user.AuthData) {
		return "", user, false, nil
	}
	photonID := *user.AuthData
	rec, found, err := registry.FindByPhotonID(photonID)
	if err != nil {
		return "", user, false, err
	}
	// Зеркало указывает на запись, связанную не с вызывающим, — зеркалу не
	// верим.
	if !found || rec.MMUserID != callerID {
		return "", user, false, nil
	}
	return photonID, user, true, nil
}

// getMyPhotonProfile — Action 211.
func getMyPhotonProfile(d profileDeps, callerID string) Response {
	photonID, user, found, err := ownPhotonID(d.registry, d.users, callerID)
	if err != nil {
		d.logger("[Photon][Profile] own profile lookup failed", "error", err.Error())
		return profileFail(errProfileLookupFailed)
	}
	if !found {
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
		updater:  api,
		showFullName: func() bool {
			// Та же политика, что sanitize.go: без конфигурации — минимум.
			cfg := api.GetConfig()
			return cfg != nil && cfg.GetSanitizeOptions()["fullname"]
		},
		logger: api.LogWarn,
	}
}

// normalizeDisplayName — правила ADR-091 §2.2. Возвращает нормализованное
// имя либо код отказа. Пустое имя после обрезки — законный результат
// (очистка, §2.3).
func normalizeDisplayName(raw string) (string, string) {
	if !utf8.ValidString(raw) {
		return "", errDisplayNameInvalid
	}
	name := strings.TrimSpace(raw)
	if name == "" {
		return "", ""
	}
	if utf8.RuneCountInString(name) > maxDisplayNameRunes {
		return "", errDisplayNameTooLong
	}
	onlyDigitsAndSpaces := true
	for _, r := range name {
		// Cc — управляющие; Cf — форматные (bidi-переопределения, нулевой
		// ширины); Zl, Zp — разделители строк и абзацев.
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
			return "", errDisplayNameInvalid
		}
		if !unicode.IsDigit(r) && !unicode.IsSpace(r) {
			onlyDigitsAndSpaces = false
		}
	}
	// Имя показывается ВМЕСТО PhotonID: «765 805 407» выдавал бы себя за
	// чужой PhotonID. Проверяется на нормализованном имени (§2.2).
	if onlyDigitsAndSpaces {
		return "", errDisplayNameInvalid
	}
	return name, ""
}

// setMyDisplayName — Action 216 (ADR-091). Меняет ТОЛЬКО Nickname
// вызывающего; субъект — только он сам, чужого идентификатора запрос не несёт.
func setMyDisplayName(d profileDeps, callerID string, raw json.RawMessage) Response {
	var p setMyDisplayNamePayload
	dec := json.NewDecoder(bytes.NewReader(raw))
	// Лишнее поле (username, props, user_id…) — отказ, а не молчаливый
	// пропуск: клиент, пославший его, ожидает, что оно что-то значит.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil || p.DisplayName == nil {
		return profileFail(errCodeInvalidRequest)
	}
	name, code := normalizeDisplayName(*p.DisplayName)
	if code != "" {
		return profileFail(code)
	}

	photonID, user, found, err := ownPhotonID(d.registry, d.users, callerID)
	if err != nil {
		d.logger("[Photon][Profile] own profile lookup failed (set name)", "error", err.Error())
		return profileFail(errProfileLookupFailed)
	}
	if !found {
		return profileFail(errSenderHasNoPhotonID)
	}

	user.Nickname = name
	updated, appErr := d.updater.UpdateUser(user)
	if appErr != nil {
		d.logger("[Photon][Profile] UpdateUser failed (set name)", "error", appErr.Error())
		return profileFail(errProfileUpdateFailed)
	}
	// Ответ — тот же минимальный профиль, что 211 (ADR-081 §2.5): ни
	// username, ни email, ни внутреннего id.
	return Response{OK: true, Data: profileOf(photonID, updated, true)}
}
