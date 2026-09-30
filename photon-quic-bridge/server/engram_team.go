package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/mattermost/mattermost/server/public/model"
)

// Обязательное членство пользователя ENGRAM в скрытой команде Mattermost
// «engram».
//
// Каждый успешный POST /api/v1/auth/google ПЕРЕД выдачей сессии гарантирует
// «пользователь Mattermost ∈ команда engram». Не удалось — сессия не
// выдаётся: пользователь с сессией, но без команды, получил бы молча
// неработающие функции, а повторный вход всё доводит до конца, потому что
// каждый шаг идемпотентен.
//
// Всё — штатным plugin API Mattermost 9.11, без SQL:
//
//	GetTeamByName   → app.GetTeamByName
//	CreateTeam      → app.CreateTeam            (ТОЛЬКО при активации плагина)
//	GetTeamMember   → app.GetTeamMember
//	CreateTeamMember→ app.AddTeamMember → AddUserToTeam → JoinUserToTeam
//	                  → teamService.JoinUserToTeam
//
// teamService.JoinUserToTeam (teams/teams.go:139-195) сам идемпотентен:
// активное членство — «alreadyAdded», ничего не пишется; удалённое
// (DeleteAt > 0) — восстанавливается UpdateMember; отсутствующее —
// создаётся SaveMember. Лимит TeamSettings.MaxUsersPerTeam проверяется при
// создании и при восстановлении.

const (
	engramTeamName        = "engram"
	engramTeamDisplayName = "ENGRAM"

	// errCodeTeamUnavailable — команды нет, она настроена не так, как
	// требуется, либо её не удалось прочитать. Временная серверная причина.
	errCodeTeamUnavailable = "engram_team_unavailable"

	// errCodeTeamFull — сработал TeamSettings.MaxUsersPerTeam. Отдельный код,
	// чтобы переполнение команды не выглядело сбоем Google или входа.
	errCodeTeamFull = "engram_team_full"

	// errCodeTeamMembership — прочий отказ членства.
	errCodeTeamMembership = "engram_team_membership_error"
)

// Идентификаторы AppError Mattermost 9.11 для лимита участников команды
// (app/team.go JoinUserToTeam): при создании членства лимит сообщает store
// (ErrLimitExceeded), при восстановлении — сам сервис (MaxMemberCountError).
var teamFullErrorIDs = map[string]bool{
	"app.team.join_user_to_team.save_member.max_accounts.app_error": true,
	"app.team.join_user_to_team.max_accounts.app_error":             true,
}

// teamAdminAPI — операции над самой командой. Нужны ТОЛЬКО при активации.
type teamAdminAPI interface {
	GetTeamByName(name string) (*model.Team, *model.AppError)
	CreateTeam(team *model.Team) (*model.Team, *model.AppError)
}

// teamMemberAPI — то, что нужно входу. CreateTeam здесь нет намеренно:
// создать команду из входа невозможно по построению.
type teamMemberAPI interface {
	GetTeamByName(name string) (*model.Team, *model.AppError)
	GetTeamMember(teamID, userID string) (*model.TeamMember, *model.AppError)
	CreateTeamMember(teamID, userID string) (*model.TeamMember, *model.AppError)
}

// checkEngramTeam — команда такая, какой ENGRAM её требует.
//
// Email обязан быть пустым не для красоты: JoinUserToTeam делает участника
// АДМИНИСТРАТОРОМ команды, если его email совпадает с email команды
// (teams/teams.go:160). AllowedDomains отвергал бы пользователей по домену
// Google-адреса. Существующую команду с другими настройками плагин НЕ
// переделывает молча — это решение администратора; вход до исправления
// отказывает с errCodeTeamUnavailable.
func checkEngramTeam(t *model.Team) error {
	var bad []string
	if t.Type != model.TeamInvite {
		bad = append(bad, fmt.Sprintf("type=%q (нужно %q)", t.Type, model.TeamInvite))
	}
	if t.AllowOpenInvite {
		bad = append(bad, "allow_open_invite=true")
	}
	if t.Email != "" {
		bad = append(bad, "email не пуст")
	}
	if t.AllowedDomains != "" {
		bad = append(bad, "allowed_domains не пуст")
	}
	if t.DeleteAt != 0 {
		bad = append(bad, "команда удалена")
	}
	if len(bad) > 0 {
		return fmt.Errorf("engram team misconfigured: %s", strings.Join(bad, ", "))
	}
	return nil
}

// ensureEngramTeam — при активации плагина: найти команду по имени, а если
// её нет — создать с требуемыми настройками. Не при каждом входе.
func ensureEngramTeam(api teamAdminAPI) (*model.Team, error) {
	t, appErr := api.GetTeamByName(engramTeamName)
	if appErr == nil && t != nil {
		return t, checkEngramTeam(t)
	}
	if appErr != nil && appErr.StatusCode != http.StatusNotFound {
		return nil, fmt.Errorf("get team %q: %s", engramTeamName, appErr.Error())
	}

	created, appErr := api.CreateTeam(&model.Team{
		Name:            engramTeamName,
		DisplayName:     engramTeamDisplayName,
		Type:            model.TeamInvite,
		AllowOpenInvite: false,
		Email:           "",
		AllowedDomains:  "",
	})
	if appErr != nil {
		// Другой узел кластера мог создать её между чтением и записью.
		if again, getErr := api.GetTeamByName(engramTeamName); getErr == nil && again != nil {
			return again, checkEngramTeam(again)
		}
		return nil, fmt.Errorf("create team %q: %s", engramTeamName, appErr.Error())
	}
	return created, checkEngramTeam(created)
}

// engramTeam — id команды, найденной без создания. Кэшируется после первого
// успеха; неудача не кэшируется — следующий вход попробует снова (например,
// после того как администратор исправил настройки команды).
type engramTeam struct {
	mu sync.Mutex
	id string
}

func (e *engramTeam) set(id string) {
	e.mu.Lock()
	e.id = id
	e.mu.Unlock()
}

func (e *engramTeam) resolve(api teamMemberAPI) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.id != "" {
		return e.id, nil
	}
	t, appErr := api.GetTeamByName(engramTeamName)
	if appErr != nil || t == nil {
		if appErr != nil {
			return "", fmt.Errorf("get team %q: %s", engramTeamName, appErr.Error())
		}
		return "", fmt.Errorf("team %q not found", engramTeamName)
	}
	if err := checkEngramTeam(t); err != nil {
		return "", err
	}
	e.id = t.Id
	return e.id, nil
}

// ensureEngramMembership — пользователь ∈ команда engram. Пустой код — успех.
//
// Активное членство — ни одной записи. Нет или удалено — CreateTeamMember,
// который сам создаёт либо восстанавливает (JoinUserToTeam). Повторный
// вызов после успеха снова находит активное членство и ничего не делает.
func ensureEngramMembership(d authDeps, userID string) string {
	teamID, err := d.engramTeam.resolve(d.teams)
	if err != nil {
		d.logger("[Photon][Auth] engram team unavailable", "error", err.Error())
		return errCodeTeamUnavailable
	}

	member, appErr := d.teams.GetTeamMember(teamID, userID)
	switch {
	case appErr == nil && member != nil && member.DeleteAt == 0:
		return ""
	case appErr != nil && appErr.StatusCode != http.StatusNotFound:
		d.logger("[Photon][Auth] engram team membership lookup failed", "error", appErr.Error())
		return errCodeTeamMembership
	}

	// Нет членства (404) либо оно удалено (DeleteAt > 0).
	if _, appErr := d.teams.CreateTeamMember(teamID, userID); appErr != nil {
		if teamFullErrorIDs[appErr.Id] {
			d.logger("[Photon][Auth] engram team is full (TeamSettings.MaxUsersPerTeam)", "error", appErr.Error())
			return errCodeTeamFull
		}
		d.logger("[Photon][Auth] engram team membership failed", "error", appErr.Error())
		return errCodeTeamMembership
	}
	return ""
}
