package main

import (
	"net/http"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
)

// Обязательное членство в команде engram (engram_team.go). Подделка plugin
// API повторяет teamService.JoinUserToTeam Mattermost 9.11
// (teams/teams.go:139-195): активное членство — без записи, удалённое —
// восстанавливается, отсутствующее — создаётся; лимит MaxUsersPerTeam — те же
// идентификаторы AppError, что у сервера.

type fakeTeams struct {
	team    *model.Team
	members map[string]*model.TeamMember
	users   *fakeUsers

	maxUsers     int
	getMemberErr *model.AppError
	createErr    *model.AppError
	getByNameErr *model.AppError

	getByNameCalls int
	createCalls    int
	writes         int
	createdTeams   []*model.Team

	// sessionsAtJoin — сколько сессий было выдано к моменту записи
	// членства: проверка порядка «членство → сессия».
	sessionsAtJoin []int
}

func goodEngramTeam() *model.Team {
	return &model.Team{Id: "teamengram", Name: engramTeamName, DisplayName: "ENGRAM", Type: model.TeamInvite}
}

func newFakeTeams(users *fakeUsers) *fakeTeams {
	return &fakeTeams{team: goodEngramTeam(), members: map[string]*model.TeamMember{}, users: users}
}

func (f *fakeTeams) GetTeamByName(name string) (*model.Team, *model.AppError) {
	f.getByNameCalls++
	if f.getByNameErr != nil {
		return nil, f.getByNameErr
	}
	if f.team == nil || f.team.Name != name {
		return nil, model.NewAppError("GetTeamByName", "app.team.get_by_name.missing.app_error", nil, "", http.StatusNotFound)
	}
	return f.team, nil
}

func (f *fakeTeams) CreateTeam(t *model.Team) (*model.Team, *model.AppError) {
	out := *t
	out.Id = "teamcreated"
	f.createdTeams = append(f.createdTeams, &out)
	f.team = &out
	return &out, nil
}

func (f *fakeTeams) GetTeamMember(teamID, userID string) (*model.TeamMember, *model.AppError) {
	if f.getMemberErr != nil {
		return nil, f.getMemberErr
	}
	m, ok := f.members[teamID+"/"+userID]
	if !ok {
		return nil, model.NewAppError("GetTeamMember", "app.team.get_member.missing.app_error", nil, "", http.StatusNotFound)
	}
	cp := *m
	return &cp, nil
}

func (f *fakeTeams) active() int {
	n := 0
	for _, m := range f.members {
		if m.DeleteAt == 0 {
			n++
		}
	}
	return n
}

func (f *fakeTeams) CreateTeamMember(teamID, userID string) (*model.TeamMember, *model.AppError) {
	f.createCalls++
	if f.createErr != nil {
		return nil, f.createErr
	}
	key := teamID + "/" + userID
	if m, ok := f.members[key]; ok {
		if m.DeleteAt == 0 {
			return m, nil // alreadyAdded
		}
		if f.maxUsers > 0 && f.active() >= f.maxUsers {
			return nil, model.NewAppError("JoinUserToTeam", "app.team.join_user_to_team.max_accounts.app_error", nil, "", http.StatusBadRequest)
		}
		m.DeleteAt = 0
		f.writes++
		f.sessionsAtJoin = append(f.sessionsAtJoin, len(f.users.sessions))
		return m, nil
	}
	if f.maxUsers > 0 && f.active() >= f.maxUsers {
		return nil, model.NewAppError("JoinUserToTeam", "app.team.join_user_to_team.save_member.max_accounts.app_error", nil, "", http.StatusBadRequest)
	}
	m := &model.TeamMember{TeamId: teamID, UserId: userID, SchemeUser: true, CreateAt: 1}
	f.members[key] = m
	f.writes++
	f.sessionsAtJoin = append(f.sessionsAtJoin, len(f.users.sessions))
	return m, nil
}

func (f *fakeTeams) memberOf(userID string) *model.TeamMember {
	return f.members[f.team.Id+"/"+userID]
}

// 1. Новый пользователь → членство создаётся, и ДО выдачи сессии.
func TestEngramTeam_NewUserGetsMembershipBeforeSession(t *testing.T) {
	r := newAuthRig(t)
	status, resp := r.login(t)
	if status != http.StatusOK || !resp.OK {
		t.Fatalf("вход: %d %+v", status, resp)
	}
	userID := r.users.sessions[0].UserId
	m := r.teams.memberOf(userID)
	if m == nil || m.DeleteAt != 0 {
		t.Fatalf("членства нет: %+v", m)
	}
	if len(r.teams.sessionsAtJoin) != 1 || r.teams.sessionsAtJoin[0] != 0 {
		t.Fatalf("членство записано не до сессии: %v", r.teams.sessionsAtJoin)
	}
}

// 2. Повторный вход → членство не дублируется, записи нет.
func TestEngramTeam_RepeatLoginIsIdempotent(t *testing.T) {
	r := newAuthRig(t)
	for i := 0; i < 3; i++ {
		if status, resp := r.login(t); status != http.StatusOK || !resp.OK {
			t.Fatalf("вход %d: %d %+v", i, status, resp)
		}
	}
	if len(r.teams.members) != 1 {
		t.Fatalf("членств: %d", len(r.teams.members))
	}
	if r.teams.writes != 1 || r.teams.createCalls != 1 {
		t.Fatalf("повторный вход писал членство: writes=%d createCalls=%d", r.teams.writes, r.teams.createCalls)
	}
	if len(r.users.sessions) != 3 {
		t.Fatalf("сессий: %d", len(r.users.sessions))
	}
}

// 3. Существующий пользователь без членства → добавляется; с удалённым → восстанавливается.
func TestEngramTeam_ExistingUserMembershipIsRestored(t *testing.T) {
	r := newAuthRig(t)
	if status, _ := r.login(t); status != http.StatusOK {
		t.Fatal("первый вход")
	}
	userID := r.users.sessions[0].UserId

	// Членство потеряно целиком (как у пользователей, заведённых до этого шага).
	delete(r.teams.members, r.teams.team.Id+"/"+userID)
	if status, resp := r.login(t); status != http.StatusOK || !resp.OK {
		t.Fatalf("вход без членства: %d %+v", status, resp)
	}
	if m := r.teams.memberOf(userID); m == nil || m.DeleteAt != 0 {
		t.Fatalf("не добавлено: %+v", m)
	}

	// Членство удалено (DeleteAt > 0) — восстанавливается тем же вызовом.
	r.teams.memberOf(userID).DeleteAt = 12345
	if status, resp := r.login(t); status != http.StatusOK || !resp.OK {
		t.Fatalf("вход с удалённым членством: %d %+v", status, resp)
	}
	if m := r.teams.memberOf(userID); m.DeleteAt != 0 {
		t.Fatalf("не восстановлено: DeleteAt=%d", m.DeleteAt)
	}
	if len(r.teams.members) != 1 {
		t.Fatalf("дубль членства: %d", len(r.teams.members))
	}
}

// 4. Ошибка добавления → сессия не выдаётся; лимит — свой код.
func TestEngramTeam_FailureIssuesNoSession(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(r *authRig)
		status int
		code   string
	}{
		{"CreateTeamMember fails", func(r *authRig) {
			r.teams.createErr = model.NewAppError("JoinUserToTeam", "app.team.join_user_to_team.save_member.app_error", nil, "", 500)
		}, http.StatusInternalServerError, errCodeTeamMembership},
		{"MaxUsersPerTeam on create", func(r *authRig) {
			r.teams.maxUsers = 1
			r.teams.members["teamengram/someone"] = &model.TeamMember{TeamId: "teamengram", UserId: "someone"}
		}, http.StatusServiceUnavailable, errCodeTeamFull},
		{"membership lookup fails", func(r *authRig) {
			r.teams.getMemberErr = model.NewAppError("GetTeamMember", "app.team.get_member.app_error", nil, "", 500)
		}, http.StatusInternalServerError, errCodeTeamMembership},
		{"team missing", func(r *authRig) { r.teams.team = nil }, http.StatusServiceUnavailable, errCodeTeamUnavailable},
		{"team email set (would make users admins)", func(r *authRig) {
			r.teams.team.Email = "admin@example.com"
		}, http.StatusServiceUnavailable, errCodeTeamUnavailable},
		{"team open for invites", func(r *authRig) {
			r.teams.team.AllowOpenInvite = true
		}, http.StatusServiceUnavailable, errCodeTeamUnavailable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newAuthRig(t)
			c.setup(r)
			status, resp := r.login(t)
			requireCode(t, status, resp, c.status, c.code)
			if resp.SessionToken != "" || len(r.users.sessions) != 0 {
				t.Fatalf("сессия выдана при отказе членства: %+v", r.users.sessions)
			}
		})
	}
}

// Лимит при ВОССТАНОВЛЕНИИ удалённого членства — тоже errCodeTeamFull.
func TestEngramTeam_FullOnRestoreIsDiagnosable(t *testing.T) {
	r := newAuthRig(t)
	if status, _ := r.login(t); status != http.StatusOK {
		t.Fatal("первый вход")
	}
	userID := r.users.sessions[0].UserId
	r.teams.memberOf(userID).DeleteAt = 1
	r.teams.members["teamengram/other"] = &model.TeamMember{TeamId: "teamengram", UserId: "other"}
	r.teams.maxUsers = 1
	status, resp := r.login(t)
	requireCode(t, status, resp, http.StatusServiceUnavailable, errCodeTeamFull)
	if len(r.users.sessions) != 1 {
		t.Fatal("сессия выдана при переполненной команде")
	}
}

// 5. Активное членство → вход успешен, записи в команду нет.
func TestEngramTeam_ActiveMembershipNeedsNoWrite(t *testing.T) {
	r := newAuthRig(t)
	if status, _ := r.login(t); status != http.StatusOK {
		t.Fatal("первый вход")
	}
	calls := r.teams.createCalls
	status, resp := r.login(t)
	if status != http.StatusOK || !resp.OK || resp.SessionToken == "" {
		t.Fatalf("вход с активным членством: %d %+v", status, resp)
	}
	if r.teams.createCalls != calls {
		t.Fatal("для активного членства вызван CreateTeamMember")
	}
}

// Вход не создаёт команду и ищет её один раз.
func TestEngramTeam_LoginNeverCreatesTeamAndCachesIt(t *testing.T) {
	r := newAuthRig(t)
	r.login(t)
	r.login(t)
	if len(r.teams.createdTeams) != 0 {
		t.Fatal("вход создал команду")
	}
	if r.teams.getByNameCalls != 1 {
		t.Fatalf("команда искалась %d раз", r.teams.getByNameCalls)
	}

	// Отказ не кэшируется: исправленную команду следующий вход находит.
	r2 := newAuthRig(t)
	r2.teams.team.Email = "x@example.com"
	st, rs := r2.login(t)
	requireCode(t, st, rs, http.StatusServiceUnavailable, errCodeTeamUnavailable)
	r2.teams.team.Email = ""
	if status, resp := r2.login(t); status != http.StatusOK || !resp.OK {
		t.Fatalf("после исправления команды: %d %+v", status, resp)
	}
}

// ensureEngramTeam — активация: найти; нет — создать с требуемыми настройками.
func TestEnsureEngramTeam(t *testing.T) {
	f := newFakeTeams(newFakeUsers())
	f.team = nil
	team, err := ensureEngramTeam(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.createdTeams) != 1 {
		t.Fatalf("создано команд: %d", len(f.createdTeams))
	}
	c := f.createdTeams[0]
	if c.Name != "engram" || c.Type != model.TeamInvite || c.AllowOpenInvite || c.Email != "" || c.AllowedDomains != "" {
		t.Fatalf("настройки созданной команды: %+v", c)
	}
	if team.Id != "teamcreated" {
		t.Fatal(team.Id)
	}

	// Существующая — не пересоздаётся.
	if _, err := ensureEngramTeam(f); err != nil || len(f.createdTeams) != 1 {
		t.Fatalf("повторная активация: err=%v created=%d", err, len(f.createdTeams))
	}

	// Существующая с чужими настройками — ошибка, НЕ молчаливая правка.
	f.team.Email = "boss@example.com"
	if _, err := ensureEngramTeam(f); err == nil {
		t.Fatal("команда с email принята")
	}
	if f.team.Email != "boss@example.com" || len(f.createdTeams) != 1 {
		t.Fatal("плагин переделал чужую команду")
	}

	// Сбой чтения — ошибка, без попытки создать.
	g := newFakeTeams(newFakeUsers())
	g.getByNameErr = model.NewAppError("GetTeamByName", "app.team.get_by_name.app_error", nil, "", 500)
	if _, err := ensureEngramTeam(g); err == nil || len(g.createdTeams) != 0 {
		t.Fatalf("сбой чтения: err=%v created=%d", err, len(g.createdTeams))
	}
}
