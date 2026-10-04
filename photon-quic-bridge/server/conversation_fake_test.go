package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
)

// fakeConvMM — Mattermost для 900–902 и распределителя. Повторяет факты,
// на которых стоит спецификация: F5 (лимит каналов проверяется раньше
// уникальности, конфликт возвращает существующий канал), F6 (CreateTeam
// создаёт два канала по умолчанию, конфликт имени команды не возвращает).
// Посты пишутся в PostgreSQL своим соединением, как это делает Mattermost.
type fakeConvMM struct {
	mu          sync.Mutex
	db          *sql.DB
	maxChannels *int64

	teams       map[string]*model.Team     // по имени
	teamMembers map[string]map[string]bool // teamID → userID
	channels    map[string]*model.Channel  // teamID/name
	chanCount   map[string]int64           // teamID → неархивных каналов

	createTeamCalls    int
	createChannelCalls int

	// Внедрение отказов: срабатывают по одному разу на единицу счётчика.
	teamCreateThenFail int // создать команду и вернуть неопределённую ошибку
	teamFailNoCreate   int // вернуть неопределённую ошибку, ничего не создав
	teamAppearsForeign int // перед вызовом создать «чужую» команду с этим именем
	chanCreateThenFail int // создать канал и вернуть неопределённую ошибку
	postFailOnCall     int // номер вызова CreatePost (с 1), который откажет
	// postCreateThenFailOnCall — вызов, который запишет пост и вернёт ошибку
	// (ответ потерян: пост есть, Bridge о нём не знает).
	postCreateThenFailOnCall int
	postCalls                int
	memberFail               int
}

func newFakeConvMM(db *sql.DB, maxChannels int64) *fakeConvMM {
	return &fakeConvMM{
		db: db, maxChannels: model.NewPointer(maxChannels),
		teams: map[string]*model.Team{}, teamMembers: map[string]map[string]bool{},
		channels: map[string]*model.Channel{}, chanCount: map[string]int64{},
	}
}

func convAppErr(id string, status int) *model.AppError {
	return model.NewAppError("fake", id, nil, "", status)
}

func (f *fakeConvMM) addTeamLocked(name string) *model.Team {
	t := &model.Team{Id: model.NewId(), Name: name, DisplayName: name, Type: model.TeamInvite, CreateAt: model.GetMillis()}
	f.teams[name] = t
	f.teamMembers[t.Id] = map[string]bool{}
	f.chanCount[t.Id] = 2 // town-square, off-topic (F6)
	return t
}

func (f *fakeConvMM) CreateTeam(team *model.Team) (*model.Team, *model.AppError) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createTeamCalls++
	if f.teamAppearsForeign > 0 {
		f.teamAppearsForeign--
		f.addTeamLocked(team.Name)
	}
	if _, ok := f.teams[team.Name]; ok {
		return nil, convAppErr(mmErrTeamNameExists, http.StatusBadRequest)
	}
	if f.teamFailNoCreate > 0 {
		f.teamFailNoCreate--
		return nil, convAppErr("app.team.save.app_error", http.StatusInternalServerError)
	}
	t := f.addTeamLocked(team.Name)
	t.Type, t.AllowOpenInvite = team.Type, team.AllowOpenInvite
	if f.teamCreateThenFail > 0 {
		f.teamCreateThenFail--
		return nil, convAppErr("app.channel.create_channel.internal_error", http.StatusInternalServerError)
	}
	return t, nil
}

func (f *fakeConvMM) GetTeamByName(name string) (*model.Team, *model.AppError) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.teams[name]; ok {
		return t, nil
	}
	return nil, convAppErr("app.team.get_by_name.missing.app_error", http.StatusNotFound)
}

func (f *fakeConvMM) CreateTeamMember(teamID, userID string) (*model.TeamMember, *model.AppError) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.memberFail > 0 {
		f.memberFail--
		return nil, convAppErr("app.team.join_user_to_team.save_member.app_error", http.StatusInternalServerError)
	}
	f.teamMembers[teamID][userID] = true
	return &model.TeamMember{TeamId: teamID, UserId: userID}, nil
}

func (f *fakeConvMM) CreateChannel(ch *model.Channel) (*model.Channel, *model.AppError) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createChannelCalls++
	if *f.maxChannels >= 0 && f.chanCount[ch.TeamId] >= *f.maxChannels {
		return nil, convAppErr(mmErrChannelLimit, http.StatusBadRequest) // F5: раньше уникальности
	}
	key := ch.TeamId + "/" + ch.Name
	if existing, ok := f.channels[key]; ok {
		return existing, convAppErr(mmErrChannelExists, http.StatusBadRequest)
	}
	c := ch.DeepCopy()
	c.Id = model.NewId()
	f.channels[key] = c
	f.chanCount[ch.TeamId]++
	if f.chanCreateThenFail > 0 {
		f.chanCreateThenFail--
		return nil, convAppErr("app.channel.create_channel.internal_error", http.StatusInternalServerError)
	}
	return c, nil
}

func (f *fakeConvMM) GetChannelByName(teamID, name string, _ bool) (*model.Channel, *model.AppError) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.channels[teamID+"/"+name]; ok {
		return c, nil
	}
	return nil, convAppErr("app.channel.get_by_name.missing.app_error", http.StatusNotFound)
}

func (f *fakeConvMM) CreatePost(p *model.Post) (*model.Post, *model.AppError) {
	f.mu.Lock()
	f.postCalls++
	fail := f.postFailOnCall == f.postCalls
	lost := f.postCreateThenFailOnCall == f.postCalls
	f.mu.Unlock()
	if fail {
		return nil, convAppErr("app.post.save.app_error", http.StatusInternalServerError)
	}
	out := p.Clone()
	out.Id = model.NewId()
	if out.CreateAt == 0 {
		out.CreateAt = model.GetMillis()
	}
	props, _ := json.Marshal(out.GetProps())
	if _, err := f.db.Exec(`INSERT INTO posts (id, channelid, userid, createat, updateat, deleteat, message, type, props)
VALUES ($1, $2, $3, $4, $4, 0, $5, $6, $7::jsonb)`, out.Id, out.ChannelId, out.UserId, out.CreateAt, out.Message, out.Type, string(props)); err != nil {
		return nil, convAppErr("store", http.StatusInternalServerError)
	}
	if lost {
		return nil, convAppErr("app.post.create_post.lost_response", http.StatusInternalServerError)
	}
	return out, nil
}

func (f *fakeConvMM) GetConfig() *model.Config {
	f.mu.Lock()
	defer f.mu.Unlock()
	cfg := &model.Config{}
	cfg.TeamSettings.MaxChannelsPerTeam = model.NewPointer(*f.maxChannels)
	return cfg
}

func (f *fakeConvMM) setMaxChannels(n int64) { f.mu.Lock(); *f.maxChannels = n; f.mu.Unlock() }

// fillTeam — каналы, созданные вне Bridge, занимают предел команды.
func (f *fakeConvMM) fillTeam(teamName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.teams[teamName]
	f.chanCount[t.Id] = *f.maxChannels
}

func (f *fakeConvMM) channelsNamed(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.channels {
		if c.Name == name {
			n++
		}
	}
	return n
}

// fakeConvIdentity — реестр ENGRAM и пользователи Mattermost.
type fakeConvIdentity struct {
	mu    sync.Mutex
	recs  map[string]AccountRecord // PhotonID → запись
	users map[string]*model.User   // MM id → пользователь
}

func newFakeConvIdentity() *fakeConvIdentity {
	return &fakeConvIdentity{recs: map[string]AccountRecord{}, users: map[string]*model.User{}}
}

// person — пользователь ENGRAM: PhotonID и аккаунт Mattermost.
func (f *fakeConvIdentity) person(pid string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	mmID := model.NewId()
	f.recs[pid] = AccountRecord{State: StateLinked, PhotonID: pid, MMUserID: mmID}
	auth := pid
	f.users[mmID] = &model.User{Id: mmID, AuthService: engramAuthService, AuthData: &auth}
	return mmID
}

// plainUser — пользователь Mattermost без входа ENGRAM (PhotonID нет).
func (f *fakeConvIdentity) plainUser() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	mmID := model.NewId()
	f.users[mmID] = &model.User{Id: mmID}
	return mmID
}

func (f *fakeConvIdentity) FindByPhotonID(pid string) (AccountRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.recs[pid]
	return r, ok, nil
}

func (f *fakeConvIdentity) GetUser(id string) (*model.User, *model.AppError) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if u, ok := f.users[id]; ok {
		c := *u
		return &c, nil
	}
	return nil, convAppErr("app.user.missing_account.const", http.StatusNotFound)
}

func (f *fakeConvIdentity) deactivate(mmID string) {
	f.mu.Lock()
	f.users[mmID].DeleteAt = 1
	f.mu.Unlock()
}

// groupRig — схема PostgreSQL, фиктивный Mattermost, реестр и сервис.
type groupRig struct {
	db  *sql.DB
	mm  *fakeConvMM
	id  *fakeConvIdentity
	svc *convService
}

const convTestBot = "bot-engram-bridge"

func newGroupRig(t *testing.T, maxChannels int64) *groupRig {
	t.Helper()
	db := convMigrated(t)
	r := &groupRig{db: db, mm: newFakeConvMM(db, maxChannels), id: newFakeConvIdentity()}
	r.svc = &convService{
		db: db, mm: r.mm, registry: r.id, users: r.id, botID: convTestBot,
		now:      func() int64 { return time.Now().UnixMilli() },
		lockWait: 10 * time.Second, pollEvery: 5 * time.Millisecond,
		logger: func(string, ...any) {},
	}
	return r
}

func createReq(requestID, title string, members ...string) map[string]any {
	if members == nil {
		members = []string{}
	}
	return map[string]any{
		"request_id": requestID, "kind": "group", "title": title,
		"history_policy": "since_join", "member_photon_ids": members,
	}
}

func newRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	h := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])
}
