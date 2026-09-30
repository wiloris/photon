package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin/plugintest"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Acceptance-условия ADR-082 §4. Реестр НАСТОЯЩИЙ (поверх fakeKV); Mattermost —
// дублёр, повторяющий ту семантику, от которой зависит контракт:
//   - GetDirectChannel СОЗДАЁТ канал, GetChannelByName — нет;
//   - GetPostsAfter: CreateAt строго больше, чем у курсора; неизвестный
//     курсор — пустой список, не ошибка (sqlstore getPostsAround);
//   - CreatePost отвергает текст длиннее maxPostSize кодом
//     model.post.is_valid.msg.app_error (model.Post.IsValid).

type fakeDM struct {
	users        map[string]*model.User
	channels     map[string]*model.Channel // id → канал
	byName       map[string]string         // имя → id (только личные, TeamId "")
	posts        []*model.Post
	clock        int64
	maxPostSize  int
	created      int // сколько каналов создано
	createCalls  int // сколько раз вызван CreatePost
	getPostCalls int // сколько раз вызван GetPost
}

func newFakeDM() *fakeDM {
	return &fakeDM{
		users:       map[string]*model.User{},
		channels:    map[string]*model.Channel{},
		byName:      map[string]string{},
		clock:       1_000_000,
		maxPostSize: model.PostMessageMaxRunesV2,
	}
}

func (f *fakeDM) GetUser(id string) (*model.User, *model.AppError) {
	if u, ok := f.users[id]; ok {
		return u, nil
	}
	return nil, model.NewAppError("GetUser", "app.user.missing_account.const", nil, "", http.StatusNotFound)
}

func (f *fakeDM) GetDirectChannel(a, b string) (*model.Channel, *model.AppError) {
	name := model.GetDMNameFromIds(a, b)
	if id, ok := f.byName[name]; ok {
		return f.channels[id], nil
	}
	f.created++
	ch := &model.Channel{Id: fmt.Sprintf("chan%022d", f.created), Name: name, Type: model.ChannelTypeDirect}
	f.channels[ch.Id] = ch
	f.byName[name] = ch.Id
	return ch, nil
}

func (f *fakeDM) GetChannelByName(teamID, name string, _ bool) (*model.Channel, *model.AppError) {
	if id, ok := f.byName[name]; ok && teamID == "" {
		return f.channels[id], nil
	}
	return nil, model.NewAppError("GetChannelByName", "app.channel.get_by_name.missing.app_error", nil, "", http.StatusNotFound)
}

func (f *fakeDM) CreatePost(p *model.Post) (*model.Post, *model.AppError) {
	f.createCalls++
	if len([]rune(p.Message)) > f.maxPostSize {
		return nil, model.NewAppError("Post.IsValid", "model.post.is_valid.msg.app_error", nil, "", http.StatusBadRequest)
	}
	f.clock++
	stored := p.Clone()
	stored.Id = fmt.Sprintf("post%022d", len(f.posts)+1)
	stored.CreateAt = f.clock
	f.posts = append(f.posts, stored)
	return stored, nil
}

func (f *fakeDM) GetPost(id string) (*model.Post, *model.AppError) {
	f.getPostCalls++
	for _, p := range f.posts {
		if p.Id == id && p.DeleteAt == 0 {
			return p, nil
		}
	}
	return nil, model.NewAppError("GetPost", "app.post.get.app_error", nil, "", http.StatusNotFound)
}

func (f *fakeDM) channelPosts(channelID string) []*model.Post {
	var out []*model.Post
	for _, p := range f.posts {
		if p.ChannelId == channelID && p.DeleteAt == 0 {
			out = append(out, p)
		}
	}
	return out
}

func postList(ps []*model.Post) *model.PostList {
	l := model.NewPostList()
	for _, p := range ps {
		l.AddPost(p)
		l.AddOrder(p.Id)
	}
	return l
}

// Самые свежие perPage — как GetPostsPage (порядок выдачи — новые первыми).
func (f *fakeDM) GetPostsForChannel(channelID string, _, perPage int) (*model.PostList, *model.AppError) {
	ps := f.channelPosts(channelID)
	sort.Slice(ps, func(i, j int) bool { return ps[i].CreateAt > ps[j].CreateAt })
	if len(ps) > perPage {
		ps = ps[:perPage]
	}
	return postList(ps), nil
}

func (f *fakeDM) GetPostsAfter(channelID, postID string, _, perPage int) (*model.PostList, *model.AppError) {
	var cursorAt int64 = -1
	for _, p := range f.posts { // подзапрос не смотрит на DeleteAt
		if p.Id == postID {
			cursorAt = p.CreateAt
		}
	}
	if cursorAt < 0 {
		return postList(nil), nil // неизвестный курсор — пусто, не ошибка
	}
	var ps []*model.Post
	for _, p := range f.channelPosts(channelID) {
		if p.CreateAt > cursorAt {
			ps = append(ps, p)
		}
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].CreateAt < ps[j].CreateAt })
	if len(ps) > perPage {
		ps = ps[:perPage]
	}
	return postList(ps), nil
}

type dmRig struct {
	kv       *fakeKV
	registry *AccountRegistry
	mm       *fakeDM
	regErr   error
}

type dmParty struct {
	mmID, photonID, subject, email, login string
}

func newDMRig(t *testing.T) *dmRig {
	t.Helper()
	kv := newFakeKV()
	return &dmRig{kv: kv, registry: NewAccountRegistry(kv), mm: newFakeDM()}
}

func (r *dmRig) FindByPhotonID(id string) (AccountRecord, bool, error) {
	if r.regErr != nil {
		return AccountRecord{}, false, r.regErr
	}
	return r.registry.FindByPhotonID(id)
}

func (r *dmRig) deps() directMessageDeps {
	return directMessageDeps{registry: r, api: r.mm, logger: func(string, ...any) {}}
}

// party — пользователь, прошедший ENGRAM-вход (ADR-080): запись linked и
// пользователь Mattermost с AuthService engram.
func (r *dmRig) party(t *testing.T, name string) dmParty {
	t.Helper()
	subject := "google-sub-" + name
	email := name + "@example.com"
	mmID := fmt.Sprintf("mmuser%020s", name)
	acc, err := r.registry.Register("google", subject, email)
	require.NoError(t, err)
	acc, err = r.registry.Link("google", subject, mmID)
	require.NoError(t, err)
	u := engramUser(acc, email)
	u.Id = mmID
	r.mm.users[mmID] = u
	return dmParty{mmID: mmID, photonID: acc.PhotonID, subject: subject, email: email, login: acc.LoginName}
}

func (r *dmRig) send(t *testing.T, from dmParty, to, text string) Response {
	t.Helper()
	return sendDirectMessage(r.deps(), from.mmID, pl(t, map[string]string{"to_photon_id": to, "text": text}))
}

func (r *dmRig) list(t *testing.T, who dmParty, with string, extra map[string]any) Response {
	t.Helper()
	payload := map[string]any{"with_photon_id": with}
	for k, v := range extra {
		payload[k] = v
	}
	return listDirectMessages(r.deps(), who.mmID, pl(t, payload))
}

func sentMessage(t *testing.T, resp Response) DirectMessage {
	t.Helper()
	require.True(t, resp.OK, "ответ: %+v", resp)
	return resp.Data.(map[string]DirectMessage)["message"]
}

func messagesOf(t *testing.T, resp Response) []DirectMessage {
	t.Helper()
	require.True(t, resp.OK, "ответ: %+v", resp)
	return resp.Data.(map[string][]DirectMessage)["messages"]
}

// ── отправка ────────────────────────────────────────────────────────

func TestDMSendAToB(t *testing.T) {
	r := newDMRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")

	m := sentMessage(t, r.send(t, a, b.photonID, "привет, B"))
	require.Equal(t, a.photonID, m.AuthorPhotonID)
	require.Equal(t, "привет, B", m.Text)
	require.NotEmpty(t, m.ID)
	require.NotZero(t, m.CreatedAt)

	require.Equal(t, 1, r.mm.created, "первое сообщение создаёт личный канал")
	require.Len(t, r.mm.posts, 1)
	post := r.mm.posts[0]
	require.Equal(t, a.mmID, post.UserId, "автор — вызывающий, из сессии")
	require.Equal(t, r.mm.byName[model.GetDMNameFromIds(a.mmID, b.mmID)], post.ChannelId,
		"пост лёг в личный канал именно A и B")

	sentMessage(t, r.send(t, a, b.photonID, "ещё"))
	require.Equal(t, 1, r.mm.created, "второе сообщение идёт в тот же канал")
}

func TestDMTextIsStoredUnchanged(t *testing.T) {
	r := newDMRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	text := "  отступ\nстрока 2\t🙂  "
	require.Equal(t, text, sentMessage(t, r.send(t, a, b.photonID, text)).Text)
	require.Equal(t, text, r.mm.posts[0].Message)
}

func TestDMTextLimits(t *testing.T) {
	r := newDMRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")

	for _, empty := range []string{"", " ", "\n\t  "} {
		require.Equal(t, dmFail(errMessageEmpty), r.send(t, a, b.photonID, empty), "%q", empty)
	}
	// Руны, а не байты: «я» — два байта UTF-8.
	sentMessage(t, r.send(t, a, b.photonID, strings.Repeat("я", 16383)))
	calls := r.mm.createCalls
	require.Equal(t, dmFail(errMessageTooLong), r.send(t, a, b.photonID, strings.Repeat("я", 16384)))
	require.Equal(t, calls, r.mm.createCalls, "слишком длинный текст дошёл до Mattermost — отказ обязан быть ДО него")
}

// Установка с меньшим столбцом posts.message отвергает сама — ответ тот же.
func TestDMMattermostLengthRejectionMapsToTooLong(t *testing.T) {
	r := newDMRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	r.mm.maxPostSize = 4000
	require.Equal(t, dmFail(errMessageTooLong), r.send(t, a, b.photonID, strings.Repeat("x", 4001)))
}

func TestDMSelfMessageIsAllowed(t *testing.T) {
	r := newDMRig(t)
	a := r.party(t, "a")
	m := sentMessage(t, r.send(t, a, a.photonID, "заметка себе"))
	require.Equal(t, a.photonID, m.AuthorPhotonID)
	require.Contains(t, r.mm.byName, model.GetDMNameFromIds(a.mmID, a.mmID))

	msgs := messagesOf(t, r.list(t, a, a.photonID, nil))
	require.Len(t, msgs, 1)
	require.Equal(t, a.photonID, msgs[0].AuthorPhotonID)
}

// ── адресация ───────────────────────────────────────────────────────

func TestDMAddressing(t *testing.T) {
	r := newDMRig(t)
	a := r.party(t, "a")
	for _, bad := range []string{"", "12345678", "123 456 789", "12345678a"} {
		require.Equal(t, dmFail(errInvalidPhotonID), r.send(t, a, bad, "x"), "send %q", bad)
		require.Equal(t, dmFail(errInvalidPhotonID), r.list(t, a, bad, nil), "list %q", bad)
	}
	require.Equal(t, dmFail(errPhotonIDNotFound), r.send(t, a, "000000001", "x"))
	require.Equal(t, dmFail(errPhotonIDNotFound), r.list(t, a, "000000001", nil))

	c := r.party(t, "c")
	r.mm.users[c.mmID].DeleteAt = 1
	require.Equal(t, dmFail(errPhotonIDNotFound), r.send(t, a, c.photonID, "x"), "деактивированный")

	require.Zero(t, r.mm.created, "неудачная адресация создала канал")
	require.Empty(t, r.mm.posts)
}

func TestDMSenderWithoutPhotonID(t *testing.T) {
	r := newDMRig(t)
	b := r.party(t, "b")
	r.mm.users["mm-plain"] = &model.User{Id: "mm-plain", Username: "plain"}
	plain := dmParty{mmID: "mm-plain"}
	require.Equal(t, dmFail(errSenderHasNoPhotonID), r.send(t, plain, b.photonID, "x"))
	require.Equal(t, dmFail(errSenderHasNoPhotonID), r.list(t, plain, b.photonID, nil))
}

func TestDMRegistryFailure(t *testing.T) {
	r := newDMRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	r.regErr = errors.New("kv down")
	require.Equal(t, dmFail(errMessageFailed), r.send(t, a, b.photonID, "x"))
	require.Equal(t, dmFail(errMessageFailed), r.list(t, a, b.photonID, nil))
}

// ── история ─────────────────────────────────────────────────────────

func TestDMListBothSidesWithAuthors(t *testing.T) {
	r := newDMRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	sentMessage(t, r.send(t, a, b.photonID, "A→B"))
	sentMessage(t, r.send(t, b, a.photonID, "B→A"))

	for _, view := range []struct {
		who  dmParty
		with string
	}{{a, b.photonID}, {b, a.photonID}} {
		msgs := messagesOf(t, r.list(t, view.who, view.with, nil))
		require.Len(t, msgs, 2)
		require.Equal(t, "A→B", msgs[0].Text)
		require.Equal(t, a.photonID, msgs[0].AuthorPhotonID)
		require.Equal(t, "B→A", msgs[1].Text)
		require.Equal(t, b.photonID, msgs[1].AuthorPhotonID)
		require.Less(t, msgs[0].CreatedAt, msgs[1].CreatedAt, "по возрастанию времени")
	}
}

func TestDMListAfterIsStrictAndIncremental(t *testing.T) {
	r := newDMRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	m1 := sentMessage(t, r.send(t, a, b.photonID, "1"))
	m2 := sentMessage(t, r.send(t, b, a.photonID, "2"))

	after1 := messagesOf(t, r.list(t, b, a.photonID, map[string]any{"after": m1.ID}))
	require.Len(t, after1, 1)
	require.Equal(t, m2.ID, after1[0].ID, "строго после курсора — сам курсор не входит")

	require.Empty(t, messagesOf(t, r.list(t, b, a.photonID, map[string]any{"after": m2.ID})),
		"опрос без новых сообщений — пусто")

	m3 := sentMessage(t, r.send(t, a, b.photonID, "3"))
	next := messagesOf(t, r.list(t, b, a.photonID, map[string]any{"after": m2.ID}))
	require.Len(t, next, 1)
	require.Equal(t, m3.ID, next[0].ID)
}

func TestDMListInvalidCursor(t *testing.T) {
	r := newDMRig(t)
	a, b, c := r.party(t, "a"), r.party(t, "b"), r.party(t, "c")

	// Канала A–B ещё нет — курсора из него быть не может.
	require.Equal(t, dmFail(errInvalidCursor), r.list(t, a, b.photonID, map[string]any{"after": "post-x"}))

	sentMessage(t, r.send(t, a, b.photonID, "ab"))
	other := sentMessage(t, r.send(t, a, c.photonID, "ac"))
	require.Equal(t, dmFail(errInvalidCursor), r.list(t, a, b.photonID, map[string]any{"after": "no-such-post"}),
		"неизвестный id")
	require.Equal(t, dmFail(errInvalidCursor), r.list(t, a, b.photonID, map[string]any{"after": other.ID}),
		"курсор из чужого разговора")
}

func TestDMListLimit(t *testing.T) {
	r := newDMRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	for i := 0; i < 3; i++ {
		sentMessage(t, r.send(t, a, b.photonID, fmt.Sprint(i)))
	}
	for _, bad := range []int{0, -1, 201} {
		require.Equal(t, dmFail(errInvalidLimit), r.list(t, a, b.photonID, map[string]any{"limit": bad}), "%d", bad)
	}
	last := messagesOf(t, r.list(t, a, b.photonID, map[string]any{"limit": 1}))
	require.Len(t, last, 1)
	require.Equal(t, "2", last[0].Text, "без after — самые свежие")
	require.Len(t, messagesOf(t, r.list(t, a, b.photonID, map[string]any{"limit": 200})), 3)
}

func TestDMListDoesNotCreateChannel(t *testing.T) {
	r := newDMRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	require.Empty(t, messagesOf(t, r.list(t, a, b.photonID, nil)))
	require.Zero(t, r.mm.created, "чтение создало личный канал")
}

func TestDMListSkipsSystemAndForeignPosts(t *testing.T) {
	r := newDMRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	m := sentMessage(t, r.send(t, a, b.photonID, "настоящее"))
	chID := r.mm.posts[0].ChannelId
	r.mm.clock++
	r.mm.posts = append(r.mm.posts,
		&model.Post{Id: "sys", ChannelId: chID, UserId: a.mmID, Type: model.PostTypeJoinChannel, CreateAt: r.mm.clock},
		&model.Post{Id: "alien", ChannelId: chID, UserId: "mm-third", CreateAt: r.mm.clock + 1})

	msgs := messagesOf(t, r.list(t, b, a.photonID, nil))
	require.Len(t, msgs, 1)
	require.Equal(t, m.ID, msgs[0].ID)
}

// ── приватность (ADR-082 §3.2) ──────────────────────────────────────

func TestDMResponsesDoNotLeakInternalIDs(t *testing.T) {
	r := newDMRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	responses := map[string]Response{"send": r.send(t, a, b.photonID, "hi")}
	responses["list"] = r.list(t, b, a.photonID, nil)
	chName := model.GetDMNameFromIds(a.mmID, b.mmID)
	chID := r.mm.byName[chName]

	for name, resp := range responses {
		body, err := json.Marshal(resp)
		require.NoError(t, err)
		for _, secret := range []string{a.mmID, b.mmID, chID, chName, a.login, b.login, a.email, b.email, a.subject, b.subject, "from_plugin"} {
			require.NotContains(t, string(body), secret, "%s: утёк %q", name, secret)
		}
	}
	// Форма сообщения — ровно четыре поля.
	body, _ := json.Marshal(responses["send"])
	var env struct {
		Data struct {
			Message map[string]any `json:"message"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(body, &env))
	keys := []string{}
	for k := range env.Data.Message {
		keys = append(keys, k)
	}
	require.ElementsMatch(t, []string{"id", "author_photon_id", "text", "created_at"}, keys)
}

// ── авторизация (ADR-082 §3.7) ──────────────────────────────────────

// dmFixture — стенд authz с реестром: A и B связаны с пользователями
// фикстуры uA и uB.
func dmFixture(t *testing.T) (*authz, *fakeAuthzAPI, string, string) {
	t.Helper()
	z := newFixture()
	api := z.api.(*fakeAuthzAPI)
	kv := newFakeKV()
	reg := NewAccountRegistry(kv)
	link := func(subject, mm string) string {
		acc, err := reg.Register("google", subject, subject+"@example.com")
		require.NoError(t, err)
		_, err = reg.Link("google", subject, mm)
		require.NoError(t, err)
		return acc.PhotonID
	}
	pidA, pidB := link("sub-a", uA), link("sub-b", uB)
	z.accounts = reg
	return z, api, pidA, pidB
}

func TestDMAuthzBeforeChannelExists(t *testing.T) {
	z, _, pidA, _ := dmFixture(t)
	const nobody = "user-without-grants"

	require.NoError(t, z.authorize(uB, ActionSendDirectMessage, pl(t, map[string]string{"to_photon_id": pidA})),
		"канала нет — достаточно create_direct_channel")
	require.ErrorIs(t, z.authorize(nobody, ActionSendDirectMessage, pl(t, map[string]string{"to_photon_id": pidA})),
		errForbidden, "без create_direct_channel")
	require.NoError(t, z.authorize(nobody, ActionListDirectMessages, pl(t, map[string]string{"with_photon_id": pidA})),
		"канала нет — читать нечего, ответ пустой")
	// Негодный и несуществующий PhotonID — объекта нет; различимый ответ
	// даёт обработчик.
	for _, pid := range []string{"bad", "000000001"} {
		require.NoError(t, z.authorize(uB, ActionSendDirectMessage, pl(t, map[string]string{"to_photon_id": pid})), pid)
	}
}

func TestDMAuthzOnExistingChannel(t *testing.T) {
	z, api, pidA, _ := dmFixture(t)
	chID := "dm-ab"
	api.channels[chID] = &model.Channel{Id: chID, Name: model.GetDMNameFromIds(uA, uB), Type: model.ChannelTypeDirect}
	api.byName["/"+model.GetDMNameFromIds(uA, uB)] = chID

	send := pl(t, map[string]string{"to_photon_id": pidA})
	list := pl(t, map[string]string{"with_photon_id": pidA})
	require.ErrorIs(t, z.authorize(uB, ActionSendDirectMessage, send), errForbidden, "нет create_post в канале")
	require.ErrorIs(t, z.authorize(uB, ActionListDirectMessages, list), errForbidden, "нет чтения канала")

	api.grant(uB, "channel", chID, model.PermissionCreatePost, model.PermissionReadChannelContent)
	require.NoError(t, z.authorize(uB, ActionSendDirectMessage, send))
	require.NoError(t, z.authorize(uB, ActionListDirectMessages, list))
}

func TestDMAuthzFailsClosed(t *testing.T) {
	z, _, pidA, _ := dmFixture(t)
	z.accounts = nil
	require.ErrorIs(t, z.authorize(uB, ActionSendDirectMessage, pl(t, map[string]string{"to_photon_id": pidA})), errForbidden)

	z, _, pidA, _ = dmFixture(t)
	z.accounts = failingFinder{}
	require.ErrorIs(t, z.authorize(uB, ActionListDirectMessages, pl(t, map[string]string{"with_photon_id": pidA})), errForbidden)
}

type failingFinder struct{}

func (failingFinder) FindByPhotonID(string) (AccountRecord, bool, error) {
	return AccountRecord{}, false, errors.New("kv down")
}

func TestDMHTTPWithoutSessionIsUnauthorized(t *testing.T) {
	api := &plugintest.API{}
	api.On("LogDebug", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Maybe()
	api.On("GetSession", "bogus").Return(nil,
		model.NewAppError("GetSession", "session.not_found", nil, "", http.StatusUnauthorized))
	p := &Plugin{}
	p.SetAPI(api)
	p.router = p.initRouter()
	p.quicListener = &QuicListener{plugin: p, dispatcher: &Dispatcher{plugin: p}}

	for _, action := range []int{ActionSendDirectMessage, ActionListDirectMessages} {
		for _, token := range []string{"", "bogus"} {
			body, err := json.Marshal(map[string]any{
				"auth":    map[string]string{"session_token": token},
				"header":  map[string]any{"action": action, "resource": "UserResource"},
				"payload": map[string]string{"to_photon_id": "123456789", "with_photon_id": "123456789", "text": "x"},
			})
			require.NoError(t, err)
			w := httptest.NewRecorder()
			p.ServeHTTP(nil, w, httptest.NewRequest(http.MethodPost, "/api/v1/action", bytes.NewReader(body)))
			require.Equal(t, http.StatusUnauthorized, w.Code, "action %d, token %q", action, token)
		}
	}
}
