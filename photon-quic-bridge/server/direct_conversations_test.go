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

// Acceptance-условия ADR-084 §5. Реестр НАСТОЯЩИЙ; Mattermost — дублёр
// fakeDM (личные каналы и посты ADR-082) плюс GetChannelsForTeamForUser с
// семантикой SqlChannelStore.GetChannels: пустой teamId — все каналы
// участия, включая командные; непустой — этой команды и личные.

type convFake struct {
	*fakeDM
	teamChannels map[string][]string // id канала команды → участники
	teamArgs     []string            // с каким teamId звали
	channelsErr  *model.AppError
}

func newConvFake(mm *fakeDM) *convFake {
	return &convFake{fakeDM: mm, teamChannels: map[string][]string{}}
}

func (f *convFake) GetChannelsForTeamForUser(teamID, userID string, _ bool) ([]*model.Channel, *model.AppError) {
	f.teamArgs = append(f.teamArgs, teamID)
	if f.channelsErr != nil {
		return nil, f.channelsErr
	}
	var out []*model.Channel
	for id, ch := range f.channels {
		member := false
		if members, ok := f.teamChannels[id]; ok {
			for _, m := range members {
				member = member || m == userID
			}
			if teamID != "" && ch.TeamId != teamID && ch.TeamId != "" {
				member = false
			}
		} else {
			a, b := ch.GetBothUsersForDM()
			member = a == userID || b == userID
		}
		if !member {
			continue
		}
		c := *ch
		for _, p := range f.posts { // LastPostAt считает и системные посты
			if p.ChannelId == id && p.CreateAt > c.LastPostAt {
				c.LastPostAt = p.CreateAt
			}
		}
		out = append(out, &c)
	}
	return out, nil
}

type convRig struct {
	*dmRig
	conv *convFake
}

func newConvRig(t *testing.T) *convRig {
	r := newDMRig(t)
	return &convRig{dmRig: r, conv: newConvFake(r.mm)}
}

func (r *convRig) inbox(t *testing.T, who dmParty, payload any) Response {
	t.Helper()
	raw := json.RawMessage(`{}`)
	if payload != nil {
		raw = pl(t, payload)
	}
	return listDirectConversations(conversationDeps{registry: r, api: r.conv, logger: func(string, ...any) {}}, who.mmID, raw)
}

func conversationsOf(t *testing.T, resp Response) []DirectConversation {
	t.Helper()
	require.True(t, resp.OK, "ответ: %+v", resp)
	return resp.Data.(map[string][]DirectConversation)["conversations"]
}

func peers(cs []DirectConversation) []string {
	out := []string{}
	for _, c := range cs {
		out = append(out, c.PeerPhotonID)
	}
	return out
}

// ── основное ────────────────────────────────────────────────────────

func TestInboxEmpty(t *testing.T) {
	r := newConvRig(t)
	a := r.party(t, "a")
	require.Empty(t, conversationsOf(t, r.inbox(t, a, nil)))
	require.Equal(t, []string{""}, r.conv.teamArgs, "каналы берутся без команды — teamId пустой")
}

// Настоящий Mattermost 9.11 на пользователя без единого канала отвечает не
// пустым списком, а ошибкой app.channel.get_channels.not_found.app_error (404;
// app/channel.go, GetChannelsForTeamForUser). Для 215 это «пусто» (ADR-084,
// acceptance), а не message_failed. Найдено на стенде 2026-10-05: у каждого
// нового пользователя «Чаты» показывали «Ошибка сервера: message_failed».
func TestInboxEmptyWhenMattermostReportsNoChannels(t *testing.T) {
	r := newConvRig(t)
	a := r.party(t, "a")
	r.conv.channelsErr = model.NewAppError("GetChannelsForUser", mmNoChannelsErrorID, nil, "", 404)
	require.Empty(t, conversationsOf(t, r.inbox(t, a, nil)))

	// Иной отказ с тем же статусом — по-прежнему сбой Mattermost (ADR-084 §2.5).
	r.conv.channelsErr = model.NewAppError("GetChannelsForUser", "app.channel.get_channels.get.app_error", nil, "", 404)
	require.Equal(t, dmFail(errMessageFailed), r.inbox(t, a, nil))
}

// Главный сценарий ADR-084: B пишет A, A не знает PhotonID B — и видит.
func TestInboxShowsConversationStartedByPeer(t *testing.T) {
	r := newConvRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	m := sentMessage(t, r.send(t, b, a.photonID, "привет от B"))

	cs := conversationsOf(t, r.inbox(t, a, nil))
	require.Len(t, cs, 1)
	require.Equal(t, b.photonID, cs[0].PeerPhotonID)
	require.Equal(t, m, cs[0].LastMessage)
	require.Equal(t, b.photonID, cs[0].LastMessage.AuthorPhotonID)

	// У отправителя тот же разговор, собеседник — A.
	cb := conversationsOf(t, r.inbox(t, b, nil))
	require.Equal(t, []string{a.photonID}, peers(cb))
}

func TestInboxLastMessageAndOrderFollowActivity(t *testing.T) {
	r := newConvRig(t)
	a, b, c := r.party(t, "a"), r.party(t, "b"), r.party(t, "c")
	sentMessage(t, r.send(t, a, b.photonID, "1 → B"))
	sentMessage(t, r.send(t, a, c.photonID, "2 → C"))
	require.Equal(t, []string{c.photonID, b.photonID}, peers(conversationsOf(t, r.inbox(t, a, nil))))

	reply := sentMessage(t, r.send(t, b, a.photonID, "3 ← B"))
	cs := conversationsOf(t, r.inbox(t, a, nil))
	require.Equal(t, []string{b.photonID, c.photonID}, peers(cs), "новое сообщение поднимает разговор")
	require.Equal(t, reply, cs[0].LastMessage)
}

func TestInboxSelfConversationUsesOwnPhotonID(t *testing.T) {
	r := newConvRig(t)
	a := r.party(t, "a")
	m := sentMessage(t, r.send(t, a, a.photonID, "заметка"))
	cs := conversationsOf(t, r.inbox(t, a, nil))
	require.Len(t, cs, 1)
	require.Equal(t, a.photonID, cs[0].PeerPhotonID)
	require.Equal(t, m, cs[0].LastMessage)
}

// ── что в список не входит ──────────────────────────────────────────

func TestInboxSkipsUnconfirmedAndInactivePeers(t *testing.T) {
	r := newConvRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")

	// Собеседник не из ENGRAM (обычный пользователь Mattermost).
	r.mm.users["mm-plain"] = &model.User{Id: "mm-plain", Username: "plain"}
	ch, _ := r.mm.GetDirectChannel(a.mmID, "mm-plain")
	_, _ = r.mm.CreatePost(&model.Post{UserId: "mm-plain", ChannelId: ch.Id, Message: "hi"})

	// Поддельное зеркало: AuthData указывает на PhotonID B, реестр связан не с ним.
	forged := b.photonID
	r.mm.users["mm-forger"] = &model.User{Id: "mm-forger", AuthService: engramAuthService, AuthData: &forged}
	ch2, _ := r.mm.GetDirectChannel(a.mmID, "mm-forger")
	_, _ = r.mm.CreatePost(&model.Post{UserId: "mm-forger", ChannelId: ch2.Id, Message: "я B"})

	// Деактивированный ENGRAM-собеседник.
	c := r.party(t, "c")
	sentMessage(t, r.send(t, c, a.photonID, "до деактивации"))
	r.mm.users[c.mmID].DeleteAt = 1

	// Исчезнувший пользователь.
	ch3, _ := r.mm.GetDirectChannel(a.mmID, "mm-gone")
	_, _ = r.mm.CreatePost(&model.Post{UserId: a.mmID, ChannelId: ch3.Id, Message: "пусто"})

	require.Empty(t, conversationsOf(t, r.inbox(t, a, nil)))
}

func TestInboxSkipsChannelWithoutMessagesAndIgnoresSystemPosts(t *testing.T) {
	r := newConvRig(t)
	a, b, c := r.party(t, "a"), r.party(t, "b"), r.party(t, "c")
	_, _ = r.mm.GetDirectChannel(a.mmID, c.mmID) // канал есть, сообщений нет

	m := sentMessage(t, r.send(t, b, a.photonID, "настоящее"))
	chID := r.mm.byName[model.GetDMNameFromIds(a.mmID, b.mmID)]
	r.mm.clock++
	r.mm.posts = append(r.mm.posts, &model.Post{Id: "sys", ChannelId: chID, UserId: a.mmID,
		Type: model.PostTypeJoinChannel, CreateAt: r.mm.clock})

	cs := conversationsOf(t, r.inbox(t, a, nil))
	require.Equal(t, []string{b.photonID}, peers(cs))
	require.Equal(t, m, cs[0].LastMessage, "системный пост не становится последним сообщением")
}

// Командный канал в список не попадает — даже с именем в форме личного.
func TestInboxOnlyDirectChannels(t *testing.T) {
	r := newConvRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	id := "team-chan"
	r.mm.channels[id] = &model.Channel{Id: id, TeamId: "team1", Type: model.ChannelTypeOpen,
		Name: model.GetDMNameFromIds(a.mmID, b.mmID)}
	r.conv.teamChannels[id] = []string{a.mmID, b.mmID}
	_, _ = r.mm.CreatePost(&model.Post{UserId: b.mmID, ChannelId: id, Message: "в команде"})

	require.Empty(t, conversationsOf(t, r.inbox(t, a, nil)))
}

// ── параметры и отказы ──────────────────────────────────────────────

func TestInboxLimit(t *testing.T) {
	r := newConvRig(t)
	a, b, c := r.party(t, "a"), r.party(t, "b"), r.party(t, "c")
	sentMessage(t, r.send(t, b, a.photonID, "b"))
	sentMessage(t, r.send(t, c, a.photonID, "c"))

	for _, bad := range []int{0, -1, 201} {
		require.Equal(t, dmFail(errInvalidLimit), r.inbox(t, a, map[string]int{"limit": bad}), "%d", bad)
	}
	require.Equal(t, []string{c.photonID}, peers(conversationsOf(t, r.inbox(t, a, map[string]int{"limit": 1}))))
	require.Len(t, conversationsOf(t, r.inbox(t, a, map[string]int{"limit": 200})), 2)
}

func TestInboxSenderWithoutPhotonID(t *testing.T) {
	r := newConvRig(t)
	r.mm.users["mm-plain"] = &model.User{Id: "mm-plain"}
	require.Equal(t, dmFail(errSenderHasNoPhotonID), r.inbox(t, dmParty{mmID: "mm-plain"}, nil))
}

func TestInboxFailures(t *testing.T) {
	r := newConvRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	sentMessage(t, r.send(t, b, a.photonID, "x"))

	r.conv.channelsErr = model.NewAppError("GetChannelsForTeamForUser", "db", nil, "", 500)
	require.Equal(t, dmFail(errMessageFailed), r.inbox(t, a, nil))

	r.conv.channelsErr = nil
	r.regErr = errors.New("kv down")
	require.Equal(t, dmFail(errMessageFailed), r.inbox(t, a, nil))
}

// ── приватность (ADR-084 §2.4) ──────────────────────────────────────

func TestInboxDoesNotLeakInternalIDs(t *testing.T) {
	r := newConvRig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	sentMessage(t, r.send(t, b, a.photonID, "hi"))
	chName := model.GetDMNameFromIds(a.mmID, b.mmID)

	body, err := json.Marshal(r.inbox(t, a, nil))
	require.NoError(t, err)
	for _, secret := range []string{a.mmID, b.mmID, r.mm.byName[chName], chName, a.login, b.login, a.email, b.email, a.subject, b.subject, "from_plugin", "last_post_at"} {
		require.NotContains(t, string(body), secret, "утёк %q", secret)
	}
	var env struct {
		Data struct {
			Conversations []map[string]any `json:"conversations"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(body, &env))
	keys := []string{}
	for k := range env.Data.Conversations[0] {
		keys = append(keys, k)
	}
	require.ElementsMatch(t, []string{"peer_photon_id", "last_message"}, keys)
}

// ── авторизация ─────────────────────────────────────────────────────

func TestInboxAuthzRule(t *testing.T) {
	require.NoError(t, newFixture().authorize("user-without-grants", ActionListDirectConversations, pl(t, map[string]string{})),
		"список собственных разговоров доступен любому вошедшему — как getMe")
}

func TestInboxHTTPWithoutSessionIsUnauthorized(t *testing.T) {
	api := &plugintest.API{}
	api.On("LogDebug", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Maybe()
	api.On("GetSession", "bogus").Return(nil,
		model.NewAppError("GetSession", "session.not_found", nil, "", http.StatusUnauthorized))
	p := &Plugin{}
	p.SetAPI(api)
	p.router = p.initRouter()
	p.quicListener = &QuicListener{plugin: p, dispatcher: &Dispatcher{plugin: p}}

	for _, token := range []string{"", "bogus"} {
		body, err := json.Marshal(map[string]any{
			"auth":    map[string]string{"session_token": token},
			"header":  map[string]any{"action": ActionListDirectConversations, "resource": "UserResource"},
			"payload": map[string]any{},
		})
		require.NoError(t, err)
		w := httptest.NewRecorder()
		p.ServeHTTP(nil, w, httptest.NewRequest(http.MethodPost, "/api/v1/action", bytes.NewReader(body)))
		require.Equal(t, http.StatusUnauthorized, w.Code, "token %q", token)
	}
}
