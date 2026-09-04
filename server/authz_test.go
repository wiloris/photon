package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
	photonpb "github.com/wiloris/photon-core/protobuf"
)

// Стенд повторяет фикстуру измеренного baseline'а (photon-p14-p14a):
// A — system_admin и член PRIV, B — обычный пользователь, в PRIV не входит.
const (
	uA     = "edx85ysmhtfbfntybmh1gqmp3h"
	uB     = "dxeit3gu9bbrteuz3pqq4tq4dh"
	team   = "hw6di856mfbkm8baubont8egfe"
	cPub   = "chan_pub"
	cPub2  = "chan_pub2"
	cPriv  = "chan_priv"
	cWall  = "chan_wall_a"
	cDM    = "chan_dm_a"
	pPriv  = "post_priv_by_a"
	pPub   = "post_pub_by_a"
	pPubB  = "post_pub_by_b"
	fPriv  = "file_in_priv"
	fLoose = "file_without_channel"
)

func newFixture() *authz {
	f := &fakeAuthzAPI{
		channels: map[string]*model.Channel{},
		posts:    map[string]*model.Post{},
		files:    map[string]*model.FileInfo{},
		byName:   map[string]string{},
		grants:   map[string]bool{},
	}
	add := func(id, name string, t model.ChannelType, teamID string) {
		f.channels[id] = &model.Channel{Id: id, Name: name, DisplayName: name, Type: t, TeamId: teamID}
		if teamID != "" {
			f.byName[teamID+"/"+name] = id
		}
	}
	add(cPub, "p14-pub", model.ChannelTypeOpen, team)
	add(cPub2, "p14-pub2", model.ChannelTypeOpen, team)
	add(cPriv, "p14-priv", model.ChannelTypePrivate, team)
	add(cWall, wallChannelName(uA), model.ChannelTypePrivate, team)
	add(cDM, uA+"__"+uA, model.ChannelTypeDirect, "")

	f.posts[pPriv] = &model.Post{Id: pPriv, UserId: uA, ChannelId: cPriv}
	f.posts[pPub] = &model.Post{Id: pPub, UserId: uA, ChannelId: cPub}
	f.posts[pPubB] = &model.Post{Id: pPubB, UserId: uB, ChannelId: cPub}
	f.files[fPriv] = &model.FileInfo{Id: fPriv, ChannelId: cPriv}
	f.files[fLoose] = &model.FileInfo{Id: fLoose, ChannelId: ""} // не прикреплён — §5

	// A — system_admin: права системного уровня, поэтому проходит всюду.
	f.grant(uA, "system", "",
		model.PermissionReadChannelContent, model.PermissionReadChannel,
		model.PermissionCreatePost, model.PermissionEditPost, model.PermissionDeletePost,
		model.PermissionEditOthersPosts, model.PermissionDeleteOthersPosts,
		model.PermissionAddReaction, model.PermissionRemoveReaction,
		model.PermissionUploadFile, model.PermissionViewMembers, model.PermissionViewTeam,
		model.PermissionManagePrivateChannelMembers, model.PermissionManagePublicChannelMembers,
		model.PermissionManagePrivateChannelProperties, model.PermissionManagePublicChannelProperties,
		model.PermissionCreatePublicChannel, model.PermissionCreatePrivateChannel,
		model.PermissionCreateDirectChannel, model.PermissionReadPublicChannel,
		model.PermissionJoinPublicChannels,
	)
	// B — team_user: команда.
	f.grant(uB, "team", team,
		model.PermissionViewTeam, model.PermissionReadPublicChannel,
		model.PermissionJoinPublicChannels, model.PermissionCreatePublicChannel,
		model.PermissionCreatePrivateChannel, model.PermissionListTeamChannels,
	)
	f.grant(uB, "system", "", model.PermissionCreateDirectChannel, model.PermissionViewMembers)
	// B — channel_user в PUB (член) и больше нигде.
	f.grant(uB, "channel", cPub,
		model.PermissionReadChannel, model.PermissionReadChannelContent,
		model.PermissionCreatePost, model.PermissionEditPost, model.PermissionDeletePost,
		model.PermissionAddReaction, model.PermissionRemoveReaction, model.PermissionUploadFile,
		model.PermissionManagePublicChannelMembers, model.PermissionManagePublicChannelProperties,
	)
	return newAuthz(f)
}

func pl(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// TestAuthzCoversEveryAction — полнота таблицы относительно того же источника,
// из которого Bridge берёт коды (ADR-025, ADR-039 §6). Новый action без
// правила роняет тесты, а не проходит молча.
func TestAuthzCoversEveryAction(t *testing.T) {
	for code, name := range photonpb.Action_name {
		if code == 0 {
			continue // ACTION_UNSPECIFIED — не действие
		}
		_, ok := rules[int(code)]
		require.True(t, ok, "у action %d (%s) нет правила авторизации", code, name)
	}
	for code := range rules {
		_, ok := photonpb.Action_name[int32(code)]
		require.True(t, ok, "правило для %d есть, а такого action в enum нет", code)
	}
	require.Equal(t, len(photonpb.Action_name)-1, len(rules))
}

// TestAuthzUnknownActionDenied — §7.1: поведение по умолчанию — отказ, даже
// при том, что тест полноты делает это состояние недостижимым.
func TestAuthzUnknownActionDenied(t *testing.T) {
	require.ErrorIs(t, newFixture().authorize(uB, 999, pl(t, map[string]string{})), errForbidden)
}

// TestAuthzBaselineVulnerabilities — по одной строке на измеренный обход из
// матрицы 56×2 (2026-09-04). До слоя авторизации все они возвращали ok:true.
func TestAuthzBaselineVulnerabilities(t *testing.T) {
	chPriv := map[string]string{"channel_id": cPriv}
	poPriv := map[string]string{"post_id": pPriv}
	fiPriv := map[string]string{"file_id": fPriv}

	cases := []struct {
		name    string
		action  int
		payload any
	}{
		{"100 createPost в приватный канал", 100, map[string]string{"channel_id": cPriv, "message": "x"}},
		{"101 getFeed приватного канала", 101, chPriv},
		{"104 getThread чужого поста", 104, poPriv},
		{"105 replyToPost в приватный", 105, map[string]string{"root_id": pPriv, "message": "x"}},
		{"106 pinPost в приватном", 106, poPriv},
		{"107 unpinPost в приватном", 107, poPriv},
		{"108 getPinnedPosts приватного", 108, chPriv},
		{"110 addReaction на чужой пост", 110, map[string]string{"post_id": pPriv, "emoji_name": "eyes"}},
		{"111 removeReaction на чужой пост", 111, map[string]string{"post_id": pPriv, "emoji_name": "eyes"}},
		{"112 getReactions чужого поста", 112, poPriv},
		{"113 toggleLike чужого поста", 113, poPriv},
		{"114 getPostSocialInfo чужого поста", 114, poPriv},
		{"301 getChannelMembers приватного", 301, chPriv},
		{"303 getChannel приватного", 303, chPriv},
		{"304 getChannelByName приватного", 304, map[string]string{"team_id": team, "name": "p14-priv"}},
		{"309 addMember себя в приватный", 309, map[string]string{"channel_id": cPriv, "user_id": uB}},
		{"311 updateChannel приватного", 311, map[string]string{"channel_id": cPriv, "display_name": "hijacked"}},
		{"314 getChannelMember в приватном", 314, map[string]string{"channel_id": cPriv, "user_id": uA}},
		{"500 uploadFile в приватный", 500, map[string]string{"channel_id": cPriv, "file_name": "x", "file_bytes": "eA=="}},
		{"501 downloadFile из приватного", 501, fiPriv},
		{"502 getFileInfo из приватного", 502, fiPriv},
		{"503 getThumbnail из приватного", 503, fiPriv},
		{"504 getPreview из приватного", 504, fiPriv},
		{"505 getImageURL из приватного", 505, fiPriv},
		{"600 getOrCreateWall чужой стены", 600, map[string]string{"team_id": team, "user_id": uA}},
		{"601 getWallPosts чужой стены", 601, map[string]string{"channel_id": cWall}},
		{"602 publishWallPost на чужую стену", 602, map[string]string{"channel_id": cWall, "message": "x"}},
		{"101 getFeed чужой переписки", 101, map[string]string{"channel_id": cDM}},
		{"102 deletePost чужого поста", 102, poPriv},
		{"103 updatePost чужого поста", 103, map[string]string{"post_id": pPriv, "message": "x"}},
	}
	z := newFixture()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.ErrorIs(t, z.authorize(uB, c.action, pl(t, c.payload)), errForbidden)
		})
	}
}

// TestAuthzAllowedStaysAllowed — вторая половина: правило, отвергающее всё,
// не является правилом. Разрешённое обязано работать.
func TestAuthzAllowedStaysAllowed(t *testing.T) {
	z := newFixture()
	cases := []struct {
		name    string
		subject string
		action  int
		payload any
	}{
		{"B getMe", uB, 200, map[string]string{}},
		{"B getFeed публичного, где он член", uB, 101, map[string]string{"channel_id": cPub}},
		{"B getFeed публичного, где он НЕ член", uB, 101, map[string]string{"channel_id": cPub2}},
		{"B pinPost в публичном, где НЕ член", uB, 106, map[string]string{"post_id": pPub}},
		{"B createPost в публичный", uB, 100, map[string]string{"channel_id": cPub, "message": "x"}},
		{"B deletePost собственного поста", uB, 102, map[string]string{"post_id": pPubB}},
		{"B getOrCreateWall собственной стены", uB, 600, map[string]string{"team_id": team}},
		{"B engramQuery", uB, 401, map[string]string{"key": "k"}},
		{"B engramDelete", uB, 402, map[string]string{"key": "k"}},
		{"B listChannels", uB, 300, map[string]string{"team_id": team}},
		{"B searchPosts", uB, 109, map[string]string{"team_id": team, "query": "q"}},
		{"A getFeed приватного", uA, 101, map[string]string{"channel_id": cPriv}},
		{"A downloadFile", uA, 501, map[string]string{"file_id": fPriv}},
		{"A getWallPosts своей стены", uA, 601, map[string]string{"channel_id": cWall}},
		{"A addMember в приватный", uA, 309, map[string]string{"channel_id": cPriv, "user_id": uB}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.NoError(t, z.authorize(c.subject, c.action, pl(t, c.payload)))
		})
	}
}

// TestAuthzOwnerSelectorIsSelector — ADR-039 §3. Измеренный дефект: A с ролью
// system_admin не мог удалить пост B, потому что owner-проверка решала сама.
// Селектор обязан выбирать между delete_post и delete_others_posts, а не
// отказывать.
func TestAuthzOwnerSelectorIsSelector(t *testing.T) {
	z := newFixture()
	require.NoError(t, z.authorize(uA, 102, pl(t, map[string]string{"post_id": pPubB})),
		"admin с delete_others_posts обязан удалять чужой пост")
	require.NoError(t, z.authorize(uB, 102, pl(t, map[string]string{"post_id": pPubB})),
		"владелец с delete_post обязан удалять свой")
	require.ErrorIs(t, z.authorize(uB, 102, pl(t, map[string]string{"post_id": pPub})), errForbidden,
		"без delete_others_posts чужой пост удалять нельзя")
	require.ErrorIs(t, z.authorize(uB, 103, pl(t, map[string]string{"post_id": pPub})), errForbidden)
}

// TestAuthzFailClosed — §7.2: неразрешимый объект даёт отказ, а не «проверять
// нечего». Отдельно — файл с пустым ChannelId, о котором предупреждает §5.
func TestAuthzFailClosed(t *testing.T) {
	z := newFixture()
	for _, c := range []struct {
		name    string
		action  int
		payload any
	}{
		{"пустой channel_id", 101, map[string]string{"channel_id": ""}},
		{"несуществующий канал", 101, map[string]string{"channel_id": "nope"}},
		{"несуществующий пост", 104, map[string]string{"post_id": "nope"}},
		{"несуществующий файл", 501, map[string]string{"file_id": "nope"}},
		{"файл без канала (§5)", 501, map[string]string{"file_id": fLoose}},
		{"несуществующий канал по имени", 304, map[string]string{"team_id": team, "name": "nope"}},
		{"битый payload", 101, "не-объект"},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.ErrorIs(t, z.authorize(uA, c.action, pl(t, c.payload)), errForbidden,
				"даже администратору: объект не разрешён — отказ")
		})
	}
}

// TestAuthzUniformDenial — §7.3: «нет прав» и «нет объекта» неразличимы,
// иначе перебор идентификаторов рисует карту чужих объектов.
func TestAuthzUniformDenial(t *testing.T) {
	z := newFixture()
	missing := z.authorize(uB, 304, pl(t, map[string]string{"team_id": team, "name": "no-such-channel"}))
	forbidden := z.authorize(uB, 304, pl(t, map[string]string{"team_id": team, "name": "p14-priv"}))
	require.Error(t, missing)
	require.Error(t, forbidden)
	require.Equal(t, missing.Error(), forbidden.Error(),
		"текст отказа обязан совпадать для несуществующего и запрещённого")
}

// TestAuthzReadCPublicFallback — §4. Обе ветки read(C) на одном стенде.
// Порядок веток здесь НЕ проверяется: живой прогон его не различал, и тест,
// который бы это утверждал, утверждал бы больше, чем измерено.
func TestAuthzReadCPublicFallback(t *testing.T) {
	z := newFixture()
	require.NoError(t, z.authorize(uB, 101, pl(t, map[string]string{"channel_id": cPub2})),
		"не-член читает публичный канал по team-праву read_public_channel")
	require.ErrorIs(t, z.authorize(uB, 101, pl(t, map[string]string{"channel_id": cPriv})), errForbidden,
		"в приватном канала-скоупа нет и fallback не применяется")

	withCompliance := newFixture()
	withCompliance.api.(*fakeAuthzAPI).compliance = true
	require.ErrorIs(t, withCompliance.authorize(uB, 101, pl(t, map[string]string{"channel_id": cPub2})), errForbidden,
		"при включённом compliance публичный fallback выключается")
}

// TestAuthzChannelTypeSelectors — §3: константа выбирается по типу канала,
// своей модели «приватный/публичный» не заводится.
func TestAuthzChannelTypeSelectors(t *testing.T) {
	z := newFixture()
	require.NoError(t, z.authorize(uB, 309, pl(t, map[string]string{"channel_id": cPub2, "user_id": uB})),
		"self-add в публичный — join_public_channels у team_user есть")
	require.ErrorIs(t, z.authorize(uB, 309, pl(t, map[string]string{"channel_id": cPub2, "user_id": uA})), errForbidden,
		"добавить другого в публичный без manage_public_channel_members нельзя")
	require.NoError(t, z.authorize(uB, 311, pl(t, map[string]string{"channel_id": cPub, "display_name": "x"})),
		"свойства публичного канала, где B — член")
	require.ErrorIs(t, z.authorize(uB, 311, pl(t, map[string]string{"channel_id": cPriv, "display_name": "x"})), errForbidden)
}

var _ = fmt.Sprintf
