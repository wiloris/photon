package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// createPost с photon_media: проверка владения и построение props.
//
// Ключевое свойство: props строятся из АВТОРИТЕТНЫХ метаданных хранилища, а не
// из того, что прислал клиент. Иначе сообщение несло бы ложь о вложении.

func postHandlerWithMedia(t *testing.T, uploaderID string) (*PostHandler, *fakeMediaStore, *plugintestAPI) {
	t.Helper()
	api := quietAPI()
	svcStore := newFakeMediaStore()
	svc := NewMediaService(svcStore)
	h := &PostHandler{plugin: handlersFor(api), media: svc}
	return h, svcStore, api
}

func storedObject(id, channel, uploader string, body []byte) *MediaObject {
	return &MediaObject{
		MediaID: id, ChannelID: channel, UploaderID: uploader,
		SHA256: hexSum(body), Size: int64(len(body)), Mime: "image/png", Name: "p.png",
		State: MediaStored, CreatedAt: 1, ExpiresAt: 2,
	}
}

func TestCreatePostBuildsPropsFromStoreMetadata(t *testing.T) {
	h, store, api := postHandlerWithMedia(t, "u1")
	body := mediaBytes(100)
	store.add(storedObject("mediaobj1", "c1", "u1", body))
	store.setBlob("mediaobj1", body)

	var captured *model.Post
	api.On("CreatePost", mock.Anything).Run(func(args mock.Arguments) {
		captured = args.Get(0).(*model.Post)
	}).Return(&model.Post{Id: "post1", ChannelId: "c1"}, nil)

	// Клиент ЛЖЁТ о содержимом: чужой sha256, чужой размер, чужое имя.
	payload, _ := json.Marshal(CreatePostPayload{
		ChannelID: "c1", Message: "hi",
		PhotonMedia: []MediaRef{{
			MediaID: "mediaobj1", SHA256: "0000000000000000000000000000000000000000000000000000000000000000",
			Size: 999999, Mime: "text/plain", Name: "подделка.txt",
		}},
	})
	resp := h.createPost("u1", payload)
	require.True(t, resp.OK, resp.Error)
	require.NotNil(t, captured)

	raw, err := json.Marshal(captured.GetProps()["photon_media"])
	require.NoError(t, err)
	var got []MediaRef
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Len(t, got, 1)

	require.Equal(t, hexSum(body), got[0].SHA256, "sha256 обязан браться из хранилища, а не из запроса")
	require.Equal(t, int64(len(body)), got[0].Size, "размер обязан браться из хранилища")
	require.Equal(t, "p.png", got[0].Name, "имя обязано браться из хранилища")
	require.Equal(t, "image/png", got[0].Mime)
}

// Сырые props от клиента не принимаются: поля для них в payload нет, и
// присланное игнорируется.
func TestCreatePostIgnoresRawProps(t *testing.T) {
	h, _, api := postHandlerWithMedia(t, "u1")
	var captured *model.Post
	api.On("CreatePost", mock.Anything).Run(func(args mock.Arguments) {
		captured = args.Get(0).(*model.Post)
	}).Return(&model.Post{Id: "post1"}, nil)

	// props и photon_media подсунуты в обход типизированного поля.
	raw := json.RawMessage(`{"channel_id":"c1","message":"hi",
		"props":{"photon_media":[{"media_id":"чужой"}],"deleteBy":"подделка"}}`)
	resp := h.createPost("u1", raw)
	require.True(t, resp.OK, resp.Error)
	require.NotNil(t, captured)
	require.Empty(t, captured.GetProps(), "сырые props клиента не должны попадать в пост")
}

// Чужой media_id: объект есть, но загружен не этим пользователем.
func TestCreatePostRejectsForeignMedia(t *testing.T) {
	h, store, api := postHandlerWithMedia(t, "u1")
	body := mediaBytes(50)
	store.add(storedObject("mediaobj2", "c1", "чужой-пользователь", body))
	store.setBlob("mediaobj2", body)
	api.On("CreatePost", mock.Anything).Return(&model.Post{Id: "post1"}, nil).Maybe()

	payload, _ := json.Marshal(CreatePostPayload{
		ChannelID: "c1", Message: "hi",
		PhotonMedia: []MediaRef{{MediaID: "mediaobj2"}},
	})
	resp := h.createPost("u1", payload)
	require.False(t, resp.OK, "чужая media не должна прикрепляться")
	require.Equal(t, errForbidden.Error(), resp.Error)
}

// Media из другого канала: пост в c1, объект в c2.
func TestCreatePostRejectsMediaFromAnotherChannel(t *testing.T) {
	h, store, api := postHandlerWithMedia(t, "u1")
	body := mediaBytes(50)
	store.add(storedObject("mediaobj3", "c2", "u1", body))
	store.setBlob("mediaobj3", body)
	api.On("CreatePost", mock.Anything).Return(&model.Post{Id: "post1"}, nil).Maybe()

	payload, _ := json.Marshal(CreatePostPayload{
		ChannelID: "c1", Message: "hi",
		PhotonMedia: []MediaRef{{MediaID: "mediaobj3"}},
	})
	resp := h.createPost("u1", payload)
	require.False(t, resp.OK)
	require.Equal(t, errForbidden.Error(), resp.Error)
}

// Несуществующий объект — тот же forbidden, что и отсутствие права.
func TestCreatePostRejectsUnknownMedia(t *testing.T) {
	h, _, api := postHandlerWithMedia(t, "u1")
	api.On("CreatePost", mock.Anything).Return(&model.Post{Id: "post1"}, nil).Maybe()
	payload, _ := json.Marshal(CreatePostPayload{
		ChannelID: "c1", Message: "hi",
		PhotonMedia: []MediaRef{{MediaID: "nosuchobject000000000000"}},
	})
	resp := h.createPost("u1", payload)
	require.False(t, resp.OK)
	require.Equal(t, errForbidden.Error(), resp.Error)
}

// Недоступный объект: известен, но байтов нет.
func TestCreatePostRejectsUnavailableMedia(t *testing.T) {
	h, store, api := postHandlerWithMedia(t, "u1")
	o := storedObject("mediaobj4", "c1", "u1", mediaBytes(10))
	o.State = MediaExpired
	store.add(o)
	api.On("CreatePost", mock.Anything).Return(&model.Post{Id: "post1"}, nil).Maybe()

	payload, _ := json.Marshal(CreatePostPayload{
		ChannelID: "c1", Message: "hi",
		PhotonMedia: []MediaRef{{MediaID: "mediaobj4"}},
	})
	resp := h.createPost("u1", payload)
	require.False(t, resp.OK)
	require.Equal(t, "unavailable", resp.Error)
}

// Уже прикреплённый объект нельзя прикрепить ко второму посту.
func TestCreatePostRejectsAlreadyAttachedMedia(t *testing.T) {
	h, store, api := postHandlerWithMedia(t, "u1")
	body := mediaBytes(10)
	o := storedObject("mediaobj5", "c1", "u1", body)
	o.PostID = "другой-пост"
	o.State = MediaAvailable
	store.add(o)
	store.setBlob("mediaobj5", body)
	api.On("CreatePost", mock.Anything).Return(&model.Post{Id: "post1"}, nil).Maybe()

	payload, _ := json.Marshal(CreatePostPayload{
		ChannelID: "c1", Message: "hi",
		PhotonMedia: []MediaRef{{MediaID: "mediaobj5"}},
	})
	resp := h.createPost("u1", payload)
	require.False(t, resp.OK)
}

// Успешное прикрепление доводит объект до AVAILABLE и связывает с постом.
func TestCreatePostAttachesMedia(t *testing.T) {
	h, store, api := postHandlerWithMedia(t, "u1")
	body := mediaBytes(100)
	store.add(storedObject("mediaobj6", "c1", "u1", body))
	store.setBlob("mediaobj6", body)
	api.On("CreatePost", mock.Anything).Return(&model.Post{Id: "post7", ChannelId: "c1"}, nil)

	payload, _ := json.Marshal(CreatePostPayload{
		ChannelID: "c1", Message: "hi",
		PhotonMedia: []MediaRef{{MediaID: "mediaobj6"}},
	})
	require.True(t, h.createPost("u1", payload).OK)

	back, err := store.Get("mediaobj6")
	require.NoError(t, err)
	require.Equal(t, "post7", back.PostID)
	require.Equal(t, MediaAvailable, back.State)
}

// Пост НЕ создаётся, если хоть одно вложение не прошло проверку.
func TestCreatePostNotCreatedWhenMediaRejected(t *testing.T) {
	h, store, api := postHandlerWithMedia(t, "u1")
	body := mediaBytes(10)
	store.add(storedObject("mediaobj8", "c1", "u1", body))
	store.setBlob("mediaobj8", body)
	created := false
	api.On("CreatePost", mock.Anything).Run(func(mock.Arguments) { created = true }).
		Return(&model.Post{Id: "p"}, nil).Maybe()

	payload, _ := json.Marshal(CreatePostPayload{
		ChannelID: "c1", Message: "hi",
		PhotonMedia: []MediaRef{{MediaID: "mediaobj8"}, {MediaID: "нет-такого"}},
	})
	resp := h.createPost("u1", payload)
	require.False(t, resp.OK)
	require.False(t, created, "пост не должен создаваться, если вложение отвергнуто")
}

// Дубликат media_id в одном сообщении отвергается.
func TestCreatePostRejectsDuplicateMedia(t *testing.T) {
	h, store, api := postHandlerWithMedia(t, "u1")
	body := mediaBytes(10)
	store.add(storedObject("mediaobj9", "c1", "u1", body))
	store.setBlob("mediaobj9", body)
	api.On("CreatePost", mock.Anything).Return(&model.Post{Id: "p"}, nil).Maybe()

	payload, _ := json.Marshal(CreatePostPayload{
		ChannelID: "c1", Message: "hi",
		PhotonMedia: []MediaRef{{MediaID: "mediaobj9"}, {MediaID: "mediaobj9"}},
	})
	require.False(t, h.createPost("u1", payload).OK)
}

// Пост без вложений props не получает вовсе.
func TestCreatePostWithoutMediaHasNoProps(t *testing.T) {
	h, _, api := postHandlerWithMedia(t, "u1")
	var captured *model.Post
	api.On("CreatePost", mock.Anything).Run(func(args mock.Arguments) {
		captured = args.Get(0).(*model.Post)
	}).Return(&model.Post{Id: "p"}, nil)

	payload, _ := json.Marshal(CreatePostPayload{ChannelID: "c1", Message: "hi"})
	require.True(t, h.createPost("u1", payload).OK)
	require.Empty(t, captured.GetProps())
	_ = bytes.MinRead
}
