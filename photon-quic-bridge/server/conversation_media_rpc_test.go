package main

import (
	"encoding/json"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
)

// Проекция вложений разговора пересекает НАСТОЯЩУЮ границу plugin RPC (стенд —
// media_rpc_test.go). PG-тесты M1 публикуют пост мимо gob и оставались
// зелёными, пока живой прогон не упал на «gob: type not registered for
// interface: []main.convMediaProjection».
func TestM1ProjectionCrossesPluginRPC(t *testing.T) {
	atts := []ConvAttachment{
		{MediaID: "m1", Kind: "file", Mime: "application/octet-stream", Name: "a.bin", Size: 307200, SHA256: "ab"},
		{MediaID: "m2", Kind: "file", Mime: "application/octet-stream", Name: "b.bin", Size: 1, SHA256: "cd"},
	}
	proj, err := projectionOf(atts)
	require.NoError(t, err)

	post := &model.Post{UserId: "u1", ChannelId: "c1"}
	post.AddProp(propPhotonMedia, proj)
	got, err := throughPluginRPC(t, post)
	require.NoError(t, err, "props обязаны кодироваться gob'ом на границе plugin RPC")
	requireJSONCanonical(t, "props", map[string]any(got.GetProps()))

	// Читатель ленты получает props как JSON из БД — тот же путь, что здесь.
	raw, err := json.Marshal(got.GetProps()[propPhotonMedia])
	require.NoError(t, err)
	back := parseMediaProjection(string(raw))
	require.Equal(t, []string{"m1", "m2"}, []string{back[0].MediaID, back[1].MediaID}, "порядок сохраняется")
	require.Equal(t, int64(307200), back[0].Size)
	require.Equal(t, "a.bin", back[0].Name)
}
