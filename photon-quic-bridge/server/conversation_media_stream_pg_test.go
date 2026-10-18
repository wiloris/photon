package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

// 703/704 на НАСТОЯЩЕМ QUIC-стриме поверх НАСТОЯЩЕГО PostgreSQL (ADR-124 M1,
// contract §8.3–§8.4). Слушатель — тот же handleConnection, что в
// производстве; сервис разговоров — из mediaRig; сессия стрима — пользователь
// Mattermost обвязки.

// convMediaListener — слушатель, чья сессия "sid" принадлежит mmUser.
func convMediaListener(t *testing.T, r *mediaRig, mmUser string) (string, chan string) {
	t.Helper()
	logged := make(chan string, 64)
	api := quietAPIWithErrorSink(logged)
	api.On("GetSession", "sid").Return(&model.Session{Id: "sid", UserId: mmUser}, nil).Maybe()
	cfg := &model.Config{}
	cfg.SetDefaults()
	api.On("GetConfig").Return(cfg).Maybe()

	prev := mediaStreamDeadline
	mediaStreamDeadline = 300 * time.Millisecond
	t.Cleanup(func() { mediaStreamDeadline = prev })

	p := handlersFor(api)
	d := NewDispatcher(p)
	d.authz = newAuthz(api, r.store)
	d.mediaService = r.media
	d.conversation = &ConversationHandler{plugin: p, service: r.svc}
	d.media = &MediaHandler{plugin: p, media: r.media, conv: r.svc}
	ql := NewQuicListener(p, nil, d)

	ln, err := quic.ListenAddr("127.0.0.1:0", selfSignedTLS(t), &quic.Config{KeepAlivePeriod: 15 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept(context.Background())
			if err != nil {
				return
			}
			go ql.handleConnection(conn)
		}
	}()
	return ln.Addr().String(), logged
}

func TestM1StreamUploadThenDownload(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	addrA, _ := convMediaListener(t, r, a)
	addrB, _ := convMediaListener(t, r, b)
	connA := dialAndHandshake(t, addrA)
	connB := dialAndHandshake(t, addrB)

	body := mediaBytes(300_000)
	resp, err := doMediaStream(t, connA, ActionConvMediaUpload, ConvMediaUploadPayload{
		ConversationID: id, UploadKey: "op:0", Kind: "file", Name: "big.bin",
		SHA256: hexSum(body), BodyLength: int64(len(body)),
	}, body, nil)
	require.NoError(t, err)
	require.True(t, resp["ok"].(bool), resp["error"])
	mid := resp["data"].(map[string]any)["media_id"].(string)
	require.True(t, blobEquals(r.store, mid, body))

	msgOf(t, r.sendMedia(t, a, id, newRequestID(), "файл", mid))

	var got bytes.Buffer
	dl, err := doMediaStream(t, connB, ActionConvMediaDownload, convMediaIDPayload{MediaID: mid}, nil, &got)
	require.NoError(t, err)
	require.True(t, dl["ok"].(bool), dl["error"])
	data := dl["data"].(map[string]any)
	require.Equal(t, convMediaMime, data["mime"])
	require.Equal(t, hexSum(body), data["sha256"])
	require.Equal(t, float64(len(body)), dl["body_length"])
	require.True(t, bytes.Equal(body, got.Bytes()), "скачано не то")
}

// Посторонний получает отказ ДО кадра данных; тело не отдаётся.
func TestM1StreamOutsiderForbidden(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB)
	mid := r.mustUpload(t, a, id, "k", "s.bin", []byte("secret"))
	msgOf(t, r.sendMedia(t, a, id, newRequestID(), "x", mid))
	addrC, _ := convMediaListener(t, r, c)
	var got bytes.Buffer
	dl, err := doMediaStream(t, dialAndHandshake(t, addrC), ActionConvMediaDownload, convMediaIDPayload{MediaID: mid}, nil, &got)
	require.NoError(t, err)
	require.False(t, dl["ok"].(bool))
	require.Equal(t, convErrForbidden, dl["error"])
	require.Zero(t, got.Len())
}

// Загрузка с телом короче объявленного: отказ, объекта нет (contract §8.10 п. 10).
func TestM1StreamShortUploadRejected(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	addrA, _ := convMediaListener(t, r, a)
	conn := dialAndHandshake(t, addrA)
	body := mediaBytes(1000)

	st, err := conn.OpenStreamSync(context.Background())
	require.NoError(t, err)
	enc := json.NewEncoder(st)
	require.NoError(t, enc.Encode(HeaderFrame{Action: ActionConvMediaUpload, Resource: "MediaResource"}))
	require.NoError(t, enc.Encode(ConvMediaUploadPayload{ConversationID: id, UploadKey: "short", Kind: "file",
		Name: "s.bin", SHA256: hexSum(body), BodyLength: int64(len(body))}))
	_, err = st.Write(body[:400])
	require.NoError(t, err)
	require.NoError(t, st.Close()) // half-close записи до объявленной длины
	_ = st.SetReadDeadline(time.Now().Add(3 * time.Second))
	var resp map[string]any
	require.NoError(t, json.NewDecoder(st).Decode(&resp))
	require.False(t, resp["ok"].(bool))
	require.Equal(t, convErrInvalidRequest, resp["error"])
	require.Equal(t, 0, r.store.objectCount())
}

// Отказ скачивания ПОСЛЕ кадра ответа завершается сбросом стрима (Н-1 (а)):
// клиент получает ошибку стрима с кодом 0x704, а не ждёт до закрытия
// соединения.
func TestM1StreamDownloadFailureResetsStream(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	body := mediaBytes(2 << 20)
	mid := r.mustUpload(t, a, id, "k", "big.bin", body)
	msgOf(t, r.sendMedia(t, a, id, newRequestID(), "x", mid))

	addrB, logged := convMediaListener(t, r, b)
	conn := dialSmallWindow(t, addrB) // окно меньше тела: сервер упрётся во флоу-контроль
	st, err := conn.OpenStreamSync(context.Background())
	require.NoError(t, err)
	enc := json.NewEncoder(st)
	require.NoError(t, enc.Encode(HeaderFrame{Action: ActionConvMediaDownload, Resource: "MediaResource"}))
	require.NoError(t, enc.Encode(convMediaIDPayload{MediaID: mid}))
	require.NoError(t, st.SetReadDeadline(time.Now().Add(3*time.Second)))
	dec := json.NewDecoder(st)
	var resp map[string]any
	require.NoError(t, dec.Decode(&resp))
	require.True(t, resp["ok"].(bool), resp["error"])
	// Тело не читаем: сервер упирается в окно и через скользящий дедлайн
	// (300 мс в тесте) отказывает.
	select {
	case msg := <-logged:
		require.True(t, strings.Contains(msg, "704 failed after header"), msg)
	case <-time.After(5 * time.Second):
		t.Fatal("сервер не сообщил об отказе отдачи за 5 с")
	}
	// Теперь дочитываем: должен прийти сброс стрима, а не EOF и не ожидание.
	require.NoError(t, st.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, err = io.Copy(io.Discard, bodyAfterFrames(dec, st))
	var se *quic.StreamError
	require.True(t, errors.As(err, &se), "ожидался сброс стрима, получено %v", err)
	require.Equal(t, convMediaResetCode, se.ErrorCode)
}

// По HTTP-входу потоковые 703/704 не исполняются; 705 исполняется.
func TestM1StreamActionsRefusedWithoutStream(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	mid := r.mustUpload(t, a, id, "k", "h.bin", []byte("http"))
	h := &MediaHandler{media: r.media, conv: r.svc}
	for _, action := range []int{ActionConvMediaUpload, ActionConvMediaDownload} {
		resp := h.Handle(a, action, pl(t, map[string]string{"media_id": mid}))
		require.False(t, resp.OK)
		require.Contains(t, resp.Error, "requires a media stream")
	}
	resp := h.Handle(a, ActionConvMediaInfo, pl(t, map[string]string{"media_id": mid}))
	require.True(t, resp.OK, "705: %+v", resp)
}
