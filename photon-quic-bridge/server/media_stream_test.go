package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Этап 5: провод между кадрированием и уже готовым MediaService.
//
// Тесты идут через НАСТОЯЩИЙ quic-go и настоящий handleConnection: проверяется
// граница кадра и жизненный цикл стрима, а мок транспорта воспроизвёл бы ни
// то ни другое.

func mediaBytes(n int) []byte {
	// Начинается с приманки, похожей на JSON-кадр: неверная граница не должна
	// выглядеть работающей.
	b := []byte(`{"looks":"like a frame"}` + "\n")
	for len(b) < n {
		b = append(b, byte(len(b)%251))
	}
	return b[:n]
}

func hexSum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// mediaListener поднимает настоящий QUIC и обслуживает соединения продакшн
// путём. Возвращает адрес и хранилище, чтобы тест мог заглянуть в результат.
func mediaListener(t *testing.T, uploaderID string) (string, *fakeMediaStore, *MediaService) {
	addr, store, svc, _ := mediaListenerWithLog(t, uploaderID)
	return addr, store, svc
}

// dialSmallWindow — клиент с маленьким окном приёма.
//
// Нужен, чтобы сервер упёрся в флоу-контроль на объекте разумного размера:
// иначе зависшее скачивание пришлось бы воспроизводить десятками мегабайт.
func dialSmallWindow(t *testing.T, addr string) *quic.Conn {
	t.Helper()
	conn, err := quic.DialAddr(context.Background(), addr,
		&tls.Config{InsecureSkipVerify: true, NextProtos: []string{"photon-quic-v1"}},
		&quic.Config{
			KeepAlivePeriod:                15 * time.Second,
			InitialStreamReceiveWindow:     16 << 10,
			MaxStreamReceiveWindow:         32 << 10,
			InitialConnectionReceiveWindow: 24 << 10,
			MaxConnectionReceiveWindow:     48 << 10,
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseWithError(0, "") })

	hs, err := conn.OpenStreamSync(context.Background())
	require.NoError(t, err)
	enc := json.NewEncoder(hs)
	require.NoError(t, enc.Encode(AuthFrame{SessionToken: "sid"}))
	require.NoError(t, enc.Encode(map[string]string{"mode": "action"}))
	var ack Response
	require.NoError(t, json.NewDecoder(hs).Decode(&ack))
	require.True(t, ack.OK)
	_ = hs.Close()
	return conn
}

// mediaListenerWithLog — то же, но с наблюдением за LogError сервера.
//
// Нужно там, где следствие ненаблюдаемо по проводу: ответ уже отправлен, и
// об ошибке отдачи сервер может сообщить только в журнал.
func mediaListenerWithLog(t *testing.T, uploaderID string) (string, *fakeMediaStore, *MediaService, chan string) {
	t.Helper()

	logged := make(chan string, 32)
	api := quietAPIWithErrorSink(logged)
	sess := &model.Session{Id: "sid", UserId: uploaderID}
	api.On("GetSession", "sid").Return(sess, nil).Maybe()
	cfg := &model.Config{}
	cfg.SetDefaults()
	api.On("GetConfig").Return(cfg).Maybe()
	ch := &model.Channel{Id: "c1", Type: model.ChannelTypeOpen, TeamId: "t1"}
	api.On("GetChannel", "c1").Return(ch, nil).Maybe()
	// Любой другой канал не существует: резолв обязан отказать, а не упасть.
	api.On("GetChannel", mock.Anything).Return(nil,
		model.NewAppError("GetChannel", "not_found", nil, "", 404)).Maybe()
	api.On("HasPermissionToChannel", uploaderID, "c1", model.PermissionUploadFile).Return(true).Maybe()
	api.On("HasPermissionToChannel", uploaderID, "c1", model.PermissionReadChannelContent).Return(true).Maybe()

	// Короткий дедлайн: тест зависшей передачи не должен ждать полминуты.
	prev := mediaStreamDeadline
	mediaStreamDeadline = 300 * time.Millisecond
	t.Cleanup(func() { mediaStreamDeadline = prev })

	p := handlersFor(api)
	store := newFakeMediaStore()
	svc := NewMediaService(store)
	d := NewDispatcher(p)
	d.authz = newAuthz(api, store)
	d.mediaService = svc
	d.media = &MediaHandler{plugin: p, media: d.mediaService}
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
	return ln.Addr().String(), store, svc, logged
}

// doMediaStream — клиентская половина: два JSON-кадра, затем тело/чтение.
func doMediaStream(t *testing.T, conn *quic.Conn, action int, payload any,
	body []byte, sink io.Writer) (map[string]any, error) {
	t.Helper()
	st, err := conn.OpenStreamSync(context.Background())
	require.NoError(t, err)
	enc := json.NewEncoder(st)
	require.NoError(t, enc.Encode(HeaderFrame{Action: action, Resource: "MediaResource"}))
	require.NoError(t, enc.Encode(payload))
	if body != nil {
		_, err = st.Write(body)
		require.NoError(t, err)
	}

	// Предел чтения ответа обязателен: без дедлайна на сервере клиент ждал бы
	// вечно, и мутация «дедлайна нет» дала бы зависание вместо падения.
	// Зависший тест дефект не ловит.
	_ = st.SetReadDeadline(time.Now().Add(3 * time.Second))

	dec := json.NewDecoder(st)
	var resp map[string]any
	if err := dec.Decode(&resp); err != nil {
		return nil, err
	}
	if sink != nil {
		if ok, _ := resp["ok"].(bool); ok {
			n, _ := resp["body_length"].(float64)
			src := bodyAfterFrames(dec, st)
			if _, err := io.CopyN(sink, src, int64(n)); err != nil {
				return resp, err
			}
		}
	}
	_ = st.Close()
	return resp, nil
}

// ── сквозной путь ────────────────────────────────────────────────────────────

func TestMediaStreamUploadThenDownloadRoundTrip(t *testing.T) {
	addr, store, _ := mediaListener(t, "u1")
	conn := dialAndHandshake(t, addr)

	for _, size := range []int{40, 5000, 300_000} {
		body := mediaBytes(size)
		resp, err := doMediaStream(t, conn, ActionMediaUpload, MediaUploadPayload{
			ChannelID: "c1", Name: "a.bin", Mime: "application/octet-stream",
			SHA256: hexSum(body), BodyLength: int64(size),
		}, body, nil)
		require.NoError(t, err)
		require.Truef(t, resp["ok"].(bool), "размер %d: %v", size, resp["error"])

		data := resp["data"].(map[string]any)
		mediaID := data["media_id"].(string)
		require.Equal(t, hexSum(body), data["sha256"], "размер %d", size)

		// Байты на сервере — те самые.
		require.True(t, blobEquals(store, mediaID, body), "размер %d: на сервере не те байты", size)

		var got bytes.Buffer
		dl, err := doMediaStream(t, conn, ActionMediaDownload, MediaIDPayload{MediaID: mediaID}, nil, &got)
		require.NoError(t, err)
		require.Truef(t, dl["ok"].(bool), "скачивание %d: %v", size, dl["error"])
		require.Truef(t, bytes.Equal(got.Bytes(), body),
			"размер %d: скачано %d байт, ожидалось %d", size, got.Len(), size)
	}
}

// Объявлено больше, чем прислано: отказ, а не частичный успех.
func TestMediaStreamRejectsShortBody(t *testing.T) {
	addr, store, _ := mediaListener(t, "u1")
	conn := dialAndHandshake(t, addr)
	body := mediaBytes(500)

	resp, err := doMediaStream(t, conn, ActionMediaUpload, MediaUploadPayload{
		ChannelID: "c1", Name: "a.bin", SHA256: hexSum(body), BodyLength: 5000,
	}, body, nil)
	// Сервер ждёт 5000 байт и упирается в дедлайн; либо ответ с отказом, либо
	// обрыв — оба исхода означают «не принято».
	if err == nil {
		require.False(t, resp["ok"].(bool))
	}
	require.Zero(t, store.objectCount(), "недобор не должен создавать объект")
}

// Хеш считается по РЕАЛЬНО принятым байтам.
func TestMediaStreamRejectsWrongHash(t *testing.T) {
	addr, store, _ := mediaListener(t, "u1")
	conn := dialAndHandshake(t, addr)
	body := mediaBytes(1000)

	resp, err := doMediaStream(t, conn, ActionMediaUpload, MediaUploadPayload{
		ChannelID: "c1", Name: "a.bin", SHA256: hexSum(mediaBytes(999)), BodyLength: 1000,
	}, body, nil)
	require.NoError(t, err)
	require.False(t, resp["ok"].(bool))
	require.Equal(t, "integrity", resp["error"])
	require.Zero(t, store.objectCount())
}

// Прислано больше объявленного: принимается ровно объявленное.
func TestMediaStreamReadsExactlyDeclaredLength(t *testing.T) {
	addr, store, _ := mediaListener(t, "u1")
	conn := dialAndHandshake(t, addr)
	want := mediaBytes(1000)

	resp, err := doMediaStream(t, conn, ActionMediaUpload, MediaUploadPayload{
		ChannelID: "c1", Name: "a.bin", SHA256: hexSum(want), BodyLength: 1000,
	}, append(append([]byte(nil), want...), []byte("ЛИШНЕЕ")...), nil)
	require.NoError(t, err)
	require.True(t, resp["ok"].(bool), resp["error"])
	id := resp["data"].(map[string]any)["media_id"].(string)
	require.True(t, blobEquals(store, id, want), "в тело попало лишнее")
}

// Медийный путь ходит в ТУ ЖЕ авторизацию: без права на канал загрузка
// отвергается тем же forbidden.
func TestMediaStreamUsesSharedAuthorize(t *testing.T) {
	addr, store, _ := mediaListener(t, "u1")
	conn := dialAndHandshake(t, addr)
	body := mediaBytes(100)

	resp, err := doMediaStream(t, conn, ActionMediaUpload, MediaUploadPayload{
		ChannelID: "чужой-канал", Name: "a.bin", SHA256: hexSum(body), BodyLength: 100,
	}, body, nil)
	require.NoError(t, err)
	require.False(t, resp["ok"].(bool))
	require.Equal(t, "forbidden", resp["error"])
	require.Zero(t, store.objectCount(), "неавторизованная загрузка не должна создавать объект")
}

// Скачивание несуществующего объекта — forbidden, а не «нет такого»:
// существование чужих media_id не должно быть пробуемым.
func TestMediaStreamDownloadUnknownIsForbidden(t *testing.T) {
	addr, _, _ := mediaListener(t, "u1")
	conn := dialAndHandshake(t, addr)
	var got bytes.Buffer
	resp, err := doMediaStream(t, conn, ActionMediaDownload,
		MediaIDPayload{MediaID: "nosuchobject000000000000"}, nil, &got)
	require.NoError(t, err)
	require.False(t, resp["ok"].(bool))
	require.Equal(t, "forbidden", resp["error"])
	require.Zero(t, got.Len())
}

// Дедлайн: клиент объявил тело и не шлёт его.
//
// Проверяется НАБЛЮДАЕМОЕ следствие, а не сам факт вызова SetReadDeadline:
// сервер обязан освободиться сам и ОТВЕТИТЬ. Клиентский предел чтения втрое
// больше серверного дедлайна, поэтому отсутствие дедлайна даёт не зависание, а
// падение по таймауту чтения — зависший тест дефект не ловит.
func TestMediaStreamDeadlineReleasesStalledUpload(t *testing.T) {
	addr, store, _ := mediaListener(t, "u1")
	conn := dialAndHandshake(t, addr)

	st, err := conn.OpenStreamSync(context.Background())
	require.NoError(t, err)
	enc := json.NewEncoder(st)
	require.NoError(t, enc.Encode(HeaderFrame{Action: ActionMediaUpload, Resource: "MediaResource"}))
	require.NoError(t, enc.Encode(MediaUploadPayload{
		ChannelID: "c1", Name: "a.bin", SHA256: hexSum(mediaBytes(10)), BodyLength: 10 << 20,
	}))
	_, _ = st.Write(mediaBytes(100)) // немного и молчим

	// Сервер обязан упереться в свой дедлайн (300 мс в тестах) и ответить.
	require.NoError(t, st.SetReadDeadline(time.Now().Add(3*time.Second)))
	var resp map[string]any
	err = json.NewDecoder(st).Decode(&resp)
	require.NoErrorf(t, err,
		"ответа не пришло за 3 с: сервер не освободился сам — дедлайн медийного стрима не работает")
	require.False(t, resp["ok"].(bool), "зависшая загрузка не может быть успешной")

	// Соединение остаётся пригодным для обычных действий.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, doAction(ctx, conn), "соединение перестало принимать обычные действия")

	require.Zero(t, store.objectCount(), "зависшая загрузка не должна оставлять объект")
	_ = st.Close()
}

// СТРУКТУРНАЯ гарантия: тело не читается целиком в память.
//
// Проверка нужна именно структурная. Прочитать тело в []byte и отдать его
// сервису функционально КОРРЕКТНО — все остальные тесты останутся зелёными, а
// сервер начнёт падать на большом файле у пользователя. Поведением такое не
// ловится, поэтому ловится текстом: на медийном пути не должно быть ни одного
// чтения «до конца в память».
func TestMediaPathNeverReadsWholeBodyIntoMemory(t *testing.T) {
	files := []string{"media_stream.go", "mediaservice.go"}
	banned := []string{"io.ReadAll", "ioutil.ReadAll", "ioutil.ReadFile", "os.ReadFile"}

	for _, name := range files {
		raw, err := os.ReadFile(name)
		require.NoErrorf(t, err, "не прочитан %s", name)
		for _, b := range banned {
			require.NotContainsf(t, string(raw), b,
				"%s содержит %s — тело медиа обязано идти потоком, а не собираться в память "+
					"(media-transfer.md §10)", name, b)
		}
	}
}

// Зависшее СКАЧИВАНИЕ: клиент прочитал заголовок и перестал читать тело.
//
// Симметрично зависшей загрузке, но наблюдается иначе: ответ уже отправлен, и
// сообщить об ошибке по проводу нечем. Наблюдаемое следствие — сервер обязан
// упереться в дедлайн записи и ЗАПИСАТЬ ошибку, а не висеть до закрытия
// соединения.
//
// Счётчик горутин для этого не годится: клиент и сервер живут в одном
// процессе, и открытые клиентом стримы держат свои горутины независимо от
// сервера. Проверялось — измерение оказалось нечувствительным.
//
// Тест добавлен по итогам мутации: снятие дедлайна с отдачи проходило зелёным,
// потому что все прежние проверки касались только приёма.
func TestMediaStreamDeadlineReleasesStalledDownload(t *testing.T) {
	addr, store, _, logged := mediaListenerWithLog(t, "u1")
	conn := dialSmallWindow(t, addr)

	// Объект заведомо больше окна флоу-контроля КЛИЕНТА (см. dialSmallWindow):
	// иначе сервер запишет всё в буфер и не заблокируется вовсе. С окнами по
	// умолчанию (до 6 МБ на стрим, 15 МБ на соединение) для этого понадобился
	// бы объект в десятки мегабайт — проверялось.
	body := mediaBytes(2 << 20)
	store.add(&MediaObject{
		MediaID: "bigobject00000000000000a", ChannelID: "c1", UploaderID: "u1",
		SHA256: hexSum(body), Size: int64(len(body)), State: MediaAvailable,
	})
	store.setBlob("bigobject00000000000000a", body)

	st, err := conn.OpenStreamSync(context.Background())
	require.NoError(t, err)
	enc := json.NewEncoder(st)
	require.NoError(t, enc.Encode(HeaderFrame{Action: ActionMediaDownload, Resource: "MediaResource"}))
	require.NoError(t, enc.Encode(MediaIDPayload{MediaID: "bigobject00000000000000a"}))

	require.NoError(t, st.SetReadDeadline(time.Now().Add(3*time.Second)))
	var resp map[string]any
	require.NoError(t, json.NewDecoder(st).Decode(&resp))
	require.True(t, resp["ok"].(bool), resp["error"])
	// Дальше НЕ читаем: сервер упрётся в флоу-контроль.

	deadline := time.After(5 * time.Second)
	for {
		select {
		case msg := <-logged:
			if strings.Contains(msg, "Download failed after header") {
				_ = st.Close()
				return
			}
		case <-deadline:
			_ = st.Close()
			t.Fatal("сервер не сообщил об ошибке отдачи за 5 с: дедлайн записи не сработал")
		}
	}
}
