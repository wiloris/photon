package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-core/services/session"
)

// Отказ аутентификации обязан дойти до клиента КАДРОМ, а не ошибкой
// соединения.
//
// Дефект: после writeError(stream, "unauthorized") сервер немедленно звал
// conn.CloseWithError. CloseWithError отправляет CONNECTION_CLOSE сразу, не
// дожидаясь доставки уже записанных данных стрима, поэтому клиент получал
// ошибку соединения вместо {"ok":false}. Измерено настоящим клиентом
// photon-core против развёрнутого Bridge: 50 попыток из 50 давали
// ErrMalformedFrame (класс INTERNAL) вместо ErrHandshakeRejected (класс
// PERMISSION_DENIED). Для приложения это разница между «перелогиниться» и
// «повторить вызов».
//
// Тесты поднимают НАСТОЯЩИЙ quic-go и настоящий handleConnection: дефект
// живёт в порядке закрытия соединения, и мок транспорта его не воспроизвёл бы.
// Классификацию кадра на стороне клиента проверяет photon-core
// (services/session, services/dispatcher/classify_test.go) — здесь
// проверяется ровно то, чего там проверить нельзя: что кадр вообще доходит.

// startRejectingListener поднимает настоящий QUIC, у которого ЛЮБАЯ сессия
// недействительна.
func startRejectingListener(t *testing.T) string {
	t.Helper()

	api := quietAPI()
	api.On("GetSession", mock.Anything).Return(
		(*model.Session)(nil),
		model.NewAppError("GetSession", "session.not_found", nil, "", 401),
	).Maybe()
	cfg := &model.Config{}
	cfg.SetDefaults()
	api.On("GetConfig").Return(cfg).Maybe()

	p := handlersFor(api)
	ql := NewQuicListener(p, nil, NewDispatcher(p))

	ln, err := quic.ListenAddr("127.0.0.1:0", selfSignedTLS(t), &quic.Config{
		KeepAlivePeriod: 15 * time.Second,
	})
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
	return ln.Addr().String()
}

// dialRejected открывает соединение, шлёт auth и mode, возвращает соединение
// и стрим handshake'а, НЕ читая ответа.
func dialRejected(t *testing.T, addr string) (*quic.Conn, *quic.Stream) {
	t.Helper()
	conn, err := quic.DialAddr(context.Background(), addr,
		&tls.Config{InsecureSkipVerify: true, NextProtos: []string{"photon-quic-v1"}},
		&quic.Config{
			KeepAlivePeriod: 15 * time.Second,
		})
	require.NoError(t, err)

	hs, err := conn.OpenStreamSync(context.Background())
	require.NoError(t, err)
	enc := json.NewEncoder(hs)
	require.NoError(t, enc.Encode(AuthFrame{SessionToken: "revoked-or-unknown"}))
	require.NoError(t, enc.Encode(map[string]string{"mode": "action"}))
	return conn, hs
}

// Главная регрессия: кадр отказа читается клиентом.
//
// Мутация «вернуть conn.CloseWithError сразу после writeError» роняет именно
// этот тест: декодер получает ошибку соединения вместо JSON.
func TestUnauthorizedRejectionReachesClient(t *testing.T) {
	addr := startRejectingListener(t)
	conn, hs := dialRejected(t, addr)
	defer func() { _ = conn.CloseWithError(0, "") }()

	require.NoError(t, hs.SetReadDeadline(time.Now().Add(5*time.Second)))
	var ack Response
	require.NoError(t, json.NewDecoder(hs).Decode(&ack),
		"кадр отказа не дошёл: сервер закрыл соединение раньше доставки")
	require.False(t, ack.OK, "отказ обязан приходить как ok:false")
	require.Equal(t, "unauthorized", ack.Error)
}

// Соединение после отказа не удерживается: если клиент его не закрывает,
// закрывает сервер по своему пределу.
//
// Без предела неаутентифицированный пир держал бы соединение сколько угодно:
// сервер шлёт keep-alive каждые 15 с, и idle-timeout (30 с) не наступает.
func TestUnauthorizedConnectionClosedByServerTimeout(t *testing.T) {
	prev := unauthorizedLingerNanos.Load()
	unauthorizedLingerNanos.Store(int64(300 * time.Millisecond))
	t.Cleanup(func() { unauthorizedLingerNanos.Store(prev) })

	addr := startRejectingListener(t)
	before := runtime.NumGoroutine()
	conn, hs := dialRejected(t, addr)
	defer func() { _ = conn.CloseWithError(0, "") }()

	var ack Response
	require.NoError(t, json.NewDecoder(hs).Decode(&ack))
	require.False(t, ack.OK)

	// Клиент намеренно НЕ закрывает соединение.
	select {
	case <-conn.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("сервер не закрыл соединение отвергнутого клиента")
	}

	// И горутина обслуживания уходит вместе с соединением, а не остаётся
	// висеть после истечения предела.
	requireGoroutinesSettle(t, before)
}

// requireGoroutinesSettle ждёт, пока число горутин вернётся к исходному.
// Допуск в две горутины — на внутренние таймеры quic-go, снимаемые не
// мгновенно; накопление на десятках соединений это не скрывает.
func requireGoroutinesSettle(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.Gosched()
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("горутины не освободились: было %d, стало %d (предел ожидания %s)",
		before, runtime.NumGoroutine(), unauthorizedLinger())
}

// Закрытие клиентом заканчивает ожидание немедленно, а не по пределу.
func TestUnauthorizedServerStopsWaitingWhenClientCloses(t *testing.T) {
	prev := unauthorizedLingerNanos.Load()
	unauthorizedLingerNanos.Store(int64(10 * time.Second))
	t.Cleanup(func() { unauthorizedLingerNanos.Store(prev) })

	addr := startRejectingListener(t)
	before := runtime.NumGoroutine()

	const n = 20
	for i := 0; i < n; i++ {
		conn, hs := dialRejected(t, addr)
		var ack Response
		require.NoError(t, json.NewDecoder(hs).Decode(&ack))
		require.False(t, ack.OK)
		require.NoError(t, conn.CloseWithError(0, ""))
	}

	// Горутины обслуживания обязаны уйти по закрытию клиентом, не дожидаясь
	// предела: иначе 10 с × число отказов копились бы на сервере.
	requireGoroutinesSettle(t, before)
}

// ── Сквозная проверка: НАСТОЯЩИЙ клиент photon-core против настоящего
//    handleConnection ─────────────────────────────────────────────────────
//
// Тесты выше доказывают, что кадр отказа доходит; тесты photon-core
// (services/dispatcher/classify_test.go) — что ok:false классифицируется как
// PERMISSION_DENIED. Между ними оставался стык: доходит ли кадр ИМЕННО в том
// виде, в каком его разбирает ReadHandshakeAck. Здесь стык закрывается — без
// сети и без развёрнутого сервера.
//
// Транспорт для клиента приходится собирать здесь: adapters/quic из
// photon-core собран под quic-go v0.48, а Bridge живёт на v0.59, и обе
// версии в одной сборке несовместимы (CLAUDE.md, «Расхождение окружений»).
// Реализуется ровно тот минимум ports.Transport, который нужен handshake'у;
// это тестовая оснастка, а не второй адаптер. По той же причине здесь нет
// импорта services/dispatcher.

type v59Transport struct{}

func (v59Transport) Connect(ctx context.Context, ep ports.Endpoint) (ports.Connection, error) {
	c, err := quic.DialAddr(ctx, string(ep),
		&tls.Config{InsecureSkipVerify: true, NextProtos: []string{"photon-quic-v1"}},
		&quic.Config{KeepAlivePeriod: 15 * time.Second})
	if err != nil {
		return nil, err
	}
	return v59Conn{c}, nil
}

func (v59Transport) Close() error { return nil }

type v59Conn struct{ c *quic.Conn }

func (c v59Conn) OpenStream(ctx context.Context) (ports.Stream, error) {
	s, err := c.c.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return v59Stream{s}, nil
}

func (c v59Conn) AcceptStream(ctx context.Context) (ports.Stream, error) {
	s, err := c.c.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return v59Stream{s}, nil
}

func (c v59Conn) OpenUniStream(ctx context.Context) (ports.SendStream, error) {
	s, err := c.c.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (c v59Conn) AcceptUniStream(ctx context.Context) (ports.RecvStream, error) {
	return c.c.AcceptUniStream(ctx)
}

func (c v59Conn) RemoteEndpoint() ports.Endpoint {
	return ports.Endpoint(c.c.RemoteAddr().String())
}

func (c v59Conn) Close() error { return c.c.CloseWithError(0, "") }

type v59Stream struct{ s *quic.Stream }

func (s v59Stream) Read(p []byte) (int, error)  { return s.s.Read(p) }
func (s v59Stream) Write(p []byte) (int, error) { return s.s.Write(p) }
func (s v59Stream) Close() error                { return s.s.Close() }

// Отказ аутентификации доходит до клиента КАК ErrHandshakeRejected, а не как
// ошибка кадра. Мутация «сразу CloseWithError» даёт здесь ErrMalformedFrame —
// ровно то, что наблюдалось на живом Bridge 50 раз из 50.
func TestUnauthorizedIsHandshakeRejectedForRealClient(t *testing.T) {
	addr := startRejectingListener(t)

	s, err := session.New(session.Config{
		Transport:    v59Transport{},
		Endpoint:     ports.Endpoint(addr),
		SessionToken: "revoked-or-unknown",
		Mode:         session.ModeAction,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = s.Connect(ctx)

	require.Error(t, err)
	require.True(t, errors.Is(err, session.ErrHandshakeRejected),
		"ожидался ErrHandshakeRejected, получено: %v", err)
	require.False(t, errors.Is(err, session.ErrMalformedFrame),
		"отказ пришёл ошибкой кадра — сервер закрыл соединение раньше доставки: %v", err)
	// Классификацию ErrHandshakeRejected -> ClassPermissionDenied проверяет
	// photon-core (services/dispatcher/classify_test.go). Импортировать
	// dispatcher здесь нельзя: он тянет adapters/quic, собранный под quic-go
	// v0.48, а Bridge живёт на v0.59 — две версии в одной сборке
	// несовместимы (CLAUDE.md, «Расхождение окружений»).
	_ = s.Close()
}
