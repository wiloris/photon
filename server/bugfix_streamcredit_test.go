package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

// Исчерпание кредита MAX_STREAMS.
//
// Дефект: handleStream разбирал два JSON-кадра и закрывал стрим, НИКОГДА не
// доводя до конца приёмную сторону. Для QUIC стрим при этом не завершается, и
// кредит MAX_STREAMS не возвращается: одно соединение переживало ровно
// DefaultMaxIncomingStreams (100) стримов за всю жизнь, после чего клиент
// вечно ждал открытия следующего.
//
// Найдено измерением на живом Bridge 0.4.1: 150 действий getMe подряд на одном
// соединении останавливались на действии №100 (handshake-стрим занимает первый
// слот), а ровно 99 проходили. Новое соединение работало снова — то есть предел
// оказался пожизненным для соединения, а не конкурентным.
//
// Тесты поднимают НАСТОЯЩИЙ quic-go и настоящий handleConnection: дефект живёт
// в жизненном цикле стрима, и мок транспорта его бы не воспроизвёл.

func selfSignedTLS(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	require.NoError(t, err)
	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"photon-quic-v1"}}
}

// startTestListener поднимает настоящий QUIC на случайном порту и обслуживает
// соединения тем же handleConnection, что и продакшн.
func startTestListener(t *testing.T) string {
	t.Helper()

	api := quietAPI()
	sess := &model.Session{Id: "sid", UserId: "u1", ExpiresAt: 0}
	api.On("GetSession", "sid").Return(sess, nil).Maybe()
	cfg := &model.Config{}
	cfg.SetDefaults()
	api.On("GetConfig").Return(cfg).Maybe()

	p := handlersFor(api)
	ql := NewQuicListener(p, nil, NewDispatcher(p))

	tlsCfg := selfSignedTLS(t)
	ln, err := quic.ListenAddr("127.0.0.1:0", tlsCfg, &quic.Config{
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

// dialAndHandshake — клиент: auth-кадр, кадр режима, подтверждение.
func dialAndHandshake(t *testing.T, addr string) *quic.Conn {
	t.Helper()
	conn, err := quic.DialAddr(context.Background(), addr,
		&tls.Config{InsecureSkipVerify: true, NextProtos: []string{"photon-quic-v1"}},
		&quic.Config{KeepAlivePeriod: 15 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseWithError(0, "") })

	hs, err := conn.OpenStreamSync(context.Background())
	require.NoError(t, err)
	enc := json.NewEncoder(hs)
	require.NoError(t, enc.Encode(AuthFrame{SessionToken: "sid"}))
	require.NoError(t, enc.Encode(map[string]string{"mode": "action"}))
	var ack Response
	require.NoError(t, json.NewDecoder(hs).Decode(&ack))
	require.True(t, ack.OK, "handshake отвергнут: %s", ack.Error)
	_ = hs.Close()
	return conn
}

// doAction — одно действие на своём стриме, как это делает клиент Photon.
//
// Код действия заведомо неизвестный: проверяется ЖИЗНЕННЫЙ ЦИКЛ стрима, а не
// работа обработчика. Ответ при этом пишется тем же writeResponse и стрим
// закрывается тем же путём, что у любого действия.
func doAction(ctx context.Context, conn *quic.Conn) error {
	st, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(st)
	if err := enc.Encode(HeaderFrame{Action: 99999, Resource: "probe"}); err != nil {
		return err
	}
	if err := enc.Encode(map[string]any{}); err != nil {
		return err
	}
	var resp Response
	if err := json.NewDecoder(st).Decode(&resp); err != nil {
		return err
	}
	_ = st.Close()
	return nil
}

// РЕГРЕССИЯ: 150 последовательных действий на ОДНОМ соединении.
//
// На дефектном коде тест останавливается на действии №100: стрим не
// открывается, потому что кредит исчерпан и никогда не возвращается.
func TestStreamCreditIsReturnedAfterEachAction(t *testing.T) {
	addr := startTestListener(t)
	conn := dialAndHandshake(t, addr)

	const total = 150
	for i := 1; i <= total; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := doAction(ctx, conn)
		cancel()
		require.NoErrorf(t, err, "действие №%d не прошло: кредит MAX_STREAMS не возвращается", i)
	}
}

// Исправление не имеет права превратить обработку в последовательную: действия
// на разных стримах обязаны идти конкурентно.
func TestConcurrentActionStreamsStillWork(t *testing.T) {
	addr := startTestListener(t)
	conn := dialAndHandshake(t, addr)

	const n = 60
	var wg sync.WaitGroup
	errs := make(chan error, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := doAction(ctx, conn); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err, "конкурентное действие не прошло")
	}
	t.Logf("%d конкурентных действий за %s", n, time.Since(start).Round(time.Millisecond))
}

// Конкурентность и исчерпание вместе: 200 действий волнами по 50.
func TestConcurrentWavesExceedStreamLimit(t *testing.T) {
	addr := startTestListener(t)
	conn := dialAndHandshake(t, addr)

	for wave := 1; wave <= 4; wave++ {
		var wg sync.WaitGroup
		errs := make(chan error, 50)
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := doAction(ctx, conn); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoErrorf(t, err, "волна %d: действие не прошло", wave)
		}
	}
}
