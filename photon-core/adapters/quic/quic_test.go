package quic

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/wiloris/photon/photon-core/ports"
)

// Тесты работают против НАСТОЯЩЕГО QUIC-сервера, поднятого в том же
// процессе, а не против моков: адаптер — тонкая обёртка над quic-go, и
// мок проверял бы только то, что мы правильно вызвали свои же заглушки.

// testServer — минимальный QUIC-сервер для тестов.
type testServer struct {
	ln       *quicgo.Listener
	addr     string
	tlsConf  *tls.Config
	certPool *x509.CertPool
}

func newTestServer(t *testing.T, handle func(quicgo.Connection)) *testServer {
	t.Helper()

	cert, pool := selfSignedCert(t)
	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{defaultALPN},
	}

	ln, err := quicgo.ListenAddr("127.0.0.1:0", tlsConf, nil)
	if err != nil {
		t.Fatalf("ListenAddr: %v", err)
	}

	srv := &testServer{ln: ln, addr: ln.Addr().String(), tlsConf: tlsConf, certPool: pool}

	go func() {
		for {
			conn, err := ln.Accept(context.Background())
			if err != nil {
				return // листенер закрыт
			}
			if handle != nil {
				go handle(conn)
			}
		}
	}()

	t.Cleanup(func() { _ = ln.Close() })
	return srv
}

// clientTLS — TLS-конфиг клиента, доверяющий сертификату тестового
// сервера. Не InsecureSkipVerify: проверяем, что адаптер работает с
// реальной верификацией цепочки, а не только с отключённой.
func (s *testServer) clientTLS() *tls.Config {
	return &tls.Config{
		RootCAs:    s.certPool,
		ServerName: "localhost",
		NextProtos: []string{defaultALPN},
	}
}

func selfSignedCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}

	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}

// ── Compile-time контракт ────────────────────────────────────────────

func TestImplementsPortsTransport(t *testing.T) {
	// Дублирует var _ ports.Transport в quic.go, но как явный тест —
	// чтобы при поломке было видно в выводе тестов, а не только при
	// сборке.
	var _ ports.Transport = New(Config{})
}

// ── Connect ──────────────────────────────────────────────────────────

func TestConnect_Success(t *testing.T) {
	srv := newTestServer(t, nil)
	tr := New(Config{TLSConfig: srv.clientTLS()})
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := tr.Connect(ctx, ports.Endpoint(srv.addr))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	if conn.RemoteEndpoint() == "" {
		t.Fatal("RemoteEndpoint is empty")
	}
}

func TestConnect_UnreachableEndpoint(t *testing.T) {
	tr := New(Config{
		TLSConfig:        &tls.Config{InsecureSkipVerify: true, NextProtos: []string{defaultALPN}},
		HandshakeTimeout: 500 * time.Millisecond,
	})
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 127.0.0.1:1 — порт, на котором заведомо никто не слушает.
	if _, err := tr.Connect(ctx, "127.0.0.1:1"); err == nil {
		t.Fatal("expected error connecting to an unreachable endpoint")
	}
}

func TestConnect_MalformedEndpoint(t *testing.T) {
	tr := New(Config{TLSConfig: &tls.Config{InsecureSkipVerify: true, NextProtos: []string{defaultALPN}}})
	defer tr.Close()

	if _, err := tr.Connect(context.Background(), "not-an-address"); err == nil {
		t.Fatal("expected error for a malformed endpoint")
	}
}

func TestConnect_RespectsCancelledContext(t *testing.T) {
	tr := New(Config{TLSConfig: &tls.Config{InsecureSkipVerify: true, NextProtos: []string{defaultALPN}}})
	defer tr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := tr.Connect(ctx, "127.0.0.1:1"); err == nil {
		t.Fatal("expected error with a cancelled context")
	}
}

func TestConnect_AfterTransportClose(t *testing.T) {
	srv := newTestServer(t, nil)
	tr := New(Config{TLSConfig: srv.clientTLS()})

	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := tr.Connect(context.Background(), ports.Endpoint(srv.addr)); err == nil {
		t.Fatal("Connect after Transport.Close must fail")
	}
}

func TestTransportClose_Idempotent(t *testing.T) {
	tr := New(Config{})
	for i := 0; i < 3; i++ {
		if err := tr.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
}

func TestTransportClose_DoesNotCloseExistingConnections(t *testing.T) {
	// Контракт ports.Transport.Close: освобождает общие ресурсы, но НЕ
	// закрывает уже установленные Connection.
	echo := func(c quicgo.Connection) {
		s, err := c.AcceptStream(context.Background())
		if err != nil {
			return
		}
		io.Copy(s, s)
		s.Close()
	}
	srv := newTestServer(t, echo)
	tr := New(Config{TLSConfig: srv.clientTLS()})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := tr.Connect(ctx, ports.Endpoint(srv.addr))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	if err := tr.Close(); err != nil {
		t.Fatalf("Transport.Close: %v", err)
	}

	// Соединение должно продолжать работать после закрытия транспорта.
	st, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream after Transport.Close: %v", err)
	}
	if _, err := st.Write([]byte("still alive")); err != nil {
		t.Fatalf("Write after Transport.Close: %v", err)
	}
	st.Close()

	buf := make([]byte, len("still alive"))
	if _, err := io.ReadFull(st, buf); err != nil {
		t.Fatalf("Read after Transport.Close: %v", err)
	}
	if string(buf) != "still alive" {
		t.Fatalf("got %q, want %q", buf, "still alive")
	}
}

// ── Bidirectional streams ────────────────────────────────────────────

func TestOpenStream_EchoRoundTrip(t *testing.T) {
	echo := func(c quicgo.Connection) {
		s, err := c.AcceptStream(context.Background())
		if err != nil {
			return
		}
		io.Copy(s, s)
		s.Close()
	}
	srv := newTestServer(t, echo)
	tr := New(Config{TLSConfig: srv.clientTLS()})
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := tr.Connect(ctx, ports.Endpoint(srv.addr))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	st, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	msg := []byte("photon protocol payload")
	if _, err := st.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Half-close: сигнализируем серверу конец данных, приём остаётся
	// открытым — ровно то, что обещает ports.SendStream.Close.
	if err := st.Close(); err != nil {
		t.Fatalf("Close (half-close): %v", err)
	}

	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("ReadAll after half-close: %v", err)
	}
	if string(got) != string(msg) {
		t.Fatalf("echo: got %q, want %q", got, msg)
	}
}

func TestAcceptStream_ServerInitiated(t *testing.T) {
	// Сервер сам открывает поток к клиенту — проверяем, что
	// Connection.AcceptStream его принимает (это приём ПОТОКА, не
	// соединения; граница dial-only не нарушена — см. ADR-012).
	serverPush := func(c quicgo.Connection) {
		s, err := c.OpenStreamSync(context.Background())
		if err != nil {
			return
		}
		s.Write([]byte("server hello"))
		s.Close()
	}
	srv := newTestServer(t, serverPush)
	tr := New(Config{TLSConfig: srv.clientTLS()})
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := tr.Connect(ctx, ports.Endpoint(srv.addr))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	st, err := conn.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "server hello" {
		t.Fatalf("got %q, want %q", got, "server hello")
	}
}

// ── Unidirectional streams ───────────────────────────────────────────

func TestOpenUniStream_ClientToServer(t *testing.T) {
	received := make(chan string, 1)
	handler := func(c quicgo.Connection) {
		s, err := c.AcceptUniStream(context.Background())
		if err != nil {
			return
		}
		data, _ := io.ReadAll(s)
		received <- string(data)
	}
	srv := newTestServer(t, handler)
	tr := New(Config{TLSConfig: srv.clientTLS()})
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := tr.Connect(ctx, ports.Endpoint(srv.addr))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	st, err := conn.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	if _, err := st.Write([]byte("uni payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	st.Close()

	select {
	case got := <-received:
		if got != "uni payload" {
			t.Fatalf("got %q, want %q", got, "uni payload")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for uni stream data on server")
	}
}

func TestAcceptUniStream_ServerPush(t *testing.T) {
	// Основной сценарий использования по контракту порта: push-события
	// сервера (Subscribe в photon-quic-bridge).
	push := func(c quicgo.Connection) {
		s, err := c.OpenUniStreamSync(context.Background())
		if err != nil {
			return
		}
		s.Write([]byte(`{"event":"posted"}`))
		s.Close()
	}
	srv := newTestServer(t, push)
	tr := New(Config{TLSConfig: srv.clientTLS()})
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := tr.Connect(ctx, ports.Endpoint(srv.addr))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	st, err := conn.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("AcceptUniStream: %v", err)
	}
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != `{"event":"posted"}` {
		t.Fatalf("got %q", got)
	}
}

// ── Multiplexing ─────────────────────────────────────────────────────

func TestMultipleStreamsOnOneConnection(t *testing.T) {
	// Ключевое свойство QUIC, ради которого он и выбран: несколько
	// независимых потоков в одном соединении.
	echo := func(c quicgo.Connection) {
		for {
			s, err := c.AcceptStream(context.Background())
			if err != nil {
				return
			}
			go func(s quicgo.Stream) {
				io.Copy(s, s)
				s.Close()
			}(s)
		}
	}
	srv := newTestServer(t, echo)
	tr := New(Config{TLSConfig: srv.clientTLS()})
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := tr.Connect(ctx, ports.Endpoint(srv.addr))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	const n = 10
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			st, err := conn.OpenStream(ctx)
			if err != nil {
				errCh <- err
				return
			}
			msg := []byte{byte('A' + i)}
			if _, err := st.Write(msg); err != nil {
				errCh <- err
				return
			}
			st.Close()
			got, err := io.ReadAll(st)
			if err != nil {
				errCh <- err
				return
			}
			if len(got) != 1 || got[0] != msg[0] {
				errCh <- errors.New("echo mismatch on stream")
				return
			}
			errCh <- nil
		}(i)
	}

	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
	}
}

// ── Connection lifecycle ─────────────────────────────────────────────

func TestConnectionClose_ThenOpenStreamFails(t *testing.T) {
	srv := newTestServer(t, nil)
	tr := New(Config{TLSConfig: srv.clientTLS()})
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := tr.Connect(ctx, ports.Endpoint(srv.addr))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Connection.Close: %v", err)
	}

	if _, err := conn.OpenStream(ctx); err == nil {
		t.Fatal("OpenStream on a closed connection must fail")
	}
}

func TestConnectionClose_Idempotent(t *testing.T) {
	srv := newTestServer(t, nil)
	tr := New(Config{TLSConfig: srv.clientTLS()})
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := tr.Connect(ctx, ports.Endpoint(srv.addr))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	for i := 0; i < 3; i++ {
		// Повторный Close не должен паниковать (может вернуть ошибку —
		// это допустимо, важно отсутствие паники).
		_ = conn.Close()
	}
}

// ── TLS / ALPN ───────────────────────────────────────────────────────

func TestDefaultALPNIsSet(t *testing.T) {
	tr := New(Config{})
	if len(tr.tlsConf.NextProtos) == 0 || tr.tlsConf.NextProtos[0] != defaultALPN {
		t.Fatalf("NextProtos = %v, want [%s]", tr.tlsConf.NextProtos, defaultALPN)
	}
}

func TestUserTLSConfigIsNotMutated(t *testing.T) {
	// Адаптер не должен молча дописывать ALPN в переданный вызывающим
	// конфиг — тот может использоваться и в другом месте.
	userConf := &tls.Config{ServerName: "example.com"}
	_ = New(Config{TLSConfig: userConf})

	if len(userConf.NextProtos) != 0 {
		t.Fatalf("adapter mutated the caller's tls.Config: NextProtos = %v", userConf.NextProtos)
	}
}

func TestUserALPNIsPreserved(t *testing.T) {
	userConf := &tls.Config{NextProtos: []string{"custom-alpn"}}
	tr := New(Config{TLSConfig: userConf})
	if tr.tlsConf.NextProtos[0] != "custom-alpn" {
		t.Fatalf("NextProtos = %v, want [custom-alpn]", tr.tlsConf.NextProtos)
	}
}

func TestConnect_ALPNMismatchFails(t *testing.T) {
	srv := newTestServer(t, nil)

	badALPN := srv.clientTLS()
	badALPN.NextProtos = []string{"wrong-alpn"}
	tr := New(Config{TLSConfig: badALPN, HandshakeTimeout: 2 * time.Second})
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := tr.Connect(ctx, ports.Endpoint(srv.addr)); err == nil {
		t.Fatal("expected handshake failure on ALPN mismatch")
	}
}

func TestConnect_UntrustedCertificateFails(t *testing.T) {
	srv := newTestServer(t, nil)

	// Клиент без RootCAs сервера — сертификат самоподписанный, доверия нет.
	tr := New(Config{
		TLSConfig:        &tls.Config{ServerName: "localhost", NextProtos: []string{defaultALPN}},
		HandshakeTimeout: 2 * time.Second,
	})
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := tr.Connect(ctx, ports.Endpoint(srv.addr)); err == nil {
		t.Fatal("expected failure with an untrusted certificate — adapter must not skip verification by default")
	}
}
