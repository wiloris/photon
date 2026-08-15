package tcp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon-core/ports"
)

// Тесты идут против настоящих TCP-сокетов (net.Pipe не подходит: нужен
// реальный сокет, чтобы проверить поведение при peer shutdown).

type testPair struct {
	client ports.Connection
	server ports.Connection
}

// newPair поднимает TCP-листенер, соединяется с ним и оборачивает обе
// стороны в мультиплексор.
func newPair(t *testing.T) *testPair {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	srvCh := make(chan ports.Connection, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		srvCh <- Accept(nc)
	}()

	tr := New(Config{})
	t.Cleanup(func() { tr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := tr.Connect(ctx, ports.Endpoint(ln.Addr().String()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	var server ports.Connection
	select {
	case server = <-srvCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for server side")
	}

	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return &testPair{client: client, server: server}
}

func ctx5s(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// ── Контракт порта ──────────────────────────────────────────────────

func TestImplementsPortsTransport(t *testing.T) {
	var _ ports.Transport = New(Config{})
}

// ── 3. Одно TCP-соединение → много logical streams ─────────────────

func TestMultipleLogicalStreamsOverOneSocket(t *testing.T) {
	p := newPair(t)
	ctx := ctx5s(t)

	// Сервер эхо-обрабатывает всё, что придёт.
	go func() {
		for {
			s, err := p.server.AcceptStream(context.Background())
			if err != nil {
				return
			}
			go func(s ports.Stream) {
				io.Copy(s, s)
				s.Close()
			}(s)
		}
	}()

	const n = 10
	var wg sync.WaitGroup
	errCh := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := p.client.OpenStream(ctx)
			if err != nil {
				errCh <- err
				return
			}
			// Уникальные данные на поток — если мультиплексор перепутает
			// потоки, эхо вернёт чужое.
			msg := markerBytes(i, 512)
			if _, err := s.Write(msg); err != nil {
				errCh <- err
				return
			}
			s.Close()

			got, err := io.ReadAll(s)
			if err != nil {
				errCh <- err
				return
			}
			if !equal(got, msg) {
				errCh <- errors.New("payload mismatch — streams got mixed up")
				return
			}
			errCh <- nil
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
	}
}

// bytes создаёт узнаваемый буфер: все байты равны marker.
func markerBytes(marker, size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte('A' + marker)
	}
	return b
}

func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ── 4. Многопоточность: payload не перемешивается ──────────────────

func TestConcurrentWritesDoNotInterleave(t *testing.T) {
	p := newPair(t)
	ctx := ctx5s(t)

	const streams = 8
	const chunk = 4096

	received := make(map[uint32][]byte)
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for i := 0; i < streams; i++ {
			s, err := p.server.AcceptStream(context.Background())
			if err != nil {
				return
			}
			wg.Add(1)
			go func(s ports.Stream, idx int) {
				defer wg.Done()
				data, _ := io.ReadAll(s)
				mu.Lock()
				received[uint32(idx)] = data
				mu.Unlock()
			}(s, i)
		}
		wg.Wait()
	}()

	var wg sync.WaitGroup
	sent := make([][]byte, streams)
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := p.client.OpenStream(ctx)
			if err != nil {
				t.Errorf("OpenStream: %v", err)
				return
			}
			msg := markerBytes(i, chunk)
			sent[i] = msg
			s.Write(msg)
			s.Close()
		}(i)
	}
	wg.Wait()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout receiving streams")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != streams {
		t.Fatalf("received %d streams, want %d", len(received), streams)
	}
	// Каждый принятый поток должен состоять из одного повторяющегося
	// байта — если кадры перемешались, будут разные байты в одном потоке.
	for idx, data := range received {
		if len(data) != chunk {
			t.Fatalf("stream %d: got %d bytes, want %d", idx, len(data), chunk)
		}
		first := data[0]
		for i, b := range data {
			if b != first {
				t.Fatalf("stream %d: byte %d = %q, but stream starts with %q — frames interleaved",
					idx, i, b, first)
			}
		}
	}
}

// ── 7. Half-close / EOF ─────────────────────────────────────────────

func TestHalfClose_ReaderGetsDataThenEOF(t *testing.T) {
	p := newPair(t)
	ctx := ctx5s(t)

	s, err := p.client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := s.Write([]byte("before fin")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	s.Close() // FIN

	srvStream, err := p.server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	got, err := io.ReadAll(srvStream)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "before fin" {
		t.Fatalf("got %q, want %q", got, "before fin")
	}
}

func TestHalfClose_ReceiveStaysOpenAfterLocalClose(t *testing.T) {
	// Ключевое свойство half-close по контракту порта:
	// SendStream.Close завершает отправку, НЕ закрывая приём.
	p := newPair(t)
	ctx := ctx5s(t)

	go func() {
		s, err := p.server.AcceptStream(context.Background())
		if err != nil {
			return
		}
		io.ReadAll(s) // дождаться FIN клиента
		s.Write([]byte("reply after your fin"))
		s.Close()
	}()

	s, err := p.client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	s.Write([]byte("question"))
	s.Close() // наш FIN — приём должен остаться открытым

	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("ReadAll after local Close: %v", err)
	}
	if string(got) != "reply after your fin" {
		t.Fatalf("got %q — receive side was closed by local Close, violating half-close contract", got)
	}
}

func TestStreamClose_Idempotent(t *testing.T) {
	p := newPair(t)
	ctx := ctx5s(t)

	s, err := p.client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
}

func TestWriteAfterCloseFails(t *testing.T) {
	p := newPair(t)
	ctx := ctx5s(t)

	s, err := p.client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	s.Close()
	if _, err := s.Write([]byte("nope")); err == nil {
		t.Fatal("Write after Close must fail")
	}
}

// ── Uni streams ─────────────────────────────────────────────────────

func TestUniStream_ClientToServer(t *testing.T) {
	p := newPair(t)
	ctx := ctx5s(t)

	got := make(chan string, 1)
	go func() {
		s, err := p.server.AcceptUniStream(context.Background())
		if err != nil {
			return
		}
		data, _ := io.ReadAll(s)
		got <- string(data)
	}()

	s, err := p.client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	s.Write([]byte(`{"event":"posted"}`))
	s.Close()

	select {
	case v := <-got:
		if v != `{"event":"posted"}` {
			t.Fatalf("got %q", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for uni stream")
	}
}

func TestUniStream_ServerPush(t *testing.T) {
	p := newPair(t)
	ctx := ctx5s(t)

	go func() {
		s, err := p.server.OpenUniStream(context.Background())
		if err != nil {
			return
		}
		s.Write([]byte("push payload"))
		s.Close()
	}()

	s, err := p.client.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("AcceptUniStream: %v", err)
	}
	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "push payload" {
		t.Fatalf("got %q", got)
	}
}

// ── 8. Peer shutdown разбудит ожидающие Read ───────────────────────

func TestPeerShutdownWakesBlockedRead(t *testing.T) {
	p := newPair(t)
	ctx := ctx5s(t)

	s, err := p.client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	s.Write([]byte("hi"))

	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		// Первое чтение может вернуть эхо; нам важно, что последующее
		// чтение не зависнет навсегда после закрытия соединения.
		for {
			_, err := s.Read(buf)
			if err != nil {
				readDone <- err
				return
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)
	p.server.Close() // peer shutdown

	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("expected an error after peer shutdown")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read stayed blocked after peer shutdown — connection close must wake all readers")
	}
}

func TestConnectionClose_WakesAcceptStream(t *testing.T) {
	p := newPair(t)

	done := make(chan error, 1)
	go func() {
		_, err := p.client.AcceptStream(context.Background())
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	p.client.Close()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("AcceptStream must fail after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AcceptStream stayed blocked after Close")
	}
}

func TestAcceptStream_RespectsContext(t *testing.T) {
	p := newPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := p.client.AcceptStream(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

// ── Lifecycle ───────────────────────────────────────────────────────

func TestConnectionClose_ThenOpenStreamFails(t *testing.T) {
	p := newPair(t)
	p.client.Close()

	if _, err := p.client.OpenStream(context.Background()); err == nil {
		t.Fatal("OpenStream after Close must fail")
	}
}

func TestConnectionClose_Idempotent(t *testing.T) {
	p := newPair(t)
	for i := 0; i < 3; i++ {
		if err := p.client.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
}

func TestRemoteEndpointNotEmpty(t *testing.T) {
	p := newPair(t)
	if p.client.RemoteEndpoint() == "" {
		t.Fatal("RemoteEndpoint is empty")
	}
}

// ── Transport lifecycle ─────────────────────────────────────────────

func TestTransportClose_Idempotent(t *testing.T) {
	tr := New(Config{})
	for i := 0; i < 3; i++ {
		if err := tr.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
}

func TestConnect_AfterTransportClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	tr := New(Config{})
	tr.Close()

	if _, err := tr.Connect(context.Background(), ports.Endpoint(ln.Addr().String())); err == nil {
		t.Fatal("Connect after Transport.Close must fail")
	}
}

func TestConnect_UnreachableEndpoint(t *testing.T) {
	tr := New(Config{})
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := tr.Connect(ctx, "127.0.0.1:1"); err == nil {
		t.Fatal("expected error connecting to an unreachable endpoint")
	}
}

func TestConnect_RespectsCancelledContext(t *testing.T) {
	tr := New(Config{})
	defer tr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := tr.Connect(ctx, "127.0.0.1:1"); err == nil {
		t.Fatal("expected error with a cancelled context")
	}
}

// ── Крупные данные (разбиение на кадры) ────────────────────────────

func TestLargeWriteSplitsIntoFrames(t *testing.T) {
	// Запись больше maxFrameSize должна пройти через несколько кадров
	// и собраться на той стороне без потерь.
	p := newPair(t)
	ctx := ctx5s(t)

	const size = maxFrameSize*3 + 1234

	got := make(chan []byte, 1)
	go func() {
		s, err := p.server.AcceptStream(context.Background())
		if err != nil {
			return
		}
		data, _ := io.ReadAll(s)
		got <- data
	}()

	s, err := p.client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	msg := make([]byte, size)
	for i := range msg {
		msg[i] = byte(i % 251)
	}
	n, err := s.Write(msg)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != size {
		t.Fatalf("Write returned %d, want %d", n, size)
	}
	s.Close()

	select {
	case data := <-got:
		if len(data) != size {
			t.Fatalf("received %d bytes, want %d", len(data), size)
		}
		for i := range data {
			if data[i] != byte(i%251) {
				t.Fatalf("byte %d corrupted: got %d, want %d", i, data[i], byte(i%251))
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout receiving large payload")
	}
}
