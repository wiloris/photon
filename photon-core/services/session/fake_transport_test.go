package session

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon-core/ports"
)

// Фиктивный ports.Transport поверх net.Pipe.
//
// Ограничение записано осознанно (bridge-session.md §8): net.Pipe не
// воспроизводит ни таймингов сети, ни поведения QUIC при обрыве, а его Close
// закрывает обе стороны, тогда как ports.Stream.Close — half-close только
// пишущей. Именно этот класс расхождения (fakeSpawner и net.Pipe в Phase 9-10
// против настоящих процессов в Phase 11) скрыл три реальных бага. Поэтому
// тесты ниже проверяют ЛОГИКУ ПРОТОКОЛА, а прогон против живого
// photon-quic-bridge остаётся отдельным непроверенным пунктом (§9).

// streamHandler обслуживает серверную сторону одного стрима.
type streamHandler func(t *testing.T, idx int, srv io.ReadWriteCloser)

type fakeTransport struct {
	connectErr error
	// connectBlock, если не nil, удерживает Connect до закрытия канала или
	// отмены ctx — для проверки Close во время Connect.
	connectBlock chan struct{}

	conn *fakeConn

	// next, если задан, вызывается на каждый Connect вместо выдачи conn.
	// Нужен там, где важна СМЕНА соединения: одно и то же значение conn не
	// позволяет отличить «старое закрыли» от «новое не открыли».
	next func() (ports.Connection, error)

	mu       sync.Mutex
	connects int
}

func (f *fakeTransport) Connect(ctx context.Context, ep ports.Endpoint) (ports.Connection, error) {
	f.mu.Lock()
	f.connects++
	f.mu.Unlock()

	if f.connectBlock != nil {
		select {
		case <-f.connectBlock:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.connectErr != nil {
		return nil, f.connectErr
	}
	if f.next != nil {
		return f.next()
	}
	return f.conn, nil
}

func (f *fakeTransport) Close() error { return nil }

func (f *fakeTransport) connectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connects
}

type fakeConn struct {
	t       *testing.T
	handler streamHandler

	pushes chan []byte

	mu        sync.Mutex
	idx       int
	openErr   error
	acceptErr error
	broken    chan struct{}
	closed    bool
	closes    int
	toClose   []io.Closer
	openCalls int
}

// breakConnection воспроизводит обрыв уже установленного соединения: и
// OpenStream, и AcceptUniStream начинают отказывать, как при idle-timeout
// QUIC. Без этого обрыв нечем было воспроизвести — fakeConn умел только
// «не подключиться», а не «связь была и пропала».
func (c *fakeConn) breakConnection(err error) {
	c.mu.Lock()
	c.openErr = err
	c.acceptErr = err
	if c.broken != nil {
		close(c.broken)
		c.broken = nil
	}
	c.mu.Unlock()
}

var _ ports.Connection = (*fakeConn)(nil)

func newFakeConn(t *testing.T, h streamHandler) *fakeConn {
	c := &fakeConn{t: t, handler: h, pushes: make(chan []byte, 8),
		broken: make(chan struct{})}
	t.Cleanup(func() {
		c.mu.Lock()
		for _, cl := range c.toClose {
			cl.Close()
		}
		c.mu.Unlock()
	})
	return c
}

func (c *fakeConn) OpenStream(ctx context.Context) (ports.Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	if c.openErr != nil {
		err := c.openErr
		c.mu.Unlock()
		return nil, err
	}
	idx := c.idx
	c.idx++
	c.openCalls++
	cli, srv := net.Pipe()
	c.toClose = append(c.toClose, cli, srv)
	c.mu.Unlock()

	if c.handler != nil {
		go c.handler(c.t, idx, srv)
	}
	return pipeStream{cli}, nil
}

func (c *fakeConn) AcceptStream(ctx context.Context) (ports.Stream, error) {
	return nil, io.EOF
}

func (c *fakeConn) OpenUniStream(ctx context.Context) (ports.SendStream, error) {
	return nil, io.EOF
}

func (c *fakeConn) AcceptUniStream(ctx context.Context) (ports.RecvStream, error) {
	c.mu.Lock()
	if c.acceptErr != nil {
		err := c.acceptErr
		c.mu.Unlock()
		return nil, err
	}
	broken := c.broken
	c.mu.Unlock()

	select {
	case <-broken:
		c.mu.Lock()
		err := c.acceptErr
		c.mu.Unlock()
		return nil, err
	case p := <-c.pushes:
		cli, srv := net.Pipe()
		c.mu.Lock()
		c.toClose = append(c.toClose, cli, srv)
		c.mu.Unlock()
		go func() {
			srv.Write(p)
			srv.Close()
		}()
		return pipeStream{cli}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *fakeConn) RemoteEndpoint() ports.Endpoint { return "fake://bridge" }

func (c *fakeConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.closes++
	c.mu.Unlock()
	return nil
}

// closeCount — сколько раз соединение закрывали. Именно счётчик, а не
// признак: «закрыто» не отличает одно закрытие от двух, а двойное закрытие
// настоящего QUIC-соединения — отдельный дефект.
func (c *fakeConn) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

func (c *fakeConn) streamsOpened() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.openCalls
}

type pipeStream struct{ net.Conn }

func (p pipeStream) Close() error { return p.Conn.Close() }

// ── Помощники серверной стороны ─────────────────────────────────────────────

// readFrames читает n JSON-значений одним декодером в переданные приёмники.
func readFrames(t *testing.T, r io.Reader, into ...interface{}) error {
	t.Helper()
	dec := json.NewDecoder(r)
	for _, v := range into {
		if err := dec.Decode(v); err != nil {
			return err
		}
	}
	return nil
}

func writeJSON(t *testing.T, w io.Writer, v interface{}) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Logf("server write: %v", err)
	}
}

// okHandshake — сервер, принимающий handshake и отвечающий ok, затем
// отвечающий на action'ы значением resp.
func okHandshake(resp interface{}) streamHandler {
	return func(t *testing.T, idx int, srv io.ReadWriteCloser) {
		defer srv.Close()
		if idx == 0 {
			var auth, mode map[string]interface{}
			if err := readFrames(t, srv, &auth, &mode); err != nil {
				return
			}
			writeJSON(t, srv, map[string]interface{}{"ok": true})
			return
		}
		var hdr, payload map[string]interface{}
		if err := readFrames(t, srv, &hdr, &payload); err != nil {
			return
		}
		writeJSON(t, srv, resp)
	}
}

func newReadySession(t *testing.T, h streamHandler, mode Mode) (*Session, *fakeConn) {
	t.Helper()
	conn := newFakeConn(t, h)
	tr := &fakeTransport{conn: conn}
	s, err := New(Config{
		Transport: tr, Endpoint: "fake://bridge",
		SessionToken: "tok", Mode: mode,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, conn
}
