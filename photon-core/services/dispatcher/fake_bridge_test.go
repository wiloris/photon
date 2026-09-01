package dispatcher

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

// Фиктивный Bridge поверх net.Pipe.
//
// Своя копия, а не общая с services/session: тамошняя лежит в её пакете и
// импортироваться не может, а «вынести в общий тестовый пакет» здесь — плохой
// размен. Этому фейку нужно другое: считать соединения (синглфлайт), рвать
// связь между вызовами, держать action без ответа (таймаут и Destroy над
// висящим вызовом).
//
// Ограничение то же и записано так же честно (bridge-session.md §8): net.Pipe
// не воспроизводит ни таймингов сети, ни поведения QUIC при обрыве. Именно
// этот класс расхождения скрыл три бага Phase 9-10, найденных Phase 11 на
// настоящих процессах. Поэтому здесь проверяется ЛОГИКА, а прогон против
// живого Bridge остаётся отдельным пунктом (action-routing.md §12).

type fakeBridge struct {
	mu sync.Mutex

	connects int
	conns    []*fakeConn
	closed   bool

	// connectGate, если не nil, удерживает Connect до закрытия канала или
	// отмены ctx. Нужен, чтобы параллельные вызовы гарантированно застали
	// подключение идущим, а не завершившимся.
	connectGate chan struct{}
	connectErr  error

	// handshakeOK/handshakeErr — ответ на handshake.
	handshakeOK  bool
	handshakeErr string

	// action отвечает на action-стрим. nil означает «не отвечать вовсе»:
	// стрим остаётся открытым, вызывающий обязан уйти по своему дедлайну.
	action func(hdr map[string]any, payload json.RawMessage) any
}

func newFakeBridge() *fakeBridge {
	return &fakeBridge{
		handshakeOK: true,
		action: func(hdr map[string]any, payload json.RawMessage) any {
			return map[string]any{"ok": true, "data": map[string]any{"echo": hdr["action"]}}
		},
	}
}

func (f *fakeBridge) Connect(ctx context.Context, ep ports.Endpoint) (ports.Connection, error) {
	f.mu.Lock()
	f.connects++
	gate := f.connectGate
	err := f.connectErr
	f.mu.Unlock()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}

	c := &fakeConn{br: f, closeCh: make(chan struct{})}
	f.mu.Lock()
	f.conns = append(f.conns, c)
	f.mu.Unlock()
	return c, nil
}

func (f *fakeBridge) Close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	return nil
}

func (f *fakeBridge) connectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connects
}

func (f *fakeBridge) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// breakLast воспроизводит обрыв уже установленного соединения: OpenStream
// начинает отказывать, как при idle-timeout QUIC. Без этого обрыв нечем
// воспроизвести — фейк умел бы только «не подключиться», а это другое
// свойство (bridge-session.md §6 п. 7).
func (f *fakeBridge) breakLast(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.conns) == 0 {
		return
	}
	c := f.conns[len(f.conns)-1]
	c.mu.Lock()
	c.openErr = err
	c.mu.Unlock()
}

type fakeConn struct {
	br *fakeBridge

	closeCh chan struct{}

	mu      sync.Mutex
	idx     int
	openErr error
	closed  bool
	pipes   []io.Closer
}

var _ ports.Connection = (*fakeConn)(nil)

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
	cli, srv := net.Pipe()
	c.pipes = append(c.pipes, cli, srv)
	c.mu.Unlock()

	go c.serve(idx, srv)
	return pipeStream{cli}, nil
}

func (c *fakeConn) serve(idx int, srv net.Conn) {
	defer srv.Close()
	dec := json.NewDecoder(srv)

	if idx == 0 { // handshake: auth → mode → ack
		var auth, mode map[string]any
		if dec.Decode(&auth) != nil || dec.Decode(&mode) != nil {
			return
		}
		ack := map[string]any{"ok": c.br.handshakeOK}
		if !c.br.handshakeOK {
			ack["error"] = c.br.handshakeErr
		} else {
			ack["data"] = map[string]any{"status": "ready"}
		}
		json.NewEncoder(srv).Encode(ack)
		return
	}

	var hdr map[string]any
	var payload json.RawMessage
	if dec.Decode(&hdr) != nil || dec.Decode(&payload) != nil {
		return
	}
	if c.br.action == nil {
		// Ответа нет намеренно: вызывающий обязан уйти по своему дедлайну, а
		// не ждать вечно. Стрим держится открытым до закрытия соединения.
		<-c.closeCh
		return
	}
	json.NewEncoder(srv).Encode(c.br.action(hdr, payload))
}

func (c *fakeConn) AcceptStream(ctx context.Context) (ports.Stream, error) { return nil, io.EOF }
func (c *fakeConn) OpenUniStream(ctx context.Context) (ports.SendStream, error) {
	return nil, io.EOF
}
func (c *fakeConn) AcceptUniStream(ctx context.Context) (ports.RecvStream, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (c *fakeConn) RemoteEndpoint() ports.Endpoint { return "fake://bridge" }

func (c *fakeConn) Close() error {
	c.mu.Lock()
	first := !c.closed
	c.closed = true
	pipes := c.pipes
	c.pipes = nil
	c.mu.Unlock()
	if first {
		// Ровно одно закрытие: второй close(chan) паникует, а Close сессии
		// идемпотентен и вызывается повторно.
		close(c.closeCh)
	}
	for _, p := range pipes {
		p.Close()
	}
	return nil
}

type pipeStream struct{ net.Conn }

func (p pipeStream) Close() error { return p.Conn.Close() }

// timeUnit — базовая единица дедлайнов в тестах. Достаточно велика, чтобы
// net.Pipe успел отработать на загруженной машине, и достаточно мала, чтобы
// прогон оставался быстрым.
const timeUnit = 200 * time.Millisecond

// newDispatcher — диспетчер поверх фиктивного Bridge, с короткими дедлайнами:
// тест, который ждёт секунды, перестают запускать.
func newDispatcher(t *testing.T, br *fakeBridge, tune func(*Config)) *Dispatcher {
	t.Helper()
	cfg := Config{
		Endpoint:       "fake://bridge",
		SessionToken:   "tok",
		ConnectTimeout: 2 * timeUnit,
		CallTimeout:    3 * timeUnit,
		Transport:      br,
	}
	if tune != nil {
		tune(&cfg)
	}
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}
