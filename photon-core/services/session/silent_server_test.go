package session

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
)

// ── T-11b: сервер принял соединение и молчит на уровне протокола ───────────
//
// Найдено сквозным тестом пути подключения (ADR-130): QUIC-соединение
// установлено, ack handshake'а не приходит. Чтение ack ctx не наблюдает, и
// до исправления Connect — а с ним Dispatcher.Close и Photon_Destroy —
// висели бесконечно. T-11 этого не ловил: его транспорт висит в Connect, а
// не в чтении после него.
//
// Подделка здесь своя: закрытие соединения закрывает его потоки (контракт
// ports.Connection.Close) — общая fakeConn этого не делает.

type silentConn struct {
	mu     sync.Mutex
	pipes  []net.Conn
	closes int
}

func (c *silentConn) OpenStream(context.Context) (ports.Stream, error) {
	cli, srv := net.Pipe()
	go io.Copy(io.Discard, srv) // читает запрос, не отвечает никогда
	c.mu.Lock()
	c.pipes = append(c.pipes, cli, srv)
	c.mu.Unlock()
	return cli, nil
}
func (c *silentConn) AcceptStream(context.Context) (ports.Stream, error)        { return nil, io.EOF }
func (c *silentConn) OpenUniStream(context.Context) (ports.SendStream, error)   { return nil, io.EOF }
func (c *silentConn) AcceptUniStream(context.Context) (ports.RecvStream, error) { return nil, io.EOF }
func (c *silentConn) RemoteEndpoint() ports.Endpoint                            { return "silent" }
func (c *silentConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes++
	for _, p := range c.pipes {
		p.Close()
	}
	return nil
}
func (c *silentConn) closeCount() int { c.mu.Lock(); defer c.mu.Unlock(); return c.closes }

type silentTransport struct{ conn *silentConn }

func (t *silentTransport) Connect(context.Context, ports.Endpoint) (ports.Connection, error) {
	return t.conn, nil
}
func (t *silentTransport) Close() error { return nil }

func TestT11b_SilentServerConnectTimeout(t *testing.T) {
	conn := &silentConn{}
	s, _ := New(Config{Transport: &silentTransport{conn}, Endpoint: "e", SessionToken: "t", Mode: ModeAction})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	errCh := make(chan error, 1)
	go func() { errCh <- s.Connect(ctx) }()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Connect = %v, want DeadlineExceeded", err)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("Connect вернулся через %v", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect висит на молчащем сервере после истечения ctx")
	}
	if s.State() != StateFailed || conn.closeCount() == 0 {
		t.Fatalf("state %v, закрытий соединения %d", s.State(), conn.closeCount())
	}
}

func TestT11b_CloseDuringSilentHandshake(t *testing.T) {
	conn := &silentConn{}
	s, _ := New(Config{Transport: &silentTransport{conn}, Endpoint: "e", SessionToken: "t", Mode: ModeAction})
	errCh := make(chan error, 1)
	go func() { errCh <- s.Connect(context.Background()) }()
	time.Sleep(50 * time.Millisecond) // Connect дошёл до чтения ack
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close висит, пока handshake ждёт молчащий сервер")
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("Connect = %v, want ErrSessionClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect не вернулся после Close")
	}
	if s.State() != StateClosed || conn.closeCount() == 0 {
		t.Fatalf("state %v, закрытий соединения %d", s.State(), conn.closeCount())
	}
}
