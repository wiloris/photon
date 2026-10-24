package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
)

// Фиктивный Bridge для тестов доставки событий.
//
// Своя копия, а не общая с services/dispatcher: тамошняя лежит в её пакете и
// импортироваться не может. Здесь нужна ровно одна возможность, которой у неё
// нет, — отдавать push на подписное соединение, поднятое НАСТОЯЩИМ насосом
// через настоящий Photon_SetEventCallback.
//
// Ограничение записывается честно: net.Pipe не воспроизводит ни таймингов
// сети, ни поведения QUIC при обрыве. Здесь проверяется ЛОГИКА границы ABI —
// реентерабельность, порядок, остановка, — а не сеть.

type fakeTransport struct {
	mu    sync.Mutex
	conns []*fakeSubConn

	// holdClose, если задан до Connect, удерживает первый Close соединения,
	// пока тест его не закроет; closeEntered получает сигнал, когда Close
	// вошёл в удержание.
	//
	// Нужен ради контролируемого перекрытия Destroy (ADR-059): насос закрывает
	// подписную сессию ДО выхода из d.inflight, поэтому удержанный Close
	// держит и обычный Destroy (Dispatcher.Close ждёт inflight), и отложенное
	// уничтожение (finishDeferredTeardown идёт после выхода насоса) — оба ДО
	// освобождения handle. Второй Destroy, запущенный в это время, заведомо
	// начат внутри окна времени жизни. Рабочий код для этого не меняется.
	holdClose    chan struct{}
	closeEntered chan struct{}
}

func (f *fakeTransport) Connect(ctx context.Context, ep ports.Endpoint) (ports.Connection, error) {
	c := &fakeSubConn{
		closeCh:      make(chan struct{}),
		push:         make(chan []byte, 32),
		pushBreak:    make(chan struct{}),
		holdClose:    f.holdClose,
		closeEntered: f.closeEntered,
	}
	f.mu.Lock()
	f.conns = append(f.conns, c)
	f.mu.Unlock()
	return c, nil
}

func (f *fakeTransport) Close() error { return nil }

// waitSub дожидается соединения, подтверждённого в режиме subscribe.
func (f *fakeTransport) waitSub(t *testing.T) *fakeSubConn {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		conns := append([]*fakeSubConn(nil), f.conns...)
		f.mu.Unlock()
		for _, c := range conns {
			c.mu.Lock()
			sub := c.subscribe
			c.mu.Unlock()
			if sub {
				return c
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("подписное соединение не поднялось")
	return nil
}

type fakeSubConn struct {
	closeCh   chan struct{}
	push      chan []byte
	pushBreak chan struct{}

	mu        sync.Mutex
	idx       int
	closed    bool
	subscribe bool
	pipes     []io.Closer
	breakOnce sync.Once

	holdClose    chan struct{}
	closeEntered chan struct{}
}

var _ ports.Connection = (*fakeSubConn)(nil)

func (c *fakeSubConn) OpenStream(ctx context.Context) (ports.Stream, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	idx := c.idx
	c.idx++
	cli, srv := net.Pipe()
	c.pipes = append(c.pipes, cli, srv)
	c.mu.Unlock()

	go c.serve(idx, srv)
	return fakePipeStream{cli}, nil
}

func (c *fakeSubConn) serve(idx int, srv net.Conn) {
	defer srv.Close()
	dec := json.NewDecoder(srv)

	if idx == 0 { // handshake: auth → mode → ack
		var auth, mode map[string]any
		if dec.Decode(&auth) != nil || dec.Decode(&mode) != nil {
			return
		}
		requested, _ := mode["mode"].(string)
		status := "ready"
		if requested == "subscribe" {
			status = "subscribed"
			c.mu.Lock()
			c.subscribe = true
			c.mu.Unlock()
		}
		_ = json.NewEncoder(srv).Encode(map[string]any{
			"ok": true, "data": map[string]any{"status": status},
		})
		return
	}

	var hdr map[string]any
	var payload json.RawMessage
	if dec.Decode(&hdr) != nil || dec.Decode(&payload) != nil {
		return
	}
	_ = json.NewEncoder(srv).Encode(map[string]any{"ok": true, "data": map[string]any{}})
}

func (c *fakeSubConn) AcceptStream(ctx context.Context) (ports.Stream, error) { return nil, io.EOF }
func (c *fakeSubConn) OpenUniStream(ctx context.Context) (ports.SendStream, error) {
	return nil, io.EOF
}

func (c *fakeSubConn) AcceptUniStream(ctx context.Context) (ports.RecvStream, error) {
	select {
	case b := <-c.push:
		return fakeRecv{bytes.NewReader(b)}, nil
	case <-c.pushBreak:
		return nil, io.ErrUnexpectedEOF
	case <-c.closeCh:
		return nil, io.ErrClosedPipe
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *fakeSubConn) RemoteEndpoint() ports.Endpoint { return "fake://bridge" }

func (c *fakeSubConn) Close() error {
	c.mu.Lock()
	first := !c.closed
	c.closed = true
	pipes := c.pipes
	c.pipes = nil
	c.mu.Unlock()
	if first {
		close(c.closeCh)
	}
	for _, p := range pipes {
		p.Close()
	}
	if first && c.holdClose != nil {
		if c.closeEntered != nil {
			select {
			case c.closeEntered <- struct{}{}:
			default:
			}
		}
		<-c.holdClose
	}
	return nil
}

func (c *fakeSubConn) sendPush(t *testing.T, frame string) {
	t.Helper()
	select {
	case c.push <- []byte(frame):
	case <-time.After(time.Second):
		t.Fatal("очередь push фейка переполнена")
	}
}

func (c *fakeSubConn) breakPush() { c.breakOnce.Do(func() { close(c.pushBreak) }) }

type fakeRecv struct{ r *bytes.Reader }

func (r fakeRecv) Read(p []byte) (int, error) { return r.r.Read(p) }

type fakePipeStream struct{ net.Conn }

func (p fakePipeStream) Close() error { return p.Conn.Close() }
