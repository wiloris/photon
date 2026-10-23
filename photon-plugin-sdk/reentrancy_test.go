package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"
)

// TestReentrantCall_HandlerBlockingCallReturnsErrInsteadOfHanging proves
// two things, not just one: that a blocking SDK call made from inside an
// OnEvent handler (using the ctx it was given, the natural thing to do)
// returns ErrReentrantCall promptly instead of hanging, AND that the
// session is still fully usable afterward — the handler returned, Run's
// read loop kept going, and a normal call made from outside any handler
// succeeds. Catching the deadlock is worthless if doing so quietly
// breaks the session instead.
func TestReentrantCall_HandlerBlockingCallReturnsErrInsteadOfHanging(t *testing.T) {
	runtimeSide, pluginSide := net.Pipe()
	defer runtimeSide.Close()

	// Both halves of the handshake block on the pipe until matched by a
	// read/write on the other end, so the runtime side's whole exchange
	// has to run concurrently with Connect, not after it returns.
	handshakeErr := make(chan error, 1)
	go func() {
		if err := encodeMessage(runtimeSide, &message{Type: msgHello, ProtocolVersion: 1}); err != nil {
			handshakeErr <- err
			return
		}
		ack, err := decodeMessage(runtimeSide)
		if err != nil {
			handshakeErr <- err
			return
		}
		if ack.Type != msgHelloAck {
			handshakeErr <- errors.New("expected HELLO_ACK, got " + ack.Type.String())
			return
		}
		handshakeErr <- nil
	}()

	p, err := Connect(context.Background(), Config{
		Conn:            pluginSide,
		PluginID:        "com.engram.test",
		ProtocolVersion: 1,
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := <-handshakeErr; err != nil {
		t.Fatalf("runtime-side handshake: %v", err)
	}

	callErr := make(chan error, 1)
	handlerReturned := make(chan struct{})
	p.OnEvent(func(ctx context.Context, ev Event) error {
		defer close(handlerReturned)
		// Passing the given ctx straight through — the natural,
		// idiomatic thing to do, and exactly what ErrReentrantCall's
		// detection depends on (see its doc comment).
		_, err := p.Storage().Get(ctx, "ns", "key")
		callErr <- err
		return nil
	})

	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(context.Background()) }()

	if err := encodeMessage(runtimeSide, &message{Type: msgEvent, EventType: "test"}); err != nil {
		t.Fatalf("send EVENT: %v", err)
	}

	select {
	case err := <-callErr:
		if !errors.Is(err, ErrReentrantCall) {
			t.Fatalf("handler's SDK call returned %v, want ErrReentrantCall", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler's SDK call hung instead of returning ErrReentrantCall — " +
			"exactly the deadlock this check exists to prevent")
	}

	select {
	case <-handlerReturned:
	case <-time.After(time.Second):
		t.Fatal("handler did not return after its call failed with ErrReentrantCall")
	}

	// The real assertion: Run's read loop must still be alive and
	// answering normal calls made OUTSIDE any handler. A version of
	// this check that merely avoided the hang by wedging the session
	// shut would pass everything above and fail here.
	reqSeen := make(chan struct{})
	go func() {
		defer close(reqSeen)
		req, err := decodeMessage(runtimeSide)
		if err != nil || req.Type != msgRequest {
			return
		}
		payload, _ := json.Marshal(map[string]any{"value": []byte("ok")})
		_ = encodeMessage(runtimeSide, &message{Type: msgResponse, RequestID: req.RequestID, Payload: payload})
	}()

	val, err := p.Storage().Get(context.Background(), "ns", "key2")
	if err != nil {
		t.Fatalf("call after ErrReentrantCall failed — session broke instead of continuing: %v", err)
	}
	if string(val) != "ok" {
		t.Fatalf("got %q, want %q", val, "ok")
	}

	select {
	case <-reqSeen:
	case <-time.After(time.Second):
		t.Fatal("runtime side never saw the follow-up request")
	}

	_ = p.Close()
}
