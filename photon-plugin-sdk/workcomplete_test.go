package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"
)

/*
 * ADR-074 on the SDK side: what the plugin's own process can prove.
 *
 * Two claims live here and nowhere else, because only here are they
 * deterministic:
 *
 *   T11 — an over-limit payload is refused BEFORE anything reaches the
 *         channel, and the session survives the refusal;
 *   T12 — WorkComplete(WorkFailure, …) returns nil when the report is
 *         delivered. The error return means "not delivered", never "the work
 *         failed" (ADR-074 §4.1).
 *
 * T12 cannot be proven end-to-end through the host: a plugin observes its own
 * call's outcome only by logging it, and a log line written after the last
 * awaited report races with STOP. That race is the very reason ADR-072 took
 * "count:" out of the barrier role, and re-introducing it here would repeat
 * the mistake.
 */

// connectedPair brings up a client against a runtime side we drive by hand.
func connectedPair(t *testing.T) (Plugin, net.Conn) {
	t.Helper()

	runtimeSide, pluginSide := net.Pipe()
	t.Cleanup(func() { runtimeSide.Close() })

	// Both halves of the handshake block on the pipe until matched, so the
	// runtime side has to run concurrently with Connect.
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

	go func() { _ = p.Run(context.Background()) }()
	return p, runtimeSide
}

// T11 — over-limit payload refused locally, nothing written, session intact.
//
// "Nothing written" is measured, not assumed: net.Pipe is unbuffered, so a
// frame the SDK tried to send would block until someone read it. The runtime
// side deliberately reads with a deadline and must time out.
func TestADR074_T11_OverLimitPayloadRefusedBeforeReachingTheChannel(t *testing.T) {
	p, runtimeSide := connectedPair(t)

	over := make([]byte, MaxWorkPayloadBytes+1)
	err := p.WorkComplete(context.Background(), WorkSuccess, over)
	if err == nil {
		t.Fatal("payload over the limit was accepted")
	}
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("error %v, want one wrapping ErrInvalidRequest", err)
	}

	if err := runtimeSide.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if m, err := decodeMessage(runtimeSide); err == nil {
		t.Fatalf("a frame reached the channel despite the local refusal: %s op=%q", m.Type, m.Op)
	}
	if err := runtimeSide.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear deadline: %v", err)
	}

	// The refusal is by form, not a protocol violation: the session must
	// still be usable. Proven by a report that does go through.
	done := make(chan error, 1)
	go func() { done <- p.WorkComplete(context.Background(), WorkSuccess, []byte("after")) }()

	req, err := decodeMessage(runtimeSide)
	if err != nil {
		t.Fatalf("session did not survive the refusal: %v", err)
	}
	if req.Op != "work.complete" {
		t.Fatalf("op %q, want work.complete", req.Op)
	}
	if err := encodeMessage(runtimeSide, &message{Type: msgResponse, RequestID: req.RequestID, Payload: []byte("{}")}); err != nil {
		t.Fatalf("respond: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("report after a refused one failed: %v", err)
	}
}

// T12 — a delivered failure report returns nil, and carries status+payload.
//
// The mutation this catches (M5) is the natural mistake: making WorkComplete
// return an error when the status is WorkFailure. That would make a plugin
// treat its own correctly-delivered report as a delivery failure and take its
// error path — while the host has already recorded the report.
func TestADR074_T12_DeliveredFailureReportReturnsNil(t *testing.T) {
	p, runtimeSide := connectedPair(t)

	payload := []byte{0x00, 0xFF, 0xFE, 0x01, 0x80, 0x81, 0xC0, 0xAF, 0x00, 'z'}

	done := make(chan error, 1)
	go func() { done <- p.WorkComplete(context.Background(), WorkFailure, payload) }()

	req, err := decodeMessage(runtimeSide)
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if req.Op != "work.complete" {
		t.Fatalf("op %q, want work.complete", req.Op)
	}

	// The envelope is what ADR-074 §4 specifies, and the payload survives
	// the JSON/base64 round trip byte for byte.
	var got struct {
		Status  string `json:"status"`
		Payload []byte `json:"payload"`
	}
	if err := json.Unmarshal(req.Payload, &got); err != nil {
		t.Fatalf("envelope is not the documented JSON: %v (%q)", err, req.Payload)
	}
	if got.Status != string(WorkFailure) {
		t.Fatalf("status %q, want %q", got.Status, WorkFailure)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatalf("payload changed on the wire:\n got  %v\n want %v", got.Payload, payload)
	}

	if err := encodeMessage(runtimeSide, &message{Type: msgResponse, RequestID: req.RequestID, Payload: []byte("{}")}); err != nil {
		t.Fatalf("respond: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("WorkComplete(WorkFailure) returned %v; a delivered report must return nil (ADR-074 §4.1)", err)
	}
}

// An unknown status is refused locally, for the same reason and in the same
// way as an over-limit payload: the envelope is the SDK's business, its
// contents are not.
func TestADR074_UnknownStatusRefusedLocally(t *testing.T) {
	p, runtimeSide := connectedPair(t)

	err := p.WorkComplete(context.Background(), WorkStatus("maybe"), nil)
	if err == nil {
		t.Fatal("unknown status was accepted")
	}
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("error %v, want one wrapping ErrInvalidRequest", err)
	}

	if err := runtimeSide.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if m, err := decodeMessage(runtimeSide); err == nil {
		t.Fatalf("a frame reached the channel: %s op=%q", m.Type, m.Op)
	}
}

// The limit is measured on RAW bytes, not on the encoded envelope.
//
// base64 inflates by 4/3, so a payload exactly at the limit produces a JSON
// body well above it. If the check ever moves to the encoded form, this test
// fails — which is the point: the contract is stated in raw bytes (§7).
func TestADR074_LimitIsMeasuredOnRawBytesNotOnTheEncodedEnvelope(t *testing.T) {
	p, runtimeSide := connectedPair(t)

	exact := make([]byte, MaxWorkPayloadBytes)
	for i := range exact {
		exact[i] = byte(i % 251)
	}

	done := make(chan error, 1)
	go func() { done <- p.WorkComplete(context.Background(), WorkSuccess, exact) }()

	req, err := decodeMessage(runtimeSide)
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if len(req.Payload) <= MaxWorkPayloadBytes {
		t.Fatalf("encoded envelope is %d bytes, expected it to exceed the raw limit of %d — "+
			"otherwise this test proves nothing", len(req.Payload), MaxWorkPayloadBytes)
	}
	if err := encodeMessage(runtimeSide, &message{Type: msgResponse, RequestID: req.RequestID, Payload: []byte("{}")}); err != nil {
		t.Fatalf("respond: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("payload of exactly the limit was refused: %v", err)
	}
}
