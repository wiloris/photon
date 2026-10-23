// Command permission is a Plugin Test Laboratory fixture
// (docs/test-laboratory.md, "permission"). It does not repeat the
// Authorizer unit tests, which already prove it answers well-formed
// questions correctly. It asks questions the way an attacker would —
// malformed, spoofed, or simply denied — and reports what actually came
// back, as a real process, so the security boundary is proven against
// the real IPC wire, not a mock of it.
//
// Two attacks (unsupported-op, plugin-id-spoof) bypass the SDK
// entirely and speak the wire protocol directly: the SDK's typed API
// structurally cannot send an unknown op string or inject an extra
// identity-shaped field into a request, so going through it would prove
// nothing about whether the runtime itself rejects these — an untrusted
// plugin is not obligated to use the SDK at all.
//
// Mode is selected by PHOTON_TEST_MODE, same convention as hello/crash.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/wiloris/photon-plugin-runtime/ipc"
	sdk "github.com/wiloris/photon-plugin-sdk"
)

const defaultPluginID = "com.engram.test.permission"

// pluginID returns the identity this process claims during handshake.
// Overridable via PHOTON_TEST_PLUGIN_ID: the namespace-escape scenario
// needs the SAME binary to run as two distinct plugin identities
// (victim and attacker) in the same test, and the manifest's
// ExpectedPluginID must match whatever this process actually sends.
func pluginID() string {
	if id := os.Getenv("PHOTON_TEST_PLUGIN_ID"); id != "" {
		return id
	}
	return defaultPluginID
}

func main() {
	switch os.Getenv("PHOTON_TEST_MODE") {
	case "attempt-write":
		attemptWrite()
	case "namespace-victim":
		namespaceVictim()
	case "namespace-attacker":
		namespaceAttacker()
	case "revoke-live":
		revokeLive()
	case "flood":
		flood()
	case "unsupported-op":
		unsupportedOp()
	case "plugin-id-spoof":
		pluginIDSpoof()
	default:
		os.Exit(2)
	}
}

// ── SDK-based attacks ────────────────────────────────────────────────

func attemptWrite() {
	p, runDone := connect()
	ctx := context.Background()
	err := p.Storage().Put(ctx, "x", "y", []byte("z"))
	logResult(p, ctx, "put", err)
	<-runDone
}

func namespaceVictim() {
	p, runDone := connect()
	ctx := context.Background()
	err := p.Storage().Put(ctx, "secret", "value", []byte("victim-only-data"))
	logResult(p, ctx, "victim_write", err)
	<-runDone
}

// namespaceAttacker tries to reach the victim's data by crafting the
// namespace argument — a value the SDK lets it set freely, since
// restricting it would be a check the SDK cannot make trustworthy (see
// sdk-boundary.md: "не проверка в SDK — преобразование на стороне
// runtime"). What's under test is whether the runtime's own prefixing
// stops these regardless of what string is sent.
func namespaceAttacker() {
	p, runDone := connect()
	ctx := context.Background()

	attempts := []string{
		"secret", // own prefix, sanity check — must NOT be the victim's value
		"../permission-victim/secret",
		"plugin.com.engram.test.permission-victim.secret",
		"com.engram.test.permission-victim.secret",
	}
	for i, ns := range attempts {
		val, err := p.Storage().Get(ctx, ns, "value")
		result := classify(err)
		if err == nil {
			result = "OK:" + string(val)
		}
		logMessage(p, ctx, fmt.Sprintf("attacker_get_%d:%s", i, result))
	}
	<-runDone
}

func revokeLive() {
	p, runDone := connect()
	ctx := context.Background()

	// OnEvent's handler runs synchronously inside Run()'s own read
	// loop (one event at a time, by design — see api.go). Calling
	// Storage().Get() from directly within it would deadlock: the
	// request write could happen, but nothing could ever deliver its
	// response, because that delivery is also Run()'s job, and Run()
	// would be stuck waiting for this handler to return. The retry
	// itself runs in its own goroutine, only signaled by the event.
	retry := make(chan struct{}, 1)
	p.OnEvent(func(_ context.Context, ev sdk.Event) error {
		if ev.Type == "retry" {
			select {
			case retry <- struct{}{}:
			default:
			}
		}
		return nil
	})

	_, err := p.Storage().Get(ctx, "x", "y")
	logResult(p, ctx, "first_get", err)

	go func() {
		<-retry
		_, err := p.Storage().Get(ctx, "x", "y")
		logResult(p, ctx, "second_get", err)
	}()

	<-runDone
}

func flood() {
	p, runDone := connect()
	ctx := context.Background()

	const n = 1000
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := p.Storage().Get(ctx, "x", "y")
			results <- err
		}()
	}

	forbidden, other := 0, 0
	for i := 0; i < n; i++ {
		if errors.Is(<-results, sdk.ErrForbidden) {
			forbidden++
		} else {
			other++
		}
	}
	logMessage(p, ctx, fmt.Sprintf("flood:forbidden=%d,other=%d,total=%d", forbidden, other, n))

	<-runDone
}

func connect() (sdk.Plugin, chan error) {
	p, err := sdk.Connect(context.Background(), sdk.Config{
		PluginID:        pluginID(),
		ProtocolVersion: 1,
	})
	if err != nil {
		os.Exit(1)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(context.Background()) }()
	return p, runDone
}

func classify(err error) string {
	switch {
	case err == nil:
		return "OK"
	case errors.Is(err, sdk.ErrForbidden):
		return "FORBIDDEN"
	case errors.Is(err, sdk.ErrUnauthorized):
		return "UNAUTHORIZED"
	case errors.Is(err, sdk.ErrUnsupported):
		return "UNSUPPORTED"
	case errors.Is(err, sdk.ErrNotFound):
		return "NOT_FOUND"
	default:
		return "OTHER:" + err.Error()
	}
}

func logResult(p sdk.Plugin, ctx context.Context, name string, err error) {
	logMessage(p, ctx, name+":"+classify(err))
}

func logMessage(p sdk.Plugin, ctx context.Context, msg string) {
	_ = p.Log(ctx, sdk.LogInfo, msg)
}

// ── Raw-wire attacks (no SDK) ───────────────────────────────────────

// stdio is a minimal io.ReadWriter over the process's own stdin/stdout —
// the same channel the SDK uses internally, but without going through
// it: the point of these two attacks is proving the runtime rejects
// them regardless of what client sent them.
type stdio struct{}

func (stdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }

func rawHandshake(rw io.ReadWriter) error {
	hello, err := ipc.Decode(rw)
	if err != nil {
		return fmt.Errorf("read HELLO: %w", err)
	}
	if hello.Type != ipc.MsgHello {
		return fmt.Errorf("expected HELLO, got %s", hello.Type)
	}
	return ipc.Encode(rw, &ipc.Message{
		Type:                 ipc.MsgHelloAck,
		HelloProtocolVersion: hello.HelloProtocolVersion,
		HelloPluginID:        pluginID(),
	})
}

// rawLog sends a log.write request and discards its response — log.write
// requires no permission, so this cannot itself fail the way the attack
// requests can, and is only used here to report the attack's outcome.
func rawLog(rw io.ReadWriter, id uint64, msg string) {
	payload, _ := json.Marshal(map[string]string{"level": "info", "message": msg})
	_ = ipc.Encode(rw, &ipc.Message{Type: ipc.MsgRequest, RequestID: id, Op: "log.write", Payload: payload})
	_, _ = ipc.Decode(rw)
}

// rawWaitForStop reads until STOP arrives, acknowledges it, and returns —
// mirroring what the SDK's Run() does internally for the clean-shutdown
// path, so host still classifies this process as ExitCleanStop like
// every other well-behaved plugin in the lab.
func rawWaitForStop(rw io.ReadWriter) {
	for {
		m, err := ipc.Decode(rw)
		if err != nil {
			return
		}
		if m.Type == ipc.MsgStop {
			_ = ipc.Encode(rw, &ipc.Message{Type: ipc.MsgStopAck})
			return
		}
	}
}

func unsupportedOp() {
	rw := stdio{}
	if err := rawHandshake(rw); err != nil {
		os.Exit(1)
	}

	if err := ipc.Encode(rw, &ipc.Message{
		Type:      ipc.MsgRequest,
		RequestID: 1,
		Op:        "storage.teleport", // not in the v1 catalog (ipc/operations.go)
	}); err != nil {
		os.Exit(1)
	}
	resp, err := ipc.Decode(rw)
	if err != nil {
		os.Exit(1)
	}
	rawLog(rw, 2, "unsupported_op:"+string(resp.ErrorCode))

	rawWaitForStop(rw)
}

func pluginIDSpoof() {
	rw := stdio{}
	if err := rawHandshake(rw); err != nil {
		os.Exit(1)
	}

	// Message has no plugin_id field for MsgRequest by design (see
	// ipc/protocol.go) — HelloPluginID is the only identity-shaped
	// field that exists on the struct at all, so it's the only place a
	// spoofing attempt can even be written down. The runtime must
	// still resolve identity from the channel (Caller, set at Start),
	// never from this.
	if err := ipc.Encode(rw, &ipc.Message{
		Type:          ipc.MsgRequest,
		RequestID:     1,
		Op:            "storage.put", // this plugin's own identity has no grants
		HelloPluginID: "com.engram.test.permission-victim",
	}); err != nil {
		os.Exit(1)
	}
	resp, err := ipc.Decode(rw)
	if err != nil {
		os.Exit(1)
	}
	rawLog(rw, 2, "plugin_id_spoof:"+string(resp.ErrorCode))

	rawWaitForStop(rw)
}
