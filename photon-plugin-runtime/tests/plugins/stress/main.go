// Command stress is a Plugin Test Laboratory fixture
// (docs/test-laboratory.md, "stress"). Every mode here behaves like a
// DoS source on purpose — flooding requests, sending oversized frames,
// never reading, never responding to STOP, spawning children — and the
// property under test in every case is the same: the runtime survives
// it, bounded, without hanging or growing without limit. A crash of
// THIS process is never the point; what fails or hangs would be the
// runtime's own code.
//
// Mode is selected by PHOTON_TEST_MODE, same convention as the other
// four plugins in this lab.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/wiloris/photon-plugin-runtime/ipc"
	sdk "github.com/wiloris/photon-plugin-sdk"
)

const defaultPluginID = "com.engram.test.stress"

func pluginID() string {
	if id := os.Getenv("PHOTON_TEST_PLUGIN_ID"); id != "" {
		return id
	}
	return defaultPluginID
}

func main() {
	switch os.Getenv("PHOTON_TEST_MODE") {
	case "flood":
		flood()
	case "large-payload":
		largePayload()
	case "large-payload-raw":
		largePayloadRaw()
	case "slow-reader":
		slowReader()
	case "hang":
		hang()
	case "fork-bomb":
		forkBomb()
	case "fork-bomb-child":
		forkBombChild()
	default:
		os.Exit(2)
	}
}

// ── flood: thousands of requests back to back, no pauses ─────────────

func flood() {
	p, runDone := connect()
	ctx := context.Background()

	n, err := strconv.Atoi(os.Getenv("PHOTON_TEST_FLOOD_COUNT"))
	if err != nil || n <= 0 {
		n = 5000
	}

	start := time.Now()
	errs := 0
	for i := 0; i < n; i++ {
		if err := p.Log(ctx, sdk.LogInfo, "flood-item"); err != nil {
			errs++
		}
	}
	elapsed := time.Since(start)

	logMessage(p, ctx, fmt.Sprintf("flood:done:%d:%d:%dms", n, errs, elapsed.Milliseconds()))
	<-runDone
}

// ── large-payload: SDK-mediated, within the wire limit ────────────────

func largePayload() {
	p, runDone := connect()
	ctx := context.Background()

	size, err := strconv.Atoi(os.Getenv("PHOTON_TEST_PAYLOAD_SIZE"))
	if err != nil || size <= 0 {
		size = 1 << 20 // 1 MiB, comfortably under the 4 MiB wire limit
	}
	value := make([]byte, size)

	putErr := p.Storage().Put(ctx, "data", "big", value)
	report(p, ctx, "large", putErr)

	// Prove the session is still usable after handling a large payload —
	// a leaked buffer or a corrupted read position would show up here.
	followErr := p.Log(ctx, sdk.LogInfo, "large-followup")
	report(p, ctx, "large_followup", followErr)

	<-runDone
}

// ── large-payload-raw: bypasses the SDK, which would refuse to encode
// an oversized frame itself (wire.go has the same maxMessageSize check
// as the runtime's ipc/codec.go). Going through the SDK here would
// prove nothing about whether the RUNTIME enforces the limit — only
// that the SDK does. This writes a raw length-prefixed header claiming
// a size far past the limit, using encoding/binary directly instead of
// ipc.Encode (which would refuse the same way wire.go does), then exits
// without ever sending a body: docs/ipc.md's codec is documented to
// reject the length BEFORE allocating a buffer for the body, so the
// runtime must not be waiting to read one.

func largePayloadRaw() {
	rw := stdio{}
	if err := rawHandshake(rw); err != nil {
		os.Exit(1)
	}

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 0xFFFFFFF0) // far past maxMessageSize (4 MiB)
	_, _ = os.Stdout.Write(hdr[:])
	// Deliberately no body: the point is that the runtime must reject
	// the header without ever asking for one.
}

// ── slow-reader: completes the handshake, then never reads again —
// not even the responses to its own log.write calls, let alone events.
// No Run() goroutine is ever started; nothing drains stdout past the
// initial handshake read that Connect itself performs.
//
// Blocks via a long timer rather than select{}: with every goroutine in
// the process genuinely asleep and nothing left that could ever wake
// one, Go's own runtime deadlock detector (not the plugin runtime)
// steps in and kills the process outright ("all goroutines are asleep -
// deadlock!", exit code 2) — a real crash found while building this
// fixture, not the runtime behavior under test. A pending timer keeps
// the process legitimately alive from Go's perspective, so what
// actually terminates it is host.Stop's forced-kill path, not Go's own
// deadlock detection racing ahead of it.

func slowReader() {
	if _, err := sdk.Connect(context.Background(), sdk.Config{
		PluginID:        pluginID(),
		ProtocolVersion: 1,
	}); err != nil {
		os.Exit(1)
	}
	time.Sleep(time.Hour)
}

// ── hang: connects and runs normally, but its OnStop handler never
// returns — STOP arrives, gets handled, and the handler blocks forever
// instead of letting handleStop send STOP_ACK. From the runtime's side
// this looks exactly like an unresponsive plugin that ignores shutdown.
//
// See slowReader's comment: blocks via time.Sleep, not select{}, for
// the same reason — select{} here previously left Go's own deadlock
// detector as the thing actually killing the process, not host.Stop.

func hang() {
	p, runDone := connect()
	ctx := context.Background()
	p.OnStop(func(context.Context) { time.Sleep(time.Hour) })
	logMessage(p, ctx, "hang:ready")
	<-runDone
}

// ── fork-bomb: same unresponsive-to-STOP behavior as hang, but first
// spawns a handful of child processes that outlive this plugin's own
// unwillingness to exit gracefully. These children are NOT plugins —
// they never speak IPC, never touch stdin/stdout (exec.Command leaves
// Stdin/Stdout/Stderr nil, which Go redirects to /dev/null, so they
// can't interfere with THIS process's own IPC channel). They just sleep
// for a bounded safety window and exit on their own if nothing reaps
// them first. What's under test is whether the runtime's forced
// termination of THIS process also reaches them (sandbox.md's Setpgid:
// same process group, so a group-wide kill should take both down
// together) — the one sandbox property that cannot be checked without a
// real process tree.

func forkBomb() {
	p, runDone := connect()
	ctx := context.Background()
	p.OnStop(func(context.Context) { time.Sleep(time.Hour) })

	const n = 3
	pids := make([]int, 0, n)
	for i := 0; i < n; i++ {
		cmd := exec.Command(os.Args[0])
		cmd.Env = append(os.Environ(), "PHOTON_TEST_MODE=fork-bomb-child")
		if err := cmd.Start(); err != nil {
			os.Exit(1)
		}
		pids = append(pids, cmd.Process.Pid)
		// Not waited on: they're meant to outlive this call, and
		// reaping them is not this process's job — the runtime will
		// kill this whole process group, or it won't, which is
		// exactly what's being tested.
	}

	msg := "children:"
	for i, pid := range pids {
		if i > 0 {
			msg += ","
		}
		msg += strconv.Itoa(pid)
	}
	logMessage(p, ctx, msg)

	<-runDone
}

func forkBombChild() {
	// Safety net only: if the test's own cleanup or the runtime's kill
	// somehow misses this process, it exits on its own rather than
	// lingering on the machine indefinitely.
	time.Sleep(30 * time.Second)
}

// ── shared helpers ─────────────────────────────────────────────────

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

func report(p sdk.Plugin, ctx context.Context, name string, err error) {
	status := "OK"
	if err != nil {
		status = fmt.Sprintf("ERROR:%v", err)
	}
	logMessage(p, ctx, name+":"+status)
}

func logMessage(p sdk.Plugin, ctx context.Context, msg string) {
	_ = p.Log(ctx, sdk.LogInfo, msg)
}

// ── raw wire (large-payload-raw only) ─────────────────────────────────

type stdio struct{}

func (stdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }

func rawHandshake(rw stdio) error {
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
