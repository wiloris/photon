// Command crash is a Plugin Test Laboratory fixture
// (docs/test-laboratory.md, "crash"). It proves that a plugin process
// dying by any means does not take the runtime down with it, and that
// host classifies each death correctly.
//
// Mode is selected by the PHOTON_TEST_MODE environment variable, not a
// command-line argument: arguments are visible in `ps` and would make the
// driving test depend on a specific way of invoking the process.
package main

import (
	"context"
	"os"
	"time"

	sdk "github.com/wiloris/photon-plugin-sdk"
)

const pluginID = "com.engram.test.crash"

func main() {
	switch os.Getenv("PHOTON_TEST_MODE") {
	case "before-handshake":
		// Die before ever reading HELLO, let alone answering it. The
		// runtime's handshake() is left with nothing coming back —
		// ExitHandshakeFailure, not ExitCrash.
		os.Exit(1)

	case "exit":
		mustConnect()
		os.Exit(1)

	case "panic":
		p := mustConnect()
		ctx := context.Background()
		go func() { _ = p.Run(ctx) }()
		// Perform a real operation, then crash while "handling" its
		// result — a plugin that dies mid-operation, not before ever
		// talking to the runtime (before-handshake) and not with
		// nothing outstanding (exit).
		_, _ = p.Storage().Get(ctx, "x", "y")
		panic("crash: deliberate panic during operation handling")

	case "mid-request":
		p := mustConnect()
		ctx := context.Background()
		go func() { _ = p.Run(ctx) }()
		// Fire a request and die without ever waiting for its
		// response — the runtime is left holding a request whose
		// caller no longer exists by the time an answer is ready.
		go func() { _, _ = p.Storage().Get(ctx, "x", "y") }()
		time.Sleep(50 * time.Millisecond) // give the request time to reach the wire
		os.Exit(1)

	default:
		// Unknown mode: fail loudly and distinctly, not silently as
		// any of the above.
		os.Exit(2)
	}
}

func mustConnect() sdk.Plugin {
	p, err := sdk.Connect(context.Background(), sdk.Config{
		PluginID:        pluginID,
		ProtocolVersion: 1,
	})
	if err != nil {
		os.Exit(1)
	}
	return p
}
