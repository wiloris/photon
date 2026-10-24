// Command hello is a Plugin Test Laboratory fixture
// (docs/test-laboratory.md, "hello"). It proves the base path works as a
// real OS process, not the in-process net.Pipe stand-in used through
// Phase 10: handshake, one authorized operation, clean shutdown on STOP.
//
// It is not a demonstration of SDK usage. If this fails, the other four
// plugins in the laboratory (crash, permission, storage, stress) are
// pointless to run — they all build on this same path working.
package main

import (
	"context"
	"os"

	sdk "github.com/wiloris/photon/photon-plugin-sdk"
)

const pluginID = "com.engram.test.hello"

func main() {
	ctx := context.Background()

	p, err := sdk.Connect(ctx, sdk.Config{
		PluginID:        pluginID,
		ProtocolVersion: 1,
	})
	if err != nil {
		os.Exit(1)
	}

	// Run() reads incoming messages off the wire and delivers responses
	// to pending calls — it must be running before any API call can
	// possibly complete. It returns once STOP_ACK has been sent and the
	// session closed on that request; an exit here that wasn't requested
	// by the runtime is classified by host as ExitCrash, not
	// ExitCleanStop, which is exactly the distinction the driving test
	// checks.
	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(ctx) }()

	if _, err := p.Storage().Get(ctx, "greeting", "message"); err != nil {
		os.Exit(2)
	}

	// Best-effort signal that Get() already returned successfully — the
	// driving test waits for this call, not for storage.get's own
	// response, so it never races ahead of Get() actually completing on
	// this side. The error is intentionally ignored: this call exists
	// for test synchronization, not as a checked assertion, and a
	// session closed by the runtime while it's in flight is expected,
	// not a failure.
	_ = p.Log(ctx, sdk.LogInfo, "ready")

	if err := <-runDone; err != nil {
		os.Exit(3)
	}
}
