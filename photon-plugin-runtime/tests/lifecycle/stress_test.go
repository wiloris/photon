package lifecycle

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-plugin-runtime/host"
	"github.com/wiloris/photon/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon/photon-plugin-runtime/permissions"
	"github.com/wiloris/photon/photon-plugin-runtime/sandbox"
)

const stressPluginID = "com.engram.test.stress"

func stressManifest(entrypoint string) *manifest.Manifest {
	return &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: stressPluginID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      entrypoint,
			InstancePolicy:  manifest.InstanceSingle,
		},
		Permissions: manifest.Permissions{Requested: []string{"storage.read", "storage.write"}},
	}
}

// startStressHost builds and starts the stress plugin under mode, wired
// to simpleHandler (already sufficient here: every mode either uses
// log.write/storage.* through simpleHandler or bypasses the handler
// entirely via raw wire). Isolation is left at its default (none) unless
// iso is non-nil — only the fork-bomb test needs real Setpgid.
func startStressHost(t *testing.T, mode string, iso host.Isolation) (*host.Host, *logCapture) {
	t.Helper()

	dir := t.TempDir()
	entrypoint := buildPlugin(t, "stress", dir)
	t.Setenv("PHOTON_TEST_MODE", mode)

	m := stressManifest(entrypoint)
	policy := permissions.NewPolicy()
	if err := policy.Grant(permissions.Subject{PluginID: stressPluginID}, []string{"storage.read", "storage.write"}); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	auth := permissions.NewAuthorizer(policy)
	capture := newLogCapture()

	h := host.New(host.Config{
		Manifest:    m,
		PackageRoot: dir,
		Caller:      ports.PublicKey("stress-test-caller-" + mode),
		Policy:      policy,
		Authorizer:  auth,
		Handler:     simpleHandler(capture),
		Isolation:   iso,
	})

	// context.Background(), not a function-local WithTimeout: Start's
	// ctx is the base for the whole session lifetime, and Stop/Wait
	// happen later in a separate call — see permission_test.go's
	// startPermissionHost for the full explanation.
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start (mode=%s): %v", mode, err)
	}
	return h, capture
}

// TestStress_FloodManyRequestsSucceedWithoutUnboundedGrowth: §5, "flood"
// — thousands of requests with no pauses between them. The runtime must
// keep answering (no request left unanswered, no deadlock) and must not
// accumulate per-request state: goroutine count in THIS process (which
// hosts the Session servicing every request) should return to roughly
// its pre-flood baseline once the flood is done, not grow with N.
func TestStress_FloodManyRequestsSucceedWithoutUnboundedGrowth(t *testing.T) {
	const n = 5000
	t.Setenv("PHOTON_TEST_FLOOD_COUNT", strconv.Itoa(n))

	before := runtime.NumGoroutine()

	h, capture := startStressHost(t, "flood", nil)
	defer stopAndExpectClean(t, h)

	got := capture.waitForPrefix(t, "flood:done:", 30*time.Second)
	parts := strings.Split(strings.TrimPrefix(got, "flood:done:"), ":")
	if len(parts) != 3 {
		t.Fatalf("malformed flood report: %q", got)
	}
	gotN, errs := parts[0], parts[1]
	if gotN != strconv.Itoa(n) {
		t.Fatalf("flood report count = %q, want %d", gotN, n)
	}
	if errs != "0" {
		t.Fatalf("flood report errors = %q, want 0 (all %d requests should have been answered): %q", errs, n, got)
	}

	// Goroutines spun up to service in-flight requests should have
	// wound down well before this point — the flood already completed
	// on the plugin side, meaning every response was already delivered.
	// A generous tolerance, not an exact match: unrelated goroutines
	// (GC, timers) fluctuate regardless of this test.
	after := runtime.NumGoroutine()
	if after > before+10 {
		t.Fatalf("goroutine count grew from %d to %d after a %d-request flood — suggests per-request state is not being cleaned up", before, after, n)
	}
}

// TestStress_LargePayloadWithinLimitSucceeds: §5, "large-payload",
// within-bounds half — a payload comfortably under the 4 MiB wire limit
// goes through normally, and the session is still usable afterward.
func TestStress_LargePayloadWithinLimitSucceeds(t *testing.T) {
	t.Setenv("PHOTON_TEST_PAYLOAD_SIZE", strconv.Itoa(1<<20)) // 1 MiB

	h, capture := startStressHost(t, "large-payload", nil)
	defer stopAndExpectClean(t, h)

	if got := capture.waitForPrefix(t, "large:", 10*time.Second); got != "large:OK" {
		t.Fatalf("large payload put: got %q, want large:OK", got)
	}
	if got := capture.waitForPrefix(t, "large_followup:", 10*time.Second); got != "large_followup:OK" {
		t.Fatalf("post-large-payload call: got %q, want large_followup:OK — session must survive a large message", got)
	}
}

// TestStress_LargePayloadOverLimitRejectedByRuntimeWithoutHang: §5,
// "large-payload", over-the-limit half — a raw-wire frame claiming a
// size far past maxMessageSize (4 MiB), sent directly (bypassing the
// SDK, which enforces the identical limit client-side and would refuse
// to construct such a frame at all — proving nothing about the runtime
// itself). The runtime must reject it promptly, without hanging waiting
// to read a body it should never have agreed to receive, and without
// taking any other plugin down with it.
func TestStress_LargePayloadOverLimitRejectedByRuntimeWithoutHang(t *testing.T) {
	h, _ := startStressHost(t, "large-payload-raw", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	status, err := h.Wait(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Wait did not return in time — runtime hung on an oversized frame instead of rejecting it: %v", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("runtime took %s to reject an oversized frame header — should be near-instant, no allocation or body read involved", elapsed)
	}
	if status.Kind != host.ExitProtocolViolation {
		t.Fatalf("exit kind = %v, want %v (code=%d err=%v)", status.Kind, host.ExitProtocolViolation, status.Code, status.Err)
	}

	// Sibling unaffected — same check crash_test.go makes after its own
	// modes: one plugin's abuse must not taint the next.
	helloStatus := runHelloOnce(t, "stress-sibling-caller")
	if helloStatus.Kind != host.ExitCleanStop {
		t.Fatalf("hello after stress large-payload-raw: kind = %v, want %v", helloStatus.Kind, host.ExitCleanStop)
	}
}

// TestStress_SlowReaderEventQueueDropsInsteadOfGrowing: §5,
// "slow-reader" — a plugin that completes the handshake and then never
// reads anything again, not even events. docs/ipc.md §8 requires the
// event queue to be bounded and to drop rather than grow when a plugin
// falls behind; SendEvent must never block regardless of how far behind
// the plugin is.
func TestStress_SlowReaderEventQueueDropsInsteadOfGrowing(t *testing.T) {
	h, _ := startStressHost(t, "slow-reader", nil)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = h.Stop(ctx)
		_, _ = h.Wait(ctx)
	}()

	// Comfortably past both the 64-entry event queue and a typical OS
	// pipe buffer (64 KiB): if SendEvent blocked instead of dropping,
	// this loop would hang well short of 5000 iterations.
	const n = 5000
	payload := make([]byte, 200)

	start := time.Now()
	dropped := 0
	for i := 0; i < n; i++ {
		if err := h.SendEvent("stress.tick", payload); err != nil {
			dropped++
		}
	}
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Fatalf("%d SendEvent calls against a non-reading plugin took %s — SendEvent must never block", n, elapsed)
	}
	if dropped == 0 {
		t.Fatalf("0 of %d events were dropped against a plugin that never reads — the event queue does not appear to be bounded", n)
	}
	t.Logf("%d/%d events dropped (queue correctly bounded)", dropped, n)
}

// TestStress_HangForciblyTerminatedWithinBoundedTime: §5, "hang" — a
// plugin whose OnStop handler never returns, so STOP_ACK never comes.
// docs/ipc.md §9: the runtime must not hang waiting for it — forced
// termination must land within the sum of the two bounded waits
// host.Stop uses (gracefulStopTimeout + gracefulExitTimeout), plus
// slack, not "eventually".
func TestStress_HangForciblyTerminatedWithinBoundedTime(t *testing.T) {
	h, capture := startStressHost(t, "hang", nil)
	capture.waitForPrefix(t, "hang:ready", 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	if err := h.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	status, err := h.Wait(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if elapsed > 4*time.Second {
		t.Fatalf("Stop+Wait against an unresponsive plugin took %s — forced termination is supposed to be bounded", elapsed)
	}
	if status.Kind != host.ExitCleanStop {
		t.Fatalf("exit kind = %v, want %v (a stop we requested is always clean_stop, even when forced)", status.Kind, host.ExitCleanStop)
	}

	// Runtime still works after forcibly killing an unresponsive plugin.
	helloStatus := runHelloOnce(t, "stress-sibling-caller-hang")
	if helloStatus.Kind != host.ExitCleanStop {
		t.Fatalf("hello after stress hang: kind = %v, want %v", helloStatus.Kind, host.ExitCleanStop)
	}
}

// TestStress_ForkBombChildrenDieWithPlugin: §5, "fork-bomb" — the one
// property in this whole lab that cannot be checked in-process. A
// plugin that forks children and then, like "hang", never acknowledges
// STOP, forcing the runtime down the same forced-kill path exercised
// above. sandbox.md documents Setpgid specifically so that killing the
// plugin also kills what it spawned (same process group); this is the
// only test that actually drives that path with real descendants and
// checks whether they're still alive afterward.
//
// Requires real Linux sandboxing (Setpgid) — host.Config.Isolation
// defaults to none, under which no process group isolation exists at
// all and this property wouldn't even be applicable.
func TestStress_ForkBombChildrenDieWithPlugin(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux-specific: current platform is " + runtime.GOOS)
	}

	h, capture := startStressHost(t, "fork-bomb", sandbox.NewLinux())

	got := capture.waitForPrefix(t, "children:", 10*time.Second)
	pidsStr := strings.TrimPrefix(got, "children:")
	var pids []int
	for _, s := range strings.Split(pidsStr, ",") {
		pid, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("malformed children report: %q", got)
		}
		pids = append(pids, pid)
	}
	if len(pids) == 0 {
		t.Fatalf("no child PIDs reported: %q", got)
	}
	for _, pid := range pids {
		if !processAlive(pid) {
			t.Fatalf("child pid %d reported by the plugin is already dead before Stop was even called", pid)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	status, err := h.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if status.Kind != host.ExitCleanStop {
		t.Fatalf("exit kind = %v, want %v", status.Kind, host.ExitCleanStop)
	}

	// SIGKILL delivery/reaping can lag scheduling by a hair; poll
	// briefly rather than asserting instantaneously.
	deadline := time.Now().Add(2 * time.Second)
	var alive []int
	for {
		alive = nil
		for _, pid := range pids {
			if processAlive(pid) {
				alive = append(alive, pid)
			}
		}
		if len(alive) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(alive) > 0 {
		t.Fatalf("child pids %v are still alive after the plugin was force-killed — Setpgid is applied (sandbox/linux.go) but the kill path does not appear to use it (host/spawner.go kills only the single plugin pid)", alive)
	}
}

// processAlive reports whether pid refers to a live process, using
// signal 0 (POSIX: no signal sent, only existence/permission checked).
func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	return err != syscall.ESRCH
}
