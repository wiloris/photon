package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/host"
	"github.com/wiloris/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon-plugin-runtime/permissions"
	"github.com/wiloris/photon-plugin-runtime/supervisor"
)

const crashPluginID = "com.engram.test.crash"

func crashManifest(entrypoint string) *manifest.Manifest {
	return &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: crashPluginID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      entrypoint,
			InstancePolicy:  manifest.InstanceSingle,
		},
		Permissions: manifest.Permissions{Requested: []string{"storage.read"}},
	}
}

// runCrashOnce builds the crash plugin, runs it once in the given mode as
// a real process, and returns its exit status. It fails the test only on
// infrastructure problems (build, Start); classification is left to the
// caller, since different modes expect different ExitKind values.
func runCrashOnce(t *testing.T, mode string) host.ExitStatus {
	t.Helper()

	dir := t.TempDir()
	entrypoint := buildPlugin(t, "crash", dir)
	t.Setenv("PHOTON_TEST_MODE", mode)

	m := crashManifest(entrypoint)
	policy := permissions.NewPolicy()
	if err := policy.Grant(permissions.Subject{PluginID: crashPluginID}, []string{"storage.read"}); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	auth := permissions.NewAuthorizer(policy)

	h := host.New(host.Config{
		Manifest:    m,
		PackageRoot: dir,
		Caller:      ports.PublicKey("crash-test-caller"),
		Policy:      policy,
		Authorizer:  auth,
		Handler: ipc.HandlerFunc(func(_ context.Context, op string, _ []byte) ([]byte, error) {
			if op == "storage.get" {
				return json.Marshal(map[string]any{"value": []byte("x")})
			}
			return nil, errors.New("unexpected op: " + op)
		}),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := h.Start(ctx); err != nil {
		t.Fatalf("Start (mode=%s): %v", mode, err)
	}

	// A bounded Wait is itself part of what's being tested: a runtime
	// that hangs trying to deal with a dead process would time out here,
	// not report a status — see TestCrash_MidRequestMode.
	status, err := h.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait (mode=%s) did not return in time — runtime hung instead of detecting the crash: %v", mode, err)
	}
	if h.IsRunning() {
		t.Fatalf("host still reports running after %s crash", mode)
	}
	return status
}

// TestCrash_ExitMode: os.Exit(1) right after handshake — the simplest
// crash, proving host handles a nonzero exit code correctly.
func TestCrash_ExitMode(t *testing.T) {
	status := runCrashOnce(t, "exit")
	if status.Kind != host.ExitCrash {
		t.Fatalf("kind = %v, want %v (code=%d err=%v)", status.Kind, host.ExitCrash, status.Code, status.Err)
	}
	if status.Code == 0 {
		t.Fatalf("exit code = 0, want non-zero — plugin called os.Exit(1)")
	}
	if !status.Kind.AutoRestartAllowed() {
		t.Fatalf("ExitCrash must be eligible for restart with backoff, per docs/lifecycle.md §5")
	}
}

// TestCrash_PanicMode: an unrecovered panic while handling the result of
// a real operation — a crash during work, not before or after it.
func TestCrash_PanicMode(t *testing.T) {
	status := runCrashOnce(t, "panic")
	if status.Kind != host.ExitCrash {
		t.Fatalf("kind = %v, want %v (code=%d err=%v)", status.Kind, host.ExitCrash, status.Code, status.Err)
	}
	if status.Code == 0 {
		t.Fatalf("exit code = 0, want non-zero — plugin panicked")
	}
}

// TestCrash_MidRequestMode: the plugin fires a request and dies before
// the runtime can possibly answer it. The property under test is that
// host.Wait returns promptly (enforced by runCrashOnce's context
// timeout) rather than the runtime hanging while trying to deliver a
// response nobody is listening for anymore.
func TestCrash_MidRequestMode(t *testing.T) {
	status := runCrashOnce(t, "mid-request")
	if status.Kind != host.ExitCrash {
		t.Fatalf("kind = %v, want %v (code=%d err=%v)", status.Kind, host.ExitCrash, status.Code, status.Err)
	}
}

// TestCrash_BeforeHandshakeMode: the plugin dies before ever reading
// HELLO. This must NOT be classified the same as a crash: nothing about
// the plugin's own operation failed, its handshake simply never
// happened, and host.classify distinguishes the two so supervisor
// applies the right restart reasoning.
func TestCrash_BeforeHandshakeMode(t *testing.T) {
	status := runCrashOnce(t, "before-handshake")
	if status.Kind != host.ExitHandshakeFailure {
		t.Fatalf("kind = %v, want %v (code=%d err=%v) — dying before HELLO_ACK must not be classified as a crash",
			status.Kind, host.ExitHandshakeFailure, status.Code, status.Err)
	}
	if !status.Kind.AutoRestartAllowed() {
		t.Fatalf("ExitHandshakeFailure must be eligible for restart with backoff, per docs/lifecycle.md §5")
	}
}

// TestCrash_RuntimeSurvivesAllModes runs all four crash modes back to
// back and then confirms a well-behaved plugin still completes a full
// clean lifecycle afterward — proving a crashing plugin doesn't leave
// any process-wide state (goroutines, permission registrations, host
// invariants) corrupted behind it. This is the direct check for
// docs/test-laboratory.md's "runtime остаётся работоспособным после
// каждого режима".
func TestCrash_RuntimeSurvivesAllModes(t *testing.T) {
	for _, mode := range []string{"exit", "panic", "mid-request", "before-handshake"} {
		t.Run(mode, func(t *testing.T) {
			runCrashOnce(t, mode)
		})
	}

	t.Run("hello_still_works", func(t *testing.T) {
		runHelloOnce(t, "post-crash-hello-caller")
	})
}

// crashSupervisorFactory returns a HostFactory that spawns the crash
// plugin (real process, no fakeSpawner) every time supervisor restarts
// it. Mode is controlled by the caller's PHOTON_TEST_MODE (t.Setenv)
// before Start, not by this factory — every restart re-execs the same
// entrypoint, which reads the same env var each time.
func crashSupervisorFactory(t *testing.T, entrypoint, dir string) supervisor.HostFactory {
	t.Helper()
	return func() (*host.Host, error) {
		m := crashManifest(entrypoint)
		policy := permissions.NewPolicy()
		if err := policy.Grant(permissions.Subject{PluginID: crashPluginID}, []string{"storage.read"}); err != nil {
			return nil, err
		}
		auth := permissions.NewAuthorizer(policy)

		return host.New(host.Config{
			Manifest:    m,
			PackageRoot: dir,
			Caller:      ports.PublicKey("crash-supervisor-caller"),
			Policy:      policy,
			Authorizer:  auth,
			Handler: ipc.HandlerFunc(func(_ context.Context, op string, _ []byte) ([]byte, error) {
				if op == "storage.get" {
					return json.Marshal(map[string]any{"value": []byte("x")})
				}
				return nil, errors.New("unexpected op: " + op)
			}),
		}), nil
	}
}

// fastRestartConfig mirrors supervisor's own test helper (fastConfig in
// supervisor_test.go): small delays and a small MaxAttempts so the test
// doesn't wait through real backoff, while still exercising the real
// exponential/jitter/limit logic, not a stub of it.
func fastRestartConfig() supervisor.RestartConfig {
	c := supervisor.DefaultRestartConfig()
	c.BaseDelay = 20 * time.Millisecond
	c.MaxDelay = 100 * time.Millisecond
	c.MaxAttempts = 3
	c.StableAfter = time.Hour // don't let a fast real process count as "stable"
	c.JitterFraction = 0
	return c
}

// waitForSupervisorState polls until the supervisor reaches want or the
// timeout expires — supervisor has no blocking "wait for state" API by
// design (docs/lifecycle.md: no component may block the caller
// indefinitely), so polling is correct here, not a workaround.
func waitForSupervisorState(t *testing.T, s *supervisor.Supervisor, want supervisor.State, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if s.State() == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for %s, current state %s", want, s.State())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestCrash_RestartedWithBackoffBoundedByMaxAttempts covers
// docs/test-laboratory.md's "crash перезапускается с backoff... серия
// ограничена MaxAttempts" for the "exit" mode, against a real process
// each attempt — not the in-process fakeSpawner supervisor_test.go
// already covers this same policy logic with.
func TestCrash_RestartedWithBackoffBoundedByMaxAttempts(t *testing.T) {
	dir := t.TempDir()
	entrypoint := buildPlugin(t, "crash", dir)
	t.Setenv("PHOTON_TEST_MODE", "exit")

	s := supervisor.New(supervisor.Config{
		Factory: crashSupervisorFactory(t, entrypoint, dir),
		Restart: fastRestartConfig(),
	})

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitForSupervisorState(t, s, supervisor.StateDisabled, 15*time.Second)

	// Decide (restart.go) disables once consecutiveFailures > MaxAttempts,
	// i.e. the counter reaches MaxAttempts+1 at the moment of disable —
	// same convention supervisor_test.go's TestCrashLoop_EventuallyDisables
	// checks against for spawn count.
	if f := s.Failures(); f > fastRestartConfig().MaxAttempts+1 {
		t.Fatalf("failures = %d, limit is %d — crash loop was not bounded", f, fastRestartConfig().MaxAttempts+1)
	}
}

// TestCrash_BeforeHandshakeRestartedAndBounded is the same property for
// "before-handshake": ExitHandshakeFailure restarts with backoff too,
// and that series is bounded exactly like ExitCrash's.
func TestCrash_BeforeHandshakeRestartedAndBounded(t *testing.T) {
	dir := t.TempDir()
	entrypoint := buildPlugin(t, "crash", dir)
	t.Setenv("PHOTON_TEST_MODE", "before-handshake")

	s := supervisor.New(supervisor.Config{
		Factory: crashSupervisorFactory(t, entrypoint, dir),
		Restart: fastRestartConfig(),
	})

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitForSupervisorState(t, s, supervisor.StateDisabled, 15*time.Second)

	if f := s.Failures(); f > fastRestartConfig().MaxAttempts+1 {
		t.Fatalf("failures = %d, limit is %d — handshake-failure loop was not bounded", f, fastRestartConfig().MaxAttempts+1)
	}
}

// TestCrash_OtherPluginsUnaffectedBySibling runs a crash-looping plugin
// under supervisor concurrently with a well-behaved hello run, proving
// one plugin's failure has no effect on another's — the direct check for
// docs/test-laboratory.md's "другие плагины, запущенные параллельно,
// продолжают работать".
func TestCrash_OtherPluginsUnaffectedBySibling(t *testing.T) {
	crashDir := t.TempDir()
	crashEntrypoint := buildPlugin(t, "crash", crashDir)
	// Scoped to this test's lifetime by t.Setenv. hello, started
	// concurrently below, inherits this too (env vars are process-wide
	// for spawned children) but never reads PHOTON_TEST_MODE, so it's
	// harmless for it — only crash's own main.go acts on it.
	t.Setenv("PHOTON_TEST_MODE", "exit")

	s := supervisor.New(supervisor.Config{
		Factory: crashSupervisorFactory(t, crashEntrypoint, crashDir),
		Restart: fastRestartConfig(),
	})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start crash supervisor: %v", err)
	}
	defer s.Stop(context.Background())

	// While the crash loop is running (bounded, but running concurrently
	// with the assertion below), hello must complete a full clean
	// lifecycle without interference.
	runHelloOnce(t, "sibling-hello-caller")

	waitForSupervisorState(t, s, supervisor.StateDisabled, 15*time.Second)
}
