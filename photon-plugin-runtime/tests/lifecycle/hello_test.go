// Package lifecycle drives the Plugin Test Laboratory plugins as real OS
// processes (docs/test-laboratory.md). Unlike Phase 9-10, nothing here is
// faked: exec.Command, real stdin/stdout, real process exit.
package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/host"
	"github.com/wiloris/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon-plugin-runtime/permissions"
)

const helloPluginID = "com.engram.test.hello"

// buildPlugin compiles a test-laboratory plugin from tests/plugins/<name>
// into dir, returning the binary's filename — suitable as-is for
// manifest.RuntimeSpec.Entrypoint, which must be relative to PackageRoot.
func buildPlugin(t *testing.T, name, dir string) string {
	t.Helper()

	bin := name
	src := filepath.Join("..", "plugins", name)

	cmd := exec.Command("go", "build", "-o", filepath.Join(dir, bin), src)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s plugin: %v\n%s", name, err, out)
	}
	return bin
}

func helloManifest(entrypoint string) *manifest.Manifest {
	return &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: helloPluginID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      entrypoint,
			InstancePolicy:  manifest.InstanceSingle,
		},
		Permissions: manifest.Permissions{Requested: []string{"storage.read"}},
	}
}

// runHelloOnce builds and runs the hello plugin through a full clean
// lifecycle (handshake -> authorized storage.get -> STOP -> STOP_ACK ->
// process exit), failing the test immediately on any deviation, and
// returns the resulting exit status for the caller's own assertions.
//
// Factored out so other plugins' tests (crash, in particular) can use a
// known-good hello run as a "the runtime still works" check, without
// duplicating the whole setup.
func runHelloOnce(t *testing.T, caller string) host.ExitStatus {
	t.Helper()

	dir := t.TempDir()
	entrypoint := buildPlugin(t, "hello", dir)

	m := helloManifest(entrypoint)

	policy := permissions.NewPolicy()
	if err := policy.Grant(permissions.Subject{PluginID: helloPluginID}, []string{"storage.read"}); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	auth := permissions.NewAuthorizer(policy)

	const wantValue = "hello-value"
	handlerCalled := make(chan struct{}, 1)
	errUnexpectedOp := errors.New("unexpected op")

	h := host.New(host.Config{
		Manifest:    m,
		PackageRoot: dir,
		Caller:      ports.PublicKey(caller),
		Policy:      policy,
		Authorizer:  auth,
		Handler: ipc.HandlerFunc(func(_ context.Context, _ ipc.CallerInfo, op string, _ []byte) ([]byte, error) {
			switch op {
			case "storage.get":
				return json.Marshal(map[string]any{"value": []byte(wantValue)})

			case "log.write":
				// The plugin only reaches this call after its
				// storage.get already returned successfully (program
				// order in tests/plugins/hello/main.go) — waiting for
				// this, rather than for storage.get's own invocation,
				// is what avoids racing Stop() ahead of the plugin
				// actually having received its value.
				select {
				case handlerCalled <- struct{}{}:
				default:
				}
				return json.Marshal(map[string]any{})

			default:
				return nil, errUnexpectedOp
			}
		}),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := h.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for the plugin's post-Get() confirmation before requesting a
	// stop — this proves handshake succeeded, the granted permission
	// actually reached the process, and storage.get already returned a
	// value on the plugin's side (not just that the runtime handled the
	// request — see the log.write case above).
	select {
	case <-handlerCalled:
	case st := <-h.Exited():
		t.Fatalf("hello plugin exited before confirming storage.get: %v", st)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for hello plugin to confirm storage.get — handshake likely failed")
	}

	if err := h.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	status, err := h.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if status.Kind != host.ExitCleanStop {
		t.Fatalf("exit kind = %v, want %v (err=%v, code=%d)", status.Kind, host.ExitCleanStop, status.Err, status.Code)
	}
	if status.Code != 0 {
		t.Fatalf("exit code = %d, want 0", status.Code)
	}
	if status.Kind.AutoRestartAllowed() {
		t.Fatalf("ExitCleanStop must not be eligible for auto-restart, per docs/lifecycle.md §6")
	}
	if h.IsRunning() {
		t.Fatalf("host still reports running after clean stop")
	}
	return status
}

// TestHello_RealProcessEndToEnd is the base-path check the rest of the
// laboratory depends on: a real process goes through exec.Command ->
// stdin/stdout -> handshake -> authorized operation -> STOP -> STOP_ACK ->
// process exit, and host classifies the result as ExitCleanStop.
//
// Failure here means the other four plugins (crash, permission, storage,
// stress) are pointless to run — see docs/test-laboratory.md, "hello".
func TestHello_RealProcessEndToEnd(t *testing.T) {
	runHelloOnce(t, "hello-test-caller")
}
