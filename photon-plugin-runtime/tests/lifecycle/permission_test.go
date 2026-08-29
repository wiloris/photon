package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/host"
	"github.com/wiloris/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon-plugin-runtime/permissions"
)

const permissionPluginID = "com.engram.test.permission"

func permissionManifest(entrypoint string) *manifest.Manifest {
	return &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: permissionPluginID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      entrypoint,
			InstancePolicy:  manifest.InstanceSingle,
		},
	}
}

// logCapture collects log.write messages from a plugin's Handler calls
// and lets tests wait for a specific one instead of racing Stop() ahead
// of a message the plugin hasn't sent yet — same principle as hello's
// handlerCalled channel, generalized to carry the message text.
type logCapture struct {
	mu       sync.Mutex
	messages []string
	notify   chan struct{}
}

func newLogCapture() *logCapture {
	return &logCapture{notify: make(chan struct{}, 4096)}
}

func (c *logCapture) capture(payload []byte) ([]byte, error) {
	var req struct {
		Level   string `json:"level"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.messages = append(c.messages, req.Message)
	c.mu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
	return json.Marshal(map[string]any{})
}

// waitForPrefix blocks until a captured message starts with prefix, or
// fails the test after timeout. Checking already-captured messages
// before waiting on notify avoids missing a message that arrived
// between an earlier check and this call.
func (c *logCapture) waitForPrefix(t *testing.T, prefix string, timeout time.Duration) string {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if m, ok := c.findPrefix(prefix); ok {
			return m
		}
		select {
		case <-c.notify:
		case <-deadline:
			t.Fatalf("timed out waiting for a log message with prefix %q; got: %v", prefix, c.all())
		}
	}
}

func (c *logCapture) findPrefix(prefix string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.messages {
		if strings.HasPrefix(m, prefix) {
			return m, true
		}
	}
	return "", false
}

func (c *logCapture) all() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.messages...)
}

// simpleHandler answers log.write via capture and storage.get/put/delete
// with fixed canned data — sufficient for every attack except
// namespace-escape, which needs real per-caller prefixing (see
// sharedNamespaceStore below).
func simpleHandler(capture *logCapture) ipc.Handler {
	return ipc.HandlerFunc(func(_ context.Context, _ ipc.CallerInfo, op string, payload []byte) ([]byte, error) {
		switch op {
		case "log.write":
			return capture.capture(payload)
		case "storage.get":
			return json.Marshal(map[string]any{"value": []byte("x")})
		case "storage.put", "storage.delete":
			return json.Marshal(map[string]any{})
		default:
			return nil, fmt.Errorf("unexpected op: %s", op)
		}
	})
}

// startPermissionHost builds and starts the permission plugin in the
// given mode with the given grants, returning the host and a capture of
// everything it logs. Callers are responsible for Stop/Wait.
func startPermissionHost(t *testing.T, mode string, granted []string) (*host.Host, *logCapture, *permissions.Policy) {
	t.Helper()

	dir := t.TempDir()
	entrypoint := buildPlugin(t, "permission", dir)
	t.Setenv("PHOTON_TEST_MODE", mode)

	m := permissionManifest(entrypoint)
	policy := permissions.NewPolicy()
	// Always Grant, even with an empty/nil set: this represents a
	// legitimately installed plugin with zero permissions, not an
	// unknown subject. Skipping Grant() here would leave the subject
	// entirely unregistered, and Evaluate would return ErrUnknownSubject
	// (INTERNAL_ERROR) instead of denying — a different, wrong outcome
	// from the FORBIDDEN this test is actually checking for.
	if err := policy.Grant(permissions.Subject{PluginID: permissionPluginID}, granted); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	auth := permissions.NewAuthorizer(policy)
	capture := newLogCapture()

	h := host.New(host.Config{
		Manifest:    m,
		PackageRoot: dir,
		Caller:      ports.PublicKey("permission-test-caller-" + mode),
		Policy:      policy,
		Authorizer:  auth,
		Handler:     simpleHandler(capture),
	})

	// Deliberately context.Background(), not a function-local
	// WithTimeout: Start's ctx is the base for the WHOLE session
	// lifetime (session.Run and every per-request context derive from
	// it), and Stop/Wait happen later, in a separate function call —
	// a deferred cancel() here would fire the instant this function
	// returns and tear the session down before it does anything.
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start (mode=%s): %v", mode, err)
	}
	return h, capture, policy
}

// stopAndExpectClean stops h and asserts a clean exit — every attack in
// this file is expected to leave the process itself well-behaved even
// when the specific operation it attempted was denied.
func stopAndExpectClean(t *testing.T, h *host.Host) {
	t.Helper()
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
		t.Fatalf("exit kind = %v, want %v (a denied operation must not make the plugin process itself misbehave)",
			status.Kind, host.ExitCleanStop)
	}
}

// TestPermission_OperationWithoutGrant: docs/test-laboratory.md §3, row 1
// — storage.put attempted with no grants at all.
func TestPermission_OperationWithoutGrant(t *testing.T) {
	h, capture, _ := startPermissionHost(t, "attempt-write", nil)
	defer stopAndExpectClean(t, h)

	if got := capture.waitForPrefix(t, "put:", 5*time.Second); got != "put:FORBIDDEN" {
		t.Fatalf("got %q, want put:FORBIDDEN", got)
	}
}

// TestPermission_ReadDoesNotImplyWrite: row 2 — storage.read is granted,
// storage.put is attempted. Must not be derived from storage.read.
func TestPermission_ReadDoesNotImplyWrite(t *testing.T) {
	h, capture, _ := startPermissionHost(t, "attempt-write", []string{"storage.read"})
	defer stopAndExpectClean(t, h)

	if got := capture.waitForPrefix(t, "put:", 5*time.Second); got != "put:FORBIDDEN" {
		t.Fatalf("got %q, want put:FORBIDDEN — storage.read must not imply storage.write", got)
	}
}

// sharedNamespaceStore backs the namespace-escape test with a real
// per-caller prefix, exactly matching sdk-boundary.md's requirement
// that "namespace принудительно ограничен... не проверка в SDK, а
// преобразование на стороне runtime". Each host's Handler closure
// captures its OWN plugin ID at construction time (known to the
// runtime, never to the plugin) and applies it unconditionally — the
// namespace string the plugin sends never influences whose prefix is
// used, only what comes after it.
type sharedNamespaceStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

// handler намеренно НЕ захватывает plugin_id замыканием: идентичность
// приходит параметром из конфигурации сессии (ADR-036). Раньше она
// передавалась при конструировании, и тест доказывал изоляцию лишь при
// условии, что handler создан правильно; теперь она приходит оттуда же,
// откуда её получит настоящий исполнитель Phase 13.
func (s *sharedNamespaceStore) handler(capture *logCapture) ipc.Handler {
	return ipc.HandlerFunc(func(_ context.Context, caller ipc.CallerInfo, op string, payload []byte) ([]byte, error) {
		callerPluginID := caller.PluginID
		switch op {
		case "log.write":
			return capture.capture(payload)

		case "storage.put":
			var req struct {
				Namespace, Key string
				Value          []byte
			}
			if err := json.Unmarshal(payload, &req); err != nil {
				return nil, err
			}
			key := "plugin." + callerPluginID + "." + req.Namespace + "|" + req.Key
			s.mu.Lock()
			s.data[key] = req.Value
			s.mu.Unlock()
			return json.Marshal(map[string]any{})

		case "storage.get":
			var req struct{ Namespace, Key string }
			if err := json.Unmarshal(payload, &req); err != nil {
				return nil, err
			}
			key := "plugin." + callerPluginID + "." + req.Namespace + "|" + req.Key
			s.mu.Lock()
			val := s.data[key]
			s.mu.Unlock()
			return json.Marshal(map[string]any{"value": val})

		default:
			return nil, fmt.Errorf("unexpected op: %s", op)
		}
	})
}

// TestPermission_NamespaceEscapeIsolated: row 3 — explicit reference to
// another plugin's namespace. Two real processes: a victim that writes
// a secret, and an attacker that tries several crafted namespace
// strings hoping to land on the victim's actual storage key. None may
// succeed, because the runtime always prepends the ATTACKER's own
// identity, never anything derived from what the attacker sent.
func TestPermission_NamespaceEscapeIsolated(t *testing.T) {
	const victimID = "com.engram.test.permission-victim"
	const attackerID = "com.engram.test.permission" // matches PluginID baked into the binary

	store := &sharedNamespaceStore{data: map[string][]byte{}}

	// ── victim: writes a secret under its own real identity ──────────
	victimDir := t.TempDir()
	victimEntry := buildPlugin(t, "permission", victimDir)
	t.Setenv("PHOTON_TEST_MODE", "namespace-victim")
	t.Setenv("PHOTON_TEST_PLUGIN_ID", victimID)

	victimManifest := &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: victimID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      victimEntry,
			InstancePolicy:  manifest.InstanceSingle,
		},
	}
	victimPolicy := permissions.NewPolicy()
	if err := victimPolicy.Grant(permissions.Subject{PluginID: victimID}, []string{"storage.write"}); err != nil {
		t.Fatalf("Grant (victim): %v", err)
	}
	victimAuth := permissions.NewAuthorizer(victimPolicy)
	victimCapture := newLogCapture()

	victimHost := host.New(host.Config{
		Manifest:    victimManifest,
		PackageRoot: victimDir,
		Caller:      ports.PublicKey("permission-victim-caller"),
		Policy:      victimPolicy,
		Authorizer:  victimAuth,
		Handler:     store.handler(victimCapture),
	})

	// context.Background(): see startPermissionHost for why a
	// function-local WithTimeout cancelled right after Start() would
	// tear the session down before it does anything.
	if err := victimHost.Start(context.Background()); err != nil {
		t.Fatalf("Start (victim): %v", err)
	}

	if got := victimCapture.waitForPrefix(t, "victim_write:", 5*time.Second); got != "victim_write:OK" {
		t.Fatalf("victim write: got %q, want victim_write:OK", got)
	}
	stopAndExpectClean(t, victimHost)

	// ── attacker: tries to read the victim's key via crafted namespaces ──
	attackerDir := t.TempDir()
	attackerEntry := buildPlugin(t, "permission", attackerDir)
	t.Setenv("PHOTON_TEST_MODE", "namespace-attacker")
	// t.Setenv only reverts at test cleanup, not on a second call — must
	// explicitly reset this back from the victim's ID set above.
	t.Setenv("PHOTON_TEST_PLUGIN_ID", attackerID)

	attackerManifest := permissionManifest(attackerEntry)
	attackerPolicy := permissions.NewPolicy()
	if err := attackerPolicy.Grant(permissions.Subject{PluginID: attackerID}, []string{"storage.read"}); err != nil {
		t.Fatalf("Grant (attacker): %v", err)
	}
	attackerAuth := permissions.NewAuthorizer(attackerPolicy)
	attackerCapture := newLogCapture()

	attackerHost := host.New(host.Config{
		Manifest:    attackerManifest,
		PackageRoot: attackerDir,
		Caller:      ports.PublicKey("permission-attacker-caller"),
		Policy:      attackerPolicy,
		Authorizer:  attackerAuth,
		Handler:     store.handler(attackerCapture),
	})

	if err := attackerHost.Start(context.Background()); err != nil {
		t.Fatalf("Start (attacker): %v", err)
	}
	defer stopAndExpectClean(t, attackerHost)

	for i := 0; i < 4; i++ {
		prefix := fmt.Sprintf("attacker_get_%d:", i)
		got := attackerCapture.waitForPrefix(t, prefix, 5*time.Second)
		if strings.Contains(got, "victim-only-data") {
			t.Fatalf("namespace escape succeeded: %q leaked the victim's secret", got)
		}
	}
}

// TestPermission_UnknownOperationIsUnsupportedNotForbidden: row 4 — an
// operation not in the v1 catalog must answer UNSUPPORTED, not
// FORBIDDEN: "I don't know this operation" and "you can't do this" are
// different situations (ADR-032).
func TestPermission_UnknownOperationIsUnsupportedNotForbidden(t *testing.T) {
	h, capture, _ := startPermissionHost(t, "unsupported-op", nil)
	defer stopAndExpectClean(t, h)

	if got := capture.waitForPrefix(t, "unsupported_op:", 5*time.Second); got != "unsupported_op:UNSUPPORTED" {
		t.Fatalf("got %q, want unsupported_op:UNSUPPORTED", got)
	}
}

// TestPermission_PluginIDInBodyIsIgnored: row 5 — the request claims to
// be from a DIFFERENT plugin that genuinely has the permission this
// plugin lacks. If the runtime read identity from the message instead
// of the channel, this would succeed.
func TestPermission_PluginIDInBodyIsIgnored(t *testing.T) {
	dir := t.TempDir()
	entrypoint := buildPlugin(t, "permission", dir)
	t.Setenv("PHOTON_TEST_MODE", "plugin-id-spoof")

	m := permissionManifest(entrypoint)
	policy := permissions.NewPolicy()
	// The real (channel) identity is registered with zero permissions —
	// explicit empty Grant, not skipped: an ungranted-but-known subject
	// denies with FORBIDDEN, an unregistered one errors with
	// ErrUnknownSubject/INTERNAL_ERROR (see startPermissionHost).
	if err := policy.Grant(permissions.Subject{PluginID: permissionPluginID}, nil); err != nil {
		t.Fatalf("Grant (real identity): %v", err)
	}
	// The spoofed target genuinely has the permission this plugin's
	// real (channel) identity does not — proving the spoof had zero
	// effect requires that the grant existing somewhere doesn't matter.
	if err := policy.Grant(permissions.Subject{PluginID: "com.engram.test.permission-victim"}, []string{"storage.write"}); err != nil {
		t.Fatalf("Grant (spoof target): %v", err)
	}
	auth := permissions.NewAuthorizer(policy)
	capture := newLogCapture()

	h := host.New(host.Config{
		Manifest:    m,
		PackageRoot: dir,
		Caller:      ports.PublicKey("permission-spoof-caller"),
		Policy:      policy,
		Authorizer:  auth,
		Handler:     simpleHandler(capture),
	})

	// See startPermissionHost for why this is context.Background(),
	// not a cancelled-after-return WithTimeout.
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer stopAndExpectClean(t, h)

	if got := capture.waitForPrefix(t, "plugin_id_spoof:", 5*time.Second); got != "plugin_id_spoof:FORBIDDEN" {
		t.Fatalf("got %q, want plugin_id_spoof:FORBIDDEN — identity must come from the channel, not the request body", got)
	}
}

// TestPermission_RevokedPermissionBlocksImmediately: row 6 — a
// permission revoked on a live session must be enforced on the very
// next request, without the plugin reconnecting.
func TestPermission_RevokedPermissionBlocksImmediately(t *testing.T) {
	h, capture, policy := startPermissionHost(t, "revoke-live", []string{"storage.read"})
	defer stopAndExpectClean(t, h)

	if got := capture.waitForPrefix(t, "first_get:", 5*time.Second); got != "first_get:OK" {
		t.Fatalf("got %q, want first_get:OK", got)
	}

	policy.Revoke(permissions.Subject{PluginID: permissionPluginID}, "storage.read")
	if err := h.SendEvent("retry", nil); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}

	if got := capture.waitForPrefix(t, "second_get:", 5*time.Second); got != "second_get:FORBIDDEN" {
		t.Fatalf("got %q, want second_get:FORBIDDEN — revocation must take effect on the same session immediately", got)
	}
}

// TestPermission_FloodDoesNotDegradeRuntime: row 7 — a thousand denied
// requests back to back must all be rejected and must not hang or crash
// the runtime.
func TestPermission_FloodDoesNotDegradeRuntime(t *testing.T) {
	h, capture, _ := startPermissionHost(t, "flood", nil)
	defer stopAndExpectClean(t, h)

	got := capture.waitForPrefix(t, "flood:", 15*time.Second)
	want := "flood:forbidden=1000,other=0,total=1000"
	if got != want {
		t.Fatalf("got %q, want %q — every one of 1000 requests without permission must be FORBIDDEN", got, want)
	}
}
