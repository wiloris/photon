// End-to-end scenarios on the REAL executor: docs/test-laboratory.md §6,
// "sdkscenario".
//
// Chain under test: plugin process → SDK → IPC → Authorizer → coreops →
// bbolt / aesgcm / ed25519. Every other file in this package runs plugins
// against fake ipc.HandlerFuncs; coreops itself is tested by calling Handle
// directly. This file is the first place the two halves meet.
//
// KEY MATERIAL BOUNDARY. The bbolt file lives in t.TempDir(); the ed25519 pair
// and the AES master key live in this test process's memory for one test run.
// That is a test rig — the same practice as coreops_test.go — and NOT an
// answer to core-operations.md §11.1 (where the master key comes from, that it
// survives restarts) nor a decision under ADR-057 for a permanent Plugin Lab.
package lifecycle

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/wiloris/photon-core/adapters/aesgcm"
	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	ed25519adapter "github.com/wiloris/photon-core/adapters/ed25519"
	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/coreops"
	"github.com/wiloris/photon-plugin-runtime/host"
	"github.com/wiloris/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon-plugin-runtime/permissions"
)

const (
	scenarioA  = "com.engram.test.sdkscenario-a"
	scenarioB  = "com.engram.test.sdkscenario-b"
	scenarioNS = "plugins"
)

// Grants: A has everything the catalog offers; B lacks identity.sign.
var (
	grantsA = []string{"storage.read", "storage.write", "identity.read", "identity.sign", "crypto.use"}
	grantsB = []string{"storage.read", "storage.write", "identity.read", "crypto.use"}
)

// executorCall — one operation that reached the executor, and what it returned.
type executorCall struct {
	pluginID string
	op       string
	response []byte
}

// coreRig — a real coreops over real adapters, plus an observer in front of it.
type coreRig struct {
	st      ports.Storage
	private ports.PrivateKey
	public  ports.PublicKey
	exec    ipc.Handler
	fixture string // directory holding the built sdkscenario binary
	entry   string

	mu       sync.Mutex
	calls    []executorCall
	captures map[string]*logCapture
	logs     []string
}

func newCoreRig(t *testing.T) *coreRig {
	t.Helper()
	ctx := context.Background()

	st, err := bboltadapter.New(filepath.Join(t.TempDir(), "scenario.db"), []bboltadapter.Migration{{
		Version: 1,
		Up: func(tx *bolt.Tx) error {
			_, e := tx.CreateBucketIfNotExists([]byte(scenarioNS))
			return e
		},
	}})
	if err != nil {
		t.Fatalf("bbolt.New: %v", err)
	}
	if err := st.Migrate(ctx, 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(func() { st.Close(context.Background()) })

	id := ed25519adapter.New()
	pair, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	cr := aesgcm.New()
	master, err := cr.GenerateKey(ctx)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	r := &coreRig{st: st, private: pair.Private, public: pair.Public, captures: map[string]*logCapture{}}
	real, err := coreops.New(coreops.Config{
		Storage:          st,
		StorageNamespace: scenarioNS,
		Identity:         id,
		DeviceKey:        pair.Private,
		Crypto:           cr,
		DeriveKey: func(ctx context.Context, domain string) (ports.SymmetricKey, error) {
			return cr.DeriveKey(ctx, master, domain)
		},
		Log: r.onLog,
	})
	if err != nil {
		t.Fatalf("coreops.New: %v", err)
	}

	// The observer changes nothing: it records and forwards.
	r.exec = ipc.HandlerFunc(func(ctx context.Context, caller ipc.CallerInfo, op string, payload []byte) ([]byte, error) {
		out, err := real.Handle(ctx, caller, op, payload)
		r.mu.Lock()
		r.calls = append(r.calls, executorCall{caller.PluginID, op, append([]byte(nil), out...)})
		r.mu.Unlock()
		return out, err
	})

	r.fixture = t.TempDir()
	r.entry = buildPlugin(t, "sdkscenario", r.fixture)
	return r
}

// onLog routes coreops's log sink to the capture of the running plugin.
func (r *coreRig) onLog(pluginID, level, message string) {
	r.mu.Lock()
	r.logs = append(r.logs, message)
	c := r.captures[pluginID]
	r.mu.Unlock()
	if c != nil {
		payload, _ := json.Marshal(map[string]string{"level": level, "message": message})
		_, _ = c.capture(payload)
	}
}

func scenarioManifest(pluginID, entrypoint string, requested []string) *manifest.Manifest {
	return &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: pluginID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      entrypoint,
			InstancePolicy:  manifest.InstanceSingle,
		},
		Permissions: manifest.Permissions{Requested: requested},
	}
}

// run starts one plugin process in the given mode, waits for "done:<mode>",
// stops it cleanly and returns everything it logged.
func (r *coreRig) run(t *testing.T, pluginID, mode string, env map[string]string) *logCapture {
	t.Helper()
	grants := grantsA
	if pluginID == scenarioB {
		grants = grantsB
	}
	for _, k := range []string{"PHOTON_TEST_VALUE", "PHOTON_TEST_BINDING", "PHOTON_TEST_DOMAIN_INPUT"} {
		t.Setenv(k, env[k])
	}
	t.Setenv("PHOTON_TEST_MODE", mode)
	t.Setenv("PHOTON_TEST_PLUGIN_ID", pluginID)

	policy := permissions.NewPolicy()
	if err := policy.Grant(permissions.Subject{PluginID: pluginID}, grants); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	capture := newLogCapture()
	r.mu.Lock()
	r.captures[pluginID] = capture
	r.mu.Unlock()

	h := host.New(host.Config{
		Manifest:    scenarioManifest(pluginID, r.entry, grants),
		PackageRoot: r.fixture,
		Caller:      ports.PublicKey("sdkscenario-caller-" + pluginID + "-" + mode),
		Policy:      policy,
		Authorizer:  permissions.NewAuthorizer(policy),
		Handler:     r.exec,
	})
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start (plugin=%s mode=%s): %v", pluginID, mode, err)
	}
	capture.waitForPrefix(t, "done:"+mode, 15*time.Second)
	stopAndExpectClean(t, h)

	r.mu.Lock()
	delete(r.captures, pluginID)
	r.mu.Unlock()
	return capture
}

func (r *coreRig) reached(pluginID, op string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.calls {
		if c.pluginID == pluginID && c.op == op {
			return true
		}
	}
	return false
}

// value returns the text after "name:" in the first matching message.
func value(t *testing.T, c *logCapture, name string) string {
	t.Helper()
	m, ok := c.findPrefix(name + ":")
	if !ok {
		t.Fatalf("no %q in plugin log: %v", name, c.all())
	}
	return strings.TrimPrefix(m, name+":")
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func unb64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("not base64 (%q): %v", s, err)
	}
	return b
}

// domainInput is the TRUSTED oracle for ADR-037, written from the ADR text:
//
//	"plugin-sign:" ‖ uint32be(len(plugin_id)) ‖ plugin_id ‖ ":" ‖ data
//
// It deliberately does not call into coreops.
func domainInput(pluginID string, data []byte) []byte {
	var b bytes.Buffer
	b.WriteString("plugin-sign:")
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(pluginID)))
	b.Write(n[:])
	b.WriteString(pluginID)
	b.WriteString(":")
	b.Write(data)
	return b.Bytes()
}

// ── S1 ───────────────────────────────────────────────────────────────────────

// TestSDKScenario_StorageReachesRealBbolt: S1 — SDK storage calls land in the
// real bbolt behind coreops, under a key that carries the caller's plugin id.
func TestSDKScenario_StorageReachesRealBbolt(t *testing.T) {
	r := newCoreRig(t)

	rt := r.run(t, scenarioA, "storage-roundtrip", nil)
	if got := value(t, rt, "roundtrip"); got != "OK" {
		t.Fatalf("roundtrip = %q, want OK", got)
	}
	if got := value(t, rt, "after_delete"); got != "NOT_FOUND" {
		t.Fatalf("get after delete = %q, want NOT_FOUND", got)
	}

	const secret = "s1-value-in-bbolt"
	w := r.run(t, scenarioA, "storage-write", map[string]string{"PHOTON_TEST_VALUE": secret})
	if got := value(t, w, "write"); got != "OK" {
		t.Fatalf("write = %q, want OK", got)
	}

	// Oracle: read the SAME ports.Storage directly, bypassing SDK, IPC and coreops.
	var keys [][]byte
	err := r.st.View(context.Background(), func(tx ports.ReadTx) error {
		return tx.ForEach(scenarioNS, func(k, v []byte) error {
			if string(v) == secret {
				keys = append(keys, append([]byte(nil), k...))
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("direct bbolt read: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("value found under %d keys in bbolt, want exactly 1", len(keys))
	}
	if !bytes.Contains(keys[0], []byte(scenarioA)) || !bytes.Contains(keys[0], []byte("secret")) {
		t.Fatalf("bbolt key %q does not carry plugin id %q and the plugin's key", keys[0], scenarioA)
	}
}

// ── S2 ───────────────────────────────────────────────────────────────────────

// TestSDKScenario_NamespaceIsolatedOnRealStore: S2 — B cannot see A's key on
// the real store, neither by get nor by list; A still reads its own value.
func TestSDKScenario_NamespaceIsolatedOnRealStore(t *testing.T) {
	r := newCoreRig(t)

	if got := value(t, r.run(t, scenarioA, "storage-write", map[string]string{"PHOTON_TEST_VALUE": "a-only"}), "write"); got != "OK" {
		t.Fatalf("A write = %q", got)
	}

	b := r.run(t, scenarioB, "storage-read", nil)
	if got := value(t, b, "read"); got != "NOT_FOUND" {
		t.Fatalf("B read of A's key = %q, want NOT_FOUND", got)
	}
	if got := value(t, b, "list"); got != "keys:" {
		t.Fatalf("B list = %q, want no keys", got)
	}

	a := r.run(t, scenarioA, "storage-read", nil)
	if got := value(t, a, "read"); got != "value:a-only" {
		t.Fatalf("A read = %q, want value:a-only", got)
	}
	if got := value(t, a, "list"); got != "keys:secret" {
		t.Fatalf("A list = %q, want keys:secret", got)
	}
}

// ── S3 ───────────────────────────────────────────────────────────────────────

// TestSDKScenario_SignIsDomainSeparated: S3 — the device signature a plugin
// obtains is over the ADR-037 domain input, never over the raw bytes.
func TestSDKScenario_SignIsDomainSeparated(t *testing.T) {
	r := newCoreRig(t)
	data := []byte("s3 payload")

	c := r.run(t, scenarioA, "sign", map[string]string{
		"PHOTON_TEST_VALUE":        b64(data),
		"PHOTON_TEST_DOMAIN_INPUT": b64(domainInput(scenarioA, data)),
	})
	pub := unb64(t, value(t, c, "pub"))
	sig := unb64(t, value(t, c, "sig"))

	if !bytes.Equal(pub, r.public) {
		t.Fatalf("identity.public_key returned a key that is not the device key")
	}
	// Independent oracle: the standard library, not coreops.
	if !ed25519.Verify(ed25519.PublicKey(pub), domainInput(scenarioA, data), sig) {
		t.Fatalf("signature does not verify over the ADR-037 domain input")
	}
	if ed25519.Verify(ed25519.PublicKey(pub), data, sig) {
		t.Fatalf("signature verifies over RAW data — the plugin got a device signature under arbitrary bytes")
	}
	// The same through the SDK: identity.verify checks exactly what it is given.
	if got := value(t, c, "verify_raw"); got != "false" {
		t.Fatalf("SDK verify over raw data = %q, want false", got)
	}
	if got := value(t, c, "verify_domain"); got != "true" {
		t.Fatalf("SDK verify over domain input = %q, want true", got)
	}
}

// ── S4 ───────────────────────────────────────────────────────────────────────

// TestSDKScenario_SignaturesNotInterchangeableBetweenPlugins: S4 — a signature
// obtained by one plugin does not verify as another plugin's, in either
// direction.
func TestSDKScenario_SignaturesNotInterchangeableBetweenPlugins(t *testing.T) {
	r := newCoreRig(t)
	data := []byte("same bytes for both plugins")

	sigA := unb64(t, value(t, r.run(t, scenarioA, "sign", map[string]string{
		"PHOTON_TEST_VALUE":        b64(data),
		"PHOTON_TEST_DOMAIN_INPUT": b64(domainInput(scenarioA, data)),
	}), "sig"))

	// A second SIGNING identity: B itself lacks identity.sign on purpose (S6),
	// so S4 uses b2, which run() grants the same set as A.
	const signerB = "com.engram.test.sdkscenario-b2"
	sigB := unb64(t, value(t, r.run(t, signerB, "sign", map[string]string{
		"PHOTON_TEST_VALUE":        b64(data),
		"PHOTON_TEST_DOMAIN_INPUT": b64(domainInput(signerB, data)),
	}), "sig"))

	pub := ed25519.PublicKey(r.public)
	if bytes.Equal(sigA, sigB) {
		t.Fatalf("two plugins received the same signature for the same data")
	}
	if ed25519.Verify(pub, domainInput(signerB, data), sigA) {
		t.Fatalf("A's signature verifies as %s's", signerB)
	}
	if ed25519.Verify(pub, domainInput(scenarioA, data), sigB) {
		t.Fatalf("%s's signature verifies as A's", signerB)
	}
}

// ── S5 ───────────────────────────────────────────────────────────────────────

// TestSDKScenario_CryptoBindsPluginAndBindingData: S5 — ciphertext decrypts
// only for the plugin that produced it and only with the same binding data.
func TestSDKScenario_CryptoBindsPluginAndBindingData(t *testing.T) {
	r := newCoreRig(t)
	plaintext := []byte("s5 plaintext")
	b1, b2 := []byte("binding-1"), []byte("binding-2")

	ct := unb64(t, value(t, r.run(t, scenarioA, "encrypt", map[string]string{
		"PHOTON_TEST_VALUE":   b64(plaintext),
		"PHOTON_TEST_BINDING": b64(b1),
	}), "ct"))
	if bytes.Contains(ct, plaintext) {
		t.Fatalf("ciphertext contains the plaintext")
	}

	decrypt := func(pluginID string, binding []byte) string {
		return value(t, r.run(t, pluginID, "decrypt", map[string]string{
			"PHOTON_TEST_VALUE":   b64(ct),
			"PHOTON_TEST_BINDING": b64(binding),
		}), "pt")
	}
	if got := decrypt(scenarioA, b1); got != b64(plaintext) {
		t.Fatalf("A decrypt with the same binding = %q, want the plaintext", got)
	}
	if got := decrypt(scenarioA, b2); got == b64(plaintext) {
		t.Fatalf("A decrypted with a DIFFERENT binding_data")
	}
	if got := decrypt(scenarioB, b1); got == b64(plaintext) {
		t.Fatalf("B decrypted A's ciphertext")
	}
}

// ── S6 ───────────────────────────────────────────────────────────────────────

// TestSDKScenario_ForbiddenNeverReachesExecutor: S6 — B has no identity.sign;
// the SDK call gets ErrForbidden AND the real executor is never invoked for
// it. The observer proves the second half: denial happens before coreops.
// UNSUPPORTED is not repeated here — it is decided in the session and proven
// by TestPermission_*.
func TestSDKScenario_ForbiddenNeverReachesExecutor(t *testing.T) {
	r := newCoreRig(t)
	data := []byte("s6")

	c := r.run(t, scenarioB, "sign", map[string]string{
		"PHOTON_TEST_VALUE":        b64(data),
		"PHOTON_TEST_DOMAIN_INPUT": b64(domainInput(scenarioB, data)),
	})
	if got := value(t, c, "sig"); got != "FORBIDDEN" {
		t.Fatalf("B identity.sign = %q, want FORBIDDEN (sdk.ErrForbidden)", got)
	}
	// Control: the observer does see B's permitted call — so "not seen" below
	// is a measurement, not a blind observer.
	if !r.reached(scenarioB, "identity.public_key") {
		t.Fatalf("observer did not see B's permitted identity.public_key — the observer is not in the path")
	}
	if r.reached(scenarioB, "identity.sign") {
		t.Fatalf("identity.sign from B reached the executor despite the missing grant")
	}
}

// ── S7 ───────────────────────────────────────────────────────────────────────

// TestSDKScenario_PrivateKeyNeverCrossesIPC: S7 — after exercising every mode
// for both identities, neither the private key nor its seed appears in any
// executor response or any plugin log line, raw or base64.
func TestSDKScenario_PrivateKeyNeverCrossesIPC(t *testing.T) {
	r := newCoreRig(t)
	data := []byte("s7")
	sign := map[string]string{"PHOTON_TEST_VALUE": b64(data), "PHOTON_TEST_DOMAIN_INPUT": b64(domainInput(scenarioA, data))}

	for _, id := range []string{scenarioA, scenarioB} {
		r.run(t, id, "storage-roundtrip", nil)
		r.run(t, id, "storage-write", map[string]string{"PHOTON_TEST_VALUE": "v"})
		r.run(t, id, "storage-read", nil)
		r.run(t, id, "sign", sign)
		ct := value(t, r.run(t, id, "encrypt", map[string]string{"PHOTON_TEST_VALUE": b64(data), "PHOTON_TEST_BINDING": b64(data)}), "ct")
		r.run(t, id, "decrypt", map[string]string{"PHOTON_TEST_VALUE": ct, "PHOTON_TEST_BINDING": b64(data)})
	}

	secrets := map[string][]byte{"private key": r.private, "seed": r.private[:ed25519.SeedSize]}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		t.Fatalf("observer saw no executor calls — nothing was scanned")
	}
	var haystacks [][]byte
	for _, c := range r.calls {
		haystacks = append(haystacks, c.response)
	}
	for _, l := range r.logs {
		haystacks = append(haystacks, []byte(l))
	}
	for name, s := range secrets {
		needles := [][]byte{s, []byte(base64.StdEncoding.EncodeToString(s)), []byte(base64.RawStdEncoding.EncodeToString(s))}
		for _, h := range haystacks {
			for _, n := range needles {
				if bytes.Contains(h, n) {
					t.Fatalf("%s crossed the IPC boundary: found in %q", name, h)
				}
			}
		}
	}
}
