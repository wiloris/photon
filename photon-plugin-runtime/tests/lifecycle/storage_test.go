package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-plugin-runtime/host"
	"github.com/wiloris/photon/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon/photon-plugin-runtime/permissions"
)

const storagePluginID = "com.engram.test.storage"

func storageManifest(pluginID, entrypoint string) *manifest.Manifest {
	return &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: pluginID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      entrypoint,
			InstancePolicy:  manifest.InstanceSingle,
		},
		Permissions: manifest.Permissions{Requested: []string{"storage.read", "storage.write"}},
	}
}

// sharedStorageStore backs every storage test in this file with the
// same real per-caller prefixing used in permission_test.go's
// namespace-escape test: each host's Handler closure captures its own
// plugin ID at construction time and prepends it unconditionally.
// Unlike permission, nothing here tries to escape it — the property
// under test is that two WELL-BEHAVED plugins never even notice each
// other, not that a malicious one fails to break in.
type sharedStorageStore struct {
	mu   sync.Mutex
	data map[string][]byte // "plugin.<id>.<namespace>|<key>" -> value
}

func newSharedStorageStore() *sharedStorageStore {
	return &sharedStorageStore{data: map[string][]byte{}}
}

func (s *sharedStorageStore) fullKey(callerPluginID, namespace, key string) string {
	return "plugin." + callerPluginID + "." + namespace + "|" + key
}

// handler намеренно НЕ захватывает plugin_id замыканием: идентичность
// приходит параметром из конфигурации сессии (ADR-036). Раньше она
// передавалась при конструировании, и тест доказывал изоляцию лишь при
// условии, что handler создан правильно; теперь она приходит оттуда же,
// откуда её получит настоящий исполнитель Phase 13.
func (s *sharedStorageStore) handler(capture *logCapture) ipc.Handler {
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
			s.mu.Lock()
			s.data[s.fullKey(callerPluginID, req.Namespace, req.Key)] = req.Value
			s.mu.Unlock()
			return json.Marshal(map[string]any{})

		case "storage.get":
			var req struct{ Namespace, Key string }
			if err := json.Unmarshal(payload, &req); err != nil {
				return nil, err
			}
			s.mu.Lock()
			val := s.data[s.fullKey(callerPluginID, req.Namespace, req.Key)]
			s.mu.Unlock()
			return json.Marshal(map[string]any{"value": val})

		case "storage.delete":
			var req struct{ Namespace, Key string }
			if err := json.Unmarshal(payload, &req); err != nil {
				return nil, err
			}
			s.mu.Lock()
			delete(s.data, s.fullKey(callerPluginID, req.Namespace, req.Key))
			s.mu.Unlock()
			return json.Marshal(map[string]any{})

		case "storage.list":
			var req struct{ Namespace, Prefix string }
			if err := json.Unmarshal(payload, &req); err != nil {
				return nil, err
			}
			nsPrefix := "plugin." + callerPluginID + "." + req.Namespace + "|"
			s.mu.Lock()
			var keys []string
			for full := range s.data {
				if !strings.HasPrefix(full, nsPrefix) {
					continue
				}
				bare := strings.TrimPrefix(full, nsPrefix)
				if strings.HasPrefix(bare, req.Prefix) {
					keys = append(keys, bare)
				}
			}
			s.mu.Unlock()
			sort.Strings(keys)
			return json.Marshal(map[string]any{"keys": keys})

		default:
			return nil, fmt.Errorf("unexpected op: %s", op)
		}
	})
}

// runStorageMode builds and runs the storage plugin once, under the
// given identity/value/mode, against the given shared store, and
// returns its log capture for assertions. Callers own Stop/Wait via the
// returned host.
func runStorageMode(t *testing.T, store *sharedStorageStore, pluginID, mode, value string) (*host.Host, *logCapture) {
	t.Helper()

	dir := t.TempDir()
	entrypoint := buildPlugin(t, "storage", dir)
	t.Setenv("PHOTON_TEST_MODE", mode)
	t.Setenv("PHOTON_TEST_PLUGIN_ID", pluginID)
	t.Setenv("PHOTON_TEST_VALUE", value)

	m := storageManifest(pluginID, entrypoint)
	policy := permissions.NewPolicy()
	if err := policy.Grant(permissions.Subject{PluginID: pluginID}, []string{"storage.read", "storage.write"}); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	auth := permissions.NewAuthorizer(policy)
	capture := newLogCapture()

	h := host.New(host.Config{
		Manifest:    m,
		PackageRoot: dir,
		Caller:      ports.PublicKey("storage-test-caller-" + pluginID + "-" + mode),
		Policy:      policy,
		Authorizer:  auth,
		Handler:     store.handler(capture),
	})

	// context.Background(), not a function-local WithTimeout: Start's
	// ctx is the base for the whole session, and Stop/Wait happen in a
	// separate call — see permission_test.go's startPermissionHost for
	// the full explanation of why a deferred cancel() here is wrong.
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start (plugin=%s mode=%s): %v", pluginID, mode, err)
	}
	return h, capture
}

// TestStorage_IdenticalKeysDoNotCollide: docs/test-laboratory.md §4,
// "данные не пересекаются при одинаковых ключах" — two plugins write
// DIFFERENT values to the SAME namespace/key and each reads back its
// own, never FORBIDDEN, never the other's value.
func TestStorage_IdenticalKeysDoNotCollide(t *testing.T) {
	store := newSharedStorageStore()

	hA, capA := runStorageMode(t, store, "com.engram.test.storage-a", "write-secret", "value-a")
	if got := capA.waitForPrefix(t, "write:", 5*time.Second); got != "write:OK" {
		t.Fatalf("plugin A write: got %q, want write:OK", got)
	}
	stopAndExpectClean(t, hA)

	hB, capB := runStorageMode(t, store, "com.engram.test.storage-b", "write-secret", "value-b")
	if got := capB.waitForPrefix(t, "write:", 5*time.Second); got != "write:OK" {
		t.Fatalf("plugin B write: got %q, want write:OK", got)
	}
	stopAndExpectClean(t, hB)

	hA2, capA2 := runStorageMode(t, store, "com.engram.test.storage-a", "read-secret", "")
	if got := capA2.waitForPrefix(t, "read:", 5*time.Second); got != "read:value-a" {
		t.Fatalf("plugin A read: got %q, want read:value-a", got)
	}
	stopAndExpectClean(t, hA2)

	hB2, capB2 := runStorageMode(t, store, "com.engram.test.storage-b", "read-secret", "")
	if got := capB2.waitForPrefix(t, "read:", 5*time.Second); got != "read:value-b" {
		t.Fatalf("plugin B read: got %q, want read:value-b", got)
	}
	stopAndExpectClean(t, hB2)
}

// TestStorage_ListDoesNotLeakOtherPluginsKeys: §4, "list не показывает
// чужие ключи" — two plugins each write the same three key names under
// different values; List must return exactly three keys for each, never
// more, never the sibling's values.
func TestStorage_ListDoesNotLeakOtherPluginsKeys(t *testing.T) {
	store := newSharedStorageStore()

	hA, capA := runStorageMode(t, store, "com.engram.test.storage-a", "write-many", "a")
	if got := capA.waitForPrefix(t, "write_many:", 5*time.Second); got != "write_many:OK" {
		t.Fatalf("plugin A write_many: got %q, want write_many:OK", got)
	}
	stopAndExpectClean(t, hA)

	hB, capB := runStorageMode(t, store, "com.engram.test.storage-b", "write-many", "b")
	if got := capB.waitForPrefix(t, "write_many:", 5*time.Second); got != "write_many:OK" {
		t.Fatalf("plugin B write_many: got %q, want write_many:OK", got)
	}
	stopAndExpectClean(t, hB)

	hA2, capA2 := runStorageMode(t, store, "com.engram.test.storage-a", "list-keys", "")
	wantA := "list:k1,k2,k3"
	if got := capA2.waitForPrefix(t, "list:", 5*time.Second); got != wantA {
		t.Fatalf("plugin A list: got %q, want %q — exactly its own three keys, no more", got, wantA)
	}
	stopAndExpectClean(t, hA2)

	hB2, capB2 := runStorageMode(t, store, "com.engram.test.storage-b", "list-keys", "")
	wantB := "list:k1,k2,k3"
	if got := capB2.waitForPrefix(t, "list:", 5*time.Second); got != wantB {
		t.Fatalf("plugin B list: got %q, want %q — exactly its own three keys, no more", got, wantB)
	}
	stopAndExpectClean(t, hB2)
}

// TestStorage_RestartPreservesData: §4, "перезапуск плагина сохраняет
// его данные" (lifecycle.md §8) — a second, entirely separate process
// with the SAME plugin identity reads a value it never itself wrote.
// The persistence lives in the shared store (standing in for the real
// backing ports.Storage, which Phase 13's Dispatcher will eventually
// wire up) — what's under test is that host/ipc don't do anything that
// would prevent a restarted instance from seeing its own prior state.
func TestStorage_RestartPreservesData(t *testing.T) {
	store := newSharedStorageStore()
	const id = "com.engram.test.storage-restart"

	h1, cap1 := runStorageMode(t, store, id, "write-secret", "persisted-value")
	if got := cap1.waitForPrefix(t, "write:", 5*time.Second); got != "write:OK" {
		t.Fatalf("first instance write: got %q, want write:OK", got)
	}
	stopAndExpectClean(t, h1)

	// A genuinely new process — new host, new PackageRoot/TempDir, same
	// plugin_id — never wrote anything itself.
	h2, cap2 := runStorageMode(t, store, id, "read-secret", "")
	if got := cap2.waitForPrefix(t, "read:", 5*time.Second); got != "read:persisted-value" {
		t.Fatalf("restarted instance read: got %q, want read:persisted-value", got)
	}
	stopAndExpectClean(t, h2)
}

// TestStorage_DeletingOnePluginDoesNotAffectAnother: §4, "удаление
// плагина не затрагивает данные другого" — plugin A deletes its own
// key; plugin B's identically-named key is untouched.
func TestStorage_DeletingOnePluginDoesNotAffectAnother(t *testing.T) {
	store := newSharedStorageStore()

	hA, capA := runStorageMode(t, store, "com.engram.test.storage-a", "write-secret", "value-a")
	if got := capA.waitForPrefix(t, "write:", 5*time.Second); got != "write:OK" {
		t.Fatalf("plugin A write: got %q, want write:OK", got)
	}
	stopAndExpectClean(t, hA)

	hB, capB := runStorageMode(t, store, "com.engram.test.storage-b", "write-secret", "value-b")
	if got := capB.waitForPrefix(t, "write:", 5*time.Second); got != "write:OK" {
		t.Fatalf("plugin B write: got %q, want write:OK", got)
	}
	stopAndExpectClean(t, hB)

	hADel, capADel := runStorageMode(t, store, "com.engram.test.storage-a", "delete-secret", "")
	if got := capADel.waitForPrefix(t, "delete:", 5*time.Second); got != "delete:OK" {
		t.Fatalf("plugin A delete: got %q, want delete:OK", got)
	}
	stopAndExpectClean(t, hADel)

	hB2, capB2 := runStorageMode(t, store, "com.engram.test.storage-b", "read-secret", "")
	if got := capB2.waitForPrefix(t, "read:", 5*time.Second); got != "read:value-b" {
		t.Fatalf("plugin B read after A's delete: got %q, want read:value-b — A's delete must not reach B's data", got)
	}
	stopAndExpectClean(t, hB2)
}
