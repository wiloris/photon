package bbolt

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/wiloris/photon/photon-core/ports"
)

func newTestStorage(t *testing.T, migrations []Migration) *Storage {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := New(path, migrations)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// createNamespaceMigration — тестовая миграция, создающая один bucket.
func createNamespaceMigration(version int, namespace string) Migration {
	return Migration{
		Version: version,
		Up: func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(namespace))
			return err
		},
	}
}

func TestPutGetRoundtrip(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t, []Migration{createNamespaceMigration(1, "test.ns")})

	if err := s.Migrate(ctx, 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	err := s.Update(ctx, func(tx ports.WriteTx) error {
		return tx.Put("test.ns", []byte("k1"), []byte("v1"))
	})
	if err != nil {
		t.Fatalf("Update/Put: %v", err)
	}

	var got []byte
	err = s.View(ctx, func(tx ports.ReadTx) error {
		v, err := tx.Get("test.ns", []byte("k1"))
		got = v
		return err
	})
	if err != nil {
		t.Fatalf("View/Get: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("got %q, want %q", got, "v1")
	}
}

func TestGetMissingKeyReturnsErrNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t, []Migration{createNamespaceMigration(1, "test.ns")})
	if err := s.Migrate(ctx, 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	err := s.View(ctx, func(tx ports.ReadTx) error {
		_, err := tx.Get("test.ns", []byte("missing"))
		return err
	})
	if !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestNamespaceNotPreparedByMigrateReturnsErrNamespaceNotFound(t *testing.T) {
	ctx := context.Background()
	// Ни одной миграции не зарегистрировано — namespace "test.ns"
	// никогда не создаётся.
	s := newTestStorage(t, nil)

	err := s.View(ctx, func(tx ports.ReadTx) error {
		_, err := tx.Get("test.ns", []byte("k1"))
		return err
	})
	if !errors.Is(err, ports.ErrNamespaceNotFound) {
		t.Fatalf("View/Get: got %v, want ErrNamespaceNotFound", err)
	}

	err = s.Update(ctx, func(tx ports.WriteTx) error {
		return tx.Put("test.ns", []byte("k1"), []byte("v1"))
	})
	if !errors.Is(err, ports.ErrNamespaceNotFound) {
		t.Fatalf("Update/Put: got %v, want ErrNamespaceNotFound", err)
	}
}

func TestPortDoesNotCreateNamespaceImplicitly(t *testing.T) {
	// Прямая проверка ADR-010: Put не должен тихо создавать namespace,
	// даже если это было бы удобно.
	ctx := context.Background()
	s := newTestStorage(t, nil)

	err := s.Update(ctx, func(tx ports.WriteTx) error {
		return tx.Put("never.migrated", []byte("k"), []byte("v"))
	})
	if !errors.Is(err, ports.ErrNamespaceNotFound) {
		t.Fatalf("got %v, want ErrNamespaceNotFound (namespace must not be created implicitly)", err)
	}
}

func TestUpdateRollsBackOnError(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t, []Migration{createNamespaceMigration(1, "test.ns")})
	if err := s.Migrate(ctx, 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	sentinel := errors.New("boom")
	err := s.Update(ctx, func(tx ports.WriteTx) error {
		if err := tx.Put("test.ns", []byte("k1"), []byte("v1")); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want sentinel error", err)
	}

	// Изменение должно быть откачено — ключа быть не должно.
	err = s.View(ctx, func(tx ports.ReadTx) error {
		_, err := tx.Get("test.ns", []byte("k1"))
		return err
	})
	if !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("after rollback: got %v, want ErrNotFound", err)
	}
}

func TestDeleteMissingKeyIsNotError(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t, []Migration{createNamespaceMigration(1, "test.ns")})
	if err := s.Migrate(ctx, 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	err := s.Update(ctx, func(tx ports.WriteTx) error {
		return tx.Delete("test.ns", []byte("never-existed"))
	})
	if err != nil {
		t.Fatalf("Delete of missing key should not error, got %v", err)
	}
}

func TestForEachPrefix(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t, []Migration{createNamespaceMigration(1, "test.ns")})
	if err := s.Migrate(ctx, 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	seed := map[string]string{
		"user:1":    "a",
		"user:2":    "b",
		"session:1": "c",
	}
	err := s.Update(ctx, func(tx ports.WriteTx) error {
		for k, v := range seed {
			if err := tx.Put("test.ns", []byte(k), []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	found := map[string]string{}
	err = s.View(ctx, func(tx ports.ReadTx) error {
		return tx.ForEachPrefix("test.ns", []byte("user:"), func(k, v []byte) error {
			found[string(k)] = string(v)
			return nil
		})
	})
	if err != nil {
		t.Fatalf("ForEachPrefix: %v", err)
	}
	if len(found) != 2 {
		t.Fatalf("got %d entries, want 2 (session:1 must be excluded): %v", len(found), found)
	}
	if found["user:1"] != "a" || found["user:2"] != "b" {
		t.Fatalf("unexpected contents: %v", found)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	applied := 0
	migrations := []Migration{
		{
			Version: 1,
			Up: func(tx *bolt.Tx) error {
				applied++
				_, err := tx.CreateBucketIfNotExists([]byte("test.ns"))
				return err
			},
		},
	}
	s := newTestStorage(t, migrations)

	if err := s.Migrate(ctx, 1); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if err := s.Migrate(ctx, 1); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if applied != 1 {
		t.Fatalf("migration applied %d times, want 1 (Migrate must be idempotent)", applied)
	}
}

func TestMigrateAppliesInVersionOrder(t *testing.T) {
	ctx := context.Background()
	var order []int
	migrations := []Migration{
		{Version: 3, Up: func(tx *bolt.Tx) error { order = append(order, 3); return nil }},
		{Version: 1, Up: func(tx *bolt.Tx) error { order = append(order, 1); return nil }},
		{Version: 2, Up: func(tx *bolt.Tx) error { order = append(order, 2); return nil }},
	}
	s := newTestStorage(t, migrations)

	if err := s.Migrate(ctx, 3); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("migrations applied out of order: %v", order)
	}
}

func TestMigratePartialThenContinue(t *testing.T) {
	ctx := context.Background()
	var order []int
	migrations := []Migration{
		{Version: 1, Up: func(tx *bolt.Tx) error { order = append(order, 1); return nil }},
		{Version: 2, Up: func(tx *bolt.Tx) error { order = append(order, 2); return nil }},
	}
	s := newTestStorage(t, migrations)

	if err := s.Migrate(ctx, 1); err != nil {
		t.Fatalf("Migrate to 1: %v", err)
	}
	if err := s.Migrate(ctx, 2); err != nil {
		t.Fatalf("Migrate to 2: %v", err)
	}
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("unexpected migration order: %v", order)
	}
}

func TestCloseThenOperationErrors(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := New(path, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err = s.View(ctx, func(tx ports.ReadTx) error { return nil })
	if err == nil {
		t.Fatal("View after Close должен возвращать ошибку, а не паниковать")
	}
}

func TestValueSurvivesAfterTransaction(t *testing.T) {
	// Регрессия на конкретный bbolt-нюанс: []byte, возвращаемый
	// bbolt.Bucket.Get, невалиден после закрытия транзакции — адаптер
	// обязан копировать данные (см. readTx.Get).
	ctx := context.Background()
	s := newTestStorage(t, []Migration{createNamespaceMigration(1, "test.ns")})
	if err := s.Migrate(ctx, 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := s.Update(ctx, func(tx ports.WriteTx) error {
		return tx.Put("test.ns", []byte("k1"), []byte("original-value"))
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	var got []byte
	if err := s.View(ctx, func(tx ports.ReadTx) error {
		v, err := tx.Get("test.ns", []byte("k1"))
		got = v
		return err
	}); err != nil {
		t.Fatalf("View: %v", err)
	}

	// Провоцируем ещё одну транзакцию, которая могла бы переиспользовать
	// внутренние буферы bbolt, если бы got не был скопирован.
	_ = s.Update(ctx, func(tx ports.WriteTx) error {
		return tx.Put("test.ns", []byte("k2"), []byte("filler-data-to-touch-pages"))
	})

	if string(got) != "original-value" {
		t.Fatalf("value corrupted after transaction ended: got %q", got)
	}
}

func TestForEachOnEmptyNamespace(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t, []Migration{createNamespaceMigration(1, "test.ns")})
	if err := s.Migrate(ctx, 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	calls := 0
	err := s.View(ctx, func(tx ports.ReadTx) error {
		return tx.ForEach("test.ns", func(key, value []byte) error {
			calls++
			return nil
		})
	})
	if err != nil {
		t.Fatalf("ForEach on empty namespace: %v", err)
	}
	if calls != 0 {
		t.Fatalf("ForEach on empty namespace called fn %d times, want 0", calls)
	}
}

func TestGetAfterDeleteReturnsErrNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t, []Migration{createNamespaceMigration(1, "test.ns")})
	if err := s.Migrate(ctx, 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	if err := s.Update(ctx, func(tx ports.WriteTx) error {
		return tx.Put("test.ns", []byte("k"), []byte("v"))
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := s.Update(ctx, func(tx ports.WriteTx) error {
		return tx.Delete("test.ns", []byte("k"))
	}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	err := s.View(ctx, func(tx ports.ReadTx) error {
		_, err := tx.Get("test.ns", []byte("k"))
		return err
	})
	if !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("Get after Delete: expected ErrNotFound, got %v", err)
	}
}

func TestMigrateBeyondLastRegisteredVersion(t *testing.T) {
	// Если зарегистрированы миграции только до версии 3, а вызван
	// Migrate(ctx, 100) — итоговая записанная версия схемы должна
	// остаться 3 (реальность), а не лживо стать 100.
	ctx := context.Background()
	migrations := []Migration{
		createNamespaceMigration(1, "ns1"),
		createNamespaceMigration(2, "ns2"),
		createNamespaceMigration(3, "ns3"),
	}
	s := newTestStorage(t, migrations)

	if err := s.Migrate(ctx, 100); err != nil {
		t.Fatalf("Migrate(ctx, 100): %v", err)
	}

	err := s.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(metaBucket)
		if meta == nil {
			t.Fatal("_meta bucket does not exist after Migrate")
		}
		v := meta.Get(versionKey)
		if v == nil {
			t.Fatal("schema_version key does not exist after Migrate")
		}
		got := int(binary.BigEndian.Uint64(v))
		if got != 3 {
			t.Fatalf("schema_version = %d, want 3 (highest actually-applied migration, not the requested target)", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verifying schema_version: %v", err)
	}
}

func TestNewRejectsGapInMigrationVersions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	_, err := New(path, []Migration{
		createNamespaceMigration(1, "ns1"),
		createNamespaceMigration(3, "ns3"), // дыра: версии 2 нет
	})
	if err == nil {
		t.Fatal("New: expected error for gap in migration versions (1, 3), got nil")
	}
}

func TestNewRejectsDuplicateMigrationVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	_, err := New(path, []Migration{
		createNamespaceMigration(1, "ns1"),
		createNamespaceMigration(1, "ns1-again"), // дубликат версии 1
	})
	if err == nil {
		t.Fatal("New: expected error for duplicate migration version 1, got nil")
	}
}

func TestViewRespectsCancelledContext(t *testing.T) {
	s := newTestStorage(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.View(ctx, func(tx ports.ReadTx) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("View with cancelled context: expected context.Canceled, got %v", err)
	}
}

func TestUpdateRespectsCancelledContext(t *testing.T) {
	s := newTestStorage(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.Update(ctx, func(tx ports.WriteTx) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Update with cancelled context: expected context.Canceled, got %v", err)
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
