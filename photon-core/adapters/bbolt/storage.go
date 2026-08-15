// Package bbolt реализует ports.Storage поверх go.etcd.io/bbolt.
// Первый адаптер Storage port (см. ROADMAP.md, Phase 2).
package bbolt

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	bolt "go.etcd.io/bbolt"

	"github.com/wiloris/photon-core/ports"
)

// metaBucket и versionKey — внутренние детали адаптера для отслеживания
// версии схемы. Не являются namespace в смысле ports.Storage: не
// проходят через Get/Put/ForEach, недоступны вызывающему коду порта.
var (
	metaBucket = []byte("_meta")
	versionKey = []byte("schema_version")
)

// Migration — шаг миграции схемы. Version — целевая версия, после
// применения которой схема считается находящейся на этой версии.
// Up получает сырую bbolt-транзакцию (не ports.WriteTx) — миграции
// содержат детали, специфичные для bbolt (напрямую создают bucket'ы),
// поэтому сознательно не абстрагированы через ports.
type Migration struct {
	Version int
	Up      func(tx *bolt.Tx) error
}

// Storage реализует ports.Storage.
type Storage struct {
	db         *bolt.DB
	migrations []Migration
}

var _ ports.Storage = (*Storage)(nil)

// New открывает (или создаёт) bbolt-базу по path. migrations — шаги
// миграции схемы, отсортированные или нет (сортируются внутри New).
// Версии обязаны быть последовательными без пропусков (1, 2, 3, ...) —
// New возвращает ошибку при обнаружении дыры или дубликата, чтобы
// забытая по ошибке миграция не осталась незамеченной молча.
func New(path string, migrations []Migration) (*Storage, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("bbolt: open %s: %w", path, err)
	}

	sorted := make([]Migration, len(migrations))
	copy(sorted, migrations)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Version < sorted[j].Version
	})

	for i, m := range sorted {
		want := i + 1
		if m.Version != want {
			db.Close()
			return nil, fmt.Errorf("bbolt: migrations must be sequential starting at 1 without gaps: expected version %d at position %d, got %d", want, i, m.Version)
		}
	}

	return &Storage{db: db, migrations: sorted}, nil
}

// View — см. ports.Storage. bbolt не поддерживает отмену транзакции
// через context на середине выполнения; ctx проверяется только перед
// стартом транзакции (best-effort).
func (s *Storage) View(ctx context.Context, fn func(tx ports.ReadTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.View(func(tx *bolt.Tx) error {
		return fn(&readTx{tx: tx})
	})
}

// Update — см. ports.Storage. Та же оговорка про context, что и View.
func (s *Storage) Update(ctx context.Context, fn func(tx ports.WriteTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return fn(&writeTx{readTx: &readTx{tx: tx}})
	})
}

// Migrate — см. ports.Storage. Идемпотентна: если текущая версия схемы
// уже >= targetVersion, это no-op. Применяет все зарегистрированные
// миграции с Version в диапазоне (текущая, targetVersion] по порядку.
func (s *Storage) Migrate(ctx context.Context, targetVersion int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(metaBucket)
		if err != nil {
			return fmt.Errorf("bbolt: create meta bucket: %w", err)
		}

		current := 0
		if v := meta.Get(versionKey); v != nil {
			current = int(binary.BigEndian.Uint64(v))
		}

		if current >= targetVersion {
			return nil
		}

		for _, m := range s.migrations {
			if m.Version <= current || m.Version > targetVersion {
				continue
			}
			if err := m.Up(tx); err != nil {
				return fmt.Errorf("bbolt: migration %d: %w", m.Version, err)
			}
			current = m.Version
		}

		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(current))
		return meta.Put(versionKey, buf)
	})
}

// Close — см. ports.Storage. ctx игнорируется: bbolt.DB.Close не
// принимает context и не выполняет сетевых или иных операций,
// которые имело бы смысл отменять.
func (s *Storage) Close(ctx context.Context) error {
	_ = ctx
	return s.db.Close()
}
