// Package bbolt реализует ports.Storage поверх go.etcd.io/bbolt.
// Первый адаптер Storage port (см. ROADMAP.md, Phase 2).
package bbolt

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

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

// OpenTimeout — сколько New ждёт файл, занятый другим владельцем.
//
// Законный держатель файла короткий: хост плагинов открывает engram.db на
// время миграции реестра и одного запуска, CLI — на время команды. Пять
// секунд переживают такого держателя с запасом и при этом остаются
// ошибкой, которую видно: Photon_Create — синхронный вызов, и ждать дольше
// значит держать вызывающего без ответа ради состояния, которое чаще всего
// означает дефект (второй инстанс на том же файле).
const OpenTimeout = 5 * time.Second

// openTimeout — действующее значение; переменная только ради тестов пакета,
// которым незачем ждать пять секунд, чтобы проверить сам факт отказа.
var openTimeout = OpenTimeout

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
//
// Открытие ограничено по времени (openTimeout). bbolt берёт эксклюзивный
// flock на файл, и с Timeout = 0 (умолчание, nil-опции) ждёт занятый файл
// БЕСКОНЕЧНО — второй инстанс того же процесса или чужой процесс на том же
// файле подвешивали вызывающего навсегда. ADR-064 п. 1.6 обещает на
// «блокировку другим процессом» отказ создания, а не ожидание; истёкший
// таймаут возвращается ошибкой, которая оборачивает bolt.ErrTimeout.
func New(path string, migrations []Migration) (*Storage, error) {
	opts := *bolt.DefaultOptions // прочие умолчания — как при nil-опциях
	opts.Timeout = openTimeout
	db, err := bolt.Open(path, 0600, &opts)
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return nil, fmt.Errorf("bbolt: open %s: file is locked by another holder, gave up after %v: %w", path, openTimeout, err)
		}
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
