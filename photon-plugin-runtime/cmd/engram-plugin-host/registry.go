package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon/photon-core/adapters/bbolt"
	"github.com/wiloris/photon/photon-core/ports"
)

/*
 * Реестр установленных плагинов (ADR-068 п. 6) — МИНИМАЛЬНЫЙ.
 *
 * Два вида записей и ничего больше:
 *
 *	next_migration_version  →  монотонный счётчик
 *	plugin:<plugin_id>      →  назначенный номер версии миграции
 *
 * ЧЕМ ОН НЕ ЯВЛЯЕТСЯ (п. 6.3, перечислено, чтобы не вырос): ни версий
 * пакетов, ни источников и URL, ни проверки подписей, ни разрешения
 * зависимостей, ни политики обновления, ни хранения выданных прав. Ничего из
 * этого для миграции не нужно.
 *
 * Зачем счётчик, если номер можно взять как max+1: удаление строки не должно
 * освобождать номер. bbolt применяет миграции current+1..target, и
 * переиспользованный номер означал бы пропущенную миграцию у того, кто уже
 * обновился (п. 5.6). Счётчик даёт это свойство без кладбища записей.
 */

const (
	// registryNamespace принадлежит ХОСТУ и не пересекается с пространством
	// плагинов: их namespace начинаются с "plugin." (п. 4.4).
	registryNamespace = "engram.host.registry"

	// registrySchemaVersion — версия самого namespace реестра. Он мигрируется
	// первым и всегда: без него неоткуда узнать, какие ещё namespace нужны.
	registrySchemaVersion = 1

	counterKey    = "next_migration_version"
	pluginKeyPfx  = "plugin:"
	firstPluginID = registrySchemaVersion + 1 // номера плагинов идут после реестра
)

// pluginNamespace — имя namespace плагина (ADR-068 п. 4.4).
//
// plugin_id используется ДОСЛОВНО: он уже канонический —
// manifest/validator.go проверяет ^[a-z0-9]+(\.[a-z0-9][a-z0-9-]*)+$ и он
// используется как есть в ключах coreops, в permissions.Subject и в
// CallerInfo. Нормализация не вводится: преобразовывать нечего (п. 4.3).
func pluginNamespace(pluginID string) string { return "plugin." + pluginID }

// registryMigration — миграция самого реестра. Всегда первая в списке.
func registryMigration() bboltadapter.Migration {
	return bboltadapter.Migration{
		Version: registrySchemaVersion,
		Up: func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(registryNamespace))
			return err
		},
	}
}

// newBootStorage открывает хранилище, зная ОДНУ миграцию — сам реестр.
// Вынесено отдельно, потому что тем же открытием пользуются проверки
// инварианта п. 5.1: воспроизвести окно краха иначе нечем.
func newBootStorage(path string) (ports.Storage, error) {
	return bboltadapter.New(path, []bboltadapter.Migration{registryMigration()})
}

// installed — строка реестра.
type installed struct {
	PluginID string
	Version  int
}

// readRegistry читает реестр целиком, В ПОРЯДКЕ ВОЗРАСТАНИЯ версий.
//
// Порядок обязателен (п. 5.5): bbolt.Migrate обходит слайс как есть и не
// сортирует его, поэтому несортированный список пропустил бы миграции молча.
func readRegistry(ctx context.Context, st ports.Storage) ([]installed, int, error) {
	var (
		rows []installed
		next = firstPluginID
	)

	err := st.View(ctx, func(tx ports.ReadTx) error {
		if raw, err := tx.Get(registryNamespace, []byte(counterKey)); err == nil {
			n, convErr := strconv.Atoi(string(raw))
			if convErr != nil {
				return fmt.Errorf("счётчик версий повреждён: %q", raw)
			}
			next = n
		} else if !errors.Is(err, ports.ErrNotFound) {
			return err
		}

		return tx.ForEachPrefix(registryNamespace, []byte(pluginKeyPfx), func(k, v []byte) error {
			id := strings.TrimPrefix(string(k), pluginKeyPfx)
			n, convErr := strconv.Atoi(string(v))
			if convErr != nil {
				return fmt.Errorf("версия плагина %q повреждена: %q", id, v)
			}
			rows = append(rows, installed{PluginID: id, Version: n})
			return nil
		})
	})
	if err != nil {
		return nil, 0, err
	}

	sort.Slice(rows, func(i, j int) bool { return rows[i].Version < rows[j].Version })
	return rows, next, nil
}

// ensureInstalled регистрирует плагин, если его там ещё нет, и возвращает
// назначенный номер версии.
//
// Повторный вызов для уже установленного плагина — НЕ ошибка и не выдаёт
// нового номера: установка идемпотентна, как и миграция, которую она влечёт.
func ensureInstalled(ctx context.Context, st ports.Storage, pluginID string) (int, error) {
	var version int

	err := st.Update(ctx, func(tx ports.WriteTx) error {
		key := pluginKeyPfx + pluginID

		if raw, err := tx.Get(registryNamespace, []byte(key)); err == nil {
			n, convErr := strconv.Atoi(string(raw))
			if convErr != nil {
				return fmt.Errorf("версия плагина %q повреждена: %q", pluginID, raw)
			}
			version = n
			return nil
		} else if !errors.Is(err, ports.ErrNotFound) {
			return err
		}

		next := firstPluginID
		if raw, err := tx.Get(registryNamespace, []byte(counterKey)); err == nil {
			n, convErr := strconv.Atoi(string(raw))
			if convErr != nil {
				return fmt.Errorf("счётчик версий повреждён: %q", raw)
			}
			next = n
		} else if !errors.Is(err, ports.ErrNotFound) {
			return err
		}

		version = next

		// Счётчик и строка пишутся ОДНОЙ транзакцией: иначе крах между ними
		// либо выдал бы двум плагинам один номер, либо потерял бы номер
		// навсегда.
		if err := tx.Put(registryNamespace, []byte(key), []byte(strconv.Itoa(version))); err != nil {
			return err
		}
		return tx.Put(registryNamespace, []byte(counterKey), []byte(strconv.Itoa(version+1)))
	})
	if err != nil {
		return 0, err
	}
	return version, nil
}

// migrationsFor строит список миграций: реестр первым, затем namespace
// плагинов по возрастанию версий.
func migrationsFor(rows []installed) ([]bboltadapter.Migration, int) {
	out := []bboltadapter.Migration{registryMigration()}
	target := registrySchemaVersion

	for _, r := range rows {
		ns := pluginNamespace(r.PluginID)
		out = append(out, bboltadapter.Migration{
			Version: r.Version,
			Up: func(tx *bolt.Tx) error {
				_, err := tx.CreateBucketIfNotExists([]byte(ns))
				return err
			},
		})
		if r.Version > target {
			target = r.Version
		}
	}
	return out, target
}

// containsPlugin — установлен ли плагин по реестру.
func containsPlugin(rows []installed, id string) bool {
	for _, r := range rows {
		if r.PluginID == id {
			return true
		}
	}
	return false
}
