package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	ed25519adapter "github.com/wiloris/photon-core/adapters/ed25519"
)

// Коды выхода режима -init-identity (ADR-086 §4.1).
const (
	initIdentityCreated = 0
	initIdentityFailed  = 1
	initIdentityExists  = 3
)

/*
 * runInitIdentity — режим -init-identity: создать device.key и завершиться.
 *
 * Ключ создаётся ТОЙ ЖЕ функцией, что и под -init (loadDeviceKey), — со всеми
 * правилами ADR-069: O_EXCL, 0600, отказ при существующем и при испорченном
 * ключе. Своей копии этих правил здесь нет, и потому они не могут разойтись.
 *
 * Чего режим НЕ делает, и это граница решения, а не экономия:
 *   - не открывает хранилище и не создаёт engram.db;
 *   - не запускает плагин;
 *   - не трогает master.key — его жизненный цикл вне ADR-069 и ADR-086
 *     (создаётся, как прежде, первым запуском плагина).
 */
func runInitIdentity(keyDir string) int {
	err := initIdentity(context.Background(), keyDir)
	switch {
	case err == nil:
		fmt.Println("идентичность устройства создана:", filepath.Join(keyDir, deviceKeyFile))
		return initIdentityCreated
	case errors.Is(err, errDeviceKeyExists):
		fmt.Fprintln(os.Stderr, err)
		return initIdentityExists
	default:
		fmt.Fprintf(os.Stderr, "идентичность устройства: %v\n", err)
		return initIdentityFailed
	}
}

func initIdentity(ctx context.Context, keyDir string) error {
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		return fmt.Errorf("каталог ключей: %w", err)
	}
	id := ed25519adapter.New()
	_, err := loadDeviceKey(filepath.Join(keyDir, deviceKeyFile), true, func() ([]byte, error) {
		pair, genErr := id.Generate(ctx)
		if genErr != nil {
			return nil, genErr
		}
		return pair.Private, nil
	})
	return err
}
