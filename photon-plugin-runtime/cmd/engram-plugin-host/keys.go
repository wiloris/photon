package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/wiloris/photon-core/ports"
)

/*
 * Ключевой материал ENGRAM-хоста (ADR-068 п. 3).
 *
 * МАТЕРИАЛОВ РОВНО ДВА, и оба обязаны пережить перезапуск:
 *
 *   1. приватный ключ устройства Ed25519 (64 байта) — coreops.Config.DeviceKey.
 *      Без персистентности меняется идентичность устройства: identity.sign
 *      перестаёт сходиться, identity.public_key отдаёт другой ключ;
 *   2. симметричный ключ (32 байта), питающий DeriveKey. В coreops.Config его
 *      НЕТ — он живёт в замыкании композиции, и §11.1 прямо требует, чтобы
 *      исполнитель не знал его источника. Без персистентности плагин не
 *      расшифрует то, что зашифровал до перезапуска (§6).
 *
 * ПОЧЕМУ ДВА, А НЕ ОДИН. §6 показывает форму DeriveKey(ctx, deviceKey, …), и
 * ADR-068 не называет её запрещённой — он её не выбирает. Причины в п. 10.1 и
 * в разделе отвергнутых альтернатив: aesgcm.DeriveKey требует ровно 32 байта,
 * ключ Ed25519 — 64, и решение «какие именно 32» криптографическое, а не
 * техническое; плюс один секрет обслуживал бы и подпись, и шифрование.
 *
 * ПОЧЕМУ НЕ В bbolt: хранилище доступно исполнителю операций плагинов, и ключ
 * внутри защищаемого им хранилища — замок внутри запираемого шкафа (п. 3.4).
 *
 * ЭТО ИНТЕРИМ, названный интеримом (п. 3.5). Ключи лежат на диске в открытом
 * виде, защищённые только правами файла. ADR-021 п. 3 требует средств ОС, и
 * ADR-068 его не ослабляет: файл заменяется адаптером, как только у любого
 * клиента появится платформенное хранилище (критерий п. 3.6).
 */

const (
	deviceKeyFile = "device.key"
	masterKeyFile = "master.key"

	// Права файла ключа. Группе и остальным — ничего.
	keyFileMode os.FileMode = 0o600
)

// keyMaterial — то, что хост прочитал или породил при старте.
type keyMaterial struct {
	Device ports.PrivateKey
	Master ports.SymmetricKey
}

// loadOrCreateKeys читает ключи из каталога, а недостающие порождает.
//
// Порождение и чтение НЕ РАЗЛИЧАЮТСЯ вызывающим намеренно: единственное, что
// ему нужно знать, — ключи есть и они те же, что в прошлый раз. Разница между
// «первый запуск» и «последующий» наблюдаема по файлам, а не по возвращаемому
// значению.
func loadOrCreateKeys(ctx context.Context, dir string, id ports.Identity, cr ports.Crypto) (keyMaterial, error) {
	var km keyMaterial

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return km, fmt.Errorf("каталог ключей: %w", err)
	}

	devRaw, err := loadOrCreate(filepath.Join(dir, deviceKeyFile), func() ([]byte, error) {
		pair, genErr := id.Generate(ctx)
		if genErr != nil {
			return nil, genErr
		}
		return pair.Private, nil
	})
	if err != nil {
		return km, fmt.Errorf("ключ устройства: %w", err)
	}

	masterRaw, err := loadOrCreate(filepath.Join(dir, masterKeyFile), func() ([]byte, error) {
		key, genErr := cr.GenerateKey(ctx)
		if genErr != nil {
			return nil, genErr
		}
		return key, nil
	})
	if err != nil {
		return km, fmt.Errorf("мастер-ключ: %w", err)
	}

	km.Device = ports.PrivateKey(devRaw)
	km.Master = ports.SymmetricKey(masterRaw)
	return km, nil
}

/*
 * loadOrCreate — прочитать файл, а если его нет — породить и записать.
 *
 * Запись идёт с O_EXCL, и это не перестраховка: без него два процесса,
 * стартовавших одновременно на одном каталоге, молча затёрли бы ключ друг
 * друга, и половина уже зашифрованных данных стала бы нечитаемой. С O_EXCL
 * проигравший получает ErrExist и перечитывает файл победителя.
 *
 * Хранится base64, а не сырые байты: ключ иногда приходится посмотреть глазами
 * или перенести, и текстовый файл для этого честнее бинарного. На защиту это
 * не влияет — её обеспечивают права, а не формат.
 */
func loadOrCreate(path string, gen func() ([]byte, error)) ([]byte, error) {
	if raw, err := readKeyFile(path); err == nil {
		return raw, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	fresh, err := gen()
	if err != nil {
		return nil, fmt.Errorf("породить: %w", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, keyFileMode)
	switch {
	case errors.Is(err, os.ErrExist):
		// Кто-то успел раньше. Его ключ — действующий.
		return readKeyFile(path)
	case err != nil:
		return nil, fmt.Errorf("создать %s: %w", filepath.Base(path), err)
	}

	_, writeErr := f.WriteString(base64.StdEncoding.EncodeToString(fresh) + "\n")
	closeErr := f.Close()
	if writeErr != nil {
		return nil, fmt.Errorf("записать %s: %w", filepath.Base(path), writeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("закрыть %s: %w", filepath.Base(path), closeErr)
	}
	return fresh, nil
}

func readKeyFile(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Права проверяются при ЧТЕНИИ, а не только при создании: файл мог быть
	// принесён извне или испорчен chmod'ом, и молча им пользоваться нельзя.
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return nil, fmt.Errorf("%s доступен группе или остальным (%o), ожидалось %o",
			filepath.Base(path), perm, keyFileMode)
	}

	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("%s не разбирается как base64: %w", filepath.Base(path), err)
	}
	if len(decoded) == 0 {
		return nil, fmt.Errorf("%s пуст", filepath.Base(path))
	}
	return decoded, nil
}
