package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/wiloris/photon/photon-core/ports"
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
 *
 * ── ADR-069: СОЗДАНИЕ ИДЕНТИЧНОСТИ — ОПЕРАЦИЯ ЯВНАЯ ─────────────────────
 *
 * Два материала здесь живут по РАЗНЫМ правилам, и это не небрежность, а
 * граница принятого решения (ADR-069 §3):
 *
 *   device.key — обычный запуск его только ЧИТАЕТ. Нет файла — отказ.
 *                Создаёт его единственная явная операция, и повторить её на
 *                уже существующем ключе нельзя;
 *   master.key — ведёт себя как прежде, включая создание отсутствующего
 *                файла. ADR-069 его НЕ рассматривает: симметричный материал
 *                связан с нерешённым ADR-068 §10.1 (DeriveKey), и выводить
 *                его поведение по аналогии с device.key нельзя.
 *
 * Дефект, который это устраняет, назван в ADR-068 §10.4: хост не различал
 * «первичную инициализацию» и «потерю ключа» — обе выглядят как «файла нет» —
 * и молча выбирал первое. Идентичность устройства менялась без сигнала, а
 * доклад «подпись проверена» оставался правдой: другим ключом.
 *
 * ЧЕГО МЕХАНИЗМ НЕ ДЕЛАЕТ (ADR-069 §2): он не обнаруживает утрату. Отличить
 * «никогда не было» от «было и пропало» нельзя без нового persisted state, а
 * такое состояние удаляется тем же движением, что и ключ. Неоднозначность не
 * устранена — она передана вызывающему, который теперь обязан заявить о
 * создании явно.
 */

// errDeviceKeyExists — явная инициализация встретила существующую
// идентичность (ADR-069 §4.2). Отдельное значение нужно режиму
// -init-identity: «уже есть» и «испорчен» он сообщает разными кодами
// выхода (ADR-086 §4.1), а текст ошибки для этого не годится.
var errDeviceKeyExists = errors.New("инициализация не заменяет идентичность устройства")

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

// loadOrCreateKeys читает ключевой материал из каталога.
//
// initIdentity — ЗАЯВЛЕНИЕ ВЫЗЫВАЮЩЕГО о том, что идентичности ещё нет и её
// нужно создать (ADR-069 п. 4.1). Без него отсутствующий device.key —
// ошибка, а не повод породить новый.
//
// На master.key признак не влияет: его жизненный цикл ADR-069 не меняет
// (§3). Разное поведение двух соседних файлов выглядит непоследовательным
// ровно до того места, где записана причина, — см. шапку файла.
func loadOrCreateKeys(ctx context.Context, dir string, id ports.Identity, cr ports.Crypto, initIdentity bool) (keyMaterial, error) {
	var km keyMaterial

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return km, fmt.Errorf("каталог ключей: %w", err)
	}

	devRaw, err := loadDeviceKey(filepath.Join(dir, deviceKeyFile), initIdentity, func() ([]byte, error) {
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
 * loadDeviceKey — ключ устройства по правилам ADR-069.
 *
 * Четыре состояния, и ни одно не сводится к другому:
 *
 *	файл читается,   без флага → загрузить (обычный запуск);
 *	файл читается,   с флагом  → ОТКАЗ: идентичность уже есть (п. 4.2);
 *	файла нет,       без флага → ОТКАЗ: обычный запуск не создаёт (п. 4.1);
 *	файла нет,       с флагом  → создать.
 *
 * ПОРЯДОК ПРОВЕРОК СУЩЕСТВЕНЕН. Сначала readKeyFile, и только потом разбор
 * флага: испорченный ключ (не base64, пустой, права шире 0600) обязан
 * остаться отказом ДАЖЕ с флагом инициализации. Иначе флаг стал бы способом
 * обойти проверку целостности — «не читается, значит создам новый» (п. 4.2
 * последним абзацем).
 *
 * «Уже существует» и «испорчен» тоже не смешиваются: второй случай уходит
 * наверх своей ошибкой, называющей причину.
 */
func loadDeviceKey(path string, initialize bool, gen func() ([]byte, error)) ([]byte, error) {
	raw, err := readKeyFile(path)
	switch {
	case err == nil && initialize:
		return nil, fmt.Errorf("%s уже существует: %w", filepath.Base(path), errDeviceKeyExists)
	case err == nil:
		return raw, nil
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	case !initialize:
		// Здесь и был дефект ADR-068 §10.4: эта ветка порождала новый ключ.
		return nil, fmt.Errorf(
			"%s отсутствует: обычный запуск не создаёт идентичность устройства; "+
				"если её ещё не было — запустите с явной инициализацией",
			filepath.Base(path))
	}

	fresh, err := gen()
	if err != nil {
		return nil, fmt.Errorf("породить: %w", err)
	}

	// Проигравший гонку получает ОТКАЗ, а не ключ победителя: два
	// одновременных создания идентичности не могут оба быть первыми
	// (ADR-069 §5). Для master.key ниже выбран противоположный исход — там
	// это не создание идентичности, а продолжение прежнего поведения.
	switch err := writeKeyFile(path, fresh); {
	case errors.Is(err, os.ErrExist):
		return nil, fmt.Errorf(
			"%s создан другим процессом одновременно: инициализация не повторяется",
			filepath.Base(path))
	case err != nil:
		return nil, err
	}
	return fresh, nil
}

/*
 * loadOrCreate — прочитать файл, а если его нет — породить и записать.
 *
 * ОСТАЛСЯ ТОЛЬКО ДЛЯ master.key. Это прежнее поведение, сохранённое
 * сознательно: ADR-069 §3 ограничивает решение ключом устройства и прямо
 * запрещает выводить отсюда будущее поведение симметричного материала.
 *
 * Запись идёт с O_EXCL, и это не перестраховка: без него два процесса,
 * стартовавших одновременно на одном каталоге, молча затёрли бы ключ друг
 * друга, и половина уже зашифрованных данных стала бы нечитаемой. С O_EXCL
 * проигравший получает ErrExist и перечитывает файл победителя.
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

	switch err := writeKeyFile(path, fresh); {
	case errors.Is(err, os.ErrExist):
		// Кто-то успел раньше. Его ключ — действующий.
		return readKeyFile(path)
	case err != nil:
		return nil, err
	}
	return fresh, nil
}

/*
 * writeKeyFile — создать файл ключа с O_EXCL и правами keyFileMode.
 *
 * os.ErrExist возвращается вызывающему НЕОБЁРНУТЫМ по смыслу: что делать с
 * проигранной гонкой, решают по-разному device.key и master.key, и решение
 * это принадлежит им, а не записи.
 *
 * Хранится base64, а не сырые байты: ключ иногда приходится посмотреть глазами
 * или перенести, и текстовый файл для этого честнее бинарного. На защиту это
 * не влияет — её обеспечивают права, а не формат.
 */
func writeKeyFile(path string, key []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, keyFileMode)
	switch {
	case errors.Is(err, os.ErrExist):
		return err
	case err != nil:
		return fmt.Errorf("создать %s: %w", filepath.Base(path), err)
	}

	_, writeErr := f.WriteString(base64.StdEncoding.EncodeToString(key) + "\n")
	closeErr := f.Close()
	if writeErr != nil {
		return fmt.Errorf("записать %s: %w", filepath.Base(path), writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("закрыть %s: %w", filepath.Base(path), closeErr)
	}
	return nil
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
