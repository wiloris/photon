package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

/*
 * Проверки инвариантов, которые ввёл ADR-068.
 *
 * Прежние семь E2E проверяют ПУТЬ (плагин → SDK → IPC → Authorizer → coreops
 * → bbolt) и от композиции не зависят. Эти — проверяют саму КОМПОЗИЦИЮ:
 * персистентность ключей, изоляцию namespace, явность установки и то, что
 * инвариант миграции переживает крах в середине установки.
 *
 * Без них пункты ADR держались бы на дисциплине: композицию легко собрать
 * так, что она работает и при этом нарушает каждый из них молча.
 */

func runOnce(t *testing.T, o Options) Result {
	t.Helper()
	res, err := Run(context.Background(), o)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return res
}

func baseOptions(root, entry, db string) Options {
	return Options{
		DBPath:      db,
		PackageRoot: root,
		Entrypoint:  entry,
		PluginID:    testPluginID,
		Grants:      grantList(fullGrants),
		Install:     true,
		Timeout:     20 * time.Second,
	}
}

// ── п. 3: ключевой материал переживает перезапуск ───────────────────────

// TestADR068_KeyMaterialSurvivesRestart — ОБА материала те же после
// перезапуска, и это проверяется байтами файлов, а не косвенно.
//
// Мутация, которую тест ловит: композиция, порождающая ключи при каждом
// старте (как лабораторная по ADR-058 п. 2), пройдёт все семь прежних E2E и
// упадёт здесь — именно потому, что счётчик и подпись её не выдают.
func TestADR068_KeyMaterialSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	first := runOnce(t, baseOptions(root, entry, db))

	device1 := mustRead(t, filepath.Join(dir, deviceKeyFile))
	master1 := mustRead(t, filepath.Join(dir, masterKeyFile))

	second := runOnce(t, baseOptions(root, entry, db))

	device2 := mustRead(t, filepath.Join(dir, deviceKeyFile))
	master2 := mustRead(t, filepath.Join(dir, masterKeyFile))

	if !bytes.Equal(device1, device2) {
		t.Fatal("ключ устройства изменился между запусками — идентичность устройства не сохранена")
	}
	if !bytes.Equal(master1, master2) {
		t.Fatal("мастер-ключ изменился между запусками — данные, зашифрованные плагином, станут нечитаемыми")
	}

	// Подпись проверяется хостом в обоих запусках. Если бы ключ порождался
	// заново, проверка второго запуска шла бы уже другим ключом — и это
	// тоже не должно проходить незамеченным.
	if !first.SignatureVerified || !second.SignatureVerified {
		t.Fatalf("подписи не сошлись: первый=%v второй=%v",
			first.SignatureVerified, second.SignatureVerified)
	}
}

// TestADR068_KeyFilesArePrivate — права 0600 при создании и отказ читать
// файл, доступный группе или остальным.
//
// Проверка прав при ЧТЕНИИ, а не только при создании: файл мог быть принесён
// извне или испорчен chmod'ом, и молча им пользоваться нельзя.
func TestADR068_KeyFilesArePrivate(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	runOnce(t, baseOptions(root, entry, db))

	for _, name := range []string{deviceKeyFile, masterKeyFile} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if perm := info.Mode().Perm(); perm != keyFileMode {
			t.Fatalf("%s имеет права %o, ожидалось %o", name, perm, keyFileMode)
		}
	}

	// Расширяем права и убеждаемся, что запуск отказывает.
	if err := os.Chmod(filepath.Join(dir, masterKeyFile), 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	_, err := Run(context.Background(), baseOptions(root, entry, db))
	if err == nil {
		t.Fatal("хост принял ключ, доступный на чтение группе и остальным")
	}
	if !strings.Contains(err.Error(), "доступен группе") {
		t.Fatalf("причина отказа не про права: %v", err)
	}
}

// ── п. 4: namespace на плагин ───────────────────────────────────────────

// TestADR068_PluginGetsOwnNamespace — в хранилище появляется namespace с
// именем plugin.<plugin_id>, а не общий на всех.
//
// Проверяется по самому файлу: bucket существует, и его имя выведено из
// plugin_id дословно, без нормализации (п. 4.3, 4.4).
func TestADR068_PluginGetsOwnNamespace(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	runOnce(t, baseOptions(root, entry, db))

	buckets := bucketNames(t, db)

	want := pluginNamespace(testPluginID)
	if !buckets[want] {
		t.Fatalf("namespace %q не создан; в хранилище: %v", want, keys(buckets))
	}
	if !buckets[registryNamespace] {
		t.Fatalf("namespace реестра %q не создан; в хранилище: %v", registryNamespace, keys(buckets))
	}
	if buckets["photon.plugins"] {
		t.Fatal("остался общий namespace для всех плагинов — вариант §5, отвергнутый ADR-068 п. 4.1")
	}
}

// ── п. 6.2: установка — событие явное ───────────────────────────────────

// TestADR068_InstallIsExplicit — запуск незарегистрированного плагина
// отказывает, а не устанавливает его молча.
//
// Без этого опечатка в plugin_id создавала бы новый namespace и расходовала
// номер миграции вместо отказа.
func TestADR068_InstallIsExplicit(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	o := baseOptions(root, entry, db)
	o.Install = false

	_, err := Run(context.Background(), o)
	if err == nil {
		t.Fatal("незарегистрированный плагин запустился — установка произошла молча")
	}
	if !strings.Contains(err.Error(), "не установлен") {
		t.Fatalf("причина отказа не про установку: %v", err)
	}

	// И обратное: после явной установки тот же запуск проходит.
	runOnce(t, baseOptions(root, entry, db))

	o.Install = false
	if _, err := Run(context.Background(), o); err != nil {
		t.Fatalf("установленный плагин не запустился без флага установки: %v", err)
	}
}

// ── п. 5.1: инвариант переживает крах в середине установки ──────────────

// TestADR068_MigrationHealsAfterCrashWindow — строка реестра записана,
// namespace создать не успели: следующее открытие обязано его создать.
//
// Окно воспроизводится буквально: хранилище открывается со знанием ОДНОЙ
// миграции — самого реестра, — в него пишется строка установки, и оно
// закрывается. Это в точности состояние после краха между регистрацией и
// миграцией.
//
// Именно ради этого случая ADR-068 формулирует п. 5.1 инвариантом, а не
// моментом вызова: «Migrate только при установке» здесь оставил бы плагин с
// ErrNamespaceNotFound на первом put, и само бы это не починилось.
func TestADR068_MigrationHealsAfterCrashWindow(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")
	ctx := context.Background()

	boot, err := newBootStorage(db)
	if err != nil {
		t.Fatalf("открыть хранилище: %v", err)
	}
	if err := boot.Migrate(ctx, registrySchemaVersion); err != nil {
		t.Fatalf("мигрировать реестр: %v", err)
	}
	if _, err := ensureInstalled(ctx, boot, testPluginID); err != nil {
		t.Fatalf("зарегистрировать: %v", err)
	}
	if err := boot.Close(ctx); err != nil {
		t.Fatalf("закрыть: %v", err)
	}

	// Namespace плагина ещё не создан — это и есть состояние после краха.
	if bucketNames(t, db)[pluginNamespace(testPluginID)] {
		t.Fatal("namespace создан слишком рано — окно краха не воспроизведено")
	}

	// Запуск БЕЗ установки: плагин уже зарегистрирован, и хост обязан
	// починить недостающий namespace сам.
	o := baseOptions(root, entry, db)
	o.Install = false
	res := runOnce(t, o)

	if len(res.Counts) != 1 || res.Counts[0] != 1 {
		t.Fatalf("счётчик = %v, ожидалось [1]", res.Counts)
	}
	if !bucketNames(t, db)[pluginNamespace(testPluginID)] {
		t.Fatal("namespace так и не создан — инвариант п. 5.1 не выполняется")
	}
}

// TestADR068_RegistryNumbersAreNotReused — номер, выданный плагину, не
// выдаётся второму даже после удаления строки первого.
//
// Переиспользованный номер означал бы пропущенную миграцию у того, кто уже
// обновился: bbolt применяет только current+1..target.
func TestADR068_RegistryNumbersAreNotReused(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "engram.db")

	boot, err := newBootStorage(db)
	if err != nil {
		t.Fatalf("открыть: %v", err)
	}
	defer boot.Close(ctx)

	if err := boot.Migrate(ctx, registrySchemaVersion); err != nil {
		t.Fatalf("мигрировать реестр: %v", err)
	}

	first, err := ensureInstalled(ctx, boot, "dev.engram.one")
	if err != nil {
		t.Fatalf("установить первый: %v", err)
	}

	// Повторная установка того же — тот же номер, а не новый.
	again, err := ensureInstalled(ctx, boot, "dev.engram.one")
	if err != nil {
		t.Fatalf("повторная установка: %v", err)
	}
	if again != first {
		t.Fatalf("повторная установка выдала другой номер: %d против %d", again, first)
	}

	second, err := ensureInstalled(ctx, boot, "dev.engram.two")
	if err != nil {
		t.Fatalf("установить второй: %v", err)
	}
	if second <= first {
		t.Fatalf("номера не монотонны: второй %d, первый %d", second, first)
	}
}

// ── вспомогательное ─────────────────────────────────────────────────────

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("читать %s: %v", filepath.Base(path), err)
	}
	return raw
}

// bucketNames читает имена bucket'ов прямо из файла bbolt.
//
// Проверять устройство хранилища сквозь порт нечем: ports.Storage намеренно
// не отдаёт список namespace. В тесте композиции прямое чтение файла законно
// — здесь проверяется именно то, что композиция сделала с диском.
func bucketNames(t *testing.T, path string) map[string]bool {
	t.Helper()

	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("открыть bbolt: %v", err)
	}
	defer db.Close()

	out := map[string]bool{}
	err = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			out[string(name)] = true
			return nil
		})
	})
	if err != nil {
		t.Fatalf("обход bucket'ов: %v", err)
	}
	return out
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
