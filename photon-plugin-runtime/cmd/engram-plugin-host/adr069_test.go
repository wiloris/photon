package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

/*
 * Проверки контракта ADR-069: создание идентичности устройства — операция
 * ЯВНАЯ, а утрата device.key не порождает новую идентичность.
 *
 * Дефект, который они закрывают (ADR-068 §10.4), был тихим по природе: хост
 * работал, возвращал код 0 и докладывал «подпись проверена» — другим ключом.
 * Поэтому ни одна проверка здесь не ограничивается тем, что запуск
 * завершился ошибкой: каждая смотрит на ФАЙЛЫ. «Отказали, но всё-таки
 * создали» — ровно то поведение, ради которого это пишется, и текст ошибки
 * его не выдаёт.
 *
 * Про master.key здесь нет ни одной проверки, и это не пропуск: ADR-069 §3
 * ограничивает решение ключом устройства и запрещает выводить отсюда
 * поведение симметричного материала.
 */

// runOptions — опции запуска, каждый раз свежие.
func runOptions(root, entry, db string, initIdentity bool) Options {
	o := baseOptions(root, entry, db)
	o.InitIdentity = initIdentity
	return o
}

func deviceKeyPath(dir string) string { return filepath.Join(dir, deviceKeyFile) }

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	t.Fatalf("stat %s: %v", path, err)
	return false
}

// ── T1. Обычный запуск без device.key ───────────────────────────────────

// TestADR069_PlainRunWithoutDeviceKeyRefuses — отказ, И НИЧЕГО НЕ СОЗДАНО.
//
// Вторая половина существеннее первой: до ADR-069 этот запуск проходил
// успешно, породив новую идентичность. Проверка только на `err != nil`
// прошла бы и на мутации «отказать, но сначала создать ключ».
func TestADR069_PlainRunWithoutDeviceKeyRefuses(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	_, err := Run(context.Background(), runOptions(root, entry, db, false))
	if err == nil {
		t.Fatal("обычный запуск без device.key прошёл — идентичность создана молча")
	}
	if !strings.Contains(err.Error(), deviceKeyFile) {
		t.Fatalf("отказ не называет отсутствующий файл: %v", err)
	}

	if exists(t, deviceKeyPath(dir)) {
		t.Fatal("device.key создан несмотря на отказ")
	}
	// Отказ произошёл ДО того, как появилось хоть какое-то состояние:
	// ключи читаются раньше открытия хранилища.
	if exists(t, db) {
		t.Fatal("хранилище создано несмотря на отказ")
	}
}

// ── T2. Явная инициализация на пустом каталоге ──────────────────────────

// TestADR069_InitCreatesDeviceKey — ключ создан, права ровно 0600.
func TestADR069_InitCreatesDeviceKey(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	res, err := Run(context.Background(), runOptions(root, entry, db, true))
	if err != nil {
		t.Fatalf("инициализация: %v", err)
	}
	if len(res.Counts) != 1 || res.Counts[0] != 1 {
		t.Fatalf("счётчик = %v, ожидалось [1]", res.Counts)
	}

	info, err := os.Stat(deviceKeyPath(dir))
	if err != nil {
		t.Fatalf("device.key не создан: %v", err)
	}
	if perm := info.Mode().Perm(); perm != keyFileMode {
		t.Fatalf("device.key имеет права %o, ожидалось %o", perm, keyFileMode)
	}
}

// ── T3. Обычный запуск после инициализации ──────────────────────────────

// TestADR069_PlainRunAfterInitUsesSameKey — тот же ключ побайтово.
//
// Сравнение байтов, а не «запуск прошёл»: регенерация тоже даёт успешный
// запуск, и именно этим дефект и был незаметен.
func TestADR069_PlainRunAfterInitUsesSameKey(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	if _, err := Run(context.Background(), runOptions(root, entry, db, true)); err != nil {
		t.Fatalf("инициализация: %v", err)
	}
	before := mustRead(t, deviceKeyPath(dir))

	res, err := Run(context.Background(), runOptions(root, entry, db, false))
	if err != nil {
		t.Fatalf("обычный запуск после инициализации: %v", err)
	}
	if len(res.Counts) != 1 || res.Counts[0] != 2 {
		t.Fatalf("счётчик = %v, ожидалось [2] — состояние не сохранилось", res.Counts)
	}
	if !res.SignatureVerified {
		t.Fatal("подпись не сошлась — запуск идёт другим ключом")
	}

	if after := mustRead(t, deviceKeyPath(dir)); !bytes.Equal(before, after) {
		t.Fatal("device.key изменился при обычном запуске")
	}
}

// ── T4. Утрата созданного ключа ─────────────────────────────────────────

// TestADR069_DeletedDeviceKeyIsNotRegenerated — отказ, новый ключ НЕ
// появился, хранилище НЕ тронуто.
//
// Последнее проверяется побайтово: «отказ заодно прибрался» — отдельная
// мутация, и она нарушала бы п. 4.3 молча.
func TestADR069_DeletedDeviceKeyIsNotRegenerated(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	if _, err := Run(context.Background(), runOptions(root, entry, db, true)); err != nil {
		t.Fatalf("инициализация: %v", err)
	}

	storageBefore := mustRead(t, db)
	masterBefore := mustRead(t, filepath.Join(dir, masterKeyFile))

	if err := os.Remove(deviceKeyPath(dir)); err != nil {
		t.Fatalf("удалить device.key: %v", err)
	}

	_, err := Run(context.Background(), runOptions(root, entry, db, false))
	if err == nil {
		t.Fatal("запуск после утраты device.key прошёл — идентичность подменена молча")
	}
	if exists(t, deviceKeyPath(dir)) {
		t.Fatal("device.key порождён заново после утраты")
	}

	if !bytes.Equal(storageBefore, mustRead(t, db)) {
		t.Fatal("хранилище изменилось при отказе — plugin storage не должен трогаться (п. 4.3)")
	}
	if !bytes.Equal(masterBefore, mustRead(t, filepath.Join(dir, masterKeyFile))) {
		t.Fatal("master.key изменился при отказе")
	}
}

// ── T5. Повторная инициализация ─────────────────────────────────────────

// TestADR069_InitOnExistingKeyRefuses — отказ, байты не изменились.
//
// Это граница между «создать» и «ensure» (ADR-069 §5). Мутация, которую
// тест ловит: инициализация, молча принимающая существующий ключ, — она
// прошла бы все остальные проверки этого файла.
func TestADR069_InitOnExistingKeyRefuses(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	if _, err := Run(context.Background(), runOptions(root, entry, db, true)); err != nil {
		t.Fatalf("инициализация: %v", err)
	}
	before := mustRead(t, deviceKeyPath(dir))

	_, err := Run(context.Background(), runOptions(root, entry, db, true))
	if err == nil {
		t.Fatal("повторная инициализация прошла — операция создания ведёт себя как ensure")
	}
	if !strings.Contains(err.Error(), "уже существует") {
		t.Fatalf("причина отказа не про существующую идентичность: %v", err)
	}

	if after := mustRead(t, deviceKeyPath(dir)); !bytes.Equal(before, after) {
		t.Fatal("device.key заменён повторной инициализацией")
	}
}

// ── T6. Испорченный ключ ────────────────────────────────────────────────

// TestADR069_BrokenDeviceKeyRefusesBothWays — отказ и при обычном запуске,
// и при инициализации.
//
// Вторая половина — не симметрия ради симметрии, а закрытие дыры, которую
// механизм создаёт сам: если бы инициализация «чинила» нечитаемый ключ, флаг
// стал бы способом обойти проверку целостности, и три отказа readKeyFile
// перестали бы что-либо значить.
func TestADR069_BrokenDeviceKeyRefusesBothWays(t *testing.T) {
	cases := []struct {
		name    string
		content string
		mode    os.FileMode
		reason  string
	}{
		{"не base64", "!!! не ключ !!!\n", keyFileMode, "base64"},
		{"пустой", "", keyFileMode, "пуст"},
		{"права 0644", "", 0o644, "доступен группе"},
	}

	for _, c := range cases {
		for _, initIdentity := range []bool{false, true} {
			dir := t.TempDir()
			root, entry := buildCounter(t, dir)
			db := filepath.Join(dir, "engram.db")

			body := c.content
			if c.mode != keyFileMode {
				// Для проверки прав содержимое обязано быть валидным: иначе
				// отказ пришёл бы по другой причине и тест измерял бы не то.
				body = mustValidKeyBody(t)
			}
			if err := os.WriteFile(deviceKeyPath(dir), []byte(body), c.mode); err != nil {
				t.Fatalf("%s: подготовить файл: %v", c.name, err)
			}
			// WriteFile уважает umask — права выставляются явно.
			if err := os.Chmod(deviceKeyPath(dir), c.mode); err != nil {
				t.Fatalf("%s: chmod: %v", c.name, err)
			}

			_, err := Run(context.Background(), runOptions(root, entry, db, initIdentity))
			if err == nil {
				t.Fatalf("%s (инициализация=%v): запуск прошёл на испорченном ключе",
					c.name, initIdentity)
			}
			if !strings.Contains(err.Error(), c.reason) {
				t.Fatalf("%s (инициализация=%v): причина отказа не та: %v",
					c.name, initIdentity, err)
			}
		}
	}
}

// mustValidKeyBody — содержимое заведомо корректного файла ключа.
//
// Берётся из настоящей инициализации, а не собирается руками: формат файла
// — забота keys.go, и дублировать его в тесте значило бы завести второе
// место, где он записан.
func mustValidKeyBody(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	if _, err := Run(context.Background(), runOptions(root, entry, db, true)); err != nil {
		t.Fatalf("подготовка образца ключа: %v", err)
	}
	return string(mustRead(t, deviceKeyPath(dir)))
}
