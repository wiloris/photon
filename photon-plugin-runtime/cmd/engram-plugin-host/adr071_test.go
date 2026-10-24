package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-plugin-runtime/manifest"
)

/*
 * Проверки integration contract ADR-071: манифест и корень пакета приходят
 * аргументами запуска.
 *
 * Два свойства здесь проверяются ПОРЯДКОМ, а не конечной ошибкой, и это
 * главное отличие этих тестов от проверки «запуск отказал»:
 *
 *   - структурно негодный манифест обязан быть отвергнут ДО создания ключей,
 *     открытия хранилища и записи в реестр. Перенос Validate ниже
 *     loadOrCreateKeys оставляет тот же текст ошибки — и тест обязан упасть
 *     именно на следе, а не на сообщении;
 *   - файловая проверка обязана сработать ДО host.Start и своей ошибкой:
 *     без неё запуск всё равно упадёт, но позже и по другой причине.
 *
 * Про permissions.requested против -grants здесь нет ни одной проверки
 * намеренно: ADR-071 оставил семантику deferred, и тест закрепил бы
 * поведение, которое ещё не решено.
 */

// validManifest — манифест, проходящий структурную валидацию.
func validManifest(pluginID, entrypoint string) *manifest.Manifest {
	return &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: pluginID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      entrypoint,
		},
	}
}

// writeManifest кладёт манифест на диск как JSON — в том виде, в каком его
// принесёт пакет плагина.
func writeManifest(t *testing.T, dir string, m *manifest.Manifest) string {
	t.Helper()

	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("сериализовать манифест: %v", err)
	}
	path := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("записать манифест: %v", err)
	}
	return path
}

// ── разбор аргументов: режимы и их взаимное исключение ──────────────────

// TestADR071_ModesAreMutuallyExclusive — оба режима сразу отвергаются.
//
// Мутация, которую ловит: «манифест важнее» или «-plugin важнее». Любой
// приоритет проходит молча и расходится ровно тогда, когда значения разные.
func TestADR071_ModesAreMutuallyExclusive(t *testing.T) {
	dir := t.TempDir()
	mpath := writeManifest(t, dir, validManifest(testPluginID, "libcounter.so"))

	_, err := parseArgs([]string{
		"-db", filepath.Join(dir, "engram.db"),
		"-manifest", mpath,
		"-package-root", dir,
		"-plugin", filepath.Join(dir, "counter"),
	})
	if err == nil {
		t.Fatal("оба режима приняты одновременно — один источник молча победил другой")
	}
	if !strings.Contains(err.Error(), "взаимоисключающие") {
		t.Fatalf("причина отказа не про режимы: %v", err)
	}
}

// TestADR071_ManifestModeRequiresPackageRoot — без корня запуск невозможен.
func TestADR071_ManifestModeRequiresPackageRoot(t *testing.T) {
	dir := t.TempDir()
	mpath := writeManifest(t, dir, validManifest(testPluginID, "libcounter.so"))

	_, err := parseArgs([]string{"-db", filepath.Join(dir, "engram.db"), "-manifest", mpath})
	if err == nil {
		t.Fatal("manifest-режим принят без -package-root — корень выведен из воздуха")
	}
	if !strings.Contains(err.Error(), "package-root") {
		t.Fatalf("причина отказа не про корень: %v", err)
	}
}

// ── -id: ожидание, а не источник истины ─────────────────────────────────

// TestADR071_ExpectedIDMatches — совпавший -id не мешает и не становится
// источником.
func TestADR071_ExpectedIDMatches(t *testing.T) {
	dir := t.TempDir()
	mpath := writeManifest(t, dir, validManifest(testPluginID, "libcounter.so"))

	o, err := parseArgs([]string{
		"-db", filepath.Join(dir, "engram.db"),
		"-manifest", mpath, "-package-root", dir,
		"-id", testPluginID,
	})
	if err != nil {
		t.Fatalf("совпавший -id отвергнут: %v", err)
	}
	if o.PluginID != testPluginID || o.Entrypoint != "libcounter.so" {
		t.Fatalf("plugin_id=%q entrypoint=%q — значения не из манифеста", o.PluginID, o.Entrypoint)
	}
	if o.Manifest == nil {
		t.Fatal("манифест не дошёл до Options")
	}
}

// TestADR071_ExpectedIDMismatchRefuses — несовпадение отвергается.
//
// Это и есть сценарий ADR-070 §5.4: пакет обновился и объявляет другую
// идентичность, а права вычислены для прежней.
func TestADR071_ExpectedIDMismatchRefuses(t *testing.T) {
	dir := t.TempDir()
	mpath := writeManifest(t, dir, validManifest(testPluginID, "libcounter.so"))

	_, err := parseArgs([]string{
		"-db", filepath.Join(dir, "engram.db"),
		"-manifest", mpath, "-package-root", dir,
		"-id", "dev.engram.other",
	})
	if err == nil {
		t.Fatal("несовпадающий -id принят — права достались бы другой идентичности")
	}
	if !strings.Contains(err.Error(), "не совпадает") {
		t.Fatalf("причина отказа не про несовпадение: %v", err)
	}
}

// TestADR071_ManifestIsSourceOfTruthWithoutID — без -id источник один.
func TestADR071_ManifestIsSourceOfTruthWithoutID(t *testing.T) {
	dir := t.TempDir()
	mpath := writeManifest(t, dir, validManifest("dev.engram.other", "libother.so"))

	o, err := parseArgs([]string{
		"-db", filepath.Join(dir, "engram.db"),
		"-manifest", mpath, "-package-root", dir,
	})
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	if o.PluginID != "dev.engram.other" {
		t.Fatalf("plugin_id = %q, ожидался из манифеста", o.PluginID)
	}
	if o.Entrypoint != "libother.so" {
		t.Fatalf("entrypoint = %q, ожидался из манифеста", o.Entrypoint)
	}
}

// TestADR071_EmptyIDIsStillGiven — `-id ""` ЗАДАН, и это отличается от
// отсутствия флага.
//
// Тест закрывает ровно ту ловушку, ради которой умолчание убрано из флага:
// реализация, проверяющая заданность сравнением с "", пропустит этот случай
// и не заметит несовпадения. flag.Visit — единственный способ ответить.
func TestADR071_EmptyIDIsStillGiven(t *testing.T) {
	dir := t.TempDir()
	mpath := writeManifest(t, dir, validManifest(testPluginID, "libcounter.so"))

	_, err := parseArgs([]string{
		"-db", filepath.Join(dir, "engram.db"),
		"-manifest", mpath, "-package-root", dir,
		"-id", "",
	})
	if err == nil {
		t.Fatal("пустой -id принят как «не задан» — заданность проверяется сравнением со строкой")
	}
	if !strings.Contains(err.Error(), "не совпадает") {
		t.Fatalf("причина отказа не про несовпадение: %v", err)
	}
}

// ── legacy-режим не меняется ────────────────────────────────────────────

// TestADR071_LegacyModeUnchanged — корень и entrypoint выводятся из -plugin,
// а без -id подставляется прежнее умолчание.
func TestADR071_LegacyModeUnchanged(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "counter")

	o, err := parseArgs([]string{"-db", filepath.Join(dir, "engram.db"), "-plugin", bin})
	if err != nil {
		t.Fatalf("legacy-режим отвергнут: %v", err)
	}
	if o.Manifest != nil {
		t.Fatal("в legacy-режиме появился внешний манифест")
	}
	if o.PackageRoot != dir || o.Entrypoint != "counter" {
		t.Fatalf("корень=%q entrypoint=%q — выведены не из -plugin", o.PackageRoot, o.Entrypoint)
	}
	if o.PluginID != legacyDefaultPluginID {
		t.Fatalf("plugin_id = %q, ожидалось прежнее умолчание %q", o.PluginID, legacyDefaultPluginID)
	}
}

// ── порядок: структурная проверка до побочных эффектов ──────────────────

// TestADR071_StructuralValidationPrecedesSideEffects — негодный манифест не
// оставляет следа.
//
// ПРОВЕРЯЕТСЯ СЛЕД, А НЕ ОШИБКА. Если перенести Validate ниже
// loadOrCreateKeys, Migrate или ensureInstalled, текст ошибки не изменится —
// изменится то, что на диске: появится device.key, файл хранилища и
// израсходованный номер миграции, который по ADR-068 §6.1 не возвращается.
func TestADR071_StructuralValidationPrecedesSideEffects(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "engram.db")

	bad := validManifest("НЕ ИДЕНТИФИКАТОР", "libcounter.so")

	_, err := Run(context.Background(), Options{
		Manifest:     bad,
		DBPath:       db,
		PackageRoot:  dir,
		Grants:       grantList(fullGrants),
		InitIdentity: true,
		Install:      true,
		Timeout:      20 * time.Second,
	})
	if err == nil {
		t.Fatal("негодный манифест принят")
	}
	if !errors.Is(err, manifest.ErrInvalidPluginID) {
		t.Fatalf("причина отказа не про plugin_id: %v", err)
	}

	if exists(t, filepath.Join(dir, deviceKeyFile)) {
		t.Fatal("device.key создан — структурная проверка идёт ПОСЛЕ ключей")
	}
	if exists(t, filepath.Join(dir, masterKeyFile)) {
		t.Fatal("master.key создан — структурная проверка идёт ПОСЛЕ ключей")
	}
	if exists(t, db) {
		t.Fatal("хранилище создано — структурная проверка идёт ПОСЛЕ открытия storage")
	}
}

// ── порядок: файловая проверка до запуска ───────────────────────────────

// TestADR071_FilesystemValidationRejectsMissingEntrypoint — entrypoint,
// которого нет в корне, отвергается ВТОРЫМ Validate.
//
// Проверяется конкретная ошибка, а не факт отказа: без второго вызова запуск
// тоже не состоится — host.Start упадёт на попытке выполнить несуществующий
// файл, но позже и с другой причиной.
func TestADR071_FilesystemValidationRejectsMissingEntrypoint(t *testing.T) {
	dir := t.TempDir()
	root, _ := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	m := validManifest(testPluginID, "libnope.so")

	_, err := Run(context.Background(), Options{
		Manifest:     m,
		DBPath:       db,
		PackageRoot:  root,
		Grants:       grantList(fullGrants),
		InitIdentity: true,
		Install:      true,
		Timeout:      20 * time.Second,
	})
	if err == nil {
		t.Fatal("несуществующий entrypoint принят")
	}
	if !errors.Is(err, manifest.ErrEntrypointNotFound) {
		t.Fatalf("отказ пришёл не от файловой проверки манифеста: %v", err)
	}
}

// ── идентичность из манифеста доходит до реестра и namespace ────────────

// TestADR071_ManifestPluginIDDrivesRegistryIdentity — реестр и namespace
// выведены из манифеста, а не из CLI.
//
// Взят идентификатор, ОТЛИЧНЫЙ от того, что объявляет настоящий плагин:
// запуск дойдёт до handshake и там откажет, но к этому моменту реестр и
// namespace уже созданы — и созданы по манифесту. Если бы источником был
// CLI, появился бы namespace плагина по умолчанию.
func TestADR071_ManifestPluginIDDrivesRegistryIdentity(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	const other = "dev.engram.other"
	m := validManifest(other, entry)

	_, err := Run(context.Background(), Options{
		Manifest:     m,
		DBPath:       db,
		PackageRoot:  root,
		PluginID:     testPluginID, // перезаписывается манифестом
		Grants:       grantList(fullGrants),
		InitIdentity: true,
		Install:      true,
		Timeout:      20 * time.Second,
	})
	if err == nil {
		t.Fatal("плагин, объявивший другой plugin_id, прошёл handshake")
	}

	buckets := bucketNames(t, db)
	if !buckets[pluginNamespace(other)] {
		t.Fatalf("namespace %q не создан; в хранилище: %v", pluginNamespace(other), keys(buckets))
	}
	if buckets[pluginNamespace(testPluginID)] {
		t.Fatal("создан namespace по значению из Options — источником истины стал не манифест")
	}
}

// ── внешний манифест целиком доходит до Host ────────────────────────────

// TestADR071_ExternalManifestRunsPlugin — полный путь с внешним манифестом
// работает: плагин запускается, докладывает и подписывает.
//
// Без этой проверки предыдущие доказывали бы только отказы.
func TestADR071_ExternalManifestRunsPlugin(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	m := validManifest(testPluginID, entry)

	res, err := Run(context.Background(), Options{
		Manifest:     m,
		DBPath:       db,
		PackageRoot:  root,
		Grants:       grantList(fullGrants),
		InitIdentity: true,
		Install:      true,
		Timeout:      20 * time.Second,
	})
	if err != nil {
		t.Fatalf("запуск с внешним манифестом: %v", err)
	}
	if len(res.Counts) != 1 || res.Counts[0] != 1 {
		t.Fatalf("счётчик = %v, ожидалось [1]", res.Counts)
	}
	if !res.SignatureVerified {
		t.Fatal("подпись не сошлась")
	}
}
