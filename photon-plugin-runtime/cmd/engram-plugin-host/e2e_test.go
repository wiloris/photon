package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-plugin-runtime/host"
)

/*
 * E2E первого настоящего плагина ENGRAM: НАСТОЯЩИЕ ПРОЦЕССЫ, настоящее
 * bbolt-хранилище, настоящий Authorizer.
 *
 * Фиктивного ничего нет вовсе. Плагин собирается компилятором и запускается
 * как отдельный процесс, операции идут через IPC и Authorizer в coreops,
 * данные ложатся в файл. Именно поэтому тест ловит то, чего не видит
 * in-process проверка: сохранение состояния между остановкой и повторным
 * запуском проверяется РАЗНЫМИ процессами, а не разными объектами.
 */

const (
	testPluginID = "dev.engram.counter"
	fullGrants   = "storage.read,storage.write,identity.sign"
)

// buildCounter собирает плагин из examples/counter.
func buildCounter(t *testing.T, dir string) (root, entry string) {
	t.Helper()

	bin := "counter"
	out := filepath.Join(dir, bin)

	cmd := exec.Command("go", "build", "-o", out, filepath.Join("..", "..", "examples", "counter"))
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("сборка плагина: %v\n%s", err, b)
	}
	return dir, bin
}

func grantList(s string) []string { return splitList(s) }

// ── 1. Полный путь целиком ──────────────────────────────────────────────

// TestE2E_FullPath — один запуск проходит весь путь:
// плагин → SDK → IPC → Authorizer → coreops → bbolt → обратно через журнал.
func TestE2E_FullPath(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)

	res, err := Run(context.Background(), Options{
		DBPath:       filepath.Join(dir, "engram.db"),
		PackageRoot:  root,
		Entrypoint:   entry,
		PluginID:     testPluginID,
		Grants:       grantList(fullGrants),
		InitIdentity: true, // свежий каталог: device.key ещё нет (ADR-069 п. 4.1)
		Install:      true,
		Timeout:      20 * time.Second,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(res.Counts) != 1 || res.Counts[0] != 1 {
		t.Fatalf("счётчик первого запуска = %v, ожидалось [1]", res.Counts)
	}
	if res.Exit.Kind != host.ExitCleanStop {
		// Остановка по STOP, а не падение: иначе путь пройден не до конца,
		// и различие между ExitCleanStop и ExitCrash — ровно то, ради чего
		// host их вообще разделяет.
		t.Fatalf("завершение = %v (код %d, err=%v), ожидалась чистая остановка",
			res.Exit.Kind, res.Exit.Code, res.Exit.Err)
	}
	if len(res.Signature) == 0 {
		t.Fatal("подписи нет, хотя право identity.sign выдано")
	}
	if !res.SignatureVerified {
		t.Fatal("подпись не сошлась при проверке ключом устройства")
	}
}

// ── 2. Состояние переживает остановку и повторный запуск ────────────────

// TestE2E_StatePersistsAcrossRestarts — обязательная проверка среза.
//
// Три ПОЛНОСТЬЮ независимых запуска: каждый со своим процессом плагина,
// своим host, своим coreops и своим открытием файла. Общий у них только
// путь к базе. Счётчик обязан расти 1 → 2 → 3.
//
// Мутация, которую тест ловит: если плагин держит счётчик в памяти или
// хост подменяет хранилище временным, второй запуск вернёт 1.
func TestE2E_StatePersistsAcrossRestarts(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	for run, want := range []int{1, 2, 3} {
		res, err := Run(context.Background(), Options{
			DBPath:      db,
			PackageRoot: root,
			Entrypoint:  entry,
			PluginID:    testPluginID,
			Grants:      grantList(fullGrants),
			// Инициализация ровно на первом запуске: дальше device.key уже
			// есть, и повторная попытка — отказ (ADR-069 п. 4.2). Три
			// запуска подряд проходят только при ОДНОЙ инициализации.
			InitIdentity: run == 0,
			Install:      true,
			Timeout:      20 * time.Second,
		})
		if err != nil {
			t.Fatalf("запуск №%d: %v", run+1, err)
		}
		if len(res.Counts) != 1 || res.Counts[0] != want {
			t.Fatalf("запуск №%d: счётчик = %v, ожидалось [%d]", run+1, res.Counts, want)
		}
	}
}

// TestE2E_SeparateStorageStartsFromScratch — обратная сторона того же
// свойства: другой файл означает другое состояние.
//
// Без этой проверки предыдущая прошла бы и на глобальной переменной внутри
// процесса теста.
func TestE2E_SeparateStorageStartsFromScratch(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)

	// initIdentity — параметром, а не полем по умолчанию: хранилища разные, а
	// каталог ключей один (он выводится из пути базы, а обе базы лежат в dir).
	// Значит инициализация здесь ровно одна — при первом запуске.
	opts := func(db string, initIdentity bool) Options {
		return Options{
			DBPath:       db,
			PackageRoot:  root,
			Entrypoint:   entry,
			PluginID:     testPluginID,
			Grants:       grantList(fullGrants),
			InitIdentity: initIdentity,
			Install:      true,
			Timeout:      20 * time.Second,
		}
	}

	first, err := Run(context.Background(), opts(filepath.Join(dir, "a.db"), true))
	if err != nil {
		t.Fatalf("первое хранилище: %v", err)
	}
	second, err := Run(context.Background(), opts(filepath.Join(dir, "b.db"), false))
	if err != nil {
		t.Fatalf("второе хранилище: %v", err)
	}

	if first.Counts[0] != 1 || second.Counts[0] != 1 {
		t.Fatalf("разные хранилища дали %v и %v, ожидалось [1] и [1]",
			first.Counts, second.Counts)
	}
}

// TestE2E_DataReachesTheFile — состояние лежит НА ДИСКЕ, а не в памяти
// процесса теста.
//
// Файл проверяется напрямую: после запуска он существует и непуст. Это
// грубая проверка, и она такой и задумана — точное устройство ключа
// coreops не экспортирует, и повторять его раскладку здесь значило бы
// завести вторую копию того, о чём keys.go прямо предупреждает.
func TestE2E_DataReachesTheFile(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)
	db := filepath.Join(dir, "engram.db")

	if _, err := Run(context.Background(), Options{
		DBPath:       db,
		PackageRoot:  root,
		Entrypoint:   entry,
		PluginID:     testPluginID,
		Grants:       grantList(fullGrants),
		InitIdentity: true, // свежий каталог: device.key ещё нет (ADR-069 п. 4.1)
		Install:      true,
		Timeout:      20 * time.Second,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	info, err := os.Stat(db)
	if err != nil {
		t.Fatalf("файл хранилища не создан: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("файл хранилища пуст — данные не дошли до диска")
	}
}

// ── 3. Путь «вниз»: событие от хоста ────────────────────────────────────

// TestE2E_EventTriggersReport — хост отправляет событие, плагин
// перечитывает счётчик и докладывает снова.
//
// Проверяется и то, что событие НЕ увеличивает счётчик: доклад по запросу
// читает состояние, а не меняет его.
func TestE2E_EventTriggersReport(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)

	res, err := Run(context.Background(), Options{
		DBPath:       filepath.Join(dir, "engram.db"),
		PackageRoot:  root,
		Entrypoint:   entry,
		PluginID:     testPluginID,
		Grants:       grantList(fullGrants),
		Events:       []string{"engram.counter.report", "engram.counter.report"},
		InitIdentity: true, // свежий каталог: device.key ещё нет (ADR-069 п. 4.1)
		Install:      true,
		Timeout:      20 * time.Second,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := []int{1, 1, 1}
	if len(res.Counts) != len(want) {
		t.Fatalf("докладов %d, ожидалось %d: %v", len(res.Counts), len(want), res.Counts)
	}
	for i, n := range res.Counts {
		if n != want[i] {
			t.Fatalf("доклад №%d = %d, ожидалось %d (событие не должно менять счётчик)",
				i+1, n, want[i])
		}
	}
}

// ── 4. Authorizer действительно стоит на пути ───────────────────────────

// TestE2E_MissingPermissionStopsTheOperation — без identity.sign подпись не
// появляется, а счётчик продолжает работать.
//
// Это failure-oriented проверка того, что Authorizer не декоративен: если
// право перестанет проверяться, подпись придёт и тест упадёт.
func TestE2E_MissingPermissionStopsTheOperation(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)

	res, err := Run(context.Background(), Options{
		DBPath:      filepath.Join(dir, "engram.db"),
		PackageRoot: root,
		Entrypoint:  entry,
		PluginID:    testPluginID,
		// identity.sign НЕ выдан.
		Grants:       []string{"storage.read", "storage.write"},
		InitIdentity: true, // свежий каталог: device.key ещё нет (ADR-069 п. 4.1)
		Install:      true,
		Timeout:      20 * time.Second,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(res.Counts) != 1 || res.Counts[0] != 1 {
		t.Fatalf("счётчик = %v, ожидалось [1]: отсутствие права на подпись не должно мешать считать",
			res.Counts)
	}
	if len(res.Signature) != 0 {
		t.Fatal("подпись появилась без права identity.sign — Authorizer не сработал")
	}

	var said bool
	for _, l := range res.Lines {
		if strings.Contains(l, "signature unavailable") {
			said = true
		}
	}
	if !said {
		t.Fatalf("плагин не сообщил об отказе подписи, журнал: %v", res.Lines)
	}
}

// TestE2E_StorageDeniedBreaksTheCounter — без storage.write плагин не может
// сохранить счётчик и обязан завершиться с ошибкой, а не тихо сделать вид,
// что сохранил.
func TestE2E_StorageDeniedBreaksTheCounter(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildCounter(t, dir)

	_, err := Run(context.Background(), Options{
		DBPath:       filepath.Join(dir, "engram.db"),
		PackageRoot:  root,
		Entrypoint:   entry,
		PluginID:     testPluginID,
		Grants:       []string{"storage.read"},
		InitIdentity: true, // свежий каталог: device.key ещё нет (ADR-069 п. 4.1)
		Install:      true,
		Timeout:      10 * time.Second,
	})
	if err == nil {
		t.Fatal("без storage.write запуск завершился успешно — запись прошла без права")
	}
}
