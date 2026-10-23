package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

/*
 * ПЕРВЫЙ ВНЕШНИЙ ПЛАГИН ЧЕРЕЗ НАСТОЯЩИЙ ПРОЦЕСС ХОСТА.
 *
 * Чем это отличается от остальных тестов пакета, и почему одного их набора
 * было недостаточно: они зовут Run() внутри процесса теста, то есть проверяют
 * композицию как библиотеку. Здесь запускается СОБРАННЫЙ БИНАРЬ хоста с теми
 * же аргументами, какие даст ему любой вызывающий, — то есть проверяется путь,
 * который до сих пор существовал только в командной строке человека:
 *
 *	engram-plugin-host --manifest … --package-root … → процесс плагина
 *	  → HELLO/HELLO_ACK → REQUEST(storage.get/put, identity.sign) → RESPONSE
 *
 * Манифест берётся НЕ синтетический: используется файл, который плагин несёт
 * в своём каталоге (examples/counter/manifest.json). Подмена его на
 * собранный в тесте объект ослабила бы проверку ровно в том месте, ради
 * которого она написана, — доказательстве, что настоящий манифест настоящего
 * плагина исполним хостом как есть.
 *
 * Доказательство REQUEST/RESPONSE — косвенное по форме и прямое по существу:
 * счётчик растёт между запусками, а значит storage.get вернул записанное
 * прошлым запуском значение; подпись сошлась у хоста, а значит identity.sign
 * прошёл до исполнителя и вернулся. Ни то, ни другое невозможно без пары
 * REQUEST → RESPONSE. Трассировки протокола продакшн-хост не ведёт, и
 * заводить её ради теста значило бы добавить механизм ради доказательства.
 */

// buildHostBinary собирает сам хост — тот самый бинарь, что уедет на
// устройство.
func buildHostBinary(t *testing.T, dir string) string {
	t.Helper()

	out := filepath.Join(dir, "engram-plugin-host")
	cmd := exec.Command("go", "build", "-o", out, ".")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("сборка хоста: %v\n%s", err, b)
	}
	return out
}

// shippedManifest — путь к манифесту, который лежит В КАТАЛОГЕ ПЛАГИНА.
func shippedManifest(t *testing.T) string {
	t.Helper()

	path := filepath.Join("..", "..", "examples", "counter", "manifest.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("манифест плагина не найден: %v", err)
	}
	return path
}

// TestProductionPath_ExternalPluginThroughHostProcess — внешний плагин,
// внешний манифест, настоящий процесс хоста.
func TestProductionPath_ExternalPluginThroughHostProcess(t *testing.T) {
	work := t.TempDir()
	hostBin := buildHostBinary(t, work)

	// Каталог пакета отдельно от каталога данных: на Android корень пакета
	// доступен только на чтение (ADR-070 §6.4), и смешивать их в тесте
	// значило бы проверять раскладку, которой там не будет.
	pkg := filepath.Join(work, "pkg")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatalf("каталог пакета: %v", err)
	}
	buildCounter(t, pkg)

	data := filepath.Join(work, "data")
	if err := os.MkdirAll(data, 0o755); err != nil {
		t.Fatalf("каталог данных: %v", err)
	}

	run := func(first bool) string {
		t.Helper()

		args := []string{
			"-db", filepath.Join(data, "engram.db"),
			"-keys", filepath.Join(data, "keys"),
			"-manifest", shippedManifest(t),
			"-package-root", pkg,
		}
		if first {
			// Создание идентичности и установка — события явные
			// (ADR-069, ADR-068 п. 6.2), и повторения не требуют.
			args = append(args, "-init", "-install")
		}

		cmd := exec.Command(hostBin, args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("запуск хоста (первый=%v): %v\n%s", first, err, out)
		}
		return string(out)
	}

	first := run(true)

	for _, want := range []string{
		"плагин:    dev.engram.counter", // идентичность пришла из манифеста
		"счётчик:   [1]",                // storage.put прошёл
		"подпись:   проверена хостом",   // identity.sign прошёл и сошёлся
		"count:1", // доклад плагина в журнале
	} {
		if !strings.Contains(first, want) {
			t.Fatalf("в выводе нет %q:\n%s", want, first)
		}
	}

	// Второй запуск — БЕЗ -init и -install: идентичность и регистрация уже
	// есть. Рост счётчика доказывает, что storage.get вернул записанное
	// прошлым процессом, то есть REQUEST дошёл до исполнителя и RESPONSE
	// вернулся с данными, а не с пустотой.
	second := run(false)
	if !strings.Contains(second, "счётчик:   [2]") {
		t.Fatalf("второй запуск не увидел состояние первого:\n%s", second)
	}
	if !strings.Contains(second, "подпись:   проверена хостом") {
		t.Fatalf("подпись второго запуска не сошлась — ключ не пережил перезапуск:\n%s", second)
	}
}
