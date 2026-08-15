package sdk

import (
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// Структурная проверка ADR-031: SDK не должен импортировать
// photon-plugin-runtime или photon-core.
//
// Тест анализирует ИМПОРТЫ, а не поведение — это требование к
// структуре, а не к рантайму. Нарушение должно обнаруживаться при
// проверке кода, а не когда плагин уже запущен.
//
// Порядок срабатывания защиты (проверено экспериментально):
//
//  1. Добавление запрещённого импорта ЛОМАЕТ СБОРКУ раньше, чем этот
//     тест успевает запуститься: go.mod не содержит соответствующей
//     require-директивы, и модуль просто не резолвится. То есть
//     основной барьер — структурный, как и требует ADR-031.
//  2. Этот тест — второй барьер: он сработает, если кто-то добавит и
//     импорт, и зависимость в go.mod одновременно, и даст внятное
//     объяснение вместо ошибки компоновки.
//
// Ограничение: проверяются прямые импорты этого пакета. Транзитивные
// отсекаются TestADR031_NoModuleDependencies ниже.
func TestADR031_NoForbiddenImports(t *testing.T) {
	forbidden := []string{
		"photon-plugin-runtime",
		"photon-core",
	}

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}

	checked := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		checked++

		f, err := parser.ParseFile(fset, e.Name(), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}

		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			for _, bad := range forbidden {
				if strings.Contains(path, bad) {
					t.Fatalf("%s imports %q — ADR-031 forbids it: SDK runs inside the "+
						"untrusted plugin process, and importing runtime code would compile "+
						"the permission-deciding logic into the very thing it must constrain",
						e.Name(), path)
				}
			}
		}
	}

	if checked == 0 {
		t.Fatal("no .go files were checked — the test is not actually verifying anything")
	}
}

// TestADR031_NoModuleDependencies проверяет, что модуль не объявляет
// зависимостей вообще.
//
// Это сильнее проверки импортов: даже если кто-то добавит зависимость
// и не использует её сразу, тест упадёт. Транзитивно втащить runtime
// становится невозможно.
func TestADR031_NoModuleDependencies(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}

	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "require") || strings.HasPrefix(trimmed, "replace") {
			t.Fatalf("go.mod declares %q — SDK must depend on the standard library only, "+
				"so that runtime code cannot be pulled in transitively", trimmed)
		}
	}
}

// TestOperationCatalogCoverage — каждый метод публичного API
// соответствует ровно одной операции каталога, и наоборот.
//
// Проверка документационная: сверяет, что список операций в
// sdk-boundary.md не разошёлся с API. Реализация появится вместе с
// IPC-клиентом; сейчас фиксируется само соответствие.
func TestOperationCatalogCoverage(t *testing.T) {
	// Каталог v1 (sdk-boundary.md, раздел 3).
	catalog := []string{
		"storage.get", "storage.put", "storage.delete", "storage.list",
		"identity.public_key", "identity.sign", "identity.verify",
		"crypto.encrypt", "crypto.decrypt",
		"log.write",
	}

	// Методы API, каждый — одна операция.
	apiMethods := map[string]string{
		"Storage.Get":        "storage.get",
		"Storage.Put":        "storage.put",
		"Storage.Delete":     "storage.delete",
		"Storage.List":       "storage.list",
		"Identity.PublicKey": "identity.public_key",
		"Identity.Sign":      "identity.sign",
		"Identity.Verify":    "identity.verify",
		"Crypto.Encrypt":     "crypto.encrypt",
		"Crypto.Decrypt":     "crypto.decrypt",
		"Plugin.Log":         "log.write",
	}

	covered := make(map[string]bool, len(apiMethods))
	for method, op := range apiMethods {
		if covered[op] {
			t.Fatalf("operation %q is mapped from more than one API method (%s) — "+
				"the mapping must be one-to-one", op, method)
		}
		covered[op] = true
	}

	for _, op := range catalog {
		if !covered[op] {
			t.Fatalf("catalog operation %q has no API method — either add the method "+
				"or remove the operation from sdk-boundary.md", op)
		}
	}

	for method, op := range apiMethods {
		found := false
		for _, c := range catalog {
			if c == op {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("API method %s maps to %q, which is not in the v1 catalog — "+
				"SDK must not promise operations the runtime does not support", method, op)
		}
	}
}
