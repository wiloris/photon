package ipc

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ADR-130 §2.8: диагностика инстанса Core (819/820) плагинам недоступна.
// Проверяется структурно, а не поведением: у плагина нет пути к ней, если
// (1) в каталоге операций нет ни одного имени домена diag и ни одной
// операции, называющей коды 819/820, и (2) модуль runtime не импортирует ни
// диспетчер ядра (единственный исполнитель 819/820), ни сам services/diag.
// Новая операция diag.* или такой импорт роняют этот тест.
func TestNoDiagnosticsForPlugins(t *testing.T) {
	ops := map[string]bool{}
	for op := range opPermissions {
		ops[op] = true
	}
	for op := range opsWithoutPermission {
		ops[op] = true
	}
	for op := range ops {
		if strings.HasPrefix(op, "diag.") || strings.Contains(op, "819") || strings.Contains(op, "820") {
			t.Errorf("операция %q открывает плагину диагностику ядра", op)
		}
	}

	root := ".."
	// Строки собираются из частей: иначе этот файл совпадал бы сам с собой.
	const core = `"github.com/wiloris/photon/photon-core/services/`
	forbidden := []string{core + "dispatcher\"", core + "diag\""}
	checked := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "testdata" || strings.HasPrefix(d.Name(), ".")) && path != root {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		checked++
		for _, imp := range forbidden {
			if strings.Contains(string(src), imp) {
				t.Errorf("%s импортирует %s — путь плагина к 819/820", path, imp)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 20 {
		t.Fatalf("проверено %d файлов — обход не нашёл модуль", checked)
	}
}
