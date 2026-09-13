package main_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// T13 — структурная граница импортов (docs/plugin-lab.md §10, ADR-058 п. 1).
//
// В не-тестовом коде photon-plugin-runtime адаптеры photon-core импортируются
// ТОЛЬКО в cmd/devtools-runtime, и этот пакет — main. Иначе лабораторная
// композиция могла бы уехать в общий пакет и стать образцом для продакшн-хоста.
// Импорты разбираются go/parser, а не подстрокой: комментарий или строка с
// путём адаптера не должны ни ронять проверку, ни проходить её.
func TestLab_T13_AdapterImportsOnlyInDevtoolsRuntime(t *testing.T) {
	const adapters = "github.com/wiloris/photon-core/adapters"
	const allowed = "cmd/devtools-runtime"

	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("корень модуля: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("%s не корень модуля: %v", root, err)
	}

	fset := token.NewFileSet()
	var inAllowed int
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); name == ".git" || name == "testdata" || name == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, filepath.Dir(path))
		rel = filepath.ToSlash(rel)
		if rel == allowed && f.Name.Name != "main" {
			t.Errorf("%s: пакет %q — лабораторная композиция обязана жить в package main", path, f.Name.Name)
		}
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if p != adapters && !strings.HasPrefix(p, adapters+"/") {
				continue
			}
			if rel != allowed {
				t.Errorf("%s импортирует %s — адаптеры в не-тестовом коде runtime допустимы только в %s (ADR-058 п. 1)",
					filepath.Join(rel, filepath.Base(path)), p, allowed)
				continue
			}
			inAllowed++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("обход модуля: %v", err)
	}
	// Контроль: если импортов нет и в разрешённом месте, проверка обходит не
	// то дерево и ничего не измеряет.
	if inAllowed == 0 {
		t.Fatalf("в %s не найдено ни одного импорта адаптеров — проверка обходит не тот код", allowed)
	}
}
