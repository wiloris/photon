package main_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// T13 — структурная граница импортов (docs/plugin-lab.md §10, ADR-058 п. 1,
// ADR-068 п. 7.2).
//
// В не-тестовом коде photon-plugin-runtime адаптеры photon-core импортируются
// ТОЛЬКО в перечисленных ниже композициях, и каждая — package main. Иначе
// композиция могла бы уехать в общий пакет и стать образцом для чужого хоста.
// Импорты разбираются go/parser, а не подстрокой: комментарий или строка с
// путём адаптера не должны ни ронять проверку, ни проходить её.
//
// СПИСОК ПОИМЁННЫЙ, и это его суть (ADR-068 п. 7.2). Правило вида «любой
// cmd/*» разрешило бы третью композицию молча — а именно её появление и нужно
// замечать. Второе значение добавлено решением ADR-068, а не правкой теста
// ради прохождения: продакшн-композиция ENGRAM — самостоятельный
// композиционный корень, и общего пакета с лабораторией у неё нет (ADR-058
// п. 1 остаётся в силе буквально).
func TestLab_T13_AdapterImportsOnlyInDevtoolsRuntime(t *testing.T) {
	const adapters = "github.com/wiloris/photon/photon-core/adapters"
	allowed := map[string]bool{
		"cmd/devtools-runtime":   true, // лаборатория, ADR-057/058
		"cmd/engram-plugin-host": true, // продакшн-композиция ENGRAM, ADR-068
	}

	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("корень модуля: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("%s не корень модуля: %v", root, err)
	}

	fset := token.NewFileSet()
	inAllowed := make(map[string]int, len(allowed))
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
		if allowed[rel] && f.Name.Name != "main" {
			t.Errorf("%s: пакет %q — композиция обязана жить в package main", path, f.Name.Name)
		}
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if p != adapters && !strings.HasPrefix(p, adapters+"/") {
				continue
			}
			if !allowed[rel] {
				t.Errorf("%s импортирует %s — адаптеры в не-тестовом коде runtime допустимы только в %v (ADR-058 п. 1, ADR-068 п. 7.2)",
					filepath.Join(rel, filepath.Base(path)), p, allowedList(allowed))
				continue
			}
			inAllowed[rel]++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("обход модуля: %v", err)
	}
	// Контроль: если импортов нет в КАЖДОМ разрешённом месте, проверка
	// обходит не то дерево и ничего не измеряет. Проверяется поимённо: одной
	// суммы мало — она прошла бы и тогда, когда вторая композиция исчезла.
	for place := range allowed {
		if inAllowed[place] == 0 {
			t.Fatalf("в %s не найдено ни одного импорта адаптеров — проверка обходит не тот код", place)
		}
	}
}

// allowedList — разрешённые места в стабильном порядке, для сообщения об
// ошибке. Порядок обхода map случаен, а сообщение теста обязан быть
// воспроизводимым.
func allowedList(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
