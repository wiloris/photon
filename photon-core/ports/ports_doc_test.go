package ports_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Документ docs/ports.md обязан описывать РОВНО те порты, которые есть в коде.
//
// Проверка структурная, а не содержательная: убедиться, что текст остался
// верным по смыслу, тест не может, а вот поймать расхождение по составу —
// может, и именно оно случается само собой. Десятый порт, добавленный новым
// ADR, уронит этот тест, пока не будет описан; удалённый — пока не будет
// убран из таблиц.
//
// Почему это вообще нужно: docs/ports.md — последнее условие закрытия
// Phase 1, и документ без сторожа устаревает молча. Тот же приём уже
// применяется к каталогу действий в Developer Tools и к таблице правил
// авторизации Bridge — расхождение ловится тестом, а не аудитом.

// portInterfaces — имена экспортируемых интерфейсов пакета, найденные в
// исходниках. Именно интерфейсы: типы-значения (PublicKey, Notification,
// ExternalIdentity) контрактами не являются и отдельного раздела не требуют.
func portInterfaces(t *testing.T) []string {
	t.Helper()

	entries, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("список файлов пакета: %v", err)
	}

	var names []string
	fset := token.NewFileSet()
	for _, f := range entries {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatalf("разбор %s: %v", f, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || !ts.Name.IsExported() {
				return true
			}
			if _, ok := ts.Type.(*ast.InterfaceType); ok {
				names = append(names, ts.Name.Name)
			}
			return true
		})
	}
	sort.Strings(names)
	if len(names) == 0 {
		t.Fatal("в пакете не найдено ни одного интерфейса — разбор сломан")
	}
	return names
}

func portsDoc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "docs", "ports.md"))
	if err != nil {
		t.Fatalf("docs/ports.md — условие закрытия Phase 1, он обязан существовать: %v", err)
	}
	return string(b)
}

func TestPortsDocMentionsEveryInterface(t *testing.T) {
	doc := portsDoc(t)
	for _, name := range portInterfaces(t) {
		if !strings.Contains(doc, "`"+name+"`") {
			t.Errorf("порт %s не описан в docs/ports.md", name)
		}
	}
}

// Таблицы документа и состав пакета совпадают в обе стороны.
//
// Только упоминания в тексте недостаточно: проверка «имя встречается
// где-нибудь» пропускает потерю строки таблицы — имя остаётся в соседнем
// разделе, и таблица молча становится неполной. Это выяснилось мутацией:
// удаление строки `Notifier` тест не ловил. Поэтому здесь — именно
// табличные строки, и обе стороны сразу.
func TestPortsDocTablesMatchPackage(t *testing.T) {
	doc := portsDoc(t)
	known := map[string]bool{}
	for _, name := range portInterfaces(t) {
		known[name] = true
	}

	// Имя с цифрой (P2P) обязано попадать в выборку: без этого проверка
	// молча пропускала бы целый порт.
	row := regexp.MustCompile(`(?m)^\| ` + "`" + `([A-Z][A-Za-z0-9]*)` + "`" + ` \|.*$`)
	described := map[string]bool{}
	for _, m := range row.FindAllStringSubmatch(doc, -1) {
		name, line := m[1], m[0]
		if !known[name] {
			t.Errorf("docs/ports.md описывает контракт %s, которого нет в пакете", name)
		}
		// Перечень реализаций (раздел 5) — две колонки, и стоять только в
		// нём недостаточно: он говорит, ЕСТЬ ли адаптер, а не что контракт
		// собой представляет. Описанием считается строка с тремя и более
		// колонками — таблицы разделов 1 и «Вспомогательные контракты».
		if strings.Count(line, "|") >= 4 {
			described[name] = true
		}
	}
	for name := range known {
		if !described[name] {
			t.Errorf("контракт %s не описан ни в одной содержательной таблице docs/ports.md", name)
		}
	}
}
