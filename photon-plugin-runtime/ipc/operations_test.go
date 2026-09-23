package ipc

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// Тесты каталога операций и маппинга прав.
//
// До ADR-076 машинной проверки у этой таблицы не было вовсе: расхождение
// каталога и документа ловилось только чтением. Здесь оно ловится сборкой.

// p2pOps — шесть операций домена, внесённых ADR-076 §3.3. Форма заморожена:
// изменение требует отдельного ADR, поэтому список записан явно, а не выведен
// префиксом из самой таблицы — иначе тест согласился бы с любой её правкой.
var p2pOps = []string{
	"p2p.create_offer",
	"p2p.accept_offer",
	"p2p.complete",
	"p2p.peer",
	"p2p.send",
	"p2p.close",
}

func TestP2POperationsRequireTheSingleConnectPermission(t *testing.T) {
	for _, op := range p2pOps {
		perm, known, requires := permissionForOp(op)
		if !known {
			t.Errorf("операция %q неизвестна каталогу — ADR-076 §3.3 требует её наличия", op)
			continue
		}
		if !requires {
			t.Errorf("операция %q не требует права: p2p.* без права — это сеть без разрешения", op)
			continue
		}
		if perm != "p2p.connect" {
			t.Errorf("операция %q требует %q, а ADR-076 §3.5 фиксирует единственное право p2p.connect", op, perm)
		}
	}
}

// Домен не должен обрасти операциями мимо ADR: таблица и §3.3 — один список.
func TestP2PDomainHasExactlySixOperations(t *testing.T) {
	var got []string
	for op := range opPermissions {
		if strings.HasPrefix(op, "p2p.") {
			got = append(got, op)
		}
	}
	if len(got) != len(p2pOps) {
		t.Fatalf("в таблице %d операций p2p.* (%v), ADR-076 §3.3 фиксирует %d (%v)",
			len(got), got, len(p2pOps), p2pOps)
	}
}

// Ни одна операция p2p.* не имеет права обойти проверку прав.
//
// opsWithoutPermission существует ровно для двух операций и ровно по одной
// причине — плагин обязан оставаться наблюдаемым и управляемым, не получив
// ничего. К сети это основание не относится.
func TestP2POperationsAreNotExemptFromPermission(t *testing.T) {
	for _, op := range p2pOps {
		if opsWithoutPermission[op] {
			t.Errorf("операция %q попала в opsWithoutPermission — сеть без права", op)
		}
	}
	if len(opsWithoutPermission) != 2 {
		t.Errorf("операций без права %d, ожидалось 2 (log.write, work.complete)", len(opsWithoutPermission))
	}
}

// Три различимых исхода (ADR-032, docs/ipc.md §5): известна+требует права,
// известна+без права, неизвестна. Смешение последних двух — самая дорогая
// ошибка здесь: «я не знаю такой операции» и «тебе нельзя» — разные ответы.
func TestPermissionForOpHasThreeDistinctOutcomes(t *testing.T) {
	if perm, known, requires := permissionForOp("p2p.send"); !known || !requires || perm == "" {
		t.Errorf("известная+с правом: получено (%q, %v, %v)", perm, known, requires)
	}
	if perm, known, requires := permissionForOp("log.write"); !known || requires || perm != "" {
		t.Errorf("известная+без права: получено (%q, %v, %v)", perm, known, requires)
	}
	if perm, known, requires := permissionForOp("p2p.teleport"); known || requires || perm != "" {
		t.Errorf("неизвестная: получено (%q, %v, %v)", perm, known, requires)
	}
}

// Каталог таблицы и каталог документа — один список.
//
// Условие 4 из шести (sdk-boundary.md, «Порядок добавления домена операций»)
// требует вносить операции в раздел 3 и в эту таблицу ОДНОВРЕМЕННО. Операция,
// известная одной стороне и неизвестной другой, — расхождение спецификации и
// кода, ровно то, что нашёл E2E-тест Phase 10. Здесь оно ловится машинно.
func TestCatalogMatchesSDKBoundaryDoc(t *testing.T) {
	data, err := os.ReadFile("../docs/sdk-boundary.md")
	if err != nil {
		t.Fatalf("чтение sdk-boundary.md: %v", err)
	}
	doc := string(data)

	// Операции в документе объявлены строками таблиц вида
	// "| `op` | `permission` | ...". Берём только их: упоминание операции в
	// прозе документом её не объявляет.
	row := regexp.MustCompile("(?m)^\\|\\s*`([a-z0-9_]+\\.[a-z0-9_]+)`\\s*\\|\\s*`([a-z0-9_.]+)`\\s*\\|")
	documented := make(map[string]string)
	for _, m := range row.FindAllStringSubmatch(doc, -1) {
		documented[m[1]] = m[2]
	}
	if len(documented) == 0 {
		t.Fatal("в документе не найдено ни одной строки операции — тест ничего не проверяет")
	}

	for op, perm := range opPermissions {
		docPerm, found := documented[op]
		if !found {
			t.Errorf("операция %q есть в таблице, но не описана в sdk-boundary.md §3", op)
			continue
		}
		if docPerm != perm {
			t.Errorf("операция %q: таблица требует %q, документ обещает %q", op, perm, docPerm)
		}
	}
	for op := range documented {
		if _, inTable := opPermissions[op]; inTable {
			continue
		}
		if opsWithoutPermission[op] {
			continue
		}
		t.Errorf("операция %q описана в sdk-boundary.md §3, но отсутствует в таблице — "+
			"плагин получит UNSUPPORTED там, где документ обещает работу", op)
	}
}
