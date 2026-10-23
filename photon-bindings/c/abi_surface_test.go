package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Поверхность C ABI после ADR-130 не выросла (§5.5): собранная
// libphoton_core.so экспортирует ровно те функции Photon_*, что объявлены в
// замороженном photon_core.h, — 12; версия — 1/0. Диагностика 819/820 ходит
// через тот же Photon_SendAction.
func TestABISurfaceUnchanged(t *testing.T) {
	if testing.Short() {
		t.Skip("собирает .so — не в -short")
	}
	if tsABIMajor() != 1 || tsABIMinor() != 0 {
		t.Fatalf("PHOTON_ABI = %d.%d, want 1.0", tsABIMajor(), tsABIMinor())
	}
	hdr, err := os.ReadFile("photon_core.h")
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^[A-Za-z][^;(]*?\b(Photon_[A-Za-z]+)\(`).FindAllStringSubmatch(string(hdr), -1) {
		declared[m[1]] = true
	}
	if len(declared) != 12 {
		t.Fatalf("в photon_core.h объявлено %d функций, want 12: %v", len(declared), keys(declared))
	}

	// Сборка — только во временный каталог: -buildmode=c-shared пишет
	// заголовок рядом с выходным файлом (README).
	dir := t.TempDir()
	so := filepath.Join(dir, "libphoton_core.so")
	if out, err := exec.Command("go", "build", "-buildmode=c-shared", "-o", so, ".").CombinedOutput(); err != nil {
		t.Fatalf("сборка .so: %v\n%s", err, out)
	}
	out, err := exec.Command("nm", "-D", "--defined-only", so).Output()
	if err != nil {
		t.Fatalf("nm: %v", err)
	}
	exported := map[string]bool{}
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) == 3 && strings.HasPrefix(f[2], "Photon_") {
			exported[f[2]] = true
		}
	}
	if strings.Join(keys(exported), ",") != strings.Join(keys(declared), ",") {
		t.Fatalf("экспорт .so и photon_core.h расходятся:\n.so %v\n.h  %v", keys(exported), keys(declared))
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
