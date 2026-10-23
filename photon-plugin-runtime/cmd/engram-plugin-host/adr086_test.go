package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

/*
 * Режим -init-identity (ADR-086 §4.1) — на настоящем бинаре хоста.
 *
 * Проверяется то, что делает режим отдельным событием, а не вариантом запуска:
 * ключ создаётся той же функцией, что под -init, а хранилища, плагина и
 * master.key режим не касается.
 */

type initRun struct {
	code int
	out  string
}

func runHost(t *testing.T, bin string, args ...string) initRun {
	t.Helper()
	cmd := exec.Command(bin, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	var ee *exec.ExitError
	switch {
	case errors.As(err, &ee):
		return initRun{ee.ExitCode(), out.String()}
	case err != nil:
		t.Fatalf("запуск хоста: %v", err)
	}
	return initRun{0, out.String()}
}

func TestADR086_InitIdentityCreatesOnlyDeviceKey(t *testing.T) {
	dir := t.TempDir()
	bin := buildHostBinary(t, dir)
	keys := filepath.Join(dir, "state", "keys")

	r := runHost(t, bin, "-init-identity", "-keys", keys)
	if r.code != initIdentityCreated {
		t.Fatalf("код %d, ожидался 0\n%s", r.code, r.out)
	}

	st, err := os.Stat(filepath.Join(keys, deviceKeyFile))
	if err != nil {
		t.Fatalf("device.key не создан: %v", err)
	}
	if st.Mode().Perm() != keyFileMode {
		t.Fatalf("права %v, ожидались %v", st.Mode().Perm(), keyFileMode)
	}
	if _, err := readKeyFile(filepath.Join(keys, deviceKeyFile)); err != nil {
		t.Fatalf("созданный ключ не читается по правилам ADR-069: %v", err)
	}

	// Ни master.key, ни хранилища, ни чего-либо ещё.
	entries, _ := os.ReadDir(keys)
	if len(entries) != 1 {
		t.Fatalf("в каталоге ключей %d записей, ожидался только device.key: %v", len(entries), entries)
	}
	parent, _ := os.ReadDir(filepath.Join(dir, "state"))
	if len(parent) != 1 {
		t.Fatalf("рядом с ключами появились файлы (хранилище?): %v", parent)
	}
}

func TestADR086_InitIdentityNeverReplacesExistingKey(t *testing.T) {
	dir := t.TempDir()
	bin := buildHostBinary(t, dir)
	keys := filepath.Join(dir, "keys")

	if r := runHost(t, bin, "-init-identity", "-keys", keys); r.code != 0 {
		t.Fatalf("первое создание: код %d\n%s", r.code, r.out)
	}
	before, _ := os.ReadFile(filepath.Join(keys, deviceKeyFile))

	r := runHost(t, bin, "-init-identity", "-keys", keys)
	if r.code != initIdentityExists {
		t.Fatalf("повтор: код %d, ожидался 3\n%s", r.code, r.out)
	}
	after, _ := os.ReadFile(filepath.Join(keys, deviceKeyFile))
	if !bytes.Equal(before, after) {
		t.Fatal("существующий device.key перезаписан")
	}
}

func TestADR086_InitIdentityRefusesCorruptKey(t *testing.T) {
	dir := t.TempDir()
	bin := buildHostBinary(t, dir)
	keys := filepath.Join(dir, "keys")
	if err := os.MkdirAll(keys, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(keys, deviceKeyFile)
	for name, setup := range map[string]func(){
		"не base64": func() { _ = os.WriteFile(path, []byte("%%%\n"), 0o600) },
		"пустой":    func() { _ = os.WriteFile(path, nil, 0o600) },
		"права 0644": func() {
			_ = os.WriteFile(path, []byte("AAAA\n"), 0o600)
			_ = os.Chmod(path, 0o644)
		},
	} {
		t.Run(name, func(t *testing.T) {
			_ = os.Remove(path)
			setup()
			before, _ := os.ReadFile(path)
			r := runHost(t, bin, "-init-identity", "-keys", keys)
			if r.code != initIdentityFailed {
				t.Fatalf("код %d, ожидался 1: испорченный ключ — не «уже есть» и не повод создать\n%s", r.code, r.out)
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(before, after) {
				t.Fatal("испорченный ключ заменён")
			}
		})
	}
}

func TestADR086_InitIdentityIsExclusiveWithPluginLaunch(t *testing.T) {
	dir := t.TempDir()
	bin := buildHostBinary(t, dir)
	keys := filepath.Join(dir, "keys")

	for _, extra := range [][]string{
		{"-db", filepath.Join(dir, "engram.db")},
		{"-manifest", filepath.Join(dir, "m.json")},
		{"-plugin", filepath.Join(dir, "p")},
		{"-init"},
		{"-install"},
		{"-events", "e"},
		{"-result-out", filepath.Join(dir, "r.json")},
	} {
		r := runHost(t, bin, append([]string{"-init-identity", "-keys", keys}, extra...)...)
		if r.code != 2 {
			t.Fatalf("%v: код %d, ожидался 2\n%s", extra, r.code, r.out)
		}
		if _, err := os.Stat(filepath.Join(keys, deviceKeyFile)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%v: ключ создан при негодных аргументах", extra)
		}
	}

	if r := runHost(t, bin, "-init-identity"); r.code != 2 || !strings.Contains(r.out, "-keys") {
		t.Fatalf("без -keys: код %d\n%s", r.code, r.out)
	}
}

// После -init-identity обычный запуск плагина идёт БЕЗ -init и работает:
// режим создал ровно ту идентичность, которую ждёт хост.
func TestADR086_PluginRunsAfterInitIdentityWithoutInit(t *testing.T) {
	dir := t.TempDir()
	bin := buildHostBinary(t, dir)
	keys := filepath.Join(dir, "keys")
	if r := runHost(t, bin, "-init-identity", "-keys", keys); r.code != 0 {
		t.Fatalf("создание: код %d\n%s", r.code, r.out)
	}

	root, entry := buildReporter(t, dir)
	mpath := writeManifest(t, dir, reporterManifest(entry))
	t.Setenv("REPORTER_SPEC", "success/empty")
	t.Setenv("REPORTER_EVENT_SPEC", "")
	r := runHost(t, bin, "-db", filepath.Join(dir, "engram.db"), "-keys", keys,
		"-manifest", mpath, "-package-root", root, "-grants", "", "-install")
	if r.code != 0 {
		t.Fatalf("запуск плагина после -init-identity: код %d\n%s", r.code, r.out)
	}
	// master.key создан первым запуском плагина — как и прежде (ADR-086 §5).
	if _, err := os.Stat(filepath.Join(keys, masterKeyFile)); err != nil {
		t.Fatalf("master.key не создан первым запуском плагина: %v", err)
	}
}

// Обычный запуск без ключа по-прежнему отказывает и ключ не создаёт
// (ADR-069 §4.1): режим не ослабил прежнее правило.
func TestADR086_PluginLaunchStillNeverCreatesKey(t *testing.T) {
	dir := t.TempDir()
	bin := buildHostBinary(t, dir)
	keys := filepath.Join(dir, "keys")
	root, entry := buildReporter(t, dir)
	mpath := writeManifest(t, dir, reporterManifest(entry))
	t.Setenv("REPORTER_SPEC", "success/empty")
	r := runHost(t, bin, "-db", filepath.Join(dir, "engram.db"), "-keys", keys,
		"-manifest", mpath, "-package-root", root, "-grants", "", "-install")
	if r.code == 0 {
		t.Fatalf("запуск без device.key прошёл\n%s", r.out)
	}
	if _, err := os.Stat(filepath.Join(keys, deviceKeyFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("запуск плагина создал device.key")
	}
}
