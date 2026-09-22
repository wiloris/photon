package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/wiloris/photon-plugin-runtime/host"
	"github.com/wiloris/photon-plugin-runtime/manifest"
)

/*
 * Проверка completion (ADR-072) на ПРОИЗВОЛЬНОМ плагине.
 *
 * Значение этого теста не в том, что он зелёный, а в том, что он не мог
 * существовать раньше: до ADR-072 единственным барьером между стартом и
 * остановкой была строка журнала "count:<N>" — формат examples/counter. Плагин,
 * не пишущий её, останавливался хостом до того, как успевал поработать.
 *
 * Поэтому здесь взят плагин, который:
 *   — делает работу АСИНХРОННО и заметное время;
 *   — не пишет ни "count:", ни "signature:";
 *   — докладывает о завершении явно и ждёт STOP от хоста.
 *
 * Доказательство «STOP не ушёл раньше доклада» — не флаг и не отметка времени,
 * а СОСТОЯНИЕ НА ДИСКЕ: все записи плагина на месте. Флаг, выставляемый той же
 * функцией, которая его проверяет, был бы тождественно истинным.
 */

const (
	slowworkID     = "dev.photon.slowwork"
	slowworkWrites = 5
)

// buildSlowwork собирает плагин с асинхронной работой.
func buildSlowwork(t *testing.T, dir string) (root, entry string) {
	t.Helper()

	out := filepath.Join(dir, "slowwork")
	cmd := exec.Command("go", "build", "-o", out, filepath.Join("..", "..", "tests", "plugins", "slowwork"))
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("сборка slowwork: %v\n%s", err, b)
	}
	return dir, "slowwork"
}

func slowworkManifest(entry string) *manifest.Manifest {
	return &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: slowworkID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      entry,
		},
		Permissions: manifest.Permissions{Requested: []string{"storage.write"}},
	}
}

// TestADR072_GenericPluginCompletesWithoutCounterFormat — полный путь
// произвольного плагина.
func TestADR072_GenericPluginCompletesWithoutCounterFormat(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildSlowwork(t, dir)
	db := filepath.Join(dir, "engram.db")

	res, err := Run(context.Background(), Options{
		Manifest:    slowworkManifest(entry),
		DBPath:      db,
		PackageRoot: root,
		// ТОЛЬКО storage.write: доклад о завершении права не требует, и
		// это здесь же и проверяется (ADR-072 §3).
		Grants:       []string{"storage.write"},
		InitIdentity: true,
		Install:      true,
		Timeout:      20 * time.Second,
	})
	if err != nil {
		t.Fatalf("запуск: %v", err)
	}

	// 1. Вся работа выполнена. Это же и доказывает, что STOP не ушёл раньше
	//    доклада: при преждевременной остановке часть записей не дошла бы.
	if n := countKeys(t, db, pluginNamespace(slowworkID)); n != slowworkWrites {
		t.Fatalf("в хранилище %d записей, ожидалось %d — STOP ушёл до конца работы", n, slowworkWrites)
	}

	// 2. Доклад получен хостом, и ровно один.
	if res.Completions != 1 {
		t.Fatalf("докладов о завершении: %d, ожидался 1", res.Completions)
	}

	// 3. Завершение штатное.
	//
	//    ЧЕГО ЭТА ПРОВЕРКА НЕ ДОКАЗЫВАЕТ: что доклад не является
	//    самоостановкой. classify сверяет stopRequested ПЕРВЫМ (host.go), и
	//    после запроса остановки любой выход считается clean_stop — значит
	//    плагин, вышедший сам сразу после доклада, здесь неотличим.
	//    Нетерминальность проверяется отдельно, событием после доклада:
	//    TestADR072_CompletionIsNotTerminal.
	if res.Exit.Kind != host.ExitCleanStop {
		t.Fatalf("завершение %v (код %d), ожидалось clean_stop", res.Exit.Kind, res.Exit.Code)
	}

	// 4. Формата counter в журнале нет вовсе.
	for _, l := range res.Lines {
		if strings.Contains(l, "count:") || strings.Contains(l, "signature:") {
			t.Fatalf("в журнале встретился формат counter: %q", l)
		}
	}
	if len(res.Counts) != 0 || res.Signature != nil {
		t.Fatalf("диагностика counter заполнена на чужом плагине: counts=%v sig=%v",
			res.Counts, res.Signature)
	}
}

// TestADR072_CompletionNeedsNoPermission — доклад проходит у плагина, которому
// не выдано НИ ОДНОГО права.
//
// Без права storage.write его записи отвергаются, работа заканчивается отказом
// — но проверяется здесь не это, а то, что причина отказа именно в записи, а не
// в недоставленном докладе: запуск обязан дойти до классификации завершения, а
// не упереться в таймаут ожидания.
func TestADR072_CompletionNeedsNoPermission(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildSlowwork(t, dir)

	_, err := Run(context.Background(), Options{
		Manifest:     slowworkManifest(entry),
		DBPath:       filepath.Join(dir, "engram.db"),
		PackageRoot:  root,
		Grants:       nil, // ни одного права
		InitIdentity: true,
		Install:      true,
		Timeout:      20 * time.Second,
	})
	if err == nil {
		t.Fatal("плагин без storage.write записал данные")
	}
	if strings.Contains(err.Error(), "не доложил о завершении") {
		t.Fatalf("запуск упёрся в ожидание доклада вместо отказа по праву: %v", err)
	}
	if !strings.Contains(err.Error(), "завершился до доклада") {
		t.Fatalf("неожиданная причина отказа: %v", err)
	}
}

// countKeys считает ключи в namespace плагина, читая файл хранилища напрямую.
//
// Через порт этого не сделать: ports.Storage не отдаёт список namespace. В
// тесте композиции прямое чтение законно — проверяется именно то, что
// композиция оставила на диске.
func countKeys(t *testing.T, path, namespace string) int {
	t.Helper()

	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("открыть bbolt: %v", err)
	}
	defer db.Close()

	n := 0
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(namespace))
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, _ []byte) error {
			n++
			return nil
		})
	})
	if err != nil {
		t.Fatalf("обход namespace: %v", err)
	}
	return n
}

// TestADR072_CompletionIsNotTerminal — доклад не останавливает плагин.
//
// Проверяется ДЕЙСТВИЕМ, а не классификацией исхода: после первого доклада
// хост шлёт событие, и плагин обязан быть жив, чтобы его принять, выполнить
// работу и доложить снова.
//
// Мутация, которую тест ловит и которую не ловил предыдущий: плагин,
// завершающий себя сразу после доклада. С ним событие уходит в пустоту,
// второго доклада нет, и запись по событию не появляется.
func TestADR072_CompletionIsNotTerminal(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildSlowwork(t, dir)
	db := filepath.Join(dir, "engram.db")

	res, err := Run(context.Background(), Options{
		Manifest:     slowworkManifest(entry),
		DBPath:       db,
		PackageRoot:  root,
		Grants:       []string{"storage.write"},
		Events:       []string{"photon.test.again"},
		InitIdentity: true,
		Install:      true,
		Timeout:      20 * time.Second,
	})
	if err != nil {
		t.Fatalf("запуск с событием: %v", err)
	}

	// Два доклада: один по окончании работы, второй по обработке события.
	if res.Completions != 2 {
		t.Fatalf("докладов о завершении: %d, ожидалось 2 — плагин не пережил собственный доклад",
			res.Completions)
	}

	// Запись, сделанная ПОСЛЕ первого доклада, на месте.
	if n := countKeys(t, db, pluginNamespace(slowworkID)); n != slowworkWrites+1 {
		t.Fatalf("в хранилище %d записей, ожидалось %d — работа по событию не выполнена",
			n, slowworkWrites+1)
	}

	if res.Exit.Kind != host.ExitCleanStop {
		t.Fatalf("завершение %v, ожидалось clean_stop", res.Exit.Kind)
	}
}
