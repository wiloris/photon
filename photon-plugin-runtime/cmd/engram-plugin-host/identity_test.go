package main

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/wiloris/photon/photon-plugin-runtime/host"
	"github.com/wiloris/photon/photon-plugin-runtime/manifest"
)

/*
 * identity.public_key и identity.verify через НАСТОЯЩИЙ процесс.
 *
 * Аудит нашёл, что эти две операции покрыты только каталожным E2E, который
 * идёт через net.Pipe и фиктивный обработчик: он доказывает форму запроса и
 * ничего не говорит о результате настоящего исполнителя. Здесь путь целиком —
 * процесс плагина, продакшн-хост, Authorizer, coreops, adapters/ed25519.
 *
 * Семантику проверяет сам плагин (tests/plugins/idcheck), потому что только он
 * находится по ту сторону границы; тест проверяет ЕГО ВЕРДИКТ по состоянию
 * хранилища. Сообщения в журнале для этого не используются: строка в журнале
 * появилась бы и от плагина, который ничего не проверил.
 */

const idcheckID = "dev.photon.idcheck"

func buildIdcheck(t *testing.T, dir string) (root, entry string) {
	t.Helper()

	out := filepath.Join(dir, "idcheck")
	cmd := exec.Command("go", "build", "-o", out, filepath.Join("..", "..", "tests", "plugins", "idcheck"))
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("сборка idcheck: %v\n%s", err, b)
	}
	return dir, "idcheck"
}

// TestIdentity_PublicKeyAndVerifyThroughRealProcess — обе операции на
// настоящей границе процесса.
func TestIdentity_PublicKeyAndVerifyThroughRealProcess(t *testing.T) {
	dir := t.TempDir()
	root, entry := buildIdcheck(t, dir)
	db := filepath.Join(dir, "engram.db")

	res, err := Run(context.Background(), Options{
		Manifest: &manifest.Manifest{
			ManifestVersion: manifest.SupportedManifestVersion,
			Identity:        manifest.Identity{PluginID: idcheckID, Version: "1.0.0"},
			Runtime: manifest.RuntimeSpec{
				ProtocolVersion: manifest.SupportedProtocolVersion,
				Entrypoint:      entry,
			},
			Permissions: manifest.Permissions{
				Requested: []string{"identity.read", "storage.write"},
			},
		},
		DBPath:      db,
		PackageRoot: root,
		// identity.read открывает public_key и verify. identity.sign НЕ
		// выдаётся намеренно: проверка идёт вектором RFC 8032, и права на
		// подпись для неё не требуется.
		Grants:       []string{"identity.read", "storage.write"},
		InitIdentity: true,
		Install:      true,
		Timeout:      20 * time.Second,
	})
	if err != nil {
		t.Fatalf("запуск: %v", err)
	}
	if res.Exit.Kind != host.ExitCleanStop {
		t.Fatalf("завершение %v (код %d), ожидалось clean_stop", res.Exit.Kind, res.Exit.Code)
	}

	// Вердикт плагина: значение появляется в хранилище ТОЛЬКО если все
	// семантические проверки прошли — включая ту, что испорченная подпись
	// объявлена неверной.
	if !storageHasValue(t, db, pluginNamespace(idcheckID), []byte("ok")) {
		t.Fatal("плагин не записал успешный вердикт: identity-проверки не прошли")
	}
}

// storageHasValue — есть ли в namespace значение с таким содержимым.
//
// Читается файл хранилища: ports.Storage не отдаёт список namespace, а ключи
// coreops строит с длиной-префиксом, и воспроизводить их формат в тесте
// значило бы завести второе место, где он записан.
func storageHasValue(t *testing.T, path, namespace string, want []byte) bool {
	t.Helper()

	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("открыть bbolt: %v", err)
	}
	defer db.Close()

	found := false
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(namespace))
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, v []byte) error {
			if bytes.Equal(v, want) {
				found = true
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("обход namespace: %v", err)
	}
	return found
}
