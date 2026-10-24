package main

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-plugin-runtime/coreops"
	"github.com/wiloris/photon/photon-plugin-runtime/manifest"
)

// Пример examples/greeter — расширение, которое ставится только из каталога
// ENSTOR (ADR-089), — работает через продакшн-хост без прав и докладывает
// свою нагрузку.
func TestGreeterExampleReportsThroughHost(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "greeter")
	if b, err := exec.Command("go", "build", "-o", out, filepath.Join("..", "..", "examples", "greeter")).CombinedOutput(); err != nil {
		t.Fatalf("сборка greeter: %v\n%s", err, b)
	}
	m := &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: "dev.photon.greeter", Version: "0.1.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      "greeter",
		},
	}
	res, err := Run(context.Background(), Options{
		Manifest: m, DBPath: filepath.Join(dir, "engram.db"), PackageRoot: dir,
		InitIdentity: true, Install: true, Timeout: 20 * time.Second,
	})
	if err != nil {
		t.Fatalf("запуск: %v", err)
	}
	if len(res.Reports) != 1 || res.Reports[0].Status != coreops.WorkSuccess {
		t.Fatalf("доклады: %+v", res.Reports)
	}
	var p struct {
		Kind   string            `json:"kind"`
		Result map[string]string `json:"result"`
	}
	if err := json.Unmarshal(res.Reports[0].Payload, &p); err != nil || p.Kind != "greeter.hello" || p.Result["extension_version"] != "0.1.0" {
		t.Fatalf("нагрузка: %s (%v)", res.Reports[0].Payload, err)
	}
}
