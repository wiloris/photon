package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-core/services/diag"
)

// Диагностика инстанса (ADR-130): включена по умолчанию, выключается
// конфигурацией, наблюдает хранилище с момента открытия.

func skipIfDiagForcedOff(t *testing.T) {
	t.Helper()
	if diagForcedOff {
		t.Skip("сборка photon_diag_off: диагностика выключена у всех инстансов")
	}
}

func TestDiagConfig(t *testing.T) {
	skipIfDiagForcedOff(t)
	for _, tc := range []struct {
		raw     string
		enabled bool
		cap     int
	}{
		{`{}`, true, diag.DefaultCapacity},
		{`{"diagnostics":{}}`, true, diag.DefaultCapacity},
		{`{"diagnostics":{"enabled":true,"capacity":64}}`, true, 64},
		{`{"diagnostics":{"enabled":false}}`, false, 0},
		{`{"bridge":{"endpoint":"x:1"},"diagnostics":{"capacity":8192,"slow_tx_ms":5}}`, true, 8192},
	} {
		cfg, err := ParseConfig([]byte(tc.raw))
		if err != nil {
			t.Fatalf("%s: %v", tc.raw, err)
		}
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("%s: New: %v", tc.raw, err)
		}
		if (d.Diag() != nil) != tc.enabled {
			t.Errorf("%s: включена=%v, want %v", tc.raw, d.Diag() != nil, tc.enabled)
		}
		if tc.enabled && d.Diag().Snapshot().Journal.Capacity != tc.cap {
			t.Errorf("%s: ёмкость %d, want %d", tc.raw, d.Diag().Snapshot().Journal.Capacity, tc.cap)
		}
		d.Close()
	}
	for _, raw := range []string{
		`{"diagnostics":{"capacity":63}}`,
		`{"diagnostics":{"capacity":8193}}`,
		`{"diagnostics":{"slow_tx_ms":-1}}`,
		`{"bridge":{"endpoint":"x:1"},"diagnostics":{"slow_call_ms":-1}}`,
	} {
		if _, err := ParseConfig([]byte(raw)); err == nil {
			t.Errorf("%s: принят", raw)
		}
	}
	// Литерал Config без секции — включена: так собирают инстанс тесты и
	// встраивающие.
	d, err := New(Config{})
	if err != nil || d.Diag() == nil {
		t.Fatalf("Config{}: diag=%v err=%v", d.Diag(), err)
	}
	d.Close()
}

// Инстанс с хранилищем: открытие, обе миграции и закрытие в журнале; снимок
// показывает открытое хранилище версии 2 со статистикой файла. Сводка
// конфигурации — без токена и без пути к файлу.
func TestDiagObservesStorageLifecycle(t *testing.T) {
	skipIfDiagForcedOff(t)
	path := filepath.Join(t.TempDir(), "engram.db")
	d, err := New(Config{StoragePath: path, Endpoint: "fake:1", SessionToken: "SECRET-TOKEN"})
	if err != nil {
		t.Fatal(err)
	}
	dg := d.Diag()
	if _, err := d.SendAction(800, []byte(`{"channel_id":"c","limit":10}`)); err != nil {
		t.Fatalf("800: %v", err)
	}
	s := dg.Snapshot()
	if s.Storage == nil || !s.Storage.Open || s.Storage.SchemaVersion != storageSchemaVersion || s.Storage.Bolt == nil || s.Storage.Bolt.FileSize <= 0 {
		t.Fatalf("снимок хранилища: %+v", s.Storage)
	}
	if s.Storage.ViewTx == 0 {
		t.Fatal("чтение 800 не прошло через декоратор")
	}
	if !s.Config.BridgeConfigured || !s.Config.StorageConfigured || s.Config.P2PConfigured {
		t.Fatalf("сводка: %+v", s.Config)
	}
	raw, _ := json.Marshal(s)
	for _, f := range []string{"SECRET-TOKEN", path, "engram.db"} {
		if strings.Contains(string(raw), f) {
			t.Fatalf("снимок содержит %q", f)
		}
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	p, _ := dg.Read(0, diag.MaxReadLimit)
	var kinds []string
	for _, e := range p.Events {
		if e.Component == diag.CompStorage {
			kinds = append(kinds, e.Kind.String())
		}
		for i := 0; i < int(e.N); i++ {
			if strings.Contains(e.Fields[i].Str, "SECRET") || strings.Contains(e.Fields[i].Str, path) {
				t.Fatalf("запись %d содержит запрещённое: %q", e.Seq, e.Fields[i].Str)
			}
		}
	}
	// Миграций две: лента (v1, localfeed.Prepare) и общая схема инстанса (v2).
	if got := strings.Join(kinds, ","); got != "open,migrate,migrate,close" {
		t.Fatalf("события хранилища: %s", got)
	}
	if dg.Snapshot().Storage.Open {
		t.Fatal("после Close хранилище числится открытым")
	}
}

// Неудачное открытие — событие open с классом failed (путь — каталог).
func TestDiagStorageOpenFailed(t *testing.T) {
	skipIfDiagForcedOff(t)
	dg, err := diag.New(diag.Config{})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if _, _, err := openStorage(Config{StoragePath: dir}, dg); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("err = %v", err)
	}
	p, _ := dg.Read(0, 10)
	if len(p.Events) != 1 || p.Events[0].Kind != diag.KindStorageOpen {
		t.Fatalf("события: %+v", p.Events)
	}
	if f, _ := p.Events[0].Get(diag.KeyClass); f.Str != "failed" {
		t.Fatalf("class = %q", f.Str)
	}
}

// Выключенная диагностика: хранилище не оборачивается — инстанс получает
// ровно то, что открыл адаптер (подставленное — тот же экземпляр).
func TestDiagDisabledLeavesStorageAsIs(t *testing.T) {
	inner := &countingStorage{Storage: openTestStorage(t)}
	d, err := New(Config{Storage: inner, Diagnostics: DiagnosticsConfig{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.Diag() != nil {
		t.Fatal("диагностика включена при Disabled")
	}
	if d.st != ports.Storage(inner) {
		t.Fatal("хранилище обёрнуто при выключенной диагностике")
	}
}

// Подставленное хранилище наблюдается, но закрывает его по-прежнему тот,
// кто его создал.
func TestDiagInjectedStorageOwnership(t *testing.T) {
	skipIfDiagForcedOff(t)
	inner := &countingStorage{Storage: openTestStorage(t)}
	d, err := New(Config{Storage: inner})
	if err != nil {
		t.Fatal(err)
	}
	if d.st == ports.Storage(inner) {
		t.Fatal("подставленное хранилище не наблюдается")
	}
	d.Close()
	if n := inner.closed.Load(); n != 0 {
		t.Fatalf("инстанс закрыл чужое хранилище %d раз", n)
	}
}

// Файл занят другим держателем — событие open с классом locked
// (ErrTimeout bbolt после OpenTimeout). Настоящий занятый файл, поэтому тест
// ждёт OpenTimeout (5 с) — укоротить его снаружи пакета адаптера нечем.
func TestDiagStorageOpenLocked(t *testing.T) {
	skipIfDiagForcedOff(t)
	if testing.Short() {
		t.Skip("ждёт OpenTimeout 5 с")
	}
	path := filepath.Join(t.TempDir(), "locked.db")
	holder, _, err := openStorage(Config{StoragePath: path}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close(context.Background())

	dg, _ := diag.New(diag.Config{})
	if _, _, err := openStorage(Config{StoragePath: path}, dg); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("err = %v", err)
	}
	p, _ := dg.Read(0, 10)
	if len(p.Events) != 1 || p.Events[0].Kind != diag.KindStorageOpen {
		t.Fatalf("события: %+v", p.Events)
	}
	f, _ := p.Events[0].Get(diag.KeyClass)
	dur, _ := p.Events[0].Get(diag.KeyDurUS)
	if f.Str != "locked" || dur.Num < 4_000_000 {
		t.Fatalf("class = %q, dur_us = %d (ожидание чужой блокировки — около OpenTimeout)", f.Str, dur.Num)
	}
}
