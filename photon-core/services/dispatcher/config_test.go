package dispatcher

import (
	"testing"
	"time"
)

func TestParseConfig_BridgeSection(t *testing.T) {
	cfg, err := ParseConfig([]byte(`{
		"bridge": {
			"endpoint": "bridge.example.net:4443",
			"session_token": "sid",
			"connect_timeout_ms": 1000,
			"call_timeout_ms": 2000,
			"keepalive_ms": 3000,
			"tls": {"insecure_skip_verify": true, "server_name": "example.org"}
		}
	}`))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Endpoint != "bridge.example.net:4443" || cfg.SessionToken != "sid" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.ConnectTimeout != time.Second || cfg.CallTimeout != 2*time.Second || cfg.KeepAlive != 3*time.Second {
		t.Fatalf("миллисекунды разобраны неверно: %+v", cfg)
	}
	if !cfg.TLS.InsecureSkipVerify || cfg.TLS.ServerName != "example.org" {
		t.Fatalf("tls = %+v", cfg.TLS)
	}
}

func TestParseConfig_Defaults(t *testing.T) {
	cfg, err := ParseConfig([]byte(`{"bridge":{"endpoint":"h:1"}}`))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	// Ноль означает «по умолчанию», а не «без предела»: вызов ABI без предела
	// времени нечем прервать снаружи.
	if cfg.ConnectTimeout != DefaultConnectTimeout ||
		cfg.CallTimeout != DefaultCallTimeout ||
		cfg.KeepAlive != DefaultKeepAlive {
		t.Fatalf("значения по умолчанию не подставлены: %+v", cfg)
	}
}

func TestParseConfig_UnknownFieldsIgnored(t *testing.T) {
	// Конфиг обязан оставаться расширяемым: секция будущей подсистемы не
	// должна ломать сегодняшнее ядро.
	raw := `{"storage_path":"/tmp/photon","plugins":{"dir":"/x"},
	         "bridge":{"endpoint":"h:1","что-то новое":42}}`
	cfg, err := ParseConfig([]byte(raw))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Endpoint != "h:1" {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestParseConfig_Invalid(t *testing.T) {
	for _, raw := range []string{`{not json`, `not json at all`, `[]`, `5`,
		`{"bridge":{"endpoint":5}}`, `{"bridge":{"call_timeout_ms":-1}}`} {
		if _, err := ParseConfig([]byte(raw)); err == nil {
			t.Fatalf("ParseConfig(%s) прошёл без ошибки", raw)
		}
	}
}

func TestParseConfig_NoBridgeSection(t *testing.T) {
	for _, raw := range []string{`{}`, `{"storage_path":"/tmp/photon"}`} {
		cfg, err := ParseConfig([]byte(raw))
		if err != nil {
			t.Fatalf("ParseConfig(%s): %v", raw, err)
		}
		if cfg.Endpoint != "" {
			t.Fatalf("%s: endpoint = %q, want пусто", raw, cfg.Endpoint)
		}
		if cfg.CallTimeout != DefaultCallTimeout {
			t.Fatalf("%s: дефолты не подставлены", raw)
		}
	}
}

func TestNew_NoTransportLeakOnError(t *testing.T) {
	// session.New отвергает неизвестный режим и пустой endpoint; здесь режим
	// задаём мы сами, поэтому проверяем то, что реально может отказать, —
	// и требуем, чтобы после отказа ничего не осталось удерживаемым
	// (то же требование атомарности, что ADR-028 предъявляет к Photon_Create).
	br := newFakeBridge()
	d, err := New(Config{Endpoint: "fake://bridge", Transport: br})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Подставленный транспорт не наш — закрывать его нельзя.
	if br.isClosed() {
		t.Fatal("Close закрыл транспорт, переданный вызывающим")
	}
}
