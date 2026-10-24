package main

// Лабораторная композиция coreops и закрытый каталог сценариев (ADR-058,
// docs/plugin-lab.md).
//
// ЭТО НЕ ОБРАЗЕЦ ДЛЯ ПРОДАКШН-ХОСТА. Здесь — единственное место в не-тестовом
// коде photon-plugin-runtime, где coreops собирается с настоящими адаптерами,
// и каждый выбор ниже — ЛАБОРАТОРНОЕ исключение, а не ответ на
// core-operations.md:
//
//   - ключи в памяти и на один запуск — не ответ на §11.1: требования
//     «переживать перезапуск» и «один на устройство» для продакшна не меняются;
//   - Migrate зовёт сам процесс — не ответ на §11.2: здесь набор фикстур закрыт
//     и известен до старта, в продакшне — нет;
//   - namespace на фикстуру — не выбор продакшн-схемы §5.
//
// Код живёт в package main и импортировать его нечем: общий пакет композиции
// был бы тем путём, которым лабораторные ответы стали бы продакшн-ответами
// (ADR-058 п. 1). Структурный тест это сторожит.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	bolt "go.etcd.io/bbolt"

	"github.com/wiloris/photon/photon-core/adapters/aesgcm"
	bboltadapter "github.com/wiloris/photon/photon-core/adapters/bbolt"
	ed25519adapter "github.com/wiloris/photon/photon-core/adapters/ed25519"
	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-plugin-runtime/coreops"
	"github.com/wiloris/photon/photon-plugin-runtime/ipc"
)

// ── закрытый каталог (plugin-lab.md §3) ──────────────────────────────────────

const (
	fixtureHello       = "hello"
	fixtureSDKScenario = "sdkscenario"

	defaultScenario = "hello"
)

// namespaceOf — отдельный namespace на фикстуру (ADR-058 п. 5). Идентичности
// внутри одной фикстуры делят его и разделяются plugin_id в ключе coreops.
var namespaceOf = map[string]string{
	fixtureHello:       "lab.hello",
	fixtureSDKScenario: "lab.sdkscenario",
}

// fixtureEnv — окружение, которым сценарий передаётся фикстуре. Все имена
// выставляются при каждом load, пустыми если сценарий их не задаёт
// (plugin-lab.md §4.4).
var fixtureEnv = []string{
	"PHOTON_TEST_MODE",
	"PHOTON_TEST_PLUGIN_ID",
	"PHOTON_TEST_VALUE",
	"PHOTON_TEST_BINDING",
	"PHOTON_TEST_DOMAIN_INPUT",
}

var (
	grantsHello = []string{"storage.read"}
	grantsA     = []string{"storage.read", "storage.write", "identity.read", "identity.sign", "crypto.use"}
	grantsB     = []string{"storage.read", "storage.write", "identity.read", "crypto.use"}
)

// scenario — строка каталога. Полностью определяет запуск: фикстуру,
// идентичность, права, режим, окружение и то, какие значения плагина
// разрешено показывать. Из канала приходит только ID.
type scenario struct {
	ID          string
	Fixture     string
	PluginID    string
	Granted     []string
	Mode        string
	Description string

	// Env — значения окружения сверх MODE и PLUGIN_ID.
	Env map[string]string

	// Show — имена значений плагина, которые попадают в results. Остальное
	// отбрасывается ДО ответа: фильтр живёт в каталоге, а не у клиента (§8).
	Show []string

	// Bootstrap — лабораторная предзагрузка исходного состояния (§3.1).
	Bootstrap bool
}

var catalog = []scenario{
	{
		ID: "hello", Fixture: fixtureHello, PluginID: "com.engram.test.hello",
		Granted:     grantsHello,
		Description: "hello читает значение, записанное предзагрузкой лаборатории",
		Show:        []string{"ready"},
		Bootstrap:   true,
	},
	{
		ID: "sdk-storage-roundtrip", Fixture: fixtureSDKScenario, PluginID: "com.engram.test.sdkscenario-a",
		Granted: grantsA, Mode: "storage-roundtrip",
		Description: "put, get, list, delete, get на настоящем bbolt",
		Show:        []string{"roundtrip", "after_delete", "done"},
	},
	{
		ID: "sdk-sign", Fixture: fixtureSDKScenario, PluginID: "com.engram.test.sdkscenario-a",
		Granted: grantsA, Mode: "sign",
		Description: "ключ устройства и подпись от настоящего исполнителя",
		// Доменный вход ADR-037 лаборатория НЕ строит (ADR-058 п. 8), поэтому
		// verify_domain — проверка по пустым байтам — не показывается.
		Env:  map[string]string{"PHOTON_TEST_VALUE": base64.StdEncoding.EncodeToString([]byte("photon-plugin-lab"))},
		Show: []string{"pub", "sig", "verify_raw", "done"},
	},
	{
		ID: "sdk-sign-forbidden", Fixture: fixtureSDKScenario, PluginID: "com.engram.test.sdkscenario-b",
		Granted: grantsB, Mode: "sign",
		Description: "отказ без identity.sign; операция не доходит до исполнителя",
		Env:         map[string]string{"PHOTON_TEST_VALUE": base64.StdEncoding.EncodeToString([]byte("photon-plugin-lab"))},
		Show:        []string{"pub", "sig", "done"},
	},
}

func findScenario(id string) (scenario, bool) {
	for _, s := range catalog {
		if s.ID == id {
			return s, true
		}
	}
	return scenario{}, false
}

// ── лабораторная композиция (plugin-lab.md §4–§6) ────────────────────────────

// lab — одноразовое хранилище и ключевой материал экземпляра.
type lab struct {
	dir    string
	st     ports.Storage
	id     *ed25519adapter.Identity
	device ports.IdentityPair
	cr     *aesgcm.Adapter
	master ports.SymmetricKey

	closeOnce sync.Once
}

// newLab собирает всё, что нужно до ready. Частично собранная лаборатория
// убирает за собой и готовность не объявляет.
func newLab(ctx context.Context) (*lab, error) {
	dir, err := os.MkdirTemp("", "photon-plugin-lab-*")
	if err != nil {
		return nil, fmt.Errorf("temporary directory: %w", err)
	}
	l := &lab{dir: dir}

	st, err := bboltadapter.New(filepath.Join(dir, "lab.db"), []bboltadapter.Migration{{
		Version: 1,
		Up: func(tx *bolt.Tx) error {
			for _, ns := range namespaceOf {
				if _, e := tx.CreateBucketIfNotExists([]byte(ns)); e != nil {
					return e
				}
			}
			return nil
		},
	}})
	if err != nil {
		l.close()
		return nil, fmt.Errorf("open storage: %w", err)
	}
	l.st = st
	// Migrate зовёт сам процесс: лабораторное решение по §11.2 (ADR-058 п. 4).
	if err := st.Migrate(ctx, 1); err != nil {
		l.close()
		return nil, fmt.Errorf("migrate storage: %w", err)
	}

	l.id = ed25519adapter.New()
	if l.device, err = l.id.Generate(ctx); err != nil {
		l.close()
		return nil, fmt.Errorf("device key: %w", err)
	}
	l.cr = aesgcm.New()
	if l.master, err = l.cr.GenerateKey(ctx); err != nil {
		l.close()
		return nil, fmt.Errorf("master key: %w", err)
	}
	return l, nil
}

// executor — настоящий coreops для namespace фикстуры.
func (l *lab) executor(namespace string, sink func(pluginID, level, message string)) (*coreops.Handler, error) {
	return coreops.New(coreops.Config{
		Storage:          l.st,
		StorageNamespace: namespace,
		Identity:         l.id,
		DeviceKey:        l.device.Private,
		Crypto:           l.cr,
		DeriveKey: func(ctx context.Context, domain string) (ports.SymmetricKey, error) {
			return l.cr.DeriveKey(ctx, l.master, domain)
		},
		Log: sink,
	})
}

// bootstrapHello — лабораторная предзагрузка фикстуры hello (plugin-lab.md
// §3.1). Действие доверенного владельца экземпляра, ДО запуска плагина:
// намеренно мимо Authorizer и мимо наблюдателя. Не модель обычного пути
// исполнения плагина и не прецедент для продакшна.
func bootstrapHello(ctx context.Context, exec *coreops.Handler, pluginID string) error {
	payload, err := json.Marshal(map[string]any{
		"namespace": "greeting", "key": "message", "value": []byte("hello-value"),
	})
	if err != nil {
		return err
	}
	_, err = exec.Handle(ctx, ipc.CallerInfo{PluginID: pluginID}, "storage.put", payload)
	return err
}

// close — штатная уборка: закрыть хранилище, удалить каталог. Вызывается
// только ПОСЛЕ остановки плагина. После kill -9 не вызывается вовсе, и
// спецификация этого не обещает (§6.2).
func (l *lab) close() {
	l.closeOnce.Do(func() {
		if l.st != nil {
			if err := l.st.Close(context.Background()); err != nil {
				fmt.Fprintf(os.Stderr, "[devtools-runtime] закрытие хранилища: %v\n", err)
			}
		}
		// Best-effort: Go не гарантирует, что копий ключа не осталось.
		for i := range l.master {
			l.master[i] = 0
		}
		for i := range l.device.Private {
			l.device.Private[i] = 0
		}
		if err := os.RemoveAll(l.dir); err != nil {
			fmt.Fprintf(os.Stderr, "[devtools-runtime] удаление временного каталога: %v\n", err)
		}
	})
}

// labV — граница лаборатории в ответах канала. Ключей здесь нет и быть не может.
type labV struct {
	KeyMaterial           string `json:"key_material"`
	Storage               string `json:"storage"`
	StorageDir            string `json:"storage_dir"`
	ProductionComposition bool   `json:"production_composition"`
}

func (l *lab) info() *labV {
	return &labV{KeyMaterial: "ephemeral", Storage: "temporary", StorageDir: l.dir, ProductionComposition: false}
}

// ── результаты плагина ───────────────────────────────────────────────────────

// splitResult разбирает строку лога фикстуры "имя:значение". Строка без
// двоеточия — имя без значения (так hello сообщает "ready").
func splitResult(message string) (string, string) {
	name, value, _ := strings.Cut(message, ":")
	return name, value
}

// displayValue — форма показа. Подпись показывается длиной и началом (§8):
// целиком она ничего не добавляет к наблюдению.
func displayValue(name, value string) string {
	if name != "sig" {
		return value
	}
	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(raw) == 0 {
		return value // FORBIDDEN и прочие исходы — как есть
	}
	head := value
	if len(head) > 16 {
		head = head[:16] + "…"
	}
	return fmt.Sprintf("len=%d b64=%s", len(raw), head)
}

func allowed(s scenario, name string) bool {
	for _, n := range s.Show {
		if n == name {
			return true
		}
	}
	return false
}
