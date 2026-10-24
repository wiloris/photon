package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon/photon-core/adapters/bbolt"
	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-core/services/localfeed"
	"github.com/wiloris/photon/photon-core/services/outbox"
	"github.com/wiloris/photon/photon-core/services/session"
)

/*
 * Локальные действия — ADR-064, первый срез.
 *
 * Главное свойство, ради которого эти тесты существуют: действие 800
 * исполняется хранилищем инстанса и ДО СЕССИИ НЕ ДОХОДИТ. Транспорт здесь
 * фиктивный и считает подключения — если локальная ветка исчезнет, счётчик
 * перестанет быть нулём, и тест упадёт на этом, а не на форме ответа.
 */

// countingStorage — хранилище с наблюдаемым закрытием.
type countingStorage struct {
	ports.Storage
	closed atomic.Int32
}

func (s *countingStorage) Close(ctx context.Context) error {
	s.closed.Add(1)
	return s.Storage.Close(ctx)
}

// openTestStorage — настоящее bbolt во временном каталоге теста.
func openTestStorage(t *testing.T) ports.Storage {
	t.Helper()
	st, err := bboltadapter.New(filepath.Join(t.TempDir(), "feed.db"), testMigrations())
	if err != nil {
		t.Fatalf("открытие хранилища: %v", err)
	}
	return st
}

// localDispatcher — инстанс с локальным backend и фиктивным Bridge.
func localDispatcher(t *testing.T, st ports.Storage) (*Dispatcher, *fakeBridge) {
	t.Helper()
	br := newFakeBridge()
	d, err := New(Config{
		Endpoint:     "fake:4443",
		SessionToken: "tok",
		Transport:    br,
		Storage:      st,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, br
}

func seed(t *testing.T, st ports.Storage, channel string, recs ...localfeed.Record) {
	t.Helper()
	feed := localfeed.New(st, nil)
	if err := feed.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if err := feed.Put(context.Background(), channel, recs); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

// ── Маршрутизация ───────────────────────────────────────────────────────

func TestLocalAction_RoutedLocallyAndNeverReachesBridge(t *testing.T) {
	st := openTestStorage(t)
	seed(t, st, "ch", localfeed.Record{ID: "p1", OrderKey: 100, Version: 100, Body: []byte(`{"message":"первый"}`)})

	d, br := localDispatcher(t, st)

	resp, err := d.SendAction(actionLocalFeedPage, []byte(`{"channel_id":"ch","limit":10}`))
	if err != nil {
		t.Fatalf("SendAction(800): %v", err)
	}

	// Ключевая проверка среза: сессия не поднималась ни разу.
	if n := br.connectCount(); n != 0 {
		t.Fatalf("локальное действие ушло в Bridge: подключений %d, ожидалось 0", n)
	}
	if !strings.Contains(string(resp), `"ok":true`) {
		t.Fatalf("конверт без ok:true: %s", resp)
	}
	if !strings.Contains(string(resp), `"p1"`) {
		t.Fatalf("запись не попала в ответ: %s", resp)
	}
}

func TestLocalAction_ResourceForIsTheArbiter(t *testing.T) {
	// Арбитр один: диапазон 800-899 называет ResourceFor, а не проверка
	// внутри диспетчера. Мутация «сравнивать код с числом в SendAction»
	// разошлась бы с ним молча.
	res, err := session.ResourceFor(actionLocalFeedPage)
	if err != nil {
		t.Fatalf("ResourceFor(800): %v", err)
	}
	if res != session.ResourceLocalFeed {
		t.Fatalf("ResourceFor(800) = %q, ожидался %q", res, session.ResourceLocalFeed)
	}
}

func TestLocalAction_ReturnsRecordsInEnvelope(t *testing.T) {
	st := openTestStorage(t)
	seed(t, st, "ch",
		localfeed.Record{ID: "a", OrderKey: 100, Version: 101, Body: []byte(`{"n":1}`)},
		localfeed.Record{ID: "b", OrderKey: 200, Version: 201, Body: []byte(`{"n":2}`)},
	)
	d, _ := localDispatcher(t, st)

	resp, err := d.SendAction(actionLocalFeedPage, []byte(`{"channel_id":"ch","limit":10}`))
	if err != nil {
		t.Fatalf("SendAction: %v", err)
	}

	var env struct {
		OK   bool `json:"ok"`
		Data struct {
			Records []struct {
				ID       string          `json:"id"`
				OrderKey int64           `json:"order_key"`
				Version  int64           `json:"version"`
				Body     json.RawMessage `json:"body"`
			} `json:"records"`
			GapAfter bool `json:"gap_after"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp, &env); err != nil {
		t.Fatalf("конверт не разбирается: %v (%s)", err, resp)
	}
	if !env.OK || len(env.Data.Records) != 2 {
		t.Fatalf("ok=%v, записей %d, ожидалось 2: %s", env.OK, len(env.Data.Records), resp)
	}
	// Порядок — от новых к старым, как отдаёт Page.
	if env.Data.Records[0].ID != "b" || env.Data.Records[1].ID != "a" {
		t.Fatalf("порядок записей не от Page: %s", resp)
	}
	if env.Data.Records[0].Version != 201 || env.Data.Records[1].OrderKey != 100 {
		t.Fatalf("поля записи потеряны: %s", resp)
	}
	// Body проходит сырым: пересборка через float64 потеряла бы точность.
	if string(env.Data.Records[0].Body) != `{"n":2}` {
		t.Fatalf("body пересобран: %s", env.Data.Records[0].Body)
	}
}

func TestLocalAction_BodyPreservedVerbatim(t *testing.T) {
	st := openTestStorage(t)
	body := []byte(`{"create_at":1758181234567,"big":123456789012345678901234567890,"e":1e3}`)
	seed(t, st, "ch", localfeed.Record{ID: "p", OrderKey: 1, Version: 1, Body: body})
	d, _ := localDispatcher(t, st)

	resp, err := d.SendAction(actionLocalFeedPage, []byte(`{"channel_id":"ch","limit":1}`))
	if err != nil {
		t.Fatalf("SendAction: %v", err)
	}
	if !strings.Contains(string(resp), string(body)) {
		t.Fatalf("body изменён при сборке конверта:\n got: %s\nwant содержит: %s", resp, body)
	}
}

// ── Инстанс без локального backend ──────────────────────────────────────

func TestLocalAction_WithoutStorageFailsExplicitly(t *testing.T) {
	br := newFakeBridge()
	d, err := New(Config{Endpoint: "fake:4443", SessionToken: "t", Transport: br})
	if err != nil {
		t.Fatalf("New без storage обязан удаться: %v", err)
	}
	defer d.Close()

	_, err = d.SendAction(actionLocalFeedPage, []byte(`{"channel_id":"ch"}`))
	if !errors.Is(err, ErrNoStorage) {
		t.Fatalf("ошибка = %v, ожидался ErrNoStorage", err)
	}
	if n := br.connectCount(); n != 0 {
		t.Fatalf("инстанс без storage ушёл в Bridge: подключений %d", n)
	}
	// Класс отказа тот же, что у сетевого действия без адресата.
	if got := Classify(err); got != ClassNotConnected {
		t.Fatalf("класс = %v, ожидался ClassNotConnected", got)
	}
}

// ── Время жизни ─────────────────────────────────────────────────────────

func TestClose_ClosesOwnedStorageOnly(t *testing.T) {
	// Подставленное через шов хранилище закрывает тот, кто его создал —
	// ровно как с транспортом.
	st := &countingStorage{Storage: openTestStorage(t)}
	d, _ := localDispatcher(t, st)

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := st.closed.Load(); n != 0 {
		t.Fatalf("Close закрыл чужое хранилище (%d раз)", n)
	}
	_ = st.Storage.Close(context.Background())
}

func TestClose_ClosesStorageOpenedByInstance(t *testing.T) {
	// Хранилище, открытое по пути из конфигурации, принадлежит инстансу.
	// Доказательство закрытия — повторное открытие того же файла: bbolt
	// держит эксклюзивную блокировку, и незакрытый файл открыть нельзя.
	path := filepath.Join(t.TempDir(), "feed.db")
	d, err := New(Config{StoragePath: path})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	again, err := New(Config{StoragePath: path})
	if err != nil {
		t.Fatalf("после Close файл остался заблокирован — хранилище не закрыто: %v", err)
	}
	_ = again.Close()
}

// ── Отказ открытия ──────────────────────────────────────────────────────

func TestNew_StorageOpenFailureDoesNotCreateInstance(t *testing.T) {
	// Путь указывает на каталог: конфигурация годна, открыть нельзя.
	dir := filepath.Join(t.TempDir(), "notafile")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	d, err := New(Config{StoragePath: dir})
	if err == nil {
		_ = d.Close()
		t.Fatal("New обязан был отказать: по этому пути хранилище не открыть")
	}
	if d != nil {
		t.Fatal("при отказе инстанс возвращаться не должен")
	}
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("ошибка = %v, ожидался ErrStorageUnavailable", err)
	}
	// Класс отличает недоступное хранилище от негодных аргументов: за него
	// вызывающему нечего исправлять в конфигурации.
	if got := Classify(err); got != ClassInternal {
		t.Fatalf("класс = %v, ожидался ClassInternal", got)
	}
}

// ── Негодный payload ────────────────────────────────────────────────────

func TestLocalAction_InvalidPayloadRejected(t *testing.T) {
	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)

	for _, bad := range []string{`{}`, `{"channel_id":""}`} {
		_, err := d.SendAction(actionLocalFeedPage, []byte(bad))
		if !errors.Is(err, session.ErrInvalidPayload) {
			t.Fatalf("payload %s: ошибка = %v, ожидался ErrInvalidPayload", bad, err)
		}
	}
}

// testMigrations — тот же список, что строит openStorage: лента (v1) и
// очередь исходящих (v2, ADR-093 §2.8).
func testMigrations() []bboltadapter.Migration {
	return []bboltadapter.Migration{
		{Version: localfeed.SchemaVersion, Up: func(tx *bolt.Tx) error {
			_, e := tx.CreateBucketIfNotExists([]byte(localfeed.Namespace))
			return e
		}},
		{Version: 2, Up: func(tx *bolt.Tx) error {
			_, e := tx.CreateBucketIfNotExists([]byte(outbox.Namespace))
			return e
		}},
	}
}
