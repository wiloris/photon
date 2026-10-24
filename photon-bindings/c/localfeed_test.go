package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unsafe"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon/photon-core/adapters/bbolt"
	"github.com/wiloris/photon/photon-core/services/localfeed"
)

/*
 * Локальная лента через границу ABI — первый срез ADR-064.
 *
 * Здесь проверяется то, чего не видно с уровня диспетчера: что цепочка
 * Photon_Create → config_json(storage) → Photon_SendAction(800) →
 * localfeed.Store.Page → response_json работает целиком, через настоящие
 * экспортируемые функции.
 *
 * Сети в этих тестах нет вовсе: инстанс создаётся БЕЗ секции bridge, и это
 * законное состояние (ADR-064 п. 1.5) — локальное действие адресата не
 * требует.
 */

const actionLocalFeedPageC = 800

// seedFeed кладёт записи в хранилище ДО создания инстанса: записи в срез не
// входят (ADR-064 п. 3), и брать их неоткуда, кроме как напрямую.
func seedFeed(t *testing.T, path, channel string, recs ...localfeed.Record) {
	t.Helper()
	st, err := bboltadapter.New(path, []bboltadapter.Migration{{
		Version: localfeed.SchemaVersion,
		Up: func(tx *bolt.Tx) error {
			_, e := tx.CreateBucketIfNotExists([]byte(localfeed.Namespace))
			return e
		},
	}})
	if err != nil {
		t.Fatalf("открытие хранилища: %v", err)
	}
	feed := localfeed.New(st, nil)
	if err := feed.Prepare(context.Background()); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if err := feed.Put(context.Background(), channel, recs); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func storageConfig(t *testing.T, path string) unsafe.Pointer {
	t.Helper()
	return cstr(t, fmt.Sprintf(`{"storage":{"path":%q}}`, path))
}

// ── Цепочка целиком ─────────────────────────────────────────────────────

func TestLocalFeed_PageThroughABI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "feed.db")
	seedFeed(t, path, "ch",
		localfeed.Record{ID: "a", OrderKey: 100, Version: 101, Body: []byte(`{"message":"старое"}`)},
		localfeed.Record{ID: "b", OrderKey: 200, Version: 201, Body: []byte(`{"message":"новое"}`)},
	)

	h, rc := tsCreate(storageConfig(t, path), false)
	if rc != tsOK || h == nil {
		t.Fatalf("Photon_Create со storage: rc = %d", rc)
	}
	defer tsDestroy(h)

	rc, respOut := tsSendAction(h, actionLocalFeedPageC, cstr(t, `{"channel_id":"ch","limit":10}`), true)
	if rc != tsOK {
		t.Fatalf("SendAction(800): rc = %d, ожидался PHOTON_OK", rc)
	}
	if respOut == nil {
		t.Fatal("response_out не установлен при PHOTON_OK")
	}
	defer tsFreeString(respOut)

	body := tsGoString(tsStringData(respOut))

	var env struct {
		OK   bool `json:"ok"`
		Data struct {
			Records []struct {
				ID   string          `json:"id"`
				Body json.RawMessage `json:"body"`
			} `json:"records"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatalf("ответ не разбирается: %v (%s)", err, body)
	}
	if !env.OK {
		t.Fatalf("ok != true: %s", body)
	}
	if len(env.Data.Records) != 2 || env.Data.Records[0].ID != "b" {
		t.Fatalf("записи не те: %s", body)
	}
	if string(env.Data.Records[0].Body) != `{"message":"новое"}` {
		t.Fatalf("body изменён: %s", env.Data.Records[0].Body)
	}
}

// ── Инстанс без локального backend ──────────────────────────────────────

func TestLocalFeed_WithoutStorageInstanceIsValid(t *testing.T) {
	// Отсутствие секции storage — НЕ ошибка создания (ADR-064 п. 1.5).
	h, rc := tsCreate(cstr(t, `{}`), false)
	if rc != tsOK || h == nil {
		t.Fatalf("инстанс без storage обязан создаваться: rc = %d", rc)
	}
	defer tsDestroy(h)

	rc, respOut := tsSendAction(h, actionLocalFeedPageC, cstr(t, `{"channel_id":"ch"}`), true)
	if rc != tsErrNotConnected {
		t.Fatalf("rc = %d, ожидался PHOTON_ERR_NOT_CONNECTED: локального backend нет", rc)
	}
	if respOut != nil {
		t.Fatal("при не-OK response_out устанавливаться не должен")
	}

	// Причина доступна текстом — ровно для этого GetLastError и существует.
	if msg := tsGoString(tsGetLastError(h)); !strings.Contains(msg, "storage") {
		t.Fatalf("last error = %q, ожидалось упоминание storage", msg)
	}
}

// ── Отказ открытия ──────────────────────────────────────────────────────

func TestLocalFeed_UnopenableStorageFailsCreateWithInternal(t *testing.T) {
	// Путь указывает на каталог: конфигурация годна, открыть нельзя.
	dir := filepath.Join(t.TempDir(), "notafile")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	h, rc := tsCreate(storageConfig(t, dir), false)
	if rc != tsErrInternal {
		t.Fatalf("rc = %d, ожидался PHOTON_ERR_INTERNAL (ADR-064 п. 1.6)", rc)
	}
	if h != nil {
		t.Fatal("при отказе открытия *handle обязан остаться NULL (ADR-027/028)")
	}
}

func TestLocalFeed_MalformedStorageSectionIsInvalidArgument(t *testing.T) {
	// Негодная конфигурация — ошибка вызывающего, другой код.
	h, rc := tsCreate(cstr(t, `{"storage":{"path":42}}`), false)
	if rc != tsErrInvalidArgument {
		t.Fatalf("rc = %d, ожидался PHOTON_ERR_INVALID_ARGUMENT", rc)
	}
	if h != nil {
		t.Fatal("*handle обязан остаться NULL")
	}
}

// ── Время жизни ─────────────────────────────────────────────────────────

func TestLocalFeed_DestroyClosesStorage(t *testing.T) {
	// Доказательство закрытия — повторное открытие того же файла: bbolt
	// держит эксклюзивную блокировку, и незакрытый файл открыть нельзя.
	path := filepath.Join(t.TempDir(), "feed.db")

	h, rc := tsCreate(storageConfig(t, path), false)
	if rc != tsOK {
		t.Fatalf("Create: rc = %d", rc)
	}
	tsDestroy(h)

	again, rc := tsCreate(storageConfig(t, path), false)
	if rc != tsOK {
		t.Fatalf("после Destroy файл остался заблокирован — хранилище не закрыто (rc = %d)", rc)
	}
	tsDestroy(again)
}

// Регрессия: занятый файл не подвешивает Photon_Create (ADR-064 п. 1.6).
//
// До исправления bboltadapter.New открывал bbolt без таймаута, и второй
// инстанс на том же файле ждал в Photon_Create бесконечно. Второй Create
// идёт в горутине под сторожем: тест не может зависнуть, даже если дефект
// вернётся, — он упадёт по сторожу.
func TestLocalFeed_LockedStorageFailsCreateWithinTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "feed.db")
	seedFeed(t, path, "ch",
		localfeed.Record{ID: "a", OrderKey: 100, Version: 101, Body: []byte(`{"message":"x"}`)},
	)

	first, rc := tsCreate(storageConfig(t, path), false)
	if rc != tsOK || first == nil {
		t.Fatalf("первый Create: rc = %d", rc)
	}
	defer tsDestroy(first)

	type result struct {
		h  unsafe.Pointer
		rc int
	}
	cfg := storageConfig(t, path)
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		h, rc := tsCreate(cfg, false)
		done <- result{h, rc}
	}()

	var r result
	watchdog := bboltadapter.OpenTimeout + 15*time.Second
	select {
	case r = <-done:
	case <-time.After(watchdog):
		t.Fatalf("второй Photon_Create не вернулся за %v: открытие занятого файла снова ждёт бесконечно", watchdog)
	}
	elapsed := time.Since(start)

	if r.rc != tsErrInternal {
		if r.h != nil {
			tsDestroy(r.h)
		}
		t.Fatalf("rc = %d, ожидался PHOTON_ERR_INTERNAL: файл занят (ADR-064 п. 1.6)", r.rc)
	}
	if r.h != nil {
		t.Fatal("при отказе *handle обязан остаться NULL (ADR-027/028)")
	}
	if elapsed > bboltadapter.OpenTimeout+5*time.Second {
		t.Fatalf("отказ за %v — дольше таймаута открытия %v", elapsed, bboltadapter.OpenTimeout)
	}
	t.Logf("второй Create отказал за %v (таймаут %v)", elapsed, bboltadapter.OpenTimeout)

	// Первый инстанс продолжает работать.
	rc2, respOut := tsSendAction(first, actionLocalFeedPageC, cstr(t, `{"channel_id":"ch","limit":10}`), true)
	if rc2 != tsOK || respOut == nil {
		t.Fatalf("первый инстанс после отказа второго: SendAction(800) rc = %d", rc2)
	}
	defer tsFreeString(respOut)
	if body := tsGoString(tsStringData(respOut)); !strings.Contains(body, `"id":"a"`) {
		t.Fatalf("первый инстанс после отказа второго читает не то: %s", body)
	}
}
