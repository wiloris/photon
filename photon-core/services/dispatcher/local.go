package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	"github.com/wiloris/photon-core/ports"
	photonpb "github.com/wiloris/photon-core/protobuf"
	"github.com/wiloris/photon-core/services/localfeed"
	"github.com/wiloris/photon-core/services/session"
)

/*
 * Локальные действия (ADR-064).
 *
 * Здесь живёт всё, что отличает локальное действие от сетевого: открытие
 * хранилища инстанса, его закрытие и исполнение кодов диапазона 800-899.
 *
 * Чего здесь НЕТ и по решению ADR-064 быть не должно: записи (Put, Ingest),
 * синхронизации с Bridge, доменной модели. Первый срез — одно действие на
 * чтение, и оно доказывает цепочку целиком.
 */

// actionLocalFeedPage — код действия «страница локальной ленты».
//
// Берётся из канонического enum, а не вписан числом: источник кодов один
// (ADR-025), и расхождение между ним и диспетчером было бы невидимым.
const actionLocalFeedPage = int32(photonpb.Action_ACTION_LOCAL_FEED_PAGE)

// openStorage открывает локальное хранилище по конфигурации.
//
// Возвращает (nil, false, nil), когда backend не настроен: это законное
// состояние инстанса (ADR-064 п. 1.5), а не ошибка.
//
// Второе значение — владение: хранилище, подставленное через шов
// Config.Storage, закрывает тот, кто его создал, ровно как с транспортом.
func openStorage(cfg Config) (ports.Storage, bool, error) {
	if cfg.Storage != nil {
		return cfg.Storage, false, nil
	}
	if cfg.StoragePath == "" {
		return nil, false, nil
	}

	// Миграция bbolt-специфична намеренно (adapters/bbolt, комментарий к
	// Migration): порт namespace не создаёт, и абстрагировать создание
	// bucket'а через ports было бы выдумыванием общего там, где его нет.
	// Диспетчер уже строит конкретный adapters/quic — см. newQUICTransport.
	st, err := bboltadapter.New(cfg.StoragePath, []bboltadapter.Migration{{
		Version: localfeed.SchemaVersion,
		Up: func(tx *bolt.Tx) error {
			_, e := tx.CreateBucketIfNotExists([]byte(localfeed.Namespace))
			return e
		},
	}})
	if err != nil {
		return nil, false, fmt.Errorf("%w: open %s: %v", ErrStorageUnavailable, cfg.StoragePath, err)
	}
	return st, true, nil
}

// prepareFeed выполняет миграцию схемы ленты.
//
// Migrate зовёт инстанс, сразу после открытия (ADR-064 п. 1.7): набор
// namespace здесь закрыт и известен — один. Вопрос §11.2 core-operations.md
// («кто зовёт Migrate, когда список плагинов не известен заранее») этим не
// закрывается и не затрагивается.
func prepareFeed(ctx context.Context, st ports.Storage) (*localfeed.Store, error) {
	feed := localfeed.New(st, nil)
	if err := feed.Prepare(ctx); err != nil {
		return nil, fmt.Errorf("%w: migrate: %v", ErrStorageUnavailable, err)
	}
	return feed, nil
}

// ── Действие 800: страница локальной ленты ──────────────────────────────

// feedPageRequest — payload_json действия ACTION_LOCAL_FEED_PAGE.
//
// Поля повторяют параметры localfeed.Store.Page и ничего к ним не добавляют:
// срез делает существующий метод достижимым, а не проектирует новый.
type feedPageRequest struct {
	ChannelID string `json:"channel_id"`
	AfterKey  int64  `json:"after_key"`
	AfterID   string `json:"after_id"`
	Limit     int    `json:"limit"`
}

// localAction исполняет код диапазона 800-899.
//
// Ответ собирается тем же envelope, что и ответ Bridge: клиент не обязан
// различать, откуда пришли данные, — форма конверта {ok,data,error} одна
// (ADR-038, ADR-064 п. 2.4).
func (d *Dispatcher) localAction(ctx context.Context, action int32, payload []byte) ([]byte, error) {
	d.mu.Lock()
	feed := d.feed
	d.mu.Unlock()

	if feed == nil {
		return nil, ErrNoStorage
	}

	switch action {
	case actionLocalFeedPage:
		return feedPage(ctx, feed, payload)
	}

	// Код в диапазоне есть, обработчика нет: такое состояние возможно только
	// между добавлением кода в enum и его реализацией. Молчать нельзя —
	// клиент получил бы пустой успех.
	return nil, fmt.Errorf("%w: %d", session.ErrUnknownAction, action)
}

func feedPage(ctx context.Context, feed *localfeed.Store, payload []byte) ([]byte, error) {
	var req feedPageRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("%w: %v", session.ErrInvalidPayload, err)
	}
	if req.ChannelID == "" {
		return nil, fmt.Errorf("%w: channel_id is required", session.ErrInvalidPayload)
	}

	page, err := feed.Page(ctx, req.ChannelID, req.AfterKey, req.AfterID, req.Limit)
	if err != nil {
		return nil, fmt.Errorf("%w: page: %v", ErrStorageUnavailable, err)
	}

	data, err := encodePage(page)
	if err != nil {
		return nil, err
	}
	// Тот же конверт, что у сетевого ответа. ok:true, потому что вердикта у
	// локального чтения не бывает: оно либо прочитало, либо отказало вызовом.
	return envelope(session.Response{OK: true, Data: data})
}

// encodePage собирает data конверта.
//
// Body записи вставляется СЫРЫМИ байтами — по тому же правилу, что data
// события (events.go): ядро их не разбирает, а пересборка через float64
// потеряла бы точность чисел.
func encodePage(p localfeed.PageResult) ([]byte, error) {
	var b bytes.Buffer
	b.WriteString(`{"records":[`)
	for i, r := range p.Records {
		if i > 0 {
			b.WriteByte(',')
		}
		id, err := json.Marshal(r.ID)
		if err != nil {
			return nil, fmt.Errorf("dispatcher: encode record id: %w", err)
		}
		b.WriteString(`{"id":`)
		b.Write(id)
		b.WriteString(`,"order_key":`)
		b.WriteString(strconv.FormatInt(r.OrderKey, 10))
		b.WriteString(`,"version":`)
		b.WriteString(strconv.FormatInt(r.Version, 10))
		if len(r.Body) > 0 {
			if !json.Valid(r.Body) {
				return nil, fmt.Errorf("%w: record %s body is not valid JSON", ErrStorageUnavailable, r.ID)
			}
			b.WriteString(`,"body":`)
			b.Write(r.Body)
		}
		b.WriteByte('}')
	}
	b.WriteString(`],"gap_after":`)
	b.WriteString(strconv.FormatBool(p.GapAfter))
	b.WriteByte('}')
	return b.Bytes(), nil
}
