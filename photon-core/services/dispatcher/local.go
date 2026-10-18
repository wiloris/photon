package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	"github.com/wiloris/photon-core/ports"
	photonpb "github.com/wiloris/photon-core/protobuf"
	"github.com/wiloris/photon-core/services/localfeed"
	"github.com/wiloris/photon-core/services/outbox"
	"github.com/wiloris/photon-core/services/session"
)

/*
 * Локальные действия (ADR-064, ADR-065).
 *
 * Здесь живёт всё, что отличает локальное действие от сетевого: открытие
 * хранилища инстанса, его закрытие и исполнение кодов диапазона 800-899.
 *
 * Прежняя редакция этой шапки говорила, что записи (Put, Ingest) здесь нет и
 * «по решению ADR-064 быть не должно». Первая половина перестала быть правдой
 * с ADR-065, вторая была неточна уже тогда: ADR-064 запись не запрещал, он
 * оставил её следующему решению вместе с вопросом «откуда берутся записи».
 *
 * Чего здесь НЕТ и не появляется: синхронизации с Bridge, доменной модели,
 * планировщика. Действие исполняется либо сессией, либо хранилищем — третьего
 * рода маршрутизации не заводится (ADR-065 п. 11.2).
 */

// Коды локальных действий.
//
// Берутся из канонического enum, а не вписаны числами: источник кодов один
// (ADR-025), и расхождение между ним и диспетчером было бы невидимым.
const (
	actionLocalFeedPage      = int32(photonpb.Action_ACTION_LOCAL_FEED_PAGE)
	actionLocalFeedPut       = int32(photonpb.Action_ACTION_LOCAL_FEED_PUT)
	actionLocalFeedIngest    = int32(photonpb.Action_ACTION_LOCAL_FEED_INGEST)
	actionLocalFeedGapAnchor = int32(photonpb.Action_ACTION_LOCAL_FEED_GAP_ANCHOR)
	actionLocalFeedCatalog   = int32(photonpb.Action_ACTION_LOCAL_FEED_CATALOG)
	actionLocalFeedCursor    = int32(photonpb.Action_ACTION_LOCAL_FEED_CURSOR)
	actionOutboxEnqueue      = int32(photonpb.Action_ACTION_OUTBOX_ENQUEUE)
	actionOutboxLease        = int32(photonpb.Action_ACTION_OUTBOX_LEASE)
	actionOutboxComplete     = int32(photonpb.Action_ACTION_OUTBOX_COMPLETE)
	actionOutboxFail         = int32(photonpb.Action_ACTION_OUTBOX_FAIL)
	actionOutboxRetry        = int32(photonpb.Action_ACTION_OUTBOX_RETRY)
	actionOutboxCancel       = int32(photonpb.Action_ACTION_OUTBOX_CANCEL)
	actionOutboxList         = int32(photonpb.Action_ACTION_OUTBOX_LIST)
	actionLocalFeedClear     = int32(photonpb.Action_ACTION_LOCAL_FEED_CLEAR)
)

// storageSchemaVersion — версия схемы хранилища инстанса. Версия — файла, а
// не namespace: миграции bbolt идут одним списком (ADR-010).
//
//	1 — photon.localfeed (ADR-064)
//	2 — photon.outbox (ADR-093 §2.8)
const storageSchemaVersion = 2

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
	st, err := bboltadapter.New(cfg.StoragePath, []bboltadapter.Migration{
		{
			Version: localfeed.SchemaVersion,
			Up: func(tx *bolt.Tx) error {
				_, e := tx.CreateBucketIfNotExists([]byte(localfeed.Namespace))
				return e
			},
		},
		{
			// Файл v1 (только лента) поднимается до v2 при следующем
			// открытии; данные ленты не трогаются.
			Version: 2,
			Up: func(tx *bolt.Tx) error {
				_, e := tx.CreateBucketIfNotExists([]byte(outbox.Namespace))
				return e
			},
		},
	})
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
func prepareFeed(ctx context.Context, st ports.Storage) (*localfeed.Store, *outbox.Store, error) {
	feed := localfeed.New(st, nil)
	if err := feed.Prepare(ctx); err != nil {
		return nil, nil, fmt.Errorf("%w: migrate: %v", ErrStorageUnavailable, err)
	}
	// Очередь исходящих — в том же файле (ADR-093 §2.8): подтверждение
	// отправки ложится одной транзакцией с блоком ленты (ADR-095 п. 2.3).
	if err := st.Migrate(ctx, storageSchemaVersion); err != nil {
		return nil, nil, fmt.Errorf("%w: migrate: %v", ErrStorageUnavailable, err)
	}
	// Migrate без зарегистрированной миграции v2 (хранилище, подставленное
	// через Config.Storage со своим списком) проходит молча и namespace не
	// создаёт. Узнать об этом на первом действии очереди — поздно и
	// невнятно; проверяется здесь, и инстанс не создаётся.
	if err := st.View(ctx, func(tx ports.ReadTx) error {
		_, err := tx.Get(outbox.Namespace, []byte{0})
		if errors.Is(err, ports.ErrNotFound) {
			return nil
		}
		return err
	}); err != nil {
		return nil, nil, fmt.Errorf("%w: namespace %s: %v", ErrStorageUnavailable, outbox.Namespace, err)
	}
	return feed, outbox.New(st), nil
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
	// Цель подписки от хранилища тоже не зависит (ADR-123 §2.3).
	if action == actionLocalEventsTarget {
		return d.eventsTarget(payload)
	}
	// Передача файла вложения (ADR-124 M1) — тоже: хранилище ей не нужно.
	if isMediaFileAction(action) {
		return d.mediaFileAction(ctx, action, payload)
	}
	d.mu.Lock()
	feed, box := d.feed, d.box
	d.mu.Unlock()

	if feed == nil {
		return nil, ErrNoStorage
	}

	switch action {
	case actionLocalFeedPage:
		return feedPage(ctx, feed, payload)
	case actionLocalFeedPut:
		return feedPut(ctx, feed, payload)
	case actionLocalFeedIngest:
		return feedIngest(ctx, feed, box, payload)
	case actionLocalFeedGapAnchor:
		return feedGapAnchor(ctx, feed, payload)
	case actionLocalFeedCatalog:
		return feedCatalog(ctx, feed)
	case actionLocalFeedCursor:
		return feedCursor(ctx, feed, payload)
	case actionLocalFeedClear:
		return feedClear(ctx, feed, payload)
	case actionOutboxEnqueue, actionOutboxLease, actionOutboxComplete, actionOutboxFail,
		actionOutboxRetry, actionOutboxCancel, actionOutboxList:
		return outboxAction(ctx, box, action, payload)
	}

	// Код в диапазоне есть, обработчика нет: такое состояние возможно только
	// между добавлением кода в enum и его реализацией. Молчать нельзя —
	// клиент получил бы пустой успех.
	return nil, fmt.Errorf("%w: %d", session.ErrUnknownAction, action)
}

// feedClearRequest — payload_json действия ACTION_LOCAL_FEED_CLEAR (ADR-112).
type feedClearRequest struct {
	ChannelID string `json:"channel_id"`
}

// feedClear — действие 813. Очищает ленту канала одной транзакцией; очередь
// исходящих не трогается (ADR-112 п. 2.2).
func feedClear(ctx context.Context, feed *localfeed.Store, payload []byte) ([]byte, error) {
	var req feedClearRequest
	if err := decodeRequest(payload, &req); err != nil {
		return nil, err
	}
	if req.ChannelID == "" {
		return nil, fmt.Errorf("%w: channel_id is required", session.ErrInvalidPayload)
	}
	n, err := feed.Clear(ctx, req.ChannelID)
	if err != nil {
		return nil, wrapFeedWriteError(err)
	}
	data, err := json.Marshal(struct {
		Removed int `json:"removed"`
	}{Removed: n})
	if err != nil {
		return nil, fmt.Errorf("dispatcher: encode clear result: %w", err)
	}
	return envelope(session.Response{OK: true, Data: data})
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
		// Та же классификация, что у записи: негодный channel_id (ADR-095
		// п. 2.8) — ошибка аргумента, а не сбой хранилища.
		return nil, wrapFeedWriteError(err)
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

// ── Действия 801-803: запись и позиция пропуска (ADR-065) ───────────────

/*
 * Здесь проходит граница домена. Всё, что ниже, принимает УЖЕ
 * доменно-агностичные записи и утверждения: ни `Post`, ни `create_at`, ни
 * `update_at` в этом файле нет и быть не может (ADR-065 п. 1).
 *
 * Разбор ТИПИЗИРОВАННЫЙ, а не в map[string]any, и это не стилистика:
 * OrderKey и Version — int64 порядка 1.7e12 (миллисекунды), и разбор через
 * float64 потерял бы точность молча, без единой ошибки. Поля объявлены int64,
 * поэтому encoding/json отвергает и дробное, и переполнение сам (ADR-065
 * п. 2.2).
 */

// wireRecord — запись в payload. Повторяет localfeed.Record и ничего к нему
// не добавляет: срез делает существующий метод достижимым, а не проектирует
// новый.
//
// Body — json.RawMessage: тело проходит побайтово и ядром не разбирается
// (ADR-034). Проверять его на валидность здесь незачем и нечем — payload уже
// разобран encoding/json, значит любой добравшийся сюда Body валиден по
// построению. Проверка в localfeed.encode остаётся для вызывающих изнутри
// ядра.
type wireRecord struct {
	ID       string          `json:"id"`
	OrderKey int64           `json:"order_key"`
	Version  int64           `json:"version"`
	Body     json.RawMessage `json:"body"`
}

// wirePosition — позиция, замороженная ADR-052. Второй формы курсора не
// вводится ни здесь, ни где-либо ещё.
type wirePosition struct {
	OrderKey int64  `json:"order_key"`
	ID       string `json:"id"`
}

type feedPutRequest struct {
	ChannelID string       `json:"channel_id"`
	Records   []wireRecord `json:"records"`
}

type feedIngestRequest struct {
	ChannelID string        `json:"channel_id"`
	Records   []wireRecord  `json:"records"`
	Dir       string        `json:"dir"`
	Anchor    *wirePosition `json:"anchor"`
	AtEnd     bool          `json:"at_end"`

	// ADR-095 п. 2.4. Cursor — указатель: «поле отсутствует» (курсор не
	// меняется) и «пустая строка» (ошибка аргумента) — разные случаи.
	Cursor   *string  `json:"cursor"`
	Confirms []string `json:"confirms"`
}

type feedGapAnchorRequest struct {
	ChannelID string `json:"channel_id"`
}

// toRecords переводит записи провода в записи ленты. Ровно перенос полей:
// решать, что есть идентификатор и порядок, — дело вызывающего.
func toRecords(in []wireRecord) []localfeed.Record {
	out := make([]localfeed.Record, 0, len(in))
	for _, r := range in {
		out = append(out, localfeed.Record{
			ID: r.ID, OrderKey: r.OrderKey, Version: r.Version, Body: r.Body,
		})
	}
	return out
}

// parseDirection — имя направления с провода в localfeed.Direction.
//
// Умолчания НЕТ намеренно: пустая строка отвергается наравне с опечаткой.
// Трактовать "" как "newest" значило бы вернуть ровно тот дефект, который
// ADR-065 п. 3.7 закрыл в самом localfeed, — неизвестное значение, молча
// получающее поведение известного.
//
// Проверка здесь и проверка в localfeed не дублируют друг друга: у них разные
// входы (строка провода против Direction) и разные вызывающие. Инвариант
// localfeed защищает вызывающих ИЗНУТРИ ядра, у которых строки нет вовсе.
func parseDirection(s string) (localfeed.Direction, error) {
	switch s {
	case "newest":
		return localfeed.DirNewest, nil
	case "after":
		return localfeed.DirAfter, nil
	case "before":
		return localfeed.DirBefore, nil
	}
	return 0, fmt.Errorf("%w: unknown dir %q", session.ErrInvalidPayload, s)
}

// feedWriteArgErrors — ошибки localfeed, являющиеся ошибкой АРГУМЕНТА, а не
// сбоем хранилища.
//
// Критерий тот же, которым пользуется classTable: есть ли что исправлять
// вызывающему. У всех восьми есть — повторять вызов с тем же payload
// бессмысленно.
//
// Список проверяется на полноту машинно (TestLocalWrite_EveryLocalfeedSentinelIsClassified):
// новый sentinel в localfeed, не попавший ни сюда, ни в список «не аргумент»,
// роняет тест. Без этого он уехал бы в ClassInternal молча, и вызывающий
// пошёл бы искать причину в хранилище вместо своего payload.
var feedWriteArgErrors = []error{
	localfeed.ErrEmptyChannel,
	localfeed.ErrEmptyRecordID,
	localfeed.ErrAnchorRequired,
	localfeed.ErrEmptyBlock,
	localfeed.ErrUnknownDirection,
	localfeed.ErrAnchorNotAllowed,
	localfeed.ErrRecordCrossesAnchor,
	localfeed.ErrEmptyCursor,
	localfeed.ErrInvalidChannelID,
}

// wrapFeedWriteError относит ошибку записи к классу исхода вызова.
//
// Оборачивание двойным %w сохраняет ОБА: класс берётся из существующей
// classTable по session.ErrInvalidPayload, а исходный sentinel остаётся
// доступен errors.Is для того, кто захочет различить причину. Тот же приём,
// что у session.markLost, — и та же причина: терять исходную ошибку ради
// класса незачем.
func wrapFeedWriteError(err error) error {
	for _, sentinel := range feedWriteArgErrors {
		if errors.Is(err, sentinel) {
			return fmt.Errorf("%w: %w", session.ErrInvalidPayload, err)
		}
	}
	return fmt.Errorf("%w: %w", ErrStorageUnavailable, err)
}

// decodeRequest — разбор payload действия.
//
// DisallowUnknownFields НЕ включается: неизвестное поле игнорируется — то же
// правило, по которому ядро читает config_json (config.go). Отвергать payload
// из-за поля, которого ядро не знает, значило бы ломать клиента при каждом
// расширении формы.
func decodeRequest(payload []byte, dst any) error {
	if err := json.Unmarshal(payload, dst); err != nil {
		return fmt.Errorf("%w: %v", session.ErrInvalidPayload, err)
	}
	return nil
}

// feedPut — действие 801. Положить записи, о непрерывности не утверждать.
func feedPut(ctx context.Context, feed *localfeed.Store, payload []byte) ([]byte, error) {
	var req feedPutRequest
	if err := decodeRequest(payload, &req); err != nil {
		return nil, err
	}
	if req.ChannelID == "" {
		return nil, fmt.Errorf("%w: channel_id is required", session.ErrInvalidPayload)
	}

	recs := toRecords(req.Records)
	if err := feed.Put(ctx, req.ChannelID, recs); err != nil {
		return nil, wrapFeedWriteError(err)
	}

	// stored — столько, сколько прислали: Put пишет каждую запись и версий не
	// сверяет. Числа Ingest здесь неуместны — у Put их просто нет.
	data, err := json.Marshal(struct {
		Stored int `json:"stored"`
	}{Stored: len(recs)})
	if err != nil {
		return nil, fmt.Errorf("dispatcher: encode put result: %w", err)
	}
	return envelope(session.Response{OK: true, Data: data})
}

// feedIngest — действие 802. Положить блок и то, что он доказал о полноте;
// с ADR-095 — ещё курсор сервера и подтверждения исходящих, всё одной
// транзакцией (IngestWith + outbox.ConfirmTx).
func feedIngest(ctx context.Context, feed *localfeed.Store, box *outbox.Store, payload []byte) ([]byte, error) {
	var req feedIngestRequest
	if err := decodeRequest(payload, &req); err != nil {
		return nil, err
	}
	if req.ChannelID == "" {
		return nil, fmt.Errorf("%w: channel_id is required", session.ErrInvalidPayload)
	}

	dir, err := parseDirection(req.Dir)
	if err != nil {
		return nil, err
	}

	blk := localfeed.Block{Records: toRecords(req.Records), Dir: dir, AtEnd: req.AtEnd}
	if req.Anchor != nil {
		blk.Anchor = &localfeed.Position{OrderKey: req.Anchor.OrderKey, ID: req.Anchor.ID}
	}

	var opt localfeed.IngestOptions
	if req.Cursor != nil {
		opt.Cursor = []byte(*req.Cursor)
	}
	var confirmed outbox.ConfirmResult
	if len(req.Confirms) > 0 {
		opt.Also = func(tx ports.WriteTx) error {
			var err error
			confirmed, err = box.ConfirmTx(tx, req.Confirms)
			return err
		}
	}

	res, err := feed.IngestWith(ctx, req.ChannelID, blk, opt)
	if err != nil {
		return nil, wrapFeedWriteError(err)
	}

	data, err := encodeIngestResult(res, confirmed)
	if err != nil {
		return nil, err
	}
	return envelope(session.Response{OK: true, Data: data})
}

// encodeIngestResult — все семь счётчиков, без отбора.
//
// GapsClosed ЗНАКОВЫЙ: отрицательное значение законно и означает новый
// пропуск — так выглядит первый шаг возвращения после offline
// (localfeed.IngestResult). Vanished отдаётся как СИГНАЛ и только: политика
// удаления закрыта ADR-054 и ADR-060, автоматического удаления нет, и ничего
// по этому числу не предписывается.
//
// confirms присутствует всегда (ADR-095 п. 2.5): без поля confirms в payload
// — нули, чтобы форма ответа не зависела от формы запроса.
func encodeIngestResult(r localfeed.IngestResult, c outbox.ConfirmResult) ([]byte, error) {
	data, err := json.Marshal(struct {
		Stored     int                  `json:"stored"`
		Segments   int                  `json:"segments"`
		Updated    int                  `json:"updated"`
		Unchanged  int                  `json:"unchanged"`
		Stale      int                  `json:"stale"`
		Vanished   int                  `json:"vanished"`
		GapsClosed int                  `json:"gaps_closed"`
		Confirms   outbox.ConfirmResult `json:"confirms"`
	}{
		Stored: r.Stored, Segments: r.Segments, Updated: r.Updated,
		Unchanged: r.Unchanged, Stale: r.Stale, Vanished: r.Vanished,
		GapsClosed: r.GapsClosed, Confirms: c,
	})
	if err != nil {
		return nil, fmt.Errorf("dispatcher: encode ingest result: %w", err)
	}
	return data, nil
}

// feedGapAnchor — действие 803. Позиция, от которой закрывать верхний пропуск.
//
// Производная от сегментов, вычисляемая при каждом вызове: состояние
// синхронизации хранится только в них, второго источника истины нет
// (ADR-065 п. 10).
//
// «Пропуска нет» — НЕ ошибка и не пустой ответ: это found:false. Отдавать
// нулевую позицию значило бы предложить вызывающему закрывать пропуск от
// начала ленты.
func feedGapAnchor(ctx context.Context, feed *localfeed.Store, payload []byte) ([]byte, error) {
	var req feedGapAnchorRequest
	if err := decodeRequest(payload, &req); err != nil {
		return nil, err
	}
	if req.ChannelID == "" {
		return nil, fmt.Errorf("%w: channel_id is required", session.ErrInvalidPayload)
	}

	pos, found, err := feed.GapAnchor(ctx, req.ChannelID)
	if err != nil {
		return nil, wrapFeedWriteError(err)
	}

	out := struct {
		Found  bool          `json:"found"`
		Anchor *wirePosition `json:"anchor,omitempty"`
	}{Found: found}
	if found {
		out.Anchor = &wirePosition{OrderKey: pos.OrderKey, ID: pos.ID}
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("dispatcher: encode gap anchor result: %w", err)
	}
	return envelope(session.Response{OK: true, Data: data})
}
