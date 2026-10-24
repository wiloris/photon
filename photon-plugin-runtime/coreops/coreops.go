// Package coreops — исполнитель операций каталога поверх портов ядра.
//
// Спецификация: docs/core-operations.md (Phase 13). Реализует ipc.Handler:
// то, что до сих пор изображали тестовые handler'ы лаборатории Phase 11.
//
// Живёт в photon-plugin-runtime, а не в photon-core: ipc.Handler —
// интерфейс runtime, и реализация его ядром означала бы импорт runtime
// ядром, прямое нарушение ADR-030. Зависимость идёт в обратную сторону —
// runtime уже импортирует photon-core ради ports.
//
// Сети не требует. Всё, с чем работает, — порты: ports.Storage,
// ports.Identity, ports.Crypto.
package coreops

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"time"

	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-core/services/p2phost"
	"github.com/wiloris/photon/photon-plugin-runtime/ipc"
)

// Config — зависимости исполнителя.
//
// Только порты и функции. Инстанса, конфигурации приложения и путей к файлам
// здесь нет намеренно: компонент обязан собираться и тестироваться без
// обвязки C ABI (docs/core-operations.md §5.4).
type Config struct {
	// Storage — хранилище. Обязателен.
	Storage ports.Storage

	// StorageNamespace — namespace ports.Storage, в котором живут данные
	// ВСЕХ плагинов. Изоляция между ними — префиксом ключа (§5), а не
	// отдельным namespace на плагин: кто и когда зовёт Migrate, остаётся
	// открытым вопросом (§11.2), и завязываться на его ответ здесь нельзя.
	StorageNamespace string

	// Identity — подпись и проверка. Обязателен.
	Identity ports.Identity

	// DeviceKey — приватный ключ устройства для identity.sign.
	//
	// Плагину не выдаётся никогда и ни в каком виде (ADR-021): операции
	// «получить приватный ключ» в каталоге нет и не будет.
	DeviceKey ports.PrivateKey

	// Crypto — симметричное шифрование. Обязателен.
	Crypto ports.Crypto

	// DeriveKey выводит ключ плагина по домену.
	//
	// Предусловие композиции (§6): исполнитель НЕ знает, откуда берётся
	// мастер-ключ. Он зовёт эту функцию со своим доменом plugin.<id> и
	// получает детерминированный производный ключ. Источник мастер-ключа —
	// вопрос сборки приложения, открыт до cmd/photon-core-server (§11.1).
	DeriveKey func(ctx context.Context, domain string) (ports.SymmetricKey, error)

	// Log принимает записи log.write. Обязателен: единственная операция без
	// права, и молча её терять нельзя.
	Log func(pluginID, level, message string)

	// OnWorkComplete — плагин доложил, что текущая единица работы
	// закончена, и чем именно (ADR-072, нагрузка — ADR-074).
	//
	// ОТДЕЛЬНЫЙ колбэк, а не уровень журнала: доклад о завершении —
	// структурный факт жизненного цикла, и смешение его с текстом
	// сообщений вернуло бы разбор строк, ради устранения которого
	// операция и вводится.
	//
	// payload НЕПРОЗРАЧЕН для исполнителя: он его не разбирает, не
	// проверяет на JSON и не знает ни одного имени поля внутри. Схема —
	// договор плагина с его потребителем (ADR-074 §3). Байты приходят
	// ровно те, что отправил плагин.
	//
	// Исполнитель НЕ РЕШАЕТ, что делать дальше: он сообщает факт
	// композиции. Остановка остаётся правом хоста (ADR-072 §3), и
	// WorkFailure её не ускоряет и не меняет ExitKind (ADR-074 §5).
	// nil означает, что слушателя нет — доклад тогда безвреден.
	OnWorkComplete func(pluginID string, status WorkStatus, payload []byte)

	// MaxLogBytes ограничивает размер одной записи. 0 — значение по
	// умолчанию. Ограничение обязательно: без него log.write это DoS через
	// диск, а права на неё не требуется (§8).
	MaxLogBytes int

	// P2P — прямые соединения (ADR-076). НЕОБЯЗАТЕЛЕН.
	//
	// nil означает композицию без P2P, и это нормальный случай, а не
	// ошибка: все шесть операций домена отвечают UNSUPPORTED, как
	// неизвестная операция (ADR-076 §7.6). Хост, не раздающий P2P, обязан
	// отвечать «такой операции у меня нет», а не падать и не молчать.
	P2P ports.P2P

	// EmitEvent доставляет событие плагину (MsgEvent, Runtime → Plugin).
	//
	// Обязателен, КОГДА задан P2P: без него принятые кадры некуда девать, и
	// канал молча стал бы односторонним. Возвращённая ошибка значима —
	// очередь событий отбрасывает при переполнении, и это приводит к
	// закрытию сессии с reason slow_consumer (ADR-076 §7.3), а не к
	// молчаливой потере сообщения.
	EmitEvent func(pluginID, eventType string, payload []byte) error

	// P2PEstablishTimeout — потолок ожидания установления на стороне
	// исполнителя. 0 — значение по умолчанию.
	//
	// Обязан быть СТРОГО меньше ipc.DefaultRequestTimeout — инвариант
	// ADR-076 §7.7, проверяется в New. Фактический срок каждого ожидания
	// дополнительно выводится из дедлайна самого запроса (см. p2p.go), так
	// что инвариант держится и при нестандартном RequestTimeout.
	P2PEstablishTimeout time.Duration
}

const defaultMaxLogBytes = 4096

// WorkStatus — исход единицы работы, доложенный плагином (ADR-074 §5).
//
// ТИП ОБЪЯВЛЕН ЗДЕСЬ ЗАНОВО, а не взят из photon-plugin-sdk, и это не
// дублирование по недосмотру: runtime — доверенная сторона, и заимствовать
// определения у кода, исполняемого в недоверенном процессе, он не может
// (ADR-031). Общий пакет «ради DRY» внёс бы код доверенной стороны в граф
// зависимостей плагина. Совпадать обязаны строки на проводе, а не типы в
// Go, — и это проверяется тестом, а не компилятором.
type WorkStatus string

const (
	WorkSuccess WorkStatus = "success"
	WorkFailure WorkStatus = "failure"
)

// maxWorkPayloadBytes — предел нагрузки доклада в СЫРЫХ байтах (ADR-074 §7).
//
// Не равен defaultMaxLogBytes намеренно: предел журнала уже доказал
// недостаточность для результата работы (ADR-074 §2.3). Выбран по измерениям
// — амплификация памяти кадра ×5 и исполнение хоста на телефоне, — а не по
// потолку кадра IPC (4 MiB), который является техническим ceiling, но
// пределом доклада быть не должен.
const maxWorkPayloadBytes = 64 << 10

// Handler исполняет операции каталога. Один на runtime, не по одному на
// плагин: идентичность приходит параметром каждого вызова (ADR-036).
type Handler struct {
	cfg Config

	// p2p — единый P2P-исполнитель Core (services/p2phost) с владельцем-
	// плагином. Реестр по-прежнему принадлежит runtime (ADR-076 §3.8): у
	// плагина только непрозрачная строка handle. nil — композиция без P2P.
	p2p *p2phost.Executor
}

var _ ipc.Handler = (*Handler)(nil)

// New проверяет конфигурацию и создаёт исполнителя.
//
// Отсутствие любой зависимости — ошибка конструирования, а не повод
// отвечать плагину UNSUPPORTED в рантайме: неполно собранный исполнитель
// выглядел бы как работающий и отказывал бы выборочно.
func New(cfg Config) (*Handler, error) {
	switch {
	case cfg.Storage == nil:
		return nil, errors.New("coreops: Storage is required")
	case cfg.StorageNamespace == "":
		return nil, errors.New("coreops: StorageNamespace is required")
	case cfg.Identity == nil:
		return nil, errors.New("coreops: Identity is required")
	case len(cfg.DeviceKey) == 0:
		return nil, errors.New("coreops: DeviceKey is required")
	case cfg.Crypto == nil:
		return nil, errors.New("coreops: Crypto is required")
	case cfg.DeriveKey == nil:
		return nil, errors.New("coreops: DeriveKey is required")
	case cfg.Log == nil:
		return nil, errors.New("coreops: Log is required")
	}
	if cfg.MaxLogBytes <= 0 {
		cfg.MaxLogBytes = defaultMaxLogBytes
	}
	// P2P необязателен; но если он задан, композиция обязана быть полной.
	if cfg.P2P != nil {
		if cfg.EmitEvent == nil {
			return nil, errors.New("coreops: EmitEvent is required when P2P is configured")
		}
		if cfg.P2PEstablishTimeout <= 0 {
			cfg.P2PEstablishTimeout = defaultP2PEstablishTimeout
		}
		// Инвариант ADR-076 §7.7, проверенный при конструировании, а не
		// оставленный на внимательность композиции: ожидание установления
		// обязано кончаться причиной со стороны P2P, а не дедлайном IPC.
		if cfg.P2PEstablishTimeout >= ipc.DefaultRequestTimeout {
			return nil, fmt.Errorf(
				"coreops: P2PEstablishTimeout (%s) must be strictly less than ipc.DefaultRequestTimeout (%s)",
				cfg.P2PEstablishTimeout, ipc.DefaultRequestTimeout)
		}
	}
	h := &Handler{cfg: cfg}
	if cfg.P2P != nil {
		ex, err := newP2PExecutor(cfg)
		if err != nil {
			return nil, err
		}
		h.p2p = ex
	}
	return h, nil
}

// Handle — см. ipc.Handler.
//
// Вызывается ТОЛЬКО после успешной авторизации: право уже проверено сессией
// по таблице ipc/operations.go (ADR-032). Здесь прав не проверяют повторно —
// вторая таблица разошлась бы с первой.
func (h *Handler) Handle(ctx context.Context, caller ipc.CallerInfo, op string, payload []byte) ([]byte, error) {
	// Пустой PluginID означал бы общий namespace для всех плагинов — то
	// есть отсутствие изоляции. Сессия его таким не отдаёт, но проверка
	// стоит здесь, а не в тесте: цена ошибки — утечка между плагинами.
	if caller.PluginID == "" {
		return nil, ipc.NewOpError(ipc.ErrInternal, "caller identity is missing")
	}

	switch op {
	case "storage.get":
		return h.storageGet(ctx, caller, payload)
	case "storage.put":
		return h.storagePut(ctx, caller, payload)
	case "storage.delete":
		return h.storageDelete(ctx, caller, payload)
	case "storage.list":
		return h.storageList(ctx, caller, payload)

	case "identity.public_key":
		return h.identityPublicKey(ctx)
	case "identity.sign":
		return h.identitySign(ctx, caller, payload)
	case "identity.verify":
		return h.identityVerify(ctx, payload)

	case "crypto.encrypt":
		return h.cryptoEncrypt(ctx, caller, payload)
	case "crypto.decrypt":
		return h.cryptoDecrypt(ctx, caller, payload)

	case "log.write":
		return h.logWrite(caller, payload)

	case "work.complete":
		return h.workComplete(caller, payload)

	// p2p.* — ADR-076. Config.P2P == nil даёт UNSUPPORTED внутри каждой
	// ветки: композиция без P2P нормальна, и отличать её от неизвестной
	// операции плагину незачем (§7.6).
	case "p2p.create_offer":
		return h.p2pCreateOffer(ctx, caller, payload)
	case "p2p.accept_offer":
		return h.p2pAcceptOffer(ctx, caller, payload)
	case "p2p.complete":
		return h.p2pComplete(ctx, caller, payload)
	case "p2p.peer":
		return h.p2pPeer(ctx, caller, payload)
	case "p2p.send":
		return h.p2pSend(ctx, caller, payload)
	case "p2p.close":
		return h.p2pClose(ctx, caller, payload)
	}

	// Неизвестная операция. Сессия отсекает те, которых нет в таблице
	// операций, но каталог таблицы и каталог исполнителя — разные списки,
	// и расхождение между ними обязано быть громким.
	return nil, ipc.NewOpError(ipc.ErrUnsupported, "unknown operation")
}

// ── storage ──────────────────────────────────────────────────────────

func (h *Handler) storageGet(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	var req struct {
		Namespace string `json:"namespace"`
		Key       string `json:"key"`
	}
	if err := decode(payload, &req); err != nil {
		return nil, err
	}

	var value []byte
	err := h.cfg.Storage.View(ctx, func(tx ports.ReadTx) error {
		v, getErr := tx.Get(h.cfg.StorageNamespace, storageKey(caller.PluginID, req.Namespace, req.Key))
		if getErr != nil {
			return getErr
		}
		// Значение принадлежит транзакции: копируем, иначе после её
		// завершения байты могут быть переиспользованы адаптером.
		value = append([]byte(nil), v...)
		return nil
	})
	if err != nil {
		return nil, storageErr(err)
	}
	return encode(map[string]any{"value": value})
}

func (h *Handler) storagePut(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	var req struct {
		Namespace string `json:"namespace"`
		Key       string `json:"key"`
		Value     []byte `json:"value"`
	}
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	err := h.cfg.Storage.Update(ctx, func(tx ports.WriteTx) error {
		return tx.Put(h.cfg.StorageNamespace, storageKey(caller.PluginID, req.Namespace, req.Key), req.Value)
	})
	if err != nil {
		return nil, storageErr(err)
	}
	return encode(struct{}{})
}

func (h *Handler) storageDelete(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	var req struct {
		Namespace string `json:"namespace"`
		Key       string `json:"key"`
	}
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	err := h.cfg.Storage.Update(ctx, func(tx ports.WriteTx) error {
		return tx.Delete(h.cfg.StorageNamespace, storageKey(caller.PluginID, req.Namespace, req.Key))
	})
	if err != nil {
		return nil, storageErr(err)
	}
	return encode(struct{}{})
}

func (h *Handler) storageList(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	var req struct {
		Namespace string `json:"namespace"`
		Prefix    string `json:"prefix"`
	}
	if err := decode(payload, &req); err != nil {
		return nil, err
	}

	// Префикс плагина ПРИСТАВЛЯЕТСЯ к его собственному, а не используется
	// вместо него: иначе storage.read означало бы «читать всё» (§5).
	own := storagePrefix(caller.PluginID, req.Namespace)
	scan := append(append([]byte(nil), own...), req.Prefix...)

	keys := []string{}
	err := h.cfg.Storage.View(ctx, func(tx ports.ReadTx) error {
		return tx.ForEachPrefix(h.cfg.StorageNamespace, scan, func(key, _ []byte) error {
			// Ключи возвращаются БЕЗ префикса: результат list обязан
			// подставляться в get без преобразований (§5). Несовпадение
			// префикса — не «вернуть как есть», а внутренняя ошибка.
			bare, stripErr := stripPrefix(key, own)
			if stripErr != nil {
				return stripErr
			}
			keys = append(keys, bare)
			return nil
		})
	})
	if err != nil {
		return nil, storageErr(err)
	}
	return encode(map[string]any{"keys": keys})
}

// ── identity ─────────────────────────────────────────────────────────

func (h *Handler) identityPublicKey(ctx context.Context) ([]byte, error) {
	pub, err := h.cfg.Identity.PublicKeyOf(ctx, h.cfg.DeviceKey)
	if err != nil {
		return nil, ipc.NewOpError(ipc.ErrInternal, "identity unavailable")
	}
	return encode(map[string]any{"public_key": []byte(pub)})
}

func (h *Handler) identitySign(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	var req struct {
		Data []byte `json:"data"`
	}
	if err := decode(payload, &req); err != nil {
		return nil, err
	}

	sig, err := h.cfg.Identity.Sign(ctx, h.cfg.DeviceKey, pluginSignInput(caller.PluginID, req.Data))
	if err != nil {
		return nil, ipc.NewOpError(ipc.ErrInternal, "sign failed")
	}
	return encode(map[string]any{"signature": []byte(sig)})
}

func (h *Handler) identityVerify(ctx context.Context, payload []byte) ([]byte, error) {
	var req struct {
		PublicKey []byte `json:"public_key"`
		Data      []byte `json:"data"`
		Signature []byte `json:"signature"`
	}
	if err := decode(payload, &req); err != nil {
		return nil, err
	}

	// Проверяется РОВНО переданное, без доменного префикса. Операция общего
	// назначения: ею проверяют подписи, сделанные вне Photon (ADR-037).
	valid, err := h.cfg.Identity.Verify(ctx, ports.PublicKey(req.PublicKey), req.Data, ports.Signature(req.Signature))
	if err != nil {
		return nil, ipc.NewOpError(ipc.ErrInvalidRequest, "verify failed")
	}
	return encode(map[string]any{"valid": valid})
}

// pluginSignInput — доменно-разделённое представление подписываемого
// (ADR-037):
//
//	"plugin-sign:" ‖ uint32be(len(plugin_id)) ‖ plugin_id ‖ ":" ‖ data
//
// Без него плагин с правом identity.sign получал бы подпись УСТРОЙСТВА под
// любым содержимым — включая то, что в другом контексте является вызовом
// протокола. Двоеточие после plugin_id — читаемость при отладке, а не
// барьер: границу задаёт длина.
func pluginSignInput(pluginID string, data []byte) []byte {
	var b bytes.Buffer
	b.WriteString("plugin-sign:")
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(pluginID)))
	b.Write(n[:])
	b.WriteString(pluginID)
	b.WriteString(":")
	b.Write(data)
	return b.Bytes()
}

// ── crypto ───────────────────────────────────────────────────────────

// keyDomain — домен вывода ключа плагина. Совпадение строки с префиксом
// ключей хранилища — удобство чтения, не связь: DeriveKey оговаривает, что
// domain и namespace независимы (§6).
func keyDomain(pluginID string) string { return "plugin." + pluginID }

func (h *Handler) cryptoEncrypt(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	var req struct {
		Plaintext   []byte `json:"plaintext"`
		BindingData []byte `json:"binding_data"`
	}
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	key, err := h.cfg.DeriveKey(ctx, keyDomain(caller.PluginID))
	if err != nil {
		return nil, ipc.NewOpError(ipc.ErrInternal, "key unavailable")
	}
	ct, err := h.cfg.Crypto.Encrypt(ctx, key, req.Plaintext, req.BindingData)
	if err != nil {
		return nil, cryptoErr(err)
	}
	return encode(map[string]any{"ciphertext": ct})
}

func (h *Handler) cryptoDecrypt(ctx context.Context, caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	var req struct {
		Ciphertext  []byte `json:"ciphertext"`
		BindingData []byte `json:"binding_data"`
	}
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	key, err := h.cfg.DeriveKey(ctx, keyDomain(caller.PluginID))
	if err != nil {
		return nil, ipc.NewOpError(ipc.ErrInternal, "key unavailable")
	}
	pt, err := h.cfg.Crypto.Decrypt(ctx, key, req.Ciphertext, req.BindingData)
	if err != nil {
		return nil, cryptoErr(err)
	}
	return encode(map[string]any{"plaintext": pt})
}

// ── log ──────────────────────────────────────────────────────────────

func (h *Handler) logWrite(caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	var req struct {
		Level   string `json:"level"`
		Message string `json:"message"`
	}
	if err := decode(payload, &req); err != nil {
		return nil, err
	}
	// Объём ограничивает runtime: права на эту операцию не требуется, и без
	// ограничения это DoS через диск (§8).
	if len(req.Message) > h.cfg.MaxLogBytes {
		req.Message = req.Message[:h.cfg.MaxLogBytes]
	}
	h.cfg.Log(caller.PluginID, req.Level, req.Message)
	return encode(struct{}{})
}

// workComplete — доклад плагина о завершении единицы работы и его исходе
// (ADR-072; нагрузка и статус — ADR-074).
//
// Ответ пустой и успешный ВСЕГДА, когда конверт разобран, — в том числе при
// WorkFailure (ADR-074 §4.1). Доклад ничего не просит, поэтому отклонить его
// «по существу» нельзя; отказ возможен только ПО ФОРМЕ конверта: негодный
// JSON, неизвестный статус, нагрузка сверх предела.
//
// Операция НЕ ТЕРМИНАЛЬНА: сессия продолжается, плагин продолжает работать, и
// доклад может прийти снова — например, после обработки события. Повторный
// доклад НЕ ЗАМЕНЯЕТ предыдущий: здесь нет состояния «последний результат»,
// каждый вызов передаётся композиции отдельно (ADR-074 §10).
func (h *Handler) workComplete(caller ipc.CallerInfo, payload []byte) ([]byte, error) {
	var req struct {
		Status  WorkStatus `json:"status"`
		Payload []byte     `json:"payload"`
	}
	if err := decode(payload, &req); err != nil {
		return nil, err
	}

	/*
	 * Пустой статус — УСПЕХ, и это не послабление, а требование
	 * совместимости (ADR-074 §12): плагин, собранный до ADR-074, шлёт
	 * доклад без полей вовсе, decode оставляет нулевые значения, и такой
	 * доклад обязан значить ровно то же, что значил раньше.
	 */
	switch req.Status {
	case "":
		req.Status = WorkSuccess
	case WorkSuccess, WorkFailure:
	default:
		return nil, ipc.NewOpError(ipc.ErrInvalidRequest, "unknown work status")
	}

	/*
	 * Предел проверяется ПОСЛЕ разбора JSON, то есть по декодированным
	 * байтам, — не по длине base64-строки и не по размеру тела запроса
	 * (ADR-074 §7). Иначе один и тот же результат проходил бы или нет в
	 * зависимости от кодировки транспорта.
	 *
	 * ОТКАЗ, А НЕ ОБРЕЗКА — в отличие от log.write. Там укороченная строка
	 * остаётся строкой журнала; здесь она стала бы правдоподобным, но
	 * ложным результатом, и потребитель не смог бы отличить её от целого.
	 */
	if len(req.Payload) > maxWorkPayloadBytes {
		return nil, ipc.NewOpError(ipc.ErrInvalidRequest, "work payload exceeds limit")
	}

	if h.cfg.OnWorkComplete != nil {
		h.cfg.OnWorkComplete(caller.PluginID, req.Status, req.Payload)
	}
	return encode(struct{}{})
}

// ── общее ────────────────────────────────────────────────────────────

func decode(payload []byte, v any) error {
	if len(payload) == 0 {
		// Пустой payload допустим для операций без аргументов.
		return nil
	}
	if err := json.Unmarshal(payload, v); err != nil {
		return ipc.NewOpError(ipc.ErrInvalidRequest, "malformed payload")
	}
	return nil
}

func encode(v any) ([]byte, error) {
	out, err := json.Marshal(v)
	if err != nil {
		return nil, ipc.NewOpError(ipc.ErrInternal, "response encoding failed")
	}
	return out, nil
}

// storageErr отображает ошибки порта на коды протокола (§9).
//
// Текст ошибки адаптера плагину НЕ уходит: он содержит пути файлов и имена
// namespace.
func storageErr(err error) error {
	switch {
	case errors.Is(err, ports.ErrNotFound):
		return ipc.NewOpError(ipc.ErrNotFound, "key not found")
	case errors.Is(err, ports.ErrNamespaceNotFound):
		// Дефект развёртывания, а не ошибка плагина. НЕ NOT_FOUND: иначе
		// плагин не отличит «моего ключа нет» от «хранилище не
		// подготовлено», а это разные ситуации.
		return ipc.NewOpError(ipc.ErrInternal, "storage is not ready")
	}
	var opErr *ipc.OpError
	if errors.As(err, &opErr) {
		return err
	}
	return ipc.NewOpError(ipc.ErrInternal, "storage failure")
}

// cryptoErr — см. §9. Три причины отказа расшифровки не различаются
// намеренно: плагин, умеющий их отличать, получает оракул для проверки
// догадок о чужих ключах.
func cryptoErr(err error) error {
	switch {
	case errors.Is(err, ports.ErrDecryptionFailed):
		return ipc.NewOpError(ipc.ErrInvalidRequest, "decryption failed")
	case errors.Is(err, ports.ErrInvalidKey):
		return ipc.NewOpError(ipc.ErrInvalidRequest, "invalid key")
	}
	return ipc.NewOpError(ipc.ErrInternal, "crypto failure")
}
