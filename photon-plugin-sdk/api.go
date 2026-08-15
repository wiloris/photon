package sdk

import "context"

// Публичный API v1 для авторов плагинов.
//
// Каждый метод — ровно ОДНА операция из каталога
// (photon-plugin-runtime/docs/sdk-boundary.md, раздел 3). Соответствие
// однозначное: нет методов, которые превращаются в несколько запросов,
// и нет операций каталога без соответствующего метода.
//
// SDK не решает, разрешена ли операция: он отправляет запрос и
// возвращает ответ. Проверку выполняет runtime на каждый вызов
// (ADR-031).

// Plugin — точка входа для автора плагина.
//
// Получается из Connect и живёт до завершения процесса. Все обращения
// к возможностям Photon идут через поля этой структуры.
type Plugin interface {
	// Storage — хранилище плагина.
	Storage() Storage

	// Identity — криптографическая идентичность устройства.
	Identity() Identity

	// Crypto — шифрование данных плагина.
	Crypto() Crypto

	// Log пишет в журнал Runtime.
	//
	// Единственная возможность, не требующая права: плагин должен
	// уметь сообщить о проблеме, даже не получив ничего
	// (sdk-boundary.md, раздел 3).
	Log(ctx context.Context, level LogLevel, message string) error

	// OnEvent регистрирует обработчик событий от Runtime.
	//
	// Обработчик вызывается последовательно, по одному событию за раз:
	// параллельная доставка потребовала бы от автора плагина
	// синхронизации, которую SDK не может обеспечить за него.
	OnEvent(handler EventHandler)

	// OnStop регистрирует обработчик штатной остановки.
	//
	// Вызывается при получении STOP от Runtime. После возврата из
	// обработчика SDK отвечает STOP_ACK. Время ограничено Runtime —
	// затянувшийся обработчик не остановит остановку
	// (docs/lifecycle.md, раздел 4).
	OnStop(handler func(ctx context.Context))

	// Run обслуживает сессию до её завершения.
	//
	// Возвращается, когда Runtime закрыл соединение или запросил
	// остановку. Переподключения нет: сессия не переживает разрыв
	// (sdk-boundary.md, раздел 5).
	Run(ctx context.Context) error

	// Close завершает сессию со стороны плагина.
	Close() error
}

// ── Storage ─────────────────────────────────────────────────────────

// Storage — хранилище плагина.
//
// Пространство имён назначает Runtime (plugin.<plugin_id>.*), плагин
// его не выбирает и не видит. Параметр namespace здесь — СУФФИКС
// внутри собственного пространства плагина, а не произвольный
// namespace ядра: попытка обратиться к чужим данным невозможна не
// потому, что SDK это проверяет, а потому что Runtime подставляет
// префикс сам (sdk-boundary.md, раздел 3).
type Storage interface {
	// Get возвращает значение. ErrNotFound, если ключа нет.
	//
	// op: storage.get, permission: storage.read
	Get(ctx context.Context, namespace, key string) ([]byte, error)

	// Put записывает значение.
	//
	// op: storage.put, permission: storage.write
	Put(ctx context.Context, namespace, key string, value []byte) error

	// Delete удаляет ключ. Удаление отсутствующего — не ошибка.
	//
	// op: storage.delete, permission: storage.write
	Delete(ctx context.Context, namespace, key string) error

	// List возвращает ключи с указанным префиксом.
	//
	// op: storage.list, permission: storage.read
	List(ctx context.Context, namespace, prefix string) ([]string, error)
}

// ── Identity ────────────────────────────────────────────────────────

// Identity — криптографическая идентичность устройства.
//
// Приватный ключ плагину НЕ выдаётся ни одним методом, и такого метода
// не появится: ключ не покидает устройство (ADR-021). Плагин получает
// способность использовать идентичность, но не сам секрет — Sign
// выполняется на стороне Runtime.
type Identity interface {
	// PublicKey возвращает публичный ключ устройства.
	//
	// op: identity.public_key, permission: identity.read
	PublicKey(ctx context.Context) ([]byte, error)

	// Sign подписывает данные ключом устройства.
	//
	// op: identity.sign, permission: identity.sign
	Sign(ctx context.Context, data []byte) ([]byte, error)

	// Verify проверяет подпись.
	//
	// Возвращает (false, nil) для структурно корректной, но неверной
	// подписи — ожидаемый отрицательный результат, а не ошибка. Та же
	// семантика, что у ports.Identity.Verify.
	//
	// op: identity.verify, permission: identity.read
	Verify(ctx context.Context, publicKey, data, signature []byte) (bool, error)
}

// ── Crypto ──────────────────────────────────────────────────────────

// Crypto — шифрование данных плагина.
//
// Ключ выводит Runtime из мастер-ключа по домену plugin.<plugin_id>:
// плагин ключ не выбирает и не видит. Иначе один плагин мог бы
// расшифровать данные другого (sdk-boundary.md, раздел 3).
type Crypto interface {
	// Encrypt шифрует plaintext.
	//
	// bindingData аутентифицируется, но не шифруется: Decrypt откажет,
	// если при расшифровке передано не то же значение. Если
	// привязывать не к чему — nil.
	//
	// op: crypto.encrypt, permission: crypto.use
	Encrypt(ctx context.Context, plaintext, bindingData []byte) ([]byte, error)

	// Decrypt расшифровывает ciphertext.
	//
	// Возвращает ErrDecryptionFailed на любую причину сбоя без
	// уточнения: различать «неверный ключ» и «подделанные данные» на
	// этом уровне не нужно — реакция одна.
	//
	// op: crypto.decrypt, permission: crypto.use
	Decrypt(ctx context.Context, ciphertext, bindingData []byte) ([]byte, error)
}

// ── События ─────────────────────────────────────────────────────────

// Event — событие от Runtime.
type Event struct {
	// Type — тип события ("chat.message", "storage.changed", ...).
	Type string

	// Payload — тело события; для SDK непрозрачно.
	Payload []byte
}

// EventHandler обрабатывает событие.
//
// Возврат ошибки логируется Runtime, но не влияет на доставку
// последующих событий: событие уже произошло, и плагин не может его
// «отменить».
type EventHandler func(ctx context.Context, event Event) error

// ── Журнал ──────────────────────────────────────────────────────────

// LogLevel — уровень записи журнала.
type LogLevel string

const (
	LogDebug LogLevel = "debug"
	LogInfo  LogLevel = "info"
	LogWarn  LogLevel = "warn"
	LogError LogLevel = "error"
)
