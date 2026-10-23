package diag

// Перечни ниже — схема журнала (ADR-130 §2.3). Закрытые: добавление
// события или ключа — правкой ADR, а не по месту. Числовые значения на
// провод не выходят (ответ 820 несёт имена), поэтому порядок констант можно
// менять, а имена — нет.

// Component — источник события.
type Component uint8

const (
	CompSession Component = iota + 1
	CompDispatcher
	CompEvents
	CompTransport
	CompStorage
	CompAutodial
)

var componentNames = [...]string{
	CompSession:    "session",
	CompDispatcher: "dispatcher",
	CompEvents:     "events",
	CompTransport:  "transport",
	CompStorage:    "storage",
	CompAutodial:   "autodial",
}

func (c Component) String() string {
	if int(c) < len(componentNames) && componentNames[c] != "" {
		return componentNames[c]
	}
	return "unknown"
}

// Kind — вид события. Перечень по ADR-130 §2.3; каждому виду — ровно один
// компонент (kindComponent).
type Kind uint8

const (
	// session
	KindSessionState  Kind = iota + 1 // смена состояния: from, to
	KindSessionFailed                 // причина Failed: class (connect / lost)
	KindSessionClosed                 // закрытие

	// dispatcher
	KindConnStart  // начало подключения соединения conn
	KindConnResult // исход подключения: dur_us, class
	KindSubBackoff // пауза перед переподключением подписки: attempt, dur_us
	KindCallFailed // вызов с ошибкой: action, resource, dur_us, class
	KindCallSlow   // вызов дольше порога: action, resource, dur_us, class

	// events
	KindEventSent // отправка клиенту CONNECTED / NETWORK_CHANGED: event, queue

	// transport
	KindDial       // dial: endpoint, dur_us, class, transport; при успехе — remote, family
	KindConnClosed // закрытие соединения: class

	// storage
	KindStorageOpen    // открытие: dur_us, class (ok / locked / failed)
	KindStorageMigrate // миграция: to, dur_us, class
	KindStorageClose   // закрытие: dur_us, class
	KindTxFailed       // транзакция с ошибкой: tx, class, started, rolled_back, dur_us
	KindTxSlow         // транзакция дольше порога: tx, wait_us, exec_us, commit_us

	// autodial — схема заложена заранее; событий нет, пока Autodial не
	// подключён к рабочему пути (ADR-130 §2.3, отдельное решение).
	KindRouteSelected // выбор маршрута: family, transport
	KindRouteAttempt  // попытка по маршруту: family, transport, dur_us, class

	kindEnd
)

var kindInfo = [...]struct {
	name string
	comp Component
}{
	KindSessionState:   {"state", CompSession},
	KindSessionFailed:  {"failed", CompSession},
	KindSessionClosed:  {"closed", CompSession},
	KindConnStart:      {"conn_start", CompDispatcher},
	KindConnResult:     {"conn_result", CompDispatcher},
	KindSubBackoff:     {"sub_backoff", CompDispatcher},
	KindCallFailed:     {"call_failed", CompDispatcher},
	KindCallSlow:       {"call_slow", CompDispatcher},
	KindEventSent:      {"event_sent", CompEvents},
	KindDial:           {"dial", CompTransport},
	KindConnClosed:     {"conn_closed", CompTransport},
	KindStorageOpen:    {"open", CompStorage},
	KindStorageMigrate: {"migrate", CompStorage},
	KindStorageClose:   {"close", CompStorage},
	KindTxFailed:       {"tx_failed", CompStorage},
	KindTxSlow:         {"tx_slow", CompStorage},
	KindRouteSelected:  {"route_selected", CompAutodial},
	KindRouteAttempt:   {"route_attempt", CompAutodial},
}

func (k Kind) String() string {
	if k > 0 && k < kindEnd {
		return kindInfo[k].name
	}
	return "unknown"
}

// Component — компонент, которому принадлежит вид события.
func (k Kind) Component() Component {
	if k > 0 && k < kindEnd {
		return kindInfo[k].comp
	}
	return 0
}

// Conn — какое из соединений инстанса затронуто (0 — не относится).
type Conn uint8

const (
	ConnNone Conn = iota
	ConnAction
	ConnSubscribe
	ConnMedia
)

var connNames = [...]string{ConnNone: "", ConnAction: "action", ConnSubscribe: "subscribe", ConnMedia: "media"}

func (c Conn) String() string {
	if int(c) < len(connNames) {
		return connNames[c]
	}
	return "unknown"
}

// Key — ключ поля записи. Закрытый короткий перечень (ADR-130 §2.3):
// длительности, классы, коды действий, состояния, адреса endpoint.
type Key uint8

const (
	KeyDurUS Key = iota + 1
	KeyWaitUS
	KeyExecUS
	KeyCommitUS
	KeyClass      // класс ошибки или исхода
	KeyText       // текст sentinel-ошибки, ≤ MaxText байт
	KeyAction     // код действия
	KeyResource   // ресурс действия
	KeyFrom       // прежнее состояние
	KeyTo         // новое состояние или целевая версия схемы
	KeyAttempt    // номер попытки
	KeyTx         // вид транзакции: view / update
	KeyStarted    // 1 — транзакция началась (fn вызвана)
	KeyRolledBack // 1 — транзакция откатилась
	KeyEndpoint   // адрес endpoint
	KeyQueue      // длина очереди насоса
	KeyEvent      // тип события подписки
	KeyFamily     // семейство адресов маршрута
	KeyTransport  // транспорт маршрута или подключения
	KeyRemote     // фактический адрес установленного соединения
	keyEnd
)

var keyNames = [...]string{
	KeyDurUS:      "dur_us",
	KeyWaitUS:     "wait_us",
	KeyExecUS:     "exec_us",
	KeyCommitUS:   "commit_us",
	KeyClass:      "class",
	KeyText:       "text",
	KeyAction:     "action",
	KeyResource:   "resource",
	KeyFrom:       "from",
	KeyTo:         "to",
	KeyAttempt:    "attempt",
	KeyTx:         "tx",
	KeyStarted:    "started",
	KeyRolledBack: "rolled_back",
	KeyEndpoint:   "endpoint",
	KeyQueue:      "queue",
	KeyEvent:      "event",
	KeyFamily:     "family",
	KeyTransport:  "transport",
	KeyRemote:     "remote",
}

func (k Key) String() string {
	if k > 0 && k < keyEnd {
		return keyNames[k]
	}
	return "unknown"
}

// Field — одно поле записи: число или строка. Строка не копируется, а
// разделяет память переданной — поэтому запись не аллоцирует.
type Field struct {
	Key Key
	Num int64
	Str string
	str bool
}

// IsStr — поле строковое.
func (f Field) IsStr() bool { return f.str }

// Num — числовое поле.
func Num(k Key, v int64) Field { return Field{Key: k, Num: v} }

// Str — строковое поле, усечённое до MaxText байт.
func Str(k Key, s string) Field { return Field{Key: k, Str: truncate(s), str: true} }

// Bool — числовое поле 0/1.
func Bool(k Key, v bool) Field {
	if v {
		return Field{Key: k, Num: 1}
	}
	return Field{Key: k}
}

// Event — запись журнала фиксированного размера.
type Event struct {
	Seq       uint64
	AtMonoMS  int64 // от старта инстанса, монотонные часы (ADR-130 §2.3, поправка)
	Component Component
	Kind      Kind
	Conn      Conn
	N         uint8 // занято полей в Fields
	Fields    [MaxFields]Field
}

// Get возвращает поле по ключу.
func (e *Event) Get(k Key) (Field, bool) {
	for i := 0; i < int(e.N); i++ {
		if e.Fields[i].Key == k {
			return e.Fields[i], true
		}
	}
	return Field{}, false
}

// Wire — форма записи на выходе 820: имена вместо чисел, поля — плоский
// объект. Аллоцирует — вызывается только на стороне чтения.
func (e *Event) Wire() map[string]any {
	fields := make(map[string]any, e.N)
	for i := 0; i < int(e.N); i++ {
		f := e.Fields[i]
		if f.str {
			fields[f.Key.String()] = f.Str
		} else {
			fields[f.Key.String()] = f.Num
		}
	}
	m := map[string]any{
		"seq":        e.Seq,
		"at_mono_ms": e.AtMonoMS,
		"component":  e.Component.String(),
		"kind":       e.Kind.String(),
		"fields":     fields,
	}
	if e.Conn != ConnNone {
		m["conn"] = e.Conn.String()
	}
	return m
}
