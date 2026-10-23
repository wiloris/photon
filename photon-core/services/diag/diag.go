// Package diag — встроенная диагностика инстанса Photon Core (ADR-130).
//
// Журнал событий ограниченной ёмкости, агрегированные метрики и снимок
// состояния. Сервис ядра, а не порт: внешней зависимости за ним нет, и
// ports/*.go им не меняются (ADR-020). Владелец — инстанс (dispatcher), как у
// локального хранилища (ADR-064 §1).
//
// Три свойства, ради которых пакет устроен именно так (ADR-130 §2.2, §2.10):
//
//  1. Запись не может завершиться ошибкой и ничего не возвращает. Сбой
//     диагностики (переполнение, отключение) не влияет на вызов, транзакцию и
//     соединение, за которыми она наблюдает.
//  2. Запись без аллокаций, каналов, горутин и вызовов наружу: кольцо
//     заранее выделенных записей фиксированного размера под одним мьютексом.
//     Recorder ничего не вызывает, поэтому взаимная блокировка с
//     компонентами исключена построением. Ожидание мьютекса при конкурентных
//     писателях НЕ исключено — оно измеряется (ADR-130 §5.6).
//  3. Выключенная диагностика — nil-*Diag: каждый метод начинается с одного
//     сравнения и ничего не делает. Поведение ядра неотличимо от прежнего.
//
// Содержимое записей — закрытый перечень событий (Kind) и ключей (Key).
// Payload вызовов, тексты сообщений, токен, PhotonID, ключи и пути к файлам
// пользователя сюда не попадают никогда (ADR-130 §2.8): строковые поля
// принимают только классы ошибок, тексты sentinel-ошибок, имена состояний и
// адреса endpoint, и каждое усечено до MaxText байт.
package diag

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Пределы конфигурации (ADR-130 §2.9).
const (
	DefaultCapacity = 1024
	MinCapacity     = 64
	MaxCapacity     = 8192

	DefaultSlowTx   = 100 * time.Millisecond
	DefaultSlowCall = 5 * time.Second

	// MaxReadLimit — верхняя граница limit одного чтения (действие 820).
	MaxReadLimit = 512

	// MaxFields — полей в одной записи. Записи фиксированного размера —
	// условие записи без аллокаций; лишние поля отбрасываются, а не
	// растягивают запись.
	MaxFields = 6

	// MaxText — предел строкового поля в байтах (ADR-130 §2.8: текст ошибки
	// усекается до 200 байт). Усечение — по границе символа UTF-8.
	MaxText = 200
)

// Ошибки чтения журнала. Записи ошибок не возвращают.
var (
	// ErrInvalidCursor — after_seq больше последнего выданного номера.
	// Такой курсор возникает только от чужого инстанса или от ошибки
	// вызывающего; тихий пустой ответ выглядел бы как «событий нет».
	ErrInvalidCursor = errors.New("diag: cursor is ahead of the journal")

	// ErrInvalidLimit — limit вне 1…MaxReadLimit.
	ErrInvalidLimit = errors.New("diag: limit out of range")

	// ErrInvalidCapacity — ёмкость вне MinCapacity…MaxCapacity.
	ErrInvalidCapacity = errors.New("diag: capacity out of range")
)

// Config — параметры диагностики инстанса.
type Config struct {
	Capacity int           // 0 — DefaultCapacity
	SlowTx   time.Duration // порог «медленной» транзакции; 0 — DefaultSlowTx
	SlowCall time.Duration // порог «медленного» вызова; 0 — DefaultSlowCall

	// Summary — сводка конфигурации инстанса для снимка. Без секретов: её
	// собирает владелец инстанса, и токена в ней нет по построению типа.
	Summary Summary
}

// Summary — сводка конфигурации инстанса без секретов (ADR-130 §2.5).
type Summary struct {
	BridgeConfigured  bool  `json:"bridge_configured"`
	StorageConfigured bool  `json:"storage_configured"`
	P2PConfigured     bool  `json:"p2p_configured"`
	ConnectTimeoutMS  int64 `json:"connect_timeout_ms"`
	CallTimeoutMS     int64 `json:"call_timeout_ms"`
	KeepAliveMS       int64 `json:"keepalive_ms"`
}

// Diag — диагностика одного инстанса. nil — диагностика выключена; все
// методы безопасны на nil.
type Diag struct {
	instanceID string
	cfg        Config

	// Время (ADR-130 §2.3, поправка): startMono — monotime() при создании,
	// от неё считаются at_mono_ms событий; startWallMS — календарное
	// Unix-время того же момента, снятое ОДИН раз (instance_start_wall_ms).
	startMono   int64
	startWallMS int64

	// stamp — часы метки времени у Record (stampclock.go): грубые
	// монотонные, если на этой машине они не хуже 1 мс и дешевле точных,
	// иначе monotime. Длительности — всегда monotime.
	stamp     func() int64
	stampName string

	// Журнал — кольцо на слотах (slotring.go): номер резервируется
	// атомарно, запись — под мьютексом своего слота.
	ring *slotRing

	mu      sync.Mutex
	storage *Storage // наблюдаемое хранилище, если есть

	// Соединения, вызовы, события подписки (conn.go). Индекс conns — Conn.
	conns            [ConnMedia + 1]connTrack
	inflight         atomic.Int64
	callsTotal       atomic.Uint64
	callsSlow        atomic.Uint64
	callDur          Histogram
	classMu          sync.Mutex
	classes          [maxCallClasses]callClass
	nClasses         int
	evConnected      atomic.Uint64
	evNetworkChanged atomic.Uint64
	evMessage        atomic.Uint64
}

// New создаёт диагностику. Кольцо выделяется целиком здесь — запись потом
// не аллоцирует.
func New(cfg Config) (*Diag, error) {
	if cfg.Capacity == 0 {
		cfg.Capacity = DefaultCapacity
	}
	if cfg.Capacity < MinCapacity || cfg.Capacity > MaxCapacity {
		return nil, ErrInvalidCapacity
	}
	if cfg.SlowTx <= 0 {
		cfg.SlowTx = DefaultSlowTx
	}
	if cfg.SlowCall <= 0 {
		cfg.SlowCall = DefaultSlowCall
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	wall := wallNow()
	stamp, stampName := chooseStamp()
	return &Diag{
		stamp:       stamp,
		stampName:   stampName,
		instanceID:  hex.EncodeToString(id[:]),
		cfg:         cfg,
		startMono:   monotime(),
		startWallMS: wall.UnixMilli(),
		ring:        newSlotRing(cfg.Capacity),
	}, nil
}

// InstanceStartWallMS — календарное Unix-время старта инстанса, мс; 0 при
// выключенной диагностике.
func (d *Diag) InstanceStartWallMS() int64 {
	if d == nil {
		return 0
	}
	return d.startWallMS
}

// InstanceID — случайный идентификатор инстанса. Новый идентификатор у
// читателя означает перезапуск: чтение начинается заново с after_seq = 0.
func (d *Diag) InstanceID() string {
	if d == nil {
		return ""
	}
	return d.instanceID
}

// SlowTx — порог медленной транзакции.
func (d *Diag) SlowTx() time.Duration {
	if d == nil {
		return 0
	}
	return d.cfg.SlowTx
}

// SlowCall — порог медленного вызова.
func (d *Diag) SlowCall() time.Duration {
	if d == nil {
		return 0
	}
	return d.cfg.SlowCall
}

// Record добавляет запись в журнал. Не возвращает ошибок и не ждёт
// читателя; при переполнении вытесняет старейшую запись.
//
// Время записи — одно чтение монотонных часов (clock.go; какие именно —
// stampclock.go: грубые с разрешением ≤ 1 мс там, где они дешевле, иначе
// точные): at_mono_ms — от старта инстанса, в миллисекундах. Грубые часы
// отстают от точных не больше чем на тик (≤ 1 мс), поэтому метка Record может
// оказаться на 1 мс раньше метки соседнего события с точным временем —
// порядок задаёт seq, не время (ADR-130 §2.4). Календарного времени у записи нет (ADR-130 §2.3,
// поправка): есть instance_start_wall_ms инстанса, и календарное время
// события можно лишь оценить как instance_start_wall_ms + at_mono_ms — после
// перевода системных часов оценка расходится с настоящим временем.
//
// Порядок журнала задаёт seq (резервируется атомарно, slotring.go); время —
// сведение: две почти одновременные записи из разных горутин могут иметь
// время не в порядке seq.
func (d *Diag) Record(c Component, k Kind, conn Conn, fields ...Field) {
	if d == nil {
		return
	}
	d.recordMono(d.stamp(), c, k, conn, fields...)
}

// recordMono — Record с уже прочитанным monotime(). Для мест ВНУТРИ diag,
// где тем же чтением часов уже измерена длительность (одно чтение на
// событие, а не два); время по-прежнему формирует диагностика, не
// вызывающий.
func (d *Diag) recordMono(m int64, c Component, k Kind, conn Conn, fields ...Field) {
	rel := m - d.startMono
	if rel < 0 {
		// Грубые часы отстают от точных не больше чем на тик: метка события
		// сразу после создания не уходит в минус.
		rel = 0
	}
	d.ring.put(rel/1e6, c, k, conn, fields)
}

// Mono — отметка монотонных часов диагностики (нс) для измерения
// длительности; 0 при выключенной диагностике.
func (d *Diag) Mono() int64 {
	if d == nil {
		return 0
	}
	return monotime()
}

// Page — результат одного чтения журнала (форма ответа 820).
type Page struct {
	InstanceID string
	// InstanceStartWallMS — календарное время старта инстанса (Unix, мс).
	InstanceStartWallMS int64
	Events              []Event
	// NextSeq — номер последней отданной записи, либо after_seq, если
	// отдавать нечего. Следующее чтение — с after_seq = NextSeq.
	NextSeq uint64
	// Dropped — сколько записей с номерами в (after_seq, первая отданная)
	// уже вытеснено. Ненулевое значение — явный разрыв, а не молчаливая
	// потеря.
	Dropped uint64
}

// Read отдаёт записи с seq > afterSeq по порядку, не больше limit. Журнал
// не меняется: повтор того же запроса отдаёт те же записи (пока их не
// вытеснили). Само чтение в журнал не пишется.
func (d *Diag) Read(afterSeq uint64, limit int) (Page, error) {
	if d == nil {
		return Page{}, nil
	}
	if limit < 1 || limit > MaxReadLimit {
		return Page{}, ErrInvalidLimit
	}
	events, next, dropped, err := d.ring.read(afterSeq, limit)
	if err != nil {
		return Page{}, err
	}
	return Page{InstanceID: d.instanceID, InstanceStartWallMS: d.startWallMS, Events: events, NextSeq: next, Dropped: dropped}, nil
}

// JournalStats — состояние журнала для снимка.
type JournalStats struct {
	Capacity int    `json:"capacity"`
	Len      int    `json:"len"`
	LastSeq  uint64 `json:"last_seq"`
	Evicted  uint64 `json:"evicted"`
}

// Snapshot — снимок состояния инстанса (форма ответа 819, ADR-130 §2.5).
// Метрики Autodial (RouteStats) появятся, когда он будет подключён к рабочему
// пути — отдельным решением.
type Snapshot struct {
	InstanceID string `json:"instance_id"`
	// InstanceStartWallMS — календарное Unix-время старта инстанса, мс,
	// снятое один раз при создании (ADR-130 §2.3, поправка).
	InstanceStartWallMS int64                   `json:"instance_start_wall_ms"`
	UptimeMS            int64                   `json:"uptime_ms"`
	Connections         map[string]ConnSnapshot `json:"connections"`
	Calls               CallsSnapshot           `json:"calls"`
	Events              EventsSnapshot          `json:"events"`
	Journal             JournalStats            `json:"journal"`
	Storage             *StorageSnapshot        `json:"storage,omitempty"`
	Config              ConfigSnapshot          `json:"config"`
}

// ConfigSnapshot — сводка конфигурации без секретов.
type ConfigSnapshot struct {
	Enabled    bool  `json:"enabled"`
	Capacity   int   `json:"capacity"`
	SlowTxMS   int64 `json:"slow_tx_ms"`
	SlowCallMS int64 `json:"slow_call_ms"`
	Summary
}

// Snapshot считает снимок по запросу. Состояния не меняет и в журнал не
// пишется. На выключенной диагностике — нулевой снимок с Enabled = false.
func (d *Diag) Snapshot() Snapshot {
	if d == nil {
		return Snapshot{}
	}
	js := d.ring.stats()
	d.mu.Lock()
	st := d.storage
	d.mu.Unlock()

	now := monotime()
	s := Snapshot{
		InstanceID:          d.instanceID,
		InstanceStartWallMS: d.startWallMS,
		UptimeMS:            (now - d.startMono) / 1e6,
		Connections:         d.connSnapshot(now),
		Calls:               d.callsSnapshot(),
		Events: EventsSnapshot{
			Connected:      d.evConnected.Load(),
			NetworkChanged: d.evNetworkChanged.Load(),
			Message:        d.evMessage.Load(),
		},
		Journal: js,
		Config: ConfigSnapshot{
			Enabled:    true,
			Capacity:   d.cfg.Capacity,
			SlowTxMS:   d.cfg.SlowTx.Milliseconds(),
			SlowCallMS: d.cfg.SlowCall.Milliseconds(),
			Summary:    d.cfg.Summary,
		},
	}
	if st != nil {
		ss := st.snapshot()
		s.Storage = &ss
	}
	return s
}

func (d *Diag) attachStorage(s *Storage) {
	d.mu.Lock()
	d.storage = s
	d.mu.Unlock()
}

// truncate усекает строку до MaxText байт по границе символа. Без
// аллокаций: подстрока разделяет память исходной строки.
func truncate(s string) string {
	if len(s) <= MaxText {
		return s
	}
	i := MaxText
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return s[:i]
}
