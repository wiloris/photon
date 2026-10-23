package diag

import (
	"sync"
	"sync/atomic"
	"time"
)

// Соединения, вызовы и события подписки (ADR-130 §2.3, §2.5). Методы ниже —
// узкий интерфейс записи для session и dispatcher: каждый пишет событие
// журнала и обновляет счётчики снимка. Безопасны на nil.

// connTrack — состояние одного из трёх соединений инстанса.
type connTrack struct {
	mu    sync.Mutex
	state string
	since int64 // monotime() перехода в state

	connects      atomic.Uint64 // попыток подключения (conn_start)
	connectErrors atomic.Uint64 // неудачных попыток
	established   atomic.Uint64 // удачных; reconnects = established − 1
	lost          atomic.Uint64 // обрывов установленной связи
}

// connIndex — индекс Conn в массиве треков; ConnNone треком не владеет.
func connIndex(c Conn) int {
	if c >= ConnAction && c <= ConnMedia {
		return int(c)
	}
	return 0
}

func (d *Diag) track(c Conn) *connTrack {
	if i := connIndex(c); i != 0 {
		return &d.conns[i]
	}
	return nil
}

// SessionState — смена состояния сессии соединения conn.
func (d *Diag) SessionState(conn Conn, from, to string) {
	if d == nil {
		return
	}
	now := monotime()
	if t := d.track(conn); t != nil {
		t.mu.Lock()
		t.state, t.since = to, now
		t.mu.Unlock()
	}
	d.recordMono(now, CompSession, KindSessionState, conn, Str(KeyFrom, from), Str(KeyTo, to))
}

// SessionFailed — причина перехода в Failed: reason "connect" (не удалось
// подключиться) или "lost" (оборвалась установленная связь); text — текст
// sentinel-ошибки сессии или имя класса, без подробностей причины.
func (d *Diag) SessionFailed(conn Conn, reason, text string) {
	if d == nil {
		return
	}
	if t := d.track(conn); t != nil && reason == "lost" {
		t.lost.Add(1)
	}
	d.Record(CompSession, KindSessionFailed, conn, Str(KeyClass, reason), Str(KeyText, text))
}

// SessionClosed — сессия закрыта.
func (d *Diag) SessionClosed(conn Conn) {
	d.Record(CompSession, KindSessionClosed, conn)
}

// ConnStart — диспетчер начал подключение соединения conn. Возвращает
// отметку начала для ConnResult (одно чтение часов: оно же — время события).
func (d *Diag) ConnStart(conn Conn) int64 {
	if d == nil {
		return 0
	}
	if t := d.track(conn); t != nil {
		t.connects.Add(1)
	}
	m := monotime()
	d.recordMono(m, CompDispatcher, KindConnStart, conn)
	return m
}

// ConnResult — исход подключения, начатого ConnStart: class — "ok" или
// класс ошибки диспетчера. Длительность считает диагностика.
func (d *Diag) ConnResult(conn Conn, start int64, class string, ok bool) {
	if d == nil {
		return
	}
	end := monotime()
	dur := time.Duration(end - start)
	if t := d.track(conn); t != nil {
		if ok {
			t.established.Add(1)
		} else {
			t.connectErrors.Add(1)
		}
	}
	d.recordMono(end, CompDispatcher, KindConnResult, conn, Num(KeyDurUS, dur.Microseconds()), Str(KeyClass, class))
}

// SubBackoff — пауза перед следующей попыткой подписки.
func (d *Diag) SubBackoff(attempt int, dur time.Duration) {
	d.Record(CompDispatcher, KindSubBackoff, ConnSubscribe, Num(KeyAttempt, int64(attempt)), Num(KeyDurUS, dur.Microseconds()))
}

// callClass — счётчик вызовов одного класса исхода.
type callClass struct {
	name string
	n    atomic.Uint64
}

// maxCallClasses — классов исхода у диспетчера семь; запас на будущие.
const maxCallClasses = 16

// CallStart — вызов начат: счётчик «в полёте» и отметка начала для
// CallDone.
func (d *Diag) CallStart() int64 {
	if d == nil {
		return 0
	}
	d.inflight.Add(1)
	return monotime()
}

// CallDone — вызов, начатый CallStart, завершён. class — класс исхода
// диспетчера ("ok" для состоявшегося вызова, в том числе с ok:false в
// конверте — ADR-038). Событие пишется только для неудачи (call_failed) и
// превышения порога (call_slow); остальное — счётчиками. Длительность и
// время события — одно чтение часов.
func (d *Diag) CallDone(start int64, action int32, resource string, class string, failed bool) {
	if d == nil {
		return
	}
	end := monotime()
	dur := time.Duration(end - start)
	d.inflight.Add(-1)
	d.callsTotal.Add(1)
	d.callDur.Observe(dur)
	d.callClass(class).n.Add(1)
	if failed {
		d.recordMono(end, CompDispatcher, KindCallFailed, ConnNone, Num(KeyAction, int64(action)),
			Str(KeyResource, resource), Num(KeyDurUS, dur.Microseconds()), Str(KeyClass, class))
	}
	if dur >= d.cfg.SlowCall {
		d.callsSlow.Add(1)
		d.recordMono(end, CompDispatcher, KindCallSlow, ConnNone, Num(KeyAction, int64(action)),
			Str(KeyResource, resource), Num(KeyDurUS, dur.Microseconds()), Str(KeyClass, class))
	}
}

func (d *Diag) callClass(name string) *callClass {
	d.classMu.Lock()
	defer d.classMu.Unlock()
	for i := 0; i < d.nClasses; i++ {
		if d.classes[i].name == name {
			return &d.classes[i]
		}
	}
	if d.nClasses == maxCallClasses {
		return &d.classes[maxCallClasses-1] // переполнение — в последний
	}
	c := &d.classes[d.nClasses]
	c.name = name
	d.nClasses++
	return c
}

// Типы событий подписки — те же строки, что у dispatcher (event-delivery.md).
const (
	EventConnected      = "CONNECTED"
	EventNetworkChanged = "NETWORK_CHANGED"
	EventMessage        = "MESSAGE"
)

// EventSent — событие отдано клиенту. CONNECTED и NETWORK_CHANGED пишутся в
// журнал; MESSAGE — только счётчиком (ADR-130 §2.3): поток сообщений — не
// смены состояния.
func (d *Diag) EventSent(typ string) {
	if d == nil {
		return
	}
	switch typ {
	case EventConnected:
		d.evConnected.Add(1)
	case EventNetworkChanged:
		d.evNetworkChanged.Add(1)
	case EventMessage:
		d.evMessage.Add(1)
		return
	default:
		return
	}
	d.Record(CompEvents, KindEventSent, ConnSubscribe, Str(KeyEvent, typ))
}

// ConnSnapshot — состояние одного соединения в снимке.
type ConnSnapshot struct {
	State         string `json:"state"`
	InStateMS     int64  `json:"in_state_ms"`
	Connects      uint64 `json:"connects"`
	ConnectErrors uint64 `json:"connect_errors"`
	Reconnects    uint64 `json:"reconnects"`
	Lost          uint64 `json:"lost"`
}

// CallsSnapshot — вызовы Photon_SendAction.
type CallsSnapshot struct {
	InFlight int64             `json:"in_flight"`
	Total    uint64            `json:"total"`
	Slow     uint64            `json:"slow"`
	ByClass  map[string]uint64 `json:"by_class"`
	Duration HistogramSnapshot `json:"duration"`
}

// EventsSnapshot — события подписки, отданные клиенту. Очереди у насоса нет
// (доставка синхронная, event-delivery.md §9.2) — длины очереди в снимке нет.
type EventsSnapshot struct {
	Connected      uint64 `json:"connected"`
	NetworkChanged uint64 `json:"network_changed"`
	Message        uint64 `json:"message"`
}

func (d *Diag) connSnapshot(now int64) map[string]ConnSnapshot {
	out := make(map[string]ConnSnapshot, 3)
	for c := ConnAction; c <= ConnMedia; c++ {
		t := &d.conns[c]
		t.mu.Lock()
		st, since := t.state, t.since
		t.mu.Unlock()
		cs := ConnSnapshot{
			State:         st,
			Connects:      t.connects.Load(),
			ConnectErrors: t.connectErrors.Load(),
			Lost:          t.lost.Load(),
		}
		if st == "" {
			cs.State = "none"
		} else {
			cs.InStateMS = (now - since) / 1e6
		}
		if e := t.established.Load(); e > 1 {
			cs.Reconnects = e - 1
		}
		out[c.String()] = cs
	}
	return out
}

func (d *Diag) callsSnapshot() CallsSnapshot {
	cs := CallsSnapshot{
		InFlight: d.inflight.Load(),
		Total:    d.callsTotal.Load(),
		Slow:     d.callsSlow.Load(),
		ByClass:  map[string]uint64{},
		Duration: d.callDur.snapshot(),
	}
	d.classMu.Lock()
	for i := 0; i < d.nClasses; i++ {
		cs.ByClass[d.classes[i].name] = d.classes[i].n.Load()
	}
	d.classMu.Unlock()
	return cs
}
