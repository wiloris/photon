package dispatcher

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-core/services/diag"
)

// События сессии, диспетчера, транспорта и подписки (ADR-130 §2.3, §6 п. 2)
// на фиктивном Bridge. Проверяется ЛОГИКА наблюдения; времена на настоящей
// сети — сценарии §5.1, отдельно.

// journal — все записи журнала инстанса.
func journal(t *testing.T, d *Dispatcher) []diag.Event {
	t.Helper()
	var out []diag.Event
	cur := uint64(0)
	for {
		p, err := d.Diag().Read(cur, diag.MaxReadLimit)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if p.Dropped != 0 {
			t.Fatalf("журнал вытеснил %d записей — ёмкость мала для теста", p.Dropped)
		}
		if len(p.Events) == 0 {
			return out
		}
		out = append(out, p.Events...)
		cur = p.NextSeq
	}
}

func str(e diag.Event, k diag.Key) string { f, _ := e.Get(k); return f.Str }
func num(e diag.Event, k diag.Key) int64  { f, _ := e.Get(k); return f.Num }

// find — записи вида k соединения conn (ConnNone — любого).
func find(evs []diag.Event, k diag.Kind, conn diag.Conn) []diag.Event {
	var out []diag.Event
	for _, e := range evs {
		if e.Kind == k && (conn == diag.ConnNone || e.Conn == conn) {
			out = append(out, e)
		}
	}
	return out
}

func states(evs []diag.Event, conn diag.Conn) string {
	var s []string
	for _, e := range find(evs, diag.KindSessionState, conn) {
		s = append(s, str(e, diag.KeyFrom)+">"+str(e, diag.KeyTo))
	}
	return strings.Join(s, ",")
}

func assertNoSecret(t *testing.T, d *Dispatcher, forbidden ...string) {
	t.Helper()
	for _, e := range journal(t, d) {
		for i := 0; i < int(e.N); i++ {
			for _, f := range forbidden {
				if strings.Contains(e.Fields[i].Str, f) {
					t.Fatalf("seq %d %s.%s поле %s содержит %q", e.Seq, e.Component, e.Kind, e.Fields[i].Key, f)
				}
			}
		}
	}
}

// Вызов на новом инстансе: подключение action наблюдается целиком и по
// порядку; вызов учтён в снимке; после обрыва — failed{lost}, отказ вызова с
// классом, ленивое переподключение считается reconnect.
func TestDiagActionConnectionLifecycle(t *testing.T) {
	skipIfDiagForcedOff(t)
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)

	if _, err := d.SendAction(100, []byte(`{}`)); err != nil {
		t.Fatalf("SendAction: %v", err)
	}
	evs := journal(t, d)
	if got := states(evs, diag.ConnAction); got != "created>connecting,connecting>ready" {
		t.Fatalf("переходы action: %s", got)
	}
	start, dial, result := find(evs, diag.KindConnStart, diag.ConnAction), find(evs, diag.KindDial, diag.ConnAction), find(evs, diag.KindConnResult, diag.ConnAction)
	if len(start) != 1 || len(dial) != 1 || len(result) != 1 {
		t.Fatalf("conn_start/dial/conn_result: %d/%d/%d", len(start), len(dial), len(result))
	}
	if !(start[0].Seq < dial[0].Seq && dial[0].Seq < result[0].Seq) {
		t.Fatalf("порядок: conn_start %d, dial %d, conn_result %d", start[0].Seq, dial[0].Seq, result[0].Seq)
	}
	if str(dial[0], diag.KeyEndpoint) != "fake://bridge" || str(dial[0], diag.KeyClass) != "ok" || str(result[0], diag.KeyClass) != "ok" {
		t.Fatalf("dial %v / conn_result %v", dial[0].Wire(), result[0].Wire())
	}
	s := d.Diag().Snapshot()
	if a := s.Connections["action"]; a.State != "ready" || a.Connects != 1 || a.Reconnects != 0 {
		t.Fatalf("action в снимке: %+v", a)
	}
	if s.Calls.Total != 1 || s.Calls.ByClass["ok"] != 1 || s.Calls.InFlight != 0 || len(find(evs, diag.KindCallFailed, diag.ConnNone)) != 0 {
		t.Fatalf("вызовы: %+v", s.Calls)
	}

	// Обрыв: следующий вызов падает, сессия — Failed по причине lost.
	br.breakLast(io.ErrClosedPipe)
	if _, err := d.SendAction(100, []byte(`{}`)); err == nil {
		t.Fatal("вызов на оборванном соединении прошёл")
	}
	evs = journal(t, d)
	failed := find(evs, diag.KindSessionFailed, diag.ConnAction)
	if len(failed) != 1 || str(failed[0], diag.KeyClass) != "lost" {
		t.Fatalf("session.failed: %v", failed)
	}
	cf := find(evs, diag.KindCallFailed, diag.ConnNone)
	if len(cf) != 1 || num(cf[0], diag.KeyAction) != 100 || str(cf[0], diag.KeyResource) != "PostResource" || str(cf[0], diag.KeyClass) != "not_connected" {
		t.Fatalf("call_failed: %v", cf)
	}

	// Ленивое переподключение — второй connect, reconnects = 1; прежнее
	// соединение закрыто (conn_closed).
	if _, err := d.SendAction(100, []byte(`{}`)); err != nil {
		t.Fatalf("после обрыва: %v", err)
	}
	s = d.Diag().Snapshot()
	if a := s.Connections["action"]; a.State != "ready" || a.Connects != 2 || a.Reconnects != 1 || a.Lost != 1 {
		t.Fatalf("action после переподключения: %+v", a)
	}
	if s.Calls.Total != 3 || s.Calls.ByClass["ok"] != 2 || s.Calls.ByClass["not_connected"] != 1 {
		t.Fatalf("вызовы: %+v", s.Calls)
	}
	if len(find(journal(t, d), diag.KindConnClosed, diag.ConnAction)) != 1 {
		t.Fatal("оборванное соединение не закрыто — conn_closed нет")
	}
	assertNoSecret(t, d, "tok")
}

// Неудачное подключение: dial с классом, failed{connect} без текста ошибки
// транспорта (в нём адрес «секрета»).
func TestDiagConnectFailure(t *testing.T) {
	skipIfDiagForcedOff(t)
	br := newFakeBridge()
	br.connectErr = errors.New("dial 10.0.0.1:4443 /home/user/secret refused")
	d := newDispatcher(t, br, nil)
	if _, err := d.SendAction(100, []byte(`{}`)); err == nil {
		t.Fatal("вызов прошёл без связи")
	}
	evs := journal(t, d)
	dial := find(evs, diag.KindDial, diag.ConnAction)
	failed := find(evs, diag.KindSessionFailed, diag.ConnAction)
	result := find(evs, diag.KindConnResult, diag.ConnAction)
	if len(dial) != 1 || str(dial[0], diag.KeyClass) != "error" {
		t.Fatalf("dial: %v", dial)
	}
	if len(failed) != 1 || str(failed[0], diag.KeyClass) != "connect" || str(failed[0], diag.KeyText) != "transport" {
		t.Fatalf("session.failed: %v", failed)
	}
	if len(result) != 1 || str(result[0], diag.KeyClass) == "ok" {
		t.Fatalf("conn_result: %v", result)
	}
	if a := d.Diag().Snapshot().Connections["action"]; a.State != "failed" || a.ConnectErrors != 1 {
		t.Fatalf("action: %+v", a)
	}
	assertNoSecret(t, d, "10.0.0.1", "/home/user", "secret")
}

// Отказ handshake — текст sentinel-ошибки сессии, а не текст сервера.
func TestDiagHandshakeRejected(t *testing.T) {
	skipIfDiagForcedOff(t)
	br := newFakeBridge()
	br.handshakeOK, br.handshakeErr = false, "token SECRET-SERVER-TEXT rejected"
	d := newDispatcher(t, br, nil)
	d.SendAction(100, []byte(`{}`))
	failed := find(journal(t, d), diag.KindSessionFailed, diag.ConnAction)
	if len(failed) != 1 || !strings.Contains(str(failed[0], diag.KeyText), "handshake rejected") {
		t.Fatalf("session.failed: %v", failed)
	}
	assertNoSecret(t, d, "SECRET-SERVER-TEXT")
}

// Вызов дольше порога — call_slow, даже успешный.
func TestDiagSlowCall(t *testing.T) {
	skipIfDiagForcedOff(t)
	br := newFakeBridge()
	d := newDispatcher(t, br, func(c *Config) { c.Diagnostics.SlowCall = time.Nanosecond })
	if _, err := d.SendAction(100, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	slow := find(journal(t, d), diag.KindCallSlow, diag.ConnNone)
	if len(slow) != 1 || str(slow[0], diag.KeyClass) != "ok" || num(slow[0], diag.KeyDurUS) < 0 {
		t.Fatalf("call_slow: %v", slow)
	}
	if d.Diag().Snapshot().Calls.Slow != 1 {
		t.Fatal("счётчик медленных вызовов")
	}
}

// Подписка: CONNECTED в журнале, MESSAGE — только счётчиком, обрыв —
// failed{lost} + NETWORK_CHANGED + пауза с номером попытки, восстановление —
// reconnect подписки.
func TestDiagSubscriptionLifecycle(t *testing.T) {
	skipIfDiagForcedOff(t)
	br := newFakeBridge()
	d := newDispatcher(t, br, func(c *Config) {
		c.EventBackoffMin = timeUnit / 10
		c.EventBackoffMax = timeUnit / 5
	})
	c := &collector{}
	startPump(t, br, d, c)
	c.waitFor(t, 1)
	first := br.waitSubscribeConn(t, 0)
	first.sendPush(t, `{"event":"posted","data":{"text":"СЕКРЕТНОЕ сообщение"}}`)
	c.waitFor(t, 2)
	first.breakPush()
	c.waitFor(t, 4) // NETWORK_CHANGED, CONNECTED

	evs := journal(t, d)
	sent := find(evs, diag.KindEventSent, diag.ConnSubscribe)
	var types []string
	for _, e := range sent {
		types = append(types, str(e, diag.KeyEvent))
	}
	if strings.Join(types, ",") != "CONNECTED,NETWORK_CHANGED,CONNECTED" {
		t.Fatalf("event_sent: %v (MESSAGE в журнал не пишется)", types)
	}
	if f := find(evs, diag.KindSessionFailed, diag.ConnSubscribe); len(f) != 1 || str(f[0], diag.KeyClass) != "lost" {
		t.Fatalf("subscribe failed: %v", f)
	}
	b := find(evs, diag.KindSubBackoff, diag.ConnSubscribe)
	if len(b) != 1 || num(b[0], diag.KeyAttempt) != 1 || num(b[0], diag.KeyDurUS) != (timeUnit/10).Microseconds() {
		t.Fatalf("sub_backoff: %v", b)
	}
	s := d.Diag().Snapshot()
	if s.Events.Connected != 2 || s.Events.NetworkChanged != 1 || s.Events.Message != 1 {
		t.Fatalf("события: %+v", s.Events)
	}
	if sub := s.Connections["subscribe"]; sub.State != "ready" || sub.Reconnects != 1 || sub.Lost != 1 {
		t.Fatalf("subscribe: %+v", sub)
	}
	assertNoSecret(t, d, "СЕКРЕТНОЕ", "tok")
}

// Неудачи подписки подряд — номера попыток растут, пауза удваивается.
func TestDiagSubscriptionBackoffAttempts(t *testing.T) {
	skipIfDiagForcedOff(t)
	br := newFakeBridge()
	br.handshakeOK = false
	d := newDispatcher(t, br, func(c *Config) {
		c.EventBackoffMin = timeUnit / 20
		c.EventBackoffMax = timeUnit / 5
	})
	if err := d.StartEvents(func([]byte) {}, nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * timeUnit)
	for time.Now().Before(deadline) && len(find(journal(t, d), diag.KindSubBackoff, diag.ConnSubscribe)) < 3 {
		time.Sleep(timeUnit / 20)
	}
	b := find(journal(t, d), diag.KindSubBackoff, diag.ConnSubscribe)
	if len(b) < 3 {
		t.Fatalf("пауз: %d", len(b))
	}
	for i := 0; i < 3; i++ {
		want := (timeUnit / 20) << i
		if want > timeUnit/5 {
			want = timeUnit / 5
		}
		if num(b[i], diag.KeyAttempt) != int64(i+1) || num(b[i], diag.KeyDurUS) != want.Microseconds() {
			t.Fatalf("пауза %d: attempt=%d dur_us=%d, want %d/%d", i, num(b[i], diag.KeyAttempt), num(b[i], diag.KeyDurUS), i+1, want.Microseconds())
		}
	}
}

// Медиасоединение наблюдается своей ролью.
func TestDiagMediaConnection(t *testing.T) {
	skipIfDiagForcedOff(t)
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	if _, err := d.mediaSession(context.Background()); err != nil {
		t.Fatalf("mediaSession: %v", err)
	}
	evs := journal(t, d)
	if got := states(evs, diag.ConnMedia); got != "created>connecting,connecting>ready" {
		t.Fatalf("переходы media: %s", got)
	}
	if len(find(evs, diag.KindDial, diag.ConnMedia)) != 1 || len(find(evs, diag.KindConnResult, diag.ConnMedia)) != 1 {
		t.Fatal("dial/conn_result media")
	}
	if m := d.Diag().Snapshot().Connections["media"]; m.State != "ready" || m.Connects != 1 {
		t.Fatalf("media: %+v", m)
	}
}

// Close: каждая открытая сессия — closed, каждое соединение — conn_closed;
// после Close вызовов в полёте нет.
func TestDiagCloseRecordsEverySession(t *testing.T) {
	skipIfDiagForcedOff(t)
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	c := &collector{}
	startPump(t, br, d, c)
	c.waitFor(t, 1)
	d.SendAction(100, []byte(`{}`))
	dg := d.Diag()
	d.Close()

	p, _ := dg.Read(0, diag.MaxReadLimit)
	closed := map[diag.Conn]int{}
	connClosed := map[diag.Conn]int{}
	for _, e := range p.Events {
		switch e.Kind {
		case diag.KindSessionClosed:
			closed[e.Conn]++
		case diag.KindConnClosed:
			connClosed[e.Conn]++
		}
	}
	for _, conn := range []diag.Conn{diag.ConnAction, diag.ConnSubscribe} {
		if closed[conn] < 1 || connClosed[conn] < 1 {
			t.Errorf("%s: session.closed %d, conn_closed %d", conn, closed[conn], connClosed[conn])
		}
	}
	if s := dg.Snapshot(); s.Calls.InFlight != 0 || s.Connections["action"].State != "closed" {
		t.Fatalf("после Close: in_flight %d, action %s", s.Calls.InFlight, s.Connections["action"].State)
	}
}

// Полнота схемы (ADR-130 §5.4): каждое событие §2.3, кроме Autodial (не
// подключён к рабочему пути — отдельное решение), порождается сценарием.
func TestDiagEveryKindEmitted(t *testing.T) {
	skipIfDiagForcedOff(t)
	br := newFakeBridge()
	d := newDispatcher(t, br, func(c *Config) {
		c.StoragePath = filepath.Join(t.TempDir(), "e.db")
		c.EventBackoffMin = timeUnit / 10
		c.EventBackoffMax = timeUnit / 5
		c.Diagnostics.SlowCall = time.Nanosecond
		c.Diagnostics.SlowTx = time.Nanosecond
	})
	c := &collector{}
	startPump(t, br, d, c)
	c.waitFor(t, 1)
	d.SendAction(800, []byte(`{"channel_id":"c","limit":10}`))
	d.SendAction(801, []byte(`{}`)) // невалидный блок — call_failed
	// Отказ транзакции — через хранилище инстанса (оно под декоратором).
	d.st.Update(context.Background(), func(ports.WriteTx) error { return errors.New("x") })
	d.SendAction(100, []byte(`{}`))
	br.breakLast(io.ErrClosedPipe)
	d.SendAction(100, []byte(`{}`))
	br.waitSubscribeConn(t, 0).breakPush()
	c.waitFor(t, 3)
	dg := d.Diag()
	d.Close()

	got := map[diag.Kind]bool{}
	p, _ := dg.Read(0, diag.MaxReadLimit)
	for _, e := range p.Events {
		got[e.Kind] = true
	}
	pending := map[diag.Kind]bool{diag.KindRouteSelected: true, diag.KindRouteAttempt: true}
	for k := diag.Kind(1); k.String() != "unknown"; k++ {
		if got[k] == pending[k] {
			t.Errorf("%s.%s: порождено=%v, ожидается=%v", k.Component(), k, got[k], !pending[k])
		}
	}
}
