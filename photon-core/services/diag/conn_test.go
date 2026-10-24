package diag

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
)

// fakeTransport — транспорт с подставляемой ошибкой и наблюдаемым Close.
type fakeTransport struct {
	err    error
	conn   *fakeConn
	closed int
}

func (f *fakeTransport) Connect(ctx context.Context, ep ports.Endpoint) (ports.Connection, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.conn, nil
}
func (f *fakeTransport) Close() error { f.closed++; return nil }

type fakeConn struct {
	closed   int
	closeErr error
	opened   int
}

func (c *fakeConn) OpenStream(context.Context) (ports.Stream, error) {
	c.opened++
	return nil, errors.New("open")
}
func (c *fakeConn) AcceptStream(context.Context) (ports.Stream, error)        { return nil, nil }
func (c *fakeConn) OpenUniStream(context.Context) (ports.SendStream, error)   { return nil, nil }
func (c *fakeConn) AcceptUniStream(context.Context) (ports.RecvStream, error) { return nil, nil }
func (c *fakeConn) RemoteEndpoint() ports.Endpoint                            { return "fake" }
func (c *fakeConn) Close() error                                              { c.closed++; return c.closeErr }

// Декоратор транспорта: та же ошибка, методы соединения проходят насквозь,
// закрытие соединения наблюдается, транспорт декоратор не закрывает,
// выключенная диагностика — без декоратора.
func TestTransportDecorator(t *testing.T) {
	d := newTestDiag(t, 64)
	dialErr := errors.New("refused 10.1.2.3")
	inner := &fakeTransport{err: dialErr}
	tr := WrapTransport(inner, d, ConnSubscribe)
	if _, err := tr.Connect(context.Background(), "host:1"); err != dialErr {
		t.Fatalf("Connect: err = %v, want тот же", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	inner.err = canceled.Err()
	if _, err := tr.Connect(canceled, "host:1"); err != context.Canceled {
		t.Fatalf("Connect canceled: %v", err)
	}

	closeErr := errors.New("close")
	inner.err, inner.conn = nil, &fakeConn{closeErr: closeErr}
	c, err := tr.Connect(context.Background(), "host:1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.OpenStream(context.Background()); err == nil || inner.conn.opened != 1 {
		t.Fatal("OpenStream не дошёл до соединения")
	}
	if c.RemoteEndpoint() != "fake" {
		t.Fatal("RemoteEndpoint")
	}
	if err := c.Close(); err != closeErr || inner.conn.closed != 1 {
		t.Fatalf("Close: err = %v, закрытий %d", err, inner.conn.closed)
	}
	if tr.Close() != nil || inner.closed != 0 {
		t.Fatal("декоратор закрыл чужой транспорт")
	}

	p, _ := d.Read(0, MaxReadLimit)
	var classes []string
	for _, e := range p.Events {
		if e.Kind == KindDial {
			if e.Conn != ConnSubscribe || fieldStr(e, KeyEndpoint) != "host:1" {
				t.Fatalf("dial: %v", e.Wire())
			}
			classes = append(classes, fieldStr(e, KeyClass))
		}
	}
	if len(classes) != 3 || classes[0] != "error" || classes[1] != "canceled" || classes[2] != "ok" {
		t.Fatalf("классы dial: %v", classes)
	}
	if cc := events(t, d, KindConnClosed); len(cc) != 1 || fieldStr(cc[0], KeyClass) != "local" {
		t.Fatalf("conn_closed: %v", cc)
	}
	assertNoLeak(t, d, "10.1.2.3", "refused")

	if WrapTransport(inner, nil, ConnAction) != ports.Transport(inner) {
		t.Fatal("выключенная диагностика обернула транспорт")
	}
}

// Счётчики соединений, вызовов и событий подписки в снимке.
func TestConnCallsEventsSnapshot(t *testing.T) {
	d, _ := New(Config{Capacity: 64, SlowCall: 50 * time.Millisecond})
	d.SessionState(ConnAction, "created", "connecting")
	d.ConnResult(ConnAction, d.ConnStart(ConnAction), "not_connected", false)
	d.ConnResult(ConnAction, d.ConnStart(ConnAction), "ok", true)
	d.SessionState(ConnAction, "connecting", "ready")
	d.SessionFailed(ConnAction, "lost", "transport")
	d.ConnResult(ConnAction, d.ConnStart(ConnAction), "ok", true)

	c1 := d.CallStart()
	c2 := d.CallStart()
	if d.Snapshot().Calls.InFlight != 2 {
		t.Fatal("in_flight")
	}
	d.CallDone(c1, 100, "PostResource", "ok", false)
	d.CallDone(c2-int64(60*time.Millisecond), 100, "PostResource", "timeout", true) // дольше порога
	for i := 0; i < 5; i++ {
		d.EventSent(EventMessage)
	}
	d.EventSent(EventConnected)
	d.EventSent("UNKNOWN")

	s := d.Snapshot()
	a := s.Connections["action"]
	if a.State != "ready" || a.Connects != 3 || a.ConnectErrors != 1 || a.Reconnects != 1 || a.Lost != 1 || a.InStateMS < 0 {
		t.Fatalf("action: %+v", a)
	}
	if s.Connections["media"].State != "none" {
		t.Fatalf("media без событий: %+v", s.Connections["media"])
	}
	if s.Calls.InFlight != 0 || s.Calls.Total != 2 || s.Calls.Slow != 1 || s.Calls.ByClass["ok"] != 1 || s.Calls.ByClass["timeout"] != 1 || s.Calls.Duration.N != 2 {
		t.Fatalf("calls: %+v", s.Calls)
	}
	if s.Events.Message != 5 || s.Events.Connected != 1 {
		t.Fatalf("events: %+v", s.Events)
	}
	if n := len(events(t, d, KindEventSent)); n != 1 {
		t.Fatalf("event_sent в журнале: %d, want 1 (MESSAGE и неизвестные — нет)", n)
	}
	if n := len(events(t, d, KindCallFailed)); n != 1 {
		t.Fatalf("call_failed: %d", n)
	}
	if n := len(events(t, d, KindCallSlow)); n != 1 {
		t.Fatalf("call_slow: %d", n)
	}
}

// Классов исхода больше предела — счётчик не теряет вызовы.
func TestCallClassOverflow(t *testing.T) {
	d := newTestDiag(t, 64)
	for i := 0; i < maxCallClasses+5; i++ {
		d.CallDone(d.CallStart(), 1, "", string(rune('a'+i)), false)
	}
	var sum uint64
	for _, n := range d.Snapshot().Calls.ByClass {
		sum += n
	}
	if sum != maxCallClasses+5 {
		t.Fatalf("сумма по классам %d, want %d", sum, maxCallClasses+5)
	}
}

// Nil-диагностика: новые методы безопасны.
func TestNilDiagConnMethods(t *testing.T) {
	var d *Diag
	d.SessionState(ConnAction, "a", "b")
	d.SessionFailed(ConnAction, "lost", "x")
	d.SessionClosed(ConnAction)
	d.ConnResult(ConnAction, d.ConnStart(ConnAction), "ok", true)
	d.SubBackoff(1, 0)
	d.CallDone(d.CallStart(), 1, "", "ok", false)
	d.EventSent(EventConnected)
}
