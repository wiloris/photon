package session

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-core/services/diag"
)

// Переходы сессии в журнале диагностики (ADR-130 §2.3): каждый переход —
// session.state {from, to}; причина Failed — session.failed {connect | lost};
// закрытие — session.closed. Текст ошибки транспорта в журнал не идёт.

func diagStates(t *testing.T, d *diag.Diag) (string, []diag.Event) {
	t.Helper()
	p, err := d.Read(0, diag.MaxReadLimit)
	if err != nil {
		t.Fatal(err)
	}
	var tr []string
	for _, e := range p.Events {
		f, _ := e.Get(diag.KeyFrom)
		to, _ := e.Get(diag.KeyTo)
		switch e.Kind {
		case diag.KindSessionState:
			tr = append(tr, f.Str+">"+to.Str)
		case diag.KindSessionFailed:
			c, _ := e.Get(diag.KeyClass)
			x, _ := e.Get(diag.KeyText)
			tr = append(tr, "failed:"+c.Str+":"+x.Str)
		case diag.KindSessionClosed:
			tr = append(tr, "closed")
		}
		if e.Conn != diag.ConnMedia {
			t.Fatalf("conn = %s, want media", e.Conn)
		}
	}
	return strings.Join(tr, ","), p.Events
}

func TestT19_DiagTransitions(t *testing.T) {
	d, _ := diag.New(diag.Config{Capacity: 64})
	conn := newFakeConn(t, okHandshake(map[string]interface{}{"ok": true}))
	tr := &fakeTransport{conn: conn}
	s, err := New(Config{Transport: tr, Endpoint: "fake://bridge", SessionToken: "tok", Mode: ModeAction,
		Diag: d, DiagConn: diag.ConnMedia})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	conn.mu.Lock()
	conn.openErr = io.ErrClosedPipe
	conn.mu.Unlock()
	if _, err := s.Do(ctx, 100, []byte(`{}`)); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Do: %v", err)
	}
	s.Close()
	s.Close() // повтор — без второго события

	got, _ := diagStates(t, d)
	want := "created>connecting,connecting>ready,ready>failed,failed:lost:transport,failed>closed,closed"
	if got != want {
		t.Fatalf("журнал:\n got %s\nwant %s", got, want)
	}
}

func TestT19_DiagConnectFailure(t *testing.T) {
	d, _ := diag.New(diag.Config{Capacity: 64})
	tr := &fakeTransport{connectErr: errors.New("dial 192.0.2.7: secret-detail")}
	s, _ := New(Config{Transport: tr, Endpoint: "fake://bridge", SessionToken: "tok", Mode: ModeAction,
		Diag: d, DiagConn: diag.ConnMedia})
	defer s.Close()
	if err := s.Connect(context.Background()); err == nil {
		t.Fatal("Connect прошёл")
	}
	got, evs := diagStates(t, d)
	if got != "created>connecting,connecting>failed,failed:connect:transport" {
		t.Fatalf("журнал: %s", got)
	}
	for _, e := range evs {
		for i := 0; i < int(e.N); i++ {
			if strings.Contains(e.Fields[i].Str, "192.0.2.7") || strings.Contains(e.Fields[i].Str, "secret") {
				t.Fatalf("утечка: %q", e.Fields[i].Str)
			}
		}
	}
}
