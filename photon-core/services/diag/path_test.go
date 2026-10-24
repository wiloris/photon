package diag

import (
	"context"
	"errors"
	"testing"

	"github.com/wiloris/photon/photon-core/ports"
)

// Путь подключения в событии dial (ADR-130 §2.3, дополнение): transport —
// от самого адаптера, remote и family — только у установленного соединения.

type namedFake struct {
	fakeTransport
	name string
}

func (n *namedFake) TransportName() string { return n.name }

type addrConn struct {
	fakeConn
	remote ports.Endpoint
}

func (c *addrConn) RemoteEndpoint() ports.Endpoint { return c.remote }

type remoteTransport struct {
	name   string
	remote ports.Endpoint
	err    error
}

func (r *remoteTransport) Connect(context.Context, ports.Endpoint) (ports.Connection, error) {
	if r.err != nil {
		return nil, r.err
	}
	return &addrConn{remote: r.remote}, nil
}
func (r *remoteTransport) Close() error          { return nil }
func (r *remoteTransport) TransportName() string { return r.name }

func TestFamilyOf(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"127.0.0.1:4443", "IPv4"},
		{"203.0.113.7:1", "IPv4"},
		{"[::1]:4443", "IPv6"},
		{"[2001:db8::1]:443", "IPv6"},
		{"[fe80::1%eth0]:443", "IPv6"},
		{"[::ffff:192.0.2.1]:443", "IPv4"}, // IPv4, отображённый в IPv6, — пакеты по IPv4
		{"example.com:443", ""},            // имя — семейство неизвестно
		{"fake", ""},
		{"", ""},
	} {
		if got := familyOf(c.in); got != c.want {
			t.Errorf("familyOf(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func lastDial(t *testing.T, d *Diag) Event {
	t.Helper()
	ds := events(t, d, KindDial)
	if len(ds) == 0 {
		t.Fatal("события dial нет")
	}
	return ds[len(ds)-1]
}

func has(e Event, k Key) bool { _, ok := e.Get(k); return ok }

func TestDialPathFields(t *testing.T) {
	d := newTestDiag(t, 64)
	ctx := context.Background()

	// Успех, IPv6: все шесть полей — ровно MaxFields.
	tr := WrapTransport(&remoteTransport{name: "quic", remote: "[2001:db8::7]:4443"}, d, ConnAction)
	if _, err := tr.Connect(ctx, "bridge.example:4443"); err != nil {
		t.Fatal(err)
	}
	e := lastDial(t, d)
	if fieldStr(e, KeyTransport) != "quic" || fieldStr(e, KeyRemote) != "[2001:db8::7]:4443" ||
		fieldStr(e, KeyFamily) != "IPv6" || fieldStr(e, KeyEndpoint) != "bridge.example:4443" ||
		fieldStr(e, KeyClass) != "ok" || !has(e, KeyDurUS) || int(e.N) != MaxFields {
		t.Fatalf("dial IPv6: %v", e.Wire())
	}

	// Успех, IPv4.
	tr = WrapTransport(&remoteTransport{name: "tcp", remote: "198.51.100.2:443"}, d, ConnSubscribe)
	tr.Connect(ctx, "bridge.example:443")
	if e := lastDial(t, d); fieldStr(e, KeyTransport) != "tcp" || fieldStr(e, KeyFamily) != "IPv4" ||
		fieldStr(e, KeyRemote) != "198.51.100.2:443" {
		t.Fatalf("dial IPv4: %v", e.Wire())
	}

	// Отказ: транспорт известен, адреса и семейства нет — соединения нет.
	tr = WrapTransport(&remoteTransport{name: "quic", remote: "[::1]:1", err: errors.New("no route")}, d, ConnAction)
	tr.Connect(ctx, "[::1]:1")
	if e := lastDial(t, d); fieldStr(e, KeyTransport) != "quic" || fieldStr(e, KeyClass) != "error" ||
		has(e, KeyRemote) || has(e, KeyFamily) {
		t.Fatalf("dial отказ: %v", e.Wire())
	}

	// Транспорт без имени — поля transport нет; адрес — не IP: remote есть,
	// семейства нет.
	tr = WrapTransport(&fakeTransport{conn: &fakeConn{}}, d, ConnMedia)
	tr.Connect(ctx, "host:1")
	if e := lastDial(t, d); has(e, KeyTransport) || fieldStr(e, KeyRemote) != "fake" || has(e, KeyFamily) {
		t.Fatalf("dial без имени: %v", e.Wire())
	}

	// Имя сообщает обёрнутый транспорт, а не вызывающий.
	tr = WrapTransport(&namedFake{fakeTransport: fakeTransport{conn: &fakeConn{}}, name: "masked"}, d, ConnAction)
	tr.Connect(ctx, "host:1")
	if e := lastDial(t, d); fieldStr(e, KeyTransport) != "masked" {
		t.Fatalf("имя транспорта: %v", e.Wire())
	}
}

// Ключ remote — в схеме журнала под своим именем.
func TestRemoteKeyName(t *testing.T) {
	if KeyRemote.String() != "remote" || KeyFamily.String() != "family" || KeyTransport.String() != "transport" {
		t.Fatal("имена ключей пути")
	}
}
