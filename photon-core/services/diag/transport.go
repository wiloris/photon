package diag

import (
	"context"
	"errors"
	"net"
	"net/netip"

	"github.com/wiloris/photon-core/ports"
)

// Transport — наблюдающий декоратор поверх ports.Transport (ADR-130 §2.2).
//
// Один экземпляр — на одну роль соединения (action, subscribe, media): сам
// транспорт у инстанса общий, и без роли событие dial не говорило бы, какое
// из трёх соединений подключалось. Те же инварианты, что у декоратора
// хранилища: результат и ошибка — те же самые (без обёртки), запись не
// влияет на вызов, при выключенной диагностике декоратора нет вовсе.
//
// Close декоратора транспорт НЕ закрывает: декоратор не владеет транспортом.
// Транспорт закрывает тот, кто его создал, — ровно как до ADR-130.
type Transport struct {
	inner ports.Transport
	d     *Diag
	conn  Conn
}

var _ ports.Transport = (*Transport)(nil)

// Named — транспорт, сообщающий своё имя ("quic", "tcp") для события dial.
// Необязательный интерфейс, не часть ports.Transport (ADR-020: порт не
// меняется): имя сообщает сам адаптер, поэтому в журнале — фактический
// транспорт, а не предположение вызывающего. Транспорт без имени — поля
// transport в событии нет.
type Named interface {
	TransportName() string
}

// WrapTransport возвращает inner под наблюдением d для соединения роли conn;
// при d == nil — inner как есть.
func WrapTransport(inner ports.Transport, d *Diag, conn Conn) ports.Transport {
	if d == nil || inner == nil {
		return inner
	}
	return &Transport{inner: inner, d: d, conn: conn}
}

// Connect — см. ports.Transport. Событие dial: адрес endpoint, длительность,
// класс исхода. Текст ошибки транспорта не пишется — в нём адреса и детали
// стека, а класса достаточно, чтобы отличить отмену и таймаут от отказа.
//
// Путь подключения (ADR-130 §2.3, дополнение): transport — имя, сообщённое
// самим адаптером (Named); remote и family — только у УСТАНОВЛЕННОГО
// соединения, из его RemoteEndpoint (фактический адрес после резолва). У
// неудачной попытки соединения нет, и ни адреса, ни семейства в событии нет
// — подставлять заявленный endpoint значило бы выдать попытку за путь.
// family пишется, только если remote — IP-адрес; иначе поля нет.
func (t *Transport) Connect(ctx context.Context, endpoint ports.Endpoint) (ports.Connection, error) {
	t0 := monotime()
	c, err := t.inner.Connect(ctx, endpoint)
	t1 := monotime()
	var f [MaxFields]Field
	n := 0
	add := func(x Field) { f[n] = x; n++ }
	add(Str(KeyEndpoint, string(endpoint)))
	add(Num(KeyDurUS, (t1-t0)/1e3))
	add(Str(KeyClass, dialClass(err)))
	if nm, ok := t.inner.(Named); ok {
		if name := nm.TransportName(); name != "" {
			add(Str(KeyTransport, name))
		}
	}
	if err == nil {
		if remote := string(c.RemoteEndpoint()); remote != "" {
			add(Str(KeyRemote, remote))
			if fam := familyOf(remote); fam != "" {
				add(Str(KeyFamily, fam))
			}
		}
	}
	t.d.recordMono(t1, CompTransport, KindDial, t.conn, f[:n]...)
	if err != nil {
		return nil, err
	}
	return &connection{Connection: c, d: t.d, conn: t.conn}, nil
}

// familyOf — семейство адреса "host:port" по самому адресу: "IPv4",
// "IPv6" или "" (host — не IP-адрес). IPv4, отображённый в IPv6
// (::ffff:a.b.c.d), — IPv4: пакеты идут по IPv4. Имена — те же, что у
// autodial.Family.
func familyOf(hostport string) string {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return ""
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return ""
	}
	if a.Is4() || a.Is4In6() {
		return "IPv4"
	}
	return "IPv6"
}

// Close — см. ports.Transport. Ничего не делает: см. комментарий к типу.
func (t *Transport) Close() error { return nil }

func dialClass(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	return "error"
}

// connection — соединение, закрытие которого наблюдается. Остальные методы —
// встроенного ports.Connection, без изменений.
type connection struct {
	ports.Connection
	d    *Diag
	conn Conn
}

// Close — событие conn_closed с причиной "local": соединение закрыл сам
// инстанс. Обрыв со стороны сети на уровне транспорта не виден — его
// обнаруживает сессия (session.failed с reason=lost).
func (c *connection) Close() error {
	err := c.Connection.Close()
	c.d.Record(CompTransport, KindConnClosed, c.conn, Str(KeyClass, "local"))
	return err
}
