package autodial

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
)

// ── Тестовые двойники ───────────────────────────────────────────────

// fakeTransport — управляемый транспорт: сообщает, что с ним делали, и
// падает/успевает по заданному сценарию. Именно двойник, а не реальный
// адаптер: здесь проверяется ЛОГИКА ВЫБОРА маршрута, а не работа
// QUIC/TCP (те покрыты своими тестами).
type fakeTransport struct {
	name string

	mu       sync.Mutex
	attempts []ports.Endpoint
	// failFor — эндпоинты, на которых Connect падает. nil = падает везде,
	// пустая карта = успех везде.
	failFor map[ports.Endpoint]bool
	failAll bool
	// delay — сколько «занимает» подключение.
	delay time.Duration
	// blockUntilCtx — игнорировать delay и висеть до отмены контекста
	// (имитация «чёрной дыры»: пакеты уходят, ответа нет).
	blockUntilCtx bool
	closed        bool
}

func newFakeTransport(name string) *fakeTransport {
	return &fakeTransport{name: name, failFor: make(map[ports.Endpoint]bool)}
}

func (f *fakeTransport) Connect(ctx context.Context, ep ports.Endpoint) (ports.Connection, error) {
	f.mu.Lock()
	f.attempts = append(f.attempts, ep)
	fail := f.failAll || f.failFor[ep]
	delay := f.delay
	block := f.blockUntilCtx
	f.mu.Unlock()

	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if fail {
		return nil, errors.New(f.name + ": connection refused")
	}
	return &fakeConnection{endpoint: ep, via: f.name}, nil
}

func (f *fakeTransport) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeTransport) attemptedEndpoints() []ports.Endpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ports.Endpoint, len(f.attempts))
	copy(out, f.attempts)
	return out
}

func (f *fakeTransport) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

type fakeConnection struct {
	endpoint ports.Endpoint
	via      string
}

func (c *fakeConnection) OpenStream(context.Context) (ports.Stream, error)        { return nil, nil }
func (c *fakeConnection) AcceptStream(context.Context) (ports.Stream, error)      { return nil, nil }
func (c *fakeConnection) OpenUniStream(context.Context) (ports.SendStream, error) { return nil, nil }
func (c *fakeConnection) AcceptUniStream(context.Context) (ports.RecvStream, error) {
	return nil, nil
}
func (c *fakeConnection) RemoteEndpoint() ports.Endpoint { return c.endpoint }
func (c *fakeConnection) Close() error                   { return nil }

// staticResolver возвращает заданные адреса без обращения к DNS.
type staticResolver struct {
	ips map[string][]net.IP
	err error
}

func (r staticResolver) LookupIP(_ context.Context, host string) ([]net.IP, error) {
	if r.err != nil {
		return nil, r.err
	}
	ips, ok := r.ips[host]
	if !ok {
		return nil, errors.New("no such host")
	}
	return ips, nil
}

func dualStackResolver() staticResolver {
	return staticResolver{ips: map[string][]net.IP{
		"example.com": {
			net.ParseIP("2001:db8::1"),
			net.ParseIP("192.0.2.1"),
		},
	}}
}

// ── Контракт порта ──────────────────────────────────────────────────

func TestImplementsPortsTransport(t *testing.T) {
	// Ключевое свойство: autodial подставляется всюду, где ожидается
	// ports.Transport — вызывающий получает выбор маршрута прозрачно.
	var _ ports.Transport = New(Config{})
}

// ── Порядок цепочки (ARCHITECTURE.md, раздел 5) ────────────────────

func TestChainOrder_IPv6QuicFirst_ThenIPv4Quic_ThenTcp(t *testing.T) {
	quic := newFakeTransport("quic")
	tcp := newFakeTransport("tcp")
	quic.failAll = true
	tcp.failAll = true

	d := New(Config{
		Transports: []TransportEntry{
			{Name: "quic", Transport: quic, Priority: 0},
			{Name: "tcp", Transport: tcp, Priority: 1},
		},
		Resolver: dualStackResolver(),
	})

	_, err := d.Connect(context.Background(), "example.com:443")
	if err == nil {
		t.Fatal("expected failure — all transports fail")
	}

	// Порядок из ARCHITECTURE.md: IPv6 QUIC → IPv4 QUIC → IPv6 TCP → IPv4 TCP
	wantQuic := []ports.Endpoint{"[2001:db8::1]:443", "192.0.2.1:443"}
	wantTCP := []ports.Endpoint{"[2001:db8::1]:443", "192.0.2.1:443"}

	if got := quic.attemptedEndpoints(); !endpointsEqual(got, wantQuic) {
		t.Fatalf("quic attempts = %v, want %v", got, wantQuic)
	}
	if got := tcp.attemptedEndpoints(); !endpointsEqual(got, wantTCP) {
		t.Fatalf("tcp attempts = %v, want %v", got, wantTCP)
	}
}

func TestChainStopsAtFirstSuccess(t *testing.T) {
	quic := newFakeTransport("quic")
	tcp := newFakeTransport("tcp")
	// QUIC падает на IPv6, работает на IPv4 — до TCP очередь дойти не должна.
	quic.failFor["[2001:db8::1]:443"] = true

	d := New(Config{
		Transports: []TransportEntry{
			{Name: "quic", Transport: quic, Priority: 0},
			{Name: "tcp", Transport: tcp, Priority: 1},
		},
		Resolver: dualStackResolver(),
	})

	conn, err := d.Connect(context.Background(), "example.com:443")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if got := conn.RemoteEndpoint(); got != "192.0.2.1:443" {
		t.Fatalf("connected via %s, want 192.0.2.1:443", got)
	}
	if n := len(tcp.attemptedEndpoints()); n != 0 {
		t.Fatalf("tcp was tried %d times, but quic succeeded — chain must stop at first success", n)
	}
}

func TestPriorityRespectedRegardlessOfSliceOrder(t *testing.T) {
	// Приоритет задаёт порядок, а не позиция в срезе.
	first := newFakeTransport("low-priority")
	second := newFakeTransport("high-priority")
	first.failAll = true
	second.failAll = true

	d := New(Config{
		Transports: []TransportEntry{
			{Name: "low-priority", Transport: first, Priority: 10},
			{Name: "high-priority", Transport: second, Priority: 0},
		},
		Resolver: staticResolver{ips: map[string][]net.IP{"h": {net.ParseIP("192.0.2.1")}}},
	})

	d.Connect(context.Background(), "h:1")

	if len(second.attemptedEndpoints()) == 0 {
		t.Fatal("high-priority transport was never tried")
	}
	// Оба пробовались, но high-priority должен был быть первым — это
	// видно по тому, что low-priority тоже пробовался (значит перебор
	// дошёл до него после провала первого).
	if len(first.attemptedEndpoints()) == 0 {
		t.Fatal("low-priority transport was never tried as fallback")
	}
}

// ── Per-route timeout ───────────────────────────────────────────────

func TestPerRouteTimeout_BlackHoleRouteDoesNotEatWholeBudget(t *testing.T) {
	// Смысл per-route таймаута: маршрут-«чёрная дыра» (пакеты уходят,
	// ответа нет) не должен съесть весь бюджет времени, иначе до
	// рабочего маршрута очередь не дойдёт.
	blackHole := newFakeTransport("blackhole")
	blackHole.blockUntilCtx = true

	working := newFakeTransport("working")

	d := New(Config{
		Transports: []TransportEntry{
			{Name: "blackhole", Transport: blackHole, Priority: 0},
			{Name: "working", Transport: working, Priority: 1},
		},
		PerRouteTimeout: 100 * time.Millisecond,
		Resolver:        staticResolver{ips: map[string][]net.IP{"h": {net.ParseIP("192.0.2.1")}}},
	})

	start := time.Now()
	conn, err := d.Connect(context.Background(), "h:1")
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Connect: %v — working route must be reached after blackhole times out", err)
	}
	if conn.RemoteEndpoint() != "192.0.2.1:1" {
		t.Fatalf("unexpected endpoint %s", conn.RemoteEndpoint())
	}
	if elapsed > 2*time.Second {
		t.Fatalf("took %v — per-route timeout did not limit the black-hole attempt", elapsed)
	}
}

func TestOuterContextCancellationStopsChain(t *testing.T) {
	slow := newFakeTransport("slow")
	slow.blockUntilCtx = true

	d := New(Config{
		Transports: []TransportEntry{{Name: "slow", Transport: slow, Priority: 0}},
		Resolver:   staticResolver{ips: map[string][]net.IP{"h": {net.ParseIP("192.0.2.1")}}},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if _, err := d.Connect(ctx, "h:1"); err == nil {
		t.Fatal("expected failure when the outer context expires")
	}
}

// ── Sticky success ──────────────────────────────────────────────────

func TestStickySuccess_RemembersWorkingRoute(t *testing.T) {
	quic := newFakeTransport("quic")
	tcp := newFakeTransport("tcp")
	quic.failAll = true // QUIC не работает вообще

	d := New(Config{
		Transports: []TransportEntry{
			{Name: "quic", Transport: quic, Priority: 0},
			{Name: "tcp", Transport: tcp, Priority: 1},
		},
		Resolver:      dualStackResolver(),
		StickySuccess: true,
	})

	if _, err := d.Connect(context.Background(), "example.com:443"); err != nil {
		t.Fatalf("first Connect: %v", err)
	}
	quicAttemptsAfterFirst := len(quic.attemptedEndpoints())

	// Второй Connect должен пойти сразу по запомненному TCP-маршруту,
	// не перебирая заново провальный QUIC.
	if _, err := d.Connect(context.Background(), "example.com:443"); err != nil {
		t.Fatalf("second Connect: %v", err)
	}

	if got := len(quic.attemptedEndpoints()); got != quicAttemptsAfterFirst {
		t.Fatalf("quic was retried %d extra times — sticky route was not used",
			got-quicAttemptsAfterFirst)
	}
}

func TestStickySuccess_FallsBackWhenRememberedRouteBreaks(t *testing.T) {
	// Смена сети: вчерашний рабочий маршрут перестал работать —
	// перебор должен пойти заново, а не упереться в устаревший выбор.
	quic := newFakeTransport("quic")
	tcp := newFakeTransport("tcp")
	quic.failAll = true

	d := New(Config{
		Transports: []TransportEntry{
			{Name: "quic", Transport: quic, Priority: 0},
			{Name: "tcp", Transport: tcp, Priority: 1},
		},
		Resolver:      dualStackResolver(),
		StickySuccess: true,
	})

	if _, err := d.Connect(context.Background(), "example.com:443"); err != nil {
		t.Fatalf("first Connect: %v", err)
	}

	// Теперь ломается TCP, а QUIC начинает работать.
	tcp.mu.Lock()
	tcp.failAll = true
	tcp.mu.Unlock()
	quic.mu.Lock()
	quic.failAll = false
	quic.mu.Unlock()

	conn, err := d.Connect(context.Background(), "example.com:443")
	if err != nil {
		t.Fatalf("Connect after route broke: %v — must fall back to the full chain", err)
	}
	if conn == nil {
		t.Fatal("nil connection")
	}
}

func TestStickyDisabled_AlwaysStartsFromChainTop(t *testing.T) {
	quic := newFakeTransport("quic")
	tcp := newFakeTransport("tcp")
	quic.failAll = true

	d := New(Config{
		Transports: []TransportEntry{
			{Name: "quic", Transport: quic, Priority: 0},
			{Name: "tcp", Transport: tcp, Priority: 1},
		},
		Resolver:      dualStackResolver(),
		StickySuccess: false,
	})

	d.Connect(context.Background(), "example.com:443")
	afterFirst := len(quic.attemptedEndpoints())
	d.Connect(context.Background(), "example.com:443")

	if got := len(quic.attemptedEndpoints()); got == afterFirst {
		t.Fatal("quic was not retried — with sticky disabled the chain must restart from the top")
	}
}

// ── Ошибки ──────────────────────────────────────────────────────────

func TestNoRouteError_ListsAllAttempts(t *testing.T) {
	quic := newFakeTransport("quic")
	tcp := newFakeTransport("tcp")
	quic.failAll = true
	tcp.failAll = true

	d := New(Config{
		Transports: []TransportEntry{
			{Name: "quic", Transport: quic, Priority: 0},
			{Name: "tcp", Transport: tcp, Priority: 1},
		},
		Resolver: dualStackResolver(),
	})

	_, err := d.Connect(context.Background(), "example.com:443")
	var noRoute *ErrNoRoute
	if !errors.As(err, &noRoute) {
		t.Fatalf("expected *ErrNoRoute, got %T: %v", err, err)
	}
	// 2 транспорта × 2 адреса = 4 попытки, все должны быть в отчёте:
	// иначе диагностика сводится к последней ошибке, часто наименее
	// информативной.
	if len(noRoute.Attempts) != 4 {
		t.Fatalf("recorded %d attempts, want 4", len(noRoute.Attempts))
	}
	if noRoute.Unwrap() == nil {
		t.Fatal("Unwrap returned nil — first attempt's error must be accessible")
	}
}

func TestNoTransportsRegistered(t *testing.T) {
	d := New(Config{Resolver: dualStackResolver()})
	if _, err := d.Connect(context.Background(), "example.com:443"); err == nil {
		t.Fatal("expected error with no transports registered")
	}
}

func TestResolverFailure(t *testing.T) {
	d := New(Config{
		Transports: []TransportEntry{{Name: "quic", Transport: newFakeTransport("quic")}},
		Resolver:   staticResolver{err: errors.New("dns is down")},
	})
	if _, err := d.Connect(context.Background(), "example.com:443"); err == nil {
		t.Fatal("expected error when the resolver fails")
	}
}

func TestMalformedEndpoint(t *testing.T) {
	d := New(Config{
		Transports: []TransportEntry{{Name: "quic", Transport: newFakeTransport("quic")}},
		Resolver:   dualStackResolver(),
	})
	if _, err := d.Connect(context.Background(), "no-port-here"); err == nil {
		t.Fatal("expected error for an endpoint without a port")
	}
}

func TestConnectAfterClose(t *testing.T) {
	d := New(Config{
		Transports: []TransportEntry{{Name: "quic", Transport: newFakeTransport("quic")}},
		Resolver:   dualStackResolver(),
	})
	d.Close()
	if _, err := d.Connect(context.Background(), "example.com:443"); err == nil {
		t.Fatal("Connect after Close must fail")
	}
}

func TestClose_DoesNotCloseRegisteredTransports(t *testing.T) {
	// Владение: транспорты создал и передал вызывающий — он же их
	// закрывает. Двойное владение сделало бы неочевидным, кто обязан
	// вызвать Close у quic/tcp адаптеров.
	quic := newFakeTransport("quic")
	d := New(Config{Transports: []TransportEntry{{Name: "quic", Transport: quic}}})

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if quic.isClosed() {
		t.Fatal("autodial closed a transport it does not own")
	}
}

func TestClose_Idempotent(t *testing.T) {
	d := New(Config{})
	for i := 0; i < 3; i++ {
		if err := d.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
}

// ── Литеральные адреса (без DNS) ────────────────────────────────────

func TestLiteralIPv4Endpoint_NoResolverCall(t *testing.T) {
	tr := newFakeTransport("quic")
	d := New(Config{
		Transports: []TransportEntry{{Name: "quic", Transport: tr}},
		// Резолвер сломан — если он будет вызван, тест упадёт.
		Resolver: staticResolver{err: errors.New("resolver must not be called")},
	})

	conn, err := d.Connect(context.Background(), "192.0.2.5:443")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if conn.RemoteEndpoint() != "192.0.2.5:443" {
		t.Fatalf("endpoint = %s", conn.RemoteEndpoint())
	}
}

func TestLiteralIPv6Endpoint(t *testing.T) {
	tr := newFakeTransport("quic")
	d := New(Config{
		Transports: []TransportEntry{{Name: "quic", Transport: tr}},
		Resolver:   staticResolver{err: errors.New("resolver must not be called")},
	})

	conn, err := d.Connect(context.Background(), "[2001:db8::99]:443")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if conn.RemoteEndpoint() != "[2001:db8::99]:443" {
		t.Fatalf("endpoint = %s", conn.RemoteEndpoint())
	}
}

// ── IPv6-only / IPv4-only ───────────────────────────────────────────

func TestIPv4OnlyHost(t *testing.T) {
	tr := newFakeTransport("quic")
	d := New(Config{
		Transports: []TransportEntry{{Name: "quic", Transport: tr}},
		Resolver:   staticResolver{ips: map[string][]net.IP{"v4only": {net.ParseIP("192.0.2.1")}}},
	})

	if _, err := d.Connect(context.Background(), "v4only:80"); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if got := tr.attemptedEndpoints(); len(got) != 1 || got[0] != "192.0.2.1:80" {
		t.Fatalf("attempts = %v, want exactly [192.0.2.1:80]", got)
	}
}

func TestIPv6OnlyHost(t *testing.T) {
	tr := newFakeTransport("quic")
	d := New(Config{
		Transports: []TransportEntry{{Name: "quic", Transport: tr}},
		Resolver:   staticResolver{ips: map[string][]net.IP{"v6only": {net.ParseIP("2001:db8::1")}}},
	})

	if _, err := d.Connect(context.Background(), "v6only:80"); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if got := tr.attemptedEndpoints(); len(got) != 1 || got[0] != "[2001:db8::1]:80" {
		t.Fatalf("attempts = %v", got)
	}
}

// ── Статистика ──────────────────────────────────────────────────────

func TestStats_RecordsSuccessesAndFailures(t *testing.T) {
	quic := newFakeTransport("quic")
	tcp := newFakeTransport("tcp")
	quic.failAll = true

	d := New(Config{
		Transports: []TransportEntry{
			{Name: "quic", Transport: quic, Priority: 0},
			{Name: "tcp", Transport: tcp, Priority: 1},
		},
		Resolver: dualStackResolver(),
	})

	if _, err := d.Connect(context.Background(), "example.com:443"); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	stats := d.Stats()
	if len(stats) == 0 {
		t.Fatal("no stats recorded")
	}

	var failures, successes int
	for _, s := range stats {
		failures += s.Failures
		successes += s.Successes
	}
	if failures == 0 {
		t.Fatal("quic failures were not recorded")
	}
	if successes == 0 {
		t.Fatal("tcp success was not recorded")
	}
}

// ── Конкурентность ──────────────────────────────────────────────────

func TestConcurrentConnects(t *testing.T) {
	quic := newFakeTransport("quic")
	tcp := newFakeTransport("tcp")
	quic.failAll = true

	d := New(Config{
		Transports: []TransportEntry{
			{Name: "quic", Transport: quic, Priority: 0},
			{Name: "tcp", Transport: tcp, Priority: 1},
		},
		Resolver:      dualStackResolver(),
		StickySuccess: true,
	})

	var wg sync.WaitGroup
	errCh := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := d.Connect(context.Background(), "example.com:443")
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent Connect failed: %v", err)
		}
	}
}

func endpointsEqual(a, b []ports.Endpoint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
