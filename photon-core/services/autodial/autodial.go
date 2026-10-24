// Package autodial выбирает рабочий маршрут подключения, перебирая
// зарегистрированные транспорты и адреса по приоритету.
//
// Не порт, а сервис (ARCHITECTURE.md, раздел 5: "отдельный сервис,
// который оркестрирует адаптеры транспорта"). Зависит от
// ports.Transport, не расширяет его — тот же принцип, что у
// services/account (ADR-017/024): бизнес-логика поверх портов, а не
// новая внешняя зависимость.
//
// Сам реализует ports.Transport, поэтому подставляется всюду, где
// ожидается транспорт: вызывающий получает автоматический выбор
// маршрута, не зная об этом.
package autodial

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
)

// Config — параметры движка.
type Config struct {
	// Transports — зарегистрированные транспорты с приоритетами.
	// Порядок цепочки (QUIC перед TCP, маскирующий транспорт последним)
	// задаётся здесь, а не в коде autodial: это конфигурация развёртывания.
	// Последняя ступень названа по роли: конкретного адаптера нет, и
	// цепочка от его появления не зависит.
	Transports []TransportEntry

	// PerRouteTimeout — предел на одну попытку. 0 = без отдельного
	// предела (тогда действует только общий ctx вызывающего).
	//
	// Нужен именно per-route: без него первый же "чёрная дыра"-маршрут
	// (пакеты уходят, ответа нет) съел бы весь бюджет времени, и до
	// рабочего маршрута очередь не дошла бы.
	PerRouteTimeout time.Duration

	// Resolver — если nil, используется системный DNS.
	Resolver Resolver

	// StickySuccess — запоминать удачный маршрут и пробовать его первым
	// при следующем Connect к тому же endpoint. По умолчанию включено.
	//
	// Инвалидация: при неудаче запомненный маршрут сбрасывается, и
	// перебор идёт с начала цепочки. Это покрывает смену сети
	// (Wi-Fi → LTE), когда вчерашний рабочий маршрут больше не работает.
	StickySuccess bool
}

// Dialer реализует ports.Transport, выбирая маршрут автоматически.
type Dialer struct {
	transports []TransportEntry
	timeout    time.Duration
	resolver   Resolver
	sticky     bool

	mu     sync.Mutex
	closed bool
	// best — последний удачный маршрут на endpoint.
	best map[ports.Endpoint]Route
	// stats — накопленная статистика по маршрутам, для диагностики.
	stats map[string]*RouteStats
}

var _ ports.Transport = (*Dialer)(nil)

// RouteStats — наблюдаемое качество маршрута.
//
// Осознанно минимально: только то, что известно из самих попыток
// подключения. RTT-пробы, измерение пропускной способности и прочее не
// добавляются без подтверждённой потребности (ADR-013) — время
// установления соединения уже отвечает на вопрос "какой маршрут
// быстрее", ради которого оценка и нужна.
type RouteStats struct {
	Route     string
	Attempts  int
	Successes int
	Failures  int
	// LastDuration — длительность последней попытки (успешной или нет).
	LastDuration time.Duration
	// BestDuration — минимальное время успешного подключения.
	BestDuration time.Duration
	LastError    string
}

func New(cfg Config) *Dialer {
	res := cfg.Resolver
	if res == nil {
		res = systemResolver{}
	}
	return &Dialer{
		transports: cfg.Transports,
		timeout:    cfg.PerRouteTimeout,
		resolver:   res,
		sticky:     cfg.StickySuccess,
		best:       make(map[ports.Endpoint]Route),
		stats:      make(map[string]*RouteStats),
	}
}

// ErrNoRoute означает, что ни один маршрут не сработал. Оборачивает
// ошибки всех попыток — иначе диагностика "почему не подключились"
// сводилась бы к последней ошибке, которая часто наименее интересна.
type ErrNoRoute struct {
	Endpoint ports.Endpoint
	Attempts []RouteAttempt
}

// RouteAttempt — результат одной попытки.
type RouteAttempt struct {
	Route    Route
	Err      error
	Duration time.Duration
}

func (e *ErrNoRoute) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "autodial: no working route to %s (%d attempts)", e.Endpoint, len(e.Attempts))
	for _, a := range e.Attempts {
		fmt.Fprintf(&b, "\n  %s: %v (%v)", a.Route, a.Err, a.Duration.Round(time.Millisecond))
	}
	return b.String()
}

// Unwrap возвращает ошибку первой попытки — чаще всего именно она
// содержательна (последующие маршруты падают по той же причине).
func (e *ErrNoRoute) Unwrap() error {
	if len(e.Attempts) == 0 {
		return nil
	}
	return e.Attempts[0].Err
}

// Connect — см. ports.Transport. Перебирает маршруты по приоритету,
// возвращает соединение по первому успешному.
func (d *Dialer) Connect(ctx context.Context, endpoint ports.Endpoint) (ports.Connection, error) {
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return nil, errors.New("autodial: dialer is closed")
	}

	if len(d.transports) == 0 {
		return nil, errors.New("autodial: no transports registered")
	}

	routes, err := buildRoutes(ctx, d.resolver, d.transports, endpoint)
	if err != nil {
		return nil, err
	}

	// Запомненный удачный маршрут — вперёд очереди, остальные сохраняют
	// исходный порядок как fallback.
	if d.sticky {
		routes = d.promoteBest(endpoint, routes)
	}

	attempts := make([]RouteAttempt, 0, len(routes))
	for _, r := range routes {
		if err := ctx.Err(); err != nil {
			// Общий бюджет вызывающего исчерпан — дальше не пробуем.
			attempts = append(attempts, RouteAttempt{Route: r, Err: err})
			break
		}

		conn, dur, err := d.tryRoute(ctx, r)
		d.record(r, dur, err)

		if err == nil {
			if d.sticky {
				d.mu.Lock()
				d.best[endpoint] = r
				d.mu.Unlock()
			}
			return conn, nil
		}
		attempts = append(attempts, RouteAttempt{Route: r, Err: err, Duration: dur})
	}

	if d.sticky {
		// Ни один маршрут не сработал — сбрасываем запомненный, чтобы
		// следующая попытка шла с начала цепочки, а не упиралась в
		// устаревший выбор (смена сети).
		d.mu.Lock()
		delete(d.best, endpoint)
		d.mu.Unlock()
	}

	return nil, &ErrNoRoute{Endpoint: endpoint, Attempts: attempts}
}

func (d *Dialer) tryRoute(ctx context.Context, r Route) (ports.Connection, time.Duration, error) {
	attemptCtx := ctx
	var cancel context.CancelFunc
	if d.timeout > 0 {
		attemptCtx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}

	start := time.Now()
	conn, err := r.Transport.Connect(attemptCtx, r.Endpoint)
	return conn, time.Since(start), err
}

func (d *Dialer) promoteBest(endpoint ports.Endpoint, routes []Route) []Route {
	d.mu.Lock()
	best, ok := d.best[endpoint]
	d.mu.Unlock()
	if !ok {
		return routes
	}

	out := make([]Route, 0, len(routes))
	var rest []Route
	for _, r := range routes {
		if r.TransportName == best.TransportName && r.Endpoint == best.Endpoint {
			out = append(out, r)
		} else {
			rest = append(rest, r)
		}
	}
	if len(out) == 0 {
		// Запомненного маршрута больше нет среди актуальных (сменился
		// DNS) — идём обычным порядком.
		return routes
	}
	return append(out, rest...)
}

func (d *Dialer) record(r Route, dur time.Duration, err error) {
	key := r.String()

	d.mu.Lock()
	defer d.mu.Unlock()

	st := d.stats[key]
	if st == nil {
		st = &RouteStats{Route: key}
		d.stats[key] = st
	}
	st.Attempts++
	st.LastDuration = dur
	if err != nil {
		st.Failures++
		st.LastError = err.Error()
		return
	}
	st.Successes++
	st.LastError = ""
	if st.BestDuration == 0 || dur < st.BestDuration {
		st.BestDuration = dur
	}
}

// Stats возвращает копию накопленной статистики — для Developer Tools
// (Phase 16, Network Lab) и диагностики.
func (d *Dialer) Stats() []RouteStats {
	d.mu.Lock()
	defer d.mu.Unlock()

	out := make([]RouteStats, 0, len(d.stats))
	for _, st := range d.stats {
		out = append(out, *st)
	}
	return out
}

// Close — см. ports.Transport.
//
// Не закрывает вложенные транспорты: их создал и передал вызывающий,
// он же отвечает за их жизненный цикл. Двойное владение (autodial
// закрывает то, что ему передали) сделало бы неочевидным, кто именно
// обязан вызвать Close у quic/tcp адаптеров.
func (d *Dialer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	return nil
}
