package autodial

import (
	"context"
	"fmt"
	"net"
	"sort"

	"github.com/wiloris/photon-core/ports"
)

// Family — адресное семейство маршрута.
type Family int

const (
	FamilyIPv6 Family = iota
	FamilyIPv4
)

func (f Family) String() string {
	if f == FamilyIPv6 {
		return "IPv6"
	}
	return "IPv4"
}

// TransportEntry — зарегистрированный транспорт с его приоритетом.
//
// Приоритет задаёт вызывающий, а не autodial: цепочка из ARCHITECTURE.md
// (QUIC перед TCP, Reality последним) — это конфигурация развёртывания,
// а не жёстко зашитая в код константа. Reality добавится сюда же, когда
// появится adapters/reality — без правки autodial.
type TransportEntry struct {
	// Name — для логов и диагностики ("quic", "tcp", "reality").
	Name string

	// Transport — сам адаптер.
	Transport ports.Transport

	// Priority — меньше = раньше пробуется.
	Priority int
}

// Route — один конкретный вариант подключения: транспорт + адрес.
// Логический endpoint ("example.com:443") разворачивается в несколько
// маршрутов: по одному на каждую пару (транспорт, разрешённый адрес).
type Route struct {
	TransportName string
	Transport     ports.Transport
	Family        Family
	Endpoint      ports.Endpoint
}

func (r Route) String() string {
	return fmt.Sprintf("%s/%s→%s", r.TransportName, r.Family, r.Endpoint)
}

// Resolver разрешает хост в набор IP-адресов. Интерфейс, а не прямой
// вызов net.Resolver, чтобы тесты не зависели от реального DNS.
type Resolver interface {
	LookupIP(ctx context.Context, host string) ([]net.IP, error)
}

type systemResolver struct{}

func (systemResolver) LookupIP(ctx context.Context, host string) ([]net.IP, error) {
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, a := range addrs {
		ips = append(ips, a.IP)
	}
	return ips, nil
}

// buildRoutes разворачивает логический endpoint в упорядоченный список
// маршрутов.
//
// Порядок соответствует цепочке из ARCHITECTURE.md:
//
//	IPv6 QUIC → IPv4 QUIC → IPv6 TCP → IPv4 TCP → Reality
//
// то есть сортировка сначала по приоритету транспорта, затем IPv6 перед
// IPv4. IPv6 первым — не эстетика: в сетях с рабочим IPv6 он обычно даёт
// прямую связность без NAT, а при неработающем IPv6 следующий маршрут
// пробуется сразу (см. RFC 8305, тот же мотив).
func buildRoutes(ctx context.Context, res Resolver, transports []TransportEntry, endpoint ports.Endpoint) ([]Route, error) {
	host, port, err := net.SplitHostPort(string(endpoint))
	if err != nil {
		return nil, fmt.Errorf("autodial: malformed endpoint %q: %w", endpoint, err)
	}

	var v6, v4 []net.IP
	if ip := net.ParseIP(host); ip != nil {
		// Уже литеральный адрес — резолвить нечего.
		if ip.To4() != nil {
			v4 = append(v4, ip)
		} else {
			v6 = append(v6, ip)
		}
	} else {
		ips, err := res.LookupIP(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("autodial: resolve %q: %w", host, err)
		}
		for _, ip := range ips {
			if ip.To4() != nil {
				v4 = append(v4, ip)
			} else {
				v6 = append(v6, ip)
			}
		}
	}

	if len(v6) == 0 && len(v4) == 0 {
		return nil, fmt.Errorf("autodial: no addresses for %q", host)
	}

	sorted := make([]TransportEntry, len(transports))
	copy(sorted, transports)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Priority < sorted[j].Priority
	})

	var routes []Route
	for _, te := range sorted {
		for _, group := range []struct {
			fam Family
			ips []net.IP
		}{{FamilyIPv6, v6}, {FamilyIPv4, v4}} {
			for _, ip := range group.ips {
				routes = append(routes, Route{
					TransportName: te.Name,
					Transport:     te.Transport,
					Family:        group.fam,
					Endpoint:      ports.Endpoint(net.JoinHostPort(ip.String(), port)),
				})
			}
		}
	}
	return routes, nil
}
