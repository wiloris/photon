// Package ports — см. storage.go для общих правил пакета.
package ports

import (
	"context"
	"io"
)

// Endpoint — адрес удалённой стороны. Именованный тип: формат
// (host:port, IPv6 в скобках, unix-путь, URL) не специфицируется
// портом и зависит от адаптера.
type Endpoint string

// Transport — порт сетевого транспорта ядра.
// Реализации: adapters/quic, adapters/tcp (см. ROADMAP.md, Phase 6),
// в будущем — адаптер транспорта с маскировкой, последняя ступень цепочки
// (ARCHITECTURE.md, раздел 5 — Autodial). Роль, а не конкретный продукт:
// какой механизм её займёт, порт не знает и знать не должен.
//
// Только исходящие соединения (dial); Listen — отдельный ADR при
// появлении реальной потребности, не тихое расширение (см. ADR-012).
// Выбор протокола/адреса и его смена — не забота этого порта (ADR-012):
// такой выбор делает вызывающий, подставляя нужную реализацию. Готовая
// реализация выбора — services/autodial (Phase 7): он сам реализует этот
// порт и перебирает цепочку маршрутов. В сегодняшнем пути ядра он не
// участвует — services/dispatcher строит adapters/quic напрямую, — и
// переподключением не владеет: восстановление связи принадлежит
// потребителю соединения. См. ADR-011/013/016.
type Transport interface {
	// Connect устанавливает соединение с уже разрешённым endpoint.
	Connect(ctx context.Context, endpoint Endpoint) (Connection, error)

	// Close освобождает ресурсы, общие для всех соединений (не
	// закрывает уже установленные Connection — см. Connection.Close).
	Close() error
}

// Connection — установленное соединение, способное открывать потоки,
// включая однонаправленные (см. ADR-016 — обязательны для всех
// адаптеров, не только для тех, что поддерживают их нативно).
type Connection interface {
	// OpenStream открывает двунаправленный поток по инициативе
	// локальной стороны.
	OpenStream(ctx context.Context) (Stream, error)

	// AcceptStream принимает входящий поток на уже установленном
	// соединении (не входящее соединение — граница dial-only не
	// нарушается, см. Transport).
	AcceptStream(ctx context.Context) (Stream, error)

	// OpenUniStream открывает однонаправленный поток для отправки.
	OpenUniStream(ctx context.Context) (SendStream, error)

	// AcceptUniStream принимает входящий однонаправленный поток —
	// основной сценарий: push-события сервера (см. Subscribe в
	// photon-quic-bridge).
	AcceptUniStream(ctx context.Context) (RecvStream, error)

	// RemoteEndpoint — адрес удалённой стороны, для логов/диагностики.
	RemoteEndpoint() Endpoint

	// Close закрывает соединение и все его потоки.
	Close() error
}

// Stream — двунаправленный поток байт внутри Connection.
type Stream interface {
	SendStream
	RecvStream
}

// SendStream — пишущая часть потока.
type SendStream interface {
	io.Writer

	// Close завершает отправку (half-close), не закрывая приём.
	Close() error
}

// RecvStream — читающая часть потока.
type RecvStream interface {
	io.Reader
}
