package main

import (
	"context"
	"sync"

	"github.com/wiloris/photon/photon-core/ports"
)

// countingTransport — ports.Transport, считающий подключения и хранящий
// последнее из них.
//
// Зачем нужен именно здесь, а не в тестах пакета: два свойства Phase 13.5
// на фиктивном транспорте не проверяются по существу.
//
//  1. Ленивое подключение — утверждение о том, что сети НЕ было. Отсутствие
//     сети видно только по счётчику: "ответ пришёл быстро" доказательством
//     не является, а на фиктивном транспорте быстро вообще всё.
//  2. Обрыв — здесь он настоящий: закрывается живое QUIC-соединение, а не
//     подменённый объект. Ровно поэтому ссылка на Connection сохраняется:
//     иначе оборвать соединение снизу нечем — dispatcher его наружу не
//     отдаёт и отдавать не должен.
//
// Счётчик НЕ является доказательством синглфлайта: session.Connect защищает
// себя сам, и с убранным синглфлайтом Connect всё равно вызывается один раз
// (services/dispatcher/IMPLEMENTATION_LOG.md). Признак поломки синглфлайта —
// ErrConnectInProgress у части параллельных вызовов, и проверяется он по
// классам ошибок, а не по этому числу.
type countingTransport struct {
	inner ports.Transport

	mu    sync.Mutex
	dials int
	last  ports.Connection
}

func newCountingTransport(inner ports.Transport) *countingTransport {
	return &countingTransport{inner: inner}
}

func (t *countingTransport) Connect(ctx context.Context, ep ports.Endpoint) (ports.Connection, error) {
	conn, err := t.inner.Connect(ctx, ep)
	t.mu.Lock()
	t.dials++
	if err == nil {
		t.last = conn
	}
	t.mu.Unlock()
	return conn, err
}

// Close — no-op намеренно: транспорт создан снаружи, там же и закрывается.
// Dispatcher чужой транспорт не закрывает (ownTransport), но полагаться на
// это здесь нечего: обёртка не владеет тем, что обернула.
func (t *countingTransport) Close() error { return nil }

func (t *countingTransport) dialCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dials
}

// lastConn — соединение последнего успешного дозвона.
func (t *countingTransport) lastConn() ports.Connection {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.last
}
