// Package quic реализует ports.Transport поверх QUIC (quic-go).
//
// Аудит границы (ADR-011): порт ports.Transport не знает слова "QUIC" —
// вся специфика протокола (TLS-конфигурация, ALPN, keep-alive,
// datagram-режим) инкапсулирована здесь. Порт заморожен (ADR-020),
// адаптер подстраивается под него, а не наоборот.
package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/wiloris/photon/photon-core/ports"
)

// ALPN, объявляемый при handshake. Значение совпадает с тем, что уже
// использует photon-quic-bridge ("photon-quic-v1") — иначе клиент и
// сервер не договорятся о протоколе.
const defaultALPN = "photon-quic-v1"

// Config — параметры адаптера. Все поля опциональны: нулевой Config
// даёт рабочие значения по умолчанию.
type Config struct {
	// TLSConfig — если nil, используется минимальный конфиг с
	// defaultALPN. InsecureSkipVerify НЕ включается по умолчанию:
	// отключение проверки сертификата должно быть явным решением
	// вызывающего, а не тихим поведением адаптера.
	TLSConfig *tls.Config

	// HandshakeTimeout — предел на установление соединения.
	// 0 означает значение по умолчанию quic-go.
	HandshakeTimeout time.Duration

	// KeepAlivePeriod — период keep-alive пакетов. 0 отключает.
	KeepAlivePeriod time.Duration
}

// Transport реализует ports.Transport поверх QUIC.
type Transport struct {
	tlsConf  *tls.Config
	quicConf *quicgo.Config

	mu     sync.Mutex
	closed bool
}

var _ ports.Transport = (*Transport)(nil)

// New создаёт QUIC-транспорт.
func New(cfg Config) *Transport {
	tlsConf := cfg.TLSConfig
	if tlsConf == nil {
		tlsConf = &tls.Config{NextProtos: []string{defaultALPN}}
	} else if len(tlsConf.NextProtos) == 0 {
		// Клонируем, чтобы не мутировать переданный вызывающим конфиг —
		// он может использоваться и в других местах.
		tlsConf = tlsConf.Clone()
		tlsConf.NextProtos = []string{defaultALPN}
	}

	quicConf := &quicgo.Config{
		HandshakeIdleTimeout: cfg.HandshakeTimeout,
		KeepAlivePeriod:      cfg.KeepAlivePeriod,
	}

	return &Transport{tlsConf: tlsConf, quicConf: quicConf}
}

// Connect — см. ports.Transport.
func (t *Transport) Connect(ctx context.Context, endpoint ports.Endpoint) (ports.Connection, error) {
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return nil, errors.New("quic: transport is closed")
	}

	conn, err := quicgo.DialAddr(ctx, string(endpoint), t.tlsConf, t.quicConf)
	if err != nil {
		return nil, fmt.Errorf("quic: connect %s: %w", endpoint, err)
	}
	return &connection{conn: conn, endpoint: endpoint}, nil
}

// TransportName — имя транспорта для диагностики (diag.Named): событие
// dial пишет фактический транспорт подключения. Не часть ports.Transport.
func (t *Transport) TransportName() string { return "quic" }

// Close — см. ports.Transport. Освобождает только разделяемое
// состояние транспорта; уже установленные Connection не закрывает
// (по контракту порта — это делает Connection.Close).
//
// Для quic-go разделяемого состояния, требующего явного освобождения,
// нет (DialAddr создаёт сокет на соединение), поэтому Close только
// помечает транспорт закрытым, чтобы последующие Connect отказывали
// явной ошибкой вместо создания соединений на уже "закрытом" объекте.
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

// ── Connection ──────────────────────────────────────────────────────

type connection struct {
	conn     quicgo.Connection
	endpoint ports.Endpoint
}

var _ ports.Connection = (*connection)(nil)

func (c *connection) OpenStream(ctx context.Context) (ports.Stream, error) {
	s, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("quic: open stream: %w", err)
	}
	return &stream{s: s}, nil
}

func (c *connection) AcceptStream(ctx context.Context) (ports.Stream, error) {
	s, err := c.conn.AcceptStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("quic: accept stream: %w", err)
	}
	return &stream{s: s}, nil
}

func (c *connection) OpenUniStream(ctx context.Context) (ports.SendStream, error) {
	s, err := c.conn.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("quic: open uni stream: %w", err)
	}
	return &sendStream{s: s}, nil
}

func (c *connection) AcceptUniStream(ctx context.Context) (ports.RecvStream, error) {
	s, err := c.conn.AcceptUniStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("quic: accept uni stream: %w", err)
	}
	return &recvStream{s: s}, nil
}

func (c *connection) RemoteEndpoint() ports.Endpoint {
	// Возвращаем фактический адрес соединения, а не тот, что был
	// передан в Connect: после резолва DNS они могут отличаться, а для
	// логов/диагностики (единственное назначение по контракту порта)
	// полезнее фактический.
	if addr := c.conn.RemoteAddr(); addr != nil {
		return ports.Endpoint(addr.String())
	}
	return c.endpoint
}

func (c *connection) Close() error {
	// Код 0 = штатное закрытие. quic-go требует код и строку для
	// CONNECTION_CLOSE фрейма — это деталь протокола, не порта.
	return c.conn.CloseWithError(0, "")
}

// ── Streams ─────────────────────────────────────────────────────────

type stream struct {
	s quicgo.Stream
}

var _ ports.Stream = (*stream)(nil)

func (s *stream) Read(p []byte) (int, error)  { return s.s.Read(p) }
func (s *stream) Write(p []byte) (int, error) { return s.s.Write(p) }

// Close завершает отправляющую часть (half-close), не закрывая приём —
// ровно то, что обещает ports.SendStream.Close. В quic-go это именно
// Stream.Close (для полного закрытия обеих сторон существует
// CancelRead/CancelWrite, которые порт не использует).
func (s *stream) Close() error { return s.s.Close() }

type sendStream struct {
	s quicgo.SendStream
}

var _ ports.SendStream = (*sendStream)(nil)

func (s *sendStream) Write(p []byte) (int, error) { return s.s.Write(p) }
func (s *sendStream) Close() error                { return s.s.Close() }

type recvStream struct {
	s quicgo.ReceiveStream
}

var _ ports.RecvStream = (*recvStream)(nil)

func (s *recvStream) Read(p []byte) (int, error) { return s.s.Read(p) }
