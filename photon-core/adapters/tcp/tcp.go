// Package tcp реализует ports.Transport поверх TCP.
//
// TCP, в отличие от QUIC, не имеет мультиплексирования: это один
// непрерывный байтовый поток. Поэтому адаптер строит собственный
// мультиплексор поверх сокета (формат — см. WIRE_FORMAT.md,
// зафиксирован до написания кода).
//
// Аудит границы (ADR-011/013): ports.Transport не знает ни о TCP, ни о
// кадрах, ни о stream_id. Порт заморожен (ADR-020) — адаптер
// подстраивается под него, включая построение того, чего в TCP нет.
package tcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/wiloris/photon-core/ports"
)

// Config — параметры адаптера.
type Config struct {
	// Dialer — если nil, используется net.Dialer по умолчанию.
	Dialer *net.Dialer
}

// Transport реализует ports.Transport поверх TCP.
type Transport struct {
	dialer *net.Dialer

	mu     sync.Mutex
	closed bool
}

var _ ports.Transport = (*Transport)(nil)

func New(cfg Config) *Transport {
	d := cfg.Dialer
	if d == nil {
		d = &net.Dialer{}
	}
	return &Transport{dialer: d}
}

// Connect — см. ports.Transport.
func (t *Transport) Connect(ctx context.Context, endpoint ports.Endpoint) (ports.Connection, error) {
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return nil, errors.New("tcp: transport is closed")
	}

	conn, err := t.dialer.DialContext(ctx, "tcp", string(endpoint))
	if err != nil {
		return nil, fmt.Errorf("tcp: connect %s: %w", endpoint, err)
	}
	// Сторона, инициировавшая соединение, использует чётные stream_id
	// (см. WIRE_FORMAT.md — разделение пространства ID по чётности,
	// иначе одновременные OpenStream с обеих сторон столкнулись бы).
	return newConnection(conn, endpoint, true), nil
}

// TransportName — имя транспорта для диагностики (diag.Named): событие
// dial пишет фактический транспорт подключения. Не часть ports.Transport.
func (t *Transport) TransportName() string { return "tcp" }

// Close — см. ports.Transport. Помечает транспорт закрытым; уже
// установленные Connection не закрывает (контракт порта).
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

// Accept оборачивает уже принятое TCP-соединение в ports.Connection.
//
// Не входит в ports.Transport (порт — только dial, ADR-012). Нужен для
// серверной стороны в тестах и для будущего использования вне порта:
// мультиплексор симметричен, но пространства stream_id у сторон разные.
func Accept(conn net.Conn) ports.Connection {
	return newConnection(conn, ports.Endpoint(conn.RemoteAddr().String()), false)
}

// ── Connection: мультиплексор ───────────────────────────────────────

type connection struct {
	conn     net.Conn
	endpoint ports.Endpoint

	// writeMu сериализует запись кадров в сокет. Без него два потока
	// могли бы перемешать заголовок одного кадра с телом другого.
	writeMu sync.Mutex

	mu       sync.Mutex
	streams  map[uint32]*stream
	nextID   uint32
	closed   bool
	closeErr error

	acceptBidi chan *stream
	acceptUni  chan *stream
	done       chan struct{}
}

var _ ports.Connection = (*connection)(nil)

func newConnection(nc net.Conn, endpoint ports.Endpoint, isInitiator bool) *connection {
	// Инициатор — чётные ID, принимающая сторона — нечётные.
	var first uint32 = 1
	if isInitiator {
		first = 2
	}

	c := &connection{
		conn:       nc,
		endpoint:   endpoint,
		streams:    make(map[uint32]*stream),
		nextID:     first,
		acceptBidi: make(chan *stream, 16),
		acceptUni:  make(chan *stream, 16),
		done:       make(chan struct{}),
	}
	go c.readLoop()
	return c
}

// readLoop — единственный читатель сокета. Разбирает кадры и
// раскладывает их по логическим потокам.
func (c *connection) readLoop() {
	for {
		f, err := decodeFrame(c.conn)
		if err != nil {
			// Протокольная ошибка означает некорректные/враждебные
			// данные — соединение закрывается. Ошибка ввода-вывода
			// (EOF, разрыв) — штатное завершение.
			c.closeWithError(err)
			return
		}
		if err := c.handleFrame(f); err != nil {
			c.closeWithError(err)
			return
		}
	}
}

func (c *connection) handleFrame(f frame) error {
	switch f.typ {
	case frameOpen:
		return c.handleOpen(f)
	case frameData:
		c.mu.Lock()
		s := c.streams[f.streamID]
		c.mu.Unlock()
		if s == nil {
			// Поток мог быть закрыт локально, пока кадр был в пути —
			// это гонка, а не ошибка протокола (см. WIRE_FORMAT.md).
			return nil
		}
		s.deliver(f.payload)
		return nil
	case frameFin:
		c.mu.Lock()
		s := c.streams[f.streamID]
		c.mu.Unlock()
		if s != nil {
			s.remoteFIN()
		}
		return nil
	case frameReset:
		c.mu.Lock()
		s := c.streams[f.streamID]
		delete(c.streams, f.streamID)
		c.mu.Unlock()
		if s != nil {
			s.remoteReset()
		}
		return nil
	default:
		return fmt.Errorf("%w: %s", errBadType, f.typ)
	}
}

func (c *connection) handleOpen(f frame) error {
	// Удалённая сторона обязана использовать своё пространство ID —
	// иначе это либо баг, либо попытка перехватить чужой поток.
	localEven := c.nextID%2 == 0
	remoteEven := f.streamID%2 == 0
	if localEven == remoteEven {
		return fmt.Errorf("tcp: peer opened stream %d from our ID space", f.streamID)
	}

	c.mu.Lock()
	if _, exists := c.streams[f.streamID]; exists {
		c.mu.Unlock()
		return fmt.Errorf("tcp: duplicate OPEN for stream %d", f.streamID)
	}
	uni := f.flags&flagUni != 0
	s := newStream(c, f.streamID, uni)
	if uni {
		// Uni-поток от удалённой стороны: читаем, но не пишем.
		s.localClosed = true
	}
	c.streams[f.streamID] = s
	closed := c.closed
	c.mu.Unlock()

	if closed {
		return nil
	}

	ch := c.acceptBidi
	if uni {
		ch = c.acceptUni
	}
	select {
	case ch <- s:
	case <-c.done:
	}
	return nil
}

func (c *connection) writeFrame(f frame) error {
	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		if err == nil {
			err = net.ErrClosed
		}
		return err
	}
	c.mu.Unlock()

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return encodeFrame(c.conn, f)
}

func (c *connection) openStream(ctx context.Context, uni bool) (*stream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		if err == nil {
			err = net.ErrClosed
		}
		return nil, err
	}
	id := c.nextID
	c.nextID += 2 // сохраняем чётность своего пространства ID
	s := newStream(c, id, uni)
	if uni {
		// Наш uni-поток: пишем, но не читаем. Read при remoteClosed сразу
		// отдаёт EOF и канала не ждёт, поэтому readable НЕ закрывается:
		// connClosed/remoteReset будят читателей отправкой в него, и
		// закрытый канал давал панику при закрытии соединения с открытым
		// собственным uni-потоком (TestCloseConnWithOpenOwnUniStream).
		s.remoteClosed = true
	}
	c.streams[id] = s
	c.mu.Unlock()

	var flags uint8
	if uni {
		flags = flagUni
	}
	if err := c.writeFrame(frame{typ: frameOpen, flags: flags, streamID: id}); err != nil {
		c.mu.Lock()
		delete(c.streams, id)
		c.mu.Unlock()
		return nil, err
	}
	return s, nil
}

func (c *connection) OpenStream(ctx context.Context) (ports.Stream, error) {
	return c.openStream(ctx, false)
}

func (c *connection) OpenUniStream(ctx context.Context) (ports.SendStream, error) {
	return c.openStream(ctx, true)
}

func (c *connection) AcceptStream(ctx context.Context) (ports.Stream, error) {
	select {
	case s := <-c.acceptBidi:
		return s, nil
	case <-c.done:
		return nil, c.errAfterClose()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *connection) AcceptUniStream(ctx context.Context) (ports.RecvStream, error) {
	select {
	case s := <-c.acceptUni:
		return s, nil
	case <-c.done:
		return nil, c.errAfterClose()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *connection) RemoteEndpoint() ports.Endpoint {
	if addr := c.conn.RemoteAddr(); addr != nil {
		return ports.Endpoint(addr.String())
	}
	return c.endpoint
}

func (c *connection) Close() error {
	return c.closeWithError(nil)
}

func (c *connection) closeWithError(cause error) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.closeErr = cause
	streams := make([]*stream, 0, len(c.streams))
	for _, s := range c.streams {
		streams = append(streams, s)
	}
	c.streams = make(map[uint32]*stream)
	c.mu.Unlock()

	close(c.done)
	err := c.conn.Close()

	// Разбудить всех, кто ждёт Read на логических потоках — иначе
	// закрытие TCP оставило бы их висеть навсегда.
	for _, s := range streams {
		s.connClosed(cause)
	}
	return err
}

func (c *connection) errAfterClose() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	return net.ErrClosed
}

func (c *connection) removeStream(id uint32) {
	c.mu.Lock()
	delete(c.streams, id)
	c.mu.Unlock()
}

// ── Logical stream ──────────────────────────────────────────────────

type stream struct {
	conn *connection
	id   uint32
	uni  bool

	mu           sync.Mutex
	buf          []byte
	localClosed  bool  // мы отправили FIN
	remoteClosed bool  // получили FIN
	resetErr     error // RESET или закрытие соединения
	readable     chan struct{}
}

var (
	_ ports.Stream     = (*stream)(nil)
	_ ports.SendStream = (*stream)(nil)
	_ ports.RecvStream = (*stream)(nil)
)

func newStream(c *connection, id uint32, uni bool) *stream {
	return &stream{
		conn:     c,
		id:       id,
		uni:      uni,
		readable: make(chan struct{}, 1),
	}
}

func (s *stream) deliver(data []byte) {
	s.mu.Lock()
	s.buf = append(s.buf, data...)
	s.mu.Unlock()
	s.signal()
}

func (s *stream) remoteFIN() {
	s.mu.Lock()
	s.remoteClosed = true
	s.mu.Unlock()
	s.signal()
}

func (s *stream) remoteReset() {
	s.mu.Lock()
	s.resetErr = errors.New("tcp: stream reset by peer")
	s.mu.Unlock()
	s.signal()
}

func (s *stream) connClosed(cause error) {
	s.mu.Lock()
	if s.resetErr == nil {
		if cause != nil {
			s.resetErr = cause
		} else {
			s.resetErr = net.ErrClosed
		}
	}
	s.mu.Unlock()
	s.signal()
}

// signal будит читателя, не блокируясь, если сигнал уже в канале.
func (s *stream) signal() {
	select {
	case s.readable <- struct{}{}:
	default:
	}
}

func (s *stream) Read(p []byte) (int, error) {
	for {
		s.mu.Lock()
		if len(s.buf) > 0 {
			n := copy(p, s.buf)
			s.buf = s.buf[n:]
			s.mu.Unlock()
			return n, nil
		}
		if s.resetErr != nil {
			err := s.resetErr
			s.mu.Unlock()
			return 0, err
		}
		if s.remoteClosed {
			// Буфер пуст и удалённая сторона закончила — EOF, ровно как
			// требует io.Reader.
			s.mu.Unlock()
			return 0, io.EOF
		}
		s.mu.Unlock()

		<-s.readable
	}
}

func (s *stream) Write(p []byte) (int, error) {
	s.mu.Lock()
	if s.resetErr != nil {
		err := s.resetErr
		s.mu.Unlock()
		return 0, err
	}
	if s.localClosed {
		s.mu.Unlock()
		return 0, errors.New("tcp: write on a closed stream")
	}
	s.mu.Unlock()

	// Крупная запись режется на кадры — иначе один писатель занимал бы
	// сокет надолго, блокируя все остальные потоки соединения
	// (см. WIRE_FORMAT.md, раздел Backpressure).
	written := 0
	for written < len(p) {
		end := written + maxFrameSize
		if end > len(p) {
			end = len(p)
		}
		err := s.conn.writeFrame(frame{
			typ:      frameData,
			streamID: s.id,
			payload:  p[written:end],
		})
		if err != nil {
			return written, err
		}
		written = end
	}
	return written, nil
}

// Close отправляет FIN: завершает передачу, приём остаётся открытым —
// half-close, как обещает ports.SendStream.Close.
func (s *stream) Close() error {
	s.mu.Lock()
	if s.localClosed {
		s.mu.Unlock()
		return nil // идемпотентно
	}
	s.localClosed = true
	bothDone := s.remoteClosed
	s.mu.Unlock()

	err := s.conn.writeFrame(frame{typ: frameFin, streamID: s.id})
	if bothDone {
		s.conn.removeStream(s.id)
	}
	return err
}
