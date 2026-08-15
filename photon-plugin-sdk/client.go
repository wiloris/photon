package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// IPC-клиент: превращает вызов API ровно в один запрос протокола.
//
// Чего клиент НЕ делает (sdk-boundary.md, раздел 2; ADR-031):
//   - не проверяет права: это делает Authorizer в runtime;
//   - не кэширует ответы: кэш в недоверенном процессе пережил бы отзыв
//     права;
//   - не повторяет запросы: retry обошёл бы дедлайны runtime;
//   - не объединяет операции: runtime авторизует каждую отдельно;
//   - не подставляет namespace: префикс plugin.<id>.* добавляет runtime;
//   - не изобретает коды ошибок: транслирует полученные.
//
// Соответствие строгое: один вызов API -> один REQUEST -> один
// RESPONSE или ошибка.

// Config — параметры подключения.
type Config struct {
	// Conn — канал к runtime. Если nil, используются stdin/stdout:
	// именно так runtime запускает плагин (host/spawner.go).
	Conn io.ReadWriteCloser

	// PluginID — идентификатор из манифеста. Runtime СВЕРЯЕТ его с
	// тем, что знает сам; несовпадение закрывает сессию. То есть это
	// не заявление о себе, а подтверждение уже известного.
	PluginID string

	// ProtocolVersion — версия протокола из манифеста.
	ProtocolVersion int

	// Capabilities — что плагин умеет предоставлять. Не может выходить
	// за декларированное в манифесте: runtime проверит.
	Capabilities []string
}

type client struct {
	conn io.ReadWriteCloser

	writeMu sync.Mutex

	nextID atomic.Uint64

	mu       sync.Mutex
	pending  map[uint64]chan *message
	closed   bool
	closeErr error

	eventHandler EventHandler
	stopHandler  func(ctx context.Context)

	done      chan struct{}
	closeOnce sync.Once

	cfg Config
}

var _ Plugin = (*client)(nil)

// Connect устанавливает сессию с runtime и выполняет handshake.
func Connect(ctx context.Context, cfg Config) (Plugin, error) {
	conn := cfg.Conn
	if conn == nil {
		conn = stdioConn{}
	}

	c := &client{
		conn:    conn,
		pending: make(map[uint64]chan *message),
		done:    make(chan struct{}),
		cfg:     cfg,
	}

	if err := c.handshake(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	return c, nil
}

// handshake — сторона плагина (ipc.md, раздел 1).
//
// Runtime присылает HELLO первым; плагин отвечает HELLO_ACK. Порядок
// обратный обычному «клиент инициирует»: сессию открывает тот, кто
// запустил процесс.
func (c *client) handshake(ctx context.Context) error {
	hello, err := decodeMessage(c.conn)
	if err != nil {
		return fmt.Errorf("sdk: read HELLO: %w", err)
	}
	if hello.Type != msgHello {
		return fmt.Errorf("sdk: expected HELLO, got %s", hello.Type)
	}
	if hello.ProtocolVersion != c.cfg.ProtocolVersion {
		return fmt.Errorf("sdk: protocol version mismatch: runtime %d, plugin %d",
			hello.ProtocolVersion, c.cfg.ProtocolVersion)
	}

	return c.write(&message{
		Type:            msgHelloAck,
		ProtocolVersion: c.cfg.ProtocolVersion,
		PluginID:        c.cfg.PluginID,
		Capabilities:    c.cfg.Capabilities,
	})
}

// call — единственный путь от API к протоколу.
//
// Все методы Storage/Identity/Crypto проходят здесь: это гарантирует,
// что ни один из них не может обзавестись собственной логикой поверх
// протокола.
func (c *client) call(ctx context.Context, op string, req any, resp any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		if err == nil {
			err = ErrSessionClosed
		}
		return err
	}
	c.mu.Unlock()

	var payload []byte
	if req != nil {
		var err error
		payload, err = json.Marshal(req)
		if err != nil {
			return fmt.Errorf("%w: marshal request: %v", ErrInvalidRequest, err)
		}
	}

	id := c.nextID.Add(1)
	ch := make(chan *message, 1)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrSessionClosed
	}
	c.pending[id] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	if err := c.write(&message{Type: msgRequest, RequestID: id, Op: op, Payload: payload}); err != nil {
		return err
	}

	select {
	case m := <-ch:
		// Ошибка транслируется как есть — SDK не переклассифицирует
		// решения runtime.
		if err := errorForCode(m.ErrorCode, m.ErrorMessage); err != nil {
			return err
		}
		if resp != nil && len(m.Payload) > 0 {
			if err := json.Unmarshal(m.Payload, resp); err != nil {
				return fmt.Errorf("%w: unmarshal response: %v", ErrInternal, err)
			}
		}
		return nil

	case <-ctx.Done():
		// Отменяем на стороне runtime, иначе операция продолжит
		// занимать его ресурсы (ipc.md, раздел 6).
		_ = c.write(&message{Type: msgCancel, RequestID: id})
		return ctx.Err()

	case <-c.done:
		return c.sessionError()
	}
}

// Run обслуживает сессию до завершения.
func (c *client) Run(ctx context.Context) error {
	for {
		m, err := decodeMessage(c.conn)
		if err != nil {
			c.closeWith(err)
			if errors.Is(err, io.EOF) {
				return nil // runtime закрыл канал штатно
			}
			return err
		}

		switch m.Type {
		case msgResponse:
			c.deliver(m)

		case msgEvent:
			c.handleEvent(ctx, m)

		case msgStop:
			c.handleStop(ctx)
			return nil

		case msgCancel:
			// Runtime отменил наш запрос — доставляем как ошибку тому,
			// кто ждёт.
			c.deliver(&message{
				Type:      msgResponse,
				RequestID: m.RequestID,
				ErrorCode: "CANCELLED",
			})

		default:
			// HELLO_ACK, REQUEST, STOP_ACK идут в обратном направлении.
			err := fmt.Errorf("sdk: unexpected %s from runtime", m.Type)
			c.closeWith(err)
			return err
		}
	}
}

func (c *client) deliver(m *message) {
	c.mu.Lock()
	ch := c.pending[m.RequestID]
	c.mu.Unlock()
	if ch == nil {
		// Ответ на неизвестный request_id: вызывающий уже ушёл по
		// таймауту. Не ошибка — гонка.
		return
	}
	select {
	case ch <- m:
	default:
	}
}

func (c *client) handleEvent(ctx context.Context, m *message) {
	c.mu.Lock()
	h := c.eventHandler
	c.mu.Unlock()
	if h == nil {
		return
	}
	// Последовательно, в потоке Run: параллельная доставка потребовала
	// бы от автора плагина синхронизации, которую SDK не может
	// обеспечить за него.
	_ = h(ctx, Event{Type: m.EventType, Payload: m.Payload})
}

func (c *client) handleStop(ctx context.Context) {
	c.mu.Lock()
	h := c.stopHandler
	c.mu.Unlock()

	if h != nil {
		h(ctx)
	}
	_ = c.write(&message{Type: msgStopAck})
	c.closeWith(nil)
}

func (c *client) write(m *message) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return encodeMessage(c.conn, m)
}

func (c *client) closeWith(cause error) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.closeErr = cause
		pending := make([]chan *message, 0, len(c.pending))
		for _, ch := range c.pending {
			pending = append(pending, ch)
		}
		c.mu.Unlock()

		close(c.done)
		_ = c.conn.Close()

		// Разбудить всех ожидающих: иначе они висели бы до своего
		// дедлайна на уже закрытой сессии.
		for range pending {
			// Каналы буферизованы; ожидающие выйдут по c.done.
		}
	})
}

func (c *client) sessionError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	return ErrSessionClosed
}

func (c *client) Close() error {
	c.closeWith(nil)
	return nil
}

func (c *client) OnEvent(h EventHandler) {
	c.mu.Lock()
	c.eventHandler = h
	c.mu.Unlock()
}

func (c *client) OnStop(h func(ctx context.Context)) {
	c.mu.Lock()
	c.stopHandler = h
	c.mu.Unlock()
}

func (c *client) Log(ctx context.Context, level LogLevel, message string) error {
	return c.call(ctx, "log.write", logRequest{Level: string(level), Message: message}, nil)
}

func (c *client) Storage() Storage   { return &storageClient{c: c} }
func (c *client) Identity() Identity { return &identityClient{c: c} }
func (c *client) Crypto() Crypto     { return &cryptoClient{c: c} }

// stdioConn — канал через stdin/stdout.
//
// Runtime запускает плагин, передавая ему stdin/stdout как единственный
// путь наружу (sandbox, свойство P6). Отдельного сокета нет намеренно.
type stdioConn struct{}

func (stdioConn) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdioConn) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (stdioConn) Close() error                { return nil }
