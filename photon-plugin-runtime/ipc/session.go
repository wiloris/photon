package ipc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/wiloris/photon-core/ports"
)

// Session — одна IPC-сессия с процессом плагина.
//
// Здесь сходится весь security path:
//
//	decode -> protocol validation -> caller identity -> Authorizer -> handler
//
// Базовый принцип (docs/ipc.md): IPC transport сам по себе НЕ является
// authorization boundary. "IPC connected" не равно "permission granted":
// каждый semantic request проходит проверку прав заново, даже если
// соединение установлено давно и handshake пройден.

var (
	// ErrProtocolViolation — сессия закрыта из-за нарушения протокола.
	// Не перезапускается автоматически (docs/lifecycle.md, раздел 5).
	ErrProtocolViolation = errors.New("ipc: protocol violation")

	ErrSessionClosed = errors.New("ipc: session is closed")

	// ErrHandshakeFailed — handshake не завершился успешно по причине,
	// не являющейся нарушением протокола: не удалось отправить/принять
	// сообщение (плагин умер/не отвечает), либо истёк
	// HandshakeTimeout. Отличие от ErrProtocolViolation существенно
	// для supervisor (docs/lifecycle.md §5): это восстанавливается
	// перезапуском с backoff, протокольное нарушение — нет.
	//
	// Все три места, где handshake() возвращает эту ошибку, оборачивают
	// её через %w вместе с исходной причиной — host.classify() матчит
	// по errors.Is, не по подстроке (см. IMPLEMENTATION_LOG.md: строчный
	// матчинг здесь уже один раз чуть не воскресил забытый плагин).
	ErrHandshakeFailed = errors.New("ipc: handshake failed")

	// ErrClosedDuringHandshake — сессию закрыли, пока шла валидация
	// handshake. Не нарушение протокола и не сбой плагина: инициатором
	// выступил Runtime (см. setStateIfNotClosed), а не сам handshake
	// провалился и не сам плагин что-то нарушил.
	//
	// Отдельный sentinel, а не ErrSessionClosed: тот означает «операция
	// после закрытия» (сессия была полноценно установлена и уже
	// закрыта), это же — закрытие ДО того, как handshake успел
	// завершиться хоть как-то, успешно или неуспешно. Слияние отняло бы
	// у вызывающего кода (host.classify) возможность различить
	// «закрыли работающую сессию» и «закрыли на рукопожатии».
	//
	// В host.classify() эта ошибка практически всегда перекрывается
	// проверкой stopRequested (единственный код, закрывающий сессию во
	// время handshake, — host.Stop, который выставляет stopRequested
	// раньше, чем может дойти до Close) — но матчится и отдельно как
	// ErrHandshakeFailed-подобная, на случай будущего вызывающего кода,
	// который закроет сессию во время handshake, не проходя через
	// host.Stop.
	ErrClosedDuringHandshake = errors.New("ipc: session closed during handshake")
)

// Handler исполняет разрешённую операцию.
//
// Вызывается ТОЛЬКО после успешной авторизации: сессия не передаёт
// сюда запрос, не прошедший Authorizer.
type Handler interface {
	Handle(ctx context.Context, op string, payload []byte) ([]byte, error)
}

// HandlerFunc — адаптер функции к Handler.
type HandlerFunc func(ctx context.Context, op string, payload []byte) ([]byte, error)

func (f HandlerFunc) Handle(ctx context.Context, op string, payload []byte) ([]byte, error) {
	return f(ctx, op, payload)
}

// Config — параметры сессии.
type Config struct {
	// Conn — канал к процессу плагина.
	Conn io.ReadWriteCloser

	// Caller — публичный ключ плагина, установленный Runtime ПРИ
	// ЗАПУСКЕ процесса.
	//
	// Ключевое для безопасности: идентичность приходит отсюда, а не из
	// содержимого запросов. Плагин не участвует в определении своего
	// субъекта и подделать его не может (docs/ipc.md, раздел 2).
	Caller ports.PublicKey

	// ExpectedPluginID — из манифеста. Сверяется с тем, что плагин
	// заявит в HELLO_ACK: расхождение означает, что запущен не тот
	// плагин.
	ExpectedPluginID string

	// ProtocolVersion — поддерживаемая версия протокола.
	ProtocolVersion int

	// ExpectedCapabilities — из манифеста. Плагин не может заявить в
	// рантайме больше, чем декларировал при установке.
	ExpectedCapabilities []string

	// Authorizer — проверка прав на КАЖДЫЙ запрос.
	Authorizer ports.Authorizer

	// Handler — исполнение разрешённых операций.
	Handler Handler

	// RequestTimeout — максимальный дедлайн запроса.
	//
	// Плагин не может его превысить: иначе ограничение обходится одной
	// строкой в запросе (docs/ipc.md, раздел 7).
	RequestTimeout time.Duration

	// HandshakeTimeout — предел на handshake.
	HandshakeTimeout time.Duration

	// EventQueueSize — размер очереди исходящих событий.
	//
	// Ограничена намеренно: неограниченная очередь при медленном
	// плагине означает DoS через память ЯДРА, не плагина
	// (threat-model, T9).
	EventQueueSize int
}

// Session — состояние одной сессии.
type Session struct {
	cfg Config

	writeMu sync.Mutex

	mu       sync.Mutex
	state    sessionState
	inflight map[uint64]context.CancelFunc
	closeErr error

	events chan *Message
	done   chan struct{}
	// closeOnce гарантирует, что done закрывается ровно один раз.
	// Проверки state под мьютексом недостаточно: Run вызывает
	// closeWith и при ошибке handshake, и после readLoop, а Close
	// может прийти из другой горутины — сравнение state и close(done)
	// не атомарны относительно друг друга.
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type sessionState int

const (
	stateNew sessionState = iota
	stateHandshaking
	stateRunning
	stateClosed
)

func New(cfg Config) *Session {
	if cfg.EventQueueSize <= 0 {
		cfg.EventQueueSize = 64
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = 10 * time.Second
	}
	return &Session{
		cfg:      cfg,
		inflight: make(map[uint64]context.CancelFunc),
		events:   make(chan *Message, cfg.EventQueueSize),
		done:     make(chan struct{}),
	}
}

// Run выполняет handshake и обслуживает сессию до закрытия.
func (s *Session) Run(ctx context.Context) error {
	if err := s.handshake(ctx); err != nil {
		s.closeWith(err)
		return err
	}

	s.wg.Add(1)
	go s.eventLoop()

	err := s.readLoop(ctx)
	s.closeWith(err)
	s.wg.Wait()
	return err
}

// handshake — docs/ipc.md, раздел 1.
func (s *Session) handshake(ctx context.Context) error {
	if !s.setStateIfNotClosed(stateHandshaking) {
		return fmt.Errorf("ipc: handshake: %w", ErrClosedDuringHandshake)
	}

	hctx, cancel := context.WithTimeout(ctx, s.cfg.HandshakeTimeout)
	defer cancel()

	if err := s.write(&Message{
		Type:                 MsgHello,
		HelloProtocolVersion: s.cfg.ProtocolVersion,
	}); err != nil {
		return fmt.Errorf("ipc: send HELLO: %w: %w", ErrHandshakeFailed, err)
	}

	type result struct {
		msg *Message
		err error
	}
	ch := make(chan result, 1)
	go func() {
		m, err := Decode(s.cfg.Conn)
		ch <- result{m, err}
	}()

	var m *Message
	select {
	case r := <-ch:
		if r.err != nil {
			return fmt.Errorf("ipc: read HELLO_ACK: %w: %w", ErrHandshakeFailed, r.err)
		}
		m = r.msg
	case <-hctx.Done():
		return fmt.Errorf("ipc: handshake timeout: %w: %w", ErrHandshakeFailed, hctx.Err())
	}

	if m.Type != MsgHelloAck {
		return fmt.Errorf("%w: expected HELLO_ACK, got %s", ErrProtocolViolation, m.Type)
	}
	if m.HelloProtocolVersion != s.cfg.ProtocolVersion {
		return fmt.Errorf("%w: protocol version %d, expected %d",
			ErrProtocolViolation, m.HelloProtocolVersion, s.cfg.ProtocolVersion)
	}
	// Плагин заявляет plugin_id только здесь — и Runtime СВЕРЯЕТ его с
	// манифестом, а не принимает на веру.
	if m.HelloPluginID != s.cfg.ExpectedPluginID {
		return fmt.Errorf("%w: plugin_id %q, expected %q",
			ErrProtocolViolation, m.HelloPluginID, s.cfg.ExpectedPluginID)
	}
	// Заявленные capabilities не могут выходить за декларированные в
	// манифесте.
	allowed := make(map[string]bool, len(s.cfg.ExpectedCapabilities))
	for _, c := range s.cfg.ExpectedCapabilities {
		allowed[c] = true
	}
	for _, c := range m.HelloCapabilities {
		if !allowed[c] {
			return fmt.Errorf("%w: capability %q not declared in manifest", ErrProtocolViolation, c)
		}
	}

	// Session may have been Close()d while we were reading/validating
	// HELLO_ACK above (net.Pipe's Write returning to the caller only
	// means the bytes were consumed, not that this goroutine has since
	// finished processing them) — do not resurrect a closed session
	// into stateRunning.
	if !s.setStateIfNotClosed(stateRunning) {
		return fmt.Errorf("ipc: handshake: %w", ErrClosedDuringHandshake)
	}
	return nil
}

func (s *Session) readLoop(ctx context.Context) error {
	for {
		m, err := Decode(s.cfg.Conn)
		if err != nil {
			if IsProtocolError(err) {
				return fmt.Errorf("%w: %v", ErrProtocolViolation, err)
			}
			return err // I/O: процесс завершился
		}

		switch m.Type {
		case MsgRequest:
			s.wg.Add(1)
			go func(m *Message) {
				defer s.wg.Done()
				s.handleRequest(ctx, m)
			}(m)

		case MsgCancel:
			s.cancelRequest(m.RequestID)

		case MsgStopAck:
			return nil // штатное завершение

		case MsgHelloAck:
			// Повторный handshake — невозможный переход состояния.
			return fmt.Errorf("%w: duplicate HELLO_ACK", ErrProtocolViolation)

		default:
			// HELLO, RESPONSE, EVENT, STOP идут в обратном направлении:
			// плагин не имеет права их присылать.
			return fmt.Errorf("%w: unexpected %s from plugin", ErrProtocolViolation, m.Type)
		}
	}
}

// handleRequest — security path целиком.
func (s *Session) handleRequest(ctx context.Context, m *Message) {
	// 1. Дедлайн назначает Runtime, не плагин.
	reqCtx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()

	// 2. Регистрируем для возможной отмены.
	if !s.registerInflight(m.RequestID, cancel) {
		s.respondError(m.RequestID, ErrInvalidRequest, "duplicate request_id")
		return
	}
	defer s.unregisterInflight(m.RequestID)

	// 3. Определяем требуемое право по имени операции.
	//
	//    Таблица живёт в runtime (operations.go), а не в SDK: маппинг
	//    на стороне недоверенного процесса позволил бы плагину
	//    объявить, что storage.put требует storage.read.
	perm, known, requiresPermission := permissionForOp(m.Op)
	if !known {
		// «Не знаю такой операции» — не то же самое, что «тебе
		// нельзя»: SDK должен различать эти ответы.
		s.respondError(m.RequestID, ErrUnsupported, "unknown operation: "+m.Op)
		return
	}

	if !requiresPermission {
		// Операция без права (log.write): плагин должен уметь
		// сообщить о проблеме, не получив ничего.
		s.executeAndRespond(reqCtx, m)
		return
	}

	// 4. Авторизация — на КАЖДЫЙ запрос, с идентичностью из
	//    конфигурации сессии (установлена Runtime), а не из сообщения.
	allowed, err := s.cfg.Authorizer.Authorize(reqCtx, s.cfg.Caller, ports.Permission(perm))
	if err != nil {
		// Техническая невозможность принять решение.
		// ports.ErrUnknownCaller -> UNAUTHORIZED (субъект не
		// установлен), остальное -> INTERNAL_ERROR. Разделение по
		// docs/ipc.md р.5. errors.Is, не подстрока (ADR-033): ipc
		// по-прежнему не импортирует permissions, только ports, где и
		// живёт sentinel, — контракт, а не конкретная реализация
		// Authorizer.
		code := ErrInternal
		if errors.Is(err, ports.ErrUnknownCaller) {
			code = ErrUnauthorized
		}
		s.respondError(m.RequestID, code, err.Error())
		return
	}
	if !allowed {
		// Субъект установлен, права нет. Сообщаем ПРАВО, а не
		// операцию: плагину полезнее знать, чего именно просить.
		s.respondError(m.RequestID, ErrForbidden, "permission denied: "+perm)
		return
	}

	// 5. Только теперь — исполнение.
	s.executeAndRespond(reqCtx, m)
}

// executeAndRespond выполняет операцию и отправляет ответ.
//
// Вызывается ТОЛЬКО после успешной авторизации либо для операций, не
// требующих прав. Отдельный метод, чтобы оба пути шли через один код
// и не разошлись в обработке ошибок.
func (s *Session) executeAndRespond(ctx context.Context, m *Message) {
	payload, err := s.cfg.Handler.Handle(ctx, m.Op, m.Payload)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			s.respondError(m.RequestID, ErrCancelled, "cancelled")
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			s.respondError(m.RequestID, ErrDeadlineExceeded, "deadline exceeded")
			return
		}
		s.respondError(m.RequestID, ErrInternal, err.Error())
		return
	}

	s.write(&Message{Type: MsgResponse, RequestID: m.RequestID, Payload: payload})
}

func (s *Session) registerInflight(id uint64, cancel context.CancelFunc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != stateRunning {
		return false
	}
	if _, exists := s.inflight[id]; exists {
		return false // повторное использование активного request_id
	}
	s.inflight[id] = cancel
	return true
}

func (s *Session) unregisterInflight(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, id)
}

func (s *Session) cancelRequest(id uint64) {
	s.mu.Lock()
	cancel := s.inflight[id]
	s.mu.Unlock()
	// CANCEL на неизвестный/завершённый request_id — не ошибка:
	// гонка, ответ мог быть отправлен, пока CANCEL летел.
	if cancel != nil {
		cancel()
	}
}

// SendEvent ставит событие в очередь.
//
// Очередь ограничена: при переполнении событие ОТБРАСЫВАЕТСЯ, а не
// копится. Неограниченный рост означал бы DoS через память ядра при
// медленном или зависшем плагине (threat-model, T9).
func (s *Session) SendEvent(eventType string, payload []byte) error {
	s.mu.Lock()
	closed := s.state == stateClosed
	s.mu.Unlock()
	if closed {
		return ErrSessionClosed
	}

	select {
	case s.events <- &Message{Type: MsgEvent, EventType: eventType, Payload: payload}:
		return nil
	default:
		return fmt.Errorf("ipc: event queue full, dropping %q", eventType)
	}
}

func (s *Session) eventLoop() {
	defer s.wg.Done()
	for {
		select {
		case m := <-s.events:
			if err := s.write(m); err != nil {
				return
			}
		case <-s.done:
			return
		}
	}
}

// Stop инициирует штатную остановку (docs/ipc.md, раздел 9).
func (s *Session) Stop(ctx context.Context) error {
	return s.write(&Message{Type: MsgStop})
}

func (s *Session) respondError(id uint64, code ErrorCode, msg string) {
	s.write(&Message{
		Type:         MsgResponse,
		RequestID:    id,
		ErrorCode:    code,
		ErrorMessage: msg,
	})
}

func (s *Session) write(m *Message) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return Encode(s.cfg.Conn, m)
}

// Close закрывает сессию и отменяет все активные запросы.
func (s *Session) Close() error {
	return s.closeWith(nil)
}

func (s *Session) closeWith(cause error) error {
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		// Unconditional: stateClosed is the terminal state, and this is
		// the only place allowed to enter it. Every OTHER writer of
		// s.state must check setStateIfNotClosed instead of writing
		// directly — see its doc comment for why.
		s.state = stateClosed
		s.closeErr = cause
		cancels := make([]context.CancelFunc, 0, len(s.inflight))
		for _, c := range s.inflight {
			cancels = append(cancels, c)
		}
		s.inflight = make(map[uint64]context.CancelFunc)
		s.mu.Unlock()

		close(s.done)
		// Разбудить все активные запросы: иначе они висели бы до
		// собственного дедлайна, удерживая ресурсы уже закрытой сессии.
		for _, c := range cancels {
			c()
		}
		err = s.cfg.Conn.Close()
	})
	return err
}

// State возвращает текущее состояние — для тестов и диагностики.
func (s *Session) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == stateClosed
}

// setStateIfNotClosed transitions the session to next, unless it has
// already been closed. Returns false if it has — the caller must treat
// that as "the session closed while I was working", not proceed as if
// the transition happened.
//
// stateClosed is terminal: nothing may leave it. Without this guard,
// handshake() (the only other writer of s.state — see its two call
// sites) can race Close(): Close() may set stateClosed while handshake()
// is still validating HELLO_ACK in a separate goroutine, and handshake()
// finishing afterward with an unconditional write would silently
// overwrite stateClosed back to stateHandshaking/stateRunning. Both
// writes are individually mutex-protected, so `go test -race` never
// flags this — it's a logic race (last-writer-wins across two
// independently-synchronized critical sections), not a data race. Found
// via IsClosed() returning false immediately after Close() under -race,
// which slows handshake() enough to widen the window
// (photon-plugin-runtime/IMPLEMENTATION_LOG.md has the full story).
func (s *Session) setStateIfNotClosed(next sessionState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateClosed {
		return false
	}
	s.state = next
	return true
}
