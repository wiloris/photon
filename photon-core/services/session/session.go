package session

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/wiloris/photon-core/ports"
)

// State — состояние сессии. См. диаграмму в bridge-session.md §6.
type State int

const (
	StateCreated State = iota
	StateConnecting
	StateReady
	StateFailed
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateCreated:
		return "created"
	case StateConnecting:
		return "connecting"
	case StateReady:
		return "ready"
	case StateFailed:
		return "failed"
	case StateClosed:
		return "closed"
	}
	return "unknown"
}

// Config — параметры сессии.
type Config struct {
	// Transport — замороженный порт (ADR-020). Сессия ходит через него, а не
	// через quic-go напрямую: прямая зависимость тест-клиента от quic-go
	// нормальна для клиента, проверяющего сервер, но не для кода ядра.
	Transport ports.Transport
	Endpoint  ports.Endpoint

	SessionToken string
	Mode         Mode

	// Codec — nil означает JSONCodec (ADR-034: JSON — формат по умолчанию и
	// единственный сейчас).
	Codec Codec
}

// Session — соединение с Bridge и его жизненный цикл.
//
// Реконнекта здесь НЕТ намеренно: при обрыве сессия переходит в StateFailed и
// сообщает об этом, а политику переподключения держит services/autodial
// (Phase 7, уже реализована). Вторая политика в этом пакете разошлась бы с
// первой — см. bridge-session.md §6.
type Session struct {
	mu    sync.Mutex
	cfg   Config
	codec Codec
	state State
	conn  ports.Connection

	// closeCh закрывается ровно один раз, в Close. Служит сигналом отмены
	// для идущего Connect: без него Close на подключающейся сессии ждал бы
	// завершения Connect. Прецедент прямой — вечное зависание host.Stop на
	// неотвечающем плагине (Phase 9), найденное не ревью, а тестом отказа.
	closeCh chan struct{}
}

// New создаёт сессию. Сети не касается — соединение устанавливает Connect.
//
// Это же правило теперь записано в photon_core.h для Photon_Create: сетевой
// вызов внутри конструктора означал бы создание объекта, блокирующееся на
// неопределённое время и падающее от временной недоступности сети.
func New(cfg Config) (*Session, error) {
	if cfg.Transport == nil {
		return nil, errors.New("session: Transport is required")
	}
	if cfg.Endpoint == "" {
		return nil, errors.New("session: Endpoint is required")
	}
	switch cfg.Mode {
	case ModeAction, ModeSubscribe:
	default:
		return nil, fmt.Errorf("session: unknown mode %q", cfg.Mode)
	}

	codec := cfg.Codec
	if codec == nil {
		codec = JSONCodec{}
	}

	return &Session{
		cfg:     cfg,
		codec:   codec,
		state:   StateCreated,
		closeCh: make(chan struct{}),
	}, nil
}

// State возвращает текущее состояние.
func (s *Session) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Connect устанавливает соединение и проводит handshake.
//
// На уже подключённой сессии возвращает nil, не открывая второго соединения
// (идемпотентен по результату, не по эффекту — bridge-session.md §6, п. 1).
func (s *Session) Connect(ctx context.Context) error {
	s.mu.Lock()
	switch s.state {
	case StateClosed:
		s.mu.Unlock()
		return ErrSessionClosed
	case StateReady:
		s.mu.Unlock()
		return nil
	case StateConnecting:
		s.mu.Unlock()
		return ErrConnectInProgress
	}
	s.state = StateConnecting
	closeCh := s.closeCh
	s.mu.Unlock()

	// ctx, отменяемый и вызывающим, и Close. Без второго Close пришлось бы
	// ждать завершения Connect.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-closeCh:
			cancel()
		case <-cctx.Done():
		}
	}()

	conn, err := s.cfg.Transport.Connect(cctx, s.cfg.Endpoint)
	if err != nil {
		return s.failConnect(fmt.Errorf("session: connect: %w", err))
	}

	if err := s.handshake(cctx, conn); err != nil {
		conn.Close()
		return s.failConnect(err)
	}

	s.mu.Lock()
	// Close мог произойти, пока шёл handshake. Тогда соединение закрывается,
	// а сессия остаётся закрытой: воскрешать её нельзя (bridge-session.md
	// §6, п. 4).
	if s.state == StateClosed {
		s.mu.Unlock()
		conn.Close()
		return ErrSessionClosed
	}
	s.conn = conn
	s.state = StateReady
	s.mu.Unlock()
	return nil
}

// failConnect переводит сессию в Failed, не затирая уже наступивший Closed.
func (s *Session) failConnect(err error) error {
	s.mu.Lock()
	if s.state != StateClosed {
		s.state = StateFailed
	} else {
		err = ErrSessionClosed
	}
	s.mu.Unlock()
	return err
}

// handshake проводит три фрейма первого стрима: auth → mode → ack.
func (s *Session) handshake(ctx context.Context, conn ports.Connection) error {
	st, err := conn.OpenStream(ctx)
	if err != nil {
		return fmt.Errorf("session: open handshake stream: %w", err)
	}
	// Half-close пишущей стороны после обмена: стрим handshake'а больше не
	// нужен, соединение живёт дальше.
	defer st.Close()

	if err := s.codec.WriteHandshake(st, Handshake{
		SessionToken:    s.cfg.SessionToken,
		Mode:            s.cfg.Mode,
		ProtocolVersion: ProtocolVersion,
	}); err != nil {
		return err
	}

	ack, err := s.codec.ReadHandshakeAck(st)
	if err != nil {
		return err
	}
	if !ack.OK {
		return fmt.Errorf("%w: %s", ErrHandshakeRejected, ack.Error)
	}

	// Версия проверяется ПОСЛЕ ok: если сервер отверг токен, разговор о
	// версиях не имеет смысла и увёл бы диагностику в сторону.
	//
	// Ноль = поле не прислано. Это НЕ несовместимость: развёрнутый сегодня
	// Bridge о версиях не знает (bridge-session.md §5), и отвергать его
	// значило бы сломать работающую систему этой самой проверкой.
	if ack.ProtocolVersion != 0 && ack.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("%w: server %d, client %d",
			ErrProtocolVersion, ack.ProtocolVersion, ProtocolVersion)
	}

	// Режим проверяется ПОСЛЕ версии: несовместимый протокол делает разговор
	// о режимах бессмысленным.
	//
	// Пустой Status = сервер не сообщил, и это не ошибка (bridge-session.md
	// §2.1). Но если сообщил — он обязан совпасть с запрошенным: сервер
	// молча откатывается в "action" при любой ошибке чтения mode-фрейма
	// (quic_listener.go:270-275), отвечая при этом ok:true. Без этой
	// проверки такой откат обнаруживается не здесь, а много позже — вечным
	// ожиданием в AcceptPush на соединении, где push'ей не будет.
	if want := statusForMode(s.cfg.Mode); want != "" && ack.Status != "" && ack.Status != want {
		return fmt.Errorf("%w: requested %q (expected status %q), server confirmed %q",
			ErrModeNotAccepted, s.cfg.Mode, want, ack.Status)
	}
	return nil
}

// Do отправляет action и возвращает ответ. Один action — один стрим.
//
// Не подключается неявно: на неподключённой сессии возвращает ErrNotReady
// (bridge-session.md §6, п. 5).
func (s *Session) Do(ctx context.Context, action int32, payload []byte) (Response, error) {
	// Проверка action'а — ДО обращения к состоянию и к сети: неизвестный код
	// не должен ни открывать стрим, ни зависеть от того, подключены мы или
	// нет. На границе ABI этому соответствует PHOTON_ERR_INVALID_ACTION.
	resource, err := ResourceFor(action)
	if err != nil {
		return Response{}, err
	}

	s.mu.Lock()
	switch s.state {
	case StateClosed:
		s.mu.Unlock()
		return Response{}, ErrSessionClosed
	case StateReady:
	default:
		st := s.state
		s.mu.Unlock()
		return Response{}, fmt.Errorf("%w (state=%s)", ErrNotReady, st)
	}
	conn := s.conn
	s.mu.Unlock()

	stream, err := conn.OpenStream(ctx)
	if err != nil {
		return Response{}, fmt.Errorf("session: open action stream: %w", err)
	}

	type result struct {
		resp Response
		err  error
	}
	done := make(chan result, 1)

	go func() {
		if err := s.codec.WriteRequest(stream, Request{
			Action: action, Resource: resource, Payload: payload,
		}); err != nil {
			done <- result{err: err}
			return
		}
		resp, err := s.codec.ReadResponse(stream)
		done <- result{resp: resp, err: err}
	}()

	select {
	case r := <-done:
		stream.Close()
		return r.resp, r.err

	case <-ctx.Done():
		// Освободить стрим полностью здесь НЕЛЬЗЯ: ports.Stream.Close —
		// half-close только пишущей стороны, а CancelRead/CancelWrite порт
		// намеренно не экспортирует (adapters/quic/quic.go, комментарий к
		// stream.Close). Горутина выше останется на Read до ответа сервера
		// или закрытия соединения.
		//
		// Это записанное ограничение замороженного порта, а не недоделка:
		// bridge-session.md §6 п. 6 и §9 п. 4. Расширение порта — отдельный
		// ADR (ADR-020), не побочный эффект этой фазы.
		//
		// Что здесь всё же делается: вызывающий не ждёт неотвечающий сервер
		// дольше своего ctx, и стрим закрывается на запись — сервер видит
		// конец запроса. done буферизован, поэтому горутина не повиснет на
		// отправке в канал, который никто не читает.
		stream.Close()
		return Response{}, ctx.Err()
	}
}

// AcceptPush принимает одно push-событие. Только для ModeSubscribe:
// на action-соединении сервер UniStream'ов не открывает
// (bridge-session.md §2.4).
func (s *Session) AcceptPush(ctx context.Context) (Push, error) {
	s.mu.Lock()
	switch s.state {
	case StateClosed:
		s.mu.Unlock()
		return Push{}, ErrSessionClosed
	case StateReady:
	default:
		st := s.state
		s.mu.Unlock()
		return Push{}, fmt.Errorf("%w (state=%s)", ErrNotReady, st)
	}
	if s.cfg.Mode != ModeSubscribe {
		s.mu.Unlock()
		return Push{}, fmt.Errorf("session: AcceptPush requires mode %q, have %q",
			ModeSubscribe, s.cfg.Mode)
	}
	conn := s.conn
	s.mu.Unlock()

	stream, err := conn.AcceptUniStream(ctx)
	if err != nil {
		return Push{}, fmt.Errorf("session: accept push stream: %w", err)
	}
	return s.codec.ReadPush(stream)
}

// Close закрывает сессию. Идемпотентен и безопасен из любого состояния,
// включая повторный вызов и вызов во время Connect.
//
// Прецедент, ради которого это проверяется отдельным тестом: double-free в
// ipc.closeWith при двойном вызове (Phase 9) — не проявлялся при нормальной
// работе.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.state == StateClosed {
		s.mu.Unlock()
		return nil
	}
	s.state = StateClosed
	conn := s.conn
	s.conn = nil
	// Ровно одно закрытие канала: второй close(chan) паникует, а Close
	// обязан быть безопасен при повторном вызове.
	close(s.closeCh)
	s.mu.Unlock()

	if conn != nil {
		return conn.Close()
	}
	return nil
}
