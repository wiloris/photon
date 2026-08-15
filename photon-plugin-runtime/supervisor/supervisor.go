package supervisor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wiloris/photon-plugin-runtime/host"
)

// State — состояние плагина (docs/lifecycle.md, раздел 1).
type State int

const (
	StateStopped State = iota
	StateStarting
	StateRunning
	StateStopping
	StateTerminating
	StateFailed
	StateDisabled
)

func (s State) String() string {
	switch s {
	case StateStopped:
		return "STOPPED"
	case StateStarting:
		return "STARTING"
	case StateRunning:
		return "RUNNING"
	case StateStopping:
		return "STOPPING"
	case StateTerminating:
		return "TERMINATING"
	case StateFailed:
		return "FAILED"
	case StateDisabled:
		return "DISABLED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(s))
	}
}

var (
	ErrDisabled       = errors.New("supervisor: plugin is disabled")
	ErrAlreadyManaged = errors.New("supervisor: plugin is already managed")
	ErrNotManaged     = errors.New("supervisor: plugin is not managed")
)

// HostFactory создаёт новый экземпляр host для перезапуска.
//
// Именно фабрика, а не переиспользование одного Host: экземпляр host
// одноразовый (один процесс = одна сессия), и перезапуск означает
// создание нового, а не воскрешение старого.
type HostFactory func() (*host.Host, error)

// Supervisor управляет жизненным циклом одного плагина.
//
// Базовый инвариант, проверяемый тестами: ни зависший, ни падающий в
// цикле плагин не может навечно остановить supervisor. Все ожидания
// ограничены, все состояния имеют выход.
type Supervisor struct {
	factory HostFactory
	policy  RestartConfig
	clock   Clock

	mu       sync.Mutex
	state    State
	current  *host.Host
	restart  restartState
	stopReq  bool
	disabled bool

	// generation увеличивается при каждом Stop.
	//
	// Решает гонку «ручной Stop одновременно с запланированным
	// restart»: горутина перезапуска сверяет поколение перед стартом и
	// молча уходит, если Stop пришёл, пока она спала в backoff.
	generation uint64

	runCtx    context.Context
	runCancel context.CancelFunc
	wg        sync.WaitGroup

	// stateCh — уведомления о смене состояния, для тестов и UI.
	stateCh chan State
}

// Clock — источник времени; подменяется в тестах, чтобы не ждать
// реальный backoff.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Config — параметры supervisor.
type Config struct {
	Factory HostFactory
	Restart RestartConfig
	Clock   Clock
}

func New(cfg Config) *Supervisor {
	clock := cfg.Clock
	if clock == nil {
		clock = realClock{}
	}
	return &Supervisor{
		factory: cfg.Factory,
		policy:  cfg.Restart,
		clock:   clock,
		state:   StateStopped,
		stateCh: make(chan State, 32),
	}
}

// Start запускает плагин и берёт его под надзор.
func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.disabled {
		s.mu.Unlock()
		return ErrDisabled
	}
	if s.state != StateStopped && s.state != StateFailed {
		s.mu.Unlock()
		return ErrAlreadyManaged
	}
	s.stopReq = false
	s.runCtx, s.runCancel = context.WithCancel(context.Background())
	gen := s.generation
	s.mu.Unlock()

	return s.startInstance(ctx, gen)
}

func (s *Supervisor) startInstance(ctx context.Context, gen uint64) error {
	s.setState(StateStarting)

	h, err := s.factory()
	if err != nil {
		s.setState(StateFailed)
		s.handleExit(host.ExitStatus{Kind: host.ExitStartupFailure, Code: -1, Err: err}, gen)
		return fmt.Errorf("supervisor: create host: %w", err)
	}

	if err := h.Start(ctx); err != nil {
		s.setState(StateFailed)
		// host уже сообщил причину через свой канал; читаем её, чтобы
		// не потерять классификацию.
		select {
		case st := <-h.Exited():
			s.handleExit(st, gen)
		case <-time.After(time.Second):
			s.handleExit(host.ExitStatus{Kind: host.ExitStartupFailure, Code: -1, Err: err}, gen)
		}
		return fmt.Errorf("supervisor: start: %w", err)
	}

	s.mu.Lock()
	s.current = h
	s.restart.markStarted(s.clock.Now())
	s.mu.Unlock()

	s.setState(StateRunning)

	s.wg.Add(1)
	go s.watch(h, gen)
	return nil
}

// watch ждёт завершения экземпляра и передаёт решение политике.
func (s *Supervisor) watch(h *host.Host, gen uint64) {
	defer s.wg.Done()

	select {
	case exit := <-h.Exited():
		s.handleExit(exit, gen)
	case <-s.runContext().Done():
		// Supervisor остановлен — экземпляр уже добит через Stop.
		return
	}
}

func (s *Supervisor) handleExit(exit host.ExitStatus, gen uint64) {
	s.mu.Lock()
	// Проверка поколения: если Stop пришёл, пока экземпляр завершался,
	// решение о перезапуске неактуально.
	stale := gen != s.generation || s.stopReq
	s.current = nil
	s.mu.Unlock()

	if stale {
		s.setState(StateStopped)
		return
	}

	decision := s.policy.Decide(&s.restart, exit, s.clock.Now())

	switch {
	case decision.Disable:
		s.mu.Lock()
		s.disabled = true
		s.mu.Unlock()
		s.setState(StateDisabled)

	case decision.Restart:
		s.setState(StateFailed)
		s.wg.Add(1)
		go s.scheduleRestart(decision.Delay, gen)

	default:
		s.setState(StateStopped)
	}
}

// scheduleRestart ждёт backoff и перезапускает — если за это время не
// пришёл Stop.
func (s *Supervisor) scheduleRestart(delay time.Duration, gen uint64) {
	defer s.wg.Done()

	select {
	case <-s.clock.After(delay):
	case <-s.runContext().Done():
		// Ручной Stop во время backoff: перезапуск отменён.
		// Это одна из самых неприятных гонок — без проверки плагин
		// «воскресал» бы через секунду после явной остановки.
		return
	}

	s.mu.Lock()
	stale := gen != s.generation || s.stopReq || s.disabled
	ctx := s.runCtx
	s.mu.Unlock()
	if stale {
		return
	}

	if err := s.startInstance(ctx, gen); err != nil {
		// Ошибка уже классифицирована внутри startInstance.
		return
	}
}

// Stop останавливает плагин и отменяет запланированные перезапуски.
//
// Не блокируется навечно: host гарантирует ограниченное время
// остановки даже для зависшего плагина.
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.state == StateStopped || s.state == StateDisabled {
		s.mu.Unlock()
		return nil
	}
	s.stopReq = true
	s.generation++ // обесценивает все запланированные перезапуски
	h := s.current
	cancel := s.runCancel
	s.mu.Unlock()

	s.setState(StateStopping)

	if cancel != nil {
		cancel() // будит горутины, спящие в backoff
	}
	if h != nil {
		if err := h.Stop(ctx); err != nil && !errors.Is(err, host.ErrNotRunning) {
			return err
		}
	}

	s.setState(StateStopped)
	return nil
}

// Disable переводит плагин в DISABLED вручную.
func (s *Supervisor) Disable(ctx context.Context) error {
	if err := s.Stop(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	s.disabled = true
	s.mu.Unlock()
	s.setState(StateDisabled)
	return nil
}

// Enable снимает DISABLED.
//
// Только явным действием: автоматического выхода из DISABLED нет —
// иначе решение «система решила не пытаться дальше» обесценивалось бы
// само собой (docs/lifecycle.md, раздел 7).
func (s *Supervisor) Enable() {
	s.mu.Lock()
	s.disabled = false
	s.restart.mu.Lock()
	s.restart.consecutiveFailures = 0
	s.restart.mu.Unlock()
	s.state = StateStopped
	s.mu.Unlock()
}

// Wait дожидается завершения всех фоновых горутин.
func (s *Supervisor) Wait() { s.wg.Wait() }

// State возвращает текущее состояние.
func (s *Supervisor) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Failures — счётчик последовательных неудач, для диагностики.
func (s *Supervisor) Failures() int { return s.restart.failures() }

// StateChanges — канал уведомлений о смене состояния.
func (s *Supervisor) StateChanges() <-chan State { return s.stateCh }

func (s *Supervisor) setState(st State) {
	s.mu.Lock()
	s.state = st
	s.mu.Unlock()

	// Неблокирующая отправка: медленный или отсутствующий читатель не
	// должен останавливать supervisor — тот же принцип, что для
	// очереди событий в IPC.
	select {
	case s.stateCh <- st:
	default:
	}
}

func (s *Supervisor) runContext() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runCtx == nil {
		return context.Background()
	}
	return s.runCtx
}
