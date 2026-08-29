package supervisor

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/host"
	"github.com/wiloris/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon-plugin-runtime/permissions"
	"github.com/wiloris/photon-plugin-runtime/sandbox"
)

// Базовый инвариант, против которого написаны эти тесты:
//
//	Ни зависший, ни падающий в цикле плагин не может навечно
//	остановить Supervisor.
//
// Поэтому каждый тест имеет жёсткий таймаут: если supervisor завис,
// тест падает по времени, а не висит вместе с ним.

// fakeSpawner — управляемый «процесс» плагина.
type fakeSpawner struct {
	behavior func(conn net.Conn)
	exitErr  error

	mu       sync.Mutex
	spawns   int
	killed   bool
	startErr error
}

func (s *fakeSpawner) Spawn(ctx context.Context, entrypoint string, iso host.Isolation, spec sandbox.Spec) (io.ReadWriteCloser, func() error, func(), sandbox.Applied, error) {
	s.mu.Lock()
	if s.startErr != nil {
		err := s.startErr
		s.mu.Unlock()
		return nil, nil, nil, sandbox.Applied{}, err
	}
	s.spawns++
	s.mu.Unlock()

	runtimeSide, pluginSide := net.Pipe()
	killed := make(chan struct{})
	done := make(chan struct{})

	if s.behavior != nil {
		go func() {
			defer close(done)
			defer pluginSide.Close()
			s.behavior(pluginSide)
		}()
	} else {
		close(done)
	}

	wait := func() error {
		select {
		case <-done:
		case <-killed:
		}
		return s.exitErr
	}

	var once sync.Once
	kill := func() {
		once.Do(func() {
			s.mu.Lock()
			s.killed = true
			s.mu.Unlock()
			close(killed)
			pluginSide.Close()
		})
	}
	return runtimeSide, wait, kill, sandbox.Applied{
		Platform: "test",
		Properties: map[sandbox.Property]bool{
			sandbox.PropSeparateProcess: true,
			sandbox.PropSingleChannel:   true,
			sandbox.PropTerminable:      true,
		},
	}, nil
}

func (s *fakeSpawner) spawnCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spawns
}

func testManifest() *manifest.Manifest {
	return &manifest.Manifest{
		ManifestVersion: 1,
		Identity:        manifest.Identity{PluginID: "com.engram.test", Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: 1,
			Entrypoint:      "bin/plugin",
			InstancePolicy:  manifest.InstanceSingle,
		},
		Capabilities: manifest.Capabilities{Provided: []string{"test.op"}},
	}
}

func hostFactory(sp host.Spawner) HostFactory {
	return func() (*host.Host, error) {
		m := testManifest()
		policy := permissions.NewPolicy()
		policy.Grant(permissions.Subject{PluginID: m.Identity.PluginID}, []string{"storage.read"})
		auth := permissions.NewAuthorizer(policy)

		return host.New(host.Config{
			Manifest:    m,
			PackageRoot: "/tmp/pkg",
			Caller:      ports.PublicKey("sv-key"),
			Policy:      policy,
			Authorizer:  auth,
			Handler: ipc.HandlerFunc(func(ctx context.Context, _ ipc.CallerInfo, op string, payload []byte) ([]byte, error) {
				return []byte("ok"), nil
			}),
			Spawner: sp,
		}), nil
	}
}

// goodPlugin проходит handshake и живёт заданное время.
func goodPlugin(hold time.Duration) func(net.Conn) {
	return func(conn net.Conn) {
		if _, err := ipc.Decode(conn); err != nil {
			return
		}
		ipc.Encode(conn, &ipc.Message{
			Type: ipc.MsgHelloAck, HelloProtocolVersion: 1,
			HelloPluginID: "com.engram.test", HelloCapabilities: []string{"test.op"},
		})
		if hold > 0 {
			time.Sleep(hold)
		}
	}
}

// crashingPlugin падает сразу после handshake.
func crashingPlugin() func(net.Conn) {
	return goodPlugin(20 * time.Millisecond)
}

// violatingPlugin нарушает протокол.
func violatingPlugin() func(net.Conn) {
	return func(conn net.Conn) {
		ipc.Decode(conn)
		ipc.Encode(conn, &ipc.Message{
			Type: ipc.MsgHelloAck, HelloProtocolVersion: 1, HelloPluginID: "com.engram.test",
		})
		// Сообщение неверного направления.
		ipc.Encode(conn, &ipc.Message{Type: ipc.MsgHello})
		time.Sleep(500 * time.Millisecond)
	}
}

// hangingPlugin проходит handshake и зависает навсегда.
func hangingPlugin() func(net.Conn) {
	return func(conn net.Conn) {
		ipc.Decode(conn)
		ipc.Encode(conn, &ipc.Message{
			Type: ipc.MsgHelloAck, HelloProtocolVersion: 1, HelloPluginID: "com.engram.test",
		})
		time.Sleep(10 * time.Minute) // «навсегда»
	}
}

func fastConfig() RestartConfig {
	c := DefaultRestartConfig()
	c.BaseDelay = 20 * time.Millisecond
	c.MaxDelay = 100 * time.Millisecond
	c.MaxAttempts = 3
	c.StableAfter = time.Hour // не сбрасывать счётчик в тестах
	c.JitterFraction = 0
	return c
}

func waitForState(t *testing.T, s *Supervisor, want State, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if s.State() == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for %s, current state %s", want, s.State())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// ── Базовый жизненный цикл ──────────────────────────────────────────

func TestStart_ReachesRunning(t *testing.T) {
	sp := &fakeSpawner{behavior: goodPlugin(time.Second)}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop(context.Background())

	waitForState(t, s, StateRunning, time.Second)
}

func TestStop_ReachesStopped(t *testing.T) {
	sp := &fakeSpawner{behavior: goodPlugin(5 * time.Second)}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	s.Start(context.Background())
	waitForState(t, s, StateRunning, time.Second)

	done := make(chan error, 1)
	go func() { done <- s.Stop(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Stop blocked — supervisor must not hang")
	}
	waitForState(t, s, StateStopped, time.Second)
}

// ── Crash-loop ──────────────────────────────────────────────────────

func TestCrashLoop_EventuallyDisables(t *testing.T) {
	// Плагин падает сразу после каждого запуска. Supervisor обязан
	// остановиться на DISABLED, а не крутить цикл вечно.
	sp := &fakeSpawner{behavior: crashingPlugin()}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()}) // MaxAttempts=3

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitForState(t, s, StateDisabled, 5*time.Second)

	// Ровно MaxAttempts+1 запусков: первый + перезапуски до исчерпания.
	if n := sp.spawnCount(); n > fastConfig().MaxAttempts+1 {
		t.Fatalf("spawned %d times, limit is %d — crash loop was not bounded",
			n, fastConfig().MaxAttempts+1)
	}
}

func TestCrashLoop_StartAfterDisabledIsRejected(t *testing.T) {
	sp := &fakeSpawner{behavior: crashingPlugin()}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	s.Start(context.Background())
	waitForState(t, s, StateDisabled, 5*time.Second)

	if err := s.Start(context.Background()); !errors.Is(err, ErrDisabled) {
		t.Fatalf("Start after DISABLED: got %v, want ErrDisabled", err)
	}
}

func TestEnable_ClearsDisabledAndCounter(t *testing.T) {
	sp := &fakeSpawner{behavior: crashingPlugin()}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	s.Start(context.Background())
	waitForState(t, s, StateDisabled, 5*time.Second)

	s.Enable()
	if s.State() != StateStopped {
		t.Fatalf("state after Enable = %s, want STOPPED", s.State())
	}
	if s.Failures() != 0 {
		t.Fatalf("failures after Enable = %d, want 0", s.Failures())
	}
}

// ── Protocol violation ──────────────────────────────────────────────

func TestProtocolViolation_DisablesWithoutRestart(t *testing.T) {
	// Нарушение протокола не перезапускается: оно повторится и может
	// быть попыткой атаки.
	sp := &fakeSpawner{behavior: violatingPlugin()}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitForState(t, s, StateDisabled, 5*time.Second)

	if n := sp.spawnCount(); n != 1 {
		t.Fatalf("spawned %d times — protocol_violation must not trigger a restart", n)
	}
}

// ── Гонки: Stop во время backoff ────────────────────────────────────

func TestRace_StopDuringBackoffCancelsRestart(t *testing.T) {
	// Одна из самых неприятных гонок: плагин упал, перезапуск
	// запланирован, и в этот момент приходит ручной Stop. Плагин НЕ
	// должен «воскреснуть» через секунду после явной остановки.
	c := fastConfig()
	c.BaseDelay = 300 * time.Millisecond // достаточно, чтобы успеть

	sp := &fakeSpawner{behavior: crashingPlugin()}
	s := New(Config{Factory: hostFactory(sp), Restart: c})

	s.Start(context.Background())
	// Дожидаемся падения и планирования перезапуска.
	waitForState(t, s, StateFailed, 2*time.Second)

	countBefore := sp.spawnCount()

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Ждём дольше, чем backoff: перезапуска быть не должно.
	time.Sleep(c.BaseDelay * 3)

	if n := sp.spawnCount(); n != countBefore {
		t.Fatalf("plugin was restarted %d times after an explicit Stop — "+
			"scheduled restart must be cancelled", n-countBefore)
	}
	if st := s.State(); st == StateRunning || st == StateStarting {
		t.Fatalf("state = %s after Stop — plugin came back to life", st)
	}
}

func TestRace_StopDuringStartingDoesNotHang(t *testing.T) {
	// Stop приходит, пока экземпляр ещё поднимается.
	sp := &fakeSpawner{behavior: goodPlugin(5 * time.Second)}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	s.Start(context.Background())

	done := make(chan error, 1)
	go func() { done <- s.Stop(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stop during STARTING: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Stop hung while the instance was starting")
	}
}

func TestRace_ConcurrentStopCalls(t *testing.T) {
	sp := &fakeSpawner{behavior: goodPlugin(5 * time.Second)}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	s.Start(context.Background())
	waitForState(t, s, StateRunning, time.Second)

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.Stop(context.Background())
		}()
	}

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Stop calls deadlocked")
	}

	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Stop: %v", err)
		}
	}
}

func TestRace_StartStopStress(t *testing.T) {
	// Быстрое чередование Start/Stop не должно приводить ни к
	// зависанию, ни к панике.
	sp := &fakeSpawner{behavior: goodPlugin(50 * time.Millisecond)}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			s.Start(context.Background())
			time.Sleep(5 * time.Millisecond)
			s.Stop(context.Background())
		}
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("start/stop stress deadlocked")
	}
}

// ── Зависший плагин ─────────────────────────────────────────────────

func TestHangingPlugin_StopStillReturns(t *testing.T) {
	// Главный инвариант: зависший плагин не может навечно остановить
	// supervisor.
	sp := &fakeSpawner{behavior: hangingPlugin()}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	s.Start(context.Background())
	waitForState(t, s, StateRunning, 2*time.Second)

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- s.Stop(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("Stop took %v on a hanging plugin", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a hanging plugin blocked Supervisor.Stop forever — " +
			"no component may halt the runtime indefinitely")
	}
}

func TestHangingPlugin_DisableStillReturns(t *testing.T) {
	sp := &fakeSpawner{behavior: hangingPlugin()}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	s.Start(context.Background())
	waitForState(t, s, StateRunning, 2*time.Second)

	done := make(chan error, 1)
	go func() { done <- s.Disable(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Disable: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Disable hung on a hanging plugin")
	}
	if s.State() != StateDisabled {
		t.Fatalf("state = %s, want DISABLED", s.State())
	}
}

// ── Startup failure ─────────────────────────────────────────────────

func TestStartupFailure_DisablesImmediately(t *testing.T) {
	sp := &fakeSpawner{startErr: errors.New("exec: no such file")}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	s.Start(context.Background())
	waitForState(t, s, StateDisabled, 3*time.Second)

	if n := sp.spawnCount(); n != 0 {
		t.Fatalf("spawned %d times despite a startup failure", n)
	}
}

func TestFactoryError_DoesNotHang(t *testing.T) {
	// Фабрика host может отказать — supervisor не должен зависнуть.
	s := New(Config{
		Factory: func() (*host.Host, error) { return nil, errors.New("factory boom") },
		Restart: fastConfig(),
	})

	done := make(chan error, 1)
	go func() { done <- s.Start(context.Background()) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Start must fail when the factory fails")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Start hung on a failing factory")
	}
	waitForState(t, s, StateDisabled, 2*time.Second)
}

// ── Состояния ───────────────────────────────────────────────────────

func TestStateChanges_AreObservable(t *testing.T) {
	sp := &fakeSpawner{behavior: goodPlugin(time.Second)}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	s.Start(context.Background())
	defer s.Stop(context.Background())

	var seen []State
	deadline := time.After(2 * time.Second)
collect:
	for {
		select {
		case st := <-s.StateChanges():
			seen = append(seen, st)
			if st == StateRunning {
				break collect
			}
		case <-deadline:
			t.Fatalf("did not observe RUNNING, saw: %v", seen)
		}
	}

	if len(seen) < 2 || seen[0] != StateStarting {
		t.Fatalf("expected STARTING then RUNNING, saw: %v", seen)
	}
}

func TestStateChannel_DoesNotBlockSupervisor(t *testing.T) {
	// Никто не читает stateCh. Supervisor не должен из-за этого
	// остановиться — тот же принцип, что для очереди событий в IPC.
	sp := &fakeSpawner{behavior: crashingPlugin()}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Start(context.Background())
		// Ждём DISABLED без чтения канала состояний.
		for i := 0; i < 200; i++ {
			if s.State() == StateDisabled {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
	}()

	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("supervisor blocked because nobody reads the state channel")
	}
}

func TestStopWhenAlreadyStopped(t *testing.T) {
	sp := &fakeSpawner{}
	s := New(Config{Factory: hostFactory(sp), Restart: fastConfig()})

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop on a stopped supervisor: %v", err)
	}
}
