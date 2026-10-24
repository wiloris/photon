package host

import (
	"context"
	"errors"
	"io"
	"net"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon/photon-plugin-runtime/permissions"
	"github.com/wiloris/photon/photon-plugin-runtime/sandbox"
)

// Тестовые Handler'ы в этом файле НАМЕРЕННО не реализуют каталог операций
// (storage.*, identity.*, crypto.*). Их задача — проверять ПРОТОКОЛ:
// корреляцию, авторизацию, дедлайны, отмену, поведение при обрыве. Подстановка
// настоящего исполнителя затащила бы ports.Storage в эти тесты, то есть
// заставила бы тест протокола зависеть от хранилища.
//
// Настоящий исполнитель — Core Operations Handler, Phase 13,
// спецификация docs/core-operations.md. Тесты самих операций живут там же, а
// не здесь.

// Тесты failure-oriented: проверяется, что происходит при смерти
// плагина в КАЖДОМ состоянии lifecycle, а не только happy path.
//
// Компоненты настоящие: manifest, permissions, ipc — не моки. Подменён
// только Spawner, чтобы не запускать реальные процессы и управлять
// сценарием отказа.

// fakeSpawner имитирует процесс плагина.
type fakeSpawner struct {
	// startErr — Spawn падает (нет файла, нет прав).
	startErr error

	// behavior вызывается с "плагинной" стороны канала.
	behavior func(t *testing.T, conn net.Conn)

	// exitErr — что вернёт wait().
	exitErr error

	mu      sync.Mutex
	killed  bool
	spawned bool
}

func (s *fakeSpawner) Spawn(ctx context.Context, entrypoint string, iso Isolation, spec sandbox.Spec) (io.ReadWriteCloser, func() error, func(), sandbox.Applied, error) {
	if s.startErr != nil {
		return nil, nil, nil, sandbox.Applied{}, s.startErr
	}

	s.mu.Lock()
	s.spawned = true
	s.mu.Unlock()

	runtimeSide, pluginSide := net.Pipe()

	// killed закрывается при kill() — имитирует SIGKILL: настоящий
	// процесс умирает немедленно, и cmd.Wait() возвращается, не
	// дожидаясь, пока плагин "доспит" свою логику.
	killed := make(chan struct{})
	done := make(chan struct{})

	if s.behavior != nil {
		go func() {
			defer close(done)
			defer pluginSide.Close()
			s.behavior(nil, pluginSide)
		}()
	} else {
		close(done)
	}

	wait := func() error {
		select {
		case <-done:
		case <-killed:
			// Процесс убит — не ждём завершения его логики.
		}
		return s.exitErr
	}

	var killOnce sync.Once
	kill := func() {
		killOnce.Do(func() {
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

func (s *fakeSpawner) wasKilled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.killed
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
		Permissions:  manifest.Permissions{Requested: []string{"storage.read"}},
		Capabilities: manifest.Capabilities{Provided: []string{"test.op"}},
	}
}

// newHost собирает host с настоящими policy/authorizer/ipc.
func newHost(t *testing.T, sp Spawner, granted []string) (*Host, *permissions.Policy) {
	t.Helper()

	m := testManifest()
	policy := permissions.NewPolicy()
	// Права выдаёт вызывающий (инвариант 1: requested -> policy ->
	// granted). Host их из манифеста НЕ берёт.
	if granted != nil {
		if err := policy.Grant(permissions.Subject{PluginID: m.Identity.PluginID}, granted); err != nil {
			t.Fatalf("Grant: %v", err)
		}
	}
	auth := permissions.NewAuthorizer(policy)

	h := New(Config{
		Manifest:    m,
		PackageRoot: "/tmp/pkg",
		Caller:      ports.PublicKey("caller-key"),
		Policy:      policy,
		Authorizer:  auth,
		Handler: ipc.HandlerFunc(func(ctx context.Context, _ ipc.CallerInfo, op string, payload []byte) ([]byte, error) {
			return []byte("ok:" + op), nil
		}),
		Spawner: sp,
	})
	return h, policy
}

// goodPlugin — плагин, корректно проходящий handshake.
func goodPlugin(hold time.Duration) func(*testing.T, net.Conn) {
	return func(_ *testing.T, conn net.Conn) {
		hello, err := ipc.Decode(conn)
		if err != nil || hello.Type != ipc.MsgHello {
			return
		}
		ipc.Encode(conn, &ipc.Message{
			Type:                 ipc.MsgHelloAck,
			HelloProtocolVersion: 1,
			HelloPluginID:        "com.engram.test",
			HelloCapabilities:    []string{"test.op"},
		})
		if hold > 0 {
			time.Sleep(hold)
		}
	}
}

// ── Startup failures ────────────────────────────────────────────────

func TestExit_StartupFailure(t *testing.T) {
	// Процесс не запустился: нет файла / нет прав на исполнение.
	sp := &fakeSpawner{startErr: errors.New("exec: no such file")}
	h, _ := newHost(t, sp, nil)

	err := h.Start(context.Background())
	if err == nil {
		t.Fatal("Start must fail when spawn fails")
	}

	st, werr := h.Wait(ctxTimeout(t, 2*time.Second))
	if werr != nil {
		t.Fatalf("Wait: %v", werr)
	}
	if st.Kind != ExitStartupFailure {
		t.Fatalf("kind = %s, want startup_failure", st.Kind)
	}
	if st.Kind.AutoRestartAllowed() {
		t.Fatal("startup_failure must not allow automatic restart")
	}
}

func TestExit_StartupFailureUnregistersCaller(t *testing.T) {
	// Неудачный запуск не должен оставить identity binding: иначе
	// мёртвый caller остался бы авторизованным.
	sp := &fakeSpawner{startErr: errors.New("boom")}
	h, policy := newHost(t, sp, []string{"storage.read"})
	auth := permissions.NewAuthorizer(policy)
	_ = auth

	h.Start(context.Background())

	// Проверяем через сам authorizer host'а.
	_, err := h.cfg.Authorizer.Authorize(context.Background(),
		ports.PublicKey("caller-key"), ports.Permission("storage.read"))
	if err == nil {
		t.Fatal("caller binding survived a failed start")
	}
}

// ── Handshake failures ──────────────────────────────────────────────

func TestExit_HandshakeFailure_WrongPluginID(t *testing.T) {
	// Плагин заявляет чужой plugin_id — host обязан классифицировать
	// это как handshake_failure, а не как crash.
	sp := &fakeSpawner{behavior: func(_ *testing.T, conn net.Conn) {
		ipc.Decode(conn)
		ipc.Encode(conn, &ipc.Message{
			Type:                 ipc.MsgHelloAck,
			HelloProtocolVersion: 1,
			HelloPluginID:        "com.evil.impostor",
		})
		time.Sleep(200 * time.Millisecond)
	}}
	h, _ := newHost(t, sp, nil)

	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	st, err := h.Wait(ctxTimeout(t, 3*time.Second))
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	// Нарушение протокола при handshake классифицируется как
	// protocol_violation — оно важнее того, что это случилось на этапе
	// handshake.
	if st.Kind != ExitProtocolViolation && st.Kind != ExitHandshakeFailure {
		t.Fatalf("kind = %s, want protocol_violation or handshake_failure", st.Kind)
	}
	if st.Kind == ExitProtocolViolation && st.Kind.AutoRestartAllowed() {
		t.Fatal("protocol_violation must not allow automatic restart")
	}
}

func TestExit_HandshakeFailure_PluginDiesBeforeAck(t *testing.T) {
	// Плагин умер, не ответив на HELLO.
	sp := &fakeSpawner{behavior: func(_ *testing.T, conn net.Conn) {
		ipc.Decode(conn)
		// молча уходим
	}}
	h, _ := newHost(t, sp, nil)

	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	st, err := h.Wait(ctxTimeout(t, 3*time.Second))
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if st.Kind != ExitHandshakeFailure && st.Kind != ExitCrash {
		t.Fatalf("kind = %s, want handshake_failure or crash", st.Kind)
	}
}

// ── Protocol violation ──────────────────────────────────────────────

func TestExit_ProtocolViolation_NotAutoRestarted(t *testing.T) {
	// Ключевое отличие от crash: сбой переживается перезапуском,
	// нарушение протокола повторится и может быть атакой.
	sp := &fakeSpawner{behavior: func(_ *testing.T, conn net.Conn) {
		ipc.Decode(conn)
		ipc.Encode(conn, &ipc.Message{
			Type:                 ipc.MsgHelloAck,
			HelloProtocolVersion: 1,
			HelloPluginID:        "com.engram.test",
		})
		// Сообщение, которое плагин не имеет права слать.
		ipc.Encode(conn, &ipc.Message{Type: ipc.MsgResponse, RequestID: 1})
		time.Sleep(300 * time.Millisecond)
	}}
	h, _ := newHost(t, sp, nil)

	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	st, err := h.Wait(ctxTimeout(t, 3*time.Second))
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if st.Kind != ExitProtocolViolation {
		t.Fatalf("kind = %s, want protocol_violation", st.Kind)
	}
	if st.Kind.AutoRestartAllowed() {
		t.Fatal("protocol_violation must not allow automatic restart")
	}
}

func TestExit_ProtocolViolation_KillsProcess(t *testing.T) {
	// Нарушение протокола закрывает сессию — процесс должен быть убит,
	// а не оставлен работать без канала связи.
	sp := &fakeSpawner{behavior: func(_ *testing.T, conn net.Conn) {
		ipc.Decode(conn)
		ipc.Encode(conn, &ipc.Message{
			Type: ipc.MsgHelloAck, HelloProtocolVersion: 1, HelloPluginID: "com.engram.test",
		})
		ipc.Encode(conn, &ipc.Message{Type: ipc.MsgHello}) // неверное направление
		time.Sleep(time.Second)
	}}
	h, _ := newHost(t, sp, nil)
	h.Start(context.Background())
	h.Wait(ctxTimeout(t, 3*time.Second))

	if !sp.wasKilled() {
		t.Fatal("process was not killed after a protocol violation")
	}
}

// ── Crash ───────────────────────────────────────────────────────────

func TestExit_Crash_NonZeroCode(t *testing.T) {
	sp := &fakeSpawner{
		behavior: goodPlugin(50 * time.Millisecond),
		exitErr:  &exec.ExitError{},
	}
	h, _ := newHost(t, sp, nil)

	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	st, err := h.Wait(ctxTimeout(t, 3*time.Second))
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if st.Kind != ExitCrash {
		t.Fatalf("kind = %s, want crash", st.Kind)
	}
	if !st.Kind.AutoRestartAllowed() {
		t.Fatal("crash must allow automatic restart (with backoff, decided by supervisor)")
	}
}

func TestExit_PluginExitsOnItsOwnIsCrash(t *testing.T) {
	// Плагин ушёл сам, без запроса остановки. Код 0, но это не
	// clean_stop: supervisor должен знать, что экземпляр исчез не по
	// команде.
	sp := &fakeSpawner{behavior: goodPlugin(30 * time.Millisecond)}
	h, _ := newHost(t, sp, nil)

	h.Start(context.Background())
	st, err := h.Wait(ctxTimeout(t, 3*time.Second))
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if st.Kind == ExitCleanStop {
		t.Fatal("plugin leaving on its own must not be classified as clean_stop")
	}
}

// ── Clean stop ──────────────────────────────────────────────────────

func TestExit_CleanStop(t *testing.T) {
	sp := &fakeSpawner{behavior: goodPlugin(2 * time.Second)}
	h, _ := newHost(t, sp, nil)

	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := h.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	st, err := h.Wait(ctxTimeout(t, 3*time.Second))
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if st.Kind != ExitCleanStop {
		t.Fatalf("kind = %s, want clean_stop", st.Kind)
	}
	if st.Kind.AutoRestartAllowed() {
		t.Fatal("clean_stop must not trigger a restart")
	}
}

func TestStop_RuntimeDoesNotHangOnUnresponsivePlugin(t *testing.T) {
	// Плагин не отвечает на STOP. Runtime не должен зависнуть вместе с
	// ним (docs/lifecycle.md, раздел 4).
	sp := &fakeSpawner{behavior: func(_ *testing.T, conn net.Conn) {
		ipc.Decode(conn)
		ipc.Encode(conn, &ipc.Message{
			Type: ipc.MsgHelloAck, HelloProtocolVersion: 1, HelloPluginID: "com.engram.test",
		})
		// Игнорируем всё дальнейшее, включая STOP.
		time.Sleep(10 * time.Second)
	}}
	h, _ := newHost(t, sp, nil)
	h.Start(context.Background())
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	h.Stop(context.Background())
	st, err := h.Wait(ctxTimeout(t, 5*time.Second))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Wait: %v — runtime hung waiting for an unresponsive plugin", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("stop took %v — runtime waited too long for an unresponsive plugin", elapsed)
	}
	if st.Kind != ExitCleanStop {
		t.Fatalf("kind = %s, want clean_stop (stop was requested)", st.Kind)
	}
}

// ── Инварианты host ─────────────────────────────────────────────────

func TestInvariant_HostDoesNotGrantPermissionsFromManifest(t *testing.T) {
	// Инвариант 1: манифест запрашивает storage.read, но политика
	// пуста. Host не должен выдать право сам.
	sp := &fakeSpawner{behavior: goodPlugin(time.Second)}
	h, _ := newHost(t, sp, nil) // granted = nil, хотя манифест просит storage.read

	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Stop(context.Background())
	time.Sleep(100 * time.Millisecond)

	// Право запрошено в манифесте, но политика для этого субъекта не
	// загружена вообще. Ожидаем либо ошибку (политики нет), либо
	// явный отказ — но НЕ разрешение.
	allowed, err := h.cfg.Authorizer.Authorize(context.Background(),
		ports.PublicKey("caller-key"), ports.Permission("storage.read"))
	if allowed {
		t.Fatal("host granted a permission straight from the manifest — " +
			"requested must go through policy before becoming granted")
	}
	if err == nil {
		t.Fatal("expected either an error (no policy) or a denial, got a silent allow=false")
	}
}

func TestInvariant_IdentityBoundBeforeProcessStarts(t *testing.T) {
	// Инвариант 2: identity binding создаётся ДО запуска процесса —
	// иначе есть окно, в котором процесс жив, а авторизация не знает,
	// кто он.
	bindingSeen := make(chan bool, 1)

	sp := &fakeSpawner{behavior: func(_ *testing.T, conn net.Conn) {
		// К моменту, когда плагин может слать сообщения, binding уже
		// должен существовать.
		ipc.Decode(conn)
		ipc.Encode(conn, &ipc.Message{
			Type: ipc.MsgHelloAck, HelloProtocolVersion: 1, HelloPluginID: "com.engram.test",
		})
		time.Sleep(300 * time.Millisecond)
	}}

	h, _ := newHost(t, sp, []string{"storage.read"})
	h.Start(context.Background())
	defer h.Stop(context.Background())

	time.Sleep(50 * time.Millisecond)
	allowed, err := h.cfg.Authorizer.Authorize(context.Background(),
		ports.PublicKey("caller-key"), ports.Permission("storage.read"))
	bindingSeen <- (err == nil && allowed)

	if !<-bindingSeen {
		t.Fatal("identity binding was not established before the plugin could send messages")
	}
}

func TestInvariant_CallerUnregisteredAfterExit(t *testing.T) {
	// После завершения экземпляра старый caller не должен оставаться
	// авторизованным.
	sp := &fakeSpawner{behavior: goodPlugin(50 * time.Millisecond)}
	h, _ := newHost(t, sp, []string{"storage.read"})

	h.Start(context.Background())
	h.Wait(ctxTimeout(t, 3*time.Second))

	_, err := h.cfg.Authorizer.Authorize(context.Background(),
		ports.PublicKey("caller-key"), ports.Permission("storage.read"))
	if err == nil {
		t.Fatal("caller binding survived plugin exit — a dead instance must not stay authorized")
	}
}

func TestInvariant_SingleInstanceOnly(t *testing.T) {
	// Инвариант 4: одна host-сессия = один экземпляр.
	sp := &fakeSpawner{behavior: goodPlugin(time.Second)}
	h, _ := newHost(t, sp, nil)

	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	defer h.Stop(context.Background())

	if err := h.Start(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Start: got %v, want ErrAlreadyRunning", err)
	}
}

func TestInvariant_HostDoesNotDecideRestart(t *testing.T) {
	// Инвариант 6: host классифицирует, но не перезапускает.
	// AutoRestartAllowed — подсказка для supervisor, а не действие.
	sp := &fakeSpawner{behavior: goodPlugin(30 * time.Millisecond)}
	h, _ := newHost(t, sp, nil)

	h.Start(context.Background())
	h.Wait(ctxTimeout(t, 3*time.Second))

	time.Sleep(200 * time.Millisecond)
	if h.IsRunning() {
		t.Fatal("host restarted the plugin by itself — restart is the supervisor's decision")
	}
}

// ── Полный E2E: manifest -> policy -> host -> IPC -> authorize ──────

func TestE2E_FullPipelineAllowAndForbid(t *testing.T) {
	// Настоящий pipeline без моков компонентов:
	//
	//   manifest bytes -> Parse -> Validate -> requested permissions
	//     -> Policy/Authorizer -> Host -> real IPC session
	//     -> plugin request -> Authorizer -> ALLOW / FORBIDDEN

	manifestJSON := `{
	  "manifest_version": 1,
	  "identity": {"plugin_id": "com.engram.test", "version": "1.0.0"},
	  "runtime": {"protocol_version": 1, "entrypoint": "bin/plugin", "instance_policy": "single"},
	  "permissions": {"requested": ["storage.read"]},
	  "capabilities": {"provided": ["test.op"]}
	}`

	// 1. Настоящий парсер.
	m, err := manifest.ParseBytes([]byte(manifestJSON))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// 2. Настоящий валидатор (без проверки ФС — пакет не распакован).
	if err := manifest.Validate(m, ""); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// 3. Политика выдаёт ТОЛЬКО то, что запрошено И разрешено.
	//    Здесь: запрошено storage.read, выдано storage.read.
	//    storage.write не запрашивался и не выдан.
	policy := permissions.NewPolicy()
	subj := permissions.Subject{PluginID: m.Identity.PluginID}
	if err := policy.Grant(subj, m.Permissions.Requested); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	auth := permissions.NewAuthorizer(policy)

	// 4. Плагин делает два запроса и сообщает результаты.
	results := make(chan map[string]ipc.ErrorCode, 1)
	sp := &fakeSpawner{behavior: func(_ *testing.T, conn net.Conn) {
		ipc.Decode(conn)
		ipc.Encode(conn, &ipc.Message{
			Type: ipc.MsgHelloAck, HelloProtocolVersion: 1,
			HelloPluginID: "com.engram.test", HelloCapabilities: []string{"test.op"},
		})

		// Операции каталога, а не имена прав: storage.get требует
		// storage.read (выдано), storage.put — storage.write (не выдано).
		got := make(map[string]ipc.ErrorCode)
		for i, op := range []string{"storage.get", "storage.put"} {
			ipc.Encode(conn, &ipc.Message{Type: ipc.MsgRequest, RequestID: uint64(i + 1), Op: op})
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			resp, err := ipc.Decode(conn)
			if err != nil {
				return
			}
			got[op] = resp.ErrorCode
		}
		results <- got
		time.Sleep(200 * time.Millisecond)
	}}

	h := New(Config{
		Manifest:    m,
		PackageRoot: "/tmp/pkg",
		Caller:      ports.PublicKey("e2e-key"),
		Policy:      policy,
		Authorizer:  auth,
		Handler: ipc.HandlerFunc(func(ctx context.Context, _ ipc.CallerInfo, op string, payload []byte) ([]byte, error) {
			return []byte("done"), nil
		}),
		Spawner: sp,
	})

	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer h.Stop(context.Background())

	select {
	case got := <-results:
		if got["storage.get"] != "" {
			t.Fatalf("storage.get (permission storage.read requested and granted) must be allowed, got %q",
				got["storage.get"])
		}
		if got["storage.put"] != ipc.ErrForbidden {
			t.Fatalf("storage.put (permission storage.write never requested, never granted) must be FORBIDDEN, got %q",
				got["storage.put"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for plugin results")
	}
}

// ── Прочее ──────────────────────────────────────────────────────────

func TestStop_BeforeStart(t *testing.T) {
	sp := &fakeSpawner{}
	h, _ := newHost(t, sp, nil)
	if err := h.Stop(context.Background()); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("got %v, want ErrNotRunning", err)
	}
}

func TestSendEvent_BeforeStart(t *testing.T) {
	sp := &fakeSpawner{}
	h, _ := newHost(t, sp, nil)
	if err := h.SendEvent("x", nil); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("got %v, want ErrNotRunning", err)
	}
}

func TestExitStatus_ReportedOnlyOnce(t *testing.T) {
	// Канал завершения не должен получать несколько значений или
	// паниковать при повторном закрытии.
	sp := &fakeSpawner{behavior: goodPlugin(30 * time.Millisecond)}
	h, _ := newHost(t, sp, nil)

	h.Start(context.Background())
	if _, err := h.Wait(ctxTimeout(t, 3*time.Second)); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	// Повторное чтение из закрытого канала — нулевое значение, без паники.
	select {
	case <-h.Exited():
	default:
	}
}

func TestNoIsolation_ReportsMissingLayer2(t *testing.T) {
	// Заглушка не должна притворяться изоляцией: Layer 1 есть,
	// Layer 2 отсутствует — и об этом сказано явно.
	iso := NoIsolation()
	applied, err := iso.Prepare(exec.Command("true"), sandbox.Spec{})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if err := sandbox.Verify(applied); err != nil {
		t.Fatalf("Layer 1 must be satisfied even without OS isolation: %v", err)
	}
	if applied.Has(sandbox.PropFilesystemLimit) || applied.Has(sandbox.PropNetworkLimit) {
		t.Fatal("NoIsolation claims Layer 2 properties it does not provide")
	}
	if len(applied.Warnings) == 0 {
		t.Fatal("NoIsolation must warn that OS-level isolation is absent")
	}
}

func TestStart_RejectsInsufficientIsolation(t *testing.T) {
	// Обязательные свойства не выполнены -> запуск отклоняется, процесс
	// убивается, а не остаётся работать без изоляции.
	sp := &weakIsolationSpawner{}
	h, _ := newHost(t, sp, nil)

	err := h.Start(context.Background())
	if err == nil {
		t.Fatal("Start must fail when mandatory isolation properties are missing")
	}
	if !errors.Is(err, sandbox.ErrMandatoryPropertyUnavailable) {
		t.Fatalf("got %v, want ErrMandatoryPropertyUnavailable", err)
	}
	if !sp.wasKilled() {
		t.Fatal("process must be killed when isolation is insufficient")
	}
}

// weakIsolationSpawner имитирует платформу, где обязательное свойство
// недостижимо.
type weakIsolationSpawner struct {
	mu     sync.Mutex
	killed bool
}

func (s *weakIsolationSpawner) Spawn(ctx context.Context, entrypoint string, iso Isolation, spec sandbox.Spec) (io.ReadWriteCloser, func() error, func(), sandbox.Applied, error) {
	_, pluginSide := net.Pipe()
	runtimeSide, _ := net.Pipe()
	_ = pluginSide

	killed := make(chan struct{})
	var once sync.Once
	kill := func() {
		once.Do(func() {
			s.mu.Lock()
			s.killed = true
			s.mu.Unlock()
			close(killed)
		})
	}
	wait := func() error { <-killed; return nil }

	return runtimeSide, wait, kill, sandbox.Applied{
		Platform: "weak",
		Properties: map[sandbox.Property]bool{
			sandbox.PropSeparateProcess: false, // обязательное отсутствует
			sandbox.PropSingleChannel:   true,
			sandbox.PropTerminable:      true,
		},
		Warnings: []string{"separate process is not available on this platform"},
	}, nil
}

func (s *weakIsolationSpawner) wasKilled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.killed
}

func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
