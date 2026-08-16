package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon-plugin-runtime/permissions"
	"github.com/wiloris/photon-plugin-runtime/sandbox"
)

// Host связывает уже проверенные компоненты в жизненный цикл ОДНОГО
// экземпляра плагина:
//
//	Manifest (validated) + Permissions (policy) + IPC (session)
//	                      -> plugin process
//
// Host НЕ является ещё одним security-механизмом. Он не проверяет права
// сам, не разбирает манифест заново и не решает судьбу упавшего
// процесса — только собирает готовые механизмы вместе и корректно
// связывает идентичность.
//
// Что host сознательно НЕ делает:
//   - не выдаёт права из манифеста (инвариант 1: requested -> policy -> granted);
//   - не доверяет plugin_id из IPC (инвариант 3: передаёт установленную identity);
//   - не решает о перезапуске (инвариант 6: это supervisor);
//   - не содержит sandbox-кода (инвариант 8: запрашивает изоляцию, не реализует).

var (
	ErrAlreadyRunning = errors.New("host: plugin instance is already running")
	ErrNotRunning     = errors.New("host: plugin instance is not running")
)

// gracefulStopTimeout — сколько ждём доставки STOP плагину, прежде чем
// перейти к принудительному завершению.
//
// Короткий: это не время на завершение работы плагина, а лишь время на
// ДОСТАВКУ сообщения. Плагин, не читающий свой канал, не станет читать
// его и через минуту.
const gracefulStopTimeout = 500 * time.Millisecond

// gracefulExitTimeout — сколько ждём, что процесс завершится САМ после
// того, как STOP доставлен, прежде чем убивать его.
//
// Отдельный таймаут от gracefulStopTimeout не случайно: доставка
// сообщения и обработка плагином — разные по природе события.
// STOP_ACK на проводе означает лишь то, что плагин ЗАПИСАЛ ответ, а не
// то, что процесс уже вызвал exit — это два разных момента времени,
// docs/lifecycle.md §4 требует дать процессу управляемое время на
// второе, не только на первое.
//
// Ограничен по той же причине, что и gracefulStopTimeout: неотвечающий
// или зависший после STOP_ACK процесс не должен блокировать Stop
// (см. TestStop_RuntimeDoesNotHangOnUnresponsivePlugin) — по истечении
// вызывающий код обязан перейти к kill().
const gracefulExitTimeout = 2 * time.Second

// Isolation — псевдоним sandbox.Sandbox.
//
// Host запрашивает изоляцию, но не реализует её: механизмы
// (namespaces, отдельный пользователь, Android-специфика) живут в
// пакете sandbox (инвариант 8, docs/sandbox.md раздел 1).
//
// Важно, что интерфейс возвращает sandbox.Applied, а не просто error:
// host обязан знать, какие свойства РЕАЛЬНО применены, иначе честная
// деградация (docs/sandbox.md, раздел 6) невозможна — «изоляция
// применена» было бы неотличимо от «изоляция пропущена молча».
type Isolation = sandbox.Sandbox

// NoIsolation возвращает реализацию без OS-изоляции — для разработки и
// тестов. Имя говорит прямо, что защиты нет: нейтральное название
// создавало бы ложное впечатление защищённости.
func NoIsolation() Isolation { return sandbox.NewNone() }

// Config — параметры одного экземпляра плагина.
type Config struct {
	// Manifest — УЖЕ провалидированный манифест. Host не валидирует
	// его повторно: это ответственность manifest-пакета, и дублирование
	// проверки создало бы два места, где определяется допустимость.
	Manifest *manifest.Manifest

	// PackageRoot — корень установленного пакета.
	PackageRoot string

	// Caller — публичный ключ этого экземпляра плагина.
	//
	// Идентичность устанавливает Runtime ДО запуска процесса. Плагин в
	// этом не участвует и подделать её не может: IPC-сессия получает
	// именно это значение, а не то, что плагин пришлёт в сообщении.
	Caller ports.PublicKey

	// Policy — политика прав. Host не выдаёт права сам.
	Policy *permissions.Policy

	// Authorizer — проверка прав на каждый IPC-запрос.
	Authorizer *permissions.Authorizer

	// Handler — исполнение разрешённых операций.
	Handler ipc.Handler

	// Isolation — запрашиваемая изоляция. nil означает NoIsolation().
	Isolation Isolation

	// Spawner — как запускать процесс. nil означает реальный exec.
	// Подменяется в тестах, чтобы проверять lifecycle без настоящих
	// процессов.
	Spawner Spawner
}

// Spawner запускает процесс плагина и даёт канал связи с ним.
type Spawner interface {
	// Spawn запускает процесс. Возвращает канал IPC, функцию ожидания
	// завершения, функцию принудительного завершения и сведения о
	// реально применённой изоляции.
	Spawn(ctx context.Context, entrypoint string, iso Isolation, spec sandbox.Spec) (conn io.ReadWriteCloser, wait func() error, kill func(), applied sandbox.Applied, err error)
}

// Host — один экземпляр плагина.
//
// Одна host-сессия = один plugin instance: multi-instance запрещён
// (docs/lifecycle.md, раздел 10), поэтому здесь нет коллекции сессий.
type Host struct {
	cfg Config

	mu      sync.Mutex
	running bool
	stopReq bool
	session *ipc.Session
	kill    func()
	waitFn  func() error
	applied sandbox.Applied

	exitOnce sync.Once
	exitCh   chan ExitStatus
}

func New(cfg Config) *Host {
	if cfg.Isolation == nil {
		cfg.Isolation = NoIsolation()
	}
	if cfg.Spawner == nil {
		cfg.Spawner = execSpawner{}
	}
	return &Host{cfg: cfg, exitCh: make(chan ExitStatus, 1)}
}

// PluginID — идентификатор плагина этого экземпляра.
func (h *Host) PluginID() string {
	return h.cfg.Manifest.Identity.PluginID
}

// Start запускает процесс плагина и IPC-сессию.
//
// Порядок операций существен:
//  1. связать identity (Caller -> Subject) ДО запуска процесса;
//  2. запустить процесс;
//  3. поднять IPC-сессию с УЖЕ установленной identity.
//
// Обратный порядок означал бы окно, в котором процесс запущен, а
// авторизация ещё не знает, кто он.
func (h *Host) Start(ctx context.Context) error {
	h.mu.Lock()
	if h.running {
		h.mu.Unlock()
		return ErrAlreadyRunning
	}
	h.running = true
	h.mu.Unlock()

	// 1. Identity binding. Права при этом НЕ выдаются: политику
	//    заполняет вызывающий (Runtime/supervisor) на основании
	//    requested_permissions манифеста, trust level и решения
	//    пользователя — инвариант 1.
	subject := permissions.Subject{PluginID: h.cfg.Manifest.Identity.PluginID}
	h.cfg.Authorizer.RegisterCaller(h.cfg.Caller, subject)

	entrypoint := filepath.Join(h.cfg.PackageRoot, h.cfg.Manifest.Runtime.Entrypoint)

	// 2. Запуск процесса с применением изоляции.
	//
	// AllowNetwork берётся из политики, а не из манифеста: манифест
	// лишь ЗАПРАШИВАЕТ право, выдаёт его Authorizer (инвариант 1).
	spec := sandbox.Spec{
		PackageRoot:  h.cfg.PackageRoot,
		AllowNetwork: h.networkGranted(ctx),
	}

	conn, wait, kill, applied, err := h.cfg.Spawner.Spawn(ctx, entrypoint, h.cfg.Isolation, spec)
	if err != nil {
		h.cfg.Authorizer.UnregisterCaller(h.cfg.Caller)
		h.mu.Lock()
		h.running = false
		h.mu.Unlock()
		h.reportExit(ExitStatus{Kind: ExitStartupFailure, Code: -1, Err: err})
		return fmt.Errorf("host: spawn: %w", err)
	}

	// Обязательные свойства изоляции проверяются ПОСЛЕ запуска, потому
	// что часть из них (отдельный процесс) становится фактом только
	// после fork. Если они не выполнены — процесс убивается, а не
	// остаётся работать без изоляции (docs/sandbox.md, раздел 6).
	if verr := sandbox.Verify(applied); verr != nil {
		kill()
		_ = wait()
		h.cfg.Authorizer.UnregisterCaller(h.cfg.Caller)
		h.mu.Lock()
		h.running = false
		h.mu.Unlock()
		h.reportExit(ExitStatus{Kind: ExitStartupFailure, Code: -1, Err: verr})
		return fmt.Errorf("host: isolation: %w", verr)
	}

	h.mu.Lock()
	h.kill = kill
	h.waitFn = wait
	h.applied = applied
	h.mu.Unlock()

	// 3. IPC-сессия с установленной identity.
	//
	//    Caller передаётся из конфигурации host, а НЕ извлекается из
	//    сообщений плагина — инвариант 3. IPC уже доказал этот принцип
	//    своими тестами; host обязан его соблюсти на своей стороне.
	session := ipc.New(ipc.Config{
		Conn:                 conn,
		Caller:               h.cfg.Caller,
		ExpectedPluginID:     h.cfg.Manifest.Identity.PluginID,
		ProtocolVersion:      h.cfg.Manifest.Runtime.ProtocolVersion,
		ExpectedCapabilities: h.cfg.Manifest.Capabilities.Provided,
		Authorizer:           h.cfg.Authorizer,
		Handler:              h.cfg.Handler,
	})

	h.mu.Lock()
	h.session = session
	h.mu.Unlock()

	go h.run(ctx, session, wait)
	return nil
}

// run обслуживает сессию и классифицирует завершение.
func (h *Host) run(ctx context.Context, session *ipc.Session, wait func() error) {
	sessErr := session.Run(ctx)

	// Процесс мог ещё не завершиться — например, при нарушении
	// протокола сессия закрывается первой. Убиваем и дожидаемся.
	//
	// Исключение: штатная остановка. readLoop возвращает nil сразу по
	// STOP_ACK (ipc/session.go, MsgStopAck) — это лишь момент, когда
	// плагин ЗАПИСАЛ ответ, не момент, когда процесс фактически вышел
	// (main() ещё разворачивается). Убить здесь немедленно значило бы
	// race с собственным exit(0) плагина, который тот почти всегда
	// проигрывал бы — ровно то, из-за чего штатная остановка получала
	// код -1 вместо 0. Пока Stop() уже идёт (stopRequested), решение о
	// том, сколько ждать перед kill(), принимает ОН — Stop() запускает
	// для этого отдельное ограниченное ожидание wait() (см. Stop);
	// дублировать его здесь означало бы гонку двух независимых kill().
	h.mu.Lock()
	kill := h.kill
	h.mu.Unlock()
	if kill != nil && !h.stopRequested() {
		kill()
	}
	waitErr := wait()

	h.cfg.Authorizer.UnregisterCaller(h.cfg.Caller)

	h.mu.Lock()
	h.running = false
	h.mu.Unlock()

	h.reportExit(classify(sessErr, waitErr, h.stopRequested()))
}

// classify определяет причину завершения.
//
// Порядок проверок отражает приоритет причин: нарушение протокола
// важнее кода возврата процесса, потому что оно объясняет, ПОЧЕМУ
// процесс был убит.
func classify(sessErr, waitErr error, stopRequested bool) ExitStatus {
	code := exitCode(waitErr)

	switch {
	case errors.Is(sessErr, ipc.ErrProtocolViolation):
		return ExitStatus{Kind: ExitProtocolViolation, Code: code, Err: sessErr}

	case isHandshakeFailure(sessErr):
		return ExitStatus{Kind: ExitHandshakeFailure, Code: code, Err: sessErr}

	case stopRequested:
		// Штатная остановка: даже если процесс пришлось добить, итог
		// ожидаемый (docs/lifecycle.md, раздел 4).
		return ExitStatus{Kind: ExitCleanStop, Code: code}

	case waitErr != nil || code != 0:
		return ExitStatus{Kind: ExitCrash, Code: code, Err: waitErr}

	case sessErr != nil && !errors.Is(sessErr, io.EOF):
		return ExitStatus{Kind: ExitCrash, Code: code, Err: sessErr}

	default:
		// Процесс завершился с кодом 0 без запроса остановки. Это не
		// crash по коду, но и не clean stop: плагин ушёл сам.
		// Классифицируем как crash — supervisor должен знать, что
		// экземпляр исчез не по команде.
		return ExitStatus{Kind: ExitCrash, Code: code, Err: sessErr}
	}
}

func isHandshakeFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return contains(msg, "HELLO") || contains(msg, "handshake")
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1
}

// Stop запрашивает штатную остановку (docs/lifecycle.md, раздел 4).
//
//	running -> stopping -> IPC closed -> process terminated
func (h *Host) Stop(ctx context.Context) error {
	h.mu.Lock()
	if !h.running {
		h.mu.Unlock()
		return ErrNotRunning
	}
	h.stopReq = true
	session := h.session
	h.mu.Unlock()

	exitedOnItsOwn := false

	if session != nil {
		// Просим завершиться штатно — но НЕ блокируемся на этом.
		//
		// Отправка STOP пишет в канал к плагину, а плагин может не
		// читать (завис, спит, игнорирует). Синхронная запись в таком
		// случае блокируется навсегда, и Runtime зависает вместе с
		// плагином — ровно то, что docs/lifecycle.md раздел 4
		// запрещает. Поэтому запрос уходит в отдельной горутине.
		//
		// Это ожидание — только про ДОСТАВКУ сообщения, не про то, что
		// плагин его обработал и тем более не про то, что процесс уже
		// вышел. Обработка ждётся отдельно, ниже.
		stopSent := make(chan struct{})
		go func() {
			defer close(stopSent)
			_ = session.Stop(ctx)
		}()

		select {
		case <-stopSent:
			// Плагин читает канал — STOP доставлен.
		case <-time.After(gracefulStopTimeout):
			// Плагин не читает. Дальше действуем принудительно.
		}

		// Второе, ОТДЕЛЬНОЕ ограниченное ожидание: дать процессу время
		// завершиться самому после STOP_ACK, прежде чем убивать его.
		//
		// STOP_ACK на проводе (readLoop, MsgStopAck) — это момент,
		// когда плагин ЗАПИСАЛ ответ, а не момент, когда его процесс
		// фактически вызвал exit: между ними есть измеримое время
		// (развернуть Run(), вернуться из main(), дать рантайму Go
		// завершить процесс). Раньше kill() шёл сразу за отправкой
		// STOP без этого ожидания вовсе — штатная остановка почти
		// всегда убивала процесс раньше, чем тот успевал выйти сам, и
		// код возврата был -1 вместо 0 (docs/test-laboratory.md,
		// "hello": "STOP приводит к штатному завершению с кодом 0").
		//
		// Ограничено по ТОЙ ЖЕ причине, что и ожидание доставки: если
		// плагин прислал STOP_ACK, но завис до реального exit (или
		// вовсе не читает канал и это ожидание истечёт как основное),
		// Stop не должен блокироваться вечно — ровно инвариант,
		// который проверяет
		// TestStop_RuntimeDoesNotHangOnUnresponsivePlugin.
		h.mu.Lock()
		waitFn := h.waitFn
		h.mu.Unlock()
		if waitFn != nil {
			exitedOnItsOwn = waitBounded(waitFn, gracefulExitTimeout)
		}

		// Закрытие сессии безопасно в обоих случаях: если процесс уже
		// вышел сам, сессия уже закрылась изнутри (readLoop получил
		// STOP_ACK/EOF) и это no-op через closeOnce; если не вышел —
		// разблокирует зависшую отправку STOP и прекращает обработку
		// запросов перед принудительным путём ниже.
		_ = session.Close()
	}

	if exitedOnItsOwn {
		// Процесс завершился сам в течение gracefulExitTimeout — убивать
		// уже нечего. Итоговое состояние всё равно clean_stop: остановка
		// была запрошена, значит результат ожидаемый.
		return nil
	}

	// И, наконец, гарантируем завершение самого процесса.
	//
	// Без этого шага плагин, игнорирующий STOP (или зависший уже после
	// STOP_ACK), оставался бы жив после закрытия IPC: Runtime не может
	// ждать его вечно, но и оставлять висящий процесс без канала связи
	// нельзя — он продолжал бы удерживать ресурсы, не имея возможности
	// что-либо сообщить. Порядок из docs/lifecycle.md раздел 4
	// выполняется целиком:
	//
	//	running -> stopping -> IPC closed -> process terminated
	//
	// Итоговое состояние всё равно clean_stop: остановка была
	// запрошена, значит результат ожидаемый.
	h.mu.Lock()
	kill := h.kill
	h.mu.Unlock()
	if kill != nil {
		kill()
	}
	return nil
}

// waitBounded ждёт завершения процесса через wait не дольше timeout.
// Возвращает true, если процесс завершился сам в течение этого времени.
//
// wait может быть вызван из нескольких мест конкурентно (Stop и run) —
// это безопасно: execSpawner оборачивает системный Wait в sync.Once,
// так что оба вызова получают один и тот же результат, а не гонку за
// значение ProcessState.
func waitBounded(wait func() error, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = wait()
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (h *Host) stopRequested() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stopReq
}

// Wait блокируется до завершения экземпляра и возвращает причину.
func (h *Host) Wait(ctx context.Context) (ExitStatus, error) {
	select {
	case st := <-h.exitCh:
		return st, nil
	case <-ctx.Done():
		return ExitStatus{}, ctx.Err()
	}
}

// Exited возвращает канал завершения — для supervisor, который следит
// за несколькими экземплярами.
func (h *Host) Exited() <-chan ExitStatus { return h.exitCh }

func (h *Host) reportExit(st ExitStatus) {
	h.exitOnce.Do(func() {
		h.exitCh <- st
		close(h.exitCh)
	})
}

// networkGranted спрашивает Authorizer, выдано ли плагину сетевое
// право.
//
// Sandbox не решает это сам: право приходит от политики, а sandbox
// лишь применяет соответствующее ограничение (docs/sandbox.md,
// раздел 5 — sandbox не подменяет authorization).
func (h *Host) networkGranted(ctx context.Context) bool {
	allowed, err := h.cfg.Authorizer.Authorize(ctx, h.cfg.Caller, ports.Permission("network.connect"))
	return err == nil && allowed
}

// Isolation возвращает сведения о применённой изоляции — для
// диагностики и Developer Tools.
func (h *Host) Isolation() sandbox.Applied {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.applied
}

// IsRunning — для диагностики и тестов.
func (h *Host) IsRunning() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.running
}

// SendEvent доставляет событие плагину.
func (h *Host) SendEvent(eventType string, payload []byte) error {
	h.mu.Lock()
	session := h.session
	running := h.running
	h.mu.Unlock()
	if !running || session == nil {
		return ErrNotRunning
	}
	return session.SendEvent(eventType, payload)
}
