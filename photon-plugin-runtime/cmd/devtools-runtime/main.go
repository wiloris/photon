// Command devtools-runtime — экземпляр Plugin Runtime, которым владеет
// Developer Tools (ADR-057).
//
// ЧТО ЭТО ТАКОЕ И ЧЕМ НЕ ЯВЛЯЕТСЯ.
//
// Это не демон и не production-хост. Процесс живёт ровно столько, сколько его
// родитель, управляется только им и обслуживает один экземпляр runtime,
// созданный для наблюдения. Composition root продакшна (cmd/photon-core-server)
// этим бинарём не решается и не приближается: вопросы core-operations.md §11 —
// источник мастер-ключа и кто зовёт Migrate — остаются открытыми.
//
// ГРАНИЦА ДОВЕРИЯ — РОДИТЕЛЬСТВО ПРОЦЕССА. Управляющий канал — stdin/stdout,
// то есть трубы, которыми владеет тот, кто этот процесс породил. Ни сокета, ни
// порта, ни файла: сторонний процесс до канала не дотягивается, и потому
// credential'а здесь нет — предъявлять его некому.
//
// УПРАВЛЯЮЩИЙ КАНАЛ — НЕ PLUGIN IPC. Механика похожа (родитель владеет трубами
// потомка), собеседник противоположен: у Plugin IPC он НЕДОВЕРЕННЫЙ, здесь —
// доверенный родитель. Поэтому кодек здесь свой, а пакет ipc не импортируется
// вовсе: объединение «ради DRY» стёрло бы разницу между двумя моделями доверия
// (ADR-057, docs/ipc.md §20).
//
// Протокол: по одному JSON-объекту в строке в обе стороны. stdout занят
// протоколом целиком; всё человекочитаемое уходит в stderr, иначе первая же
// отладочная строка сломала бы разбор у родителя.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/host"
	"github.com/wiloris/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon-plugin-runtime/permissions"
)

// Фикстура вертикального среза.
//
// Идентичность и права заданы ЗДЕСЬ, а не приходят снаружи: произвольная
// загрузка внешних пакетов в срез не входит (ADR-057, «что остаётся
// нерешённым»). Параметром передаётся только путь к уже собранному бинарю
// фикстуры — сборкой занимается тот, кто нас запускает.
const (
	fixturePluginID = "com.engram.test.hello"
	fixtureCaller   = "devtools-lab-instance"
	fixtureGrant    = "storage.read"
	fixtureValue    = "hello-value"
)

var (
	pluginPath = flag.String("plugin", "",
		"путь к собранному бинарю тестового плагина (обязателен)")
	stopTimeout = flag.Duration("stop-timeout", 5*time.Second,
		"предел ожидания остановки плагина")
)

// ── управляющий протокол ─────────────────────────────────────────────────────

type request struct {
	ID  int    `json:"id"`
	Cmd string `json:"cmd"`
}

type response struct {
	ID    int    `json:"id"`
	OK    bool   `json:"ok"`
	Cmd   string `json:"cmd"`
	Error string `json:"error,omitempty"`

	// Полезная нагрузка ответов. Поля необязательные: у каждой команды
	// заполняются свои, остальные опускаются.
	RuntimeState string       `json:"runtime_state,omitempty"`
	PluginID     string       `json:"plugin_id,omitempty"`
	PluginState  string       `json:"plugin_state,omitempty"`
	Granted      []string     `json:"granted,omitempty"`
	Isolation    *isolationV  `json:"isolation,omitempty"`
	Operations   []operationV `json:"operations,omitempty"`
}

type isolationV struct {
	Platform   string          `json:"platform"`
	Properties map[string]bool `json:"properties"`
	Warnings   []string        `json:"warnings,omitempty"`
}

// operationV — операция, которую плагин ВЫПОЛНИЛ.
//
// Достигнутый Handler означает, что Authorizer уже разрешил операцию: проверка
// прав стоит в сессии ДО вызова исполнителя (ipc/session.go). Поэтому поле
// называется authorized и означает ровно наблюдённый факт, а не нашу оценку.
type operationV struct {
	Op         string `json:"op"`
	Authorized bool   `json:"authorized"`
	At         string `json:"at"`
}

// ── контроллер ───────────────────────────────────────────────────────────────

type controller struct {
	mu     sync.Mutex
	h      *host.Host
	policy *permissions.Policy
	ops    []operationV
	loaded bool

	// cancel — отмена контекста ЖИЗНИ плагина, а не запуска.
	//
	// Найдено первым же прогоном: host.Start(ctx) удерживает переданный
	// контекст на всё время сессии (host.go: `go h.run(ctx, session,
	// wait)`), а не только на время запуска. Контекст с таймаутом,
	// отменённый сразу после возврата Start — естественное прочтение
	// «дедлайн запуска», — тихо роняет сессию: плагин стартует, теряет
	// канал и выходит. В doc-комментарии Start это свойство не описано.
	cancel context.CancelFunc
}

func (c *controller) record(op string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ops = append(c.ops, operationV{
		Op: op, Authorized: true, At: time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (c *controller) snapshot() []operationV {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]operationV(nil), c.ops...)
}

// handler — исполнитель операций плагина.
//
// Минимальный намеренно: срез наблюдает жизненный цикл и путь авторизации, а
// не хранилище. Неизвестная операция получает UNSUPPORTED — тот же различимый
// исход, что и у настоящего исполнителя (ADR-032).
func (c *controller) handler() ipc.Handler {
	return ipc.HandlerFunc(func(_ context.Context, _ ipc.CallerInfo, op string, _ []byte) ([]byte, error) {
		c.record(op)
		switch op {
		case "storage.get":
			return json.Marshal(map[string]any{"value": []byte(fixtureValue)})
		case "log.write":
			return json.Marshal(map[string]any{})
		default:
			return nil, ipc.NewOpError(ipc.ErrUnsupported, "operation not served by devtools runtime")
		}
	})
}

// load запускает плагин. Контекст ЗАПУСКА передаётся вызывающим и здесь не
// используется как контекст сессии — см. поле cancel.
func (c *controller) load(startCtx context.Context) error {
	c.mu.Lock()
	if c.loaded {
		c.mu.Unlock()
		return errors.New("plugin already loaded")
	}
	c.mu.Unlock()

	m := &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: fixturePluginID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      fixtureEntrypoint(*pluginPath),
			InstancePolicy:  manifest.InstanceSingle,
		},
		Permissions: manifest.Permissions{Requested: []string{fixtureGrant}},
	}

	policy := permissions.NewPolicy()
	// Grant допустим ЗДЕСЬ и только здесь: политика принадлежит экземпляру,
	// который мы сами создали (ADR-057 §7). Наружу — в управляющий канал, в
	// C ABI, в сеть — эта операция не выводится.
	if err := policy.Grant(permissions.Subject{PluginID: fixturePluginID}, []string{fixtureGrant}); err != nil {
		return fmt.Errorf("grant: %w", err)
	}

	h := host.New(host.Config{
		Manifest:    m,
		PackageRoot: fixtureRoot(*pluginPath),
		Caller:      ports.PublicKey(fixtureCaller),
		Policy:      policy,
		Authorizer:  permissions.NewAuthorizer(policy),
		Handler:     c.handler(),
	})
	// Контекст сессии живёт до stop(). Дедлайн запуска обеспечивается тем,
	// что Start блокируется до завершения handshake, а вызывающий ограничен
	// своим startCtx: если он истёк, load даже не начнётся.
	if err := startCtx.Err(); err != nil {
		return err
	}
	lifetime, cancel := context.WithCancel(context.Background())
	if err := h.Start(lifetime); err != nil {
		cancel()
		return fmt.Errorf("start plugin: %w", err)
	}

	c.mu.Lock()
	c.h, c.policy, c.loaded, c.cancel = h, policy, true, cancel
	c.mu.Unlock()
	return nil
}

// stop останавливает плагин. Идемпотентна: вызывается и по команде, и по
// закрытию управляющего канала, и порядок этих двух событий не определён.
func (c *controller) stop() {
	c.mu.Lock()
	h, cancelLifetime := c.h, c.cancel
	c.h, c.loaded, c.cancel = nil, false, nil
	c.mu.Unlock()
	if h == nil {
		return
	}
	// Сначала штатная остановка: Stop доводит STOP/STOP_ACK и дожидается
	// выхода процесса. Контекст сессии отменяется ПОСЛЕ — отмена вперёд
	// оборвала бы канал раньше, чем плагин успел ответить, и чистый выход
	// стал бы аварийным.
	ctx, cancel := context.WithTimeout(context.Background(), *stopTimeout)
	defer cancel()
	if err := h.Stop(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "[devtools-runtime] остановка плагина: %v\n", err)
	}
	if cancelLifetime != nil {
		cancelLifetime()
	}
}

func (c *controller) diagnose(r *response) {
	c.mu.Lock()
	h, policy := c.h, c.policy
	c.mu.Unlock()

	r.PluginID = fixturePluginID
	r.Operations = c.snapshot()
	if h == nil {
		r.PluginState = "not loaded"
		return
	}
	if h.IsRunning() {
		r.PluginState = "running"
	} else {
		r.PluginState = "stopped"
	}
	applied := h.Isolation()
	props := make(map[string]bool, len(applied.Properties))
	for k, v := range applied.Properties {
		props[string(k)] = v
	}
	r.Isolation = &isolationV{Platform: applied.Platform, Properties: props, Warnings: applied.Warnings}
	if policy != nil {
		if granted, err := policy.GrantedPermissions(permissions.Subject{PluginID: fixturePluginID}); err == nil {
			r.Granted = granted
		}
	}
}

// ── цикл управления ──────────────────────────────────────────────────────────

func main() {
	flag.Parse()
	if *pluginPath == "" {
		fmt.Fprintln(os.Stderr, "нужен -plugin <путь к бинарю тестового плагина>")
		os.Exit(2)
	}

	c := &controller{}
	out := json.NewEncoder(os.Stdout)
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 1<<20)

	// Первая строка — готовность. Родителю не приходится гадать, поднялись мы
	// или нет: он читает её и только потом шлёт команды.
	_ = out.Encode(response{OK: true, Cmd: "ready", RuntimeState: "ready"})

	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			_ = out.Encode(response{OK: false, Cmd: "?", Error: "invalid control frame: " + err.Error()})
			continue
		}

		resp := response{ID: req.ID, Cmd: req.Cmd, OK: true, RuntimeState: "ready"}
		switch req.Cmd {
		case "status":
			// Ничего не меняет: чистая диагностика.

		case "load":
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := c.load(ctx)
			cancel()
			if err != nil {
				resp.OK, resp.Error = false, err.Error()
			} else {
				resp.PluginID, resp.PluginState = fixturePluginID, "running"
			}

		case "diagnose":
			c.diagnose(&resp)

		case "shutdown":
			c.stop()
			resp.RuntimeState = "stopped"
			_ = out.Encode(resp)
			return

		default:
			resp.OK, resp.Error = false, "unknown command: "+req.Cmd
		}
		_ = out.Encode(resp)
	}

	// СЮДА ПРИХОДИТ ОТСУТСТВИЕ РОДИТЕЛЯ.
	//
	// Закрытый stdin означает, что родитель ушёл — штатно или нет. Оставить
	// после себя работающий runtime с живыми плагинами и без владельца
	// нельзя: он пережил бы границу доверия, которая его оправдывает
	// (ADR-057 §9). Плагин останавливается тем же путём, что по команде.
	fmt.Fprintln(os.Stderr, "[devtools-runtime] управляющий канал закрыт — остановка")
	c.stop()
}

// fixtureRoot / fixtureEntrypoint — PackageRoot и Entrypoint из пути к бинарю.
//
// Разделены потому, что host требует Entrypoint ОТНОСИТЕЛЬНО PackageRoot: путь
// целиком в Entrypoint дал бы отказ запуска, а не удобство.
func fixtureRoot(binary string) string { return filepath.Dir(binary) }

func fixtureEntrypoint(binary string) string { return filepath.Base(binary) }
