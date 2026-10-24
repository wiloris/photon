// Command devtools-runtime — экземпляр Plugin Runtime, которым владеет
// Developer Tools (ADR-057), на настоящем исполнителе coreops (ADR-058).
//
// ЧТО ЭТО ТАКОЕ И ЧЕМ НЕ ЯВЛЯЕТСЯ.
//
// Это не демон и не production-хост. Процесс живёт ровно столько, сколько его
// родитель, управляется только им и обслуживает один экземпляр runtime,
// созданный для наблюдения. Composition root продакшна (cmd/photon-core-server)
// этим бинарём не решается и не приближается: вопросы core-operations.md §11 —
// источник мастер-ключа и кто зовёт Migrate — остаются открытыми. Всё, что
// здесь им отвечает, — лабораторные исключения (lab.go, ADR-058).
//
// ГРАНИЦА ДОВЕРИЯ — РОДИТЕЛЬСТВО ПРОЦЕССА. Управляющий канал — stdin/stdout,
// то есть трубы, которыми владеет тот, кто этот процесс породил. Ни сокета, ни
// порта, ни файла: сторонний процесс до канала не дотягивается, и потому
// credential'а здесь нет — предъявлять его некому.
//
// УПРАВЛЯЮЩИЙ КАНАЛ — НЕ PLUGIN IPC. Механика похожа (родитель владеет трубами
// потомка), собеседник противоположен: у Plugin IPC он НЕДОВЕРЕННЫЙ, здесь —
// доверенный родитель. Поэтому кодек здесь свой, а пакет ipc используется
// только как тип исполнителя, не как кодек (ADR-057, docs/ipc.md §20).
//
// Протокол: по одному JSON-объекту в строке в обе стороны. stdout занят
// протоколом целиком; всё человекочитаемое уходит в stderr, иначе первая же
// отладочная строка сломала бы разбор у родителя. Точная форма —
// docs/plugin-lab.md §2.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-plugin-runtime/host"
	"github.com/wiloris/photon/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon/photon-plugin-runtime/permissions"
)

// Пути к собранным фикстурам передаёт доверенный родитель. Это не канал
// управления: из канала путь не выбирается (plugin-lab.md §4.5).
var (
	fixtureHelloPath = flag.String("fixture-hello", "",
		"путь к собранному бинарю фикстуры hello (обязателен)")
	fixtureSDKScenarioPath = flag.String("fixture-sdkscenario", "",
		"путь к собранному бинарю фикстуры sdkscenario (обязателен)")
	stopTimeout = flag.Duration("stop-timeout", 5*time.Second,
		"предел ожидания остановки плагина")
)

func fixturePath(fixture string) string {
	switch fixture {
	case fixtureHello:
		return *fixtureHelloPath
	case fixtureSDKScenario:
		return *fixtureSDKScenarioPath
	}
	return ""
}

// fixtureCaller — Caller экземпляра: нужен host'у, к правам не относится.
const fixtureCaller = "devtools-lab-instance"

// ── управляющий протокол ─────────────────────────────────────────────────────

// request — кадр родителя. Разбор СТРОГИЙ (DisallowUnknownFields): кадр с
// "grants", "key" или "plugin" отвергается, а не принимается как обычный. Иначе
// отсутствие управления правами держалось бы на том, что поле не читается.
type request struct {
	ID       int     `json:"id"`
	Cmd      string  `json:"cmd"`
	Scenario *string `json:"scenario"`
}

type response struct {
	ID    int    `json:"id"`
	OK    bool   `json:"ok"`
	Cmd   string `json:"cmd"`
	Error string `json:"error,omitempty"`

	// Полезная нагрузка ответов. Поля необязательные: у каждой команды
	// заполняются свои, остальные опускаются.
	RuntimeState string            `json:"runtime_state,omitempty"`
	Scenario     string            `json:"scenario,omitempty"`
	Fixture      string            `json:"fixture,omitempty"`
	Namespace    string            `json:"namespace,omitempty"`
	PluginID     string            `json:"plugin_id,omitempty"`
	PluginState  string            `json:"plugin_state,omitempty"`
	Granted      []string          `json:"granted,omitempty"`
	Isolation    *isolationV       `json:"isolation,omitempty"`
	Operations   []operationV      `json:"operations,omitempty"`
	Results      map[string]string `json:"results,omitempty"`
	Scenarios    []scenarioV       `json:"scenarios,omitempty"`
	Lab          *labV             `json:"lab,omitempty"`
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

// scenarioV — строка каталога в том виде, в каком её видит родитель. Режим и
// окружение фикстуры наружу не отдаются: из канала их не выбрать.
type scenarioV struct {
	ID          string   `json:"id"`
	Fixture     string   `json:"fixture"`
	PluginID    string   `json:"plugin_id"`
	Granted     []string `json:"granted"`
	Namespace   string   `json:"namespace"`
	Description string   `json:"description"`
}

// ── контроллер ───────────────────────────────────────────────────────────────

type controller struct {
	lab *lab

	mu      sync.Mutex
	h       *host.Host
	policy  *permissions.Policy
	ops     []operationV
	loaded  bool
	current *scenario
	results map[string]string

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

// observer — наблюдатель перед настоящим исполнителем: записывает операцию,
// дошедшую до него, и передаёт вызов дальше без изменений.
func (c *controller) observer(exec ipc.Handler) ipc.Handler {
	return ipc.HandlerFunc(func(ctx context.Context, caller ipc.CallerInfo, op string, payload []byte) ([]byte, error) {
		c.record(op)
		return exec.Handle(ctx, caller, op, payload)
	})
}

// onLog — сборщик результатов: log.write фикстуры приходит сюда через
// coreops. Значения, не разрешённые строкой каталога, отбрасываются сразу.
func (c *controller) onLog(pluginID, _ string, message string) {
	name, value := splitResult(message)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil || pluginID != c.current.PluginID || !allowed(*c.current, name) {
		return
	}
	if _, seen := c.results[name]; !seen {
		c.results[name] = displayValue(name, value)
	}
}

// load запускает сценарий каталога. Контекст ЗАПУСКА передаётся вызывающим и
// здесь не используется как контекст сессии — см. поле cancel.
func (c *controller) load(startCtx context.Context, id string) (scenario, error) {
	c.mu.Lock()
	if c.loaded {
		c.mu.Unlock()
		return scenario{}, errors.New("plugin already loaded")
	}
	c.mu.Unlock()

	s, ok := findScenario(id)
	if !ok {
		return scenario{}, fmt.Errorf("unknown scenario: %s", id)
	}
	path := fixturePath(s.Fixture)
	if path == "" {
		return scenario{}, fmt.Errorf("fixture binary not configured: %s", s.Fixture)
	}
	if err := startCtx.Err(); err != nil {
		return scenario{}, err
	}

	exec, err := c.lab.executor(namespaceOf[s.Fixture], c.onLog)
	if err != nil {
		return scenario{}, fmt.Errorf("executor: %w", err)
	}
	if s.Bootstrap {
		if err := bootstrapHello(startCtx, exec, s.PluginID); err != nil {
			return scenario{}, fmt.Errorf("bootstrap fixture: %w", err)
		}
	}

	// Окружение фикстуры выставляется ЦЕЛИКОМ: пустые значения тоже. Иначе
	// значение из окружения родителя дошло бы до плагина наследованием.
	for _, name := range fixtureEnv {
		value := s.Env[name]
		switch name {
		case "PHOTON_TEST_MODE":
			value = s.Mode
		case "PHOTON_TEST_PLUGIN_ID":
			value = s.PluginID
		}
		if err := os.Setenv(name, value); err != nil {
			return scenario{}, fmt.Errorf("fixture environment: %w", err)
		}
	}

	m := &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: s.PluginID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      fixtureEntrypoint(path),
			InstancePolicy:  manifest.InstanceSingle,
		},
		Permissions: manifest.Permissions{Requested: s.Granted},
	}

	policy := permissions.NewPolicy()
	// Grant допустим ЗДЕСЬ и только здесь: политика принадлежит экземпляру,
	// который мы сами создали (ADR-057 §7). Права берутся из строки каталога;
	// наружу — в управляющий канал, в C ABI, в сеть — эта операция не выводится.
	if err := policy.Grant(permissions.Subject{PluginID: s.PluginID}, s.Granted); err != nil {
		return scenario{}, fmt.Errorf("grant: %w", err)
	}

	c.mu.Lock()
	c.current, c.results = &s, map[string]string{}
	c.mu.Unlock()

	h := host.New(host.Config{
		Manifest:    m,
		PackageRoot: fixtureRoot(path),
		Caller:      ports.PublicKey(fixtureCaller),
		Policy:      policy,
		Authorizer:  permissions.NewAuthorizer(policy),
		Handler:     c.observer(exec),
	})
	// Контекст сессии живёт до stop(). Дедлайн запуска обеспечивается тем,
	// что Start блокируется до завершения handshake.
	lifetime, cancel := context.WithCancel(context.Background())
	if err := h.Start(lifetime); err != nil {
		cancel()
		return scenario{}, fmt.Errorf("start plugin: %w", err)
	}

	c.mu.Lock()
	c.h, c.policy, c.loaded, c.cancel = h, policy, true, cancel
	c.mu.Unlock()
	return s, nil
}

// stopPlugin останавливает плагин. Идемпотентна: вызывается и по команде, и по
// закрытию управляющего канала, и порядок этих двух событий не определён.
func (c *controller) stopPlugin() {
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

// shutdownLab — единый путь штатного завершения (plugin-lab.md §6.1):
// остановить плагин, затем закрыть и удалить временное хранилище. Удалять базу
// под работающим исполнителем нельзя, поэтому порядок именно такой.
func (c *controller) shutdownLab() {
	c.stopPlugin()
	c.lab.close()
}

func (c *controller) diagnose(r *response) {
	c.mu.Lock()
	h, policy, current := c.h, c.policy, c.current
	results := make(map[string]string, len(c.results))
	for k, v := range c.results {
		results[k] = v
	}
	c.mu.Unlock()

	r.Lab = c.lab.info()
	r.Operations = c.snapshot()
	if current == nil || h == nil {
		r.PluginState = "not loaded"
		return
	}
	r.Scenario, r.PluginID, r.Results = current.ID, current.PluginID, results
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
		if granted, err := policy.GrantedPermissions(permissions.Subject{PluginID: current.PluginID}); err == nil {
			r.Granted = granted
		}
	}
}

func catalogView() []scenarioV {
	out := make([]scenarioV, 0, len(catalog))
	for _, s := range catalog {
		out = append(out, scenarioV{
			ID: s.ID, Fixture: s.Fixture, PluginID: s.PluginID,
			Granted: append([]string(nil), s.Granted...), Namespace: namespaceOf[s.Fixture],
			Description: s.Description,
		})
	}
	return out
}

// ── цикл управления ──────────────────────────────────────────────────────────

func decodeRequest(line []byte) (request, error) {
	var req request
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return request{}, err
	}
	return req, nil
}

func main() {
	flag.Parse()
	if *fixtureHelloPath == "" || *fixtureSDKScenarioPath == "" {
		fmt.Fprintln(os.Stderr, "нужны -fixture-hello и -fixture-sdkscenario — пути к собранным фикстурам")
		os.Exit(2)
	}

	out := json.NewEncoder(os.Stdout)

	// Композиция ДО готовности: частично собранная лаборатория готовность не
	// объявляет (plugin-lab.md §2.4).
	l, err := newLab(context.Background())
	if err != nil {
		_ = out.Encode(response{OK: false, Cmd: "ready", Error: err.Error()})
		os.Exit(1)
	}
	c := &controller{lab: l}

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 1<<20)

	// Первая строка — готовность. Родителю не приходится гадать, поднялись мы
	// или нет: он читает её и только потом шлёт команды.
	_ = out.Encode(response{OK: true, Cmd: "ready", RuntimeState: "ready", Lab: l.info()})

	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}
		req, err := decodeRequest(line)
		if err != nil {
			_ = out.Encode(response{OK: false, Cmd: "?", Error: "invalid control frame: " + err.Error()})
			continue
		}

		resp := response{ID: req.ID, Cmd: req.Cmd, OK: true, RuntimeState: "ready"}
		if req.Scenario != nil && req.Cmd != "load" {
			resp.OK, resp.Error = false, "scenario is only valid for load"
			_ = out.Encode(resp)
			continue
		}
		switch req.Cmd {
		case "status":
			// Ничего не меняет: чистая диагностика.

		case "scenarios":
			resp.Scenarios = catalogView()

		case "load":
			id := defaultScenario
			if req.Scenario != nil {
				id = *req.Scenario
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			s, err := c.load(ctx, id)
			cancel()
			if err != nil {
				resp.OK, resp.Error = false, err.Error()
			} else {
				resp.Scenario, resp.Fixture, resp.PluginID = s.ID, s.Fixture, s.PluginID
				resp.Granted, resp.Namespace, resp.PluginState = s.Granted, namespaceOf[s.Fixture], "running"
			}

		case "diagnose":
			c.diagnose(&resp)

		case "shutdown":
			c.shutdownLab()
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
	// (ADR-057 §9). Путь тот же, что по команде shutdown, включая уборку
	// временного хранилища. Если убит САМ этот процесс (kill -9), сюда
	// управление не приходит, и уборка не гарантируется (ADR-058 п. 3).
	fmt.Fprintln(os.Stderr, "[devtools-runtime] управляющий канал закрыт — остановка")
	c.shutdownLab()
}

// fixtureRoot / fixtureEntrypoint — PackageRoot и Entrypoint из пути к бинарю.
//
// Разделены потому, что host требует Entrypoint ОТНОСИТЕЛЬНО PackageRoot: путь
// целиком в Entrypoint дал бы отказ запуска, а не удобство.
func fixtureRoot(binary string) string { return filepath.Dir(binary) }

func fixtureEntrypoint(binary string) string { return filepath.Base(binary) }
