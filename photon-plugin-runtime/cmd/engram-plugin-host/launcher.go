// Command engram-plugin-host — запуск плагина ENGRAM поверх существующего
// runtime.
//
// ЭТО LAUNCHER, А НЕ ВТОРОЙ RUNTIME. host.Host уже делает всё, что делает
// production-хост: порождает процесс, ведёт handshake, маршрутизирует
// REQUEST через Authorizer в исполнитель, доставляет события, различает
// чистую остановку и падение. Оборачивать его вторым жизненным циклом
// означало бы завести второе место, где написано одно и то же, — и они
// разошлись бы при первой же правке.
//
// Поэтому здесь ровно то, чего у host.Host нет и быть не должно: сборка
// конкретных адаптеров, выдача прав конкретному плагину и чтение его
// докладов. Своего IPC-протокола нет; JSON-протокол cmd/devtools-runtime не
// заимствуется — он лабораторный (ADR-057) и знает про закрытый набор
// фикстур.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wiloris/photon-core/adapters/aesgcm"
	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	ed25519adapter "github.com/wiloris/photon-core/adapters/ed25519"
	"github.com/wiloris/photon-core/ports"

	"github.com/wiloris/photon-plugin-runtime/coreops"
	"github.com/wiloris/photon-plugin-runtime/host"
	"github.com/wiloris/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon-plugin-runtime/permissions"
)

// Options — всё, что launcher не может решить за вызывающего.
type Options struct {
	// DBPath — файл bbolt. ОДИН И ТОТ ЖЕ путь между запусками означает, что
	// плагин увидит своё прежнее состояние; разные пути — что не увидит.
	DBPath string

	// PackageRoot и Entrypoint: host склеивает их (host.go:185), и плагин
	// запускается из PackageRoot как рабочего каталога.
	PackageRoot string
	Entrypoint  string

	PluginID string

	// Grants — права, выдаваемые плагину. Задаются КОДОМ ХОСТА: экрана
	// согласия в этом срезе нет, и делать вид, что решение приняло
	// приложение, нельзя.
	Grants []string

	// Events — события, отправляемые плагину после старта. На каждое
	// launcher ждёт новый доклад.
	Events []string

	// Timeout — сколько ждать доклада плагина. Без предела зависший плагин
	// превратил бы запуск в вечное ожидание (урок ADR-047).
	Timeout time.Duration

	// KeyDir — каталог ключевого материала (ADR-068 п. 3.3). Пустой означает
	// каталог файла хранилища: ключи и данные живут рядом, и переносятся тоже
	// вместе — порознь они бесполезны.
	KeyDir string

	// Install — регистрировать плагин, если он ещё не установлен.
	//
	// УСТАНОВКА — СОБЫТИЕ ЯВНОЕ (ADR-068 п. 5, п. 6.2). Запуск
	// незарегистрированного плагина без этого флага — ошибка, а не тихая
	// установка: иначе опечатка в plugin_id создавала бы новый namespace и
	// новый номер миграции вместо отказа.
	Install bool
}

// Result — что launcher увидел за один запуск.
type Result struct {
	// Counts — значения счётчика в порядке докладов: первый от старта,
	// последующие по событиям.
	Counts []int

	// Signature — последняя пришедшая подпись, уже декодированная.
	Signature []byte

	// SignatureVerified — проверена ли подпись ключом устройства этого
	// запуска. Проверяет ХОСТ, а не плагин: доверять подписи, проверенной
	// тем, кто её сделал, нечего.
	SignatureVerified bool

	// Lines — журнал плагина целиком, как он пришёл в Log-колбэк.
	Lines []string

	// Exit — чем закончился процесс плагина.
	Exit host.ExitStatus
}

// Run выполняет один полный цикл: открыть хранилище, выдать права, запустить
// плагин, дождаться докладов, остановить, проверить подпись.
func Run(ctx context.Context, o Options) (Result, error) {
	var res Result

	if o.Timeout <= 0 {
		o.Timeout = 15 * time.Second
	}

	id := ed25519adapter.New()
	cr := aesgcm.New()

	// ── ключевой материал: читается, а не порождается заново ─────────────
	keyDir := o.KeyDir
	if keyDir == "" {
		keyDir = filepath.Dir(o.DBPath)
	}
	keys, err := loadOrCreateKeys(ctx, keyDir, id, cr)
	if err != nil {
		return res, err
	}

	/*
	 * ── хранилище: два открытия, и это не небрежность ────────────────────
	 *
	 * Список миграций фиксируется при открытии (bboltadapter.New), а
	 * построить его можно только прочитав реестр, который лежит внутри того
	 * же хранилища. Разорвать круг можно ровно двумя способами: держать
	 * реестр в отдельном файле — или открыть дважды. Второе выбрано потому,
	 * что первое разносит состояние хоста по двум файлам, которые обязаны
	 * переноситься вместе и молча расходятся, если однажды не перенеслись.
	 *
	 * Первое открытие знает одну миграцию — сам реестр (п. 6.1).
	 */
	boot, err := newBootStorage(o.DBPath)
	if err != nil {
		return res, fmt.Errorf("открыть хранилище: %w", err)
	}
	if err := boot.Migrate(ctx, registrySchemaVersion); err != nil {
		boot.Close(context.Background())
		return res, fmt.Errorf("мигрировать реестр: %w", err)
	}

	if o.Install {
		if _, err := ensureInstalled(ctx, boot, o.PluginID); err != nil {
			boot.Close(context.Background())
			return res, fmt.Errorf("установить %s: %w", o.PluginID, err)
		}
	}

	rows, _, err := readRegistry(ctx, boot)
	if err != nil {
		boot.Close(context.Background())
		return res, fmt.Errorf("прочитать реестр: %w", err)
	}
	if err := boot.Close(context.Background()); err != nil {
		return res, fmt.Errorf("закрыть хранилище: %w", err)
	}

	if !containsPlugin(rows, o.PluginID) {
		// Отказ, а не тихая установка (п. 6.2): опечатка в plugin_id иначе
		// создала бы новый namespace и израсходовала номер миграции.
		return res, fmt.Errorf("плагин %s не установлен: запустите с установкой", o.PluginID)
	}

	/*
	 * Второе открытие — с полным списком. Migrate зовётся ПРИ КАЖДОМ
	 * открытии (инвариант п. 5.1/5.3): это закрывает окно «строка реестра
	 * записана, namespace не создан», которое иначе оставляет плагин с
	 * ErrNamespaceNotFound на первом put и не чинится само.
	 *
	 * Повторный вызов безвреден: вся партия идёт одной транзакцией, а
	 * current >= target даёт no-op.
	 */
	migrations, target := migrationsFor(rows)
	st, err := bboltadapter.New(o.DBPath, migrations)
	if err != nil {
		return res, fmt.Errorf("переоткрыть хранилище: %w", err)
	}
	defer st.Close(context.Background())

	if err := st.Migrate(ctx, target); err != nil {
		return res, fmt.Errorf("мигрировать namespace плагинов: %w", err)
	}

	// ── доклады плагина ──────────────────────────────────────────────────
	rep := newReports()

	exec, err := coreops.New(coreops.Config{
		Storage: st,
		// Namespace ПЛАГИНА, а не общий на всех (ADR-068 п. 4.1):
		// исполнитель создаётся на плагин, и ForEachPrefix одного не видит
		// ключей другого даже при ошибке в префиксе.
		StorageNamespace: pluginNamespace(o.PluginID),
		Identity:         id,
		DeviceKey:        keys.Device,
		Crypto:           cr,
		DeriveKey: func(ctx context.Context, domain string) (ports.SymmetricKey, error) {
			return cr.DeriveKey(ctx, keys.Master, domain)
		},
		Log: rep.onLog,
	})
	if err != nil {
		return res, fmt.Errorf("executor: %w", err)
	}

	// ── права ────────────────────────────────────────────────────────────
	policy := permissions.NewPolicy()
	if err := policy.Grant(permissions.Subject{PluginID: o.PluginID}, o.Grants); err != nil {
		return res, fmt.Errorf("grant: %w", err)
	}

	// ── запуск ───────────────────────────────────────────────────────────
	h := host.New(host.Config{
		Manifest:    pluginManifest(o),
		PackageRoot: o.PackageRoot,
		Caller:      ports.PublicKey("engram-host:" + o.PluginID),
		Policy:      policy,
		Authorizer:  permissions.NewAuthorizer(policy),
		Handler:     exec,
	})

	if err := h.Start(ctx); err != nil {
		return res, fmt.Errorf("start: %w", err)
	}

	// Остановка выполняется в любом случае, включая ошибку ожидания: иначе
	// процесс плагина пережил бы хозяина.
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), o.Timeout)
		defer cancel()
		_ = h.Stop(stopCtx)
	}()

	count, err := rep.waitCount(o.Timeout, h.Exited())
	if err != nil {
		return res, fmt.Errorf("first report: %w", err)
	}
	res.Counts = append(res.Counts, count)

	for _, ev := range o.Events {
		if err := h.SendEvent(ev, nil); err != nil {
			return res, fmt.Errorf("send event %q: %w", ev, err)
		}
		count, err := rep.waitCount(o.Timeout, h.Exited())
		if err != nil {
			return res, fmt.Errorf("report after event %q: %w", ev, err)
		}
		res.Counts = append(res.Counts, count)
	}

	// ── остановка и разбор ───────────────────────────────────────────────
	stopCtx, cancel := context.WithTimeout(context.Background(), o.Timeout)
	defer cancel()
	if err := h.Stop(stopCtx); err != nil {
		return res, fmt.Errorf("stop: %w", err)
	}

	exit, err := h.Wait(stopCtx)
	if err != nil {
		return res, fmt.Errorf("wait: %w", err)
	}
	res.Exit = exit
	res.Lines = rep.snapshot()

	if sig, ok := rep.signature(); ok {
		res.Signature = sig
		last := strconv.Itoa(res.Counts[len(res.Counts)-1])

		// Проверка идёт по ДОМЕННО-РАЗДЕЛЁННОМУ входу, а не по сырому
		// значению: именно его подписал coreops (ADR-037). Первая редакция
		// проверяла сырые байты и получала «подпись не сошлась» — ошибка
		// доверенной стороны, не плагина.
		pub, perr := id.PublicKeyOf(ctx, keys.Device)
		if perr != nil {
			return res, fmt.Errorf("публичный ключ устройства: %w", perr)
		}
		ok, verr := id.Verify(ctx, pub, pluginSignInput(o.PluginID, []byte(last)), sig)
		if verr != nil {
			return res, fmt.Errorf("verify: %w", verr)
		}
		res.SignatureVerified = ok
	}

	return res, nil
}

/*
 * pluginSignInput — доменно-разделённый вход подписи (ADR-037):
 *
 *	"plugin-sign:" ‖ uint32be(len(plugin_id)) ‖ plugin_id ‖ ":" ‖ data
 *
 * Знать его обязана ДОВЕРЕННАЯ сторона, и только она. Плагин подписывает
 * «свои» байты и о домене не подозревает — в этом и смысл разделения: без
 * него подпись, сделанную одним плагином, можно было бы предъявить от имени
 * другого.
 *
 * ФОРМУЛА ЗДЕСЬ — ВТОРАЯ КОПИЯ, и это названо прямо, а не замолчано.
 * coreops строит тот же вход своей неэкспортируемой pluginSignInput, и
 * сослаться на неё нельзя. Расхождение проявится не компилятором, а
 * несошедшейся подписью — ровно тем, что и случилось при первом прогоне
 * этого среза, когда проверка шла по сырым байтам. Тот же выбор и по той же
 * причине сделан в tests/lifecycle/sdkscenario_test.go, где формула записана
 * из текста ADR как доверенный оракул.
 *
 * Экспортировать её из coreops — отдельное решение, и в этот срез оно не
 * входит: срез не меняет существующие контракты.
 */
func pluginSignInput(pluginID string, data []byte) []byte {
	var b bytes.Buffer
	b.WriteString("plugin-sign:")
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(pluginID)))
	b.Write(n[:])
	b.WriteString(pluginID)
	b.WriteString(":")
	b.Write(data)
	return b.Bytes()
}

// pluginManifest собирает манифест В КОДЕ.
//
// Файла manifest.json нет намеренно: формат установленного пакета — это
// отдельное решение (где лежит, кто кладёт, кто проверяет подпись), и
// принимать его мимоходом ради первого среза значило бы зафиксировать
// формат раньше, чем появился хоть один его потребитель. Runtime принимает
// *manifest.Manifest как структуру и про файлы ничего не знает.
func pluginManifest(o Options) *manifest.Manifest {
	return &manifest.Manifest{
		ManifestVersion: 1,
		Identity: manifest.Identity{
			PluginID: o.PluginID,
			Version:  "0.1.0",
		},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: 1,
			Entrypoint:      o.Entrypoint,
		},
		Permissions: manifest.Permissions{
			Requested: o.Grants,
		},
	}
}

// reports — разбор журнала плагина.
//
// Это ЕДИНСТВЕННЫЙ существующий канал, по которому состояние плагина
// доходит до хоста: ответа плагин отправить не может (RESPONSE идёт только
// на его собственный REQUEST), а SendEvent односторонний. Log-колбэк хост
// передаёт в coreops сам, поэтому канал существует по построению, а не
// добавлен ради этого среза.
type reports struct {
	mu    sync.Mutex
	lines []string
	sig   []byte

	counts chan int
}

func newReports() *reports {
	return &reports{counts: make(chan int, 8)}
}

func (r *reports) onLog(pluginID, level, message string) {
	r.mu.Lock()
	r.lines = append(r.lines, pluginID+" ["+level+"] "+message)
	r.mu.Unlock()

	switch {
	case strings.HasPrefix(message, "count:"):
		n, err := strconv.Atoi(strings.TrimPrefix(message, "count:"))
		if err != nil {
			return
		}
		select {
		case r.counts <- n:
		default: // читатель отстал — доклад не теряется, он уже в lines
		}

	case strings.HasPrefix(message, "signature:"):
		raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(message, "signature:"))
		if err != nil {
			return
		}
		r.mu.Lock()
		r.sig = raw
		r.mu.Unlock()
	}
}

var errNoReport = errors.New("плагин не доложил счётчик в отведённое время")

/*
 * waitCount ждёт доклад — но следит и за ЗАВЕРШЕНИЕМ процесса.
 *
 * Без второго условия отказ плагина выглядел бы как таймаут: упавший
 * процесс докладывает ровно столько же, сколько зависший, и различить их по
 * одному только ожиданию нельзя. Первая редакция так и делала — и проверка
 * «без storage.write запуск обязан не пройти» честно падала, но тратила на
 * это все десять секунд, сообщая при этом не ту причину.
 */
func (r *reports) waitCount(timeout time.Duration, exited <-chan host.ExitStatus) (int, error) {
	select {
	case n := <-r.counts:
		return n, nil
	case st := <-exited:
		return 0, fmt.Errorf("плагин завершился до доклада: %v (код %d, err=%v)",
			st.Kind, st.Code, st.Err)
	case <-time.After(timeout):
		return 0, errNoReport
	}
}

func (r *reports) signature() ([]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sig == nil {
		return nil, false
	}
	out := make([]byte, len(r.sig))
	copy(out, r.sig)
	return out, true
}

func (r *reports) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.lines))
	copy(out, r.lines)
	return out
}
