// Условие 6A ADR-076 — граница домена p2p.* на НАСТОЯЩИХ процессах.
//
// Цепочка под тестом: процесс плагина → провод IPC → Authorizer → НАСТОЯЩИЙ
// coreops → реестр сессий. Фиктивен ровно один слой — ports.P2P, и это
// граница между 6A и 6B: здесь доказываются право, владение и жизненный
// цикл, там — транспорт двумя процессами Photon через adapters/pion.
//
// Почему in-process не засчитывается: net.Pipe не порождает процесса, не
// имеет кода возврата и не воспроизводит тайминги настоящего завершения.
// Пять фикстур Phase 11 нашли этим три бага, невидимых для fakeSpawner.
package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/wiloris/photon-core/adapters/aesgcm"
	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	ed25519adapter "github.com/wiloris/photon-core/adapters/ed25519"
	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/coreops"
	"github.com/wiloris/photon-plugin-runtime/host"
	"github.com/wiloris/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon-plugin-runtime/permissions"
)

const (
	p2pGuardID = "com.engram.test.p2pguard"
	p2pNS      = "plugin-data"
)

// ── фиктивный ports.P2P ─────────────────────────────────────────────
//
// Только он и фиктивен. Сессия отдаёт рабочий поток, чтобы установление
// состоялось и p2p.peer имел что подтверждать.

type fakeStream struct {
	mu     sync.Mutex
	closed bool
	ch     chan []byte
}

func newFakeStream() *fakeStream { return &fakeStream{ch: make(chan []byte, 8)} }

func (s *fakeStream) Read(p []byte) (int, error) {
	b, ok := <-s.ch
	if !ok {
		return 0, errors.New("closed")
	}
	return copy(p, b), nil
}
func (s *fakeStream) Write(p []byte) (int, error) { return len(p), nil }
func (s *fakeStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
	return nil
}

type fakeP2PSession struct {
	peer ports.PublicKey
	st   *fakeStream
}

func (s *fakeP2PSession) OpenStream(ctx context.Context) (ports.Stream, error)   { return s.st, nil }
func (s *fakeP2PSession) AcceptStream(ctx context.Context) (ports.Stream, error) { return s.st, nil }
func (s *fakeP2PSession) OpenUniStream(ctx context.Context) (ports.SendStream, error) {
	return nil, errors.New("unused")
}
func (s *fakeP2PSession) AcceptUniStream(ctx context.Context) (ports.RecvStream, error) {
	return nil, errors.New("unused")
}
func (s *fakeP2PSession) RemoteEndpoint() ports.Endpoint { return "fake" }
func (s *fakeP2PSession) Peer() ports.PublicKey          { return s.peer }
func (s *fakeP2PSession) Close() error                   { return s.st.Close() }

type fakeP2P struct{}

var _ ports.P2P = (*fakeP2P)(nil)

func (fakeP2P) CreateOffer(ctx context.Context, peer ports.PublicKey) ([]byte, error) {
	return []byte("OFFER"), nil
}
func (fakeP2P) AcceptOffer(ctx context.Context, peer ports.PublicKey, offer []byte) ([]byte, ports.Session, error) {
	return []byte("ANSWER"), &fakeP2PSession{peer: peer, st: newFakeStream()}, nil
}
func (fakeP2P) CompleteSession(ctx context.Context, peer ports.PublicKey, answer []byte) (ports.Session, error) {
	return &fakeP2PSession{peer: peer, st: newFakeStream()}, nil
}
func (fakeP2P) Close() error { return nil }

// ── стенд ───────────────────────────────────────────────────────────

// p2pRig — НАСТОЯЩИЙ coreops над настоящими адаптерами плюс сбор событий.
type p2pRig struct {
	handler *coreops.Handler
	entry   string
	dir     string

	mu       sync.Mutex
	events   []p2pEvent
	captures map[string]*logCapture
}

type p2pEvent struct {
	pluginID string
	typ      string
	body     map[string]any
}

func newP2PRig(t *testing.T) *p2pRig {
	t.Helper()
	ctx := context.Background()

	st, err := bboltadapter.New(filepath.Join(t.TempDir(), "p2p.db"), []bboltadapter.Migration{{
		Version: 1,
		Up: func(tx *bolt.Tx) error {
			_, e := tx.CreateBucketIfNotExists([]byte(p2pNS))
			return e
		},
	}})
	if err != nil {
		t.Fatalf("bbolt.New: %v", err)
	}
	if err := st.Migrate(ctx, 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(func() { st.Close(context.Background()) })

	id := ed25519adapter.New()
	pair, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	cr := aesgcm.New()
	master, err := cr.GenerateKey(ctx)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	r := &p2pRig{captures: map[string]*logCapture{}}
	h, err := coreops.New(coreops.Config{
		Storage:          st,
		StorageNamespace: p2pNS,
		Identity:         id,
		DeviceKey:        pair.Private,
		Crypto:           cr,
		DeriveKey: func(ctx context.Context, domain string) (ports.SymmetricKey, error) {
			return cr.DeriveKey(ctx, master, domain)
		},
		Log:       r.onLog,
		P2P:       fakeP2P{},
		EmitEvent: r.onEvent,
	})
	if err != nil {
		t.Fatalf("coreops.New: %v", err)
	}
	r.handler = h
	r.dir = t.TempDir()
	r.entry = buildPlugin(t, "p2pguard", r.dir)
	return r
}

func (r *p2pRig) onLog(pluginID, level, message string) {
	r.mu.Lock()
	c := r.captures[pluginID]
	r.mu.Unlock()
	if c == nil {
		return
	}
	body, _ := json.Marshal(map[string]string{"level": level, "message": message})
	_, _ = c.capture(body)
}

func (r *p2pRig) onEvent(pluginID, typ string, payload []byte) error {
	var body map[string]any
	_ = json.Unmarshal(payload, &body)
	r.mu.Lock()
	r.events = append(r.events, p2pEvent{pluginID, typ, body})
	r.mu.Unlock()
	return nil
}

func (r *p2pRig) closedEvents() []p2pEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []p2pEvent
	for _, e := range r.events {
		if e.typ == "p2p.closed" {
			out = append(out, e)
		}
	}
	return out
}

func (r *p2pRig) waitClosed(t *testing.T, pluginID string, timeout time.Duration) p2pEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, e := range r.closedEvents() {
			if e.pluginID == pluginID {
				return e
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("p2p.closed для %q не пришло за %s; события: %v", pluginID, timeout, r.closedEvents())
	return p2pEvent{}
}

// start поднимает НАСТОЯЩИЙ процесс плагина.
//
// Handler передаётся как *coreops.Handler, а НЕ завёрнутым в
// ipc.HandlerFunc: обёртка функцией потеряла бы метод ClosePluginSessions, и
// host, проверяющий его приведением типа, тихо перестал бы освобождать
// сессии. Ловушка тем неприятнее, что тесты остались бы зелёными везде,
// кроме проверки жизненного цикла.
func (r *p2pRig) start(t *testing.T, pluginID, mode string, granted []string, env map[string]string) (*host.Host, *logCapture) {
	t.Helper()
	t.Setenv("PHOTON_TEST_MODE", mode)
	t.Setenv("PHOTON_TEST_PLUGIN_ID", pluginID)
	for k, v := range env {
		t.Setenv(k, v)
	}

	m := &manifest.Manifest{
		ManifestVersion: manifest.SupportedManifestVersion,
		Identity:        manifest.Identity{PluginID: pluginID, Version: "1.0.0"},
		Runtime: manifest.RuntimeSpec{
			ProtocolVersion: manifest.SupportedProtocolVersion,
			Entrypoint:      r.entry,
			InstancePolicy:  manifest.InstanceSingle,
		},
		// Манифест ЗАПРАШИВАЕТ право всегда — в том числе когда grant не
		// выдан. Так проверяется, что capability не берётся из манифеста
		// (ADR-073: манифест объявляет, вызывающий выдаёт).
		Permissions: manifest.Permissions{Requested: []string{"p2p.connect"}},
	}

	policy := permissions.NewPolicy()
	if err := policy.Grant(permissions.Subject{PluginID: pluginID}, granted); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	capture := newLogCapture()
	r.mu.Lock()
	r.captures[pluginID] = capture
	r.mu.Unlock()

	h := host.New(host.Config{
		Manifest:    m,
		PackageRoot: r.dir,
		Caller:      ports.PublicKey("p2p-caller-" + pluginID + "-" + mode),
		Policy:      policy,
		Authorizer:  permissions.NewAuthorizer(policy),
		Handler:     r.handler,
	})
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start(%s/%s): %v", pluginID, mode, err)
	}
	return h, capture
}

// probeCodes собирает коды по всем шести операциям из журнала фикстуры.
func probeCodes(t *testing.T, c *logCapture) map[string]string {
	t.Helper()
	c.waitForPrefix(t, "p2p_probe:done", 20*time.Second)
	out := map[string]string{}
	for _, m := range c.all() {
		rest, found := strings.CutPrefix(m, "p2p_probe:")
		if !found || rest == "done" {
			continue
		}
		op, code, _ := strings.Cut(rest, "=")
		out[op] = code
	}
	return out
}

var allSix = []string{
	"p2p.create_offer", "p2p.accept_offer", "p2p.complete",
	"p2p.peer", "p2p.send", "p2p.close",
}

// ── 1. Право ────────────────────────────────────────────────────────

// Без p2p.connect отказ получают ВСЕ ШЕСТЬ, и отказ именно по праву.
//
// Манифест при этом право ЗАПРАШИВАЕТ: так проверяется ADR-073 — capability
// не берётся из манифеста, её выдаёт вызывающий. Запрошенное без выданного
// не даёт ничего.
func TestP2PWithoutGrantAllSixDenied(t *testing.T) {
	r := newP2PRig(t)
	h, capture := r.start(t, p2pGuardID, "probe-all", nil, nil)

	codes := probeCodes(t, capture)
	for _, op := range allSix {
		got, ok := codes[op]
		if !ok {
			t.Errorf("%s: фикстура не доложила исход", op)
			continue
		}
		if got != string(ipc.ErrForbidden) {
			t.Errorf("%s: код %q, ожидался %s — манифест запрашивал право, но grant не выдан",
				op, got, ipc.ErrForbidden)
		}
	}
	stopAndExpectClean(t, h)
}

// С выданным p2p.connect операции ДОХОДЯТ до исполнителя.
//
// Проверяется не «успех», а именно прохождение границы прав: исход зависит
// от аргументов (несуществующий handle законно даёт NOT_FOUND), но
// PERMISSION_DENIED не имеет права встретиться ни разу.
func TestP2PWithGrantReachesExecutor(t *testing.T) {
	r := newP2PRig(t)
	h, capture := r.start(t, p2pGuardID, "probe-all", []string{"p2p.connect"}, nil)

	codes := probeCodes(t, capture)
	for _, op := range allSix {
		got, ok := codes[op]
		if !ok {
			t.Errorf("%s: фикстура не доложила исход", op)
			continue
		}
		if got == string(ipc.ErrForbidden) {
			t.Errorf("%s: отказ по праву при выданном p2p.connect", op)
		}
		if got == string(ipc.ErrUnsupported) {
			t.Errorf("%s: UNSUPPORTED — операция не дошла до исполнителя", op)
		}
	}
	// Операции, не требующие существующего handle, обязаны пройти.
	for _, op := range []string{"p2p.create_offer", "p2p.accept_offer"} {
		if codes[op] != "" {
			t.Errorf("%s: код %q, ожидался успех", op, codes[op])
		}
	}
	// Операции над несуществующим handle обязаны дать NOT_FOUND — тот же
	// ответ, что на чужой (ADR-076 §7.2).
	for _, op := range []string{"p2p.complete", "p2p.peer", "p2p.send", "p2p.close"} {
		if codes[op] != string(ipc.ErrNotFound) {
			t.Errorf("%s: код %q, ожидался %s", op, codes[op], ipc.ErrNotFound)
		}
	}
	stopAndExpectClean(t, h)
}

// ── 2. Владение ─────────────────────────────────────────────────────

// Плагин B не может работать с handle плагина A — ДВА НАСТОЯЩИХ ПРОЦЕССА,
// оба с выданным правом.
//
// Право у вора есть: иначе тест доказывал бы отказ по праву, а не владение.
func TestP2PSessionOfAnotherPluginIsNotFound(t *testing.T) {
	r := newP2PRig(t)
	const thiefID = "com.engram.test.p2pguard.thief"

	owner, ownerLog := r.start(t, p2pGuardID, "own-session", []string{"p2p.connect"}, nil)
	line := ownerLog.waitForPrefix(t, "p2p_own:session=", 20*time.Second)
	victim := strings.TrimPrefix(line, "p2p_own:session=")
	if victim == "" {
		t.Fatalf("владелец не сообщил handle: %v", ownerLog.all())
	}
	// Владелец свою сессию видит — иначе проверка чужой ничего не значит.
	if got, _ := ownerLog.findPrefix("p2p_own:peer="); got != "p2p_own:peer=" {
		t.Fatalf("владелец не смог прочитать СВОЮ сессию: %q", got)
	}

	thief, thiefLog := r.start(t, thiefID, "steal-session", []string{"p2p.connect"},
		map[string]string{"PHOTON_TEST_VICTIM_SESSION": victim})
	thiefLog.waitForPrefix(t, "p2p_theft:done", 20*time.Second)

	for _, op := range []string{"p2p.peer", "p2p.send", "p2p.close"} {
		line, ok := thiefLog.findPrefix("p2p_theft:" + op + "=")
		if !ok {
			t.Errorf("%s: вор не доложил исход", op)
			continue
		}
		code := strings.TrimPrefix(line, "p2p_theft:"+op+"=")
		if code != string(ipc.ErrNotFound) {
			t.Errorf("%s чужой сессии дал %q, ожидался %s — чужой handle обязан быть "+
				"неотличим от несуществующего", op, code, ipc.ErrNotFound)
		}
	}

	stopAndExpectClean(t, thief)
	stopAndExpectClean(t, owner)
}

// ── 3. Жизненный цикл ───────────────────────────────────────────────

// Сессия не переживает плагина, причина названа, соседи не задеты.
func TestP2PPluginStopClosesOwnSessionsOnly(t *testing.T) {
	r := newP2PRig(t)
	const secondID = "com.engram.test.p2pguard.second"

	first, firstLog := r.start(t, p2pGuardID, "own-session", []string{"p2p.connect"}, nil)
	firstLog.waitForPrefix(t, "p2p_own:session=", 20*time.Second)

	second, secondLog := r.start(t, secondID, "own-session", []string{"p2p.connect"}, nil)
	secondLine := secondLog.waitForPrefix(t, "p2p_own:session=", 20*time.Second)
	secondSession := strings.TrimPrefix(secondLine, "p2p_own:session=")

	// Останавливаем ПЕРВЫЙ процесс по-настоящему.
	stopAndExpectClean(t, first)

	ev := r.waitClosed(t, p2pGuardID, 10*time.Second)
	if got, _ := ev.body["reason"].(string); got != "plugin_stopped" {
		t.Fatalf("reason %q, ожидался plugin_stopped", got)
	}

	// Сессия соседа не закрыта: остановка одного плагина не рвёт связь
	// другому.
	for _, e := range r.closedEvents() {
		if e.pluginID == secondID {
			t.Fatalf("закрыта сессия соседа: %+v", e)
		}
	}
	// И она по-прежнему достижима своему владельцу — проверяем через
	// исполнителя, потому что второй процесс уже ждёт STOP.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload, _ := json.Marshal(map[string]string{"session_id": secondSession})
	if _, err := r.handler.Handle(ctx, ipc.CallerInfo{PluginID: secondID}, "p2p.peer", payload); err != nil {
		t.Fatalf("сессия соседа пострадала: %v", err)
	}

	stopAndExpectClean(t, second)
	ev2 := r.waitClosed(t, secondID, 10*time.Second)
	if got, _ := ev2.body["reason"].(string); got != "plugin_stopped" {
		t.Fatalf("reason соседа %q, ожидался plugin_stopped", got)
	}
}
