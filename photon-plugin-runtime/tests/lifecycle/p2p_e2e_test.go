// Условие 6B ADR-076 — сквозной acceptance двух процессов Photon через
// НАСТОЯЩИЙ adapters/pion.
//
// Фиктивных слоёв нет ни одного. Под тестом:
//
//	Plugin A → IPC → coreops.p2p → adapters/pion → ICE → DTLS → SCTP →
//	p2pmsg → SCTP → DTLS → ICE → adapters/pion → coreops.p2p → IPC → Plugin B
//
// Процессов ЧЕТЫРЕ: два продакшн-хоста (cmd/engram-plugin-host) и по одному
// процессу плагина у каждого. Хост — именно продакшн-бинарь, а не тестовая
// сборка: композиция Pion проверяется той же, что поедет на устройство.
//
// Идентичности независимы: у каждого хоста свой каталог ключей, свой
// device.key, своё хранилище.
//
// Signaling — TCP-рандеву, поднятое этим тестом. Иллюстрация ADR-042, а не
// часть платформы: тот же приём, что в tests/p2pcheck и tests/p2pslice.
package lifecycle

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon-plugin-runtime/manifest"
)

const (
	dialInitiatorID = "com.engram.test.p2pdial.a"
	dialResponderID = "com.engram.test.p2pdial.b"
)

// ── рандеву ─────────────────────────────────────────────────────────

// rendezvous — слепой ретранслятор между двумя соединениями.
//
// Слепой намеренно: он не разбирает и не хранит ни ключей, ни блобов. Всё,
// что он умеет, — переложить байты. Если бы он понимал содержимое, тест
// доказывал бы работу своего signaling, а не Photon.
type rendezvous struct {
	ln   net.Listener
	done chan struct{}
}

func startRendezvous(t *testing.T) *rendezvous {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	r := &rendezvous{ln: ln, done: make(chan struct{})}
	go r.serve()
	t.Cleanup(func() { _ = ln.Close(); <-r.done })
	return r
}

func (r *rendezvous) addr() string { return r.ln.Addr().String() }

func (r *rendezvous) serve() {
	defer close(r.done)
	a, err := r.ln.Accept()
	if err != nil {
		return
	}
	defer a.Close()
	b, err := r.ln.Accept()
	if err != nil {
		return
	}
	defer b.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	pipe := func(dst, src net.Conn) {
		defer wg.Done()
		sc := bufio.NewScanner(src)
		sc.Buffer(make([]byte, 0, 64*1024), 8<<20)
		for sc.Scan() {
			if _, err := dst.Write(append(sc.Bytes(), '\n')); err != nil {
				return
			}
		}
	}
	go pipe(a, b)
	go pipe(b, a)
	wg.Wait()
}

// ── стенд ───────────────────────────────────────────────────────────

type e2eSide struct {
	name    string
	id      string
	role    string
	dir     string
	stdout  strings.Builder
	cmd     *exec.Cmd
	mu      sync.Mutex
	errText strings.Builder
}

// buildFromHEAD собирает бинарь из ТЕКУЩЕГО дерева.
//
// Отдельная функция и отдельный комментарий потому, что устаревший бинарь уже
// один раз делал прогон недостоверным: в Phase 12 задача сборки не зависела
// от исходников photon-core, держалась UP-TO-DATE, и native-путь молча
// проверялся против библиотеки, собранной ДО появления проверяемых действий.
// go build такой задачи не кеширует по чужому дереву, но подтвердить это
// дешевле, чем предполагать: тест собирает оба бинаря на каждом прогоне, во
// временный каталог, и ни один заранее собранный артефакт не используется.
func buildFromHEAD(t *testing.T, pkg, out string) string {
	t.Helper()
	bin := filepath.Join(out, filepath.Base(pkg))
	cmd := exec.Command("go", "build", "-o", bin, pkg)
	if outb, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("сборка %s: %v\n%s", pkg, err, outb)
	}
	return bin
}

func writeManifest(t *testing.T, dir, pluginID, entry string) string {
	t.Helper()
	m := map[string]any{
		"manifest_version": manifest.SupportedManifestVersion,
		"identity":         map[string]any{"plugin_id": pluginID, "version": "1.0.0"},
		"runtime": map[string]any{
			"protocol_version": manifest.SupportedProtocolVersion,
			"entrypoint":       entry,
			"instance_policy":  string(manifest.InstanceSingle),
		},
		"permissions": map[string]any{"requested": []string{"p2p.connect", "identity.read"}},
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	path := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return path
}

// startSide поднимает ОДИН настоящий процесс продакшн-хоста.
func startSide(t *testing.T, hostBin, pluginBin, rendezvousAddr, name, pluginID, role string, wrongPeer bool) *e2eSide {
	t.Helper()
	dir := t.TempDir()

	// entrypoint — путь ОТНОСИТЕЛЬНО package-root: манифест описывает пакет.
	entry := "plugin"
	if err := copyFile(pluginBin, filepath.Join(dir, entry)); err != nil {
		t.Fatalf("копирование плагина: %v", err)
	}
	manifestPath := writeManifest(t, dir, pluginID, entry)

	s := &e2eSide{name: name, id: pluginID, role: role, dir: dir}
	cmd := exec.Command(hostBin,
		"-db", filepath.Join(dir, "host.db"),
		"-keys", dir,
		"-init", // своя идентичность у каждой стороны (ADR-069)
		"-install",
		"-manifest", manifestPath,
		"-package-root", dir,
		"-id", pluginID,
		"-grants", "p2p.connect,identity.read",
		"-timeout", "60s",
	)
	cmd.Env = append(os.Environ(),
		"PHOTON_TEST_ROLE="+role,
		"PHOTON_TEST_PLUGIN_ID="+pluginID,
		"PHOTON_TEST_RENDEZVOUS="+rendezvousAddr,
	)
	if wrongPeer {
		cmd.Env = append(cmd.Env, "PHOTON_TEST_WRONG_PEER=1")
	}
	cmd.Stdout = &syncWriter{w: &s.stdout, mu: &s.mu}
	cmd.Stderr = &syncWriter{w: &s.errText, mu: &s.mu}
	if err := cmd.Start(); err != nil {
		t.Fatalf("запуск хоста %s: %v", name, err)
	}
	s.cmd = cmd
	return s
}

type syncWriter struct {
	w  *strings.Builder
	mu *sync.Mutex
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func (s *e2eSide) wait(t *testing.T, timeout time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(timeout):
		_ = s.cmd.Process.Kill()
		<-done
		t.Fatalf("хост %s не завершился за %s\n%s", s.name, timeout, s.output())
	}
}

func (s *e2eSide) output() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return "── " + s.name + " stdout ──\n" + s.stdout.String() +
		"\n── " + s.name + " stderr ──\n" + s.errText.String()
}

// verdicts извлекает строки фикстуры из журнала, который печатает хост.
func (s *e2eSide) verdicts() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for _, line := range strings.Split(s.stdout.String()+s.errText.String(), "\n") {
		i := strings.Index(line, "p2p_e2e:")
		if i < 0 {
			continue
		}
		k, v, _ := strings.Cut(strings.TrimSpace(line[i+len("p2p_e2e:"):]), "=")
		out[k] = v
	}
	return out
}

func (s *e2eSide) require(t *testing.T, key, want string) {
	t.Helper()
	got, ok := s.verdicts()[key]
	if !ok {
		t.Errorf("%s: фикстура не доложила %q\n%s", s.name, key, s.output())
		return
	}
	if got != want {
		t.Errorf("%s: %s=%q, ожидалось %q\n%s", s.name, key, got, want, s.output())
	}
}

func copyFile(src, dst string) error {
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, b, 0o755)
}

// ── acceptance ──────────────────────────────────────────────────────

func runE2E(t *testing.T, wrongPeer bool) (*e2eSide, *e2eSide) {
	t.Helper()
	bins := t.TempDir()
	hostBin := buildFromHEAD(t, "../../cmd/engram-plugin-host", bins)
	pluginBin := buildFromHEAD(t, "../plugins/p2pdial", bins)

	rv := startRendezvous(t)

	// Отвечающий поднимается первым: он ждёт offer, а инициатор его шлёт.
	// Порядок подключения к рандеву при этом не важен — ретранслятор
	// слепой и буферизует через TCP.
	b := startSide(t, hostBin, pluginBin, rv.addr(), "B(responder)", dialResponderID, "responder", false)
	a := startSide(t, hostBin, pluginBin, rv.addr(), "A(initiator)", dialInitiatorID, "initiator", wrongPeer)

	a.wait(t, 120*time.Second)
	b.wait(t, 120*time.Second)
	return a, b
}

// Полный путь: два процесса Photon, настоящий Pion, обмен и закрытие.
func TestP2PEndToEndTwoPhotonProcesses(t *testing.T) {
	a, b := runE2E(t, false)

	// 9. peer после establishment подтверждён с ОБЕИХ сторон: handshake
	//    ADR-041 сверил ключ, а не принял ожидание на веру.
	a.require(t, "peer", "confirmed")
	b.require(t, "peer", "confirmed")

	// 10-13. Все нагрузки дошли, побайтово и по порядку: отвечающий сверяет
	//        i-е принятое с i-м отправленным и прекращает при первом
	//        расхождении.
	a.require(t, "sent", "8")
	b.require(t, "received", "8")

	// 14. Закрывает отвечающий; инициатор узнаёт об этом событием.
	b.require(t, "close", "ok")
	a.require(t, "closed", "remote")

	for _, s := range []*e2eSide{a, b} {
		s.require(t, "done", "")
		if v, ok := s.verdicts()["offer_bytes"]; ok && v == "0" {
			t.Errorf("%s: пустой offer — согласование не собрало кандидатов", s.name)
		}
	}
	t.Logf("%s\n%s", a.output(), b.output())
}

//  15. Отрицательный путь: подменённое ожидание ключа отвергается handshake'ом
//     ADR-041 — и отвергается ОБЕИМИ сторонами, потому что transcript
//     включает оба публичных ключа и подписи перестают сходиться у каждой.
func TestP2PEndToEndWrongPeerKeyRejected(t *testing.T) {
	a, b := runE2E(t, true)

	a.require(t, "expect-wrong-peer", "")
	for _, s := range []*e2eSide{a, b} {
		v := s.verdicts()
		if v["peer"] == "confirmed" {
			t.Errorf("%s: согласование СОСТОЯЛОСЬ с подменённым ожиданием ключа\n%s",
				s.name, s.output())
		}
		if _, sent := v["sent"]; sent {
			t.Errorf("%s: сообщения пошли по неподтверждённой сессии\n%s", s.name, s.output())
		}
	}
	t.Logf("%s\n%s", a.output(), b.output())
}

var _ = fmt.Sprintf
var _ = context.Background
