package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Acceptance вертикального среза ADR-075 на ДВУХ НАСТОЯЩИХ ПРОЦЕССАХ.
//
// Два объекта в одном процессе заменой не являются. Phase 11: пять плагинов на
// настоящих процессах нашли три бага, невидимых для fakeSpawner и net.Pipe —
// они не порождают процессов, не имеют кодов возврата и не воспроизводят
// тайминги настоящего завершения. Phase 12.5: код был зелёным на фиктивном
// транспорте и сломался на первом же живом прогоне.
//
// Здесь оба процесса — настоящие: своя личность, свой ICE-агент, свой
// DTLS-контекст, своя SCTP-ассоциация, свой адресный порядок в памяти.

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "p2pslice")
	if err != nil {
		fmt.Fprintln(os.Stderr, "tempdir:", err)
		os.Exit(2)
	}
	binPath = filepath.Join(dir, "p2pslice")
	build := exec.Command("go", "build", "-o", binPath, ".")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "сборка p2pslice: %v\n%s", err, out)
		os.Exit(2)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// watcher собирает вывод процесса и выдёргивает из него адрес рандеву.
//
// Адрес нужен ДО завершения процесса, поэтому читать его из CombinedOutput
// нельзя: отвечающий живёт до конца сценария. Разбор идёт по мере поступления
// строк.
type watcher struct {
	mu      sync.Mutex
	all     bytes.Buffer
	pending []byte
	addr    chan string
	once    sync.Once
}

func newWatcher() *watcher { return &watcher{addr: make(chan string, 1)} }

func (w *watcher) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.all.Write(p)
	w.pending = append(w.pending, p...)
	for {
		i := bytes.IndexByte(w.pending, '\n')
		if i < 0 {
			break
		}
		line := string(w.pending[:i])
		w.pending = w.pending[i+1:]
		if s, found := strings.CutPrefix(line, "SIGNAL "); found {
			w.once.Do(func() { w.addr <- s })
		}
	}
	return len(p), nil
}

func (w *watcher) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.all.String()
}

type pairResult struct {
	responder, initiator string
	respErr, initErr     error
}

// runPair поднимает два процесса одного сценария и дожидается обоих.
func runPair(t *testing.T, scenario string, extra ...string) pairResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	args := func(role string, rest ...string) []string {
		return append([]string{"-mode", role, "-scenario", scenario}, append(rest, extra...)...)
	}

	rw := newWatcher()
	resp := exec.CommandContext(ctx, binPath, args("responder")...)
	resp.Stdout, resp.Stderr = rw, rw
	if err := resp.Start(); err != nil {
		t.Fatalf("запуск отвечающего: %v", err)
	}
	respDone := make(chan error, 1)
	go func() { respDone <- resp.Wait() }()

	var addr string
	select {
	case addr = <-rw.addr:
	case err := <-respDone:
		t.Fatalf("отвечающий завершился, не назвав адрес рандеву (%v)\n%s", err, rw)
	case <-time.After(30 * time.Second):
		t.Fatalf("отвечающий не назвал адрес рандеву за 30 с\n%s", rw)
	}

	iw := newWatcher()
	init := exec.CommandContext(ctx, binPath, args("initiator", "-signal", addr)...)
	init.Stdout, init.Stderr = iw, iw
	initErr := init.Run()

	var respErr error
	select {
	case respErr = <-respDone:
	case <-time.After(60 * time.Second):
		_ = resp.Process.Kill()
		respErr = <-respDone
		t.Errorf("отвечающий не завершился за 60 с после инициатора")
	}

	return pairResult{responder: rw.String(), initiator: iw.String(), respErr: respErr, initErr: initErr}
}

// requirePass — оба процесса обязаны завершиться успехом и объявить его сами.
// Кода возврата мало: процесс, вышедший до единой проверки, тоже даёт ноль.
func (r pairResult) requirePass(t *testing.T) {
	t.Helper()
	if r.respErr != nil {
		t.Errorf("отвечающий завершился с ошибкой: %v", r.respErr)
	}
	if r.initErr != nil {
		t.Errorf("инициатор завершился с ошибкой: %v", r.initErr)
	}
	for _, side := range []struct{ name, out string }{
		{"отвечающий", r.responder}, {"инициатор", r.initiator},
	} {
		if !strings.Contains(side.out, "RESULT PASS") {
			t.Errorf("%s не объявил RESULT PASS", side.name)
		}
	}
	if t.Failed() {
		t.Logf("── отвечающий ──\n%s\n── инициатор ──\n%s", r.responder, r.initiator)
	}
}

// requireMarker — конкретное свойство обязано быть НАБЛЮДЕНО, а не выведено из
// кода возврата. Без этого сценарий, тихо не дошедший до проверки, был бы
// зелёным — тот же класс ошибки, что «отвергнут без сети» в Phase 13.5.
func (r pairResult) requireMarker(t *testing.T, side, substr string) {
	t.Helper()
	out := r.responder
	if side == "initiator" {
		out = r.initiator
	}
	if !strings.Contains(out, substr) {
		t.Errorf("в выводе (%s) нет ожидаемого %q\n%s", side, substr, out)
	}
}

// ─────────────────── основной acceptance ───────────────────

// A → connect → B → message → reply → disconnect → B наблюдает отказ.
func TestAcceptanceEchoAndDisconnect(t *testing.T) {
	r := runPair(t, "echo")
	r.requirePass(t)
	r.requireMarker(t, "responder", `B получил побайтово: "hello from A"`)
	r.requireMarker(t, "initiator", `A получил ответ побайтово: "hello from B"`)
	r.requireMarker(t, "responder", "B заметил разрыв на Recv:")
	r.requireMarker(t, "responder", "B: проба OpenStream после разрыва:")
	t.Logf("как разрыв наблюдается на самом деле:\n%s", grep(r.responder, "разрыв"))
}

// ─────────────────── границы кадра ───────────────────

func TestZeroLengthFrame(t *testing.T) {
	r := runPair(t, "zero")
	r.requirePass(t)
	r.requireMarker(t, "responder", "принял пустое сообщение, отличимое от конца потока")
	r.requireMarker(t, "responder", "граница кадра не сбилась")
}

func TestExactMaxMessageSize(t *testing.T) {
	r := runPair(t, "max")
	r.requirePass(t)
	r.requireMarker(t, "responder", "B принял 1048576 байт побайтово")
}

func TestOverMaxRejectedLocallyWithoutTouchingWire(t *testing.T) {
	r := runPair(t, "oversize-local")
	r.requirePass(t)
	r.requireMarker(t, "initiator", "1 MiB + 1 отвергнут локально")
	r.requireMarker(t, "responder", "на провод отвергнутое не ушло")
}

func TestOverMaxOnWireIsTerminal(t *testing.T) {
	r := runPair(t, "oversize-wire")
	r.requirePass(t)
	r.requireMarker(t, "responder", "B отверг негодный заголовок")
}

// ─────────────────── нарушенное кадрирование ───────────────────

func TestMalformedIncompleteLength(t *testing.T) {
	r := runPair(t, "short-length")
	r.requirePass(t)
	r.requireMarker(t, "responder", "B отверг оборванный кадр")
}

func TestTruncatedPayload(t *testing.T) {
	r := runPair(t, "truncated")
	r.requirePass(t)
	r.requireMarker(t, "responder", "B отверг оборванный кадр")
}

func TestDisconnectDuringFrame(t *testing.T) {
	r := runPair(t, "disconnect-midframe")
	r.requirePass(t)
	r.requireMarker(t, "responder", "B заметил обрыв посреди кадра")
	t.Logf("наблюдённое:\n%s", grep(r.responder, "обрыв посреди кадра"))
}

// ─────────────────── личность и завершение ───────────────────

// Чужой ключ отвергается ОБЕИМИ сторонами: transcript ADR-041 включает оба
// публичных ключа, поэтому расхождение ожидания ломает проверку подписи у
// каждой. Это сильнее, чем «инициатор заметил подмену».
func TestWrongPeerIdentityRejected(t *testing.T) {
	r := runPair(t, "wrong-peer")
	r.requirePass(t)
	r.requireMarker(t, "initiator", "ожидание испорчено: подставлен чужой ключ")
	for _, side := range []string{"initiator", "responder"} {
		out := r.initiator
		if side == "responder" {
			out = r.responder
		}
		if !strings.Contains(out, "согласование отвергнуто") && !strings.Contains(out, "Peer() пуст") {
			t.Errorf("%s не отверг подмену ключа:\n%s", side, out)
		}
	}
}

// Half-close: завершение отправки не закрывает приём. Свойство заявлено
// p2p-session.md §7 и до этого среза не было покрыто ни одним тестом.
func TestCleanShutdownHalfClose(t *testing.T) {
	r := runPair(t, "clean-shutdown")
	r.requirePass(t)
	r.requireMarker(t, "responder", "io.EOF ровно на границе кадра")
	r.requireMarker(t, "responder", "приём и отправка независимы")
	r.requireMarker(t, "initiator", "A принял ответ ПОСЛЕ своего half-close")
}

// ─────────────────── объём и непрозрачность ───────────────────

func TestRepeatedMessages(t *testing.T) {
	r := runPair(t, "repeated")
	r.requirePass(t)
	r.requireMarker(t, "responder", "B принял 100 сообщений")
}

func TestBinaryPayloadIsOpaque(t *testing.T) {
	r := runPair(t, "binary")
	r.requirePass(t)
	r.requireMarker(t, "responder", "Photon их не интерпретирует")
}

// ─────────────────── измерения ───────────────────

// Числа записываются, а не оцениваются. Оптимизации до их получения не было.
func TestMeasurements(t *testing.T) {
	r := runPair(t, "measure", "-rtt-iters", "20")
	r.requirePass(t)

	got := measures(r.initiator)
	for _, key := range []string{
		"establish_ms", "rtt_us_payload_0", "rtt_us_payload_64",
		"rtt_us_payload_1024", "rtt_us_payload_65536", "rtt_us_payload_1048576",
	} {
		if _, found := got[key]; !found {
			t.Errorf("не снят замер %s", key)
		}
	}
	var b strings.Builder
	for _, k := range []string{
		"establish_ms",
		"rtt_us_payload_0", "rtt_us_payload_64", "rtt_us_payload_1024",
		"rtt_us_payload_65536", "rtt_us_payload_1048576",
		"frame_bytes_payload_0", "frame_bytes_payload_1048576",
	} {
		if v, found := got[k]; found {
			fmt.Fprintf(&b, "\n  %-28s %s", k, v)
		}
	}
	t.Logf("замеры (loopback, два процесса):%s", b.String())
}

func measures(out string) map[string]string {
	m := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		if rest, found := strings.CutPrefix(line, "MEASURE "); found {
			if parts := strings.Fields(rest); len(parts) == 2 {
				m[parts[0]] = parts[1]
			}
		}
	}
	return m
}

func grep(out, substr string) string {
	var b strings.Builder
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, substr) {
			b.WriteString("  " + line + "\n")
		}
	}
	return b.String()
}
