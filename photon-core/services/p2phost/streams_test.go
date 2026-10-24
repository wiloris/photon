package p2phost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	edadapter "github.com/wiloris/photon/photon-core/adapters/ed25519"
	pionadapter "github.com/wiloris/photon/photon-core/adapters/pion"
	"github.com/wiloris/photon/photon-core/p2pmsg"
	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-core/services/p2phost/internal/legacy"
)

// ── помощники ───────────────────────────────────────────────────────────

func shortAnnounce(c *Config) { c.AnnounceWait = 700 * time.Millisecond }

// waitStream ждёт событие потока с нужным типом; прочие сохраняются.
func waitStream(t *testing.T, e *Executor, typ, streamID string, d time.Duration) Event {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		buf := seen[e]
		for i, ev := range buf {
			if ev.Type == typ && (streamID == "" || ev.StreamID == streamID) {
				seen[e] = append(buf[:i:i], buf[i+1:]...)
				return ev
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("не дождались %s потока %q; есть: %v", typ, streamID, seen[e])
		}
		seen[e] = append(seen[e], e.Poll(MaxPoll)...)
		time.Sleep(20 * time.Millisecond)
	}
}

// established — пара новых узлов с объявленными служебными потоками.
func established(t *testing.T, mod func(*Config)) (a, b *Executor, sa, sb string) {
	t.Helper()
	a, pa := newExec(t, mod)
	b, pb := newExec(t, mod)
	sa, sb = pair(t, a, b, pa, pb, nil, nil)
	waitEvent(t, a, EventStreamsReady, sa, 10*time.Second)
	waitEvent(t, b, EventStreamsReady, sb, 10*time.Second)
	return a, b, sa, sb
}

func mustOpen(t *testing.T, e *Executor, sid, service string) string {
	t.Helper()
	id, err := e.OpenStream(context.Background(), sid, service)
	if err != nil {
		t.Fatalf("OpenStream(%s): %v", service, err)
	}
	return id
}

func mustStreamSend(t *testing.T, e *Executor, id, msg string) {
	t.Helper()
	if err := e.StreamSend(id, []byte(msg)); err != nil {
		t.Fatalf("StreamSend: %v", err)
	}
}

func expectDefault(t *testing.T, from, to *Executor, sFrom, sTo, msg string) {
	t.Helper()
	if err := from.Send(sFrom, []byte(msg)); err != nil {
		t.Fatalf("Send по потоку по умолчанию: %v", err)
	}
	if ev := waitEvent(t, to, EventMessage, sTo, 10*time.Second); string(ev.Payload) != msg {
		t.Fatalf("поток по умолчанию: %q вместо %q", ev.Payload, msg)
	}
}

// ── два новых узла ──────────────────────────────────────────────────────

// Служебный поток в обе стороны при живом потоке по умолчанию; открыть
// может и инициатор, и отвечающий.
func TestServiceStreamsBothWaysWithDefaultStream(t *testing.T) {
	a, b, sa, sb := established(t, nil)

	idA := mustOpen(t, a, sa, "photon.call/1")
	op := waitStream(t, b, EventStreamOpened, "", 10*time.Second)
	if op.Service != "photon.call/1" || op.SessionID != sb {
		t.Fatalf("OPENED: %+v", op)
	}
	idB := op.StreamID

	mustStreamSend(t, a, idA, "ping")
	if ev := waitStream(t, b, EventStreamMessage, idB, 10*time.Second); string(ev.Payload) != "ping" || ev.SessionID != sb {
		t.Fatalf("сообщение потока: %+v", ev)
	}
	mustStreamSend(t, b, idB, "pong")
	if ev := waitStream(t, a, EventStreamMessage, idA, 10*time.Second); string(ev.Payload) != "pong" {
		t.Fatalf("ответ потока: %+v", ev)
	}

	// Поток по умолчанию — как раньше, в обе стороны.
	expectDefault(t, a, b, sa, sb, "текст A")
	expectDefault(t, b, a, sb, sa, "текст B")

	// Отвечающий открывает поток к инициатору.
	idB2 := mustOpen(t, b, sb, "app:dev.example/data")
	op2 := waitStream(t, a, EventStreamOpened, "", 10*time.Second)
	if op2.Service != "app:dev.example/data" {
		t.Fatalf("OPENED у инициатора: %+v", op2)
	}
	mustStreamSend(t, b, idB2, "x")
	if ev := waitStream(t, a, EventStreamMessage, op2.StreamID, 10*time.Second); string(ev.Payload) != "x" {
		t.Fatalf("поток от отвечающего: %+v", ev)
	}
}

// Сообщения разных разговоров не смешиваются, порядок внутри каждого
// сохраняется.
func TestStreamsAreIsolatedAndOrdered(t *testing.T) {
	a, b, sa, sb := established(t, nil)
	s1 := mustOpen(t, a, sa, "svc.one")
	s2 := mustOpen(t, a, sa, "svc.two")
	r1 := waitStream(t, b, EventStreamOpened, "", 10*time.Second)
	r2 := waitStream(t, b, EventStreamOpened, "", 10*time.Second)
	byService := map[string]string{r1.Service: r1.StreamID, r2.Service: r2.StreamID}

	const n = 50
	for i := 0; i < n; i++ {
		mustStreamSend(t, a, s1, fmt.Sprintf("one-%d", i))
		mustStreamSend(t, a, s2, fmt.Sprintf("two-%d", i))
		if err := a.Send(sa, []byte(fmt.Sprintf("def-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	got := map[string][]string{}
	deadline := time.Now().Add(15 * time.Second)
	for len(got["svc.one"]) < n || len(got["svc.two"]) < n || len(got["default"]) < n {
		if time.Now().After(deadline) {
			t.Fatalf("недополучено: one=%d two=%d default=%d", len(got["svc.one"]), len(got["svc.two"]), len(got["default"]))
		}
		for _, ev := range append(seen[b], b.Poll(MaxPoll)...) {
			switch ev.Type {
			case EventStreamMessage:
				for svc, id := range byService {
					if id == ev.StreamID {
						got[svc] = append(got[svc], string(ev.Payload))
					}
				}
			case EventMessage:
				got["default"] = append(got["default"], string(ev.Payload))
			}
		}
		seen[b] = nil
		time.Sleep(10 * time.Millisecond)
	}
	for i := 0; i < n; i++ {
		if got["svc.one"][i] != fmt.Sprintf("one-%d", i) || got["svc.two"][i] != fmt.Sprintf("two-%d", i) || got["default"][i] != fmt.Sprintf("def-%d", i) {
			t.Fatalf("порядок или смешение на %d: %q %q %q", i, got["svc.one"][i], got["svc.two"][i], got["default"][i])
		}
	}
	_ = sb
}

// Закрытие потока — только этого потока: сессия, поток по умолчанию и
// соседний поток живут.
func TestStreamCloseLeavesSessionAndNeighbours(t *testing.T) {
	a, b, sa, sb := established(t, nil)
	keep := mustOpen(t, a, sa, "svc.keep")
	gone := mustOpen(t, a, sa, "svc.gone")
	rk := waitStream(t, b, EventStreamOpened, "", 10*time.Second)
	rg := waitStream(t, b, EventStreamOpened, "", 10*time.Second)
	if rk.Service != "svc.keep" {
		rk, rg = rg, rk
	}

	if err := a.StreamClose(gone); err != nil {
		t.Fatal(err)
	}
	if ev := waitStream(t, a, EventStreamClosed, gone, 5*time.Second); ev.Reason != ReasonLocal {
		t.Fatalf("локальное закрытие: %+v", ev)
	}
	if ev := waitStream(t, b, EventStreamClosed, rg.StreamID, 10*time.Second); ev.Reason != ReasonRemote {
		t.Fatalf("закрытие у собеседника: %+v", ev)
	}
	if err := a.StreamSend(gone, []byte("x")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("send в закрытый поток: %v", err)
	}
	if err := b.StreamSend(rg.StreamID, []byte("x")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("send в закрытый собеседником поток: %v", err)
	}

	mustStreamSend(t, a, keep, "still")
	if ev := waitStream(t, b, EventStreamMessage, rk.StreamID, 10*time.Second); string(ev.Payload) != "still" {
		t.Fatalf("соседний поток: %+v", ev)
	}
	expectDefault(t, b, a, sb, sa, "default after close")
	if s, _ := a.Counts(); s != 1 {
		t.Fatalf("сессий у A: %d", s)
	}
	if a.StreamCount() != 1 || b.StreamCount() != 1 {
		t.Fatalf("потоков на учёте: A=%d B=%d", a.StreamCount(), b.StreamCount())
	}
}

// Негодное имя службы на проводе — отказ потоку, не сессии.
func TestPeerRefusesInvalidServiceOnWire(t *testing.T) {
	a, b, sa, sb := established(t, nil)
	s, _ := a.lookup("", sa)
	st, err := s.sess.OpenStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conv := p2pmsg.New(st)
	if err := conv.Send([]byte("open BAD NAME")); err != nil {
		t.Fatal(err)
	}
	reply, err := conv.Recv()
	if err != nil || string(reply) != "refused invalid_service" {
		t.Fatalf("ответ %q, %v", reply, err)
	}
	if b.StreamCount() != 0 {
		t.Fatal("отвергнутый поток поставлен на учёт")
	}
	expectDefault(t, a, b, sa, sb, "after refusal")
}

func TestOpenStreamArgumentErrors(t *testing.T) {
	a, _, sa, _ := established(t, nil)
	for _, bad := range []string{"", "Upper", "1abc", "has space", strings.Repeat("a", 65)} {
		if _, err := a.OpenStream(context.Background(), sa, bad); !errors.Is(err, ErrInvalidService) {
			t.Fatalf("%q: %v", bad, err)
		}
	}
	if _, err := a.OpenStream(context.Background(), "nope", "svc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("чужая сессия: %v", err)
	}
	if err := a.StreamSend("nope", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("чужой поток: %v", err)
	}
	if err := a.StreamClose("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("закрыть чужой поток: %v", err)
	}
	id := mustOpen(t, a, sa, "svc")
	if err := a.StreamSend(id, make([]byte, MaxPayload+1)); !errors.Is(err, ErrPayloadTooBig) {
		t.Fatalf("размер: %v", err)
	}
}

// Предел считается в обе стороны вместе: потоки собеседника занимают слоты.
func TestStreamLimit(t *testing.T) {
	a, b, sa, sb := established(t, nil)
	for i := 0; i < MaxStreams; i++ {
		mustOpen(t, b, sb, fmt.Sprintf("svc.%d", i))
	}
	deadline := time.Now().Add(10 * time.Second)
	for a.StreamCount() < MaxStreams && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := a.OpenStream(context.Background(), sa, "svc.extra"); !errors.Is(err, ErrTooManyStreams) {
		t.Fatalf("сверх предела: %v", err)
	}
	if _, err := b.OpenStream(context.Background(), sb, "svc.extra"); !errors.Is(err, ErrTooManyStreams) {
		t.Fatalf("сверх предела у открывающего: %v", err)
	}
	expectDefault(t, a, b, sa, sb, "after limit")
}

// Переполнение очереди сообщениями потока закрывает поток, не сессию.
func TestStreamSlowConsumerClosesOnlyStream(t *testing.T) {
	a, b, sa, sb := established(t, nil)
	id := mustOpen(t, a, sa, "svc.flood")
	waitStream(t, b, EventStreamOpened, "", 10*time.Second)
	for i := 0; i < MaxQueuedEvents+50; i++ {
		if err := a.StreamSend(id, []byte{'x'}); err != nil {
			break // собеседник уже закрыл поток
		}
	}
	// Ждём закрытия у A, НЕ разбирая очередь B: переполнение должно наступить.
	if ev := waitStream(t, a, EventStreamClosed, id, 15*time.Second); ev.Reason != ReasonRemote {
		t.Fatalf("у отправителя: %+v", ev)
	}
	var reason string
	msgs := 0
	for _, ev := range drain(b) {
		switch ev.Type {
		case EventStreamMessage:
			msgs++
		case EventStreamClosed:
			reason = ev.Reason
		case EventClosed:
			t.Fatalf("сессия закрылась: %s", ev.Reason)
		}
	}
	if reason != ReasonSlowConsumer {
		t.Fatalf("причина %q", reason)
	}
	if msgs >= MaxQueuedEvents+50 {
		t.Fatal("переполнения не было — тест ничего не проверил")
	}
	expectDefault(t, a, b, sa, sb, "session survives")
}

// Закрытие сессии снимает её потоки без утечек. Закрывший сторона событий о
// потоках не получает; у собеседника конец потоков может проявиться раньше
// P2P_CLOSED (на уровне потока он неотличим от закрытия собеседником —
// найдено -race-прогоном), но ПОСЛЕ P2P_CLOSED событий о её потоках нет.
func TestSessionCloseDropsStreamsWithoutLeaks(t *testing.T) {
	a, pa := newExec(t, nil)
	b, pb := newExec(t, nil)
	base := runtime.NumGoroutine()
	for i := 0; i < 3; i++ {
		sa, sb := pair(t, a, b, pa, pb, nil, nil)
		waitEvent(t, a, EventStreamsReady, sa, 10*time.Second)
		waitEvent(t, b, EventStreamsReady, sb, 10*time.Second)
		mustOpen(t, a, sa, "svc.a")
		mustOpen(t, b, sb, "svc.b")
		if err := a.Close(sa); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(15 * time.Second)
		var evB []Event
		for !hasClosed(evB, sb) {
			if time.Now().After(deadline) {
				t.Fatal("собеседник не увидел закрытия сессии")
			}
			evB = append(evB, drain(b)...)
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(200 * time.Millisecond)
		evB = append(evB, drain(b)...)
		for _, ev := range drain(a) {
			if ev.Type == EventStreamClosed {
				t.Fatalf("закрывшая сторона получила событие о потоке: %+v", ev)
			}
		}
		after := false
		for _, ev := range evB {
			if ev.Type == EventClosed && ev.SessionID == sb {
				after = true
			} else if after && ev.SessionID == sb {
				t.Fatalf("событие после P2P_CLOSED: %+v", ev)
			}
		}
	}
	if a.StreamCount() != 0 || b.StreamCount() != 0 {
		t.Fatalf("потоки на учёте: A=%d B=%d", a.StreamCount(), b.StreamCount())
	}
	deadline := time.Now().Add(10 * time.Second)
	for runtime.NumGoroutine() > base+4 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if g := runtime.NumGoroutine(); g > base+4 {
		t.Fatalf("горутины: было %d, стало %d", base, g)
	}
}

// ── старый узел (код 01fa134 без изменений) ────────────────────────────

// legacySHA256 — хеш p2phost.go в 01fa134; копия в internal/legacy отличается
// от него только строкой package и путём модуля (github.com/wiloris/photon-core
// → github.com/wiloris/photon/photon-core, механическая смена пути перед первым
// публичным релизом). Обе разницы снимаются перед сверкой, эталонный хеш — прежний.
const legacySHA256 = "360d3282077e1b7841b1f34571a4a7192fee1497074e2f68268c218a5165ab56"

// legacyModulePathCount — сколько раз новый путь модуля стоит в копии: ровно в
// трёх строках импорта оригинала. Лишнее или недостающее вхождение — уже не
// механическая смена пути, и сверка отказывает до хеширования.
const legacyModulePathCount = 3

// legacyVerbatimSum — хеш копии, приведённой к оригиналу 01fa134: строка
// package и путь модуля в строках импорта — единственные допустимые разницы.
func legacyVerbatimSum(raw []byte) (string, error) {
	newPath, oldPath := []byte("github.com/wiloris/photon/photon-core"), []byte("github.com/wiloris/photon-core")
	if n := bytes.Count(raw, newPath); n != legacyModulePathCount {
		return "", fmt.Errorf("путь модуля встречается %d раз, ожидалось %d", n, legacyModulePathCount)
	}
	orig := bytes.Replace(raw, []byte("\npackage legacy\n"), []byte("\npackage p2phost\n"), 1)
	orig = bytes.ReplaceAll(orig, newPath, oldPath)
	sum := sha256.Sum256(orig)
	return hex.EncodeToString(sum[:]), nil
}

func TestLegacyCopyIsVerbatim(t *testing.T) {
	raw, err := os.ReadFile("internal/legacy/p2phost.go")
	if err != nil {
		t.Fatal(err)
	}
	got, err := legacyVerbatimSum(raw)
	if err != nil || got != legacySHA256 {
		t.Fatalf("копия старого узла изменена: %s %v", got, err)
	}
}

// Отрицательная сторона: сверка ловит изменение копии, в том числе спрятанное
// за путём модуля.
func TestLegacyCopyCheckDetectsChanges(t *testing.T) {
	raw, err := os.ReadFile("internal/legacy/p2phost.go")
	if err != nil {
		t.Fatal(err)
	}
	newPath := "github.com/wiloris/photon/photon-core"
	mutations := map[string]func([]byte) []byte{
		"комментарий в конце": func(b []byte) []byte { return append(b, "\n// x\n"...) },
		"байт в теле": func(b []byte) []byte {
			i := bytes.Index(b, []byte("func "))
			b[i+5] ^= 1
			return b
		},
		"импорт заменён на другой пакет": func(b []byte) []byte {
			return bytes.Replace(b, []byte(newPath+"/p2pmsg"), []byte(newPath+"/ports2"), 1)
		},
		"лишний импорт с новым путём": func(b []byte) []byte {
			return bytes.Replace(b, []byte("import (\n"), []byte("import (\n\t_ \""+newPath+"/ports\"\n"), 1)
		},
		"старый путь вместо нового": func(b []byte) []byte {
			return bytes.Replace(b, []byte(newPath), []byte("github.com/wiloris/photon-core"), 1)
		},
	}
	for name, mutate := range mutations {
		got, err := legacyVerbatimSum(mutate(append([]byte(nil), raw...)))
		if err == nil && got == legacySHA256 {
			t.Errorf("%s: изменение копии не обнаружено", name)
		}
	}
}

type legacyNode struct {
	e    *legacy.Executor
	pair ports.IdentityPair
	seen []legacy.Event
}

func newLegacy(t *testing.T) *legacyNode {
	t.Helper()
	id := edadapter.New()
	p, err := id.Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	e, err := legacy.New(legacy.Config{Identity: id, Self: p, EstablishTimeout: 15 * time.Second, PeerLossTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Shutdown)
	return &legacyNode{e: e, pair: p}
}

func (n *legacyNode) wait(t *testing.T, typ, sid string, d time.Duration) legacy.Event {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		for i, ev := range n.seen {
			if ev.Type == typ && ev.SessionID == sid {
				n.seen = append(n.seen[:i:i], n.seen[i+1:]...)
				return ev
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("старый узел: не дождались %s; есть %v", typ, n.seen)
		}
		n.seen = append(n.seen, n.e.Poll(legacy.MaxPoll)...)
		time.Sleep(20 * time.Millisecond)
	}
}

// legacyExchange — общая часть обоих направлений: поток по умолчанию в обе
// стороны, служебные потоки к старому узлу не открываются, старый узел не
// получает ничего лишнего, закрытие различается как раньше.
func legacyExchange(t *testing.T, old *legacyNode, cur *Executor, sOld, sCur string) {
	t.Helper()
	old.wait(t, legacy.EventEstablished, sOld, 15*time.Second)
	waitEvent(t, cur, EventEstablished, sCur, 15*time.Second)

	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("old→new %d", i)
		if err := old.e.Send(sOld, []byte(msg)); err != nil {
			t.Fatal(err)
		}
		if ev := waitEvent(t, cur, EventMessage, sCur, 10*time.Second); string(ev.Payload) != msg {
			t.Fatalf("новый получил %q вместо %q", ev.Payload, msg)
		}
		msg = fmt.Sprintf("new→old %d", i)
		if err := cur.Send(sCur, []byte(msg)); err != nil {
			t.Fatal(err)
		}
		if ev := old.wait(t, legacy.EventMessage, sOld, 10*time.Second); string(ev.Payload) != msg {
			t.Fatalf("старый получил %q вместо %q", ev.Payload, msg)
		}
	}
	// Пустое сообщение после «привета» по-прежнему доходит пустым.
	if err := cur.Send(sCur, nil); err != nil {
		t.Fatal(err)
	}
	if ev := old.wait(t, legacy.EventMessage, sOld, 10*time.Second); len(ev.Payload) != 0 {
		t.Fatalf("пустое сообщение: %q", ev.Payload)
	}

	start := time.Now()
	if _, err := cur.OpenStream(context.Background(), sCur, "photon.call/1"); !errors.Is(err, ErrPeerUnsupported) {
		t.Fatalf("служебный поток к старому узлу: %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("отказ не ограничен AnnounceWait")
	}
	for _, ev := range seen[cur] {
		if ev.Type == EventStreamsReady {
			t.Fatal("старый узел «объявил» служебные потоки")
		}
	}

	// После отказа поток по умолчанию жив, и старый узел ничего лишнего не
	// получил: ни объявления, ни служебного кадра.
	if err := old.e.Send(sOld, []byte("after")); err != nil {
		t.Fatal(err)
	}
	if ev := waitEvent(t, cur, EventMessage, sCur, 10*time.Second); string(ev.Payload) != "after" {
		t.Fatalf("после отказа: %q", ev.Payload)
	}
	time.Sleep(300 * time.Millisecond)
	for _, ev := range append(old.seen, old.e.Poll(legacy.MaxPoll)...) {
		if ev.Type == legacy.EventMessage {
			t.Fatalf("старый узел получил лишнее сообщение: %q", ev.Payload)
		}
	}
	old.seen = nil

	if err := cur.Close(sCur); err != nil {
		t.Fatal(err)
	}
	if ev := old.wait(t, legacy.EventClosed, sOld, 15*time.Second); ev.Reason != legacy.ReasonRemote && ev.Reason != legacy.ReasonFailed {
		t.Fatalf("причина у старого: %q", ev.Reason)
	}
}

func TestNewInitiatorOldResponder(t *testing.T) {
	cur, pc := newExec(t, shortAnnounce)
	old := newLegacy(t)
	ctx := context.Background()
	neg, offer, err := cur.Offer(ctx, old.pair.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	sOld, answer, err := old.e.Accept(ctx, pc.Public, offer, nil)
	if err != nil {
		t.Fatal(err)
	}
	sCur, err := cur.Complete(ctx, neg, answer)
	if err != nil {
		t.Fatal(err)
	}
	legacyExchange(t, old, cur, sOld, sCur)
}

func TestOldInitiatorNewResponder(t *testing.T) {
	cur, pc := newExec(t, shortAnnounce)
	old := newLegacy(t)
	ctx := context.Background()
	neg, offer, err := old.e.Offer(ctx, pc.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	sCur, answer, err := cur.Accept(ctx, old.pair.Public, offer, nil)
	if err != nil {
		t.Fatal(err)
	}
	sOld, err := old.e.Complete(ctx, neg, answer)
	if err != nil {
		t.Fatal(err)
	}
	legacyExchange(t, old, cur, sOld, sCur)
}

// Два старых узла — эталон: тест совместимости измеряет то же поведение, что
// и до изменений, а не своё представление о нём.
func TestOldOldBaseline(t *testing.T) {
	a, b := newLegacy(t), newLegacy(t)
	ctx := context.Background()
	neg, offer, err := a.e.Offer(ctx, b.pair.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	sb, answer, err := b.e.Accept(ctx, a.pair.Public, offer, nil)
	if err != nil {
		t.Fatal(err)
	}
	sa, err := a.e.Complete(ctx, neg, answer)
	if err != nil {
		t.Fatal(err)
	}
	a.wait(t, legacy.EventEstablished, sa, 15*time.Second)
	b.wait(t, legacy.EventEstablished, sb, 15*time.Second)
	if err := a.e.Send(sa, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if ev := b.wait(t, legacy.EventMessage, sb, 10*time.Second); string(ev.Payload) != "x" {
		t.Fatalf("%q", ev.Payload)
	}
}

func hasClosed(evs []Event, sid string) bool {
	for _, ev := range evs {
		if ev.Type == EventClosed && ev.SessionID == sid {
			return true
		}
	}
	return false
}

// ── порядок, на котором держится совместимость (детерминированно) ────────

// orderSession фиксирует порядок: поток по умолчанию → объявление.
type orderSession struct {
	peer      ports.PublicKey
	release   chan struct{} // AcceptStream отвечающего ждёт его
	st        *fakeStream
	mu        sync.Mutex
	events    []string
	announced chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	// helloAtAnnounce — сколько байт потока по умолчанию было записано к
	// моменту объявления (снимок в OpenUniStream, а не после него).
	helloAtAnnounce int
}

func (s *orderSession) log(ev string) {
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.mu.Unlock()
}
func (s *orderSession) OpenStream(context.Context) (ports.Stream, error) {
	s.log("default-open")
	return s.st, nil
}
func (s *orderSession) AcceptStream(ctx context.Context) (ports.Stream, error) {
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.mu.Lock()
	first := len(s.events) == 0 || s.events[len(s.events)-1] != "default-accepted"
	s.mu.Unlock()
	if first {
		s.log("default-accepted")
		return s.st, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (s *orderSession) OpenUniStream(context.Context) (ports.SendStream, error) {
	s.helloAtAnnounce = s.st.w.Len()
	s.log("announce")
	close(s.announced)
	return nil, io.EOF
}
func (s *orderSession) AcceptUniStream(ctx context.Context) (ports.RecvStream, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (s *orderSession) RemoteEndpoint() ports.Endpoint { return "fake" }
func (s *orderSession) Close() error {
	s.closeOnce.Do(func() { close(s.done) })
	return nil
}
func (s *orderSession) Peer() ports.PublicKey { return s.peer }

type orderP2P struct{ sess *orderSession }

func (f orderP2P) CreateOffer(context.Context, ports.PublicKey) ([]byte, error) {
	return []byte("o"), nil
}
func (f orderP2P) AcceptOffer(context.Context, ports.PublicKey, []byte) ([]byte, ports.Session, error) {
	return []byte("a"), f.sess, nil
}
func (f orderP2P) CompleteSession(context.Context, ports.PublicKey, []byte) (ports.Session, error) {
	return f.sess, nil
}
func (f orderP2P) Close() error { return nil }

// Отвечающий объявляет только ПОСЛЕ того, как принял поток по умолчанию:
// иначе служебный поток нового инициатора мог бы занять место потока по
// умолчанию (проект записи §2.2).
func TestResponderAnnouncesOnlyAfterDefaultStream(t *testing.T) {
	osn := newOrderSession()
	e, _ := newExec(t, func(c *Config) {
		c.newAdapter = func(pionadapter.Config) (ports.P2P, error) { return orderP2P{sess: osn}, nil }
	})
	if _, _, err := e.Accept(context.Background(), []byte("peer"), []byte("offer"), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-osn.announced:
		t.Fatal("объявление ушло до приёма потока по умолчанию")
	case <-time.After(300 * time.Millisecond):
	}
	close(osn.release)
	select {
	case <-osn.announced:
	case <-time.After(5 * time.Second):
		t.Fatal("объявления нет и после приёма потока по умолчанию")
	}
	osn.mu.Lock()
	defer osn.mu.Unlock()
	if len(osn.events) < 2 || osn.events[0] != "default-accepted" || osn.events[1] != "announce" {
		t.Fatalf("порядок: %v", osn.events)
	}
}

// Инициатор объявляет только после того, как открыл поток по умолчанию и
// записал «привет».
func TestInitiatorAnnouncesOnlyAfterHello(t *testing.T) {
	osn := newOrderSession()
	e, _ := newExec(t, func(c *Config) {
		c.newAdapter = func(pionadapter.Config) (ports.P2P, error) { return orderP2P{sess: osn}, nil }
	})
	neg, _, err := e.Offer(context.Background(), []byte("peer"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Complete(context.Background(), neg, []byte("answer")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-osn.announced:
	case <-time.After(5 * time.Second):
		t.Fatal("объявления нет")
	}
	osn.mu.Lock()
	events := append([]string(nil), osn.events...)
	osn.mu.Unlock()
	if len(events) < 2 || events[0] != "default-open" || events[1] != "announce" {
		t.Fatalf("порядок: %v", events)
	}
	osn.mu.Lock()
	at := osn.helloAtAnnounce
	osn.mu.Unlock()
	if at != 4 {
		t.Fatalf("к объявлению записано %d байт потока по умолчанию, ожидался «привет» (4)", at)
	}
}

// blockingReader молчит до закрытия сессии.
type blockingReader struct{ done chan struct{} }

func (r blockingReader) Read([]byte) (int, error) {
	<-r.done
	return 0, io.EOF
}

func newOrderSession() *orderSession {
	done := make(chan struct{})
	return &orderSession{peer: ports.PublicKey("peer"), release: make(chan struct{}), announced: make(chan struct{}),
		done: done, st: &fakeStream{r: blockingReader{done}}}
}

// После закрытия сессии события о её потоках не кладутся — по построению,
// а не по удачному таймингу.
func TestNoStreamEventsAfterSessionClosed(t *testing.T) {
	a, b, sa, _ := established(t, nil)
	id := mustOpen(t, a, sa, "svc.late")
	waitStream(t, b, EventStreamOpened, "", 10*time.Second)
	ss, _ := a.lookupStream(id)
	if err := a.Close(sa); err != nil {
		t.Fatal(err)
	}
	// P2P_CLOSED шлёт горутина сессии, выходя (emitClosed), — асинхронно к
	// Close: ждать его, а не читать очередь сразу, иначе законное закрытие,
	// пришедшее после первого drain, выглядело «событием после закрытия»
	// (прежний флейк, 4 %). Второе P2P_CLOSED ниже по-прежнему поймается.
	waitEvent(t, a, EventClosed, sa, 10*time.Second)
	drain(a)
	a.pushStreamMessage(ss, []byte("late"))
	a.closeStream(ss, ReasonRemote)
	a.pushIfAlive(ss.sess, Event{Type: EventStreamsReady, SessionID: sa}, false)
	if evs := drain(a); len(evs) != 0 {
		t.Fatalf("события после закрытия сессии: %+v", evs)
	}
}
