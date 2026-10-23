package main

import (
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/wiloris/photon-core/services/dispatcher"
)

// Тесты доставки событий через границу ABI (Phase 15.5).
//
// Колбэки здесь — НАСТОЯЩИЕ C-функции (см. преамбулу testsupport.go), а не
// Go-замыкания: реентерабельность иначе не проверить. Клиент зовёт Photon_*
// изнутри колбэка, и весь смысл проверок в том, что он это может.

// newEventCore — инстанс поверх фиктивного транспорта, с автоуничтожением.
func newEventCore(t *testing.T, tr *fakeTransport) unsafe.Pointer {
	t.Helper()
	h, err := tsNewCoreWithFakeDispatcher(dispatcher.Config{
		Endpoint:        "fake://bridge",
		SessionToken:    "tok",
		Transport:       tr,
		ConnectTimeout:  time.Second,
		CallTimeout:     time.Second,
		EventBackoffMin: 20 * time.Millisecond,
		EventBackoffMax: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("создание инстанса: %v", err)
	}
	tsEventReset(h)
	t.Cleanup(func() { tsDestroy(h) })
	return h
}

func waitCalls(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if tsEventCalls() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("колбэк вызван %d раз, ожидалось %d", tsEventCalls(), n)
}

// Подписка запускается ПЕРВЫМ ненулевым колбэком — без единого SendAction.
func TestEvents_FirstCallbackStartsSubscription(t *testing.T) {
	tr := &fakeTransport{}
	h := newEventCore(t, tr)

	tsSetEventCallbackKind(h, 0)
	waitCalls(t, 1)

	if got := tsEventLast(); got != `{"type":"CONNECTED"}` {
		t.Fatalf("первое событие: %s", got)
	}
	tr.waitSub(t)
}

// Событие доходит до C-колбэка целиком и в форме спецификации.
func TestEvents_MessageReachesCallback(t *testing.T) {
	tr := &fakeTransport{}
	h := newEventCore(t, tr)
	tsSetEventCallbackKind(h, 0)
	waitCalls(t, 1)

	tr.waitSub(t).sendPush(t, `{"event":"posted","data":{"id":"p1"}}`)
	waitCalls(t, 2)

	want := `{"type":"MESSAGE","event":"posted","data":{"id":"p1"}}`
	if got := tsEventLast(); got != want {
		t.Fatalf("получено %s\nожидалось %s", got, want)
	}
}

// NULL-колбэк отцепляет адресата, подписку не трогает: события отбрасываются,
// но насос жив и после повторной установки доставка возобновляется.
func TestEvents_NilCallbackDropsWithoutStoppingPump(t *testing.T) {
	tr := &fakeTransport{}
	h := newEventCore(t, tr)
	tsSetEventCallbackKind(h, 0)
	waitCalls(t, 1)
	sub := tr.waitSub(t)

	tsSetEventCallbackNil(h)
	before := tsEventCalls()
	sub.sendPush(t, `{"event":"posted"}`)
	time.Sleep(200 * time.Millisecond)
	if tsEventCalls() != before {
		t.Fatal("событие доставлено после снятия колбэка")
	}

	tsSetEventCallbackKind(h, 0)
	sub.sendPush(t, `{"event":"posted","data":{"again":true}}`)
	waitCalls(t, before+1)
	if !strings.Contains(tsEventLast(), "again") {
		t.Fatalf("доставка не возобновилась: %s", tsEventLast())
	}
}

// ── Реентерабельность: колбэк зовёт Photon_* обратно ────────────────────
//
// Все три теста упали бы дедлоком, если бы колбэк вызывался под c.mu: этот
// мьютекс берут Destroy, SetEventCallback, GetLastError и SendAction.

func TestEvents_CallbackMayCallSendAction(t *testing.T) {
	tr := &fakeTransport{}
	h := newEventCore(t, tr)
	tsSetEventCallbackKind(h, 1) // зовёт Photon_SendAction

	done := make(chan struct{})
	go func() { waitCalls(t, 1); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("дедлок: колбэк вызван под c.mu и SendAction не вернулся")
	}
	if tsReentrantRC() == -1 {
		t.Fatal("SendAction из колбэка не вернул управление")
	}
}

func TestEvents_CallbackMayCallGetLastError(t *testing.T) {
	tr := &fakeTransport{}
	h := newEventCore(t, tr)
	tsSetEventCallbackKind(h, 2) // зовёт Photon_GetLastError

	done := make(chan struct{})
	go func() { waitCalls(t, 1); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("дедлок: GetLastError из колбэка не вернулся")
	}
	if tsReentrantRC() != 0 {
		t.Fatal("GetLastError из колбэка не выполнился")
	}
}

// ── Время жизни и конкурентный Destroy (ADR-059) ────────────────────────
//
// Окно времени жизни PhotonHandle* h — до возврата первого НЕ-реентерабельного
// Photon_Destroy(h). Тесты ниже:
//   - обращаются к h только внутри окна;
//   - завершение уничтожения наблюдают через Go-объект *core, полученный,
//     пока h заведомо жив, — а не через handle;
//   - перекрытие вызовов создают удержанием Close подписного соединения
//     (fakeTransport.holdClose), а не таймингом планировщика.
//
// «Ровно один раз» держится двумя гарантиями, которые повторный teardown
// нарушил бы громко: close уже закрытого teardownDone — паника, повторный
// C.free ячейки — аварийное завершение процесса.

// coreOf — *core для h. Вызывать ТОЛЬКО внутри окна времени жизни h.
func coreOf(t *testing.T, h unsafe.Pointer) *core {
	t.Helper()
	v, ok := tsLookup(h)
	if !ok {
		t.Fatal("handle не резолвится внутри окна времени жизни")
	}
	c, ok := v.(*core)
	if !ok {
		t.Fatalf("за handle не *core: %T", v)
	}
	return c
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func newHeldTransport() *fakeTransport {
	return &fakeTransport{
		holdClose:    make(chan struct{}),
		closeEntered: make(chan struct{}, 1),
	}
}

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("не дождались: %s", what)
	}
}

// Удержание перекрытия: за это время запущенный вызов гарантированно дошёл
// до ожидания. Вывод теста от длины не зависит — проверяется только, что
// ожидающий вызов НЕ вернулся, пока уничтожение удержано.
const overlapHold = 300 * time.Millisecond

// Реентерабельный Photon_Destroy возвращается немедленно, а уничтожение
// завершается после выхода из колбэка (ADR-045 §7.1). После возврата handle
// больше не трогается: завершение видно по teardownDone.
func TestEvents_ReentrantDestroyDoesNotDeadlock(t *testing.T) {
	tr := &fakeTransport{}
	h, err := tsNewCoreWithFakeDispatcher(dispatcher.Config{
		Endpoint: "fake://bridge", SessionToken: "tok", Transport: tr,
		ConnectTimeout: time.Second, CallTimeout: time.Second,
		EventBackoffMin: 20 * time.Millisecond, EventBackoffMax: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("создание инстанса: %v", err)
	}
	c := coreOf(t, h) // внутри окна: ни одного Destroy ещё не было
	tsEventReset(h)
	tsSetEventCallbackKind(h, 3) // колбэк зовёт Photon_Destroy(h)

	done := make(chan struct{})
	go func() {
		for !tsDestroyReturned() {
			time.Sleep(5 * time.Millisecond)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("дедлок: реентерабельный Photon_Destroy не вернулся")
	}

	// С этого момента h не используется: окно закроется, когда насос выполнит
	// отложенное уничтожение, и снаружи это не наблюдаемо иначе, чем по
	// teardownDone.
	waitSignal(t, c.teardownDone, "отложенное уничтожение после возврата из колбэка")
}

// Два конкурентных Destroy, начатых внутри окна: уничтожение одно, второй
// вызов из чужого потока возвращается не раньше его завершения
// (ADR-045 §7.3, event-delivery.md §14.4 п. 3, ADR-059 п. 1).
func TestEvents_ConcurrentDestroyRunsTeardownOnce(t *testing.T) {
	tr := newHeldTransport()
	h, err := tsNewCoreWithFakeDispatcher(dispatcher.Config{
		Endpoint: "fake://bridge", SessionToken: "tok", Transport: tr,
		ConnectTimeout: time.Second, CallTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("создание инстанса: %v", err)
	}
	c := coreOf(t, h)
	tsEventReset(h)
	tsSetEventCallbackKind(h, 0)
	waitCalls(t, 1)
	tr.waitSub(t)

	first := make(chan struct{})
	go func() { tsDestroy(h); close(first) }()
	// Первый Destroy вошёл в уничтожение и стоит в удержанном Close: handle
	// ещё не освобождён, окно открыто.
	waitSignal(t, tr.closeEntered, "первый Destroy вошёл в уничтожение")

	second := make(chan struct{})
	go func() { tsDestroy(h); close(second) }()

	time.Sleep(overlapHold)
	if isClosed(first) || isClosed(second) {
		t.Fatal("Destroy вернулся, пока уничтожение удержано")
	}
	if isClosed(c.teardownDone) {
		t.Fatal("teardownDone закрыт до завершения уничтожения")
	}
	calls := tsEventCalls()

	close(tr.holdClose)
	waitSignal(t, first, "возврат первого Destroy")
	waitSignal(t, second, "возврат второго Destroy")

	// Оба вернулись — окно закрыто, h больше не трогается.
	if !isClosed(c.teardownDone) {
		t.Fatal("оба Destroy вернулись, а уничтожение не завершено")
	}
	time.Sleep(100 * time.Millisecond)
	if tsEventCalls() != calls {
		t.Fatalf("колбэк вызван после возврата Destroy: было %d, стало %d", calls, tsEventCalls())
	}
}

// Реентерабельный Destroy окно не закрывает: Destroy из чужого потока,
// начатый после его возврата, но до завершения отложенного уничтожения, ждёт
// это уничтожение (ADR-045 §7.3, строка «последующий, из чужого потока»;
// ADR-059 п. 1).
func TestEvents_DestroyFromOtherThreadWaitsForDeferredTeardown(t *testing.T) {
	tr := newHeldTransport()
	h, err := tsNewCoreWithFakeDispatcher(dispatcher.Config{
		Endpoint: "fake://bridge", SessionToken: "tok", Transport: tr,
		ConnectTimeout: time.Second, CallTimeout: time.Second,
		EventBackoffMin: 20 * time.Millisecond, EventBackoffMax: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("создание инстанса: %v", err)
	}
	c := coreOf(t, h)
	tsEventReset(h)
	tsSetEventCallbackKind(h, 3) // колбэк зовёт Photon_Destroy(h) реентерабельно

	// Реентерабельный вызов вернулся, насос вышел из колбэка и стоит в
	// удержанном Close: отложенное уничтожение ещё не началось.
	waitSignal(t, tr.closeEntered, "насос закрывает подписку после реентерабельного Destroy")
	if !tsDestroyReturned() {
		t.Fatal("реентерабельный Destroy не вернулся до закрытия подписки")
	}

	other := make(chan struct{})
	go func() { tsDestroy(h); close(other) }() // первый НЕ-реентерабельный: окно открыто

	time.Sleep(overlapHold)
	if isClosed(other) {
		t.Fatal("Destroy из чужого потока вернулся до завершения отложенного уничтожения")
	}
	if isClosed(c.teardownDone) {
		t.Fatal("teardownDone закрыт до завершения уничтожения")
	}

	close(tr.holdClose)
	waitSignal(t, other, "возврат Destroy из чужого потока")
	if !isClosed(c.teardownDone) {
		t.Fatal("Destroy вернулся, а уничтожение не завершено")
	}
}

// Отложенное уничтожение снимает handle на насосе, вне экспортируемого вызова.
// Если после него не будет ни одного вызова Photon_*, ячейка всё равно
// освобождается — проходом освобождения при снятии (ADR-059 п. 5, редакция 3).
// Без него ячейка висела бы в retired до вызова, которого может не быть.
//
// Наблюдение — состояние реестра, а не handle: к h после реентерабельного
// Destroy тест не обращается.
func TestEvents_DeferredTeardownReclaimsCellWithoutFurtherCalls(t *testing.T) {
	tr := &fakeTransport{}
	h, err := tsNewCoreWithFakeDispatcher(dispatcher.Config{
		Endpoint: "fake://bridge", SessionToken: "tok", Transport: tr,
		ConnectTimeout: time.Second, CallTimeout: time.Second,
		EventBackoffMin: 20 * time.Millisecond, EventBackoffMax: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("создание инстанса: %v", err)
	}
	c := coreOf(t, h)
	tsEventReset(h)
	tsSetEventCallbackKind(h, 3) // колбэк зовёт Photon_Destroy(h) реентерабельно

	waitSignal(t, c.teardownDone, "отложенное уничтожение после возврата из колбэка")

	// Снятие стоит после close(teardownDone) — дождаться его, не вызывая Photon_*.
	deadline := time.Now().Add(4 * time.Second)
	for {
		registered, retired := registryHolds(h)
		if !registered && !retired {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("после отложенного уничтожения без последующих вызовов: "+
				"registered=%v retired=%v — ячейка не освобождена", registered, retired)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// registryHolds — есть ли адрес в реестре живым или ожидающим освобождения.
// Читает состояние под мьютексом реестра; экспортируемым вызовом не является и
// эпоху не занимает.
func registryHolds(p unsafe.Pointer) (registered, retired bool) {
	handles.mu.Lock()
	defer handles.mu.Unlock()
	_, registered = handles.entries[p]
	for _, rc := range handles.retired {
		if rc.key == p {
			retired = true
		}
	}
	return registered, retired
}

// После завершения уничтожения колбэк не вызывается больше ни разу.
func TestEvents_NoCallbackAfterTeardown(t *testing.T) {
	tr := &fakeTransport{}
	h, err := tsNewCoreWithFakeDispatcher(dispatcher.Config{
		Endpoint: "fake://bridge", SessionToken: "tok", Transport: tr,
		ConnectTimeout: time.Second, CallTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("создание инстанса: %v", err)
	}
	tsEventReset(h)
	tsSetEventCallbackKind(h, 0)
	waitCalls(t, 1)
	sub := tr.waitSub(t)

	tsDestroy(h)
	after := tsEventCalls()

	select {
	case sub.push <- []byte(`{"event":"posted"}`):
	default:
	}
	time.Sleep(300 * time.Millisecond)
	if tsEventCalls() != after {
		t.Fatalf("колбэк вызван после возврата Destroy: было %d, стало %d", after, tsEventCalls())
	}
}

// Destroy во время паузы backoff возвращается сразу.
func TestEvents_DestroyDuringBackoff(t *testing.T) {
	tr := &fakeTransport{}
	h, err := tsNewCoreWithFakeDispatcher(dispatcher.Config{
		Endpoint: "fake://bridge", SessionToken: "tok", Transport: tr,
		ConnectTimeout: time.Second, CallTimeout: time.Second,
		EventBackoffMin: 30 * time.Second, EventBackoffMax: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("создание инстанса: %v", err)
	}
	tsEventReset(h)
	tsSetEventCallbackKind(h, 0)
	waitCalls(t, 1)

	tr.waitSub(t).breakPush()
	waitCalls(t, 2) // NETWORK_CHANGED, дальше насос уходит в паузу

	start := time.Now()
	tsDestroy(h)
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("Destroy ждал паузу backoff: %s", el)
	}
}
