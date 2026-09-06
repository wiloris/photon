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

// Реентерабельный Photon_Destroy обязан вернуться НЕМЕДЛЕННО.
//
// Прямая реализация дала бы дедлок: Destroy → Close → inflight.Wait ждёт
// насос, который стоит внутри колбэка, вызвавшего Destroy. Тот же дефект, что
// Phase 11 нашла в SDK (OnEvent/OnStop), и он считается дефектом API.
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
	tsEventReset(h)
	tsSetEventCallbackKind(h, 3) // зовёт Photon_Destroy

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

	// Уничтожение завершается само, после выхода из колбэка: handle перестаёт
	// резолвиться.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !tsCoreIsValid(h) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("отложенное уничтожение не выполнилось после возврата из колбэка")
}

// Конкурентные Destroy: уничтожение ровно один раз, и ни один вызов не падает.
func TestEvents_ConcurrentDestroyRunsTeardownOnce(t *testing.T) {
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

	const n = 8
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() { tsDestroy(h); done <- struct{}{} }()
	}
	for i := 0; i < n; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("конкурентный Destroy завис")
		}
	}
	if tsCoreIsValid(h) {
		t.Fatal("инстанс пережил Destroy")
	}
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
