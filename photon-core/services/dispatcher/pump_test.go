package dispatcher

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

// collector — потребитель событий. Заодно моделирует медленного клиента:
// очереди между насосом и ним нет намеренно (event-delivery.md §9.2).
type collector struct {
	mu     sync.Mutex
	events []string
	delay  time.Duration

	// concurrent фиксирует факт параллельного вызова: доставка обязана быть
	// последовательной, иначе на стороне JNI AttachCurrentThread выполнялся бы
	// с конкуренцией за один env.
	inFlight   int
	concurrent bool
}

func (c *collector) sink(ev []byte) {
	c.mu.Lock()
	c.inFlight++
	if c.inFlight > 1 {
		c.concurrent = true
	}
	d := c.delay
	c.mu.Unlock()

	if d > 0 {
		time.Sleep(d)
	}

	c.mu.Lock()
	c.inFlight--
	c.events = append(c.events, string(ev))
	c.mu.Unlock()
}

func (c *collector) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.events...)
}

func (c *collector) waitFor(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(20 * timeUnit)
	for time.Now().Before(deadline) {
		if got := c.snapshot(); len(got) >= n {
			return got
		}
		time.Sleep(timeUnit / 20)
	}
	t.Fatalf("получено %d событий, ожидалось %d: %v", len(c.snapshot()), n, c.snapshot())
	return nil
}

func typesOf(events []string) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		var probe struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal([]byte(e), &probe)
		out = append(out, probe.Type)
	}
	return out
}

func startPump(t *testing.T, br *fakeBridge, d *Dispatcher, c *collector) {
	t.Helper()
	if err := d.StartEvents(c.sink, nil); err != nil {
		t.Fatalf("StartEvents: %v", err)
	}
}

// Подписка поднимается сама, без единого SendAction, и первым делом сообщает
// CONNECTED.
func TestPumpConnectsAndEmitsConnected(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	c := &collector{}
	startPump(t, br, d, c)

	got := c.waitFor(t, 1)
	if got[0] != `{"type":"CONNECTED"}` {
		t.Fatalf("первое событие: %s", got[0])
	}
	// Соединение поднято именно в режиме subscribe.
	br.waitSubscribeConn(t, 0)
}

// Push доходит до потребителя, и event_json собран по спецификации.
func TestPumpDeliversMessage(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	c := &collector{}
	startPump(t, br, d, c)
	c.waitFor(t, 1)

	sub := br.waitSubscribeConn(t, 0)
	sub.sendPush(t, `{"event":"posted","data":{"id":"p1"}}`)

	got := c.waitFor(t, 2)
	want := `{"type":"MESSAGE","event":"posted","data":{"id":"p1"}}`
	if got[1] != want {
		t.Fatalf("получено %s\nожидалось %s", got[1], want)
	}
}

// Порядок событий сохраняется, и колбэки не параллельны — оба свойства даёт
// структура (одна горутина, синхронный вызов), а не дисциплина реализации.
func TestPumpPreservesOrderAndNeverOverlaps(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	c := &collector{delay: timeUnit / 10}
	startPump(t, br, d, c)
	c.waitFor(t, 1)

	sub := br.waitSubscribeConn(t, 0)
	const n = 8
	for i := 0; i < n; i++ {
		sub.sendPush(t, `{"event":"posted","data":{"i":`+string(rune('0'+i))+`}}`)
	}

	got := c.waitFor(t, n+1)
	c.mu.Lock()
	overlapped := c.concurrent
	c.mu.Unlock()
	if overlapped {
		t.Fatal("колбэки выполнялись параллельно")
	}
	for i := 0; i < n; i++ {
		want := `"i":` + string(rune('0'+i))
		if !strings.Contains(got[i+1], want) {
			t.Fatalf("событие %d нарушило порядок: %s", i, got[i+1])
		}
	}
}

// Push.Data остаются валидными после возврата декодера: срез смотрит в буфер,
// переиспользуемый между вызовами, и копируется явно. Проверяется тем, что
// вторая порция не искажает первую.
func TestPumpDataSurvivesNextDecode(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	c := &collector{}
	startPump(t, br, d, c)
	c.waitFor(t, 1)

	sub := br.waitSubscribeConn(t, 0)
	sub.sendPush(t, `{"event":"posted","data":{"first":"AAAAAAAAAAAAAAAAAAAA"}}`)
	sub.sendPush(t, `{"event":"posted","data":{"second":"BBBBBBBBBBBBBBBBBBBB"}}`)

	got := c.waitFor(t, 3)
	if !strings.Contains(got[1], "AAAAAAAAAAAAAAAAAAAA") || strings.Contains(got[1], "B") {
		t.Fatalf("первое событие искажено вторым: %s", got[1])
	}
	if !strings.Contains(got[2], "BBBBBBBBBBBBBBBBBBBB") {
		t.Fatalf("второе событие повреждено: %s", got[2])
	}
}

// Обрыв: NETWORK_CHANGED, затем НОВОЕ соединение и снова CONNECTED.
//
// Ключевое свойство — сессия после обрыва НЕ переиспользуется: markLost не
// закрывает conn, а повторный Connect перезаписал бы его, оставив прежнее
// соединение висеть навсегда (event-delivery.md §6.1). Проверяется тем, что
// у фейка появляется ВТОРОЕ подписное соединение.
func TestPumpReconnectsWithNewSessionAfterLoss(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, func(c *Config) {
		c.EventBackoffMin = timeUnit / 10
		c.EventBackoffMax = timeUnit / 5
	})
	c := &collector{}
	startPump(t, br, d, c)
	c.waitFor(t, 1)

	first := br.waitSubscribeConn(t, 0)
	first.breakPush()

	got := c.waitFor(t, 3)
	types := typesOf(got)
	if types[1] != EventNetworkChanged {
		t.Fatalf("после обрыва: %v", types)
	}
	if types[2] != EventConnected {
		t.Fatalf("восстановление не дало CONNECTED: %v", types)
	}

	second := br.waitSubscribeConn(t, 1)
	if second == first {
		t.Fatal("переиспользована прежняя сессия вместо новой")
	}
	// Прежнее соединение обязано быть закрыто, а не брошено.
	first.mu.Lock()
	closed := first.closed
	first.mu.Unlock()
	if !closed {
		t.Fatal("оборванное соединение не закрыто — дескриптор утёк бы")
	}
}

// Битый кадр отвергает СОБЫТИЕ, а не подписку.
func TestPumpMalformedFrameDoesNotKillSubscription(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	c := &collector{}
	startPump(t, br, d, c)
	c.waitFor(t, 1)

	sub := br.waitSubscribeConn(t, 0)
	sub.sendPush(t, `{oops`)
	sub.sendPush(t, `{"event":"posted","data":{"ok":true}}`)

	got := c.waitFor(t, 2)
	if typesOf(got)[1] != EventMessage {
		t.Fatalf("подписка не пережила битый кадр: %v", typesOf(got))
	}
	if strings.Contains(got[1], "oops") {
		t.Fatalf("битый кадр доставлен: %s", got[1])
	}
}

// Насос останавливается при Close: Wait не может вернуться, пока он жив.
func TestPumpStopsOnClose(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	c := &collector{}
	startPump(t, br, d, c)
	c.waitFor(t, 1)

	done := make(chan struct{})
	go func() { d.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * timeUnit):
		t.Fatal("Close не вернулся: насос не остановлен")
	}

	before := len(c.snapshot())
	sub := br.waitSubscribeConn(t, 0)
	select {
	case sub.push <- []byte(`{"event":"posted"}`):
	default:
	}
	time.Sleep(2 * timeUnit)
	if after := len(c.snapshot()); after != before {
		t.Fatalf("событие доставлено после Close: было %d, стало %d", before, after)
	}
}

// Close во время паузы backoff возвращается сразу, а не ждёт её длину.
func TestCloseDuringBackoffReturnsImmediately(t *testing.T) {
	br := newFakeBridge()
	const longBackoff = 30 * time.Second
	d := newDispatcher(t, br, func(c *Config) {
		c.EventBackoffMin = longBackoff
		c.EventBackoffMax = longBackoff
	})
	c := &collector{}
	startPump(t, br, d, c)
	c.waitFor(t, 1)

	// Уводим насос в паузу: обрыв, затем он ждёт backoff.
	br.waitSubscribeConn(t, 0).breakPush()
	c.waitFor(t, 2)

	start := time.Now()
	d.Close()
	if el := time.Since(start); el > 5*timeUnit {
		t.Fatalf("Close ждал паузу backoff: %s", el)
	}
}

// StartEvents идемпотентен: второго насоса не появляется.
func TestStartEventsIsIdempotent(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	c := &collector{}
	startPump(t, br, d, c)
	startPump(t, br, d, c)
	c.waitFor(t, 1)

	time.Sleep(3 * timeUnit)
	if got := typesOf(c.snapshot()); len(got) != 1 || got[0] != EventConnected {
		t.Fatalf("второй насос дал лишние события: %v", got)
	}
}

// На инстансе без адресата подписываться не к кому.
func TestStartEventsWithoutEndpoint(t *testing.T) {
	d, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	c := &collector{}
	if err := d.StartEvents(c.sink, nil); err != ErrNoEndpoint {
		t.Fatalf("ожидалась ErrNoEndpoint, получено %v", err)
	}
}

// После Close подписка не запускается.
func TestStartEventsAfterCloseIsRejected(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	d.Close()
	c := &collector{}
	if err := d.StartEvents(c.sink, nil); err != ErrClosed {
		t.Fatalf("ожидалась ErrClosed, получено %v", err)
	}
}

// onStop вызывается ПОСЛЕ снятия насоса с d.inflight: только тогда в нём
// безопасно звать Close, который этот же inflight и ждёт.
func TestOnStopRunsAfterPumpLeftInflight(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	c := &collector{}

	closed := make(chan error, 1)
	if err := d.StartEvents(c.sink, func() { closed <- d.Close() }); err != nil {
		t.Fatalf("StartEvents: %v", err)
	}
	c.waitFor(t, 1)

	// Отмена без Close — так поступает реентерабельный Destroy.
	d.Cancel()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close из onStop: %v", err)
		}
	case <-time.After(20 * timeUnit):
		t.Fatal("onStop не выполнил Close: насос ждал сам себя")
	}
}

// Действия и подписка живут независимо: обрыв одной не мешает другой.
func TestActionAndSubscriptionAreIndependent(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, func(c *Config) {
		c.EventBackoffMin = timeUnit / 10
		c.EventBackoffMax = timeUnit / 5
	})
	c := &collector{}
	startPump(t, br, d, c)
	c.waitFor(t, 1)

	br.waitSubscribeConn(t, 0).breakPush()
	c.waitFor(t, 3) // обрыв и восстановление подписки

	if _, err := d.SendAction(201, []byte(`{}`)); err != nil {
		t.Fatalf("action пострадал от обрыва подписки: %v", err)
	}
}

// Close НЕ возвращается, пока исполняется колбэк.
//
// Это и есть единственный барьер времени жизни — d.inflight. Прежняя редакция
// TestPumpStopsOnClose проверяла лишь «после Close событий не приходит», и
// оставалась ЗЕЛЁНОЙ на мутации, снимающей насос с inflight: там насос успевал
// выйти сам, и отсутствие события ничего не доказывало. Тот же класс ошибки,
// что зелёный регрессионный тест контекста в Phase 15.
func TestCloseWaitsForCallbackInFlight(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	sink := func(ev []byte) {
		if strings.Contains(string(ev), EventMessage) {
			once.Do(func() { close(entered) })
			<-release
		}
	}
	if err := d.StartEvents(sink, nil); err != nil {
		t.Fatalf("StartEvents: %v", err)
	}

	sub := br.waitSubscribeConn(t, 0)
	sub.sendPush(t, `{"event":"posted"}`)
	select {
	case <-entered:
	case <-time.After(20 * timeUnit):
		t.Fatal("колбэк не начался")
	}

	closed := make(chan struct{})
	go func() { d.Close(); close(closed) }()

	// Пока колбэк держат, Close обязан ждать.
	select {
	case <-closed:
		t.Fatal("Close вернулся, пока колбэк ещё исполнялся: барьер inflight не работает")
	case <-time.After(3 * timeUnit):
	}

	close(release)
	select {
	case <-closed:
	case <-time.After(20 * timeUnit):
		t.Fatal("Close не вернулся после завершения колбэка")
	}
}
