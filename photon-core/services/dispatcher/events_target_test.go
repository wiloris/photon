package dispatcher

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-core/services/session"
)

// Цель подписки — действие 814 (ADR-123 §2.3, conversation-push.md §4).

// newTargetless — инстанс как у ENGRAM: без секции bridge, транспорт — фейк.
func newTargetless(t *testing.T, br *fakeBridge, tune func(*Config)) *Dispatcher {
	t.Helper()
	cfg := Config{ConnectTimeout: 2 * timeUnit, CallTimeout: 3 * timeUnit, Transport: br}
	if tune != nil {
		tune(&cfg)
	}
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func setTarget(t *testing.T, d *Dispatcher, payload string) string {
	t.Helper()
	out, err := d.SendAction(actionLocalEventsTarget, []byte(payload))
	if err != nil {
		t.Fatalf("814 %s: %v", payload, err)
	}
	return string(out)
}

func endpointsOf(br *fakeBridge) []ports.Endpoint {
	br.mu.Lock()
	defer br.mu.Unlock()
	return append([]ports.Endpoint(nil), br.endpoints...)
}

// Колбэк, установленный до входа, не теряется: первая цель запускает насос.
func TestEventsTargetStartsRememberedPump(t *testing.T) {
	br := newFakeBridge()
	d := newTargetless(t, br, nil)
	c := &collector{}
	if err := d.StartEvents(c.sink, nil); err != ErrNoEndpoint {
		t.Fatalf("до цели ожидалась ErrNoEndpoint, получено %v", err)
	}
	time.Sleep(timeUnit)
	if n := br.connectCount(); n != 0 {
		t.Fatalf("без цели подключений быть не должно: %d", n)
	}

	got := setTarget(t, d, `{"endpoint":"bridge.test:4443","session_token":"tok-1"}`)
	if got != `{"ok":true,"data":{"target":"set"}}` {
		t.Fatalf("ответ 814: %s", got)
	}
	if strings.Contains(got, "tok-1") {
		t.Fatal("токен попал в ответ")
	}
	ev := c.waitFor(t, 1)
	if ev[0] != `{"type":"CONNECTED"}` {
		t.Fatalf("первое событие: %s", ev[0])
	}
	sub := br.waitSubscribeConn(t, 0)
	if sub.authToken() != "tok-1" {
		t.Fatalf("подписка с токеном %q", sub.authToken())
	}
	if eps := endpointsOf(br); len(eps) != 1 || eps[0] != "bridge.test:4443" {
		t.Fatalf("адреса подключений: %v", eps)
	}
	sub.sendPush(t, `{"event":"conversation_activity","data":{"conversation_id":"c1","reason":"message"}}`)
	ev = c.waitFor(t, 2)
	if ev[1] != `{"type":"MESSAGE","event":"conversation_activity","data":{"conversation_id":"c1","reason":"message"}}` {
		t.Fatalf("событие: %s", ev[1])
	}
}

// Смена цели закрывает прежнюю подписку и сразу поднимает новую с новым
// токеном; события идут только по новой.
func TestEventsTargetReplaceSwitchesSubscription(t *testing.T) {
	br := newFakeBridge()
	d := newTargetless(t, br, nil)
	c := &collector{}
	_ = d.StartEvents(c.sink, nil)
	setTarget(t, d, `{"endpoint":"bridge.test:4443","session_token":"old"}`)
	c.waitFor(t, 1)
	first := br.waitSubscribeConn(t, 0)

	setTarget(t, d, `{"endpoint":"bridge.test:4443","session_token":"new"}`)
	second := br.waitSubscribeConn(t, 1)
	if second.authToken() != "new" {
		t.Fatalf("новая подписка с токеном %q", second.authToken())
	}
	deadline := time.Now().Add(5 * timeUnit)
	for !first.isClosed() && time.Now().Before(deadline) {
		time.Sleep(timeUnit / 20)
	}
	if !first.isClosed() {
		t.Fatal("прежняя подписка осталась открытой")
	}
	ev := c.waitFor(t, 2)
	if typesOf(ev)[1] != EventConnected {
		t.Fatalf("после смены цели: %v", ev)
	}
	second.sendPush(t, `{"event":"conversation_activity","data":{"conversation_id":"c2"}}`)
	ev = c.waitFor(t, 3)
	if !strings.Contains(ev[2], `"c2"`) {
		t.Fatalf("событие новой подписки: %s", ev[2])
	}
	for _, e := range ev {
		if e == `{"type":"NETWORK_CHANGED","state":"disconnected"}` {
			t.Fatalf("смена цели — не обрыв связи: %v", ev)
		}
	}
}

// Снятие цели закрывает подписку и не переподключается; новая цель — снова.
func TestEventsTargetClearStopsReconnecting(t *testing.T) {
	br := newFakeBridge()
	d := newTargetless(t, br, func(c *Config) { c.EventBackoffMin = timeUnit / 10; c.EventBackoffMax = timeUnit / 5 })
	c := &collector{}
	_ = d.StartEvents(c.sink, nil)
	setTarget(t, d, `{"endpoint":"bridge.test:4443","session_token":"t"}`)
	c.waitFor(t, 1)
	sub := br.waitSubscribeConn(t, 0)

	if got := setTarget(t, d, `{}`); got != `{"ok":true,"data":{"target":"cleared"}}` {
		t.Fatalf("ответ снятия: %s", got)
	}
	deadline := time.Now().Add(5 * timeUnit)
	for !sub.isClosed() && time.Now().Before(deadline) {
		time.Sleep(timeUnit / 20)
	}
	if !sub.isClosed() {
		t.Fatal("подписка не закрыта снятием цели")
	}
	n := br.connectCount()
	time.Sleep(5 * timeUnit) // много больше паузы восстановления
	if got := br.connectCount(); got != n {
		t.Fatalf("без цели насос переподключался: %d → %d", n, got)
	}

	setTarget(t, d, `{"endpoint":"bridge.test:4443","session_token":"t2"}`)
	if again := br.waitSubscribeConn(t, 1); again.authToken() != "t2" {
		t.Fatalf("повторная цель: токен %q", again.authToken())
	}
}

// Смена цели прерывает паузу восстановления: новая цель не ждёт длинного
// backoff прежней.
func TestEventsTargetInterruptsBackoff(t *testing.T) {
	br := newFakeBridge()
	br.connectErr = errors.New("bridge down")
	d := newTargetless(t, br, func(c *Config) { c.EventBackoffMin = 100 * timeUnit; c.EventBackoffMax = 100 * timeUnit })
	c := &collector{}
	_ = d.StartEvents(c.sink, nil)
	setTarget(t, d, `{"endpoint":"bridge.test:4443","session_token":"revoked"}`)
	deadline := time.Now().Add(5 * timeUnit)
	for br.connectCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(timeUnit / 20)
	}
	time.Sleep(timeUnit) // насос ушёл в паузу 100 × timeUnit

	br.mu.Lock()
	br.connectErr = nil
	br.mu.Unlock()
	start := time.Now()
	setTarget(t, d, `{"endpoint":"bridge.test:4443","session_token":"fresh"}`)
	c.waitFor(t, 1)
	if el := time.Since(start); el > 10*timeUnit {
		t.Fatalf("новая цель ждала паузу прежней: %v", el)
	}
}

// Неполная или негодная цель — отказ вызова, прежняя цель не трогается.
func TestEventsTargetRejectsMalformedPayload(t *testing.T) {
	br := newFakeBridge()
	d := newTargetless(t, br, nil)
	for _, p := range []string{
		`{"endpoint":"bridge.test:4443"}`,
		`{"session_token":"t"}`,
		`{"endpoint":"no-port","session_token":"t"}`,
		`{"endpoint":1}`,
	} {
		if _, err := d.SendAction(actionLocalEventsTarget, []byte(p)); !errors.Is(err, session.ErrInvalidPayload) {
			t.Fatalf("%s: ожидалась ErrInvalidPayload, получено %v", p, err)
		}
	}
	d.mu.Lock()
	set := d.target.set()
	d.mu.Unlock()
	if set {
		t.Fatal("отвергнутый payload установил цель")
	}
}

// 814 не создаёт action-соединения: запросы хоста остаются его делом
// (ADR-094 §6.1). Сетевое действие на инстансе без bridge — ErrNoEndpoint.
func TestEventsTargetDoesNotEnableActions(t *testing.T) {
	br := newFakeBridge()
	d := newTargetless(t, br, nil)
	setTarget(t, d, `{"endpoint":"bridge.test:4443","session_token":"t"}`)
	if _, err := d.SendAction(101, []byte(`{}`)); err != ErrNoEndpoint {
		t.Fatalf("ожидалась ErrNoEndpoint, получено %v", err)
	}
	time.Sleep(timeUnit)
	if n := br.connectCount(); n != 0 {
		t.Fatalf("без колбэка и без action подключений быть не должно: %d", n)
	}
}

// Цель с Photon_Create — начальное значение той же цели: 814 её заменяет.
func TestEventsTargetReplacesCreateTimeTarget(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	c := &collector{}
	startPump(t, br, d, c)
	c.waitFor(t, 1)
	if tok := br.waitSubscribeConn(t, 0).authToken(); tok != "tok" {
		t.Fatalf("начальная цель: %q", tok)
	}
	setTarget(t, d, `{"endpoint":"other.test:4443","session_token":"later"}`)
	if tok := br.waitSubscribeConn(t, 1).authToken(); tok != "later" {
		t.Fatalf("после 814: %q", tok)
	}
	eps := endpointsOf(br)
	if eps[len(eps)-1] != "other.test:4443" {
		t.Fatalf("адрес новой подписки: %v", eps)
	}
}

// Close во время ожидания цели возвращается сразу.
func TestCloseWhileWaitingForTarget(t *testing.T) {
	br := newFakeBridge()
	d := newTargetless(t, br, nil)
	c := &collector{}
	_ = d.StartEvents(c.sink, nil)
	setTarget(t, d, `{"endpoint":"bridge.test:4443","session_token":"t"}`)
	c.waitFor(t, 1)
	setTarget(t, d, `{}`)
	done := make(chan error, 1)
	go func() { done <- d.Close() }()
	select {
	case <-done:
	case <-time.After(5 * timeUnit):
		t.Fatal("Close завис на насосе без цели")
	}
	if _, err := d.SendAction(actionLocalEventsTarget, []byte(`{}`)); err != ErrClosed {
		t.Fatalf("814 после Close: %v", err)
	}
}
