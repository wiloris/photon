package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon-core/services/session"
)

// Нумерация тестов — из action-routing.md §11.

// ── T-1: код действия отвергается без сети ──────────────────────────────────

func TestT1_UnknownActionRejectedWithoutNetwork(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)

	// 700 заменён на 750: с вводом MediaResource (ADR-049 §8) код 700
	// существует, а 750 лежит в его диапазоне и по-прежнему не существует —
	// проверяемое свойство то же.
	for _, code := range []int32{0, 42, 150, 750, 424242, -1} {
		resp, err := d.SendAction(code, []byte(`{}`))
		if !errors.Is(err, session.ErrUnknownAction) && !errors.Is(err, session.ErrNoResourceForAction) {
			t.Fatalf("code %d: err = %v, want ErrUnknownAction/ErrNoResourceForAction", code, err)
		}
		if resp != nil {
			t.Fatalf("code %d: ответ не должен возвращаться", code)
		}
		if got := Classify(err); got != ClassInvalidAction {
			t.Fatalf("code %d: Classify = %v, want ClassInvalidAction", code, got)
		}
	}

	// Главное свойство, ради которого тест написан: ни одного обращения к
	// сети. Оно ломается первым, если проверки переставить (мутация M-3).
	if n := br.connectCount(); n != 0 {
		t.Fatalf("Connect вызван %d раз(а) при неизвестном коде — валидация ушла за проверку связи", n)
	}
}

func TestT1_UnknownActionRejectedWithoutEndpoint(t *testing.T) {
	// Инстанс без адресата: неизвестный код обязан отвергаться и здесь, иначе
	// на таком инстансе «неверный код» неотличим от «нет связи».
	d, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer d.Close()

	if _, err := d.SendAction(424242, []byte(`{}`)); !errors.Is(err, session.ErrUnknownAction) {
		t.Fatalf("err = %v, want ErrUnknownAction", err)
	}
	// А известный — уже про отсутствие адресата.
	if _, err := d.SendAction(100, []byte(`{}`)); !errors.Is(err, ErrNoEndpoint) {
		t.Fatalf("err = %v, want ErrNoEndpoint", err)
	}
}

// ── T-2: payload проверяется без сети ───────────────────────────────────────

func TestT2_InvalidPayloadRejectedWithoutNetwork(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)

	for _, p := range [][]byte{nil, []byte(``), []byte(`{`), []byte(`{"a":}`), []byte(`не json`)} {
		_, err := d.SendAction(100, p)
		if !errors.Is(err, session.ErrInvalidPayload) {
			t.Fatalf("payload %q: err = %v, want ErrInvalidPayload", p, err)
		}
		if got := Classify(err); got != ClassInvalidArgument {
			t.Fatalf("payload %q: Classify = %v, want ClassInvalidArgument", p, got)
		}
	}
	if n := br.connectCount(); n != 0 {
		t.Fatalf("Connect вызван %d раз(а) при невалидном payload", n)
	}
}

// ── T-3: синглфлайт ─────────────────────────────────────────────────────────

func TestT3_ParallelCallsConnectOnce(t *testing.T) {
	const n = 16

	br := newFakeBridge()
	gate := make(chan struct{})
	br.connectGate = gate
	d := newDispatcher(t, br, func(c *Config) {
		c.ConnectTimeout = 20 * timeUnit
		c.CallTimeout = 30 * timeUnit
	})

	var wg sync.WaitGroup
	errs := make([]error, n)
	resps := make([][]byte, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resps[i], errs[i] = d.SendAction(100, []byte(`{}`))
		}(i)
	}

	// Дать всем дойти до ожидания подключения, затем отпустить единственную
	// попытку.
	time.Sleep(timeUnit)
	close(gate)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("вызов %d: %v", i, err)
		}
		if len(resps[i]) == 0 {
			t.Fatalf("вызов %d: пустой ответ", i)
		}
		// Прямое требование §3.3: ErrConnectInProgress наружу не выходит
		// никогда. Отдельная проверка есть ниже, здесь — на всякий путь.
		if errors.Is(err, session.ErrConnectInProgress) {
			t.Fatalf("вызов %d: наружу вышел ErrConnectInProgress", i)
		}
	}
	if got := br.connectCount(); got != 1 {
		t.Fatalf("Connect вызван %d раз(а), want 1", got)
	}
}

// T-3b: ErrConnectInProgress объявлен дефектом, а не состоянием
// (action-routing.md §3.3). Значит его появление обязано быть падением теста, а
// не редким случаем в логах.
func TestT3_ConnectInProgressNeverEscapes(t *testing.T) {
	const rounds, n = 8, 8

	for r := 0; r < rounds; r++ {
		br := newFakeBridge()
		d := newDispatcher(t, br, func(c *Config) {
			c.ConnectTimeout = 20 * timeUnit
			c.CallTimeout = 30 * timeUnit
		})

		var wg sync.WaitGroup
		bad := make(chan error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := d.SendAction(100, []byte(`{}`)); err != nil {
					if errors.Is(err, session.ErrConnectInProgress) {
						bad <- err
					} else {
						bad <- fmt.Errorf("неожиданная ошибка: %w", err)
					}
				}
			}()
		}
		wg.Wait()
		close(bad)
		for err := range bad {
			t.Fatalf("round %d: %v", r, err)
		}
		d.Close()
	}
}

// ── T-4: ленивость ──────────────────────────────────────────────────────────

func TestT4_NewDoesNotTouchNetwork(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)
	_ = d

	time.Sleep(timeUnit / 2)
	if n := br.connectCount(); n != 0 {
		t.Fatalf("Connect вызван %d раз(а) до первого SendAction", n)
	}
}

// ── T-5: успешный путь и формы data ─────────────────────────────────────────

func TestT5_ResponseShapes(t *testing.T) {
	cases := []struct {
		name string
		resp string
		want string
	}{
		{"объект", `{"ok":true,"data":{"id":"x"}}`, `{"ok":true,"data":{"id":"x"}}`},
		{"массив", `{"ok":true,"data":[1,2,3]}`, `{"ok":true,"data":[1,2,3]}`},
		{"null", `{"ok":true,"data":null}`, `{"ok":true,"data":null}`},
		{"нет data", `{"ok":true}`, `{"ok":true}`},
		// Числа обязаны дойти байт в байт: разбор в float64 и обратно сломал
		// бы их молча (bridge-session.md §3.1).
		{"большое число", `{"ok":true,"data":{"n":12345678901234567890}}`,
			`{"ok":true,"data":{"n":12345678901234567890}}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			br := newFakeBridge()
			br.action = func(hdr map[string]any, payload json.RawMessage) any {
				return json.RawMessage(tc.resp)
			}
			d := newDispatcher(t, br, nil)

			got, err := d.SendAction(100, []byte(`{"a":1}`))
			if err != nil {
				t.Fatalf("SendAction: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("конверт = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestT5_HeaderCarriesResourceAndAction(t *testing.T) {
	br := newFakeBridge()
	var gotHdr map[string]any
	var gotPayload json.RawMessage
	br.action = func(hdr map[string]any, payload json.RawMessage) any {
		gotHdr, gotPayload = hdr, payload
		return map[string]any{"ok": true}
	}
	d := newDispatcher(t, br, nil)

	if _, err := d.SendAction(201, []byte(`{"user_id":"u1"}`)); err != nil {
		t.Fatalf("SendAction: %v", err)
	}
	if gotHdr["resource"] != "UserResource" {
		t.Fatalf("resource = %v, want UserResource", gotHdr["resource"])
	}
	if gotHdr["action"].(float64) != 201 {
		t.Fatalf("action = %v, want 201", gotHdr["action"])
	}
	if string(gotPayload) != `{"user_id":"u1"}` {
		t.Fatalf("payload = %s — прошёл не насквозь", gotPayload)
	}
}

// ── T-6: ok:false — это состоявшийся вызов (ADR-038) ────────────────────────

func TestT6_ServerRejectionIsSuccessfulCall(t *testing.T) {
	br := newFakeBridge()
	br.action = func(hdr map[string]any, payload json.RawMessage) any {
		return json.RawMessage(`{"ok":false,"error":"team_id is required"}`)
	}
	d := newDispatcher(t, br, nil)

	got, err := d.SendAction(300, []byte(`{}`))
	if err != nil {
		t.Fatalf("ok:false обязан быть состоявшимся вызовом, а не ошибкой: %v", err)
	}
	if string(got) != `{"ok":false,"error":"team_id is required"}` {
		t.Fatalf("конверт = %s — потерян error сервера", got)
	}
	if Classify(nil) != ClassOK {
		t.Fatal("Classify(nil) должен быть ClassOK")
	}
}

func TestT6_PartialFailureEnvelopePassesThrough(t *testing.T) {
	// batchStore всегда отвечает ok:true, пряча отказы в data.errors[]
	// (bridge-session.md §2.5). Диспетчер обязан пронести это как есть — не
	// «исправлять» ok и не разбирать data.
	br := newFakeBridge()
	br.action = func(hdr map[string]any, payload json.RawMessage) any {
		return json.RawMessage(`{"ok":true,"data":{"errors":[{"key":"k1","error":"nope"}]}}`)
	}
	d := newDispatcher(t, br, nil)

	got, err := d.SendAction(404, []byte(`{}`))
	if err != nil {
		t.Fatalf("SendAction: %v", err)
	}
	if !strings.Contains(string(got), `"errors"`) {
		t.Fatalf("конверт = %s — потеряна часть data", got)
	}
}

// ── T-7: обрыв и переподключение следующим вызовом ──────────────────────────

func TestT7_LossThenReconnectOnNextCall(t *testing.T) {
	br := newFakeBridge()
	d := newDispatcher(t, br, nil)

	if _, err := d.SendAction(100, []byte(`{}`)); err != nil {
		t.Fatalf("первый вызов: %v", err)
	}
	if n := br.connectCount(); n != 1 {
		t.Fatalf("после первого вызова Connect = %d, want 1", n)
	}

	br.breakLast(errors.New("idle timeout"))

	_, err := d.SendAction(100, []byte(`{}`))
	if !errors.Is(err, session.ErrConnectionLost) {
		t.Fatalf("после обрыва err = %v, want ErrConnectionLost", err)
	}
	if got := Classify(err); got != ClassNotConnected {
		t.Fatalf("Classify = %v, want ClassNotConnected", got)
	}

	// Следующий вызов переподключается — политики повторов внутри нет, её
	// роль исполняет сам факт повторного вызова (§3.4).
	if _, err := d.SendAction(100, []byte(`{}`)); err != nil {
		t.Fatalf("третий вызов должен переподключиться: %v", err)
	}
	if n := br.connectCount(); n != 2 {
		t.Fatalf("Connect = %d, want 2 (переподключение)", n)
	}
}

// ── T-9: два инстанса не пересекаются ───────────────────────────────────────

func TestT9_InstancesAreIndependent(t *testing.T) {
	br1, br2 := newFakeBridge(), newFakeBridge()
	br1.action = func(hdr map[string]any, payload json.RawMessage) any {
		return json.RawMessage(`{"ok":true,"data":"bridge-1"}`)
	}
	br2.action = func(hdr map[string]any, payload json.RawMessage) any {
		return json.RawMessage(`{"ok":true,"data":"bridge-2"}`)
	}

	d1, err := New(Config{Endpoint: "fake://one", Transport: br1, CallTimeout: 3 * timeUnit})
	if err != nil {
		t.Fatalf("New d1: %v", err)
	}
	d2, err := New(Config{Endpoint: "fake://two", Transport: br2, CallTimeout: 3 * timeUnit})
	if err != nil {
		t.Fatalf("New d2: %v", err)
	}
	defer d2.Close()

	r1, err := d1.SendAction(100, []byte(`{}`))
	if err != nil {
		t.Fatalf("d1: %v", err)
	}
	r2, err := d2.SendAction(100, []byte(`{}`))
	if err != nil {
		t.Fatalf("d2: %v", err)
	}
	if !strings.Contains(string(r1), "bridge-1") || !strings.Contains(string(r2), "bridge-2") {
		t.Fatalf("вызовы ушли не туда: r1=%s r2=%s", r1, r2)
	}

	// Уничтожение одного инстанса не трогает второй.
	if err := d1.Close(); err != nil {
		t.Fatalf("Close d1: %v", err)
	}
	if _, err := d2.SendAction(100, []byte(`{}`)); err != nil {
		t.Fatalf("после Close d1 второй инстанс сломался: %v", err)
	}
	if br2.isClosed() {
		t.Fatal("Close d1 закрыл транспорт d2")
	}
}

// ── T-10: Close над висящим вызовом ─────────────────────────────────────────

func TestT10_CloseCancelsInflightCall(t *testing.T) {
	br := newFakeBridge()
	br.action = nil // сервер принимает запрос и не отвечает никогда
	d := newDispatcher(t, br, func(c *Config) {
		// Дедлайн вызова заведомо больше времени, которое тест готов ждать:
		// если Close начнёт ЖДАТЬ вместо того, чтобы ОТМЕНЯТЬ, тест это
		// поймает, а не замаскирует таймаутом (прецедент host.Stop, Phase 9).
		c.CallTimeout = 60 * time.Second
	})

	callDone := make(chan error, 1)
	go func() {
		_, err := d.SendAction(100, []byte(`{}`))
		callDone <- err
	}()

	// Дать вызову дойти до ожидания ответа.
	time.Sleep(timeUnit)

	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * timeUnit):
		t.Fatal("Close не вернулся — ожидание без отмены")
	}

	select {
	case err := <-callDone:
		if err == nil {
			t.Fatal("висящий вызов вернул успех после Close")
		}
		if got := Classify(err); got != ClassInvalidArgument {
			t.Fatalf("Classify(%v) = %v, want ClassInvalidArgument", err, got)
		}
	case <-time.After(10 * timeUnit):
		t.Fatal("вызов не разблокировался после Close")
	}

	// Повторный Close безопасен (Photon_Destroy вызывается дважды —
	// ADR-028), и вызовы после Close не воскрешают инстанс.
	if err := d.Close(); err != nil {
		t.Fatalf("повторный Close: %v", err)
	}
	_, err := d.SendAction(100, []byte(`{}`))
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("после Close err = %v, want ErrClosed", err)
	}
	// Класс проверяется здесь, а не только в TestClassifyMapping: на
	// уничтожаемом хендле ClassNotConnected подсказал бы «повтори» — совет,
	// который повторять не на чем (мутация M-4).
	if got := Classify(err); got != ClassInvalidArgument {
		t.Fatalf("после Close Classify = %v, want ClassInvalidArgument", got)
	}
}

func TestT10_CloseDuringConnect(t *testing.T) {
	br := newFakeBridge()
	br.connectGate = make(chan struct{}) // подключение не завершится само
	d := newDispatcher(t, br, func(c *Config) {
		c.ConnectTimeout = 60 * time.Second
		c.CallTimeout = 60 * time.Second
	})

	go d.SendAction(100, []byte(`{}`))
	time.Sleep(timeUnit)

	done := make(chan error, 1)
	go func() { done <- d.Close() }()
	select {
	case <-done:
	case <-time.After(10 * timeUnit):
		t.Fatal("Close не вернулся при идущем подключении")
	}
}

// ── T-11: инстанс без адресата ──────────────────────────────────────────────

func TestT11_InstanceWithoutBridgeIsLegal(t *testing.T) {
	// Ровно те конфиги, которые уже принимает Photon_Create. Сделать endpoint
	// обязательным — изменить поведение замороженной функции (ADR-008).
	for _, raw := range []string{`{}`, `{"storage_path":"/tmp/photon"}`, `{"bridge":{}}`} {
		cfg, err := ParseConfig([]byte(raw))
		if err != nil {
			t.Fatalf("ParseConfig(%s): %v", raw, err)
		}
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New(%s): %v", raw, err)
		}
		_, err = d.SendAction(100, []byte(`{}`))
		if !errors.Is(err, ErrNoEndpoint) {
			t.Fatalf("%s: err = %v, want ErrNoEndpoint", raw, err)
		}
		if got := Classify(err); got != ClassNotConnected {
			t.Fatalf("%s: Classify = %v, want ClassNotConnected", raw, got)
		}
		d.Close()
	}
}

// ── T-12: сервер отверг handshake ───────────────────────────────────────────

func TestT12_HandshakeRejected(t *testing.T) {
	br := newFakeBridge()
	br.handshakeOK = false
	br.handshakeErr = "invalid session token"
	d := newDispatcher(t, br, nil)

	_, err := d.SendAction(100, []byte(`{}`))
	if !errors.Is(err, session.ErrHandshakeRejected) {
		t.Fatalf("err = %v, want ErrHandshakeRejected", err)
	}
	if got := Classify(err); got != ClassPermissionDenied {
		t.Fatalf("Classify = %v, want ClassPermissionDenied", got)
	}
	if !strings.Contains(err.Error(), "invalid session token") {
		t.Fatalf("причина от сервера потеряна: %v", err)
	}
}

// ── T-13: сервер не отвечает ────────────────────────────────────────────────

func TestT13_CallTimeout(t *testing.T) {
	br := newFakeBridge()
	br.action = nil
	d := newDispatcher(t, br, func(c *Config) { c.CallTimeout = timeUnit })

	start := time.Now()
	_, err := d.SendAction(100, []byte(`{}`))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if got := Classify(err); got != ClassTimeout {
		t.Fatalf("Classify = %v, want ClassTimeout", got)
	}
	if el := time.Since(start); el > 10*timeUnit {
		t.Fatalf("вызов вернулся через %v — дедлайн не соблюдён", el)
	}
}

func TestT13_ConnectTimeout(t *testing.T) {
	br := newFakeBridge()
	br.connectGate = make(chan struct{})
	d := newDispatcher(t, br, func(c *Config) {
		c.ConnectTimeout = timeUnit
		c.CallTimeout = 30 * timeUnit
	})

	_, err := d.SendAction(100, []byte(`{}`))
	// Дедлайн подключения меньше дедлайна вызова, поэтому наружу выходит
	// неудача подключения, а не таймаут вызова.
	if err == nil {
		t.Fatal("подключение обязано было упасть по своему дедлайну")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded в цепочке", err)
	}
}

// ── Конверт ─────────────────────────────────────────────────────────────────

func TestEnvelopeRejectsMalformedData(t *testing.T) {
	// Склейка непроверенного куска отдала бы клиенту битую строку, причину
	// которой он искал бы у себя.
	_, err := envelope(session.Response{OK: true, Data: []byte(`{oops`)})
	if !errors.Is(err, session.ErrMalformedFrame) {
		t.Fatalf("err = %v, want ErrMalformedFrame", err)
	}
}

func TestEnvelopeEscapesErrorText(t *testing.T) {
	got, err := envelope(session.Response{OK: false, Error: `он сказал "нет"` + "\n"})
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	var probe struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(got, &probe); err != nil {
		t.Fatalf("конверт не разбирается: %s (%v)", got, err)
	}
	if probe.OK || probe.Error != `он сказал "нет"`+"\n" {
		t.Fatalf("конверт = %s", got)
	}
}
