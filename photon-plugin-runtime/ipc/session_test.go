package ipc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/permissions"
)

// Тестовые Handler'ы в этом файле НАМЕРЕННО не реализуют каталог операций
// (storage.*, identity.*, crypto.*). Их задача — проверять ПРОТОКОЛ:
// корреляцию, авторизацию, дедлайны, отмену, поведение при обрыве. Подстановка
// настоящего исполнителя затащила бы ports.Storage в эти тесты, то есть
// заставила бы тест протокола зависеть от хранилища.
//
// Настоящий исполнитель — Core Operations Handler, Phase 13,
// спецификация docs/core-operations.md. Тесты самих операций живут там же, а
// не здесь.

// Тесты идут через НАСТОЯЩИЙ стек:
//
//	decode -> protocol validation -> caller identity -> Authorizer -> handler
//
// Authorizer здесь реальный (permissions.Authorizer), а не заглушка:
// иначе проверялась бы только правильность вызовов, а не сама граница
// безопасности.

type testPlugin struct {
	conn net.Conn
}

// setup поднимает сессию Runtime на одном конце net.Pipe и отдаёт
// другой конец "плагину".
func setup(t *testing.T, pluginID string, caller string, granted []string, handler Handler) (*testPlugin, *Session, *permissions.Policy) {
	t.Helper()

	policy := permissions.NewPolicy()
	subj := permissions.Subject{PluginID: pluginID}
	if err := policy.Grant(subj, granted); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	auth := permissions.NewAuthorizer(policy)
	auth.RegisterCaller(ports.PublicKey(caller), subj)

	runtimeSide, pluginSide := net.Pipe()

	if handler == nil {
		handler = HandlerFunc(func(ctx context.Context, _ CallerInfo, op string, payload []byte) ([]byte, error) {
			return []byte("ok:" + op), nil
		})
	}

	sess := New(Config{
		Conn:                 runtimeSide,
		Caller:               ports.PublicKey(caller),
		ExpectedPluginID:     pluginID,
		ProtocolVersion:      1,
		ExpectedCapabilities: []string{"ssh.session"},
		Authorizer:           auth,
		Handler:              handler,
		RequestTimeout:       2 * time.Second,
		HandshakeTimeout:     2 * time.Second,
	})

	t.Cleanup(func() {
		sess.Close()
		pluginSide.Close()
	})

	return &testPlugin{conn: pluginSide}, sess, policy
}

// completeHandshake проигрывает роль плагина в handshake.
func (p *testPlugin) completeHandshake(t *testing.T, pluginID string, protocolVersion int, caps []string) {
	t.Helper()
	hello, err := Decode(p.conn)
	if err != nil {
		t.Fatalf("plugin: read HELLO: %v", err)
	}
	if hello.Type != MsgHello {
		t.Fatalf("plugin: expected HELLO, got %s", hello.Type)
	}
	if err := Encode(p.conn, &Message{
		Type:                 MsgHelloAck,
		HelloProtocolVersion: protocolVersion,
		HelloPluginID:        pluginID,
		HelloCapabilities:    caps,
	}); err != nil {
		t.Fatalf("plugin: send HELLO_ACK: %v", err)
	}
}

func (p *testPlugin) request(t *testing.T, id uint64, op string, payload []byte) *Message {
	t.Helper()
	if err := Encode(p.conn, &Message{Type: MsgRequest, RequestID: id, Op: op, Payload: payload}); err != nil {
		t.Fatalf("plugin: send REQUEST: %v", err)
	}
	p.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := Decode(p.conn)
	if err != nil {
		t.Fatalf("plugin: read RESPONSE: %v", err)
	}
	return resp
}

func runSession(t *testing.T, s *Session) chan error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(context.Background()) }()
	return errCh
}

// ── Handshake ───────────────────────────────────────────────────────

func TestHandshake_Success(t *testing.T) {
	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", []string{"storage.read"}, nil)
	runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, []string{"ssh.session"})

	resp := p.request(t, 1, "storage.get", nil)
	if resp.ErrorCode != "" {
		t.Fatalf("granted permission failed: %s %s", resp.ErrorCode, resp.ErrorMessage)
	}
}

func TestHandshake_WrongPluginIDRejected(t *testing.T) {
	// Плагин заявляет чужой plugin_id — Runtime сверяет с манифестом.
	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", []string{"storage.read"}, nil)
	errCh := runSession(t, s)
	p.completeHandshake(t, "com.evil.impostor", 1, nil)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrProtocolViolation) {
			t.Fatalf("expected ErrProtocolViolation, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("session did not reject a mismatched plugin_id")
	}
}

func TestHandshake_WrongProtocolVersionRejected(t *testing.T) {
	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", nil, nil)
	errCh := runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 99, nil)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrProtocolViolation) {
			t.Fatalf("expected ErrProtocolViolation, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("session did not reject an incompatible protocol version")
	}
}

func TestHandshake_UndeclaredCapabilityRejected(t *testing.T) {
	// Плагин заявляет в рантайме больше, чем декларировал в манифесте.
	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", nil, nil)
	errCh := runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, []string{"ssh.session", "admin.everything"})

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrProtocolViolation) {
			t.Fatalf("expected ErrProtocolViolation, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("session accepted a capability not declared in the manifest")
	}
}

func TestHandshake_RequestBeforeHandshakeRejected(t *testing.T) {
	// До handshake плагин не имеет права ни на один запрос.
	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", []string{"storage.read"}, nil)
	errCh := runSession(t, s)

	// Игнорируем HELLO, сразу шлём REQUEST вместо HELLO_ACK.
	if _, err := Decode(p.conn); err != nil {
		t.Fatalf("read HELLO: %v", err)
	}
	Encode(p.conn, &Message{Type: MsgRequest, RequestID: 1, Op: "storage.get"})

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrProtocolViolation) {
			t.Fatalf("expected ErrProtocolViolation, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("session accepted a request before handshake")
	}
}

func TestHandshake_DuplicateHelloAckRejected(t *testing.T) {
	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", nil, nil)
	errCh := runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, nil)

	Encode(p.conn, &Message{Type: MsgHelloAck, HelloProtocolVersion: 1, HelloPluginID: "com.engram.ssh"})

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrProtocolViolation) {
			t.Fatalf("expected ErrProtocolViolation, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("session accepted a duplicate HELLO_ACK")
	}
}

func TestHandshake_WrongDirectionMessageRejected(t *testing.T) {
	// RESPONSE идёт Runtime -> Plugin; плагин не вправе его присылать.
	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", nil, nil)
	errCh := runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, nil)

	Encode(p.conn, &Message{Type: MsgResponse, RequestID: 1})

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrProtocolViolation) {
			t.Fatalf("expected ErrProtocolViolation, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("session accepted a wrong-direction message")
	}
}

// ── SECURITY E2E: изоляция прав между плагинами ─────────────────────

func TestSecurityE2E_PluginsCannotUseEachOthersPermissions(t *testing.T) {
	// Сценарий из спецификации, целиком через настоящий стек:
	//
	//   Plugin A granted storage.read  -> read ALLOW, write FORBIDDEN
	//   Plugin B granted storage.write -> read FORBIDDEN, write ALLOW
	//
	// Проверяется НЕ policy напрямую, а полный путь:
	// decode -> identity binding -> Authorizer -> handler.

	pA, sA, _ := setup(t, "com.engram.a", "key-a", []string{"storage.read"}, nil)
	runSession(t, sA)
	pA.completeHandshake(t, "com.engram.a", 1, nil)

	pB, sB, _ := setup(t, "com.engram.b", "key-b", []string{"storage.write"}, nil)
	runSession(t, sB)
	pB.completeHandshake(t, "com.engram.b", 1, nil)

	cases := []struct {
		name      string
		plugin    *testPlugin
		op        string
		wantError ErrorCode
	}{
		{"A: storage.read allowed", pA, "storage.get", ""},
		{"A: storage.write forbidden", pA, "storage.put", ErrForbidden},
		{"B: storage.write allowed", pB, "storage.put", ""},
		{"B: storage.read forbidden", pB, "storage.get", ErrForbidden},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := c.plugin.request(t, uint64(i+1), c.op, nil)
			if resp.ErrorCode != c.wantError {
				t.Fatalf("op %s: got error %q (%s), want %q",
					c.op, resp.ErrorCode, resp.ErrorMessage, c.wantError)
			}
		})
	}
}

func TestSecurityE2E_ReadDoesNotImplyWrite(t *testing.T) {
	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", []string{"storage.read"}, nil)
	runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, nil)

	if resp := p.request(t, 1, "storage.get", nil); resp.ErrorCode != "" {
		t.Fatalf("storage.read must be allowed: %s", resp.ErrorCode)
	}
	if resp := p.request(t, 2, "storage.put", nil); resp.ErrorCode != ErrForbidden {
		t.Fatalf("storage.write must be FORBIDDEN, got %q", resp.ErrorCode)
	}
}

func TestSecurityE2E_EveryRequestIsAuthorized(t *testing.T) {
	// "IPC connected != permission granted": отзыв права действует
	// немедленно на следующем запросе, без переподключения.
	p, s, policy := setup(t, "com.engram.ssh", "key-ssh", []string{"storage.read"}, nil)
	runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, nil)

	if resp := p.request(t, 1, "storage.get", nil); resp.ErrorCode != "" {
		t.Fatalf("first request must succeed: %s", resp.ErrorCode)
	}

	policy.Revoke(permissions.Subject{PluginID: "com.engram.ssh"}, "storage.read")

	if resp := p.request(t, 2, "storage.get", nil); resp.ErrorCode != ErrForbidden {
		t.Fatalf("after revoke expected FORBIDDEN on the SAME session, got %q — "+
			"permission must be checked per request, not cached at connect", resp.ErrorCode)
	}
}

func TestSecurityE2E_UnknownCallerIsUnauthorizedNotForbidden(t *testing.T) {
	// UNAUTHORIZED (субъект не установлен) отличается от FORBIDDEN
	// (установлен, прав нет).
	policy := permissions.NewPolicy()
	auth := permissions.NewAuthorizer(policy) // caller НЕ зарегистрирован

	runtimeSide, pluginSide := net.Pipe()
	defer pluginSide.Close()

	sess := New(Config{
		Conn:             runtimeSide,
		Caller:           ports.PublicKey("ghost-key"),
		ExpectedPluginID: "com.engram.ghost",
		ProtocolVersion:  1,
		Authorizer:       auth,
		Handler:          HandlerFunc(func(context.Context, CallerInfo, string, []byte) ([]byte, error) { return nil, nil }),
		RequestTimeout:   2 * time.Second,
		HandshakeTimeout: 2 * time.Second,
	})
	defer sess.Close()

	go sess.Run(context.Background())

	p := &testPlugin{conn: pluginSide}
	p.completeHandshake(t, "com.engram.ghost", 1, nil)

	resp := p.request(t, 1, "storage.get", nil)
	if resp.ErrorCode != ErrUnauthorized {
		t.Fatalf("unregistered caller: got %q, want UNAUTHORIZED (not FORBIDDEN)", resp.ErrorCode)
	}
}

func TestSecurityE2E_PluginIDInRequestIsIgnoredNotTrusted(t *testing.T) {
	// В протоколе нет поля plugin_id для обычных запросов — идентичность
	// берётся из конфигурации сессии. Проверяем, что подмена невозможна
	// даже при попытке: плагин A шлёт запрос, помеченный как от B через
	// HelloPluginID (единственное поле, куда это можно вписать).
	p, s, _ := setup(t, "com.engram.a", "key-a", []string{"storage.read"}, nil)
	runSession(t, s)
	p.completeHandshake(t, "com.engram.a", 1, nil)

	// Пытаемся выдать себя за другого в теле запроса.
	Encode(p.conn, &Message{
		Type:          MsgRequest,
		RequestID:     1,
		Op:            "storage.put",  // права нет у A
		HelloPluginID: "com.engram.b", // права есть у B
	})
	p.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := Decode(p.conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.ErrorCode != ErrForbidden {
		t.Fatalf("identity spoofing via message body succeeded: got %q, want FORBIDDEN", resp.ErrorCode)
	}
}

// TestSecurityE2E_HandlerCallerComesFromConfigNotRequest — аналог
// TestSecurityE2E_PluginIDInRequestIsIgnoredNotTrusted, но на уровне Handler
// (ADR-036).
//
// Тот проверяет, что подмена идентичности не проходит через АВТОРИЗАЦИЮ.
// Этот — что она не доходит и до ИСПОЛНИТЕЛЯ: раз namespace-изоляция и вывод
// ключа для crypto.* строятся на caller.PluginID, подмена там означала бы
// доступ к чужим данным даже при корректно выданных правах.
func TestSecurityE2E_HandlerCallerComesFromConfigNotRequest(t *testing.T) {
	got := make(chan string, 1)

	h := HandlerFunc(func(_ context.Context, caller CallerInfo, _ string, _ []byte) ([]byte, error) {
		got <- caller.PluginID
		return nil, nil
	})

	p, s, _ := setup(t, "com.engram.a", "key-a", []string{"storage.read"}, h)
	runSession(t, s)
	p.completeHandshake(t, "com.engram.a", 1, nil)

	// Плагин A шлёт разрешённую ему операцию, но помечает запрос как от B —
	// HelloPluginID единственное поле, куда это можно вписать.
	Encode(p.conn, &Message{
		Type:          MsgRequest,
		RequestID:     1,
		Op:            "storage.get",
		HelloPluginID: "com.engram.b",
	})

	select {
	case id := <-got:
		if id != "com.engram.a" {
			t.Fatalf("Handler получил plugin_id из запроса: got %q, want com.engram.a — "+
				"namespace-изоляция и вывод ключа строятся на этом поле", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Handler не был вызван")
	}
}

// Пустой CallerInfo до Handler дойти не может: сессия заполняет его из
// конфигурации, а конфигурацию проверяет Validate. Тест держит это свойство
// на месте — исполнитель Phase 13 будет полагаться на непустой PluginID при
// построении namespace, и молча пустой префикс означал бы общее пространство
// для всех плагинов.
func TestSecurityE2E_HandlerCallerIsNeverEmpty(t *testing.T) {
	got := make(chan CallerInfo, 1)
	h := HandlerFunc(func(_ context.Context, caller CallerInfo, _ string, _ []byte) ([]byte, error) {
		got <- caller
		return nil, nil
	})

	p, s, _ := setup(t, "com.engram.a", "key-a", []string{"storage.read"}, h)
	runSession(t, s)
	p.completeHandshake(t, "com.engram.a", 1, nil)

	Encode(p.conn, &Message{Type: MsgRequest, RequestID: 1, Op: "storage.get"})

	select {
	case c := <-got:
		if c.PluginID == "" {
			t.Error("CallerInfo.PluginID пуст — namespace выродится в общий для всех")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Handler не был вызван")
	}
}

// ── Дедлайны и отмена ───────────────────────────────────────────────

func TestDeadline_RuntimeDeadlineCannotBeExtendedByPlugin(t *testing.T) {
	// Дедлайн назначает Runtime; плагин не может его отменить или
	// продлить, даже если операция висит.
	slow := HandlerFunc(func(ctx context.Context, _ CallerInfo, op string, payload []byte) ([]byte, error) {
		<-ctx.Done() // висим до отмены
		return nil, ctx.Err()
	})

	policy := permissions.NewPolicy()
	subj := permissions.Subject{PluginID: "com.engram.slow"}
	policy.Grant(subj, []string{"storage.read"})
	auth := permissions.NewAuthorizer(policy)
	auth.RegisterCaller(ports.PublicKey("key-slow"), subj)

	runtimeSide, pluginSide := net.Pipe()
	defer pluginSide.Close()

	sess := New(Config{
		Conn:             runtimeSide,
		Caller:           ports.PublicKey("key-slow"),
		ExpectedPluginID: "com.engram.slow",
		ProtocolVersion:  1,
		Authorizer:       auth,
		Handler:          slow,
		RequestTimeout:   300 * time.Millisecond, // короткий дедлайн Runtime
		HandshakeTimeout: 2 * time.Second,
	})
	defer sess.Close()
	go sess.Run(context.Background())

	p := &testPlugin{conn: pluginSide}
	p.completeHandshake(t, "com.engram.slow", 1, nil)

	start := time.Now()
	resp := p.request(t, 1, "storage.get", nil)
	elapsed := time.Since(start)

	if resp.ErrorCode != ErrDeadlineExceeded {
		t.Fatalf("got %q, want DEADLINE_EXCEEDED", resp.ErrorCode)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("took %v — runtime deadline was not enforced", elapsed)
	}
}

func TestCancel_StopsInFlightRequest(t *testing.T) {
	started := make(chan struct{})
	slow := HandlerFunc(func(ctx context.Context, _ CallerInfo, op string, payload []byte) ([]byte, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})

	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", []string{"storage.read"}, slow)
	runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, nil)

	Encode(p.conn, &Message{Type: MsgRequest, RequestID: 7, Op: "storage.get"})
	<-started
	Encode(p.conn, &Message{Type: MsgCancel, RequestID: 7})

	p.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := Decode(p.conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.ErrorCode != ErrCancelled {
		t.Fatalf("got %q, want CANCELLED", resp.ErrorCode)
	}
}

func TestCancel_UnknownRequestIDIsNotAnError(t *testing.T) {
	// Гонка: ответ мог быть отправлен, пока CANCEL летел.
	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", []string{"storage.read"}, nil)
	errCh := runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, nil)

	Encode(p.conn, &Message{Type: MsgCancel, RequestID: 9999})

	// Сессия должна продолжать работать.
	if resp := p.request(t, 1, "storage.get", nil); resp.ErrorCode != "" {
		t.Fatalf("session broken after stray CANCEL: %s", resp.ErrorCode)
	}
	select {
	case err := <-errCh:
		t.Fatalf("session closed after a stray CANCEL: %v", err)
	default:
	}
}

func TestDuplicateRequestIDRejected(t *testing.T) {
	blocked := make(chan struct{})
	release := make(chan struct{})
	h := HandlerFunc(func(ctx context.Context, _ CallerInfo, op string, payload []byte) ([]byte, error) {
		select {
		case blocked <- struct{}{}:
		default:
		}
		<-release
		return []byte("done"), nil
	})

	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", []string{"storage.read"}, h)
	runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, nil)
	defer close(release)

	Encode(p.conn, &Message{Type: MsgRequest, RequestID: 5, Op: "storage.get"})
	<-blocked
	// Тот же request_id, пока первый ещё в полёте.
	Encode(p.conn, &Message{Type: MsgRequest, RequestID: 5, Op: "storage.get"})

	p.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := Decode(p.conn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.ErrorCode != ErrInvalidRequest {
		t.Fatalf("duplicate in-flight request_id: got %q, want INVALID_REQUEST", resp.ErrorCode)
	}
}

// ── События ─────────────────────────────────────────────────────────

func TestEvents_DeliveredToPlugin(t *testing.T) {
	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", nil, nil)
	runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, nil)

	if err := s.SendEvent("chat.message", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}

	p.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	m, err := Decode(p.conn)
	if err != nil {
		t.Fatalf("read event: %v", err)
	}
	if m.Type != MsgEvent || m.EventType != "chat.message" {
		t.Fatalf("got %+v", m)
	}
}

func TestEvents_QueueDoesNotGrowUnbounded(t *testing.T) {
	// Плагин не читает. Очередь ограничена: неограниченный рост означал
	// бы DoS через память ЯДРА (threat-model, T9).
	runtimeSide, pluginSide := net.Pipe()
	defer pluginSide.Close()

	policy := permissions.NewPolicy()
	auth := permissions.NewAuthorizer(policy)

	sess := New(Config{
		Conn:             runtimeSide,
		Caller:           ports.PublicKey("k"),
		ExpectedPluginID: "com.engram.silent",
		ProtocolVersion:  1,
		Authorizer:       auth,
		Handler:          HandlerFunc(func(context.Context, CallerInfo, string, []byte) ([]byte, error) { return nil, nil }),
		EventQueueSize:   4,
		HandshakeTimeout: 2 * time.Second,
	})
	defer sess.Close()

	// Заполняем очередь, не запуская сессию (никто не читает).
	var dropped bool
	for i := 0; i < 100; i++ {
		if err := sess.SendEvent("spam", []byte("x")); err != nil {
			dropped = true
			break
		}
	}
	if !dropped {
		t.Fatal("event queue accepted 100 events with size 4 — it must be bounded")
	}
}

// ── Закрытие сессии ─────────────────────────────────────────────────

func TestClose_CancelsInFlightRequests(t *testing.T) {
	cancelled := make(chan struct{})
	h := HandlerFunc(func(ctx context.Context, _ CallerInfo, op string, payload []byte) ([]byte, error) {
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	})

	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", []string{"storage.read"}, h)
	runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, nil)

	Encode(p.conn, &Message{Type: MsgRequest, RequestID: 1, Op: "storage.get"})
	time.Sleep(100 * time.Millisecond)
	s.Close()

	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight request was not cancelled on session close")
	}
}

func TestClose_SessionIsUnusableAfterwards(t *testing.T) {
	// После закрытия старый caller context нельзя переиспользовать.
	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", []string{"storage.read"}, nil)
	runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, nil)

	s.Close()

	if !s.IsClosed() {
		t.Fatal("session did not report itself closed")
	}
	if err := s.SendEvent("x", nil); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("SendEvent after Close: got %v, want ErrSessionClosed", err)
	}
}

func TestClose_Idempotent(t *testing.T) {
	_, s, _ := setup(t, "com.engram.ssh", "key-ssh", nil, nil)
	for i := 0; i < 3; i++ {
		if err := s.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
}

// ── Конкурентность ──────────────────────────────────────────────────

func TestConcurrentRequests(t *testing.T) {
	h := HandlerFunc(func(ctx context.Context, _ CallerInfo, op string, payload []byte) ([]byte, error) {
		return []byte("ok"), nil
	})
	p, s, _ := setup(t, "com.engram.ssh", "key-ssh", []string{"storage.read"}, h)
	runSession(t, s)
	p.completeHandshake(t, "com.engram.ssh", 1, nil)

	const n = 30
	var wg sync.WaitGroup
	var writeMu sync.Mutex

	// Пишем параллельно, читаем последовательно: ответы могут прийти в
	// любом порядке, важно что все дойдут и не перепутаются.
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			writeMu.Lock()
			Encode(p.conn, &Message{Type: MsgRequest, RequestID: id, Op: "storage.get"})
			writeMu.Unlock()
		}(uint64(i))
	}
	wg.Wait()

	seen := make(map[uint64]bool, n)
	for i := 0; i < n; i++ {
		p.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		resp, err := Decode(p.conn)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if resp.ErrorCode != "" {
			t.Fatalf("request %d failed: %s", resp.RequestID, resp.ErrorCode)
		}
		if seen[resp.RequestID] {
			t.Fatalf("duplicate response for request_id %d", resp.RequestID)
		}
		seen[resp.RequestID] = true
	}
	if len(seen) != n {
		t.Fatalf("got %d distinct responses, want %d", len(seen), n)
	}
}
