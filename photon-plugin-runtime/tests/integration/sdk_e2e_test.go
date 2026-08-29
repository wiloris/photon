package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon-plugin-runtime/permissions"
	sdk "github.com/wiloris/photon-plugin-sdk"
)

// E2E: SDK API -> сериализация -> НАСТОЯЩИЙ ipc.Session -> реальный
// Authorizer -> handler -> ответ -> ошибка SDK.
//
// Мока IPC здесь нет намеренно. Мок проверил бы только, что SDK
// правильно вызывает свои же заглушки; здесь проверяется, что
// публичный API действительно упирается в security boundary,
// построенный в Phase 9, — и что два независимых кодека (SDK и
// runtime) совпадают побайтово.

const (
	testPluginID = "com.engram.e2e"
	testCaller   = "e2e-caller-key"
)

// setupSession поднимает реальную сессию runtime на одном конце
// net.Pipe и подключает SDK к другому.
func setupSession(t *testing.T, granted []string, handler ipc.Handler) (sdk.Plugin, *permissions.Policy) {
	t.Helper()

	policy := permissions.NewPolicy()
	subj := permissions.Subject{PluginID: testPluginID}
	if err := policy.Grant(subj, granted); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	auth := permissions.NewAuthorizer(policy)
	auth.RegisterCaller(ports.PublicKey(testCaller), subj)

	runtimeSide, pluginSide := net.Pipe()

	session := ipc.New(ipc.Config{
		Conn:             runtimeSide,
		Caller:           ports.PublicKey(testCaller),
		ExpectedPluginID: testPluginID,
		ProtocolVersion:  1,
		Authorizer:       auth,
		Handler:          handler,
		RequestTimeout:   3 * time.Second,
		HandshakeTimeout: 3 * time.Second,
	})

	go session.Run(context.Background())
	t.Cleanup(func() { session.Close() })

	// SDK подключается как настоящий плагин.
	connected := make(chan sdk.Plugin, 1)
	errCh := make(chan error, 1)
	go func() {
		p, err := sdk.Connect(context.Background(), sdk.Config{
			Conn:            pluginSide,
			PluginID:        testPluginID,
			ProtocolVersion: 1,
		})
		if err != nil {
			errCh <- err
			return
		}
		connected <- p
	}()

	select {
	case p := <-connected:
		go p.Run(context.Background())
		t.Cleanup(func() { p.Close() })
		return p, policy
	case err := <-errCh:
		t.Fatalf("sdk.Connect: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout during handshake — SDK and runtime wire formats may disagree")
	}
	return nil, nil
}

// echoHandler возвращает то, что просят, для операций каталога.
func echoHandler(t *testing.T) ipc.Handler {
	t.Helper()
	return ipc.HandlerFunc(func(ctx context.Context, _ ipc.CallerInfo, op string, payload []byte) ([]byte, error) {
		switch op {
		case "storage.get":
			return json.Marshal(map[string]any{"value": []byte("stored-value")})
		case "storage.put", "storage.delete", "log.write":
			return json.Marshal(map[string]any{})
		case "storage.list":
			return json.Marshal(map[string]any{"keys": []string{"a", "b"}})
		case "identity.public_key":
			return json.Marshal(map[string]any{"public_key": []byte("pubkey")})
		case "identity.sign":
			return json.Marshal(map[string]any{"signature": []byte("signature")})
		case "identity.verify":
			return json.Marshal(map[string]any{"valid": true})
		case "crypto.encrypt":
			return json.Marshal(map[string]any{"ciphertext": []byte("encrypted")})
		case "crypto.decrypt":
			return json.Marshal(map[string]any{"plaintext": []byte("decrypted")})
		default:
			t.Errorf("handler received an unexpected op: %q", op)
			return nil, errors.New("unexpected op")
		}
	})
}

// ── Handshake: два независимых кодека совпадают ─────────────────────

func TestE2E_HandshakeSucceeds(t *testing.T) {
	// Сам факт успешного handshake доказывает, что wire-формат SDK
	// (реализованный независимо, без импорта runtime) побайтово
	// совпадает с форматом runtime.
	p, _ := setupSession(t, nil, echoHandler(t))
	if p == nil {
		t.Fatal("no plugin")
	}
}

// ── Полная цепочка: ALLOW ───────────────────────────────────────────

func TestE2E_GrantedOperationSucceeds(t *testing.T) {
	p, _ := setupSession(t, []string{"storage.read"}, echoHandler(t))

	value, err := p.Storage().Get(context.Background(), "prefs", "theme")
	if err != nil {
		t.Fatalf("Storage.Get with granted permission: %v", err)
	}
	if string(value) != "stored-value" {
		t.Fatalf("value = %q, want %q", value, "stored-value")
	}
}

// ── Полная цепочка: FORBIDDEN ───────────────────────────────────────

func TestE2E_UngrantedOperationIsForbidden(t *testing.T) {
	// Ключевой тест: красивый API упирается в реальный Authorizer.
	// storage.read выдан, storage.write — нет.
	p, _ := setupSession(t, []string{"storage.read"}, echoHandler(t))

	err := p.Storage().Put(context.Background(), "prefs", "theme", []byte("dark"))
	if err == nil {
		t.Fatal("Storage.Put succeeded without storage.write — the API bypassed authorization")
	}
	if !errors.Is(err, sdk.ErrForbidden) {
		t.Fatalf("got %v, want ErrForbidden", err)
	}
}

func TestE2E_ReadDoesNotImplyWrite(t *testing.T) {
	p, _ := setupSession(t, []string{"storage.read"}, echoHandler(t))
	ctx := context.Background()

	if _, err := p.Storage().Get(ctx, "ns", "k"); err != nil {
		t.Fatalf("Get must be allowed: %v", err)
	}
	if err := p.Storage().Put(ctx, "ns", "k", []byte("v")); !errors.Is(err, sdk.ErrForbidden) {
		t.Fatalf("Put: got %v, want ErrForbidden", err)
	}
	if err := p.Storage().Delete(ctx, "ns", "k"); !errors.Is(err, sdk.ErrForbidden) {
		t.Fatalf("Delete: got %v, want ErrForbidden", err)
	}
}

// ── Каждая операция каталога проходит цепочку ───────────────────────

func TestE2E_AllCatalogOperations(t *testing.T) {
	all := []string{
		"storage.read", "storage.write",
		"identity.read", "identity.sign",
		"crypto.use",
	}
	p, _ := setupSession(t, all, echoHandler(t))
	ctx := context.Background()

	t.Run("storage.get", func(t *testing.T) {
		if _, err := p.Storage().Get(ctx, "ns", "k"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("storage.put", func(t *testing.T) {
		if err := p.Storage().Put(ctx, "ns", "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("storage.delete", func(t *testing.T) {
		if err := p.Storage().Delete(ctx, "ns", "k"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("storage.list", func(t *testing.T) {
		keys, err := p.Storage().List(ctx, "ns", "pre")
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 2 {
			t.Fatalf("keys = %v", keys)
		}
	})
	t.Run("identity.public_key", func(t *testing.T) {
		if _, err := p.Identity().PublicKey(ctx); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("identity.sign", func(t *testing.T) {
		if _, err := p.Identity().Sign(ctx, []byte("data")); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("identity.verify", func(t *testing.T) {
		ok, err := p.Identity().Verify(ctx, []byte("pk"), []byte("d"), []byte("sig"))
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("verify returned false")
		}
	})
	t.Run("crypto.encrypt", func(t *testing.T) {
		if _, err := p.Crypto().Encrypt(ctx, []byte("secret"), nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("crypto.decrypt", func(t *testing.T) {
		if _, err := p.Crypto().Decrypt(ctx, []byte("enc"), nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("log.write", func(t *testing.T) {
		// Единственная операция без права.
		if err := p.Log(ctx, sdk.LogInfo, "hello"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestE2E_LogWorksWithoutAnyPermissions(t *testing.T) {
	// Плагин без единого права должен уметь сообщить о проблеме.
	p, _ := setupSession(t, nil, echoHandler(t))

	if err := p.Log(context.Background(), sdk.LogError, "something went wrong"); err != nil {
		t.Fatalf("log.write must work without permissions: %v", err)
	}
}

// ── Отзыв права действует немедленно ────────────────────────────────

func TestE2E_RevokeTakesEffectWithoutReconnect(t *testing.T) {
	// SDK не кэширует: отзыв права действует на следующем вызове той
	// же сессии, без переподключения (sdk-boundary.md, раздел 2).
	p, policy := setupSession(t, []string{"storage.read"}, echoHandler(t))
	ctx := context.Background()

	if _, err := p.Storage().Get(ctx, "ns", "k"); err != nil {
		t.Fatalf("first Get: %v", err)
	}

	policy.Revoke(permissions.Subject{PluginID: testPluginID}, "storage.read")

	_, err := p.Storage().Get(ctx, "ns", "k")
	if !errors.Is(err, sdk.ErrForbidden) {
		t.Fatalf("after revoke: got %v, want ErrForbidden — SDK must not cache authorization", err)
	}
}

// ── Неизвестная операция ────────────────────────────────────────────

func TestE2E_UnknownOperationIsRejected(t *testing.T) {
	// Runtime не знает операции -> ошибка, а не молчаливый успех.
	h := ipc.HandlerFunc(func(ctx context.Context, _ ipc.CallerInfo, op string, payload []byte) ([]byte, error) {
		return nil, errors.New("unknown operation: " + op)
	})
	p, _ := setupSession(t, []string{"storage.read"}, h)

	if _, err := p.Storage().Get(context.Background(), "ns", "k"); err == nil {
		t.Fatal("unknown operation must produce an error, not silent success")
	}
}

// ── События ─────────────────────────────────────────────────────────

func TestE2E_EventsReachPlugin(t *testing.T) {
	policy := permissions.NewPolicy()
	subj := permissions.Subject{PluginID: testPluginID}
	policy.Grant(subj, nil)
	auth := permissions.NewAuthorizer(policy)
	auth.RegisterCaller(ports.PublicKey(testCaller), subj)

	runtimeSide, pluginSide := net.Pipe()
	session := ipc.New(ipc.Config{
		Conn:             runtimeSide,
		Caller:           ports.PublicKey(testCaller),
		ExpectedPluginID: testPluginID,
		ProtocolVersion:  1,
		Authorizer:       auth,
		Handler:          echoHandler(t),
		HandshakeTimeout: 3 * time.Second,
	})
	go session.Run(context.Background())
	defer session.Close()

	received := make(chan sdk.Event, 1)

	connected := make(chan sdk.Plugin, 1)
	go func() {
		p, err := sdk.Connect(context.Background(), sdk.Config{
			Conn: pluginSide, PluginID: testPluginID, ProtocolVersion: 1,
		})
		if err != nil {
			return
		}
		p.OnEvent(func(ctx context.Context, e sdk.Event) error {
			received <- e
			return nil
		})
		connected <- p
		p.Run(context.Background())
	}()

	var p sdk.Plugin
	select {
	case p = <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("handshake timeout")
	}
	defer p.Close()

	time.Sleep(100 * time.Millisecond) // дать Run запуститься
	if err := session.SendEvent("chat.message", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}

	select {
	case e := <-received:
		if e.Type != "chat.message" {
			t.Fatalf("event type = %q", e.Type)
		}
		if string(e.Payload) != `{"id":1}` {
			t.Fatalf("payload = %q", e.Payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("event never reached the plugin")
	}
}

// ── Разрыв сессии ───────────────────────────────────────────────────

func TestE2E_CallAfterSessionClosed(t *testing.T) {
	p, _ := setupSession(t, []string{"storage.read"}, echoHandler(t))

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err := p.Storage().Get(context.Background(), "ns", "k")
	if err == nil {
		t.Fatal("call after close must fail")
	}
	if !errors.Is(err, sdk.ErrSessionClosed) && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("got %v, want a session-closed error", err)
	}
}

func TestE2E_ContextCancellationStopsCall(t *testing.T) {
	// Handler висит; отмена контекста должна вернуть управление
	// плагину, а не ждать дедлайна runtime.
	h := ipc.HandlerFunc(func(ctx context.Context, _ ipc.CallerInfo, op string, payload []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	p, _ := setupSession(t, []string{"storage.read"}, h)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := p.Storage().Get(ctx, "ns", "k")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error on context cancellation")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("call took %v — cancellation did not return control promptly", elapsed)
	}
}
