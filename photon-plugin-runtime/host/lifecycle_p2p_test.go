package host

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-plugin-runtime/ipc"
	"github.com/wiloris/photon-plugin-runtime/manifest"
	"github.com/wiloris/photon-plugin-runtime/permissions"
)

// closingHandler — обработчик, которому есть что освобождать при завершении
// плагина. Соответствует необязательному интерфейсу, который host проверяет
// приведением типа (ADR-076 §3.8).
type closingHandler struct {
	ipc.Handler
	mu     sync.Mutex
	closed []string
}

func (c *closingHandler) ClosePluginSessions(pluginID string) {
	c.mu.Lock()
	c.closed = append(c.closed, pluginID)
	c.mu.Unlock()
}

func (c *closingHandler) seen() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.closed))
	copy(out, c.closed)
	return out
}

// Ресурсы с временем жизни освобождаются в ЕДИНСТВЕННОЙ точке, через
// которую проходит любое завершение плагина.
//
// Без этой проверки хук в host.run остался бы непокрытым: сам по себе
// ClosePluginSessions протестирован в coreops, но факт его ВЫЗОВА — нет, а
// молча невызванный хук выглядит точно так же, как работающий.
func TestPluginExitClosesHandlerSessions(t *testing.T) {
	ch := &closingHandler{Handler: ipc.HandlerFunc(
		func(ctx context.Context, _ ipc.CallerInfo, op string, payload []byte) ([]byte, error) {
			return []byte("ok"), nil
		})}

	m := testManifest()
	policy := permissions.NewPolicy()
	h := New(Config{
		Manifest:    m,
		PackageRoot: "/tmp/pkg",
		Caller:      ports.PublicKey("caller-key"),
		Policy:      policy,
		Authorizer:  permissions.NewAuthorizer(policy),
		Handler:     ch,
		Spawner:     &fakeSpawner{behavior: goodPlugin(0)},
	})

	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(ch.seen()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	got := ch.seen()
	if len(got) != 1 || got[0] != m.Identity.PluginID {
		t.Fatalf("ClosePluginSessions вызван как %v, ожидался один раз с %q",
			got, m.Identity.PluginID)
	}
}

var _ = manifest.InstanceSingle
