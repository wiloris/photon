package permissions

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/wiloris/photon/photon-core/ports"
)

func newTestSetup(t *testing.T) (*Policy, *Authorizer) {
	t.Helper()
	p := NewPolicy()
	return p, NewAuthorizer(p)
}

func key(s string) ports.PublicKey { return ports.PublicKey(s) }

// ── Policy: базовые решения ─────────────────────────────────────────

func TestPolicy_GrantedPermissionAllowed(t *testing.T) {
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}
	if err := p.Grant(subj, []string{"storage.read"}); err != nil {
		t.Fatalf("Grant: %v", err)
	}

	d, err := p.Evaluate(subj, "storage.read")
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if d != DecisionAllow {
		t.Fatalf("decision = %v, want ALLOW", d)
	}
}

func TestPolicy_MissingPermissionDenied(t *testing.T) {
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read"})

	d, err := p.Evaluate(subj, "network.connect")
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if d != DecisionDeny {
		t.Fatalf("decision = %v, want DENY", d)
	}
}

func TestPolicy_OnePermissionDoesNotImplyAnother(t *testing.T) {
	// storage.read НЕ даёт storage.write. Проверка точная, без
	// неявной иерархии.
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read"})

	d, err := p.Evaluate(subj, "storage.write")
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if d != DecisionDeny {
		t.Fatal("storage.read implied storage.write — permissions must be exact")
	}
}

func TestPolicy_NoWildcardSupport(t *testing.T) {
	// Wildcard не поддерживается намеренно: он создал бы неявную
	// иерархию прав, которую сложно формально проверить.
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}

	// "storage.*" синтаксически некорректен — Grant обязан отказать.
	if err := p.Grant(subj, []string{"storage.*"}); !errors.Is(err, ErrInvalidPermission) {
		t.Fatalf("expected ErrInvalidPermission for wildcard, got %v", err)
	}

	// И даже если бы такое право попало в набор, оно не давало бы
	// storage.read — проверка точная по строке.
	p.Grant(subj, []string{"storage.read"})
	d, _ := p.Evaluate(subj, "storage.anything")
	if d != DecisionDeny {
		t.Fatal("wildcard-like matching occurred")
	}
}

func TestPolicy_UnknownSubjectIsErrorNotDeny(t *testing.T) {
	// "Политика не загружена" и "право не выдано" — разные ситуации.
	// Смешение позволило бы сбою загрузки выглядеть как обычный DENY.
	p := NewPolicy()
	_, err := p.Evaluate(Subject{PluginID: "com.unknown"}, "storage.read")
	if !errors.Is(err, ErrUnknownSubject) {
		t.Fatalf("expected ErrUnknownSubject, got %v", err)
	}
}

func TestPolicy_EmptyGrantSetIsValidAndDenies(t *testing.T) {
	// Субъект с пустым набором прав — НЕ то же самое, что незагруженная
	// политика: здесь ошибки быть не должно, только DENY.
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.quiet"}
	if err := p.Grant(subj, nil); err != nil {
		t.Fatalf("Grant(nil): %v", err)
	}

	d, err := p.Evaluate(subj, "storage.read")
	if err != nil {
		t.Fatalf("empty grant set must not be an error: %v", err)
	}
	if d != DecisionDeny {
		t.Fatalf("decision = %v, want DENY", d)
	}
}

func TestPolicy_InvalidPermissionNameRejectedOnGrant(t *testing.T) {
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}
	for _, perm := range []string{"storage", "Storage.Read", "storage read", "", "storage..read", "admin"} {
		if err := p.Grant(subj, []string{perm}); !errors.Is(err, ErrInvalidPermission) {
			t.Fatalf("permission %q was accepted", perm)
		}
	}
}

func TestPolicy_RoleLikeNamesRejected(t *testing.T) {
	// Роли ("admin", "trusted") не являются capability и не должны
	// проходить: иначе trust начал бы подменять авторизацию
	// (threat-model, раздел 6).
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}
	for _, role := range []string{"admin", "trusted", "developer", "root"} {
		if err := p.Grant(subj, []string{role}); err == nil {
			t.Fatalf("role-like name %q was accepted as a permission", role)
		}
	}
}

func TestPolicy_MultiplePermissions(t *testing.T) {
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}
	perms := []string{"storage.read", "storage.write", "network.connect"}
	if err := p.Grant(subj, perms); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	for _, perm := range perms {
		d, err := p.Evaluate(subj, perm)
		if err != nil || d != DecisionAllow {
			t.Fatalf("%s: decision=%v err=%v", perm, d, err)
		}
	}
}

func TestPolicy_DuplicatePermissionsInGrant(t *testing.T) {
	// Дубликаты в списке не должны ломать выдачу — набор есть набор.
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}
	if err := p.Grant(subj, []string{"storage.read", "storage.read"}); err != nil {
		t.Fatalf("Grant with duplicates: %v", err)
	}
	granted, err := p.GrantedPermissions(subj)
	if err != nil {
		t.Fatalf("GrantedPermissions: %v", err)
	}
	if len(granted) != 1 {
		t.Fatalf("granted = %v, want exactly one entry", granted)
	}
}

// ── Изоляция между плагинами — ключевой тест ────────────────────────

func TestIsolation_PluginsDoNotShareGrants(t *testing.T) {
	// Никакого глобального набора выданных прав: право, выданное
	// одному плагину, не должно быть доступно другому.
	p := NewPolicy()
	a := Subject{PluginID: "com.engram.a"}
	b := Subject{PluginID: "com.engram.b"}

	p.Grant(a, []string{"storage.read"})
	p.Grant(b, []string{"storage.write"})

	cases := []struct {
		subj Subject
		perm string
		want Decision
	}{
		{a, "storage.read", DecisionAllow},
		{a, "storage.write", DecisionDeny}, // право B не действует для A
		{b, "storage.write", DecisionAllow},
		{b, "storage.read", DecisionDeny}, // право A не действует для B
	}
	for _, c := range cases {
		d, err := p.Evaluate(c.subj, c.perm)
		if err != nil {
			t.Fatalf("%s + %s: %v", c.subj.PluginID, c.perm, err)
		}
		if d != c.want {
			t.Fatalf("%s + %s: got %v, want %v", c.subj.PluginID, c.perm, d, c.want)
		}
	}
}

func TestIsolation_ChangingOnePolicyDoesNotAffectAnother(t *testing.T) {
	p := NewPolicy()
	a := Subject{PluginID: "com.engram.a"}
	b := Subject{PluginID: "com.engram.b"}

	p.Grant(a, []string{"storage.read"})
	p.Grant(b, []string{"storage.read"})

	p.Revoke(a, "storage.read")

	if d, _ := p.Evaluate(a, "storage.read"); d != DecisionDeny {
		t.Fatal("revoke on A did not take effect")
	}
	if d, _ := p.Evaluate(b, "storage.read"); d != DecisionAllow {
		t.Fatal("revoke on A affected B — policies must be independent")
	}
}

// ── Revoke ──────────────────────────────────────────────────────────

func TestRevoke_TakesEffectImmediately(t *testing.T) {
	// Права не кэшируются как "выданные навсегда": иначе отзыв
	// требовал бы перезапуска плагина.
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"network.connect"})

	if d, _ := p.Evaluate(subj, "network.connect"); d != DecisionAllow {
		t.Fatal("permission was not granted")
	}
	p.Revoke(subj, "network.connect")
	if d, _ := p.Evaluate(subj, "network.connect"); d != DecisionDeny {
		t.Fatal("revoked permission still allowed")
	}
}

func TestRevokeAll_MakesSubjectUnknown(t *testing.T) {
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read"})
	p.RevokeAll(subj)

	if _, err := p.Evaluate(subj, "storage.read"); !errors.Is(err, ErrUnknownSubject) {
		t.Fatalf("expected ErrUnknownSubject after RevokeAll, got %v", err)
	}
}

func TestGrant_ReplacesPreviousSet(t *testing.T) {
	// Grant описывает полное состояние прав, а не добавляет к нему.
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read", "network.connect"})
	p.Grant(subj, []string{"storage.read"})

	if d, _ := p.Evaluate(subj, "network.connect"); d != DecisionDeny {
		t.Fatal("Grant did not replace the previous set")
	}
	if d, _ := p.Evaluate(subj, "storage.read"); d != DecisionAllow {
		t.Fatal("Grant lost a permission that should remain")
	}
}

func TestGrantedPermissions_ReturnsCopy(t *testing.T) {
	// Вызывающий не должен мочь изменить политику мимо Grant/Revoke.
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read"})

	got, err := p.GrantedPermissions(subj)
	if err != nil {
		t.Fatalf("GrantedPermissions: %v", err)
	}
	got[0] = "network.connect" // портим возвращённый срез

	if d, _ := p.Evaluate(subj, "network.connect"); d != DecisionAllow {
		// Ожидаем DENY — то есть мутация НЕ повлияла.
	} else {
		t.Fatal("mutating the returned slice changed the policy")
	}
	if d, _ := p.Evaluate(subj, "storage.read"); d != DecisionAllow {
		t.Fatal("original permission was lost")
	}
}

// ── Authorizer: контракт ports.Authorizer ───────────────────────────

func TestAuthorizer_ImplementsPortsContract(t *testing.T) {
	_, a := newTestSetup(t)
	var _ ports.Authorizer = a
}

func TestAuthorizer_AllowsGrantedPermission(t *testing.T) {
	p, a := newTestSetup(t)
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read"})
	a.RegisterCaller(key("pubkey-ssh"), subj)

	ok, err := a.Authorize(context.Background(), key("pubkey-ssh"), "storage.read")
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if !ok {
		t.Fatal("granted permission was denied")
	}
}

func TestAuthorizer_DeniesMissingPermission(t *testing.T) {
	p, a := newTestSetup(t)
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read"})
	a.RegisterCaller(key("pubkey-ssh"), subj)

	ok, err := a.Authorize(context.Background(), key("pubkey-ssh"), "storage.write")
	if err != nil {
		t.Fatalf("denial must be (false, nil), not an error: %v", err)
	}
	if ok {
		t.Fatal("permission that was not granted was allowed")
	}
}

func TestAuthorizer_UnknownCallerIsErrorNotDeny(t *testing.T) {
	// "Я не знаю, кто ты" (UNAUTHORIZED) и "знаю, но нельзя"
	// (FORBIDDEN) — разные ситуации (docs/ipc.md, раздел 5).
	_, a := newTestSetup(t)

	ok, err := a.Authorize(context.Background(), key("nobody"), "storage.read")
	if !errors.Is(err, ErrUnknownCaller) {
		t.Fatalf("expected ErrUnknownCaller, got %v", err)
	}
	// ADR-033: ipc.Session matches ports.ErrUnknownCaller via errors.Is,
	// not permissions.ErrUnknownCaller — it doesn't import permissions.
	// This is the obligation the ADR calls out explicitly: if
	// permissions ever stops wrapping the ports sentinel (e.g. during a
	// refactor of the message text above), this must be the assertion
	// that catches it, not a passing string-matched test elsewhere.
	if !errors.Is(err, ports.ErrUnknownCaller) {
		t.Fatalf("expected ErrUnknownCaller to wrap ports.ErrUnknownCaller (ADR-033), got %v", err)
	}
	if ok {
		t.Fatal("unknown caller was authorized")
	}
}

func TestAuthorizer_InvalidPermissionNameIsError(t *testing.T) {
	// Опечатка в имени права не должна выглядеть как нормальный DENY.
	p, a := newTestSetup(t)
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read"})
	a.RegisterCaller(key("pubkey-ssh"), subj)

	ok, err := a.Authorize(context.Background(), key("pubkey-ssh"), "storage")
	if !errors.Is(err, ErrInvalidPermission) {
		t.Fatalf("expected ErrInvalidPermission, got %v", err)
	}
	if ok {
		t.Fatal("invalid permission name was authorized")
	}
}

func TestAuthorizer_CallerIdentityFromRegistrationNotRequest(t *testing.T) {
	// Плагин не участвует в определении своего субъекта: связь
	// ключ -> plugin_id устанавливает Runtime при запуске.
	// Незарегистрированный ключ не авторизуется, даже если субъект с
	// такими правами существует.
	p, a := newTestSetup(t)
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read"})
	a.RegisterCaller(key("legit-key"), subj)

	// Другой ключ, тот же plugin_id в политике — но связи нет.
	_, err := a.Authorize(context.Background(), key("forged-key"), "storage.read")
	if !errors.Is(err, ErrUnknownCaller) {
		t.Fatalf("a key that was never registered must not authorize: %v", err)
	}
}

func TestAuthorizer_UnregisterCallerRevokesAccess(t *testing.T) {
	p, a := newTestSetup(t)
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read"})
	a.RegisterCaller(key("pubkey-ssh"), subj)

	if ok, _ := a.Authorize(context.Background(), key("pubkey-ssh"), "storage.read"); !ok {
		t.Fatal("permission was not granted before unregister")
	}

	a.UnregisterCaller(key("pubkey-ssh"))

	if _, err := a.Authorize(context.Background(), key("pubkey-ssh"), "storage.read"); !errors.Is(err, ErrUnknownCaller) {
		t.Fatalf("expected ErrUnknownCaller after UnregisterCaller, got %v", err)
	}
}

func TestAuthorizer_DifferentCallersIndependent(t *testing.T) {
	p, a := newTestSetup(t)
	subjA := Subject{PluginID: "com.engram.a"}
	subjB := Subject{PluginID: "com.engram.b"}
	p.Grant(subjA, []string{"storage.read"})
	p.Grant(subjB, []string{"storage.write"})
	a.RegisterCaller(key("key-a"), subjA)
	a.RegisterCaller(key("key-b"), subjB)

	if ok, _ := a.Authorize(context.Background(), key("key-a"), "storage.write"); ok {
		t.Fatal("caller A got a permission granted only to B")
	}
	if ok, _ := a.Authorize(context.Background(), key("key-b"), "storage.read"); ok {
		t.Fatal("caller B got a permission granted only to A")
	}
}

func TestAuthorizer_RevokeReflectedImmediately(t *testing.T) {
	// Изменение политики применяется к следующему запросу без
	// перерегистрации и перезапуска плагина.
	p, a := newTestSetup(t)
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"network.connect"})
	a.RegisterCaller(key("pubkey-ssh"), subj)

	if ok, _ := a.Authorize(context.Background(), key("pubkey-ssh"), "network.connect"); !ok {
		t.Fatal("permission was not granted initially")
	}
	p.Revoke(subj, "network.connect")
	if ok, _ := a.Authorize(context.Background(), key("pubkey-ssh"), "network.connect"); ok {
		t.Fatal("revoked permission still authorized — policy must not be cached")
	}
}

func TestAuthorizer_RespectsCancelledContext(t *testing.T) {
	p, a := newTestSetup(t)
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read"})
	a.RegisterCaller(key("pubkey-ssh"), subj)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := a.Authorize(ctx, key("pubkey-ssh"), "storage.read"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// ── Конкурентность ──────────────────────────────────────────────────

func TestConcurrentAuthorizeIsSafe(t *testing.T) {
	// Authorize вызывается на КАЖДЫЙ запрос плагина, параллельно из
	// разных обработчиков IPC.
	p, a := newTestSetup(t)
	for i := 0; i < 4; i++ {
		subj := Subject{PluginID: string(rune('a'+i)) + ".test.plugin"}
		p.Grant(subj, []string{"storage.read"})
		a.RegisterCaller(key("key-"+subj.PluginID), subj)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			subj := Subject{PluginID: string(rune('a'+i%4)) + ".test.plugin"}
			k := key("key-" + subj.PluginID)

			for j := 0; j < 20; j++ {
				a.Authorize(context.Background(), k, "storage.read")
				a.Authorize(context.Background(), k, "storage.write")
				p.Evaluate(subj, "storage.read")
				p.GrantedPermissions(subj)
			}
		}(i)
	}

	// Параллельно меняем политику — Grant/Revoke не должны ломать
	// читателей.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			subj := Subject{PluginID: string(rune('a'+i%4)) + ".test.plugin"}
			for j := 0; j < 20; j++ {
				p.Grant(subj, []string{"storage.read", "network.connect"})
				p.Revoke(subj, "network.connect")
			}
		}(i)
	}
	wg.Wait()
}

func TestConcurrentRegisterUnregisterIsSafe(t *testing.T) {
	p, a := newTestSetup(t)
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read"})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := key("key")
			for j := 0; j < 50; j++ {
				a.RegisterCaller(k, subj)
				a.Authorize(context.Background(), k, "storage.read")
				a.UnregisterCaller(k)
			}
		}(i)
	}
	wg.Wait()
}

// ── EvaluateAll ─────────────────────────────────────────────────────

func TestEvaluateAll(t *testing.T) {
	p := NewPolicy()
	subj := Subject{PluginID: "com.engram.ssh"}
	p.Grant(subj, []string{"storage.read"})

	got, err := p.EvaluateAll(context.Background(), subj, []string{"storage.read", "storage.write"})
	if err != nil {
		t.Fatalf("EvaluateAll: %v", err)
	}
	if got["storage.read"] != DecisionAllow {
		t.Fatal("storage.read should be ALLOW")
	}
	if got["storage.write"] != DecisionDeny {
		t.Fatal("storage.write should be DENY")
	}
}
