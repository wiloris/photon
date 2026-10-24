package account

import (
	"context"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon/photon-core/adapters/bbolt"
	edadapter "github.com/wiloris/photon/photon-core/adapters/ed25519"
	"github.com/wiloris/photon/photon-core/ports"
)

// mockIdentityProvider — тестовая реализация ports.IdentityProvider.
// Живёт в тестовом файле, не в отдельном adapters/googleoidc — см.
// docs/account-device-flow.md и обсуждение порядка реализации: сервис
// пишется и тестируется на моке, реальный OIDC-адаптер — отдельным
// шагом позже, когда станет понятно, как именно сервис его использует.
type mockIdentityProvider struct {
	// tokens — заранее заданное соответствие "токен" → ExternalIdentity.
	tokens map[string]ports.ExternalIdentity
	// err — если не nil, VerifyIDToken всегда возвращает эту ошибку
	// (для проверки сценария "провайдер недоступен").
	err error
}

func newMockIdentityProvider() *mockIdentityProvider {
	return &mockIdentityProvider{tokens: make(map[string]ports.ExternalIdentity)}
}

func (m *mockIdentityProvider) addToken(token string, ext ports.ExternalIdentity) {
	m.tokens[token] = ext
}

func (m *mockIdentityProvider) VerifyIDToken(ctx context.Context, token string) (ports.ExternalIdentity, error) {
	if m.err != nil {
		return ports.ExternalIdentity{}, m.err
	}
	ext, ok := m.tokens[token]
	if !ok {
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}
	return ext, nil
}

var _ ports.IdentityProvider = (*mockIdentityProvider)(nil)

// newTestStorage — тот же bbolt-адаптер, что и в его собственных
// тестах (Phase 2, уже стабилен), с миграциями, создающими четыре
// namespace этого сервиса (см. ADR-024). Владение этими миграциями
// в проде — задача композиционного корня, не services/account (см.
// ADR-024, "владение миграциями") — здесь это тестовая заглушка той
// же роли.
func newTestStorage(t *testing.T) ports.Storage {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")

	migrations := []bboltadapter.Migration{
		{Version: 1, Up: func(tx *bolt.Tx) error {
			for _, ns := range []string{
				nsAccounts,
				nsAccountsByExternal,
				nsDevices,
				nsDevicesByPubKey,
			} {
				if _, err := tx.CreateBucketIfNotExists([]byte(ns)); err != nil {
					return err
				}
			}
			return nil
		}},
	}

	s, err := bboltadapter.New(path, migrations)
	if err != nil {
		t.Fatalf("bboltadapter.New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	if err := s.Migrate(context.Background(), 1); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return s
}

// newTestIdentity — реальный ed25519-адаптер (Phase 3, уже стабилен),
// используется в тестах AuthenticateDevice для получения настоящей
// пары ключей и настоящей подписи, а не заглушки.
func newTestIdentity() ports.Identity {
	return edadapter.New()
}
