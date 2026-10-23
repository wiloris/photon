// Package ed25519 реализует ports.Identity поверх crypto/ed25519
// (стандартная библиотека Go — внешних зависимостей нет).
package ed25519

import (
	"context"
	stded25519 "crypto/ed25519"
	"crypto/rand"
	"fmt"

	"github.com/wiloris/photon-core/ports"
)

// Identity — реализация ports.Identity. Без состояния: не хранит
// ключи, не держит соединений — создание через New не требует
// параметров и не может завершиться ошибкой.
type Identity struct{}

// New создаёт новый адаптер. Всегда успешен: crypto/ed25519 не
// требует инициализации внешних ресурсов.
func New() *Identity {
	return &Identity{}
}

// compile-time проверка соответствия ports.Identity (см. ROADMAP.md,
// Phase 1 — правило: тесты на compile-time проверку соответствия
// интерфейсам обязательны для закрытия фазы).
var _ ports.Identity = (*Identity)(nil)

// Generate создаёт новую пару Ed25519. Единственная причина ошибки —
// сбой источника случайности (rand.Reader), что на практике
// означает серьёзную проблему с ОС/окружением, а не штатный сценарий.
func (i *Identity) Generate(ctx context.Context) (ports.IdentityPair, error) {
	if err := ctx.Err(); err != nil {
		return ports.IdentityPair{}, err
	}

	pub, priv, err := stded25519.GenerateKey(rand.Reader)
	if err != nil {
		return ports.IdentityPair{}, fmt.Errorf("ed25519: generate: %w", err)
	}

	return ports.IdentityPair{
		Private: ports.PrivateKey(priv),
		Public:  ports.PublicKey(pub),
	}, nil
}

// PublicKeyOf извлекает публичный ключ из приватного. В Ed25519
// приватный ключ (64 байта) уже содержит публичный ключ как свои
// последние 32 байта — операция чисто локальная, без вычислений на
// эллиптической кривой.
func (i *Identity) PublicKeyOf(ctx context.Context, key ports.PrivateKey) (ports.PublicKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(key) != stded25519.PrivateKeySize {
		return nil, ports.ErrInvalidKey
	}

	priv := stded25519.PrivateKey(key)
	pub, ok := priv.Public().(stded25519.PublicKey)
	if !ok {
		// Недостижимо при корректной длине key — приватный ключ Ed25519
		// стандартной библиотеки всегда возвращает stded25519.PublicKey.
		return nil, ports.ErrInvalidKey
	}

	// Копируем: pub — срез, указывающий на backing array key (priv).
	// Наружу не должна уходить память, разделяемая с исходным
	// приватным ключом, — тот же принцип, что копирование значений в
	// adapters/bbolt.
	out := make([]byte, len(pub))
	copy(out, pub)
	return ports.PublicKey(out), nil
}

// Sign подписывает data приватным ключом.
func (i *Identity) Sign(ctx context.Context, key ports.PrivateKey, data []byte) (ports.Signature, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(key) != stded25519.PrivateKeySize {
		return nil, ports.ErrInvalidKey
	}

	sig := stded25519.Sign(stded25519.PrivateKey(key), data)
	return ports.Signature(sig), nil
}

// Verify проверяет подпись. Разделяет (по контракту ports.Identity):
// (false, nil) — структурно корректная, но неверная подпись;
// (false, err) — некорректная длина ключа или подписи (технический
// сбой, а не "подпись неверна").
func (i *Identity) Verify(ctx context.Context, key ports.PublicKey, data []byte, sig ports.Signature) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if len(key) != stded25519.PublicKeySize {
		return false, ports.ErrInvalidKey
	}
	if len(sig) != stded25519.SignatureSize {
		return false, ports.ErrInvalidKey
	}

	ok := stded25519.Verify(stded25519.PublicKey(key), data, []byte(sig))
	return ok, nil
}
