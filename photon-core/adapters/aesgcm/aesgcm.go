// Package aesgcm реализует ports.Crypto через AES-256-GCM
// (аутентифицированное шифрование) и HKDF-SHA256 (производные ключи).
// Только стандартная библиотека — тот же принцип, что в
// adapters/ed25519 и adapters/googleoidc.
package aesgcm

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"

	"github.com/wiloris/photon-core/ports"
)

// keySize — 32 байта для AES-256. nonceSize — стандартный для GCM
// размер (12 байт); не переиспользуется как "магическое число" вручную
// нигде, кроме этого файла.
const (
	keySize   = 32
	nonceSize = 12
)

// Adapter — реализация ports.Crypto.
type Adapter struct{}

// New создаёт адаптер. Без состояния — как и adapters/ed25519.
func New() *Adapter {
	return &Adapter{}
}

var _ ports.Crypto = (*Adapter)(nil)

// GenerateKey создаёт случайный ключ AES-256.
func (a *Adapter) GenerateKey(ctx context.Context) (ports.SymmetricKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := make([]byte, keySize)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return ports.SymmetricKey(key), nil
}

// DeriveKey — HKDF-SHA256(key, domain) → 32 байта. Один и тот же
// (key, domain) всегда даёт один и тот же результат (детерминированно,
// как того требует контракт ports.Crypto.DeriveKey) — HKDF без
// случайной соли, сама детерминированность важнее свойств соли здесь.
func (a *Adapter) DeriveKey(ctx context.Context, key ports.SymmetricKey, domain string) (ports.SymmetricKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(key) != keySize {
		return nil, ports.ErrInvalidKey
	}

	prk := hkdfExtract(nil, key)
	out, err := hkdfExpand(prk, []byte(domain), keySize)
	if err != nil {
		return nil, err
	}
	return ports.SymmetricKey(out), nil
}

// Encrypt — AES-256-GCM. Nonce генерируется случайно и хранится
// внутри возвращаемого ciphertext (первые nonceSize байт) — вызывающий
// код не имеет к нему доступа и не может допустить его повторное
// использование (см. ports.Crypto.Encrypt).
func (a *Adapter) Encrypt(ctx context.Context, key ports.SymmetricKey, plaintext, bindingData []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(key) != keySize {
		return nil, ports.ErrInvalidKey
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCMWithNonceSize(block, nonceSize)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	sealed := gcm.Seal(nil, nonce, plaintext, bindingData)
	return append(nonce, sealed...), nil
}

// Decrypt — обратная операция Encrypt. Любая причина сбоя (неверный
// ключ, повреждённые данные, несовпадающий bindingData, ciphertext
// короче nonceSize) — ErrDecryptionFailed, без различения (см.
// ports.Crypto.Decrypt).
func (a *Adapter) Decrypt(ctx context.Context, key ports.SymmetricKey, ciphertext, bindingData []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(key) != keySize {
		return nil, ports.ErrInvalidKey
	}
	if len(ciphertext) < nonceSize {
		return nil, ports.ErrDecryptionFailed
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ports.ErrInvalidKey
	}
	gcm, err := cipher.NewGCMWithNonceSize(block, nonceSize)
	if err != nil {
		return nil, err
	}

	nonce, sealed := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, sealed, bindingData)
	if err != nil {
		return nil, ports.ErrDecryptionFailed
	}
	return plaintext, nil
}
