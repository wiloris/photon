package aesgcm

import (
	"context"
	"errors"
	"testing"

	"github.com/wiloris/photon/photon-core/ports"
)

func TestGenerateKey_ProducesCorrectLength(t *testing.T) {
	a := New()
	key, err := a.GenerateKey(context.Background())
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if len(key) != keySize {
		t.Fatalf("GenerateKey produced %d bytes, want %d", len(key), keySize)
	}
}

func TestGenerateKey_ProducesDistinctKeys(t *testing.T) {
	a := New()
	ctx := context.Background()
	k1, err := a.GenerateKey(ctx)
	if err != nil {
		t.Fatalf("GenerateKey (1): %v", err)
	}
	k2, err := a.GenerateKey(ctx)
	if err != nil {
		t.Fatalf("GenerateKey (2): %v", err)
	}
	if string(k1) == string(k2) {
		t.Fatal("two calls to GenerateKey produced identical keys")
	}
}

func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	a := New()
	ctx := context.Background()
	key, _ := a.GenerateKey(ctx)

	plaintext := []byte("photon protocol secret payload")
	binding := []byte("photon.identity")

	ciphertext, err := a.Encrypt(ctx, key, plaintext, binding)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	got, err := a.Decrypt(ctx, key, ciphertext, binding)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(got) != string(plaintext) {
		t.Fatalf("Decrypt = %q, want %q", got, plaintext)
	}
}

func TestEncrypt_NilBindingData(t *testing.T) {
	a := New()
	ctx := context.Background()
	key, _ := a.GenerateKey(ctx)

	plaintext := []byte("no binding needed")
	ciphertext, err := a.Encrypt(ctx, key, plaintext, nil)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	got, err := a.Decrypt(ctx, key, ciphertext, nil)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(got) != string(plaintext) {
		t.Fatalf("Decrypt = %q, want %q", got, plaintext)
	}
}

func TestEncrypt_ProducesDistinctCiphertextsEachCall(t *testing.T) {
	// Проверяет, что nonce действительно случайный на каждый вызов —
	// одинаковый plaintext+key не должен давать одинаковый ciphertext.
	a := New()
	ctx := context.Background()
	key, _ := a.GenerateKey(ctx)
	plaintext := []byte("same message")

	c1, err := a.Encrypt(ctx, key, plaintext, nil)
	if err != nil {
		t.Fatalf("Encrypt (1): %v", err)
	}
	c2, err := a.Encrypt(ctx, key, plaintext, nil)
	if err != nil {
		t.Fatalf("Encrypt (2): %v", err)
	}
	if string(c1) == string(c2) {
		t.Fatal("two Encrypt calls with the same plaintext/key produced identical ciphertext — nonce reuse?")
	}
}

func TestDecrypt_WrongKey(t *testing.T) {
	a := New()
	ctx := context.Background()
	key1, _ := a.GenerateKey(ctx)
	key2, _ := a.GenerateKey(ctx)

	ciphertext, err := a.Encrypt(ctx, key1, []byte("secret"), nil)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	_, err = a.Decrypt(ctx, key2, ciphertext, nil)
	if !errors.Is(err, ports.ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for wrong key, got %v", err)
	}
}

func TestDecrypt_WrongBindingData(t *testing.T) {
	a := New()
	ctx := context.Background()
	key, _ := a.GenerateKey(ctx)

	ciphertext, err := a.Encrypt(ctx, key, []byte("secret"), []byte("photon.identity"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	_, err = a.Decrypt(ctx, key, ciphertext, []byte("wrong.binding"))
	if !errors.Is(err, ports.ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for mismatched bindingData, got %v", err)
	}
}

func TestDecrypt_TamperedCiphertext(t *testing.T) {
	a := New()
	ctx := context.Background()
	key, _ := a.GenerateKey(ctx)

	ciphertext, err := a.Encrypt(ctx, key, []byte("secret"), nil)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	tampered := append([]byte(nil), ciphertext...)
	tampered[len(tampered)-1] ^= 0xFF

	_, err = a.Decrypt(ctx, key, tampered, nil)
	if !errors.Is(err, ports.ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for tampered ciphertext, got %v", err)
	}
}

func TestDecrypt_TooShortCiphertext(t *testing.T) {
	a := New()
	ctx := context.Background()
	key, _ := a.GenerateKey(ctx)

	_, err := a.Decrypt(ctx, key, []byte{1, 2, 3}, nil)
	if !errors.Is(err, ports.ErrDecryptionFailed) {
		t.Fatalf("expected ErrDecryptionFailed for too-short ciphertext, got %v", err)
	}
}

func TestEncrypt_InvalidKeyLength(t *testing.T) {
	a := New()
	ctx := context.Background()

	_, err := a.Encrypt(ctx, ports.SymmetricKey([]byte{1, 2, 3}), []byte("data"), nil)
	if !errors.Is(err, ports.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
}

func TestDecrypt_InvalidKeyLength(t *testing.T) {
	a := New()
	ctx := context.Background()

	_, err := a.Decrypt(ctx, ports.SymmetricKey([]byte{1, 2, 3}), make([]byte, 32), nil)
	if !errors.Is(err, ports.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
}

func TestDeriveKey_Deterministic(t *testing.T) {
	a := New()
	ctx := context.Background()
	key, _ := a.GenerateKey(ctx)

	d1, err := a.DeriveKey(ctx, key, "plugin.wallet")
	if err != nil {
		t.Fatalf("DeriveKey (1): %v", err)
	}
	d2, err := a.DeriveKey(ctx, key, "plugin.wallet")
	if err != nil {
		t.Fatalf("DeriveKey (2): %v", err)
	}
	if string(d1) != string(d2) {
		t.Fatal("DeriveKey is not deterministic for the same (key, domain)")
	}
}

func TestDeriveKey_DifferentDomainsProduceDifferentKeys(t *testing.T) {
	a := New()
	ctx := context.Background()
	key, _ := a.GenerateKey(ctx)

	dWallet, err := a.DeriveKey(ctx, key, "plugin.wallet")
	if err != nil {
		t.Fatalf("DeriveKey (wallet): %v", err)
	}
	dChat, err := a.DeriveKey(ctx, key, "plugin.chat")
	if err != nil {
		t.Fatalf("DeriveKey (chat): %v", err)
	}
	if string(dWallet) == string(dChat) {
		t.Fatal("different domains produced the same derived key")
	}
}

func TestDeriveKey_ProducesUsableEncryptionKey(t *testing.T) {
	// Производный ключ обязан быть пригоден для Encrypt/Decrypt — не
	// просто корректной длины, а реально рабочим ключом AES-256.
	a := New()
	ctx := context.Background()
	master, _ := a.GenerateKey(ctx)
	derived, err := a.DeriveKey(ctx, master, "plugin.wallet")
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}

	ciphertext, err := a.Encrypt(ctx, derived, []byte("wallet secret"), nil)
	if err != nil {
		t.Fatalf("Encrypt with derived key: %v", err)
	}
	got, err := a.Decrypt(ctx, derived, ciphertext, nil)
	if err != nil {
		t.Fatalf("Decrypt with derived key: %v", err)
	}
	if string(got) != "wallet secret" {
		t.Fatalf("Decrypt = %q, want %q", got, "wallet secret")
	}
}

func TestDeriveKey_InvalidKeyLength(t *testing.T) {
	a := New()
	ctx := context.Background()

	_, err := a.DeriveKey(ctx, ports.SymmetricKey([]byte{1, 2, 3}), "domain")
	if !errors.Is(err, ports.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
}

func TestGenerateKey_RespectsCancelledContext(t *testing.T) {
	a := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := a.GenerateKey(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestEncrypt_RespectsCancelledContext(t *testing.T) {
	a := New()
	key, _ := a.GenerateKey(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := a.Encrypt(ctx, key, []byte("data"), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDecrypt_RespectsCancelledContext(t *testing.T) {
	a := New()
	ctx0 := context.Background()
	key, _ := a.GenerateKey(ctx0)
	ciphertext, _ := a.Encrypt(ctx0, key, []byte("data"), nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := a.Decrypt(ctx, key, ciphertext, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDeriveKey_RespectsCancelledContext(t *testing.T) {
	a := New()
	key, _ := a.GenerateKey(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := a.DeriveKey(ctx, key, "domain")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
