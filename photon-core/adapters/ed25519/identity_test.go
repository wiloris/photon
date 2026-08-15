package ed25519

import (
	"context"
	"errors"
	"testing"

	"github.com/wiloris/photon-core/ports"
)

func TestGenerate_ProducesValidPair(t *testing.T) {
	id := New()
	ctx := context.Background()

	pair, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(pair.Private) == 0 {
		t.Fatal("Private key is empty")
	}
	if len(pair.Public) == 0 {
		t.Fatal("Public key is empty")
	}
}

func TestGenerate_ProducesDistinctKeysEachCall(t *testing.T) {
	id := New()
	ctx := context.Background()

	pair1, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate (1): %v", err)
	}
	pair2, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate (2): %v", err)
	}

	if string(pair1.Private) == string(pair2.Private) {
		t.Fatal("two calls to Generate produced identical private keys")
	}
	if string(pair1.Public) == string(pair2.Public) {
		t.Fatal("two calls to Generate produced identical public keys")
	}
}

func TestPublicKeyOf_ReturnsIndependentCopy(t *testing.T) {
	id := New()
	ctx := context.Background()

	pair, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	pub, err := id.PublicKeyOf(ctx, pair.Private)
	if err != nil {
		t.Fatalf("PublicKeyOf: %v", err)
	}

	// Портим приватный ключ после получения pub — если PublicKeyOf
	// вернул срез, разделяющий backing array с pair.Private (priv.Public()
	// именно так и делает без явного копирования), это исказит pub.
	for i := range pair.Private {
		pair.Private[i] = 0xFF
	}

	if string(pub) == string(pair.Private[32:]) {
		t.Fatal("PublicKeyOf returned a slice sharing backing array with the private key — mutation leaked through")
	}
}

func TestPublicKeyOf_MatchesGeneratedPublicKey(t *testing.T) {
	id := New()
	ctx := context.Background()

	pair, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	pub, err := id.PublicKeyOf(ctx, pair.Private)
	if err != nil {
		t.Fatalf("PublicKeyOf: %v", err)
	}

	if string(pub) != string(pair.Public) {
		t.Fatal("PublicKeyOf(priv) != Public from Generate — Ed25519 invariant violated")
	}
}

func TestPublicKeyOf_InvalidKeyLength(t *testing.T) {
	id := New()
	ctx := context.Background()

	_, err := id.PublicKeyOf(ctx, ports.PrivateKey([]byte{1, 2, 3}))
	if !errors.Is(err, ports.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
}

func TestSignVerify_RoundTrip(t *testing.T) {
	id := New()
	ctx := context.Background()

	pair, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	data := []byte("photon protocol message")
	sig, err := id.Sign(ctx, pair.Private, data)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	ok, err := id.Verify(ctx, pair.Public, data, sig)
	if err != nil {
		t.Fatalf("Verify: unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("Verify returned false for a valid signature")
	}
}

func TestVerify_WrongSignature_ReturnsFalseNotError(t *testing.T) {
	id := New()
	ctx := context.Background()

	pair, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	data := []byte("original message")
	otherPair, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate (other): %v", err)
	}

	// Подпись, сделанная ДРУГИМ ключом — структурно корректная
	// подпись (правильная длина), но неверная для pair.Public.
	wrongSig, err := id.Sign(ctx, otherPair.Private, data)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	ok, err := id.Verify(ctx, pair.Public, data, wrongSig)
	if err != nil {
		t.Fatalf("Verify: expected (false, nil) for wrong signature, got error: %v", err)
	}
	if ok {
		t.Fatal("Verify returned true for a signature made with a different key")
	}
}

func TestVerify_TamperedData_ReturnsFalseNotError(t *testing.T) {
	id := New()
	ctx := context.Background()

	pair, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	data := []byte("original message")
	sig, err := id.Sign(ctx, pair.Private, data)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	tampered := []byte("tampered message")
	ok, err := id.Verify(ctx, pair.Public, tampered, sig)
	if err != nil {
		t.Fatalf("Verify: expected (false, nil) for tampered data, got error: %v", err)
	}
	if ok {
		t.Fatal("Verify returned true for tampered data")
	}
}

func TestVerify_InvalidKeyLength_ReturnsError(t *testing.T) {
	id := New()
	ctx := context.Background()

	ok, err := id.Verify(ctx, ports.PublicKey([]byte{1, 2, 3}), []byte("data"), ports.Signature(make([]byte, 64)))
	if !errors.Is(err, ports.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey, got err=%v ok=%v", err, ok)
	}
}

func TestVerify_InvalidSignatureLength_ReturnsError(t *testing.T) {
	id := New()
	ctx := context.Background()

	pair, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	ok, err := id.Verify(ctx, pair.Public, []byte("data"), ports.Signature([]byte{1, 2, 3}))
	if !errors.Is(err, ports.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey, got err=%v ok=%v", err, ok)
	}
}

func TestSign_InvalidKeyLength(t *testing.T) {
	id := New()
	ctx := context.Background()

	_, err := id.Sign(ctx, ports.PrivateKey([]byte{1, 2, 3}), []byte("data"))
	if !errors.Is(err, ports.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}
}

func TestGenerate_RespectsCancelledContext(t *testing.T) {
	id := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := id.Generate(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestPublicKeyOf_RespectsCancelledContext(t *testing.T) {
	id := New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = id.PublicKeyOf(ctx, pair.Private)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSign_RespectsCancelledContext(t *testing.T) {
	id := New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = id.Sign(ctx, pair.Private, []byte("data"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestVerify_RespectsCancelledContext(t *testing.T) {
	id := New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	sig, err := id.Sign(context.Background(), pair.Private, []byte("data"))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = id.Verify(ctx, pair.Public, []byte("data"), sig)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
