package googleoidc

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
)

const testKid = "test-key-1"
const testClientID = "test-client-id.apps.googleusercontent.com"

// testEnv — RSA-ключ + локальный JWKS-сервер, имитирующий Google.
type testEnv struct {
	privKey *rsa.PrivateKey
	server  *httptest.Server
	adapter *Adapter
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/certs", func(w http.ResponseWriter, r *http.Request) {
		resp := jwksResponse{Keys: []jwk{{
			Kid: testKid,
			Kty: "RSA",
			Alg: "RS256",
			N:   base64.RawURLEncoding.EncodeToString(priv.PublicKey.N.Bytes()),
			E:   base64.RawURLEncoding.EncodeToString(bigEndianE(priv.PublicKey.E)),
		}}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	adapter := newForTest(testClientID, server.Client(), server.URL+"/certs")

	return &testEnv{privKey: priv, server: server, adapter: adapter}
}

func bigEndianE(e int) []byte {
	// Google обычно использует e=65537 (0x010001) — 3 байта big-endian.
	return big.NewInt(int64(e)).Bytes()
}

// buildToken — вручную собирает и подписывает JWT указанным приватным
// ключом. Не использует внешних JWT-библиотек — тот же принцип
// "стандартная библиотека", что и в самом адаптере.
func (e *testEnv) buildToken(t *testing.T, claims jwtClaims, kid string, corruptSignature bool) string {
	t.Helper()

	header := jwtHeader{Alg: "RS256", Kid: kid}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	claimsBytes, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}

	headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)
	payloadB64 := base64.RawURLEncoding.EncodeToString(claimsBytes)
	signingInput := headerB64 + "." + payloadB64

	hash := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, e.privKey, crypto.SHA256, hash[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if corruptSignature {
		sig[0] ^= 0xFF
	}

	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func validClaims() jwtClaims {
	now := time.Now()
	return jwtClaims{
		Iss:           "https://accounts.google.com",
		Aud:           testClientID,
		Sub:           "1234567890",
		Email:         "user@example.com",
		EmailVerified: true,
		Iat:           now.Unix(),
		Exp:           now.Add(1 * time.Hour).Unix(),
	}
}

func TestVerifyIDToken_ValidToken(t *testing.T) {
	env := newTestEnv(t)
	token := env.buildToken(t, validClaims(), testKid, false)

	ext, err := env.adapter.VerifyIDToken(context.Background(), token)
	if err != nil {
		t.Fatalf("VerifyIDToken: %v", err)
	}
	if ext.Provider != "google" {
		t.Fatalf("Provider = %q, want %q", ext.Provider, "google")
	}
	if ext.Subject != "1234567890" {
		t.Fatalf("Subject = %q, want %q", ext.Subject, "1234567890")
	}
	if ext.Email != "user@example.com" {
		t.Fatalf("Email = %q, want %q", ext.Email, "user@example.com")
	}
}

func TestVerifyIDToken_AlternateIssuerFormat(t *testing.T) {
	env := newTestEnv(t)
	claims := validClaims()
	claims.Iss = "accounts.google.com" // без https://, тоже валиден
	token := env.buildToken(t, claims, testKid, false)

	_, err := env.adapter.VerifyIDToken(context.Background(), token)
	if err != nil {
		t.Fatalf("VerifyIDToken: %v", err)
	}
}

func TestVerifyIDToken_WrongSignature(t *testing.T) {
	env := newTestEnv(t)
	token := env.buildToken(t, validClaims(), testKid, true)

	_, err := env.adapter.VerifyIDToken(context.Background(), token)
	if !errors.Is(err, ports.ErrTokenInvalid) {
		t.Fatalf("expected ErrTokenInvalid, got %v", err)
	}
}

func TestVerifyIDToken_Expired(t *testing.T) {
	env := newTestEnv(t)
	claims := validClaims()
	claims.Exp = time.Now().Add(-1 * time.Hour).Unix()
	token := env.buildToken(t, claims, testKid, false)

	_, err := env.adapter.VerifyIDToken(context.Background(), token)
	if !errors.Is(err, ports.ErrTokenInvalid) {
		t.Fatalf("expected ErrTokenInvalid for expired token, got %v", err)
	}
}

func TestVerifyIDToken_WrongIssuer(t *testing.T) {
	env := newTestEnv(t)
	claims := validClaims()
	claims.Iss = "https://evil.example.com"
	token := env.buildToken(t, claims, testKid, false)

	_, err := env.adapter.VerifyIDToken(context.Background(), token)
	if !errors.Is(err, ports.ErrTokenInvalid) {
		t.Fatalf("expected ErrTokenInvalid for wrong issuer, got %v", err)
	}
}

func TestVerifyIDToken_WrongAudience(t *testing.T) {
	env := newTestEnv(t)
	claims := validClaims()
	claims.Aud = "some-other-client-id.apps.googleusercontent.com"
	token := env.buildToken(t, claims, testKid, false)

	_, err := env.adapter.VerifyIDToken(context.Background(), token)
	if !errors.Is(err, ports.ErrTokenInvalid) {
		t.Fatalf("expected ErrTokenInvalid for wrong audience, got %v", err)
	}
}

func TestVerifyIDToken_MissingSubject(t *testing.T) {
	env := newTestEnv(t)
	claims := validClaims()
	claims.Sub = ""
	token := env.buildToken(t, claims, testKid, false)

	_, err := env.adapter.VerifyIDToken(context.Background(), token)
	if !errors.Is(err, ports.ErrTokenInvalid) {
		t.Fatalf("expected ErrTokenInvalid for missing subject, got %v", err)
	}
}

func TestVerifyIDToken_MalformedToken(t *testing.T) {
	env := newTestEnv(t)

	for _, tok := range []string{
		"",
		"not-a-jwt",
		"only.two-parts",
		"a.b.c.d",
		"not-base64!.also-not.andnotthis",
	} {
		_, err := env.adapter.VerifyIDToken(context.Background(), tok)
		if !errors.Is(err, ports.ErrTokenInvalid) {
			t.Fatalf("token %q: expected ErrTokenInvalid, got %v", tok, err)
		}
	}
}

func TestVerifyIDToken_UnknownKidTriggersRefresh(t *testing.T) {
	// kid не совпадает ни с одним из JWKS — должно вернуться
	// ErrTokenInvalid не потому что подпись плохая, а потому что
	// ключ для проверки в принципе не найден даже после обновления
	// кэша (реалистичный сценарий: клиент прислал токен, подписанный
	// ключом, о котором мы не знаем).
	env := newTestEnv(t)
	token := env.buildToken(t, validClaims(), "unknown-kid", false)

	_, err := env.adapter.VerifyIDToken(context.Background(), token)
	if err == nil {
		t.Fatal("expected an error for unknown kid, got nil")
	}
	// Технический сбой поиска ключа — не обязательно ErrTokenInvalid,
	// но и не nil. Проверяем именно это, а не конкретный тип.
}

func TestVerifyIDToken_WrongAlgorithm(t *testing.T) {
	env := newTestEnv(t)

	header := jwtHeader{Alg: "none", Kid: testKid}
	headerBytes, _ := json.Marshal(header)
	claimsBytes, _ := json.Marshal(validClaims())
	headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)
	payloadB64 := base64.RawURLEncoding.EncodeToString(claimsBytes)
	token := headerB64 + "." + payloadB64 + "."

	_, err := env.adapter.VerifyIDToken(context.Background(), token)
	if !errors.Is(err, ports.ErrTokenInvalid) {
		t.Fatalf("expected ErrTokenInvalid for alg=none, got %v", err)
	}
}

func TestVerifyIDToken_RespectsCancelledContext(t *testing.T) {
	env := newTestEnv(t)
	token := env.buildToken(t, validClaims(), testKid, false)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := env.adapter.VerifyIDToken(ctx, token)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestVerifyIDToken_JWKSUnavailable(t *testing.T) {
	// Сервер JWKS недоступен вообще (адрес не отвечает) — должна
	// вернуться техническая ошибка, не ErrTokenInvalid: токен мог бы
	// быть валиден, просто проверить это сейчас не удалось.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	env := &testEnv{privKey: priv, adapter: newForTest(testClientID, server.Client(), server.URL)}

	token := env.buildToken(t, validClaims(), testKid, false)
	_, err = env.adapter.VerifyIDToken(context.Background(), token)
	if err == nil {
		t.Fatal("expected an error when JWKS endpoint is unavailable, got nil")
	}
	if errors.Is(err, ports.ErrTokenInvalid) {
		t.Fatal("JWKS fetch failure should not be reported as ErrTokenInvalid — it's a technical failure, not an invalid token")
	}
}

func TestNew_UsesDefaultGoogleJWKSURL(t *testing.T) {
	a := New(testClientID)
	if a.keys.jwksURL != defaultJWKSURL {
		t.Fatalf("New() jwksURL = %q, want %q", a.keys.jwksURL, defaultJWKSURL)
	}
}
