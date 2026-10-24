// Package googleoidc реализует ports.IdentityProvider для Google —
// проверка ID-токена (JWT, RS256) через публичные ключи Google.
// Только стандартная библиотека — без внешних зависимостей, тот же
// принцип, что в adapters/ed25519.
package googleoidc

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
)

// googleIssuers — оба варианта значения claim "iss", которые реально
// встречаются у Google (с https:// и без) — оба валидны согласно
// собственной документации Google OIDC.
var googleIssuers = map[string]bool{
	"https://accounts.google.com": true,
	"accounts.google.com":         true,
}

// Adapter — реализация ports.IdentityProvider для Google.
type Adapter struct {
	clientID string
	keys     *keySet
}

// New создаёт адаптер. clientID — OAuth client ID приложения; claim
// "aud" токена обязан совпадать с ним (иначе токен мог быть выпущен
// для другого приложения и предъявлен здесь по ошибке или атаке).
func New(clientID string) *Adapter {
	return &Adapter{
		clientID: clientID,
		keys:     newKeySet(http.DefaultClient, defaultJWKSURL),
	}
}

// newForTest — вариант New с переопределяемыми httpClient/jwksURL,
// для тестов с локальным httptest.Server вместо реального Google.
func newForTest(clientID string, httpClient *http.Client, jwksURL string) *Adapter {
	return &Adapter{
		clientID: clientID,
		keys:     newKeySet(httpClient, jwksURL),
	}
}

var _ ports.IdentityProvider = (*Adapter)(nil)

type jwtClaims struct {
	Iss           string `json:"iss"`
	Aud           string `json:"aud"`
	Sub           string `json:"sub"`
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	Exp           int64  `json:"exp"`
	Iat           int64  `json:"iat"`
}

type jwtHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
}

// VerifyIDToken проверяет подпись, срок действия, issuer и audience
// token. Любая причина отказа (плохой формат, неверная подпись,
// истёкший токен, неверный issuer/audience) сворачивается в
// ports.ErrTokenInvalid — порт сознательно не различает эти случаи
// для вызывающего кода (см. ports/identityprovider.go). Сбой при
// обращении к JWKS-эндпоинту Google (сеть недоступна и т.п.) —
// отдельная, не ErrTokenInvalid ошибка: это не значит, что токен
// плох, а значит, что решение принять не удалось технически.
func (a *Adapter) VerifyIDToken(ctx context.Context, token string) (ports.ExternalIdentity, error) {
	if err := ctx.Err(); err != nil {
		return ports.ExternalIdentity{}, err
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}
	headerB64, payloadB64, sigB64 := parts[0], parts[1], parts[2]

	headerBytes, err := base64.RawURLEncoding.DecodeString(headerB64)
	if err != nil {
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}
	var header jwtHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}
	if header.Alg != "RS256" {
		// Порт проверяет только RS256 (то, что реально выдаёт Google).
		// Отказ, а не попытка поддержать произвольный alg — "none"
		// или симметричные алгоритмы в JWT исторически были источником
		// уязвимостей подмены подписи.
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}

	pubKey, err := a.keys.get(header.Kid)
	if err != nil {
		// Сбой обращения к JWKS — техническая проблема, не "токен неверен".
		return ports.ExternalIdentity{}, fmt.Errorf("googleoidc: resolve signing key: %w", err)
	}

	sigBytes, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}

	signingInput := headerB64 + "." + payloadB64
	hash := sha256.Sum256([]byte(signingInput))
	if err := rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hash[:], sigBytes); err != nil {
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}
	var claims jwtClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}

	if !googleIssuers[claims.Iss] {
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}
	if a.clientID != "" && claims.Aud != a.clientID {
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}
	if claims.Exp == 0 || time.Now().Unix() >= claims.Exp {
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}
	if claims.Sub == "" {
		return ports.ExternalIdentity{}, ports.ErrTokenInvalid
	}

	return ports.ExternalIdentity{
		Provider: "google",
		Subject:  claims.Sub,
		Email:    claims.Email,
	}, nil
}
