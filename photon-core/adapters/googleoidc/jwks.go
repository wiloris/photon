package googleoidc

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// defaultJWKSURL — публичные ключи Google для проверки подписи ID-токенов.
const defaultJWKSURL = "https://www.googleapis.com/oauth2/v3/certs"

// keyCacheTTL — как долго доверяем однажды загруженному набору ключей,
// не обращаясь к jwksURL повторно. Google периодически ротирует ключи
// (обычно не чаще, чем раз в несколько часов) — фиксированный TTL,
// не парсинг Cache-Control, потому что нам не нужна точность до
// секунды, а код проще без парсинга заголовков кэширования.
const keyCacheTTL = 1 * time.Hour

type jwk struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type jwksResponse struct {
	Keys []jwk `json:"keys"`
}

// keySet — потокобезопасный кэш публичных ключей по kid, с
// принудительным обновлением, если запрошенный kid не найден (ключи
// Google ротируются, поэтому "неизвестный kid" — сигнал устаревшего
// кэша, а не обязательно невалидного токена).
type keySet struct {
	httpClient *http.Client
	jwksURL    string

	mu        sync.Mutex
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
}

func newKeySet(httpClient *http.Client, jwksURL string) *keySet {
	return &keySet{httpClient: httpClient, jwksURL: jwksURL, keys: make(map[string]*rsa.PublicKey)}
}

// get возвращает публичный ключ по kid, обновляя кэш при
// необходимости (кэш пуст, устарел по TTL, или kid не найден в уже
// загруженном наборе).
func (ks *keySet) get(kid string) (*rsa.PublicKey, error) {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	if key, ok := ks.keys[kid]; ok && time.Since(ks.fetchedAt) < keyCacheTTL {
		return key, nil
	}

	if err := ks.refresh(); err != nil {
		return nil, err
	}

	key, ok := ks.keys[kid]
	if !ok {
		return nil, fmt.Errorf("googleoidc: no key found for kid %q after refresh", kid)
	}
	return key, nil
}

func (ks *keySet) refresh() error {
	resp, err := ks.httpClient.Get(ks.jwksURL)
	if err != nil {
		return fmt.Errorf("googleoidc: fetch JWKS: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("googleoidc: fetch JWKS: unexpected status %d: %s", resp.StatusCode, body)
	}

	var parsed jwksResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return fmt.Errorf("googleoidc: decode JWKS: %w", err)
	}

	keys := make(map[string]*rsa.PublicKey, len(parsed.Keys))
	for _, k := range parsed.Keys {
		if k.Kty != "RSA" {
			continue
		}
		pub, err := rsaPublicKeyFromJWK(k)
		if err != nil {
			return fmt.Errorf("googleoidc: parse JWK %q: %w", k.Kid, err)
		}
		keys[k.Kid] = pub
	}

	ks.keys = keys
	ks.fetchedAt = time.Now()
	return nil
}

func rsaPublicKeyFromJWK(k jwk) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, fmt.Errorf("decode n: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, fmt.Errorf("decode e: %w", err)
	}

	n := new(big.Int).SetBytes(nBytes)
	e := new(big.Int).SetBytes(eBytes)

	return &rsa.PublicKey{N: n, E: int(e.Int64())}, nil
}
