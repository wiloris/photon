package aesgcm

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
)

// hkdfExtract/hkdfExpand — HKDF (RFC 5869) вручную на HMAC-SHA256.
// Не golang.org/x/crypto/hkdf: это внешняя зависимость, а сама
// конструкция — два вызова HMAC, не настолько сложная, чтобы это
// оправдывать (тот же принцип, что в adapters/ed25519/googleoidc —
// минимум внешних зависимостей).

func hkdfExtract(salt, ikm []byte) []byte {
	if salt == nil {
		salt = make([]byte, sha256.Size)
	}
	mac := hmac.New(sha256.New, salt)
	mac.Write(ikm)
	return mac.Sum(nil)
}

func hkdfExpand(prk, info []byte, length int) ([]byte, error) {
	const hashLen = sha256.Size
	if length > 255*hashLen {
		return nil, errors.New("hkdf: requested length too large")
	}

	var t []byte
	okm := make([]byte, 0, length)
	for counter := byte(1); len(okm) < length; counter++ {
		mac := hmac.New(sha256.New, prk)
		mac.Write(t)
		mac.Write(info)
		mac.Write([]byte{counter})
		t = mac.Sum(nil)
		okm = append(okm, t...)
	}
	return okm[:length], nil
}
