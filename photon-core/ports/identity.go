// Package ports — см. storage.go для общих правил пакета.
package ports

import "context"

// Identity — порт криптографической идентичности ядра.
// Первая реализация — adapters/ed25519 (см. ROADMAP.md, Phase 3).
//
// Разделение ответственности: Identity — личность и подписи, Crypto
// (crypto.go) — конфиденциальность. Персистентность приватного ключа
// — забота Storage+Crypto, не этого порта. См. ADR-011/013 —
// применяются здесь без повторения обоснования.
type Identity interface {
	// Generate создаёт новую пару ключей. Возвращает IdentityPair
	// целиком (см. ниже), а не два отдельных значения.
	Generate(ctx context.Context) (IdentityPair, error)

	// PublicKeyOf возвращает публичный ключ, соответствующий
	// приватному — нужен при повторной загрузке уже существующего
	// приватного ключа (например, из Storage), без генерации заново.
	PublicKeyOf(ctx context.Context, key PrivateKey) (PublicKey, error)

	// Sign подписывает data приватным ключом.
	Sign(ctx context.Context, key PrivateKey, data []byte) (Signature, error)

	// Verify проверяет подпись. Возвращает (false, nil) для
	// структурно корректной, но неверной подписи — это ожидаемый
	// негативный результат, не ошибка. error — только при
	// техническом сбое (некорректная длина ключа/подписи и т.п.).
	// Вызывающий код обязан различать эти два случая.
	Verify(ctx context.Context, key PublicKey, data []byte, sig Signature) (bool, error)
}

// IdentityPair — приватный и публичный ключ одной генерации.
// Структура вместо двух возвращаемых значений: расширяема в будущем
// (например, полем Algorithm) без изменения сигнатуры Generate.
type IdentityPair struct {
	Private PrivateKey
	Public  PublicKey
}

// PublicKey, PrivateKey, Signature — именованные типы поверх байт,
// деталь схемы (Ed25519 сегодня) оставлена адаптеру. PrivateKey —
// []byte, а не opaque handle — см. ADR-014 (провизорно, до появления
// backend'а с аппаратной защитой ключа).
type PublicKey []byte
type PrivateKey []byte
type Signature []byte
