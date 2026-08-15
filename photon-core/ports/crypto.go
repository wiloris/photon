// Package ports — см. storage.go для общих правил пакета.
package ports

import "context"

// Crypto — порт симметричного шифрования и производных ключей.
// Первая реализация будет определена при выборе конкретной схемы
// шифрования (см. ROADMAP.md, Phase 4).
//
// Разделение ответственности: Identity — личность и подписи (см.
// identity.go), Crypto — конфиденциальность. См. ADR-011/013/014 —
// применяются здесь без повторения обоснования.
type Crypto interface {
	// GenerateKey создаёт новый случайный симметричный ключ.
	GenerateKey(ctx context.Context) (SymmetricKey, error)

	// DeriveKey детерминированно получает под-ключ из key для домена
	// domain: один и тот же (key, domain) всегда даёт один и тот же
	// под-ключ. domain — произвольная метка домена (например,
	// "plugin.wallet", "session:1234"), не обязана совпадать с каким-
	// либо Storage namespace — это независимое от Storage понятие.
	DeriveKey(ctx context.Context, key SymmetricKey, domain string) (SymmetricKey, error)

	// Encrypt шифрует plaintext ключом key. bindingData
	// аутентифицируется, но не шифруется: не защищено от чтения, но
	// Decrypt откажет, если при расшифровке передано не то же самое
	// значение, что и при Encrypt. Если привязывать не к чему —
	// nil. Nonce целиком управляется адаптером и не выведен в
	// контракт — вызывающий код не может допустить его повторное
	// использование, потому что не имеет к нему доступа.
	Encrypt(ctx context.Context, key SymmetricKey, plaintext, bindingData []byte) (ciphertext []byte, err error)

	// Decrypt расшифровывает ciphertext, используя key и то же
	// bindingData, что было передано при Encrypt. Возвращает
	// ErrDecryptionFailed на любую причину сбоя (неверный ключ,
	// повреждённые данные, несовпадающий bindingData) — порт не
	// различает их для вызывающего кода (см. errors.go).
	Decrypt(ctx context.Context, key SymmetricKey, ciphertext, bindingData []byte) (plaintext []byte, err error)
}

// SymmetricKey — именованный тип поверх байт, деталь схемы шифрования
// оставлена адаптеру. []byte, а не opaque handle — см. ADR-014
// (применяется здесь без новой записи).
type SymmetricKey []byte
