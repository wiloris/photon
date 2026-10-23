// Package ports — см. storage.go для общих правил пакета.
package ports

import "context"

// ExternalIdentity — claims, извлечённые из проверенного ID-токена
// внешнего Identity Provider. Минимальный набор полей, необходимый
// Account Service для поиска/создания Account — не полный набор
// OIDC-claims (нет имени, аватара, локали и т.п. — это на будущее, по
// подтверждённой потребности, а не заранее).
type ExternalIdentity struct {
	// Provider — стабильный идентификатор Identity Provider (выбирается
	// реализацией адаптера, например "google", "apple" или иной — не
	// отображаемое имя, а идентификатор, по которому AccountService
	// ищет запись). Не зашит как enum/константа в порт — это деталь
	// адаптера. Обязателен: Subject уникален только внутри одного
	// IdP, не глобально.
	Provider string

	// Subject — стабильный идентификатор пользователя у Provider
	// (OIDC "sub"). Account ищется по паре (Provider, Subject), не
	// по одному Subject и не по Email — email может отсутствовать
	// или измениться, а один и тот же Subject в принципе может
	// существовать одновременно у разных IdP без конфликта: сегодня
	// единственный Provider — Google, но как только появится второй
	// (Apple, корпоративный OIDC), Subject без Provider перестаёт
	// быть надёжным ключом (см. docs/account-device-flow.md,
	// сценарий 1).
	Subject string

	// Email — может быть пустым, если IdP его не предоставляет.
	// Не используется как ключ поиска Account.
	Email string
}

// IdentityProvider — порт проверки внешнего OAuth/OIDC ID-токена.
// Первая реализация — adapters/googleoidc (Google); архитектурно
// заменим на другой (Apple, Microsoft, GitHub, корпоративный OIDC) —
// см. ADR-021, ARCHITECTURE.md раздел 9.1. Ни один метод, тип или имя
// в этом файле не упоминает Google — см. ADR-011/013.
//
// Не путать с ports.Identity (identity.go) — та же путаница, от
// которой предостерегает разница имён: Identity — криптографическая
// личность устройства (Ed25519, локальная, не требует сети);
// IdentityProvider — проверка внешней OAuth-личности пользователя
// (сетевой вызов к стороннему IdP). Разные концепции, разные порты,
// поэтому ExternalIdentity — отдельный тип, а не переиспользование
// имени Identity для другого смысла.
//
// Не описывает вход/логин как таковой (это services/account, поверх
// этого порта) — только проверку токена, который уже получен
// вызывающим кодом от IdP каким-то другим способом (эта часть — не
// ответственность ядра, а конкретного bindings/клиента, инициирующего
// OAuth-flow).
type IdentityProvider interface {
	// VerifyIDToken проверяет подпись и срок действия token, возвращая
	// claims. ErrTokenInvalid — токен невалиден, просрочен, или подпись
	// не прошла проверку у данного IdP; порт не различает эти случаи
	// для вызывающего кода (см. errors.go).
	VerifyIDToken(ctx context.Context, token string) (ExternalIdentity, error)
}
