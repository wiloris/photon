package ports

import "errors"

// Sentinel-ошибки ports. Адаптеры обязаны возвращать именно эти
// значения через errors.Is, а не создавать собственные типы ошибок
// с похожим смыслом — иначе services/ не сможет обрабатывать ошибки
// единообразно независимо от того, какой адаптер сейчас подключён
// (см. ARCHITECTURE.md, раздел 3).

// Storage
var (
	// ErrNotFound — ключ отсутствует в namespace.
	ErrNotFound = errors.New("ports: key not found")

	// ErrNamespaceNotFound — namespace не подготовлен миграцией.
	// Возвращается из Get/Put/Delete/ForEach/ForEachPrefix, если
	// Storage.Migrate ещё не зарегистрировал этот namespace.
	// Порт намеренно не создаёт namespace неявно — см. ADR-010.
	ErrNamespaceNotFound = errors.New("ports: namespace not found")

	// ErrCursorOutOfPrefix — непустой start в ForEachPrefixFrom не
	// принадлежит prefix. Это ошибка ВЫЗЫВАЮЩЕГО, а не краевой случай
	// данных: такой start возникает ровно одним способом — курсор,
	// взятый от другого префикса. Тихая пустая выдача выглядела бы как
	// «данные закончились», а обход с начала — как «данные другого
	// префикса»; обе неверны и обе молчаливы. См. ADR-047.
	ErrCursorOutOfPrefix = errors.New("ports: cursor start is outside prefix")
)

// Crypto
var (
	// ErrDecryptionFailed — ciphertext повреждён, подделан, или
	// bindingData не совпадает с тем, что было передано при
	// Encrypt. Единая ошибка на все три случая: Crypto.Decrypt —
	// аутентифицированное шифрование (см. crypto.go), различать
	// "неверный ключ" от "подделанные данные" на уровне порта
	// специально не нужно — оба случая требуют одного и того же
	// действия от вызывающего кода (отклонить данные), а более
	// детальная диагностика в лучшем случае бесполезна, а в
	// худшем — источник side-channel информации об устройстве шифра.
	ErrDecryptionFailed = errors.New("ports: decryption failed")

	// ErrInvalidKey — переданный SymmetricKey некорректен для
	// используемой адаптером схемы шифрования. Порт намеренно не
	// уточняет причину (длина, формат, повреждение) — это знание
	// принадлежит адаптеру и не должно становиться частью контракта,
	// на который полагается вызывающий код.
	ErrInvalidKey = errors.New("ports: invalid key")
)

// Media
var (
	// ErrUnsupportedFormat — адаптер не распознал или не поддерживает
	// формат переданных данных.
	ErrUnsupportedFormat = errors.New("ports: unsupported media format")
)

// P2P
var (
	// ErrSessionFailed — согласование сессии не удалось (NAT traversal
	// не удался, peer недостижим ни напрямую, ни через relay).
	ErrSessionFailed = errors.New("ports: p2p session failed")
)

// Notification
var (
	// ErrRecipientUnreachable — NotificationTarget недействителен или
	// отозван (например, push-токен просрочен или отвязан).
	ErrRecipientUnreachable = errors.New("ports: recipient unreachable")
)

// IdentityProvider
var (
	// ErrTokenInvalid — токен не прошёл проверку: невалидная подпись,
	// истёкший срок действия, неверный issuer/audience, некорректный
	// формат. Порт намеренно не различает эти причины (см.
	// identityprovider.go) — все они означают одно для вызывающего:
	// этому токену нельзя доверять. Технический сбой самой проверки
	// (например, недоступен JWKS-эндпоинт провайдера) — НЕ эта ошибка,
	// а обычный wrapped error: токен мог быть валиден, проверить не
	// удалось.
	ErrTokenInvalid = errors.New("ports: token invalid")
)

// Plugin (Authorizer)
var (
	// ErrUnknownCaller — субъект не установлен: caller не
	// зарегистрирован ни с одним известным субъектом (RegisterCaller
	// не вызывался для этого PublicKey). Отличается от (false, nil):
	// тот означает «субъект известен, права нет» (FORBIDDEN); эта
	// ошибка означает «субъект неизвестен» (UNAUTHORIZED) — то же
	// разделение, что уже зафиксировано в docs/ipc.md, раздел 5.
	// См. ADR-033.
	ErrUnknownCaller = errors.New("ports: unknown caller")
)
