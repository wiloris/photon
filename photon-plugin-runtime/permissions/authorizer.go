package permissions

import (
	"context"
	"fmt"
	"regexp"
	"sync"

	"github.com/wiloris/photon/photon-core/ports"
)

// permissionPattern — "домен.действие": chat.send, storage.read.
//
// Тот же синтаксис, что проверяет manifest, но проверка здесь
// независимая: permissions НЕ импортирует manifest. Отсутствие
// зависимости позволяет выдавать права из источников, не связанных с
// манифестом (решение пользователя, trust policy), — и означает, что
// оба пакета должны уметь проверить строку самостоятельно.
var permissionPattern = regexp.MustCompile(`^[a-z0-9]+(\.[a-z0-9]+)+$`)

func isValidPermission(perm string) bool {
	return permissionPattern.MatchString(perm)
}

// ErrUnknownCaller — предъявленный PublicKey не связан ни с одним
// известным плагином.
//
// Техническая ошибка, а не DENY: "я не знаю, кто ты" и "я знаю, и тебе
// нельзя" — разные ситуации (docs/ipc.md, раздел 5: UNAUTHORIZED
// против FORBIDDEN).
//
// Оборачивает ports.ErrUnknownCaller (ADR-033) через %w: сохраняет
// собственное имя и текст для тех, кто уже проверяет
// errors.Is(err, permissions.ErrUnknownCaller), но одновременно
// удовлетворяет errors.Is(err, ports.ErrUnknownCaller) — это то, что
// ipc.Session матчит, не импортируя permissions напрямую (ipc не
// должен зависеть от конкретной реализации Authorizer).
var ErrUnknownCaller = fmt.Errorf("permissions: caller is not a registered plugin: %w", ports.ErrUnknownCaller)

// Authorizer реализует ports.Authorizer поверх Policy.
//
// Контракт заморожен в photon-core (ADR-020): ядро определяет
// абстракцию авторизации, но не знает про плагины, манифесты и trust
// levels. Здесь — реализация, знающая про всё это.
//
// Ключевое: Authorize принимает ports.PublicKey, а НЕ plugin_id
// строкой. Идентичность вызывающего криптографическая, установленная
// Runtime при запуске процесса, — не значение, присланное самим
// плагином в запросе. Плагин физически не может подделать субъект,
// потому что не участвует в его определении (docs/ipc.md, раздел 2).
type Authorizer struct {
	policy *Policy

	mu sync.RWMutex
	// callers[hex(publicKey)] -> Subject.
	//
	// Заполняется Runtime при запуске плагина: только Runtime знает,
	// какой ключ соответствует какому plugin_id, потому что сам их и
	// связал.
	callers map[string]Subject
}

var _ ports.Authorizer = (*Authorizer)(nil)

func NewAuthorizer(policy *Policy) *Authorizer {
	return &Authorizer{
		policy:  policy,
		callers: make(map[string]Subject),
	}
}

// RegisterCaller связывает публичный ключ с субъектом.
//
// Вызывается Runtime при запуске процесса плагина, до того как плагин
// сможет отправить хоть один запрос. Плагин в этом не участвует.
func (a *Authorizer) RegisterCaller(key ports.PublicKey, subject Subject) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.callers[callerKey(key)] = subject
}

// UnregisterCaller разрывает связь ключа с субъектом — при остановке
// плагина. После этого Authorize вернёт ErrUnknownCaller.
func (a *Authorizer) UnregisterCaller(key ports.PublicKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.callers, callerKey(key))
}

// Authorize — см. ports.Authorizer.
//
// Семантика возврата задана контрактом:
//
//	(true,  nil) — разрешено
//	(false, nil) — запрещено; ожидаемый отрицательный результат
//	(false, err) — решение принять невозможно (техническая проблема)
func (a *Authorizer) Authorize(ctx context.Context, caller ports.PublicKey, permission ports.Permission) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	perm := string(permission)
	if !isValidPermission(perm) {
		// Некорректное имя — техническая ошибка вызывающего кода, а не
		// отказ в доступе: DENY здесь скрыл бы опечатку в имени права
		// за видимостью нормальной работы политики.
		return false, fmt.Errorf("%w: %q", ErrInvalidPermission, perm)
	}

	a.mu.RLock()
	subject, known := a.callers[callerKey(caller)]
	a.mu.RUnlock()
	if !known {
		return false, ErrUnknownCaller
	}

	decision, err := a.policy.Evaluate(subject, perm)
	if err != nil {
		return false, err
	}
	return decision == DecisionAllow, nil
}

// callerKey — ключ карты по байтам публичного ключа.
//
// []byte нельзя использовать как ключ карты в Go, поэтому строковое
// представление. Не hex ради читаемости: строка из байт дешевле и не
// требует кодирования на каждый Authorize, который вызывается на
// КАЖДЫЙ запрос плагина.
func callerKey(key ports.PublicKey) string {
	return string(key)
}
