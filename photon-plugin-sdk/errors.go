package sdk

import (
	"errors"
	"fmt"
)

// Ошибки SDK.
//
// Классификация повторяет коды из IPC-протокола
// (photon-plugin-runtime/docs/ipc.md, раздел 5), потому что автору
// плагина нужно различать те же ситуации, что различает Runtime.
// Отдельная, «упрощённая» классификация в SDK скрыла бы разницу,
// которая была специально введена.

var (
	// ErrInvalidRequest — запрос семантически некорректен.
	ErrInvalidRequest = errors.New("sdk: invalid request")

	// ErrUnauthorized — субъект не установлен: сессия не
	// аутентифицирована.
	//
	// Отличается от ErrForbidden принципиально: «я не знаю, кто ты»
	// означает проблему сессии, и повтор запроса бессмыслен.
	ErrUnauthorized = errors.New("sdk: unauthorized")

	// ErrForbidden — право на операцию не выдано.
	//
	// «Я знаю, кто ты, и тебе нельзя»: повтор бессмыслен до изменения
	// политики, но плагин может осмысленно сообщить об этом
	// пользователю — в отличие от ErrUnauthorized.
	ErrForbidden = errors.New("sdk: permission denied")

	// ErrNotFound — запрошенный объект не существует.
	ErrNotFound = errors.New("sdk: not found")

	// ErrInvalidState — операция невозможна в текущем состоянии.
	ErrInvalidState = errors.New("sdk: invalid state")

	// ErrUnsupported — операция не поддерживается этим Runtime.
	//
	// Возникает при вызове операции, которой нет в каталоге текущей
	// версии протокола. Молчаливое игнорирование недопустимо: плагин
	// решил бы, что операция выполнена.
	ErrUnsupported = errors.New("sdk: unsupported operation")

	// ErrInternal — сбой на стороне Runtime.
	ErrInternal = errors.New("sdk: internal runtime error")

	// ErrCancelled — запрос отменён.
	ErrCancelled = errors.New("sdk: cancelled")

	// ErrDeadlineExceeded — истёк дедлайн запроса.
	//
	// Дедлайн назначает Runtime; плагин не может его продлить
	// (docs/ipc.md, раздел 7).
	ErrDeadlineExceeded = errors.New("sdk: deadline exceeded")

	// ErrDecryptionFailed — расшифровка не удалась.
	ErrDecryptionFailed = errors.New("sdk: decryption failed")

	// ErrSessionClosed — сессия закрыта.
	//
	// Переподключения нет: новый процесс — новый handshake
	// (sdk-boundary.md, раздел 5). Плагину следует завершиться.
	ErrSessionClosed = errors.New("sdk: session closed")
)

// RuntimeError — ошибка, полученная от Runtime, с исходным кодом.
//
// Оборачивает одну из sentinel-ошибок выше, поэтому errors.Is работает
// как обычно, но сохраняет и оригинальное сообщение Runtime — оно
// содержательнее общей категории при отладке.
type RuntimeError struct {
	// Code — код из IPC-протокола ("FORBIDDEN", "NOT_FOUND", ...).
	Code string

	// Message — сообщение Runtime.
	Message string

	// sentinel — соответствующая ошибка SDK для errors.Is.
	sentinel error
}

func (e *RuntimeError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("sdk: runtime returned %s", e.Code)
	}
	return fmt.Sprintf("sdk: runtime returned %s: %s", e.Code, e.Message)
}

func (e *RuntimeError) Unwrap() error { return e.sentinel }

// errorForCode сопоставляет код протокола с sentinel-ошибкой.
//
// Неизвестный код отображается в ErrInternal, а не игнорируется: код,
// которого SDK не знает, означает рассинхрон версий, и трактовать его
// как успех нельзя.
func errorForCode(code, message string) error {
	if code == "" {
		return nil
	}

	var sentinel error
	switch code {
	case "INVALID_REQUEST":
		sentinel = ErrInvalidRequest
	case "UNAUTHORIZED":
		sentinel = ErrUnauthorized
	case "FORBIDDEN":
		sentinel = ErrForbidden
	case "NOT_FOUND":
		sentinel = ErrNotFound
	case "INVALID_STATE":
		sentinel = ErrInvalidState
	case "UNSUPPORTED":
		sentinel = ErrUnsupported
	case "CANCELLED":
		sentinel = ErrCancelled
	case "DEADLINE_EXCEEDED":
		sentinel = ErrDeadlineExceeded
	case "INTERNAL_ERROR":
		sentinel = ErrInternal
	default:
		sentinel = ErrInternal
	}

	return &RuntimeError{Code: code, Message: message, sentinel: sentinel}
}
