package dispatcher

import (
	"context"
	"errors"

	"github.com/wiloris/photon-core/services/session"
)

// Sentinel-ошибки пакета. Классификация — только errors.Is, никогда строковым
// поиском (ADR-033).
var (
	// ErrNoEndpoint — у инстанса нет адреса Bridge. Не ошибка аргументов
	// вызова: аргументы верны, отсутствует адресат. Инстанс без секции
	// "bridge" в config_json законен (action-routing.md §4.2).
	ErrNoEndpoint = errors.New("dispatcher: bridge endpoint is not configured")

	// ErrClosed — диспетчер закрыт (Photon_Destroy). Отличается от
	// session.ErrSessionClosed только уровнем; на границе ABI оба дают
	// PHOTON_ERR_INVALID_ARGUMENT, потому что хендл уходит и повторять вызов
	// не на чем.
	ErrClosed = errors.New("dispatcher: closed")
)

// Class — класс исхода вызова, из которого граница ABI получает PhotonResult.
//
// Почему классификация живёт здесь, а не в bindings, хотя PhotonResult — тип
// C. Спецификация (§7) сначала помещала таблицу в bindings; при реализации
// стало видно, что там её нельзя проверить на полноту: тест обязан находить
// новые sentinel'ы в session/errors.go, а из отдельного модуля bindings
// исходники Core недоступны без go-list-акробатики. Здесь же они лежат
// соседним каталогом, и TestClassifyCoversEverySentinel читает их напрямую —
// новый sentinel без строки в таблице роняет тест.
//
// Границы ADR-030 это не трогает: Class — обычный Go-тип, о C не знающий, а
// соответствие «ошибка ↔ код ABI» Core и так уже фиксирует — комментарий
// session.ErrUnknownAction прямо ссылается на PHOTON_ERR_INVALID_ACTION.
type Class int

// ClassInternal — НОЛЬ намеренно: незаполненное значение обязано означать
// «неизвестно, значит внутренняя ошибка», а не случайно оказаться успехом.
const (
	ClassInternal Class = iota
	ClassOK
	ClassInvalidArgument
	ClassInvalidAction
	ClassNotConnected
	ClassPermissionDenied
	ClassTimeout
)

func (c Class) String() string {
	switch c {
	case ClassOK:
		return "ok"
	case ClassInvalidArgument:
		return "invalid_argument"
	case ClassInvalidAction:
		return "invalid_action"
	case ClassNotConnected:
		return "not_connected"
	case ClassPermissionDenied:
		return "permission_denied"
	case ClassTimeout:
		return "timeout"
	case ClassInternal:
		return "internal"
	}
	return "unknown"
}

// classTable — соответствие «ошибка → класс». Порядок значим: побеждает
// первое совпадение.
//
// Порядок нужен не теоретически. session.markLost оборачивает исходную ошибку
// через `%w: %w`, поэтому одна ошибка может быть Is-совместима сразу с
// ErrConnectionLost и с чем-то из context — специфичные sentinel'ы сессии
// стоят выше context.* именно поэтому.
//
// Обоснование каждой неочевидной строки — action-routing.md §7.
var classTable = []struct {
	err   error
	class Class
}{
	// Отвергается без сети.
	{session.ErrUnknownAction, ClassInvalidAction},
	{session.ErrNoResourceForAction, ClassInvalidAction},
	{session.ErrInvalidPayload, ClassInvalidArgument},
	{ErrNoEndpoint, ClassNotConnected},

	// Связи нет. ErrNotReady и ErrConnectionLost намеренно сходятся в один
	// класс: оба ведут клиента к одному решению — повторить вызов, который и
	// переподключится (D-3, §7.1).
	{session.ErrNotReady, ClassNotConnected},
	{session.ErrConnectionLost, ClassNotConnected},

	// Хендл уходит — повторять не на чем.
	{ErrClosed, ClassInvalidArgument},
	{session.ErrSessionClosed, ClassInvalidArgument},

	// Сервер отверг session token: клиенту нужна новая аутентификация, а не
	// повтор.
	{session.ErrHandshakeRejected, ClassPermissionDenied},

	// Дефекты развёртывания и протокола: повтор не поможет. Не ABI_MISMATCH —
	// тот код означает несовпадение версии ABI клиента и ядра, и
	// переиспользовать его значило бы отправить клиента искать не ту причину.
	{session.ErrProtocolVersion, ClassInternal},
	{session.ErrModeNotAccepted, ClassInternal},
	{session.ErrMalformedFrame, ClassInternal},

	// Наружу не выходит никогда: его появление означает дефект синглфлайта
	// (action-routing.md §3.3). Строка здесь не «на всякий случай», а чтобы
	// TestConnectInProgressNeverEscapes мог утверждать это про конкретный
	// класс, а не про безымянный INTERNAL.
	{session.ErrConnectInProgress, ClassInternal},

	{context.DeadlineExceeded, ClassTimeout},
	{context.Canceled, ClassInvalidArgument},
}

// classifyKnown возвращает класс и признак того, что ошибка нашлась в
// таблице. Признак нужен тесту на полноту: без него «не нашлось» и
// «внутренняя ошибка» неразличимы, и таблица могла бы молча пустеть.
func classifyKnown(err error) (Class, bool) {
	for _, row := range classTable {
		if errors.Is(err, row.err) {
			return row.class, true
		}
	}
	return ClassInternal, false
}

// Classify относит ошибку к классу исхода вызова. nil — ClassOK.
func Classify(err error) Class {
	if err == nil {
		return ClassOK
	}
	c, _ := classifyKnown(err)
	return c
}
