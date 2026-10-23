package dispatcher

import (
	"context"
	"errors"

	"github.com/wiloris/photon-core/services/diag"
	"github.com/wiloris/photon-core/services/localfeed"
	"github.com/wiloris/photon-core/services/outbox"
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

	// ErrNoStorage — у инстанса нет локального backend (ADR-064 п. 1.5).
	// Отсутствие секции storage ошибкой создания НЕ является: инстанс
	// законен и сетевые действия исполняет. Локальное действие на таком
	// инстансе получает этот отказ — ровно как сетевое получает
	// ErrNoEndpoint на инстансе без адреса Bridge.
	ErrNoStorage = errors.New("dispatcher: local storage is not configured")

	// ErrStorageUnavailable — путь задан и годен, а хранилище по нему
	// открыть или мигрировать не удалось: права, блокировка другим
	// процессом, повреждение (ADR-064 п. 1.6). Отличается от негодной
	// конфигурации: проверять вызывающему нечего, конфигурация верна.
	ErrStorageUnavailable = errors.New("dispatcher: local storage is unavailable")
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
	// Локальный код в сессии (ADR-130 §2.8). Через SendAction недостижимо —
	// диспетчер исполняет сотню 800–899 сам; строка нужна для полноты и на
	// случай, если рубеж диспетчера однажды сломают: тогда клиент получит
	// отказ кода, а не «internal error».
	{session.ErrLocalAction, ClassInvalidAction},
	{session.ErrInvalidPayload, ClassInvalidArgument},
	{ErrNoEndpoint, ClassNotConnected},

	// Диагностика (ADR-130 §2.7): курсор впереди журнала, лимит вне 1…512,
	// ёмкость вне 64…8192 — ошибки вызывающего. 820 оборачивает их ещё и в
	// ErrInvalidPayload; строки здесь — на случай, если sentinel дойдёт
	// до границы без обёртки.
	{diag.ErrInvalidCursor, ClassInvalidArgument},
	{diag.ErrInvalidLimit, ClassInvalidArgument},
	{diag.ErrInvalidCapacity, ClassInvalidArgument},

	// Инварианты локальной ленты (ADR-065 п. 8). Все семь — ошибки
	// ВЫЗЫВАЮЩЕГО по тому же критерию, которым пользуется вся таблица: есть
	// ли что исправлять в вызове. Есть: блок противоречит сам себе, и повтор
	// с тем же payload даст тот же отказ.
	//
	// Строки стоят ВЫШЕ ErrStorageUnavailable, и это не оформление. Локальная
	// запись оборачивает ошибку в один из двух классов (local.go,
	// wrapFeedWriteError), и если новый sentinel забудут внести в его список,
	// ошибка придёт обёрнутой как сбой хранилища, сохранив исходный sentinel
	// внутри. Побеждает первое совпадение — значит выигрывает конкретная
	// причина, а не общая обёртка, и вызывающий узнаёт, что чинить payload, а
	// не диск. Тот же принцип, по которому sentinel'ы сессии стоят выше
	// context.*.
	//
	// Своего класса здесь не заводится: ClassInvalidArgument описывает ровно
	// это и уже отображён на PHOTON_ERR_INVALID_ARGUMENT.
	{localfeed.ErrEmptyChannel, ClassInvalidArgument},
	{localfeed.ErrEmptyRecordID, ClassInvalidArgument},
	{localfeed.ErrAnchorRequired, ClassInvalidArgument},
	{localfeed.ErrEmptyBlock, ClassInvalidArgument},
	{localfeed.ErrUnknownDirection, ClassInvalidArgument},
	{localfeed.ErrAnchorNotAllowed, ClassInvalidArgument},
	{localfeed.ErrRecordCrossesAnchor, ClassInvalidArgument},
	{localfeed.ErrEmptyCursor, ClassInvalidArgument},
	{localfeed.ErrInvalidChannelID, ClassInvalidArgument},

	// Исходящие (ADR-093, docs/local-actions.md). Все пять — ошибки
	// вызывающего: неверная очередь, тело, срок аренды; op_id, которого нет;
	// переход, запрещённый текущим состоянием. ErrQueueEmpty и ErrHeadLeased
	// сюда НЕ входят: это штатные исходы аренды, и наружу они выходят
	// результатом 807 ({"leased":false,"reason":…}), а не ошибкой.
	{outbox.ErrInvalidQueue, ClassInvalidArgument},
	{outbox.ErrInvalidBody, ClassInvalidArgument},
	{outbox.ErrInvalidLease, ClassInvalidArgument},
	{outbox.ErrUnknownOp, ClassInvalidArgument},
	{outbox.ErrStateConflict, ClassInvalidArgument},

	// Потоковая передача. Все четыре — ошибки АРГУМЕНТА или КАДРА, а не
	// связи: повторять вызов с теми же параметрами бессмысленно, и класс
	// обязан вести вызывающего не к ретраю, а к исправлению.
	//
	// ErrStreamDirection и ErrBodyLengthInvalid — заведомо ошибка вызывающего
	// или сервера, обнаруженная до чтения тела. ErrBodyLengthRequired и
	// ErrBodyTruncated — нарушение контракта кадра сервером: тело либо не
	// объявлено, либо кончилось раньше объявленного. Ни то ни другое не
	// лечится повтором, поэтому не ClassNotConnected (media-transfer.md §3, §7).
	{session.ErrStreamDirection, ClassInvalidArgument},
	{session.ErrBodyLengthInvalid, ClassInvalidArgument},
	{session.ErrBodyLengthRequired, ClassInvalidArgument},
	{session.ErrBodyTruncated, ClassInvalidArgument},

	// Связи нет. ErrNotReady и ErrConnectionLost намеренно сходятся в один
	// класс: оба ведут клиента к одному решению — повторить вызов, который и
	// переподключится (D-3, §7.1).
	{session.ErrNotReady, ClassNotConnected},
	{session.ErrConnectionLost, ClassNotConnected},

	// Локального backend нет. Как и ErrNoEndpoint, это не ошибка аргументов:
	// действие названо верно, исполнять его нечем.
	{ErrNoStorage, ClassNotConnected},

	// Хранилище есть в конфигурации, но недоступно. Повтор вызова не
	// поможет, и это не вина параметров — INTERNAL.
	{ErrStorageUnavailable, ClassInternal},

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
