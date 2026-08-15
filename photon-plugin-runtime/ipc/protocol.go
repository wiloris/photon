package ipc

import "fmt"

// Типы сообщений — ровно восемь, по docs/ipc.md, раздел 11.
//
// Намеренно отсутствуют: heartbeat (живость определяется по состоянию
// процесса — забота supervisor, не протокола), reconnect (сессия не
// переживает разрыв: новый процесс, новый handshake), streaming
// (потребность не подтверждена). Реализация следует спецификации и не
// расширяет её по ходу работы.
type MessageType uint8

const (
	MsgHello    MessageType = 1 // Runtime -> Plugin
	MsgHelloAck MessageType = 2 // Plugin -> Runtime
	MsgRequest  MessageType = 3 // Plugin -> Runtime
	MsgResponse MessageType = 4 // Runtime -> Plugin
	MsgCancel   MessageType = 5 // обе стороны
	MsgEvent    MessageType = 6 // Runtime -> Plugin
	MsgStop     MessageType = 7 // Runtime -> Plugin
	MsgStopAck  MessageType = 8 // Plugin -> Runtime
)

func (t MessageType) String() string {
	switch t {
	case MsgHello:
		return "HELLO"
	case MsgHelloAck:
		return "HELLO_ACK"
	case MsgRequest:
		return "REQUEST"
	case MsgResponse:
		return "RESPONSE"
	case MsgCancel:
		return "CANCEL"
	case MsgEvent:
		return "EVENT"
	case MsgStop:
		return "STOP"
	case MsgStopAck:
		return "STOP_ACK"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(t))
	}
}

func (t MessageType) valid() bool {
	return t >= MsgHello && t <= MsgStopAck
}

// ErrorCode — машинные категории ошибок (docs/ipc.md, раздел 5).
type ErrorCode string

const (
	// ErrInvalidRequest — синтаксически корректно, семантически
	// бессмысленно.
	ErrInvalidRequest ErrorCode = "INVALID_REQUEST"

	// ErrUnauthorized — субъект НЕ УСТАНОВЛЕН: handshake не пройден,
	// сессия не аутентифицирована.
	//
	// Не смешивается с ErrForbidden: "я не знаю, кто ты" означает
	// проблему сессии (повтор бессмыслен), а "знаю, но нельзя" —
	// проблему прав (плагин может осмысленно сообщить пользователю).
	// Слияние лишило бы SDK возможности обработать их различно.
	ErrUnauthorized ErrorCode = "UNAUTHORIZED"

	// ErrForbidden — субъект установлен, но capability отсутствует.
	ErrForbidden ErrorCode = "FORBIDDEN"

	ErrNotFound     ErrorCode = "NOT_FOUND"
	ErrInvalidState ErrorCode = "INVALID_STATE"
	ErrUnsupported  ErrorCode = "UNSUPPORTED"
	ErrInternal     ErrorCode = "INTERNAL_ERROR"

	// ErrCancelled — запрос отменён (CANCEL или отмена со стороны
	// Runtime).
	ErrCancelled ErrorCode = "CANCELLED"

	// ErrDeadlineExceeded — истёк дедлайн запроса.
	ErrDeadlineExceeded ErrorCode = "DEADLINE_EXCEEDED"
)

// Message — единица протокола.
//
// Поля plugin_id здесь НЕТ намеренно. Идентичность вызывающего
// определяется КАНАЛОМ, а не содержимым сообщения: Runtime сам
// запустил процесс и сам открыл соединение (docs/ipc.md, раздел 2).
// Появление plugin_id в запросе — нарушение протокола, а не поле для
// чтения.
type Message struct {
	Type MessageType

	// RequestID — корреляция запрос/ответ. Уникален в пределах СЕССИИ,
	// не плагина: после перезапуска нумерация начинается заново.
	RequestID uint64

	// Op — имя операции для MsgRequest ("storage.read").
	Op string

	// Payload — тело операции; для Runtime непрозрачно.
	Payload []byte

	// ErrorCode/ErrorMessage — заполняются в MsgResponse при ошибке.
	ErrorCode    ErrorCode
	ErrorMessage string

	// HelloProtocolVersion / HelloPluginID / HelloCapabilities —
	// только для MsgHello/MsgHelloAck.
	//
	// PluginID здесь допустим и обязателен: это единственное место,
	// где плагин заявляет о себе, и Runtime СВЕРЯЕТ заявленное с тем,
	// что уже знает из манифеста, а не принимает на веру.
	HelloProtocolVersion int
	HelloPluginID        string
	HelloCapabilities    []string

	// EventType — для MsgEvent.
	EventType string
}
