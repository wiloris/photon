package host

import "fmt"

// ExitKind — причина завершения плагина.
//
// Шесть категорий (docs/lifecycle.md, раздел 5), а не один ERROR:
// supervisor принимает разные решения в зависимости от причины, и
// склеивание их в одно значение лишило бы его этой возможности.
//
// Host КЛАССИФИЦИРУЕТ завершение, но НЕ решает, что делать дальше —
// перезапуск, backoff и disable policy принадлежат supervisor
// (инвариант 6).
type ExitKind int

const (
	// ExitCleanStop — штатная остановка по запросу Runtime.
	// Перезапуск не нужен.
	ExitCleanStop ExitKind = iota

	// ExitStartupFailure — процесс не запустился: нет файла, нет прав
	// на исполнение, ошибка exec. Перезапуск обычно бессмыслен —
	// supervisor вправе считать это против лимита агрессивнее.
	ExitStartupFailure

	// ExitHandshakeFailure — процесс запустился, handshake не прошёл:
	// несовместимая версия протокола, чужой plugin_id, таймаут.
	ExitHandshakeFailure

	// ExitCrash — процесс завершился ненулевым кодом или по сигналу.
	// Кандидат на перезапуск с backoff.
	ExitCrash

	// ExitProtocolViolation — некорректный кадр IPC, обращение в обход
	// Runtime, заявка недекларированных capabilities.
	//
	// Принципиально отличается от ExitCrash: сбой переживается
	// перезапуском, а нарушение протокола повторится с той же
	// вероятностью и может быть попыткой атаки. Supervisor НЕ
	// перезапускает автоматически.
	ExitProtocolViolation

	// ExitAuthorizationFailure — плагин систематически запрашивает то,
	// на что не имеет прав. Само по себе не завершение; host сообщает
	// об этом, supervisor решает по порогу.
	ExitAuthorizationFailure
)

func (k ExitKind) String() string {
	switch k {
	case ExitCleanStop:
		return "clean_stop"
	case ExitStartupFailure:
		return "startup_failure"
	case ExitHandshakeFailure:
		return "handshake_failure"
	case ExitCrash:
		return "crash"
	case ExitProtocolViolation:
		return "protocol_violation"
	case ExitAuthorizationFailure:
		return "authorization_failure"
	default:
		return fmt.Sprintf("unknown(%d)", int(k))
	}
}

// AutoRestartAllowed сообщает, допустим ли автоматический перезапуск
// при такой причине завершения.
//
// Это ПОДСКАЗКА для supervisor, а не решение: окончательное слово за
// ним (лимиты, backoff, disable policy). Host лишь фиксирует то, что
// следует из природы самой причины.
func (k ExitKind) AutoRestartAllowed() bool {
	switch k {
	case ExitCrash, ExitHandshakeFailure:
		return true
	case ExitCleanStop, ExitStartupFailure, ExitProtocolViolation, ExitAuthorizationFailure:
		return false
	default:
		return false
	}
}

// ExitStatus — полный результат завершения.
type ExitStatus struct {
	Kind ExitKind

	// Code — код возврата процесса, если он завершился нормально.
	// -1, если процесс убит сигналом или не запускался.
	Code int

	// Err — исходная ошибка, если была.
	Err error
}

func (s ExitStatus) String() string {
	if s.Err != nil {
		return fmt.Sprintf("%s (code=%d): %v", s.Kind, s.Code, s.Err)
	}
	return fmt.Sprintf("%s (code=%d)", s.Kind, s.Code)
}
