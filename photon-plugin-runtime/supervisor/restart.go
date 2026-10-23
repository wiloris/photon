package supervisor

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/wiloris/photon-plugin-runtime/host"
)

// RestartPolicy решает, перезапускать ли упавший плагин и через сколько.
//
// Решение принимает supervisor, а не host: host лишь классифицирует
// причину завершения (docs/lifecycle.md, раздел 5, инвариант 6).

// RestartConfig — параметры политики.
type RestartConfig struct {
	// BaseDelay — задержка перед первым перезапуском.
	BaseDelay time.Duration

	// MaxDelay — потолок экспоненциального роста.
	MaxDelay time.Duration

	// MaxAttempts — сколько последовательных неудач допустимо до
	// перехода в DISABLED.
	MaxAttempts int

	// StableAfter — сколько плагин должен проработать, чтобы счётчик
	// неудач сбросился.
	//
	// Без сброса плагин, стабильно работавший неделю и упавший
	// однажды, оказался бы ближе к DISABLED, чем заслуживает
	// (docs/lifecycle.md, раздел 6).
	StableAfter time.Duration

	// JitterFraction — доля случайного отклонения к задержке (0.2 =
	// ±20%).
	//
	// Jitter обязателен, а не «желателен»: несколько плагинов, упавших
	// одновременно по общей причине (перезапуск сети, обновление
	// ядра), без него перезапускались бы синхронной волной, повторяя
	// нагрузку, которая их и уронила.
	JitterFraction float64

	// rng — источник случайности; подменяется в тестах, чтобы jitter
	// был детерминирован.
	rng func() float64
}

// DefaultRestartConfig — разумные значения по умолчанию.
func DefaultRestartConfig() RestartConfig {
	return RestartConfig{
		BaseDelay:      1 * time.Second,
		MaxDelay:       5 * time.Minute,
		MaxAttempts:    5,
		StableAfter:    60 * time.Second,
		JitterFraction: 0.2,
	}
}

// RestartDecision — что делать после завершения плагина.
type RestartDecision struct {
	// Restart — нужно ли перезапускать.
	Restart bool

	// Delay — через сколько, если Restart == true.
	Delay time.Duration

	// Disable — исчерпан лимит либо причина не допускает перезапуска;
	// плагин переходит в DISABLED.
	Disable bool

	// Reason — человекочитаемое объяснение, для логов и Developer Tools.
	Reason string
}

// restartState — счётчики одного плагина.
type restartState struct {
	mu sync.Mutex

	// consecutiveFailures — подряд идущие неудачи. Сбрасывается после
	// StableAfter непрерывной работы.
	consecutiveFailures int

	// lastStart — когда экземпляр был запущен в последний раз.
	lastStart time.Time
}

// Decide вычисляет решение о перезапуске.
//
// Ключевое разделение (docs/lifecycle.md, раздел 5):
//
//	crash, handshake_failure  -> перезапуск с backoff
//	protocol_violation        -> НЕ перезапускается автоматически
//	clean_stop                -> перезапуск не нужен
//	startup_failure           -> перезапуск бессмыслен
//	authorization_failure     -> порог, затем DISABLED
//
// protocol_violation отделён от crash намеренно: сбой переживается
// перезапуском, а нарушение протокола повторится с той же
// вероятностью — и может быть попыткой атаки, а не багом.
func (c RestartConfig) Decide(st *restartState, exit host.ExitStatus, now time.Time) RestartDecision {
	st.mu.Lock()
	defer st.mu.Unlock()

	// Сброс счётчика после стабильной работы — до анализа причины:
	// плагин, продержавшийся StableAfter, начинает новую серию.
	if !st.lastStart.IsZero() && c.StableAfter > 0 {
		if now.Sub(st.lastStart) >= c.StableAfter {
			st.consecutiveFailures = 0
		}
	}

	switch exit.Kind {
	case host.ExitCleanStop:
		st.consecutiveFailures = 0
		return RestartDecision{Reason: "clean stop, no restart needed"}

	case host.ExitProtocolViolation:
		// Не считаем против лимита и не перезапускаем: это не сбой, а
		// поведение, требующее внимания человека.
		return RestartDecision{
			Disable: true,
			Reason:  "protocol violation: not restarted automatically",
		}

	case host.ExitStartupFailure:
		// Перезапуск бессмыслен: файла нет, прав нет — само не
		// починится. Считаем против лимита агрессивнее (сразу
		// исчерпываем), чтобы не крутить бесполезный цикл.
		st.consecutiveFailures = c.MaxAttempts
		return RestartDecision{
			Disable: true,
			Reason:  "startup failure: plugin cannot be launched",
		}

	case host.ExitAuthorizationFailure:
		st.consecutiveFailures++
		if st.consecutiveFailures >= c.MaxAttempts {
			return RestartDecision{
				Disable: true,
				Reason:  "repeated authorization failures",
			}
		}
		return RestartDecision{
			Restart: true,
			Delay:   c.delayFor(st.consecutiveFailures),
			Reason:  "authorization failure below threshold",
		}

	case host.ExitCrash, host.ExitHandshakeFailure:
		st.consecutiveFailures++
		if st.consecutiveFailures > c.MaxAttempts {
			return RestartDecision{
				Disable: true,
				Reason:  "restart limit exhausted",
			}
		}
		return RestartDecision{
			Restart: true,
			Delay:   c.delayFor(st.consecutiveFailures),
			Reason:  exit.Kind.String() + ": restarting with backoff",
		}

	default:
		return RestartDecision{
			Disable: true,
			Reason:  "unknown exit kind: " + exit.Kind.String(),
		}
	}
}

// delayFor — экспоненциальный backoff с jitter.
//
//	attempt 1 -> base
//	attempt 2 -> base*2
//	attempt 3 -> base*4
//	...       -> не выше MaxDelay
func (c RestartConfig) delayFor(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	base := float64(c.BaseDelay)
	// 2^(attempt-1), с защитой от переполнения на больших attempt.
	factor := math.Pow(2, float64(attempt-1))
	if factor > 1e6 {
		factor = 1e6
	}
	delay := base * factor

	if c.MaxDelay > 0 && delay > float64(c.MaxDelay) {
		delay = float64(c.MaxDelay)
	}

	// Jitter: ±JitterFraction от вычисленной задержки.
	if c.JitterFraction > 0 {
		r := c.random()     // [0,1)
		offset := (r*2 - 1) // [-1,1)
		delay *= 1 + offset*c.JitterFraction
		if delay < 0 {
			delay = 0
		}
	}

	return time.Duration(delay)
}

func (c RestartConfig) random() float64 {
	if c.rng != nil {
		return c.rng()
	}
	return rand.Float64()
}

// markStarted фиксирует момент запуска — от него отсчитывается
// StableAfter.
func (st *restartState) markStarted(now time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.lastStart = now
}

// failures возвращает текущий счётчик — для диагностики и тестов.
func (st *restartState) failures() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.consecutiveFailures
}
