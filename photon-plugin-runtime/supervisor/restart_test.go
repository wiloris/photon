package supervisor

import (
	"testing"
	"time"

	"github.com/wiloris/photon/photon-plugin-runtime/host"
)

// fixedRNG делает jitter детерминированным: 0.5 -> нулевое отклонение
// (offset = 0.5*2-1 = 0).
func testConfig() RestartConfig {
	c := DefaultRestartConfig()
	c.BaseDelay = 100 * time.Millisecond
	c.MaxDelay = 2 * time.Second
	c.MaxAttempts = 3
	c.StableAfter = time.Minute
	c.rng = func() float64 { return 0.5 }
	return c
}

// ── Backoff ─────────────────────────────────────────────────────────

func TestBackoff_Exponential(t *testing.T) {
	c := testConfig() // jitter нейтрализован
	want := []time.Duration{
		100 * time.Millisecond, // 2^0
		200 * time.Millisecond, // 2^1
		400 * time.Millisecond, // 2^2
		800 * time.Millisecond, // 2^3
	}
	for i, w := range want {
		got := c.delayFor(i + 1)
		if got != w {
			t.Fatalf("attempt %d: delay = %v, want %v", i+1, got, w)
		}
	}
}

func TestBackoff_CappedAtMaxDelay(t *testing.T) {
	c := testConfig()
	// 2^20 * 100ms далеко за пределом MaxDelay=2s.
	if got := c.delayFor(20); got != c.MaxDelay {
		t.Fatalf("delay = %v, want cap %v", got, c.MaxDelay)
	}
}

func TestBackoff_NoOverflowOnHugeAttempt(t *testing.T) {
	// math.Pow(2, 10000) = +Inf; без защиты получили бы бессмысленную
	// длительность.
	c := testConfig()
	got := c.delayFor(10000)
	if got != c.MaxDelay {
		t.Fatalf("delay = %v, want cap %v (no overflow)", got, c.MaxDelay)
	}
}

func TestBackoff_JitterIsApplied(t *testing.T) {
	// Jitter обязателен: без него плагины, упавшие по общей причине,
	// перезапускались бы синхронной волной.
	c := testConfig()
	c.JitterFraction = 0.5

	c.rng = func() float64 { return 0.0 } // offset = -1 -> -50%
	low := c.delayFor(1)

	c.rng = func() float64 { return 1.0 } // offset = +1 -> +50%
	high := c.delayFor(1)

	if low >= high {
		t.Fatalf("jitter has no effect: low=%v high=%v", low, high)
	}
	if low != 50*time.Millisecond {
		t.Fatalf("low = %v, want 50ms (-50%% of 100ms)", low)
	}
	if high != 150*time.Millisecond {
		t.Fatalf("high = %v, want 150ms (+50%% of 100ms)", high)
	}
}

func TestBackoff_JitterProducesSpread(t *testing.T) {
	// С настоящим rand несколько вызовов должны дать разные значения —
	// иначе синхронная волна перезапусков сохраняется.
	c := DefaultRestartConfig()
	c.BaseDelay = time.Second
	c.JitterFraction = 0.2

	seen := make(map[time.Duration]bool)
	for i := 0; i < 20; i++ {
		seen[c.delayFor(1)] = true
	}
	if len(seen) < 5 {
		t.Fatalf("only %d distinct delays out of 20 — jitter is not spreading restarts", len(seen))
	}
}

// ── Классификация причин ────────────────────────────────────────────

func TestDecide_CrashRestartsWithBackoff(t *testing.T) {
	c := testConfig()
	st := &restartState{}

	d := c.Decide(st, host.ExitStatus{Kind: host.ExitCrash}, time.Now())
	if !d.Restart {
		t.Fatal("crash must be restarted")
	}
	if d.Disable {
		t.Fatal("first crash must not disable")
	}
	if d.Delay != 100*time.Millisecond {
		t.Fatalf("delay = %v, want base 100ms", d.Delay)
	}
}

func TestDecide_ProtocolViolationIsNotRestarted(t *testing.T) {
	// Ключевое отличие от crash: сбой переживается перезапуском,
	// нарушение протокола повторится и может быть атакой.
	c := testConfig()
	st := &restartState{}

	d := c.Decide(st, host.ExitStatus{Kind: host.ExitProtocolViolation}, time.Now())
	if d.Restart {
		t.Fatal("protocol_violation must NOT be restarted automatically")
	}
	if !d.Disable {
		t.Fatal("protocol_violation must disable the plugin")
	}
	if st.failures() != 0 {
		t.Fatal("protocol_violation must not count against the crash limit — it is a separate category")
	}
}

func TestDecide_CleanStopDoesNotRestart(t *testing.T) {
	c := testConfig()
	st := &restartState{consecutiveFailures: 2}

	d := c.Decide(st, host.ExitStatus{Kind: host.ExitCleanStop}, time.Now())
	if d.Restart || d.Disable {
		t.Fatalf("clean_stop must neither restart nor disable: %+v", d)
	}
	if st.failures() != 0 {
		t.Fatal("clean_stop must reset the failure counter")
	}
}

func TestDecide_StartupFailureDisablesImmediately(t *testing.T) {
	// Нет файла / нет прав — само не починится, крутить цикл
	// бессмысленно.
	c := testConfig()
	st := &restartState{}

	d := c.Decide(st, host.ExitStatus{Kind: host.ExitStartupFailure}, time.Now())
	if d.Restart {
		t.Fatal("startup_failure must not be restarted")
	}
	if !d.Disable {
		t.Fatal("startup_failure must disable")
	}
}

func TestDecide_HandshakeFailureRestarts(t *testing.T) {
	c := testConfig()
	st := &restartState{}

	d := c.Decide(st, host.ExitStatus{Kind: host.ExitHandshakeFailure}, time.Now())
	if !d.Restart {
		t.Fatal("handshake_failure must be restarted (may be transient)")
	}
}

func TestDecide_AuthorizationFailureThreshold(t *testing.T) {
	c := testConfig() // MaxAttempts = 3
	st := &restartState{}
	now := time.Now()

	for i := 1; i < c.MaxAttempts; i++ {
		d := c.Decide(st, host.ExitStatus{Kind: host.ExitAuthorizationFailure}, now)
		if !d.Restart {
			t.Fatalf("attempt %d: must restart below threshold", i)
		}
	}
	d := c.Decide(st, host.ExitStatus{Kind: host.ExitAuthorizationFailure}, now)
	if !d.Disable {
		t.Fatal("repeated authorization failures must eventually disable")
	}
}

// ── Лимит и сброс счётчика ──────────────────────────────────────────

func TestDecide_RestartLimitExhausted(t *testing.T) {
	c := testConfig() // MaxAttempts = 3
	st := &restartState{}
	now := time.Now()

	for i := 1; i <= c.MaxAttempts; i++ {
		d := c.Decide(st, host.ExitStatus{Kind: host.ExitCrash}, now)
		if !d.Restart {
			t.Fatalf("crash %d: must still restart (limit is %d)", i, c.MaxAttempts)
		}
	}
	d := c.Decide(st, host.ExitStatus{Kind: host.ExitCrash}, now)
	if !d.Disable {
		t.Fatalf("crash %d: limit exhausted, must disable", c.MaxAttempts+1)
	}
}

func TestDecide_CounterResetsAfterStableRun(t *testing.T) {
	// Плагин, проработавший StableAfter, начинает новую серию: иначе
	// стабильно работавший неделю и упавший однажды оказался бы ближе
	// к DISABLED, чем заслуживает.
	c := testConfig()
	st := &restartState{}
	start := time.Now()

	// Две неудачи подряд.
	c.Decide(st, host.ExitStatus{Kind: host.ExitCrash}, start)
	c.Decide(st, host.ExitStatus{Kind: host.ExitCrash}, start)
	if st.failures() != 2 {
		t.Fatalf("failures = %d, want 2", st.failures())
	}

	// Экземпляр отработал дольше StableAfter.
	st.markStarted(start)
	later := start.Add(c.StableAfter + time.Second)

	d := c.Decide(st, host.ExitStatus{Kind: host.ExitCrash}, later)
	if !d.Restart {
		t.Fatal("must restart after a stable run")
	}
	if st.failures() != 1 {
		t.Fatalf("failures = %d, want 1 (counter reset then incremented)", st.failures())
	}
}

func TestDecide_CounterNotResetBeforeStableAfter(t *testing.T) {
	c := testConfig()
	st := &restartState{}
	start := time.Now()

	c.Decide(st, host.ExitStatus{Kind: host.ExitCrash}, start)
	st.markStarted(start)

	// Меньше StableAfter — счётчик не должен сбрасываться.
	tooSoon := start.Add(c.StableAfter / 2)
	c.Decide(st, host.ExitStatus{Kind: host.ExitCrash}, tooSoon)

	if st.failures() != 2 {
		t.Fatalf("failures = %d, want 2 — counter reset too early", st.failures())
	}
}
