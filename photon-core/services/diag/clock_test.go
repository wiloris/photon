package diag

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// monotime через linkname идёт вперёд и согласован с монотонной частью
// time.Now. Обязательная проверка после смены версии Go (clock.go).
func TestMonotimeMatchesTimeNow(t *testing.T) {
	m0, t0 := monotime(), time.Now()
	time.Sleep(50 * time.Millisecond)
	dm, dt := monotime()-m0, time.Since(t0)
	if dm <= 0 {
		t.Fatalf("monotime не идёт вперёд: %d", dm)
	}
	if diff := time.Duration(dm) - dt; diff < -5*time.Millisecond || diff > 5*time.Millisecond {
		t.Fatalf("monotime %v против time.Since %v", time.Duration(dm), dt)
	}
}

// Перевод календарных часов вперёд и назад (ADR-130 §2.3, поправка): порядок
// журнала и монотонные метки не нарушаются, instance_start_wall_ms инстанса
// не меняется — его снимают один раз. Перевод воспроизводится подменой
// календарных часов (wallNow): настоящий перевод системных часов требует root.
//
// И фиксируется ожидаемое расхождение: оценка календарного времени события
// instance_start_wall_ms + at_mono_ms после перевода отличается от
// «настоящего» календарного времени ровно на величину перевода.
func TestClockStepKeepsOrderAndMonotonic(t *testing.T) {
	base := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	step := time.Duration(0)
	wallNow = func() time.Time { return base.Add(step) }
	defer func() { wallNow = time.Now }()

	d := newTestDiag(t, 128)
	if d.InstanceStartWallMS() != base.UnixMilli() {
		t.Fatalf("instance_start_wall_ms = %d", d.InstanceStartWallMS())
	}
	for _, s := range []time.Duration{0, time.Hour, -2 * time.Hour, 24 * time.Hour} {
		step = s // календарные часы переведены
		for i := 0; i < 10; i++ {
			d.Record(CompStorage, KindTxFailed, ConnNone, Num(KeyDurUS, int64(i)))
		}
		time.Sleep(2 * time.Millisecond)
	}
	p, _ := d.Read(0, MaxReadLimit)
	if len(p.Events) != 40 {
		t.Fatalf("записей %d", len(p.Events))
	}
	for i := 1; i < len(p.Events); i++ {
		if p.Events[i].Seq != p.Events[i-1].Seq+1 || p.Events[i].AtMonoMS < p.Events[i-1].AtMonoMS {
			t.Fatalf("после перевода часов нарушен порядок: [%d] seq %d mono %d, [%d] seq %d mono %d",
				i-1, p.Events[i-1].Seq, p.Events[i-1].AtMonoMS, i, p.Events[i].Seq, p.Events[i].AtMonoMS)
		}
	}
	if p.InstanceStartWallMS != base.UnixMilli() || d.Snapshot().InstanceStartWallMS != base.UnixMilli() {
		t.Fatal("instance_start_wall_ms сдвинулся вместе с переводом часов")
	}
	last := p.Events[len(p.Events)-1]
	estimate := p.InstanceStartWallMS + last.AtMonoMS
	real := wallNow().UnixMilli() // «настоящие» часы после последнего перевода (+24 ч)
	if gap := real - estimate; gap < (24*time.Hour).Milliseconds()-1000 {
		t.Fatalf("оценка %d, настоящее %d: расхождение %d мс, ожидалось ≈ величины перевода", estimate, real, gap)
	}

	// Новый инстанс снимает календарное время заново.
	if d2 := newTestDiag(t, 64); d2.InstanceStartWallMS() != base.Add(step).UnixMilli() {
		t.Fatal("новый инстанс не снял календарное время")
	}
}

// Календарные часы на пути записи не читаются: в рабочем коде пакета
// time.Now встречается только в clock.go (wallNow), а wallNow — только в
// конструкторе.
func TestNoWallClockOnRecordPath(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	now := regexp.MustCompile(`time\.Now\(`)
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, _ := os.ReadFile(f)
		code := stripComments(string(src))
		if f != "clock.go" && now.MatchString(code) {
			t.Errorf("%s читает time.Now — календарные часы на пути записи", f)
		}
		if n := strings.Count(code, "wallNow()"); (f == "diag.go" && n != 1) || (f != "diag.go" && n != 0) {
			t.Errorf("%s: wallNow() вызван %d раз (ожидался один раз в New)", f, n)
		}
	}
}

func stripComments(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}
