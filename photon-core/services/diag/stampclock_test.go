package diag

import (
	"testing"
	"time"
)

func TestPickCoarseRule(t *testing.T) {
	for _, tc := range []struct {
		name         string
		avail        bool
		res          int64
		fine, coarse float64
		want         bool
	}{
		{"HPET: 1 мс и втрое дешевле", true, 1_000_000, 2000, 700, true},
		{"Redmi: 4 мс — не годится по точности", true, 4_000_000, 300, 100, false},
		{"Redmi: дороже точных", true, 1_000_000, 300, 900, false},
		{"TSC: точные дешевле", true, 1_000_000, 25, 150, false},
		{"выигрыш меньше 1.25×", true, 1_000_000, 1000, 900, false},
		{"нет грубых часов", false, 1_000_000, 2000, 700, false},
		{"разрешение неизвестно", true, 0, 2000, 700, false},
	} {
		if got := pickCoarse(tc.avail, tc.res, tc.fine, tc.coarse); got != tc.want {
			t.Errorf("%s: pickCoarse = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Грубые часы — та же шкала, что monotime: не впереди точных, отстают не
// больше тика, назад не идут.
func TestCoarseSameTimelineAsMonotime(t *testing.T) {
	if !coarseAvailable {
		t.Skip("грубых часов на этой платформе нет")
	}
	res := coarseResolution()
	if res <= 0 {
		t.Fatalf("разрешение грубых часов: %d", res)
	}
	prev := int64(0)
	for i := 0; i < 2000; i++ {
		c := coarseMono()
		f := monotime()
		if c > f {
			t.Fatalf("грубые впереди точных: %d > %d", c, f)
		}
		if lag := f - c; lag > res+int64(time.Millisecond) {
			t.Fatalf("грубые отстают на %v при разрешении %v", time.Duration(lag), time.Duration(res))
		}
		if c < prev {
			t.Fatalf("грубые пошли назад: %d < %d", c, prev)
		}
		prev = c
		if i%200 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	t.Logf("разрешение CLOCK_MONOTONIC_COARSE: %v", time.Duration(res))
}

// Record с каждыми из часов: метка не отрицательна, совпадает с точной до
// тика, запись без аллокаций; журнал (seq, курсор) — тот же.
func TestRecordWithEachStampClock(t *testing.T) {
	for _, c := range []struct {
		name string
		read func() int64
	}{{stampFine, monotime}, {stampCoarse, coarseMono}} {
		t.Run(c.name, func(t *testing.T) {
			d := newTestDiag(t, 64)
			d.stamp, d.stampName = c.read, c.name
			d.Record(CompStorage, KindTxFailed, ConnNone) // сразу после создания
			time.Sleep(15 * time.Millisecond)
			before := (monotime() - d.startMono) / 1e6
			d.Record(CompStorage, KindTxFailed, ConnNone)
			after := (monotime() - d.startMono) / 1e6
			p, _ := d.Read(0, 10)
			if len(p.Events) != 2 || p.Events[0].Seq != 1 || p.Events[1].Seq != 2 || p.NextSeq != 2 {
				t.Fatalf("журнал: %+v", p)
			}
			if p.Events[0].AtMonoMS < 0 {
				t.Fatalf("метка ушла в минус: %d", p.Events[0].AtMonoMS)
			}
			slack := int64(1)
			if c.name == stampCoarse {
				slack += coarseResolution() / 1e6
			}
			if m := p.Events[1].AtMonoMS; m < before-slack || m > after {
				t.Fatalf("метка %d мс вне [%d−%d, %d]", m, before, slack, after)
			}
			if a := testing.AllocsPerRun(1000, func() {
				d.Record(CompStorage, KindTxFailed, ConnAction, Str(KeyTx, "update"), Num(KeyDurUS, 1))
			}); a != 0 {
				t.Fatalf("Record: %.1f аллокаций", a)
			}
		})
	}
}

// Какие часы выбраны на этой машине — в лог (для сравнения замеров).
func TestChosenStampClock(t *testing.T) {
	d := newTestDiag(t, 64)
	if d.stampName != stampFine && d.stampName != stampCoarse {
		t.Fatalf("неизвестные часы: %q", d.stampName)
	}
	t.Logf("часы метки Record на этой машине: %s (разрешение грубых %v)", d.stampName, time.Duration(coarseResolution()))
}
