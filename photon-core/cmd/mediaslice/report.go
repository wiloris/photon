package main

import (
	"fmt"
	"io"
	"sync"
)

// Учёт исходов прогона.
//
// Правило то же, что в livecheck: прогон обязан быть ПРОВЕРКОЙ, а не
// распечаткой. Каждое ожидание либо подтверждено, либо нарушено, либо честно
// объявлено невыполненным — третьего состояния «напечатали и пошли дальше»
// нет. Печать «❌» без ненулевого кода возврата — тот же дефект, что зелёный
// тест, проходящий всегда.
//
// statusSkip существует отдельно от statusFail намеренно. «Не выполнено»
// означает, что стимул недостижим в этой среде (нет второго субъекта, нет
// способа перевести объект в EXPIRED без вмешательства на сервере), и
// выдавать это за успех нельзя — но и за отказ тоже: отказ означал бы, что
// система ведёт себя неверно.
type status string

const (
	statusPass status = "PASS"
	statusFail status = "FAIL"
	statusSkip status = "NOT EXECUTED"
)

type check struct {
	id   string
	what string
	st   status
	note string
}

type report struct {
	mu           sync.Mutex
	checks       []check
	measurements []string
}

func (r *report) add(c check) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks = append(r.checks, c)
}

func (r *report) pass(id, format string, a ...any) {
	r.add(check{id: id, what: fmt.Sprintf(format, a...), st: statusPass})
	fmt.Printf("  ✅ %-4s %s\n", id, fmt.Sprintf(format, a...))
}

func (r *report) fail(id, format string, a ...any) {
	r.add(check{id: id, what: fmt.Sprintf(format, a...), st: statusFail})
	fmt.Printf("  ❌ %-4s %s\n", id, fmt.Sprintf(format, a...))
}

// skip — стимул недостижим. Причина обязательна: «не выполнено» без причины
// неотличимо от забытого.
func (r *report) skip(id, what, why string) {
	r.add(check{id: id, what: what, st: statusSkip, note: why})
	fmt.Printf("  ⏭  %-4s %s — НЕ ВЫПОЛНЕНО: %s\n", id, what, why)
}

// measure — число, а не утверждение. Отдельно от проверок, потому что
// измерение не может «упасть»: порога у него нет.
func (r *report) measure(format string, a ...any) {
	r.mu.Lock()
	r.measurements = append(r.measurements, fmt.Sprintf(format, a...))
	r.mu.Unlock()
	fmt.Printf("  📐 %s\n", fmt.Sprintf(format, a...))
}

func (r *report) counts() (pass, fail, skip int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.checks {
		switch c.st {
		case statusPass:
			pass++
		case statusFail:
			fail++
		case statusSkip:
			skip++
		}
	}
	return
}

func (r *report) failures() int {
	_, f, _ := r.counts()
	return f
}

// result — итог прогона как ошибка. Ненулевой код возврата берётся отсюда и
// только отсюда.
func (r *report) result() error {
	if f := r.failures(); f > 0 {
		return fmt.Errorf("mediaslice: провалено проверок: %d", f)
	}
	return nil
}

func (r *report) print(w io.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	fmt.Fprintln(w, "\n── итог ───────────────────────────────────────")
	for _, c := range r.checks {
		mark := "✅"
		switch c.st {
		case statusFail:
			mark = "❌"
		case statusSkip:
			mark = "⏭ "
		}
		line := fmt.Sprintf("  %s %-4s %-11s %s", mark, c.id, c.st, c.what)
		if c.note != "" {
			line += " — " + c.note
		}
		fmt.Fprintln(w, line)
	}
	if len(r.measurements) > 0 {
		fmt.Fprintln(w, "\n  измерения (порога нет, упасть не могут):")
		for _, m := range r.measurements {
			fmt.Fprintf(w, "    📐 %s\n", m)
		}
	}
	var p, f, s int
	for _, c := range r.checks {
		switch c.st {
		case statusPass:
			p++
		case statusFail:
			f++
		case statusSkip:
			s++
		}
	}
	fmt.Fprintf(w, "\n  всего %d: PASS %d, FAIL %d, NOT EXECUTED %d\n", len(r.checks), p, f, s)
}
