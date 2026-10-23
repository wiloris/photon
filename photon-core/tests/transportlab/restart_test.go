package transportlab

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/wiloris/photon-core/services/diag"
	"github.com/wiloris/photon-core/services/dispatcher"
)

/*
 * S3-a плана (docs/adr130-s3-plan.md): сервер исчезает молча и возвращается
 * на тот же адрес — перезапуск плагина Bridge.
 *
 * Локально (по умолчанию): bridgesim убивается SIGKILL (без
 * CONNECTION_CLOSE) и через T поднимается новым процессом на том же порту.
 *
 * Живой режим (PHOTON_TRANSPORT_LAB_TARGET, см. lab_test.go): выключение и
 * включение — команды оператора из PHOTON_TRANSPORT_LAB_RESTART_DOWN и
 * PHOTON_TRANSPORT_LAB_RESTART_UP (sh -c); UP обязана вернуться только
 * после «QUIC listener bound» в логе. Без обеих команд живой прогон не
 * начинается. Запуск против стенда — только в согласованное окно
 * обслуживания (решение владельца 2026-10-23).
 *
 * Только при PHOTON_TRANSPORT_LAB_RESTART=1. PHOTON_TRANSPORT_LAB_RESTART_T
 * — пауза в выключенном состоянии, с (по умолчанию 10);
 * PHOTON_TRANSPORT_LAB_RESTART_PAUSE — покой между повторами, с (живой
 * режим — 30, локально — 0); PHOTON_TRANSPORT_LAB_REPEATS, _OUT — как у S1–S4.
 */

const (
	restartCallLimit = 90 * time.Second // предел ожидания первого успешного вызова
	restartLostWait  = 60 * time.Second // ожидание потери подписки от t0
)

func envSeconds(name string, def int) time.Duration {
	n := def
	if v := os.Getenv(name); v != "" {
		if _, err := fmt.Sscan(v, &n); err != nil {
			n = def
		}
	}
	return time.Duration(n) * time.Second
}

func runOperator(rp *report, label, cmdline string) error {
	s := time.Now()
	out, err := exec.Command("sh", "-c", cmdline).CombinedOutput()
	rp.add("  %s: %v, код %v, вывод %q", label, time.Since(s).Round(time.Millisecond), err, strings.TrimSpace(string(out)))
	return err
}

// serverSwitch — выключение и включение сервера с памятью о том, выключен ли
// он. restore — безопасная остановка: если сервер выключен, а включение не
// подтверждено (команда упала, прогон прерван t.Fatal), одна повторная
// попытка включения при любом выходе из теста — Bridge не остаётся
// выключенным по вине стенда. Прерывание процесса (Ctrl+C, -timeout) cleanup
// не выполняет — для него ручное восстановление в плане.
type serverSwitch struct {
	down, up func() error
	isDown   bool
}

func (s *serverSwitch) Down() error {
	s.isDown = true // и при ошибке: команда могла выключить и упасть после
	return s.down()
}

func (s *serverSwitch) Up() error {
	if err := s.up(); err != nil {
		return err
	}
	s.isDown = false
	return nil
}

// restore — повторное включение, если сервер остался выключенным.
func (s *serverSwitch) restore() (attempted bool, err error) {
	if !s.isDown {
		return false, nil
	}
	return true, s.Up()
}

func TestTransportLabRestart(t *testing.T) {
	if os.Getenv("PHOTON_TRANSPORT_LAB_RESTART") != "1" {
		t.Skip("S3-a — только при PHOTON_TRANSPORT_LAB_RESTART=1")
	}
	live := liveTarget() != ""
	down, up := os.Getenv("PHOTON_TRANSPORT_LAB_RESTART_DOWN"), os.Getenv("PHOTON_TRANSPORT_LAB_RESTART_UP")
	if live && (down == "" || up == "") {
		t.Fatal("живой S3-a без PHOTON_TRANSPORT_LAB_RESTART_DOWN/_UP — не начинается")
	}
	off := envSeconds("PHOTON_TRANSPORT_LAB_RESTART_T", 10)
	pauseDef := 0
	if live {
		pauseDef = 30
	}
	pause := envSeconds("PHOTON_TRANSPORT_LAB_RESTART_PAUSE", pauseDef)

	rp := &report{}
	// Отчёт пишется и при прерванном прогоне: метрики уже сделанных повторов
	// не теряются (cleanup выполняется после restore — LIFO).
	t.Cleanup(func() {
		out := strings.Join(rp.lines, "\n") + "\n"
		t.Log("\n" + out)
		if p := os.Getenv("PHOTON_TRANSPORT_LAB_OUT"); p != "" {
			os.WriteFile(p, []byte(out), 0o644)
		}
	})
	where := "локально: bridgesim, SIGKILL и новый процесс на том же порту"
	if live {
		where = "ЖИВОЙ РЕЖИМ: Bridge " + liveTarget() + ", выключение/включение командами оператора"
	}
	rp.add("# S3-a — сервер исчезает молча и возвращается — %s", time.Now().UTC().Format(time.RFC3339))
	rp.add("%s; выключен %v; покой между повторами %v", where, off, pause)
	rp.add("параметры ядра: ConnectTimeout %v, CallTimeout %v, EventBackoff %v…%v, KeepAlive 15 с", connectTimeout, callTimeout, backoffMin, backoffMax)

	var r *rig
	sw := &serverSwitch{}
	if live {
		sw.down = func() error { return runOperator(rp, "выключение", down) }
		sw.up = func() error { return runOperator(rp, "включение", up) }
	} else {
		sw.down = func() error { r.br.kill(); return nil }
		sw.up = func() error { r.br = startSimAt(t, r.rl.server.String()); return nil }
	}
	t.Cleanup(func() {
		if attempted, err := sw.restore(); attempted {
			rp.add("  БЕЗОПАСНАЯ ОСТАНОВКА: сервер остался выключенным — повторное включение: %v", err)
			if err != nil {
				t.Errorf("сервер НЕ включён — ручное восстановление по плану: %v", err)
			}
		}
	})

	var callDurs, detect, trSub, trAct, sessCall, sessConn []int64
	n := repeats()
	for i := 1; i <= n; i++ {
		if i > 1 {
			time.Sleep(pause)
		}
		r = newRig(t)
		t0 := r.now()
		if err := sw.Down(); err != nil {
			t.Fatalf("повтор %d: выключение: %v", i, err)
		}
		// Вызов сразу после выключения: классифицированный отказ не позже
		// CallTimeout + 1 с.
		s := time.Now()
		_, err := r.call()
		dur := time.Since(s)
		cls := dispatcher.Classify(err)
		callDurs = append(callDurs, dur.Milliseconds())
		if err == nil || cls == dispatcher.ClassInternal || dur > callTimeout+time.Second {
			t.Errorf("повтор %d: вызов при выключенном сервере err=%v класс %v за %v", i, err, cls, dur)
		}
		if rest := off - time.Since(s); rest > 0 {
			time.Sleep(rest)
		}
		t1 := r.now()
		if err := sw.Up(); err != nil {
			t.Fatalf("повтор %d: включение: %v — прогон прерван", i, err)
		}
		var okAt int64 = -1
		deadline := time.Now().Add(restartCallLimit)
		for time.Now().Before(deadline) {
			if _, err := r.call(); err == nil {
				okAt = r.now()
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		if okAt < 0 {
			t.Errorf("повтор %d: успешного вызова без перезапуска инстанса нет за %v", i, restartCallLimit)
		} else {
			sessCall = append(sessCall, okAt-t1)
		}
		var d int64 = -1
		if e, ok := r.waitEvent(t0, restartLostWait-time.Duration(r.now()-t0)*time.Millisecond, isLost(diag.ConnSubscribe)); ok {
			d = e.AtMonoMS - t0
			detect = append(detect, d)
			if time.Duration(d)*time.Millisecond > detectLimit {
				t.Errorf("повтор %d: T_detect %d мс > 45 с — НЕВЫПОЛНЕНИЕ (порог не меняется)", i, d)
			}
		} else {
			t.Errorf("повтор %d: потеря подписки не обнаружена за %v от выключения", i, restartLostWait)
		}
		var ts, ta, tc int64 = -1, -1, -1
		if e, ok := r.waitEvent(t1, 30*time.Second, connOK(diag.ConnSubscribe)); ok {
			ts = e.AtMonoMS - t1
			trSub = append(trSub, ts)
		} else {
			t.Errorf("повтор %d: подписка не восстановилась", i)
		}
		if e, ok := r.waitEvent(t1, time.Second, connOK(diag.ConnAction)); ok {
			ta = e.AtMonoMS - t1
			trAct = append(trAct, ta)
		}
		if e, ok := r.waitEvent(t1, 5*time.Second, sentEvent("CONNECTED")); ok {
			tc = e.AtMonoMS - t1
			sessConn = append(sessConn, tc)
		} else {
			t.Errorf("повтор %d: CONNECTED клиенту не отдан", i)
		}
		rp.add("  повтор %d: вызов при выключенном %v, класс %v; T_detect %d мс от выключения; от включения: подписка %d, action %d, CONNECTED %d, первый успешный вызов %d мс",
			i, dur.Round(time.Millisecond), cls, d, ts, ta, tc, okAt-t1)
		closeCheck(t, rp, r, fmt.Sprintf("повтор %d", i))
	}
	rp.add("  вызов при выключенном (граница CallTimeout+1 с = %d мс): %s", (callTimeout + time.Second).Milliseconds(), stats(callDurs))
	rp.add("  T_detect от выключения (предел 45 000 мс): %s", stats(detect))
	rp.add("  T_transport подписки от включения: %s", stats(trSub))
	rp.add("  T_transport action от включения: %s", stats(trAct))
	rp.add("  T_session — CONNECTED от включения: %s", stats(sessConn))
	rp.add("  T_session — первый успешный вызов от включения: %s", stats(sessCall))
}

// Безопасная остановка S3-a — без сети и без переменных окружения.
func TestServerSwitchRestore(t *testing.T) {
	var ups int
	upErr := errors.New("enable failed")
	mk := func(failUp bool) *serverSwitch {
		return &serverSwitch{
			down: func() error { return nil },
			up: func() error {
				ups++
				if failUp {
					return upErr
				}
				return nil
			},
		}
	}
	// Включение подтверждено — restore ничего не делает.
	ups = 0
	s := mk(false)
	s.Down()
	s.Up()
	if att, _ := s.restore(); att || ups != 1 {
		t.Fatalf("после успешного включения: attempted=%v ups=%d", att, ups)
	}
	// Прервано между выключением и включением — одна повторная попытка.
	ups = 0
	s = mk(false)
	s.Down()
	if att, err := s.restore(); !att || err != nil || ups != 1 {
		t.Fatalf("прерывание до включения: attempted=%v err=%v ups=%d", att, err, ups)
	}
	if att, _ := s.restore(); att {
		t.Fatal("после восстановления — повторная попытка")
	}
	// Включение упало — restore пробует ещё раз и сообщает ошибку.
	ups = 0
	s = mk(true)
	s.Down()
	if err := s.Up(); err == nil {
		t.Fatal("ожидалась ошибка включения")
	}
	if att, err := s.restore(); !att || !errors.Is(err, upErr) || ups != 2 {
		t.Fatalf("упавшее включение: attempted=%v err=%v ups=%d", att, err, ups)
	}
	// Команда выключения упала — сервер мог выключиться: считается выключенным.
	ups = 0
	s = &serverSwitch{down: func() error { return errors.New("ssh: exit 255") }, up: func() error { ups++; return nil }}
	s.Down()
	if att, _ := s.restore(); !att || ups != 1 {
		t.Fatalf("упавшее выключение: attempted=%v ups=%d", att, ups)
	}
}
