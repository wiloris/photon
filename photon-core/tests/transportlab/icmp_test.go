package transportlab

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-core/services/diag"
	"github.com/wiloris/photon/photon-core/services/dispatcher"
)

/*
 * S3-b, вариант 1 плана (docs/adr130-s3-plan.md): ICMP unreachable ТОЛЬКО
 * через контролируемое закрытие сокета ЛОКАЛЬНОГО ретранслятора — сервер,
 * его сеть, брандмауэр и конфигурация Bridge не затрагиваются (решение
 * владельца 2026-10-23). Только против локального bridgesim: живой Bridge
 * потребовал бы учётной записи на стенде.
 *
 * Пакеты ядра на закрытый порт получают от ядра ЭТОЙ машины ICMP port
 * unreachable; что ICMP действительно ушёл, видно по счётчику
 * Icmp OutDestUnreachs (/proc/net/snmp, без root). Измеряется, наблюдает
 * ли его QUIC-клиент ядра (неподключённый сокет, IP_RECVERR в quic-go v0.48
 * нет — гипотеза плана: не наблюдает).
 *
 * Только при PHOTON_TRANSPORT_LAB_ICMP=1; PHOTON_TRANSPORT_LAB_OUT — отчёт.
 */

func icmpOutDestUnreachs(t *testing.T) int64 {
	f, err := os.Open("/proc/net/snmp")
	if err != nil {
		t.Fatalf("/proc/net/snmp: %v", err)
	}
	defer f.Close()
	var hdr []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || fields[0] != "Icmp:" {
			continue
		}
		if hdr == nil {
			hdr = fields
			continue
		}
		for i, h := range hdr {
			if h == "OutDestUnreachs" {
				v, _ := strconv.ParseInt(fields[i], 10, 64)
				return v
			}
		}
	}
	t.Fatal("в /proc/net/snmp нет Icmp OutDestUnreachs")
	return 0
}

func TestTransportLabICMP(t *testing.T) {
	if os.Getenv("PHOTON_TRANSPORT_LAB_ICMP") != "1" {
		t.Skip("сценарий ICMP — только при PHOTON_TRANSPORT_LAB_ICMP=1")
	}
	if liveTarget() != "" {
		t.Skip("ICMP — только локально (bridgesim): живой Bridge не разрешён для этого сценария")
	}
	rp := &report{}
	rp.add("# S3-b вариант 1 — ICMP unreachable через закрытие сокета локального ретранслятора — %s", time.Now().UTC().Format(time.RFC3339))
	rp.add("ядро: dispatcher + adapters/quic (quic-go v0.48); ретранслятор локальный; сервер — bridgesim (quic-go v0.59); сервер и сеть не затрагиваются")
	rp.add("параметры ядра: ConnectTimeout %v, CallTimeout %v, EventBackoff %v…%v, KeepAlive 15 с", connectTimeout, callTimeout, backoffMin, backoffMax)
	var detect, trSub, sessCall, callDurs []int64
	earlyErrors := 0
	n := repeats()
	for i := 1; i <= n; i++ {
		r := newRig(t)
		icmp0 := icmpOutDestUnreachs(t)
		t0 := r.now()
		type res struct {
			err error
			dur time.Duration
		}
		done := make(chan res, 1)
		go func() {
			s := time.Now()
			_, err := r.call()
			done <- res{err, time.Since(s)}
		}()
		r.rl.closeFront() // сразу после начала вызова: путь к серверу «закрыт» ICMP
		cr := <-done
		cls := dispatcher.Classify(cr.err)
		callDurs = append(callDurs, cr.dur.Milliseconds())
		if cr.err == nil || cls == dispatcher.ClassInternal || cr.dur > callTimeout+time.Second {
			t.Errorf("повтор %d: вызов err=%v класс %v за %v", i, cr.err, cls, cr.dur)
		}
		if cr.err != nil && cr.dur < callTimeout-500*time.Millisecond {
			earlyErrors++ // клиент получил ошибку раньше своего таймаута — ICMP наблюдаем
		}
		lost, ok := r.waitEvent(t0, 60*time.Second, isLost(diag.ConnSubscribe))
		icmpSent := icmpOutDestUnreachs(t) - icmp0
		var d int64 = -1
		if ok {
			d = lost.AtMonoMS - t0
			detect = append(detect, d)
			if time.Duration(d)*time.Millisecond > detectLimit {
				t.Errorf("повтор %d: T_detect %d мс > 45 с", i, d)
			}
		} else {
			t.Errorf("повтор %d: потеря подписки не обнаружена за 60 с", i)
		}
		t1 := r.now()
		if err := r.rl.reopenFront(); err != nil {
			t.Fatalf("повтор %d: сокет ретранслятора не открылся: %v", i, err)
		}
		var okAt int64 = -1
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := r.call(); err == nil {
				okAt = r.now()
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		if okAt < 0 {
			t.Errorf("повтор %d: успешного вызова после открытия сокета нет за 30 с", i)
		} else {
			sessCall = append(sessCall, okAt-t1)
		}
		if e, ok := r.waitEvent(t1, 20*time.Second, connOK(diag.ConnSubscribe)); ok {
			trSub = append(trSub, e.AtMonoMS-t1)
		} else {
			t.Errorf("повтор %d: подписка не восстановилась", i)
		}
		rp.add("  повтор %d: вызов %v, класс %v; ICMP OutDestUnreachs +%d; T_detect %d мс; успешный вызов через %d мс после открытия",
			i, cr.dur.Round(time.Millisecond), cls, icmpSent, d, okAt-t1)
		if icmpSent == 0 {
			t.Errorf("повтор %d: ICMP не отправлялся — сценарий ничего не воспроизвёл", i)
		}
		closeCheck(t, rp, r, fmt.Sprintf("повтор %d", i))
	}
	rp.add("  вызов во время закрытия (граница CallTimeout+1 с = %d мс): %s", (callTimeout + time.Second).Milliseconds(), stats(callDurs))
	rp.add("  ошибок вызова раньше таймаута (ICMP наблюдаем клиентом): %d из %d", earlyErrors, n)
	rp.add("  T_detect (предел 45 000 мс): %s", stats(detect))
	rp.add("  T_transport подписки после открытия: %s", stats(trSub))
	rp.add("  T_session — первый успешный вызов после открытия: %s", stats(sessCall))
	out := strings.Join(rp.lines, "\n") + "\n"
	t.Log("\n" + out)
	if p := os.Getenv("PHOTON_TRANSPORT_LAB_OUT"); p != "" {
		os.WriteFile(p, []byte(out), 0o644)
	}
}
