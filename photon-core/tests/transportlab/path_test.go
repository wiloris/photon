package transportlab

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/wiloris/photon-core/ports"
	"github.com/wiloris/photon-core/services/diag"
	"github.com/wiloris/photon-core/services/dispatcher"
)

/*
 * Путь подключения (ADR-130 §2.3, дополнение) на всём ядре: dispatcher +
 * adapters/quic против bridgesim (quic-go v0.59), слушающего 127.0.0.1 или
 * [::1]. Без ретранслятора: он слушает только 127.0.0.1 и скрыл бы
 * семейство. Проверяется, что событие dial у action и subscribe несёт
 * transport=quic, remote — адрес слушателя, family — его семейство; у
 * неудачного подключения адреса и семейства нет.
 *
 * Чего этот сценарий НЕ доказывает: выбор семейства для ИМЕНИ с A- и
 * AAAA-записями. Здесь адреса — литералы; имени с обеими записями в этом
 * окружении нет (localhost → только 127.0.0.1), системная конфигурация не
 * меняется.
 *
 * Только при PHOTON_TRANSPORT_LAB_PATH=1 (собирает bridgesim своим
 * toolchain); PHOTON_TRANSPORT_LAB_OUT — отчёт.
 */

func pathDispatcher(t *testing.T, target string) *dispatcher.Dispatcher {
	t.Helper()
	d, err := dispatcher.New(dispatcher.Config{
		Endpoint:        ports.Endpoint(target),
		SessionToken:    "tok",
		TLS:             dispatcher.TLSOptions{InsecureSkipVerify: true, ServerName: "localhost"},
		ConnectTimeout:  2 * time.Second,
		CallTimeout:     2 * time.Second,
		EventBackoffMin: backoffMin,
		EventBackoffMax: backoffMax,
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func dialsOf(t *testing.T, d *dispatcher.Dispatcher) []diag.Event {
	t.Helper()
	p, err := d.Diag().Read(0, diag.MaxReadLimit)
	if err != nil {
		t.Fatal(err)
	}
	var out []diag.Event
	for _, e := range p.Events {
		if e.Kind == diag.KindDial {
			out = append(out, e)
		}
	}
	return out
}

func TestTransportLabPath(t *testing.T) {
	if os.Getenv("PHOTON_TRANSPORT_LAB_PATH") != "1" {
		t.Skip("путь подключения против bridgesim — только при PHOTON_TRANSPORT_LAB_PATH=1")
	}
	rp := &report{}
	rp.add("# Путь подключения: dispatcher + adapters/quic → bridgesim — %s", time.Now().UTC().Format(time.RFC3339))
	for _, c := range []struct{ listen, family string }{
		{"127.0.0.1:0", "IPv4"},
		{"[::1]:0", "IPv6"},
	} {
		sim := startSimAt(t, c.listen)
		d := pathDispatcher(t, sim.addr)
		if err := d.StartEvents(func([]byte) {}, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := d.SendAction(200, []byte(`{}`)); err != nil {
			t.Fatalf("%s: вызов: %v", c.listen, err)
		}
		// Подписка подключается асинхронно — ждём её dial.
		var ds []diag.Event
		deadline := time.Now().Add(5 * time.Second)
		for {
			ds = dialsOf(t, d)
			seen := map[diag.Conn]bool{}
			for _, e := range ds {
				seen[e.Conn] = true
			}
			if seen[diag.ConnAction] && seen[diag.ConnSubscribe] || time.Now().After(deadline) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		got := map[diag.Conn]bool{}
		for _, e := range ds {
			w := e.Wire()
			rp.add("  %s: conn=%s %v", c.listen, e.Conn, w["fields"])
			cls, _ := e.Get(diag.KeyClass)
			tr, _ := e.Get(diag.KeyTransport)
			fam, _ := e.Get(diag.KeyFamily)
			rem, _ := e.Get(diag.KeyRemote)
			if cls.Str != "ok" || tr.Str != "quic" || fam.Str != c.family || rem.Str != sim.addr {
				t.Errorf("%s: dial %s: %v (слушатель %s)", c.listen, e.Conn, w["fields"], sim.addr)
			}
			got[e.Conn] = true
		}
		if !got[diag.ConnAction] || !got[diag.ConnSubscribe] {
			t.Errorf("%s: dial action/subscribe: %v", c.listen, got)
		}
		d.Close()
		sim.Close()
	}

	// Отказ: на [::1] никто не слушает (слушатель закрыт) — dial есть, адреса
	// и семейства нет.
	sim := startSimAt(t, "[::1]:0")
	dead := sim.addr
	sim.Close()
	d := pathDispatcher(t, dead)
	if _, err := d.SendAction(200, []byte(`{}`)); err == nil {
		t.Fatal("вызов к закрытому адресу удался")
	}
	// Вызов может сдаться по CallTimeout раньше, чем подключение — по
	// ConnectTimeout; dial пишется, когда Connect вернулся. Ждём его.
	var ds []diag.Event
	for deadline := time.Now().Add(5 * time.Second); len(ds) == 0 && time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
		ds = dialsOf(t, d)
	}
	if len(ds) == 0 {
		t.Fatal("отказ: события dial нет")
	}
	for _, e := range ds {
		w := e.Wire()
		rp.add("  отказ %s: conn=%s %v", dead, e.Conn, w["fields"])
		cls, _ := e.Get(diag.KeyClass)
		tr, _ := e.Get(diag.KeyTransport)
		_, hasRem := e.Get(diag.KeyRemote)
		_, hasFam := e.Get(diag.KeyFamily)
		if cls.Str == "ok" || tr.Str != "quic" || hasRem || hasFam {
			t.Errorf("отказ: dial %v", w["fields"])
		}
	}
	d.Close()
	out := strings.Join(rp.lines, "\n") + "\n"
	t.Log("\n" + out)
	if p := os.Getenv("PHOTON_TRANSPORT_LAB_OUT"); p != "" {
		os.WriteFile(p, []byte(out), 0o644)
	}
}
