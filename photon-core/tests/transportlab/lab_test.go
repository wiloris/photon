package transportlab

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-core/services/diag"
	"github.com/wiloris/photon/photon-core/services/dispatcher"
)

/*
 * Сценарии S1–S4 ADR-130 §5.1 на настоящем QUIC.
 *
 *   ядро (services/dispatcher + adapters/quic, quic-go v0.48)
 *     → UDP-ретранслятор (relay_test.go, управляемый)
 *     → bridgesim (отдельный процесс и модуль: quic-go v0.59, как у Bridge,
 *       протокол Photon; sim_test.go — управление)
 *
 * Времена — ПО ЖУРНАЛУ ЯДРА: момент команды ретранслятору берётся как
 * uptime_ms снимка 819 той же диагностики, события — их at_mono_ms; база
 * одна (монотонные часы инстанса).
 *
 * Чем это НЕ является: прогоном против живого Bridge на стенде — для него
 * нужна сессия Mattermost на стенде (изменение сервера, отдельное
 * разрешение владельца), а перезапуск плагина в S3 ADR оговаривает
 * отдельным разрешением. Здесь проверяется транспорт и восстановление ЯДРА на
 * настоящем QUIC с параметрами Bridge (keep-alive 15 с, idle 30 с).
 *
 * ЖИВОЙ РЕЖИМ (PHOTON_TRANSPORT_LAB_TARGET=host:port): вместо bridgesim —
 * настоящий Bridge; PHOTON_TRANSPORT_LAB_SERVERNAME — имя для проверки TLS,
 * PHOTON_TRANSPORT_LAB_SESSION — Session ID временной учётной записи (только
 * из окружения, в отчёт и вывод не попадает). Сервером в живом режиме стенд
 * не управляет: S3 — ретранслятор теряет всё ДО вызова, S4 — теряет только
 * ответы (запрос доходит, ответ нет). Вызов — getMe (только чтение).
 *
 * Идёт минуты (настоящие таймауты QUIC) — только при PHOTON_TRANSPORT_LAB=1;
 * PHOTON_TRANSPORT_LAB_OUT — файл отчёта, PHOTON_TRANSPORT_LAB_REPEATS —
 * повторов (по умолчанию 5).
 */

const (
	connectTimeout = 5 * time.Second
	callTimeout    = 5 * time.Second
	backoffMin     = 500 * time.Millisecond
	backoffMax     = 5 * time.Second
	detectLimit    = 45 * time.Second
)

type rig struct {
	t      *testing.T
	br     *bridgeSim
	rl     *relay
	d      *dispatcher.Dispatcher
	fds0   int
	mu     sync.Mutex
	cursor uint64
	log    []diag.Event
	sinkMu sync.Mutex
	sink   []string
}

func fdCount() int {
	ents, _ := os.ReadDir("/proc/self/fd")
	return len(ents)
}

func liveTarget() string { return os.Getenv("PHOTON_TRANSPORT_LAB_TARGET") }

func newRig(t *testing.T) *rig {
	t.Helper()
	fds0 := fdCount() // до стенда: после закрытия всего стенда — не больше
	var br *bridgeSim
	target, token := liveTarget(), "tok"
	tlsOpt := dispatcher.TLSOptions{InsecureSkipVerify: true, ServerName: "localhost"}
	if target == "" {
		br = startSim(t)
		target = br.addr
	} else {
		token = os.Getenv("PHOTON_TRANSPORT_LAB_SESSION")
		if token == "" {
			t.Fatal("живой режим без PHOTON_TRANSPORT_LAB_SESSION")
		}
		tlsOpt = dispatcher.TLSOptions{ServerName: os.Getenv("PHOTON_TRANSPORT_LAB_SERVERNAME")}
	}
	rl, err := startRelay(target)
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{t: t, br: br, rl: rl, fds0: fds0}
	d, err := dispatcher.New(dispatcher.Config{
		Endpoint:        ports.Endpoint(rl.Addr()),
		SessionToken:    token,
		TLS:             tlsOpt,
		ConnectTimeout:  connectTimeout,
		CallTimeout:     callTimeout,
		EventBackoffMin: backoffMin,
		EventBackoffMax: backoffMax,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.d = d
	if err := d.StartEvents(func(ev []byte) {
		r.sinkMu.Lock()
		r.sink = append(r.sink, string(ev))
		r.sinkMu.Unlock()
	}, nil); err != nil {
		t.Fatal(err)
	}
	// Тёплое состояние: подписка подтверждена, action-соединение поднято.
	r.waitEvent(0, 10*time.Second, func(e diag.Event) bool { return e.Kind == diag.KindEventSent })
	if _, err := r.call(); err != nil {
		t.Fatalf("первый вызов: %v", err)
	}
	return r
}

// close — закрытие инстанса (длительность) и всего стенда; дескрипторы
// процесса сравниваются с числом ДО стенда: утечка ядра видна, сокеты
// ретранслятора и каналы bridgesim закрыты вместе со стендом.
func (r *rig) close() (closeDur time.Duration, fdsOK bool, fdsBefore, fdsAfter int) {
	t0 := time.Now()
	r.d.Close()
	closeDur = time.Since(t0)
	r.rl.Close()
	if r.br != nil {
		r.br.Close()
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		fdsAfter = fdCount()
		if fdsAfter <= r.fds0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return closeDur, fdsAfter <= r.fds0, r.fds0, fdsAfter
}

func (r *rig) now() int64 { return r.d.Diag().Snapshot().UptimeMS }

func (r *rig) call() ([]byte, error) { return r.d.SendAction(200, []byte(`{}`)) }

func (r *rig) pull() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		p, err := r.d.Diag().Read(r.cursor, diag.MaxReadLimit)
		if err != nil {
			r.t.Fatalf("журнал: %v", err)
		}
		if p.Dropped != 0 {
			r.t.Fatalf("журнал вытеснил %d записей", p.Dropped)
		}
		if len(p.Events) == 0 {
			return
		}
		r.log = append(r.log, p.Events...)
		r.cursor = p.NextSeq
	}
}

// waitEvent — первое событие с at_mono_ms ≥ from, удовлетворяющее pred.
func (r *rig) waitEvent(from int64, timeout time.Duration, pred func(diag.Event) bool) (diag.Event, bool) {
	deadline := time.Now().Add(timeout)
	for {
		r.pull()
		r.mu.Lock()
		for _, e := range r.log {
			if e.AtMonoMS >= from && pred(e) {
				r.mu.Unlock()
				return e, true
			}
		}
		r.mu.Unlock()
		if time.Now().After(deadline) {
			return diag.Event{}, false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func str(e diag.Event, k diag.Key) string { f, _ := e.Get(k); return f.Str }

func isLost(conn diag.Conn) func(diag.Event) bool {
	return func(e diag.Event) bool {
		return e.Kind == diag.KindSessionFailed && e.Conn == conn && str(e, diag.KeyClass) == "lost"
	}
}

func connOK(conn diag.Conn) func(diag.Event) bool {
	return func(e diag.Event) bool {
		return e.Kind == diag.KindConnResult && e.Conn == conn && str(e, diag.KeyClass) == "ok"
	}
}

func sentEvent(name string) func(diag.Event) bool {
	return func(e diag.Event) bool { return e.Kind == diag.KindEventSent && str(e, diag.KeyEvent) == name }
}

// stats — минимум, медиана, максимум (мс).
func stats(v []int64) string {
	if len(v) == 0 {
		return "—"
	}
	s := append([]int64(nil), v...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return fmt.Sprintf("%d / %d / %d", s[0], s[len(s)/2], s[len(s)-1])
}

type report struct {
	mu    sync.Mutex
	lines []string
}

func (rp *report) add(format string, a ...any) {
	rp.mu.Lock()
	rp.lines = append(rp.lines, fmt.Sprintf(format, a...))
	rp.mu.Unlock()
}

func repeats() int {
	n := 5
	fmt.Sscan(os.Getenv("PHOTON_TRANSPORT_LAB_REPEATS"), &n)
	return n
}

func TestTransportLab(t *testing.T) {
	if os.Getenv("PHOTON_TRANSPORT_LAB") != "1" {
		t.Skip("стенд идёт минуты — только при PHOTON_TRANSPORT_LAB=1")
	}
	rp := &report{}
	rp.add("# Сценарии S1–S4 ADR-130 §5.1 — %s", time.Now().UTC().Format(time.RFC3339))
	if liveTarget() == "" {
		rp.add("ядро: dispatcher + adapters/quic (quic-go v0.48); ретранслятор UDP; сервер — bridgesim (отдельный процесс, quic-go v0.59 как у Bridge, KeepAlive 15 с, idle 30 с)")
	} else {
		rp.add("ЖИВОЙ РЕЖИМ: ядро (dispatcher + adapters/quic) → ретранслятор UDP (локально) → Bridge %s (TLS %s); вызов getMe", liveTarget(), os.Getenv("PHOTON_TRANSPORT_LAB_SERVERNAME"))
	}
	rp.add("параметры ядра: ConnectTimeout %v, CallTimeout %v, EventBackoff %v…%v, KeepAlive по умолчанию (15 с)", connectTimeout, callTimeout, backoffMin, backoffMax)
	n := repeats()

	t.Run("S1_loss_without_close", func(t *testing.T) { s1(t, rp, n) })
	t.Run("S2_nat_rebinding", func(t *testing.T) { s2(t, rp, n) })
	t.Run("S3_server_gone_during_call", func(t *testing.T) { s3(t, rp, n) })
	t.Run("S4_response_lost", func(t *testing.T) { s4(t, rp, n) })

	out := strings.Join(rp.lines, "\n") + "\n"
	t.Log("\n" + out)
	if p := os.Getenv("PHOTON_TRANSPORT_LAB_OUT"); p != "" {
		os.WriteFile(p, []byte(out), 0o644)
	}
}

func closeCheck(t *testing.T, rp *report, r *rig, label string) {
	dur, ok, before, after := r.close()
	rp.add("  %s: close %v, дескрипторы %d → %d", label, dur.Round(time.Millisecond), before, after)
	if dur > 15*time.Second {
		t.Errorf("%s: close %v > 15 с", label, dur)
	}
	if !ok {
		t.Errorf("%s: дескрипторы не вернулись: %d → %d", label, before, after)
	}
}

// S1 — потеря пакетов без закрытия.
func s1(t *testing.T, rp *report, n int) {
	rp.add("\n## S1 — потеря пакетов без закрытия (обе стороны)")
	var detect, trSub, trAct, sessCall, sessConn []int64
	for i := 1; i <= n; i++ {
		r := newRig(t)
		t0 := r.now()
		r.rl.dropUp.Store(true)
		r.rl.dropDown.Store(true)
		lost, ok := r.waitEvent(t0, 60*time.Second, isLost(diag.ConnSubscribe))
		if !ok {
			t.Errorf("повтор %d: потеря подписки не обнаружена за 60 с", i)
			r.close()
			continue
		}
		d := lost.AtMonoMS - t0
		detect = append(detect, d)
		if time.Duration(d)*time.Millisecond > detectLimit {
			t.Errorf("повтор %d: T_detect %d мс > 45 с", i, d)
		}
		if _, ok := r.waitEvent(t0, 5*time.Second, sentEvent(diag.EventNetworkChanged)); !ok {
			t.Errorf("повтор %d: NETWORK_CHANGED клиенту не отдан", i)
		}
		time.Sleep(time.Second)
		flows0 := r.rl.Flows()
		t1 := r.now()
		r.rl.dropUp.Store(false)
		r.rl.dropDown.Store(false)

		// Вызовы раз в 300 мс до первого успешного.
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
			t.Errorf("повтор %d: успешного вызова после восстановления нет за 30 с", i)
		} else {
			sessCall = append(sessCall, okAt-t1)
		}
		if e, ok := r.waitEvent(t1, 20*time.Second, connOK(diag.ConnSubscribe)); ok {
			trSub = append(trSub, e.AtMonoMS-t1)
		} else {
			t.Errorf("повтор %d: подписка не восстановилась", i)
		}
		if e, ok := r.waitEvent(t1, 20*time.Second, connOK(diag.ConnAction)); ok {
			trAct = append(trAct, e.AtMonoMS-t1)
		} else {
			t.Errorf("повтор %d: action-соединение не восстановилось", i)
		}
		if e, ok := r.waitEvent(t1, 20*time.Second, sentEvent(diag.EventConnected)); ok {
			sessConn = append(sessConn, e.AtMonoMS-t1)
		} else {
			t.Errorf("повтор %d: CONNECTED клиенту не отдан", i)
		}
		newFlows := r.rl.Flows() - flows0
		rp.add("  повтор %d: T_detect %d мс; новых потоков на ретрансляторе %d", i, d, newFlows)
		if newFlows > 2 {
			t.Errorf("повтор %d: новых потоков %d > 2", i, newFlows)
		}
		closeCheck(t, rp, r, fmt.Sprintf("повтор %d", i))
	}
	rp.add("  T_detect (предел 45 000 мс), мин / медиана / макс: %s", stats(detect))
	rp.add("  T_transport подписки (граница BackoffMax+ConnectTimeout = %d мс): %s", (backoffMax + connectTimeout).Milliseconds(), stats(trSub))
	rp.add("  T_transport action (ленивое — по первому вызову): %s", stats(trAct))
	rp.add("  T_session: первый успешный вызов (граница +CallTimeout = %d мс): %s", (backoffMax + connectTimeout + callTimeout).Milliseconds(), stats(sessCall))
	rp.add("  T_session: CONNECTED клиенту: %s", stats(sessConn))
	for _, v := range trSub {
		if time.Duration(v)*time.Millisecond > backoffMax+connectTimeout {
			t.Errorf("T_transport подписки %d мс за границей", v)
		}
	}
	for _, v := range sessCall {
		if time.Duration(v)*time.Millisecond > backoffMax+connectTimeout+callTimeout {
			t.Errorf("T_session %d мс за границей", v)
		}
	}
}

// S2 — смена исходного порта (NAT rebinding): соединения продолжают работу
// (миграция QUIC) ИЛИ восстанавливаются в границах S1 — записывается исход.
func s2(t *testing.T, rp *report, n int) {
	rp.add("\n## S2 — смена исходного порта на ретрансляторе (NAT rebinding)")
	for i := 1; i <= n; i++ {
		r := newRig(t)
		connects0 := r.d.Diag().Snapshot().Connections
		t0 := r.now()
		if err := r.rl.rebind(); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		_, err := r.call()
		callDur := time.Since(start)
		var recovered int64 = -1
		if err != nil {
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) {
				if _, e := r.call(); e == nil {
					recovered = r.now() - t0
					break
				}
				time.Sleep(300 * time.Millisecond)
			}
		}
		time.Sleep(2 * time.Second) // окно для обнаружения потери подписки
		_, subLost := r.waitEvent(t0, 0, isLost(diag.ConnSubscribe))
		c1 := r.d.Diag().Snapshot().Connections
		outcome := "миграция: вызов прошёл по прежнему соединению"
		if err != nil {
			outcome = fmt.Sprintf("вызов отказал (%v), восстановление за %d мс", dispatcher.Classify(err), recovered)
			if recovered < 0 {
				t.Errorf("повтор %d: не восстановилось за 20 с", i)
			}
		}
		rp.add("  повтор %d: %s (вызов %v); action подключений %d → %d; подписка потеряна: %v",
			i, outcome, callDur.Round(time.Millisecond), connects0["action"].Connects, c1["action"].Connects, subLost)
		closeCheck(t, rp, r, fmt.Sprintf("повтор %d", i))
	}
}

// S3 — сервер пропадает во время вызова: вызов завершается
// классифицированной ошибкой не позже CallTimeout (+1 с), после возврата
// сервера — успешный вызов без перезапуска инстанса.
func s3(t *testing.T, rp *report, n int) {
	rp.add("\n## S3 — сервер пропал во время вызова (ретранслятор теряет всё; в живом режиме — с момента вызова)")
	rp.add("  (перезапуск плагина Bridge на стенде — не выполнялся: отдельное разрешение владельца)")
	var durs []int64
	for i := 1; i <= n; i++ {
		r := newRig(t)
		if r.br != nil {
			r.br.mode("hold")
			r.br.onAction.Store(func(int) {
				r.rl.dropUp.Store(true)
				r.rl.dropDown.Store(true)
			})
		} else {
			// Живой Bridge стендом не управляется: путь пропадает до вызова.
			r.rl.dropUp.Store(true)
			r.rl.dropDown.Store(true)
		}
		start := time.Now()
		_, err := r.call()
		dur := time.Since(start)
		durs = append(durs, dur.Milliseconds())
		cls := dispatcher.Classify(err)
		if err == nil || cls == dispatcher.ClassInternal || dur > callTimeout+time.Second {
			t.Errorf("повтор %d: err=%v класс %v за %v", i, err, cls, dur)
		}
		if r.br != nil {
			r.br.onAction.Store(func(int) {})
			r.br.mode("normal")
		}
		r.rl.dropUp.Store(false)
		r.rl.dropDown.Store(false)
		var okAfter time.Duration = -1
		t1 := time.Now()
		for time.Since(t1) < 20*time.Second {
			if _, e := r.call(); e == nil {
				okAfter = time.Since(t1)
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		if okAfter < 0 {
			t.Errorf("повтор %d: после возврата сервера успешного вызова нет", i)
		}
		rp.add("  повтор %d: вызов вернулся за %v с классом %v; успешный вызов после возврата через %v",
			i, dur.Round(time.Millisecond), cls, okAfter.Round(time.Millisecond))
		closeCheck(t, rp, r, fmt.Sprintf("повтор %d", i))
	}
	rp.add("  длительность отказавшего вызова (граница CallTimeout+1 с = %d мс): %s", (callTimeout + time.Second).Milliseconds(), stats(durs))
}

// S4 — обрыв посреди вызова: запрос дошёл, ответ потерян. Возврат не позже
// CallTimeout (+1 с), класс — сетевой/таймаут, не INTERNAL и не успех;
// следующий вызов после восстановления — успешный.
func s4(t *testing.T, rp *report, n int) {
	rp.add("\n## S4 — запрос дошёл, ответ потерян")
	var durs []int64
	for i := 1; i <= n; i++ {
		r := newRig(t)
		// Сервер отвечает через 300 мс; к этому моменту ретранслятор уже
		// теряет всё, что идёт к клиенту: запрос дошёл, ответ — нет.
		if r.br != nil {
			r.br.mode("delay 300")
			r.br.onAction.Store(func(int) { r.rl.dropDown.Store(true) })
		} else {
			// Живой Bridge: запрос проходит, все ответы теряются.
			r.rl.dropDown.Store(true)
		}
		start := time.Now()
		_, err := r.call()
		dur := time.Since(start)
		durs = append(durs, dur.Milliseconds())
		cls := dispatcher.Classify(err)
		if err == nil || cls == dispatcher.ClassInternal || dur > callTimeout+time.Second {
			t.Errorf("повтор %d: err=%v класс %v за %v", i, err, cls, dur)
		}
		if r.br != nil {
			r.br.onAction.Store(func(int) {})
			r.br.mode("normal")
		}
		r.rl.dropDown.Store(false)
		var okAfter time.Duration = -1
		t1 := time.Now()
		for time.Since(t1) < 20*time.Second {
			if _, e := r.call(); e == nil {
				okAfter = time.Since(t1)
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		if okAfter < 0 {
			t.Errorf("повтор %d: после восстановления успешного вызова нет", i)
		}
		rp.add("  повтор %d: вызов вернулся за %v с классом %v; следующий успешный через %v",
			i, dur.Round(time.Millisecond), cls, okAfter.Round(time.Millisecond))
		closeCheck(t, rp, r, fmt.Sprintf("повтор %d", i))
	}
	rp.add("  длительность вызова без ответа (граница CallTimeout+1 с = %d мс): %s", (callTimeout + time.Second).Milliseconds(), stats(durs))
}
