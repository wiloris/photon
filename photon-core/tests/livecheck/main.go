// livecheck — РУЧНОЙ инструмент, не автотест.
//
// Прогоняет services/session против живого photon-quic-bridge. Требует
// развёрнутого сервера и действующего Session ID, поэтому в `go test ./...`
// не участвует и участвовать не должен: тест, требующий внешнего сервера,
// либо делает прогон недетерминированным, либо тихо пропускается — и тогда
// он хуже отсутствующего (CLAUDE.md, «Метод работы»).
//
// Package main, а не _test.go, именно поэтому: `go test ./...` его не
// подхватывает, `go build ./...` — собирает, так что расхождение с API
// пакета ломает сборку, а не остаётся незамеченным.
//
// Зачем нужен: тест-клиент Bridge (test-client/test_photon.go) — другая
// реализация того же протокола, с прямым quic.DialAddr. Его прохождение не
// доказывает, что работает ЭТОТ пакет. Здесь всё идёт через
// ports.Transport/adapters/quic, как положено коду ядра (ADR-020).
//
// Что нашёл при первом применении (photon-core/docs/bridge-session.md §9):
// смерть подписки через 30 с простоя и отсутствие сигнала об обрыве — ни то
// ни другое net.Pipe не воспроизводит.
//
// Запуск:
//
//	go run ./tests/livecheck -mode subscribe -session <Session ID> [-trigger]
//	go run ./tests/livecheck -mode action    -session <Session ID>
//	go run ./tests/livecheck -mode lost      -session <Session ID>
//	go run ./tests/livecheck -mode dispatch  -session <Session ID>
//
// Режим dispatch добавлен в Phase 13.5 и проверяет ДРУГОЙ слой: не
// services/session напрямую, а services/dispatcher поверх неё — то есть путь,
// которым идёт Photon_SendAction. Без него прогон подтверждал бы слой, который
// и так был проверен в Phase 12.5, а добавленный — нет.
//
// Session ID — это Session.Id, а НЕ токен из /users/login: имя поля
// session_token на проводе историческое (bridge-session.md §2.1).
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	quicadapter "github.com/wiloris/photon-core/adapters/quic"
	"github.com/wiloris/photon-core/ports"
	photonpb "github.com/wiloris/photon-core/protobuf"
	"github.com/wiloris/photon-core/services/dispatcher"
	"github.com/wiloris/photon-core/services/session"
)

var (
	addr       = flag.String("addr", "vpn.wiloris.ru:4443", "адрес Bridge")
	sessionID  = flag.String("session", "", "Session ID Mattermost (обязателен)")
	mode       = flag.String("mode", "subscribe", "subscribe | action | lost | dispatch")
	keepalive  = flag.Duration("keepalive", 0, "KeepAlivePeriod клиента; 0 — как в проде")
	wait       = flag.Duration("wait", 90*time.Second, "окно ожидания push")
	lostWindow = flag.Duration("lost-window", 150*time.Second,
		"сколько молчать в -mode lost. Должно заметно превышать idle-timeout (30 с)")
	insecure = flag.Bool("insecure", false, "не проверять сертификат сервера")
	trigger  = flag.Bool("trigger", false,
		"в -mode subscribe: породить push самому (сообщение в самопереписке, затем удалить)")
)

func main() {
	flag.Parse()
	if *sessionID == "" {
		fmt.Fprintln(os.Stderr, "нужен -session <Session ID>")
		os.Exit(2)
	}

	tr := quicadapter.New(quicadapter.Config{
		TLSConfig: &tls.Config{
			MinVersion:         tls.VersionTLS13,
			InsecureSkipVerify: *insecure, //nolint:gosec
		},
		HandshakeTimeout: 10 * time.Second,
		KeepAlivePeriod:  *keepalive,
	})
	defer tr.Close()

	switch *mode {
	case "subscribe":
		runSubscribe(tr)
	case "action":
		runAction(tr)
	case "lost":
		runLost(tr)
	case "dispatch":
		runDispatch(tr)
	default:
		fmt.Fprintf(os.Stderr, "-mode: subscribe | action | lost | dispatch, получено %q\n", *mode)
		os.Exit(2)
	}

	if failures > 0 {
		fmt.Printf("\n  ИТОГ: нарушенных ожиданий — %d\n", failures)
		os.Exit(1)
	}
}

func newSession(tr *quicadapter.Transport, m session.Mode) *session.Session {
	s, err := session.New(session.Config{
		Transport: tr, Endpoint: ports.Endpoint(*addr),
		SessionToken: *sessionID, Mode: m,
	})
	if err != nil {
		fatal("New", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := s.Connect(ctx); err != nil {
		fatal("Connect", err)
	}
	fmt.Printf("  🔐 Connect ok — режим %s подтверждён сервером, state=%s\n", m, s.State())
	return s
}

func runSubscribe(tr *quicadapter.Transport) {
	fmt.Println("\n── services/session: subscribe ─────────────────")
	s := newSession(tr, session.ModeSubscribe)
	defer s.Close()

	var postID string
	if *trigger {
		fmt.Println("  ⏳ Порождаем push сами (-trigger).")
		postID = triggerPush(tr)
		defer cleanupPost(tr, postID)
	} else {
		fmt.Printf("  ⏳ Ждём push до %s. ОТПРАВЬТЕ СООБЩЕНИЕ в Mattermost.\n", *wait)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *wait)
	defer cancel()

	p, err := s.AcceptPush(ctx)
	if err != nil {
		fmt.Printf("  ❌ AcceptPush: %v\n", err)
		fmt.Printf("     errors.Is(ErrConnectionLost) = %v, state=%s\n",
			errors.Is(err, session.ErrConnectionLost), s.State())
		os.Exit(1)
	}
	fmt.Printf("  ✅ push принят САМИМ Core: event=%q user_id=%q\n", p.Event, p.UserID)
	fmt.Printf("     data (%d байт): %s\n", len(p.Data), truncate(string(p.Data), 300))
	if p.UserID == "" {
		fmt.Println("     ↑ user_id верхнего уровня пуст — ожидаемо (omitempty, см. §2.3)")
	}
}

func runAction(tr *quicadapter.Transport) {
	fmt.Println("\n── services/session: action ────────────────────")
	s := newSession(tr, session.ModeAction)
	defer s.Close()

	for _, a := range []struct {
		name string
		code photonpb.Action
	}{
		{"getMe", photonpb.Action_ACTION_GET_ME},
		{"getMyChannels", photonpb.Action_ACTION_GET_MY_CHANNELS},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		resp, err := s.Do(ctx, int32(a.code), nil)
		cancel()
		if err != nil {
			fmt.Printf("  ❌ %s (%d): %v (lost=%v)\n", a.name, a.code, err,
				errors.Is(err, session.ErrConnectionLost))
			continue
		}
		if !resp.OK {
			fmt.Printf("  ⚠️  %s (%d): ok=false error=%q\n", a.name, a.code, resp.Error)
			continue
		}
		fmt.Printf("  ✅ %s (%d): ok, data (%d байт): %s\n",
			a.name, a.code, len(resp.Data), truncate(string(resp.Data), 300))
	}
}

// runDispatch гоняет services/dispatcher — слой, который добавляет Phase 13.5.
//
// Проверяет ровно то, чего фиктивный транспорт показать не может: что
// подключение по требованию и синглфлайт работают на настоящей сети, что
// конверт ответа доходит до границы ABI в том виде, в каком его получит
// клиент, и что оборванное соединение даёт класс not_connected, а следующий
// вызов переподключается (D-3).
//
// Транспорт обёрнут счётчиком (counting.go): утверждения «сети не было» и
// «второе соединение не открывалось» иначе недоказуемы — быстрый ответ
// доказательством не является.
func runDispatch(tr *quicadapter.Transport) {
	fmt.Println("\n── services/dispatcher: action через диспетчер ──")

	ct := newCountingTransport(tr)
	d, err := dispatcher.New(dispatcher.Config{
		Endpoint:     ports.Endpoint(*addr),
		SessionToken: *sessionID,
		Transport:    ct,
	})
	if err != nil {
		fatal("dispatcher.New", err)
	}
	defer d.Close()

	// ── 1. Конструктор не касается сети ─────────────────────────────
	requireDials(ct, 0, "dispatcher.New не подключается")

	// ── 2. Неизвестный код отвергается БЕЗ сети ─────────────────────
	// На живом сервере это видно не по скорости ответа, а по тому, что
	// счётчик подключений остался нулевым.
	if _, err := d.SendAction(424242, []byte(`{}`)); err == nil {
		bad("неизвестный код принят")
	} else if c := dispatcher.Classify(err); c != dispatcher.ClassInvalidAction {
		bad("неизвестный код: класс %s, ожидался invalid_action (%v)", c, err)
	} else {
		ok("неизвестный код отвергнут: %v (класс %s)", err, c)
	}
	requireDials(ct, 0, "отказ по коду прошёл без сети")

	// ── 3. Ленивое подключение и переиспользование соединения ───────
	start := time.Now()
	first, err := d.SendAction(int32(photonpb.Action_ACTION_GET_ME), []byte(`{}`))
	firstTook := time.Since(start)
	if err != nil {
		bad("первый getMe: %v (класс %s)", err, dispatcher.Classify(err))
		return
	}
	ok("первый getMe за %s: %s", firstTook.Round(time.Millisecond), truncate(string(first), 160))
	requireDials(ct, 1, "первый SendAction подключился сам")

	start = time.Now()
	second, err := d.SendAction(int32(photonpb.Action_ACTION_GET_ME), []byte(`{}`))
	secondTook := time.Since(start)
	if err != nil {
		bad("второй getMe: %v (класс %s)", err, dispatcher.Classify(err))
		return
	}
	ok("второй getMe за %s (первый — %s)", secondTook.Round(time.Millisecond), firstTook.Round(time.Millisecond))
	requireDials(ct, 1, "второй SendAction переиспользовал соединение")
	if !bytes.Equal(first, second) {
		fmt.Println("     ⚠️  ответы различаются — это нормально, если сервер обновил update_at")
	}

	// ── 4. Отказ сервера приходит КОНВЕРТОМ, а не ошибкой вызова ────
	// getMyChannels без team_id — настоящий отказ настоящего Bridge, а не
	// подстроенный: ADR-038 требует err == nil и ok:false внутри.
	resp, err := d.SendAction(int32(photonpb.Action_ACTION_GET_MY_CHANNELS), []byte(`{}`))
	switch {
	case err != nil:
		bad("getMyChannels вернул ошибку вызова вместо конверта: %v (класс %s)",
			err, dispatcher.Classify(err))
	case bytes.Contains(resp, []byte(`"ok":false`)):
		ok("отказ сервера пришёл конвертом при err == nil: %s", truncate(string(resp), 200))
	default:
		ok("getMyChannels выполнен: %s", truncate(string(resp), 200))
		fmt.Println("     ⚠️  ok:true — проверка «ok:false конвертом» не состоялась")
	}

	// ── 5. Обрыв → not_connected ────────────────────────────────────
	// Живое QUIC-соединение закрывается снизу, из-под сессии. Это не
	// подмена объекта: соединение то самое, по которому только что шли
	// вызовы. Штатный idle-timeout для этого не годится — на сервере
	// стоит KeepAlivePeriod, и простой соединение не рвёт (-mode lost).
	conn := ct.lastConn()
	if conn == nil {
		bad("транспорт не отдал соединение — обрыв проверить нечем")
		return
	}
	conn.Close()
	if _, err := d.SendAction(int32(photonpb.Action_ACTION_GET_ME), []byte(`{}`)); err == nil {
		bad("вызов по оборванному соединению вернул успех")
	} else if c := dispatcher.Classify(err); c != dispatcher.ClassNotConnected {
		bad("обрыв дал класс %s, ожидался not_connected: %v", c, err)
	} else {
		ok("обрыв дал класс not_connected: %v", err)
		fmt.Printf("     errors.Is(ErrConnectionLost) = %v\n", errors.Is(err, session.ErrConnectionLost))
	}

	// ── 6. Следующий вызов переподключается (D-3) ───────────────────
	if _, err := d.SendAction(int32(photonpb.Action_ACTION_GET_ME), []byte(`{}`)); err != nil {
		bad("вызов после обрыва не переподключился: %v (класс %s)", err, dispatcher.Classify(err))
	} else {
		ok("следующий вызов переподключился и выполнился")
	}
	requireDials(ct, 2, "переподключение открыло ровно одно новое соединение")

	// ── 7. Синглфлайт на СВЕЖЕМ диспетчере ──────────────────────────
	// Проверяется не числом соединений (session.Connect защищает себя сам),
	// а тем, что ни один из параллельных вызовов не получил
	// ErrConnectInProgress и все они выполнились.
	ct2 := newCountingTransport(tr)
	d2, err := dispatcher.New(dispatcher.Config{
		Endpoint:     ports.Endpoint(*addr),
		SessionToken: *sessionID,
		Transport:    ct2,
	})
	if err != nil {
		fatal("dispatcher.New (второй)", err)
	}
	defer d2.Close()

	const parallel = 4
	var wg sync.WaitGroup
	errs := make([]error, parallel)
	start = time.Now()
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = d2.SendAction(int32(photonpb.Action_ACTION_GET_ME), []byte(`{}`))
		}(i)
	}
	wg.Wait()
	took := time.Since(start)

	badCalls := 0
	for i, e := range errs {
		if e == nil {
			continue
		}
		badCalls++
		bad("[%d] %v (класс %s, ErrConnectInProgress=%v)", i, e, dispatcher.Classify(e),
			errors.Is(e, session.ErrConnectInProgress))
	}
	if badCalls == 0 {
		ok("%d параллельных getMe на свежем диспетчере выполнились за %s, ни одного ErrConnectInProgress",
			parallel, took.Round(time.Millisecond))
	}
	requireDials(ct2, 1, "параллельные вызовы открыли одно соединение")
}

// runLost проверяет ErrConnectionLost на НАСТОЯЩЕМ idle-timeout, а не на
// fakeConn: подписываемся, keep-alive не шлём и ничего не делаем.
func runLost(tr *quicadapter.Transport) {
	fmt.Println("\n── services/session: обрыв по idle-timeout ─────")
	s := newSession(tr, session.ModeSubscribe)
	defer s.Close()

	fmt.Printf("  ⏳ Молчим %s. НЕ отправляйте сообщений — push отменил бы проверку.\n", *lostWindow)
	fmt.Println("     Обрыв ~30 с ожидается, ТОЛЬКО если сервер не шлёт keep-alive.")
	fmt.Println("     С KeepAlivePeriod на сервере обрыва быть не должно — это и есть")
	fmt.Println("     обратная проверка keep-alive.")

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), *lostWindow)
	defer cancel()

	_, err := s.AcceptPush(ctx)
	elapsed := time.Since(start)

	switch {
	case err == nil:
		fmt.Printf("  ⚠️  пришёл push через %s — проверка не состоялась\n", elapsed.Round(time.Second))
	case errors.Is(err, session.ErrConnectionLost):
		fmt.Printf("  ✅ ErrConnectionLost через %s\n", elapsed.Round(time.Second))
		fmt.Printf("     ошибка целиком: %v\n", err)
		fmt.Printf("     state=%s (ожидалось failed)\n", s.State())
		_, err2 := s.Do(context.Background(), int32(photonpb.Action_ACTION_GET_ME), nil)
		fmt.Printf("     повторный вызов: %v (lost=%v, notReady=%v)\n", err2,
			errors.Is(err2, session.ErrConnectionLost), errors.Is(err2, session.ErrNotReady))
	case errors.Is(err, context.DeadlineExceeded):
		// Окно истекло, обрыва не было. Для этого режима это НЕ отказ:
		// именно так выглядит работающий keep-alive на сервере.
		fmt.Printf("  ✅ обрыва не было за %s — соединение пережило простой\n",
			elapsed.Round(time.Second))
		fmt.Printf("     state=%s (ожидалось ready)\n", s.State())
		if s.State() != session.StateReady {
			fmt.Println("     ⚠️  но состояние не ready — разобраться")
			os.Exit(1)
		}

	default:
		fmt.Printf("  ❌ через %s пришла НЕ ErrConnectionLost и не таймаут окна: %v (state=%s)\n",
			elapsed.Round(time.Second), err, s.State())
		os.Exit(1)
	}
}

// ── Учёт исходов ────────────────────────────────────────────────────
//
// Прогон обязан быть проверкой, а не распечаткой: любое нарушенное ожидание
// повышает счётчик и даёт ненулевой код возврата. Печать «❌» без выхода с
// ошибкой — тот же дефект, что зелёный тест, проходящий всегда.
var failures int

func ok(format string, a ...any) { fmt.Printf("  ✅ "+format+"\n", a...) }

func bad(format string, a ...any) {
	failures++
	fmt.Printf("  ❌ "+format+"\n", a...)
}

// requireDials — утверждение о ЧИСЛЕ установленных соединений. Единственный
// способ доказать, что сети не было (см. counting.go).
func requireDials(ct *countingTransport, want int, what string) {
	if got := ct.dialCount(); got == want {
		ok("%s (подключений: %d)", what, got)
	} else {
		bad("%s: подключений %d, ожидалось %d", what, got, want)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func fatal(what string, err error) {
	fmt.Fprintf(os.Stderr, "  ❌ %s: %v\n", what, err)
	os.Exit(1)
}
