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
//	go run ./tests/livecheck -mode subscribe -session <Session ID>
//	go run ./tests/livecheck -mode action    -session <Session ID>
//	go run ./tests/livecheck -mode lost      -session <Session ID>
//
// Session ID — это Session.Id, а НЕ токен из /users/login: имя поля
// session_token на проводе историческое (bridge-session.md §2.1).
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	quicadapter "github.com/wiloris/photon-core/adapters/quic"
	"github.com/wiloris/photon-core/ports"
	photonpb "github.com/wiloris/photon-core/protobuf"
	"github.com/wiloris/photon-core/services/session"
)

var (
	addr       = flag.String("addr", "vpn.wiloris.ru:4443", "адрес Bridge")
	sessionID  = flag.String("session", "", "Session ID Mattermost (обязателен)")
	mode       = flag.String("mode", "subscribe", "subscribe | action | lost")
	keepalive  = flag.Duration("keepalive", 0, "KeepAlivePeriod клиента; 0 — как в проде")
	wait       = flag.Duration("wait", 90*time.Second, "окно ожидания push")
	lostWindow = flag.Duration("lost-window", 150*time.Second,
		"сколько молчать в -mode lost. Должно заметно превышать idle-timeout (30 с)")
	insecure = flag.Bool("insecure", false, "не проверять сертификат сервера")
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
	default:
		fmt.Fprintf(os.Stderr, "-mode: subscribe | action | lost, получено %q\n", *mode)
		os.Exit(2)
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

	fmt.Printf("  ⏳ Ждём push до %s. ОТПРАВЬТЕ СООБЩЕНИЕ в Mattermost.\n", *wait)
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
