// p2pcheck — ручной прогон adapters/pion против настоящей сети.
//
// Не автотест. В `go test ./...` не участвует и участвовать не должен:
// требует двух хостов и живой сети. Тест, зависящий от внешнего окружения,
// либо делает прогон недетерминированным, либо тихо пропускается — и тогда он
// хуже отсутствующего. Тот же принцип и та же форма, что у tests/livecheck.
//
// Loopback-тесты в adapters/pion доказывают ПРОТОКОЛ. Этот инструмент
// проверяет то, чего они не проверяют: сбор кандидатов на настоящих
// интерфейсах, выбранную пару, прохождение через NAT.
//
// Signaling здесь — простейший TCP-рандеву. Это иллюстрация ADR-042
// («доставка блобов — забота вызывающего через любой существующий канал»), а
// не часть Photon: адаптер о нём ничего не знает.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	edadapter "github.com/wiloris/photon/photon-core/adapters/ed25519"
	pionadapter "github.com/wiloris/photon/photon-core/adapters/pion"
	"github.com/wiloris/photon/photon-core/ports"
)

var (
	mode        = flag.String("mode", "", "responder | initiator")
	signalAddr  = flag.String("signal", ":7777", "адрес TCP-рандеву: responder слушает, initiator подключается")
	stunTurn    = flag.String("stun-turn", "", "список STUN/TURN через запятую; пусто — только host-кандидаты")
	turnUser    = flag.String("turn-user", "", "имя пользователя TURN")
	turnPass    = flag.String("turn-pass", "", "пароль TURN")
	forceRelay  = flag.Bool("force-relay", false, "оставить только relay-кандидатов (проверка relay-пути)")
	expectWrong = flag.Bool("expect-wrong-peer", false, "ожидать заведомо чужой ключ — проверка терминального отказа")
	cancelAt    = flag.String("cancel-at", "", "отменить контекст на шаге: offer | accept | complete")
	timeout     = flag.Duration("timeout", 60*time.Second, "общий потолок прогона")
)

var failures int

func fail(format string, a ...any) {
	fmt.Printf("  ❌ "+format+"\n", a...)
	failures++
}
func ok(format string, a ...any) { fmt.Printf("  ✅ "+format+"\n", a...) }

// Обмен идёт в ДВА этапа, и это не удобство, а следование контракту.
//
// Порт требует знать ключ peer'а уже в CreateOffer: «discovery не входит в
// этот порт — P2P начинает работать, когда peer уже известен» (ports/p2p.go).
// Поэтому сначала стороны обмениваются ключами — это discovery, лежащий ВНЕ
// P2P, — и только потом начинается согласование.
//
// Первая версия инструмента передавала в CreateOffer пустое ожидание и
// узнавала ключ из ответа. Адаптер отверг это как ответ не тому peer'у — и
// был прав: контракт нарушал инструмент, а не адаптер.
type keyMsg struct {
	PublicKey []byte `json:"pubkey"`
}

type blobMsg struct {
	Blob []byte `json:"blob"`
}

func main() {
	flag.Parse()
	if *mode != "responder" && *mode != "initiator" {
		fmt.Fprintln(os.Stderr, "нужен -mode responder|initiator")
		os.Exit(2)
	}

	id := edadapter.New()
	self, err := id.Generate(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, "Generate:", err)
		os.Exit(2)
	}
	cfg := pionadapter.Config{
		Identity: id, Self: self,
		TURNUser: *turnUser, TURNPass: *turnPass,
		ForceRelay:       *forceRelay,
		EstablishTimeout: *timeout,
	}
	if *stunTurn != "" {
		cfg.STUNTURN = strings.Split(*stunTurn, ",")
	}
	a, err := pionadapter.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "New:", err)
		os.Exit(2)
	}
	defer a.Close()

	fmt.Printf("\n── p2pcheck %s ─────────────────────────────\n", *mode)
	fmt.Printf("  свой ключ: %s\n", short(self.Public))
	fmt.Printf("  STUN/TURN: %v  force-relay=%v\n", cfg.STUNTURN, *forceRelay)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var sess ports.Session
	if *mode == "initiator" {
		sess, err = runInitiator(ctx, a, self)
	} else {
		sess, err = runResponder(ctx, a, self)
	}
	if err != nil {
		if *expectWrong || *cancelAt != "" {
			ok("ожидаемый отказ: %v", err)
			report()
			return
		}
		fail("согласование: %v", err)
		report()
		return
	}
	defer sess.Close()

	checkSession(ctx, sess)
	report()
}

func report() {
	if failures > 0 {
		fmt.Printf("\n  ИТОГ: нарушенных ожиданий — %d\n", failures)
		os.Exit(1)
	}
	fmt.Printf("\n  ИТОГ: все ожидания выполнены\n")
}

func short(k []byte) string {
	s := hex.EncodeToString(k)
	if len(s) > 16 {
		return s[:16] + "…"
	}
	return s
}

func stepCtx(ctx context.Context, step string) (context.Context, context.CancelFunc) {
	if *cancelAt == step {
		c, cancelFn := context.WithCancel(ctx)
		cancelFn() // отменён до вызова — проверяем реакцию на реальном пути
		return c, func() {}
	}
	return ctx, func() {}
}

func runInitiator(ctx context.Context, a *pionadapter.Adapter, self ports.IdentityPair) (ports.Session, error) {
	conn, err := net.DialTimeout("tcp", *signalAddr, 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("рандеву: %w", err)
	}
	defer conn.Close()

	enc, dec := json.NewEncoder(conn), json.NewDecoder(conn)

	// Этап 1 — discovery вне P2P: узнаём ключ собеседника.
	if err := enc.Encode(keyMsg{PublicKey: self.Public}); err != nil {
		return nil, fmt.Errorf("отправка ключа: %w", err)
	}
	var pk keyMsg
	if err := dec.Decode(&pk); err != nil {
		return nil, fmt.Errorf("приём ключа: %w", err)
	}
	ok("ключ peer'а получен вне P2P: %s", short(pk.PublicKey))

	expected := ports.PublicKey(pk.PublicKey)
	if *expectWrong {
		expected = wrongKey(pk.PublicKey)
		fmt.Printf("  ⚠️  подменяем ожидание на заведомо чужой ключ: %s\n", short(expected))
	}

	// Этап 2 — собственно согласование, peer уже известен.
	octx, ocancel := stepCtx(ctx, "offer")
	defer ocancel()
	t0 := time.Now()
	offer, err := a.CreateOffer(octx, expected)
	if err != nil {
		return nil, fmt.Errorf("CreateOffer: %w", err)
	}
	ok("CreateOffer: кандидаты собраны за %s, блоб %d байт", time.Since(t0).Round(time.Millisecond), len(offer))
	printCandidates(offer)

	if err := enc.Encode(blobMsg{Blob: offer}); err != nil {
		return nil, fmt.Errorf("отправка offer: %w", err)
	}
	var in blobMsg
	if err := dec.Decode(&in); err != nil {
		return nil, fmt.Errorf("приём answer: %w", err)
	}
	ok("answer получен, %d байт", len(in.Blob))
	printCandidates(in.Blob)

	cctx, ccancel := stepCtx(ctx, "complete")
	defer ccancel()
	sess, err := a.CompleteSession(cctx, expected, in.Blob)
	if err != nil {
		return nil, fmt.Errorf("CompleteSession: %w", err)
	}
	return sess, waitReady(ctx, sess)
}

func runResponder(ctx context.Context, a *pionadapter.Adapter, self ports.IdentityPair) (ports.Session, error) {
	ln, err := net.Listen("tcp", *signalAddr)
	if err != nil {
		return nil, fmt.Errorf("рандеву: %w", err)
	}
	defer ln.Close()
	fmt.Printf("  ⏳ ждём инициатора на %s\n", *signalAddr)

	conn, err := ln.Accept()
	if err != nil {
		return nil, fmt.Errorf("accept: %w", err)
	}
	defer conn.Close()

	enc, dec := json.NewEncoder(conn), json.NewDecoder(conn)

	// Этап 1 — discovery вне P2P.
	var pk keyMsg
	if err := dec.Decode(&pk); err != nil {
		return nil, fmt.Errorf("приём ключа: %w", err)
	}
	if err := enc.Encode(keyMsg{PublicKey: self.Public}); err != nil {
		return nil, fmt.Errorf("отправка ключа: %w", err)
	}
	ok("ключ peer'а получен вне P2P: %s", short(pk.PublicKey))

	expected := ports.PublicKey(pk.PublicKey)
	if *expectWrong {
		expected = wrongKey(pk.PublicKey)
		fmt.Printf("  ⚠️  подменяем ожидание на заведомо чужой ключ: %s\n", short(expected))
	}

	// Этап 2 — согласование.
	var in blobMsg
	if err := dec.Decode(&in); err != nil {
		return nil, fmt.Errorf("приём offer: %w", err)
	}
	ok("offer получен, %d байт", len(in.Blob))
	printCandidates(in.Blob)

	actx, acancel := stepCtx(ctx, "accept")
	defer acancel()
	t0 := time.Now()
	answer, sess, err := a.AcceptOffer(actx, expected, in.Blob)
	if err != nil {
		return nil, fmt.Errorf("AcceptOffer: %w", err)
	}
	ok("AcceptOffer: кандидаты собраны за %s, блоб %d байт", time.Since(t0).Round(time.Millisecond), len(answer))
	printCandidates(answer)

	if err := enc.Encode(blobMsg{Blob: answer}); err != nil {
		return nil, fmt.Errorf("отправка answer: %w", err)
	}
	return sess, waitReady(ctx, sess)
}

func wrongKey(k []byte) ports.PublicKey {
	out := make([]byte, len(k))
	copy(out, k)
	out[0] ^= 0xff
	return out
}

// waitReady дожидается установления через блокирующий метод сессии: другого
// способа узнать об исходе у порта нет, и это осознанная часть контракта
// (ADR-044 §1).
func waitReady(ctx context.Context, sess ports.Session) error {
	done := make(chan error, 1)
	go func() {
		if p := sess.Peer(); p == nil {
			done <- errors.New("сессия завершилась терминально до установления")
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func printCandidates(blob []byte) {
	var b struct {
		Candidates []string `json:"candidates"`
	}
	if json.Unmarshal(blob, &b) != nil {
		return
	}
	for _, c := range b.Candidates {
		fmt.Printf("       кандидат: %s\n", c)
	}
}

func checkSession(ctx context.Context, sess ports.Session) {
	peer := sess.Peer()
	if peer == nil {
		fail("Peer() пуст — handshake ADR-041 не подтвердил ключ")
		return
	}
	ok("Peer() подтверждён: %s", short(peer))

	ep := string(sess.RemoteEndpoint())
	ok("выбранная пара кандидатов: %s", ep)
	switch {
	case *forceRelay && !strings.Contains(ep, "relay"):
		fail("запрошен relay, но выбранная пара не relay — прогон НЕ доказывает relay-путь")
	case *forceRelay:
		ok("выбранная пара действительно relay")
	case strings.Contains(ep, "relay"):
		fail("ожидался direct, а выбран relay — прогон не доказывает direct-путь")
	default:
		ok("выбранная пара не relay — direct-путь")
	}

	if *mode == "initiator" {
		echoClient(ctx, sess)
	} else {
		echoServer(ctx, sess)
	}
}

// Инициатор пишет и ждёт эхо; отвечающий отражает. Проверяются оба вида
// потоков: двунаправленный и однонаправленный (ADR-016).
func echoClient(ctx context.Context, sess ports.Session) {
	st, err := sess.OpenStream(ctx)
	if err != nil {
		fail("OpenStream: %v", err)
		return
	}
	if _, err := st.Write([]byte("ping")); err != nil {
		fail("Write: %v", err)
		return
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(st, buf); err != nil {
		fail("чтение эха: %v", err)
		return
	}
	if string(buf) != "pong" {
		fail("эхо %q, ожидалось pong", buf)
		return
	}
	ok("двунаправленный поток: ping → pong")

	us, err := sess.OpenUniStream(ctx)
	if err != nil {
		fail("OpenUniStream: %v", err)
		return
	}
	if _, err := us.Write([]byte("uni!")); err != nil {
		fail("запись в однонаправленный: %v", err)
		return
	}
	ok("однонаправленный поток отправлен")
	time.Sleep(2 * time.Second) // дать отвечающему прочитать до Close
}

func echoServer(ctx context.Context, sess ports.Session) {
	st, err := sess.AcceptStream(ctx)
	if err != nil {
		fail("AcceptStream: %v", err)
		return
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(st, buf); err != nil {
		fail("чтение: %v", err)
		return
	}
	if _, err := st.Write([]byte("pong")); err != nil {
		fail("запись эха: %v", err)
		return
	}
	ok("двунаправленный поток: принято %q, отвечено pong", buf)

	rs, err := sess.AcceptUniStream(ctx)
	if err != nil {
		fail("AcceptUniStream: %v", err)
		return
	}
	ubuf := make([]byte, 4)
	if _, err := io.ReadFull(rs, ubuf); err != nil {
		fail("чтение однонаправленного: %v", err)
		return
	}
	ok("однонаправленный поток принят: %q", ubuf)
}
