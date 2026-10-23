package pion

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	edadapter "github.com/wiloris/photon-core/adapters/ed25519"
	"github.com/wiloris/photon-core/ports"
)

// Исчезновение собеседника БЕЗ закрытия: процесс убит, сеть пропала, телефон
// ушёл из зоны. Ни DTLS close_notify, ни SCTP ABORT не приходят — остаётся
// только тишина на ICE.
//
// Найдено разбором зависания TestCleanShutdownHalfClose (tests/p2pslice):
// отвечающий процесс завершился, а инициатор висел в Stream.Read до убийства
// по 180-секундному сроку теста. Адаптер не подписывался на состояние
// ICE-агента, и отказ ICE после установления не доходил до сессии вовсе.

const testPeerLoss = 3 * time.Second

func newPeerWithLoss(t *testing.T, loss time.Duration) peerFixture {
	t.Helper()
	id := edadapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	a, err := New(Config{Identity: id, Self: pair, EstablishTimeout: 20 * time.Second, PeerLossTimeout: loss})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return peerFixture{adapter: a, pair: pair}
}

// vanish моделирует смерть процесса собеседника: его ICE-агент закрывается,
// UDP-сокеты исчезают, и ни одно прощальное сообщение верхних слоёв уже не
// может уйти — им не через что.
func vanish(t *testing.T, s ports.Session) {
	t.Helper()
	ps, ok := s.(*session)
	if !ok || ps.agent == nil {
		t.Fatalf("сессия без ICE-агента: %T", s)
	}
	if err := ps.agent.Close(); err != nil {
		t.Fatalf("закрыть агент собеседника: %v", err)
	}
}

func TestPeerLossFailsBlockedRead(t *testing.T) {
	a, b := newPeerWithLoss(t, testPeerLoss), newPeerWithLoss(t, testPeerLoss)
	sA, sB := connect(t, a, b)
	if sA.Peer() == nil || sB.Peer() == nil {
		t.Fatal("сессия не установилась")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stA, err := sA.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := stA.Write([]byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	stB, err := sB.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	if _, err := io.ReadFull(stB, make([]byte, 1)); err != nil {
		t.Fatalf("B не принял байт: %v", err)
	}

	// A ждёт данных, которые уже никогда не придут.
	readErr := make(chan error, 1)
	go func() {
		_, err := stA.Read(make([]byte, 16))
		readErr <- err
	}()

	start := time.Now()
	vanish(t, sB)

	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("Read вернул данные от исчезнувшего собеседника")
		}
		// Потеря — отказ, а не «собеседник закончил»: по io.EOF вызывающий
		// (coreops, ADR-076 §3.4) сообщил бы плагину remote.
		if errors.Is(err, io.EOF) || !errors.Is(err, ports.ErrSessionFailed) {
			t.Fatalf("потеря собеседника выдана как %v; ожидался ports.ErrSessionFailed, не io.EOF", err)
		}
		t.Logf("Read отказал через %s: %v", time.Since(start).Round(100*time.Millisecond), err)
	case <-time.After(testPeerLoss + 10*time.Second):
		t.Fatalf("Read висит дольше %s после исчезновения собеседника — потеря не обнаружена",
			testPeerLoss+10*time.Second)
	}

	// После потери сессия терминальна: новый поток не открывается.
	octx, ocancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ocancel()
	if _, err := sA.OpenStream(octx); err == nil {
		t.Fatal("OpenStream после потери собеседника успешен")
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("OpenStream после потери висит до дедлайна, а не отказывает: %v", err)
	}
	_ = sA.Close()
	_ = sB.Close()
}

// Потеря собеседника освобождает ресурсы своей стороны: агент закрыт сразу,
// а не до Close() вызывающего, который о потере мог ещё не узнать.
func TestPeerLossReleasesLocalAgent(t *testing.T) {
	a, b := newPeerWithLoss(t, testPeerLoss), newPeerWithLoss(t, testPeerLoss)
	sA, sB := connect(t, a, b)
	if sA.Peer() == nil || sB.Peer() == nil {
		t.Fatal("сессия не установилась")
	}
	vanish(t, sB)

	agent := sA.(*session).agent
	deadline := time.Now().Add(testPeerLoss + 10*time.Second)
	for time.Now().Before(deadline) {
		// Закрытый агент отказывает в любом действии; живой — нет.
		if _, err := agent.GetLocalCandidates(); err != nil {
			_ = sA.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("агент стороны A жив после потери собеседника — ресурсы удерживаются до Close() вызывающего")
}

// Отсутствие потери не путается с потерей: молчание КОРОЧЕ срока при живом
// собеседнике сессию не рвёт. Keepalive ICE идёт и без данных.
func TestIdleSessionWithLivePeerSurvivesLossTimeout(t *testing.T) {
	a, b := newPeerWithLoss(t, testPeerLoss), newPeerWithLoss(t, testPeerLoss)
	sA, sB := connect(t, a, b)
	if sA.Peer() == nil || sB.Peer() == nil {
		t.Fatal("сессия не установилась")
	}
	time.Sleep(2*testPeerLoss + time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stA, err := sA.OpenStream(ctx)
	if err != nil {
		t.Fatalf("после простоя дольше срока потери сессия мертва: %v", err)
	}
	if _, err := stA.Write([]byte("y")); err != nil {
		t.Fatalf("Write после простоя: %v", err)
	}
	stB, err := sB.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream после простоя: %v", err)
	}
	if _, err := io.ReadFull(stB, make([]byte, 1)); err != nil {
		t.Fatalf("приём после простоя: %v", err)
	}
	_ = sA.Close()
	_ = sB.Close()
}

func TestPeerLossTimeoutValidation(t *testing.T) {
	id := edadapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Identity: id, Self: pair, EstablishTimeout: time.Second, PeerLossTimeout: -time.Second}); err == nil {
		t.Fatal("отрицательный PeerLossTimeout принят")
	}
	if _, err := New(Config{Identity: id, Self: pair, EstablishTimeout: time.Second, PeerLossTimeout: 100 * time.Millisecond}); err == nil {
		t.Fatal("PeerLossTimeout короче нижней границы принят: keepalive не успевал бы подтверждать живого собеседника")
	}
}

// Утечка ресурсов: циклы «установить — обменяться — закрыть», в том числе с
// потерей собеседника, не оставляют за собой горутин ICE/DTLS/SCTP. Горутина
// — верхняя оценка удержанного агента: у каждого живого есть свои циклы
// проверок и чтения сокета.
func TestSessionCyclesDoNotLeakGoroutines(t *testing.T) {
	a, b := newPeerWithLoss(t, testPeerLoss), newPeerWithLoss(t, testPeerLoss)
	cycle := func(lose bool) {
		sA, sB := connect(t, a, b)
		if sA.Peer() == nil || sB.Peer() == nil {
			t.Fatal("сессия не установилась")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		st, err := sA.OpenStream(ctx)
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		if _, err := st.Write([]byte("z")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if lose {
			vanish(t, sB)
			if !terminalRead(st, testPeerLoss+10*time.Second) {
				t.Fatal("потеря собеседника не обнаружена")
			}
		}
		_ = sA.Close()
		_ = sB.Close()
	}
	cycle(false) // прогрев: ленивые глобальные горутины библиотек
	base := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		cycle(i == 2)
	}
	deadline := time.Now().Add(15 * time.Second)
	for runtime.NumGoroutine() > base+2 {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("горутин после 5 циклов: %d, базовая линия %d — ресурсы сессий не освобождены\n%s",
				runtime.NumGoroutine(), base, buf[:n])
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("горутин: база %d, после 5 циклов %d", base, runtime.NumGoroutine())
}

// terminalRead ждёт, что чтение потока завершится ошибкой не позже срока.
func terminalRead(st io.Reader, d time.Duration) bool {
	done := make(chan error, 1)
	go func() {
		_, err := st.Read(make([]byte, 8))
		done <- err
	}()
	select {
	case err := <-done:
		return err != nil
	case <-time.After(d):
		return false
	}
}
