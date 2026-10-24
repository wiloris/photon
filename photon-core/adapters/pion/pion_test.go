package pion

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/pion/turn/v3"
	edadapter "github.com/wiloris/photon/photon-core/adapters/ed25519"
	"github.com/wiloris/photon/photon-core/ports"
)

// Loopback-прогон: два агента в ОДНОМ процессе, доставка блобов — прямой
// вызов. Это доказывает протокол и НЕ доказывает обход NAT: настоящий
// traversal, relay и симметричный NAT — отдельные проверки со своим
// критерием (p2p-session.md §10.2). Смешивать нельзя — это тот же класс
// ошибки, что зелёный тест на фиктивном транспорте в Phase 12.5.

var (
	_ ports.P2P     = (*Adapter)(nil)
	_ ports.Session = (*session)(nil)
)

type peerFixture struct {
	adapter *Adapter
	pair    ports.IdentityPair
}

func newPeer(t *testing.T) peerFixture {
	t.Helper()
	id := edadapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	a, err := New(Config{Identity: id, Self: pair, EstablishTimeout: 20 * time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return peerFixture{adapter: a, pair: pair}
}

// connect выполняет все три шага порта и дожидается установления обеих сторон.
//
// Контекст живёт до конца теста, а НЕ до выхода из этой функции: по ADR-044 §2
// его отмена прерывает согласование, а согласование продолжается в фоне после
// возврата из AcceptOffer/CompleteSession. Привычное `defer cancel()` здесь
// убило бы сессию — см. TestCancellationAbortsEstablishment, где это же
// свойство проверяется намеренно.
func connect(t *testing.T, a, b peerFixture) (ports.Session, ports.Session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)

	offer, err := a.adapter.CreateOffer(ctx, b.pair.Public)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	answer, bSess, err := b.adapter.AcceptOffer(ctx, a.pair.Public, offer)
	if err != nil {
		t.Fatalf("AcceptOffer: %v", err)
	}
	aSess, err := a.adapter.CompleteSession(ctx, b.pair.Public, answer)
	if err != nil {
		t.Fatalf("CompleteSession: %v", err)
	}
	return aSess, bSess
}

// Peer() обязана вернуть КРИПТОГРАФИЧЕСКИ ПОДТВЕРЖДЁННЫЙ ключ (ADR-041 §1),
// а не тот, который передали аргументом.
func TestSessionPeerIsAuthenticated(t *testing.T) {
	a, b := newPeer(t), newPeer(t)
	aSess, bSess := connect(t, a, b)
	defer aSess.Close()
	defer bSess.Close()

	if got := aSess.Peer(); string(got) != string(b.pair.Public) {
		t.Fatalf("A видит peer %x, ожидался %x", got, b.pair.Public)
	}
	if got := bSess.Peer(); string(got) != string(a.pair.Public) {
		t.Fatalf("B видит peer %x, ожидался %x", got, a.pair.Public)
	}
}

// Подмена ожидания: A запрашивает соединение с other, а отвечает B.
// Handshake обязан дать ТЕРМИНАЛЬНЫЙ отказ, а не подставить фактический ключ
// вместо ожидаемого (ADR-041 §1, p2p-session.md §3).
func TestPeerMismatchIsTerminal(t *testing.T) {
	a, b, other := newPeer(t), newPeer(t), newPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)

	offer, err := a.adapter.CreateOffer(ctx, other.pair.Public) // ждём НЕ b
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	answer, bSess, err := b.adapter.AcceptOffer(ctx, a.pair.Public, offer)
	if err != nil {
		t.Fatalf("AcceptOffer: %v", err)
	}
	defer bSess.Close()

	aSess, err := a.adapter.CompleteSession(ctx, other.pair.Public, answer)
	if err != nil {
		t.Fatalf("CompleteSession: %v", err)
	}
	defer aSess.Close()

	// Сессия обязана перейти в терминальное состояние, а не ждать таймаута:
	// ключ подтвердился, но не тот, который запрашивали.
	short, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer scancel()
	if _, err := aSess.OpenStream(short); !errors.Is(err, ports.ErrSessionFailed) {
		t.Fatalf("сессия с чужим ключом не отвергнута: %v", err)
	}
	if p := aSess.Peer(); p != nil {
		t.Fatalf("Peer() вернул ключ при неудавшемся handshake: %x", p)
	}
}

func TestBidirectionalStream(t *testing.T) {
	a, b := newPeer(t), newPeer(t)
	aSess, bSess := connect(t, a, b)
	defer aSess.Close()
	defer bSess.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	got := make(chan string, 1)
	go func() {
		st, err := bSess.AcceptStream(ctx)
		if err != nil {
			t.Errorf("AcceptStream: %v", err)
			close(got)
			return
		}
		buf := make([]byte, 5)
		if _, rerr := io.ReadFull(st, buf); rerr != nil {
			t.Errorf("Read: %v", rerr)
			close(got)
			return
		}
		if _, werr := st.Write([]byte("world")); werr != nil {
			t.Errorf("Write: %v", werr)
		}
		got <- string(buf)
	}()

	st, err := aSess.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := st.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if v, ok := <-got; !ok || v != "hello" {
		t.Fatalf("принято %q", v)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(st, buf); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if string(buf) != "world" {
		t.Fatalf("ответ %q", buf)
	}
}

// ADR-016: однонаправленные потоки обязательны для ВСЕХ адаптеров. SCTP их
// нативно не даёт — проверяем соглашение из p2p-session.md §7.
func TestUniStream(t *testing.T) {
	a, b := newPeer(t), newPeer(t)
	aSess, bSess := connect(t, a, b)
	defer aSess.Close()
	defer bSess.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	got := make(chan string, 1)
	go func() {
		rs, err := bSess.AcceptUniStream(ctx)
		if err != nil {
			t.Errorf("AcceptUniStream: %v", err)
			close(got)
			return
		}
		buf := make([]byte, 4)
		if _, rerr := io.ReadFull(rs, buf); rerr != nil {
			t.Errorf("Read: %v", rerr)
			close(got)
			return
		}
		got <- string(buf)
	}()

	ss, err := aSess.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	if _, err := ss.Write([]byte("push")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if v, ok := <-got; !ok || v != "push" {
		t.Fatalf("принято %q", v)
	}
}

// Однонаправленный поток не должен попасть в AcceptStream, и наоборот:
// иначе соглашение о направлении не работает, а тесты выше проходили бы
// по совпадению.
func TestStreamKindsAreNotMixedUp(t *testing.T) {
	a, b := newPeer(t), newPeer(t)
	aSess, bSess := connect(t, a, b)
	defer aSess.Close()
	defer bSess.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ss, err := aSess.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	if _, err := ss.Write([]byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	short, scancel := context.WithTimeout(ctx, 2*time.Second)
	defer scancel()
	if _, err := bSess.AcceptStream(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("однонаправленный поток пришёл в AcceptStream: err=%v", err)
	}
}

// ADR-044 §8: Close() экземпляра P2P не закрывает установленные сессии.
func TestAdapterCloseKeepsEstablishedSessions(t *testing.T) {
	a, b := newPeer(t), newPeer(t)
	aSess, bSess := connect(t, a, b)
	defer aSess.Close()
	defer bSess.Close()

	if err := a.adapter.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := a.adapter.Close(); err != nil {
		t.Fatalf("Close не идемпотентен: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got := make(chan struct{})
	go func() {
		if _, err := bSess.AcceptStream(ctx); err != nil {
			t.Errorf("AcceptStream после Close адаптера: %v", err)
		}
		close(got)
	}()
	if _, err := aSess.OpenStream(ctx); err != nil {
		t.Fatalf("OpenStream после Close адаптера: %v", err)
	}
	<-got

	// Новые согласования при этом прекращены.
	if _, err := a.adapter.CreateOffer(ctx, b.pair.Public); !errors.Is(err, ports.ErrSessionFailed) {
		t.Fatalf("после Close согласование началось: %v", err)
	}
}

// ADR-044 §6: повторный CompleteSession — ошибка, не вторая сессия.
func TestRepeatedCompleteSessionIsError(t *testing.T) {
	a, b := newPeer(t), newPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)

	offer, err := a.adapter.CreateOffer(ctx, b.pair.Public)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	answer, bSess, err := b.adapter.AcceptOffer(ctx, a.pair.Public, offer)
	if err != nil {
		t.Fatalf("AcceptOffer: %v", err)
	}
	defer bSess.Close()
	aSess, err := a.adapter.CompleteSession(ctx, b.pair.Public, answer)
	if err != nil {
		t.Fatalf("CompleteSession: %v", err)
	}
	defer aSess.Close()

	if _, err := a.adapter.CompleteSession(ctx, b.pair.Public, answer); !errors.Is(err, ports.ErrSessionFailed) {
		t.Fatalf("повторный CompleteSession прошёл: %v", err)
	}
}

// ADR-044 §7: несколько одновременных сессий к одному peer'у.
func TestConcurrentSessionsToSamePeer(t *testing.T) {
	a, b := newPeer(t), newPeer(t)
	s1a, s1b := connect(t, a, b)
	defer s1a.Close()
	defer s1b.Close()
	s2a, s2b := connect(t, a, b)
	defer s2a.Close()
	defer s2b.Close()

	if string(s1a.Peer()) != string(b.pair.Public) || string(s2a.Peer()) != string(b.pair.Public) {
		t.Fatal("одна из сессий не подтвердила peer")
	}
	// Закрытие одной не влияет на другую.
	if err := s1a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s2a.OpenStream(ctx); err != nil {
		t.Fatalf("вторая сессия пострадала от закрытия первой: %v", err)
	}
}

// Отмена контекста на каждом шаге (ADR-044 §2).
func TestContextCancellation(t *testing.T) {
	a, b := newPeer(t), newPeer(t)

	t.Run("CreateOffer", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := a.adapter.CreateOffer(ctx, b.pair.Public); err == nil {
			t.Fatal("отменённый контекст не остановил CreateOffer")
		}
	})

	t.Run("AcceptOffer", func(t *testing.T) {
		octx, ocancel := context.WithTimeout(context.Background(), 20*time.Second)
		t.Cleanup(ocancel)
		offer, err := a.adapter.CreateOffer(octx, b.pair.Public)
		if err != nil {
			t.Fatalf("CreateOffer: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := b.adapter.AcceptOffer(ctx, a.pair.Public, offer); err == nil {
			t.Fatal("отменённый контекст не остановил AcceptOffer")
		}
	})
}

// Битые блобы отвергаются разбором, а не таймаутом.
func TestMalformedBlobs(t *testing.T) {
	a, b := newPeer(t), newPeer(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		offer string
	}{
		{"не JSON", "{"},
		{"чужая версия", `{"v":99,"ice_ufrag":"u","ice_pwd":"p","dtls_role":"server","nonce":"AAAA"}`},
		{"чужая роль", `{"v":1,"ice_ufrag":"u","ice_pwd":"p","dtls_role":"client","nonce":"AAAA"}`},
		{"без креденшелов", `{"v":1,"dtls_role":"server","nonce":"AAAA"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := b.adapter.AcceptOffer(ctx, a.pair.Public, []byte(tc.offer)); err == nil {
				t.Fatal("битый блоб принят")
			}
		})
	}
}

// --- ADR-041: transcript ----------------------------------------------------
//
// Проверяется на уровне функции, а не через полную сессию: подделать чужой
// домен или воспроизвести старый handshake проще и точнее там, где строится
// подписываемая строка.

func TestTranscriptIsDomainSeparated(t *testing.T) {
	got := string(transcript([]byte("A"), []byte("B"), []byte("na"), []byte("nb"), []byte("bind")))
	if got[:len(handshakeDomain)] != handshakeDomain {
		t.Fatalf("transcript не начинается с домена: %q", got)
	}
	for _, foreign := range []string{"photon-device-auth:", "plugin-sign:"} {
		if len(got) >= len(foreign) && got[:len(foreign)] == foreign {
			t.Fatalf("transcript начинается чужим доменом %q — материал переносим между контекстами", foreign)
		}
	}
}

// ADR-037: границы полей кодируются ЯВНО. Без длин две разные пары полей
// давали бы одну байтовую строку, и подпись переносилась бы между ними.
func TestTranscriptFieldBoundariesAreExplicit(t *testing.T) {
	x := transcript([]byte("ab"), []byte("c"), []byte("n1"), []byte("n2"), []byte("b"))
	y := transcript([]byte("a"), []byte("bc"), []byte("n1"), []byte("n2"), []byte("b"))
	if string(x) == string(y) {
		t.Fatal("(\"ab\",\"c\") и (\"a\",\"bc\") дали одинаковый transcript — границы не закодированы")
	}
}

// Свежие nonce обеих сторон: записанный ранее handshake не воспроизводится.
func TestTranscriptBindsBothNonces(t *testing.T) {
	base := transcript([]byte("A"), []byte("B"), []byte("n1"), []byte("n2"), []byte("bind"))
	for _, tc := range []struct {
		name string
		got  []byte
	}{
		{"другой nonce инициатора", transcript([]byte("A"), []byte("B"), []byte("XX"), []byte("n2"), []byte("bind"))},
		{"другой nonce отвечающего", transcript([]byte("A"), []byte("B"), []byte("n1"), []byte("XX"), []byte("bind"))},
		{"другой channel binding", transcript([]byte("A"), []byte("B"), []byte("n1"), []byte("n2"), []byte("othr"))},
		{"переставленные ключи", transcript([]byte("B"), []byte("A"), []byte("n1"), []byte("n2"), []byte("bind"))},
	} {
		if string(tc.got) == string(base) {
			t.Fatalf("%s не изменил transcript — подпись переносима", tc.name)
		}
	}
}

// Подпись, сделанная в домене device-auth (ADR-037), не проходит как
// P2P-handshake: входные пространства не пересекаются по первому полю.
func TestForeignDomainSignatureDoesNotVerify(t *testing.T) {
	id := edadapter.New()
	ctx := context.Background()
	pair, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	msg := transcript(pair.Public, []byte("B"), []byte("n1"), []byte("n2"), []byte("bind"))
	foreign := append([]byte("photon-device-auth:"), msg[len(handshakeDomain):]...)

	sig, err := id.Sign(ctx, pair.Private, foreign)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	ok, err := id.Verify(ctx, pair.Public, msg, ports.Signature(sig))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if ok {
		t.Fatal("подпись из чужого домена принята как P2P-handshake")
	}
}

// Изолирует ИМЕННО сверку подтверждённого ключа с ожидаемым.
//
// Нужен отдельно: TestPeerMismatchIsTerminal проходит и без этой сверки,
// потому что ключи обеих сторон входят в transcript — при расхождении
// ожидания подписи просто не сходятся. Мутация (снять сверку) показала, что
// та проверка её не покрывает. Здесь обе стороны строят ОДИН И ТОТ ЖЕ
// transcript, и единственное, что отличается, — ожидание вызывающего.
func TestExpectedPeerCheckIsIndependent(t *testing.T) {
	id := edadapter.New()
	ctx := context.Background()
	initiator, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	responder, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	third, err := id.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	nonceA, nonceB := []byte("nonce-initiator"), []byte("nonce-responder")
	binding := []byte("channel-binding")

	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	done := make(chan error, 1)
	go func() {
		// Отвечающий ждёт инициатора — ожидание верное.
		_, herr := runHandshake(ctx, right, id, responder, initiator.Public,
			initiator.Public, responder.Public, nonceA, nonceB, binding)
		done <- herr
	}()

	// Инициатор строит ТОТ ЖЕ transcript, но ожидает третий ключ.
	_, err = runHandshake(ctx, left, id, initiator, third.Public,
		initiator.Public, responder.Public, nonceA, nonceB, binding)
	if err == nil {
		t.Fatal("handshake принят при несовпадении с ожидаемым ключом")
	}
	if !strings.Contains(err.Error(), "peer key mismatch") {
		t.Fatalf("ошибка не про сверку ключа: %v", err)
	}
	<-done
}

// ADR-044 §2: отмена контекста, переданного в CompleteSession, обязана
// прервать согласование — Session переходит в терминальное состояние.
//
// Найдено сетевым прогоном: establish шёл со СВОИМ фоновым контекстом и
// отмену вызывающего игнорировал. Loopback-тесты этого не показывали, потому
// что там согласование успевает завершиться раньше, чем кто-либо отменит.
//
// ДВА подтеста, и это не симметрия ради симметрии. Первая редакция теста
// состояла из одного «отмена до вызова» — и оставалась зелёной на мутации,
// возвращающей прежний дефект: CompleteSession отвергает уже отменённый
// контекст первой же строкой и до фонового согласования не доходит. Проверялся
// ранний отказ, а свойство, ради которого тест написан, не исполнялось ни разу.
// Тот же класс ошибки, что TestAuthzOwnerSelectorIsSelector в Phase 14: тест
// истинный и для своего свойства бесполезный. Ловит его второй подтест.
func TestCancellationAbortsEstablishment(t *testing.T) {
	t.Run("отмена до вызова", func(t *testing.T) {
		a, b := newPeer(t), newPeer(t)
		setup, scancel := context.WithTimeout(context.Background(), 25*time.Second)
		t.Cleanup(scancel)

		offer, err := a.adapter.CreateOffer(setup, b.pair.Public)
		if err != nil {
			t.Fatalf("CreateOffer: %v", err)
		}
		answer, bSess, err := b.adapter.AcceptOffer(setup, a.pair.Public, offer)
		if err != nil {
			t.Fatalf("AcceptOffer: %v", err)
		}
		defer bSess.Close()

		dead, dcancel := context.WithCancel(context.Background())
		dcancel()
		aSess, err := a.adapter.CompleteSession(dead, b.pair.Public, answer)
		if err != nil {
			return // отказ сразу — допустимый исход по §2
		}
		defer aSess.Close()
		if !terminalWithin(aSess, 5*time.Second) {
			t.Fatal("сессия установилась несмотря на отменённый контекст CompleteSession")
		}
	})

	t.Run("отмена после возврата", func(t *testing.T) {
		a, b := newPeer(t), newPeer(t)
		setup, scancel := context.WithTimeout(context.Background(), 25*time.Second)
		t.Cleanup(scancel)

		offer, err := a.adapter.CreateOffer(setup, b.pair.Public)
		if err != nil {
			t.Fatalf("CreateOffer: %v", err)
		}
		// Отвечающая сторона получает offer, но answer НЕ доставляется до
		// конца: согласование инициатора остаётся в работе, и его можно
		// прервать отменой — то есть проверяется фоновый путь, а не ранний
		// отказ.
		answer, bSess, err := b.adapter.AcceptOffer(setup, a.pair.Public, offer)
		if err != nil {
			t.Fatalf("AcceptOffer: %v", err)
		}
		defer bSess.Close()

		completeCtx, completeCancel := context.WithCancel(context.Background())
		aSess, err := a.adapter.CompleteSession(completeCtx, b.pair.Public, answer)
		if err != nil {
			t.Fatalf("CompleteSession: %v", err)
		}
		defer aSess.Close()

		completeCancel() // привычное `defer cancel()`, только явно
		if !terminalWithin(aSess, 5*time.Second) {
			t.Fatal("сессия пережила отмену контекста CompleteSession: согласование не наследует контекст вызывающего")
		}
	})
}

// ─── Время жизни контекста ────────────────────────────────────────────────
//
// Исправление, найденное сетевым прогоном (establish брал context.Background()
// и отмену игнорировал), имеет вторую сторону: раз фоновое согласование живёт
// на контексте вызывающего, то ВРЕМЯ ЖИЗНИ этого контекста стало частью
// контракта. Привычное `defer cancel()` вокруг одного шага убивает
// согласование, которое продолжается после возврата.
//
// Ниже это свойство закрепляется тестами, а не одним комментарием: комментарий
// не падает, когда кто-нибудь вернёт Background обратно.

// newPeerWithTimeout — тот же фикстур, но со своим потолком согласования:
// проверки потолка не должны ждать общие 20 секунд.
func newPeerWithTimeout(t *testing.T, d time.Duration) peerFixture {
	t.Helper()
	id := edadapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	a, err := New(Config{Identity: id, Self: pair, EstablishTimeout: d})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return peerFixture{adapter: a, pair: pair}
}

// terminalWithin возвращает true, если сессия пришла к терминальному исходу
// раньше срока. Ожидание идёт через Peer() — единственный способ узнать исход,
// который даёт порт (ADR-044 §1).
func terminalWithin(sess ports.Session, d time.Duration) bool {
	res := make(chan ports.PublicKey, 1)
	go func() { res <- sess.Peer() }()
	select {
	case p := <-res:
		return p == nil
	case <-time.After(d):
		return false
	}
}

// ADR-044 §2: отмена контекста AcceptOffer переводит уже выданную Session в
// терминальное состояние — в том числе когда отмена случилась ПОСЛЕ возврата
// из вызова. Это и есть настоящая форма находки сетевого прогона: там ctx
// вызывающего истекал, пока согласование шло в фоне.
//
// Срок 5 с против потолка 20 с у фикстуры различает две причины отказа:
// сработала отмена, а не истёк потолок. Без наследования контекста сессия
// продержится до потолка и тест упадёт.
func TestCancelAfterAcceptOfferReturnAbortsNegotiation(t *testing.T) {
	a, b := newPeer(t), newPeer(t)

	setup, scancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(scancel)
	offer, err := a.adapter.CreateOffer(setup, b.pair.Public)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}

	acceptCtx, acceptCancel := context.WithCancel(context.Background())
	_, bSess, err := b.adapter.AcceptOffer(acceptCtx, a.pair.Public, offer)
	if err != nil {
		t.Fatalf("AcceptOffer: %v", err)
	}
	defer bSess.Close()

	// Ровно то, что написал бы вызывающий по привычке: отмена сразу после
	// возврата. Согласование ещё идёт — CompleteSession не вызывался вовсе.
	acceptCancel()

	if !terminalWithin(bSess, 5*time.Second) {
		t.Fatal("сессия пережила отмену контекста AcceptOffer: согласование не наследует контекст вызывающего")
	}
}

// ADR-044 §2 для первого шага: отмена контекста CreateOffer освобождает
// локальные ресурсы попытки. Согласование, начатое отменённым контекстом, не
// может быть завершено позже.
//
// Без сторожа незавершённое согласование живёт до Close() адаптера, и
// CompleteSession принял бы ответ через сколько угодно времени после отмены.
func TestCancelAfterCreateOfferReleasesPendingNegotiation(t *testing.T) {
	a, b := newPeer(t), newPeer(t)

	offerCtx, offerCancel := context.WithCancel(context.Background())
	offer, err := a.adapter.CreateOffer(offerCtx, b.pair.Public)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}

	setup, scancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(scancel)
	answer, bSess, err := b.adapter.AcceptOffer(setup, a.pair.Public, offer)
	if err != nil {
		t.Fatalf("AcceptOffer: %v", err)
	}
	defer bSess.Close()

	offerCancel()

	// Сторож асинхронен: даём ему сработать, но не бесконечно.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err = a.adapter.CompleteSession(setup, b.pair.Public, answer)
		if err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("согласование пережило отмену CreateOffer: CompleteSession прошёл")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !errors.Is(err, ports.ErrSessionFailed) {
		t.Fatalf("ожидалась ErrSessionFailed, получено: %v", err)
	}
}

// ADR-044 §3: потолок обязан существовать НА ВСЁМ согласовании, а не только на
// той его части, что идёт после CompleteSession.
//
// Между CreateOffer и CompleteSession стоит доставка блоба чужим каналом
// (ADR-042), и ответ может не прийти никогда. Без потолка ICE-агент и его
// сокеты жили бы до Close() адаптера — утечка, которую видно не по задержке, а
// по счётчику дескрипторов.
func TestPendingOfferDoesNotOutliveEstablishTimeout(t *testing.T) {
	a := newPeerWithTimeout(t, 700*time.Millisecond)
	b := newPeer(t)

	setup, scancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(scancel)

	offer, err := a.adapter.CreateOffer(setup, b.pair.Public)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	answer, bSess, err := b.adapter.AcceptOffer(setup, a.pair.Public, offer)
	if err != nil {
		t.Fatalf("AcceptOffer: %v", err)
	}
	defer bSess.Close()

	// Ответ пришёл позже потолка — согласования уже нет.
	time.Sleep(1500 * time.Millisecond)

	if _, err := a.adapter.CompleteSession(setup, b.pair.Public, answer); !errors.Is(err, ports.ErrSessionFailed) {
		t.Fatalf("согласование пережило собственный потолок: %v", err)
	}
}

// Шаг handshake обязан подчиняться контексту наравне с ICE и DTLS.
//
// ICE и DTLS принимают ctx сами; чтение из потока — нет, оно блокируется до
// прихода данных. Молчащая сторона (или сторона, которой уже нет) подвесила бы
// согласование навсегда, пройдя мимо обязательного потолка ADR-044 §3.
//
// Проверяется на net.Pipe, а не на живом SCTP: нужен именно молчащий
// собеседник, которого loopback-прогон не даёт.
func TestHandshakeObeysContext(t *testing.T) {
	id := edadapter.New()
	self, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	other, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	for _, tc := range []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
	}{
		{"истёк дедлайн", func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 300*time.Millisecond)
		}},
		{"отмена вызывающим", func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(300*time.Millisecond, cancel)
			return ctx, cancel
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left, right := net.Pipe()
			defer left.Close()
			defer right.Close()
			// right никто не читает и не пишет — собеседник молчит.

			ctx, cancel := tc.ctx()
			defer cancel()

			done := make(chan error, 1)
			go func() {
				_, herr := runHandshake(ctx, left, id, self, other.Public,
					self.Public, other.Public, []byte("n1"), []byte("n2"), []byte("bind"))
				done <- herr
			}()

			select {
			case herr := <-done:
				if !errors.Is(herr, context.DeadlineExceeded) && !errors.Is(herr, context.Canceled) {
					t.Fatalf("ожидалась ошибка контекста, получено: %v", herr)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("handshake не отреагировал на контекст — согласование зависает на молчащем собеседнике")
			}
		})
	}
}

// ─── Relay-путь ───────────────────────────────────────────────────────────
//
// Проверяется ПУТЬ, а не сетевые условия. `ForceRelay` до сих пор был флагом,
// который никто не исполнял: TURN мог быть настроен и ни разу не использован,
// а прогон выглядел бы проверкой relay, не будучи ею (p2p-session.md §10.2).
//
// Что доказывают эти тесты: фильтрация кандидатов работает, аллокация на TURN
// проходит, выбранная пара действительно relay, данные по ней идут.
//
// Чего они НЕ доказывают: обход NAT. TURN-сервер здесь свой, в том же
// процессе, на loopback — сетевых условий тут нет вовсе. Настоящий
// relay-acceptance между двумя хостами через внешний TURN остаётся отдельной
// проверкой со своим критерием, и смешивать их нельзя: это тот же класс
// ошибки, что зелёный тест на фиктивном транспорте в Phase 12.5.

const turnRealm = "photon-test"

// startTURN поднимает TURN-сервер в этом же процессе и возвращает его URL.
func startTURN(t *testing.T, user, pass string) string {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen turn: %v", err)
	}
	srv, err := turn.NewServer(turn.ServerConfig{
		Realm: turnRealm,
		AuthHandler: func(username, realm string, _ net.Addr) ([]byte, bool) {
			if username != user {
				return nil, false
			}
			return turn.GenerateAuthKey(username, realm, pass), true
		},
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn: pc,
			RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{
				RelayAddress: net.ParseIP("127.0.0.1"),
				Address:      "127.0.0.1",
			},
		}},
		LoggerFactory: nopLoggerFactory{},
	})
	if err != nil {
		_ = pc.Close()
		t.Fatalf("turn server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return "turn:" + pc.LocalAddr().String()
}

func newRelayPeer(t *testing.T, turnURL, user, pass string) peerFixture {
	t.Helper()
	id := edadapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	a, err := New(Config{
		Identity: id, Self: pair,
		STUNTURN: []string{turnURL}, TURNUser: user, TURNPass: pass,
		ForceRelay:       true,
		EstablishTimeout: 25 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return peerFixture{adapter: a, pair: pair}
}

func candidatesOf(t *testing.T, raw []byte) []string {
	t.Helper()
	var b struct {
		Candidates []string `json:"candidates"`
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatalf("разбор блоба: %v", err)
	}
	return b.Candidates
}

// ForceRelay обязан исключить host- и srflx-кандидатов ЦЕЛИКОМ, а не понизить
// их приоритет: иначе на loopback победил бы host, и прогон, названный
// проверкой relay, шёл бы мимо TURN.
func TestForceRelayGathersOnlyRelayCandidates(t *testing.T) {
	url := startTURN(t, "photon", "secret")
	a := newRelayPeer(t, url, "photon", "secret")
	b := newRelayPeer(t, url, "photon", "secret")

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)

	offer, err := a.adapter.CreateOffer(ctx, b.pair.Public)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	cands := candidatesOf(t, offer)
	if len(cands) == 0 {
		t.Fatal("ни одного кандидата: аллокация на TURN не состоялась")
	}
	for _, c := range cands {
		if !strings.Contains(c, "typ relay") {
			t.Fatalf("при ForceRelay собран не-relay кандидат: %s", c)
		}
	}
}

// Полный цикл через relay: согласование, подтверждение ключа, выбранная пара,
// данные в обе стороны.
func TestRelayPathCarriesSession(t *testing.T) {
	url := startTURN(t, "photon", "secret")
	a := newRelayPeer(t, url, "photon", "secret")
	b := newRelayPeer(t, url, "photon", "secret")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	offer, err := a.adapter.CreateOffer(ctx, b.pair.Public)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	answer, bSess, err := b.adapter.AcceptOffer(ctx, a.pair.Public, offer)
	if err != nil {
		t.Fatalf("AcceptOffer: %v", err)
	}
	defer bSess.Close()
	aSess, err := a.adapter.CompleteSession(ctx, b.pair.Public, answer)
	if err != nil {
		t.Fatalf("CompleteSession: %v", err)
	}
	defer aSess.Close()

	if p := aSess.Peer(); string(p) != string(b.pair.Public) {
		t.Fatal("Peer() не подтверждён на relay-пути")
	}

	// Выбранная пара обязана быть relay — иначе проверен не тот путь.
	ep := string(aSess.RemoteEndpoint())
	if !strings.Contains(ep, "relay") {
		t.Fatalf("выбранная пара не relay: %s", ep)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		st, err := bSess.AcceptStream(ctx)
		if err != nil {
			t.Errorf("AcceptStream: %v", err)
			return
		}
		buf := make([]byte, 4)
		if _, err := io.ReadFull(st, buf); err != nil {
			t.Errorf("чтение: %v", err)
			return
		}
		if _, err := st.Write([]byte("pong")); err != nil {
			t.Errorf("запись: %v", err)
		}
	}()

	st, err := aSess.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := st.Write([]byte("ping")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(st, buf); err != nil {
		t.Fatalf("чтение эха: %v", err)
	}
	if string(buf) != "pong" {
		t.Fatalf("эхо %q, ожидалось pong", buf)
	}
	<-done
}

// ForceRelay без TURN отвергается на построении, а не оборачивается пустым
// offer'ом и отказом по потолку через десятки секунд.
func TestForceRelayWithoutTURNIsRejected(t *testing.T) {
	id := edadapter.New()
	pair, err := id.Generate(context.Background())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	for _, tc := range []struct {
		name string
		urls []string
	}{
		{"вовсе без адресов", nil},
		{"только STUN", []string{"stun:127.0.0.1:3478"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(Config{
				Identity: id, Self: pair, ForceRelay: true, STUNTURN: tc.urls,
			}); err == nil {
				t.Fatal("ForceRelay без TURN принят")
			}
		})
	}
}

// Неверные учётные данные TURN не дают аллокации. Отказ обязан прийти сразу и
// назвать причину — «ноль кандидатов», — а не выглядеть медленной сетью.
func TestRelayWithBadCredentialsFailsFast(t *testing.T) {
	url := startTURN(t, "photon", "secret")
	a := newRelayPeer(t, url, "photon", "wrong-password")
	b := newRelayPeer(t, url, "photon", "secret")

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)

	start := time.Now()
	_, err := a.adapter.CreateOffer(ctx, b.pair.Public)
	if err == nil {
		t.Fatal("offer построен без единого relay-кандидата")
	}
	if !errors.Is(err, ports.ErrSessionFailed) {
		t.Fatalf("ожидалась ErrSessionFailed, получено: %v", err)
	}
	if d := time.Since(start); d > 20*time.Second {
		t.Fatalf("отказ занял %s — неотличимо от потолка согласования", d)
	}
}

// Ни один слой Pion не должен писать в stderr вызывающего.
//
// Проверяется СТРУКТУРНО — по конфигурации каждого слоя, а не по перехвату
// stderr во время прогона. Поведенческий вариант был бы зелёным всегда:
// успешное согласование не пишет ничего и при вернувшемся умолчании, потому
// что DefaultLoggerFactory выводит только уровень Error. Тест, проходящий на
// мутации, хуже отсутствующего — а именно этой пропажи и не заметило ревью:
// до аудита nopLoggerFactory стоял на одном слое из трёх.
func TestEveryPionLayerGetsSilentLogger(t *testing.T) {
	a := newPeer(t)

	acfg, err := a.adapter.agentConfig()
	if err != nil {
		t.Fatalf("agentConfig: %v", err)
	}
	if _, ok := acfg.LoggerFactory.(nopLoggerFactory); !ok {
		t.Fatalf("ICE: LoggerFactory = %T, ожидался nopLoggerFactory — Pion будет писать в stderr", acfg.LoggerFactory)
	}

	dcfg := a.adapter.dtlsConfig()
	if _, ok := dcfg.LoggerFactory.(nopLoggerFactory); !ok {
		t.Fatalf("DTLS: LoggerFactory = %T, ожидался nopLoggerFactory", dcfg.LoggerFactory)
	}

	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	scfg := sctpConfig(left)
	if _, ok := scfg.LoggerFactory.(nopLoggerFactory); !ok {
		t.Fatalf("SCTP: LoggerFactory = %T, ожидался nopLoggerFactory", scfg.LoggerFactory)
	}
}
