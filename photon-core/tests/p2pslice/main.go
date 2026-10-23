// p2pslice — одна сторона вертикального среза «Photon A ↔ Photon B».
//
// Запускается ДВУМЯ настоящими процессами: slice_test.go поднимает
// `-mode responder` и `-mode initiator` и сверяет их вывод. Два объекта в
// одном процессе заменой не являются — прямой урок Phase 11, где пять
// плагинов на настоящих процессах нашли три бага, невидимых для fakeSpawner и
// net.Pipe, и Phase 12.5, где зелёный код на фиктивном транспорте сломался на
// первом живом прогоне.
//
// Signaling здесь — TCP-рандеву на loopback. Это часть HARNESS'А, не
// платформы: ADR-042 §4 и ADR-075 §2.13 прямо запрещают прямой доставке
// блобов «стать заготовкой будущего механизма». Та же по роли иллюстрация,
// что рандеву в tests/p2pcheck и служебные посты в tests/eventcheck.
//
// Рандеву используется и как БАРЬЕР между шагами сценария. Это законно: канал
// вне P2P-сессии, он переживает её разрыв, и именно поэтому им можно
// синхронизировать наблюдение разрыва, не гадая про тайминги.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	edadapter "github.com/wiloris/photon-core/adapters/ed25519"
	pionadapter "github.com/wiloris/photon-core/adapters/pion"
	"github.com/wiloris/photon-core/p2pmsg"
	"github.com/wiloris/photon-core/ports"
)

var (
	mode     = flag.String("mode", "", "responder | initiator")
	signal   = flag.String("signal", "127.0.0.1:0", "TCP-рандеву: responder слушает и печатает адрес, initiator подключается")
	scenario = flag.String("scenario", "echo", "имя сценария")
	timeout  = flag.Duration("timeout", 90*time.Second, "потолок всего прогона")
	rttIters = flag.Int("rtt-iters", 20, "число проходов на размер в сценарии measure")
)

var failures int

func ok(format string, a ...any)   { fmt.Printf("OK: "+format+"\n", a...) }
func fail(format string, a ...any) { fmt.Printf("FAIL: "+format+"\n", a...); failures++ }

// measure — числа печатаются, а не оцениваются; разбирает их slice_test.go.
func measure(key string, v any) { fmt.Printf("MEASURE %s %v\n", key, v) }

func main() {
	flag.Parse()
	if *mode != "responder" && *mode != "initiator" {
		fmt.Fprintln(os.Stderr, "нужен -mode responder|initiator")
		os.Exit(2)
	}
	sc, found := scenarios[*scenario]
	if !found {
		fmt.Fprintf(os.Stderr, "неизвестный сценарий %q\n", *scenario)
		os.Exit(2)
	}

	// ОДИН контекст на всё согласование, живущий до его исхода (ADR-044 §6.1,
	// p2p-session.md §6.1). Привычное `defer cancel()` вокруг одного шага
	// убило бы сессию сразу после возврата из вызова.
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := run(ctx, sc); err != nil {
		fail("%v", err)
	}
	if failures > 0 {
		fmt.Printf("RESULT FAIL %d\n", failures)
		os.Exit(1)
	}
	fmt.Println("RESULT PASS")
}

func run(ctx context.Context, sc scenario_) error {
	id := edadapter.New()
	self, err := id.Generate(ctx)
	if err != nil {
		return fmt.Errorf("Generate: %w", err)
	}
	adapter, err := pionadapter.New(pionadapter.Config{
		Identity: id, Self: self,
		// Ни STUN, ни TURN: на loopback достаточно host-кандидатов, а
		// внешних зависимостей у детерминированного теста быть не должно.
		EstablishTimeout: *timeout,
	})
	if err != nil {
		return fmt.Errorf("pion.New: %w", err)
	}
	defer adapter.Close()

	rv, err := rendezvous()
	if err != nil {
		return err
	}
	defer rv.Close()

	peerKey, err := rv.exchangeKeys(self.Public)
	if err != nil {
		return fmt.Errorf("обмен ключами: %w", err)
	}

	// Ожидание, передаваемое в CreateOffer/AcceptOffer, — НАМЕРЕНИЕ, а не
	// факт (p2p-session.md §3). Сценарий wrong-peer портит именно его.
	expected := peerKey
	if sc.corruptPeerKey && *mode == "initiator" {
		expected = flip(peerKey)
		ok("ожидание испорчено: подставлен чужой ключ %s", short(expected))
	}

	start := time.Now()
	sess, negErr := negotiate(ctx, adapter, rv, expected)

	if sc.expectNegotiationFailure {
		return checkNegotiationRejected(sess, negErr)
	}
	if negErr != nil {
		return fmt.Errorf("согласование: %w", negErr)
	}
	confirmed := sess.Peer()
	if confirmed == nil {
		return errors.New("handshake не подтвердил ключ: Peer() пуст")
	}
	if !bytes.Equal(confirmed, peerKey) {
		return fmt.Errorf("Peer() вернул %s, ожидался %s", short(confirmed), short(peerKey))
	}
	measure("establish_ms", time.Since(start).Milliseconds())
	ok("сессия установлена, Peer() подтверждён: %s (%s)", short(confirmed), sess.RemoteEndpoint())

	// Поток разговора: инициатор открывает, отвечающий принимает
	// (ADR-075 §2.3 — один bidi-поток на разговор).
	var st ports.Stream
	if *mode == "initiator" {
		st, err = sess.OpenStream(ctx)
	} else {
		st, err = sess.AcceptStream(ctx)
	}
	if err != nil {
		return fmt.Errorf("поток разговора: %w", err)
	}

	env := &env{ctx: ctx, sess: sess, st: st, conv: p2pmsg.New(st), rv: rv}

	// Барьер сразу после получения потока. Без него сценарий, рвущий сессию
	// первым же действием, гоняется с доставкой самого потока: AcceptStream у
	// отвечающего возвращается только после того, как инициатор напишет
	// признак направления, а тот может не успеть дойти до разрыва. Найдено
	// прогоном: disconnect-midframe падал на «поток разговора: context
	// deadline exceeded», то есть до единой проверки сценария.
	if *mode == "initiator" {
		if err := rv.awaitSync(); err != nil {
			return fmt.Errorf("барьер потока: %w", err)
		}
		return sc.initiator(env)
	}
	if err := rv.sendSync(); err != nil {
		return fmt.Errorf("барьер потока: %w", err)
	}
	return sc.responder(env)
}

// checkNegotiationRejected — общая проверка для сценариев, где согласование
// ОБЯЗАНО провалиться. Отказ может прийти двумя путями, и оба законны: ошибкой
// вызова либо пустым Peer() у выданной, но не установившейся сессии.
func checkNegotiationRejected(sess ports.Session, negErr error) error {
	switch {
	case negErr != nil:
		ok("согласование отвергнуто: %v", negErr)
	case sess == nil:
		return errors.New("ни сессии, ни ошибки")
	case sess.Peer() == nil:
		ok("Peer() пуст — handshake отверг ключ, сессия терминальна")
	default:
		return errors.New("согласование СОСТОЯЛОСЬ с чужим ключом — подмена не отвергнута")
	}
	return nil
}

// negotiate выполняет три шага порта. Блобы ходят через рандеву.
func negotiate(ctx context.Context, a *pionadapter.Adapter, rv *rv, expected ports.PublicKey) (ports.Session, error) {
	if *mode == "initiator" {
		offer, err := a.CreateOffer(ctx, expected)
		if err != nil {
			return nil, fmt.Errorf("CreateOffer: %w", err)
		}
		if err := rv.sendBlob(offer); err != nil {
			return nil, err
		}
		answer, err := rv.recvBlob()
		if err != nil {
			return nil, err
		}
		sess, err := a.CompleteSession(ctx, expected, answer)
		if err != nil {
			return nil, fmt.Errorf("CompleteSession: %w", err)
		}
		return sess, nil
	}
	offer, err := rv.recvBlob()
	if err != nil {
		return nil, err
	}
	answer, sess, err := a.AcceptOffer(ctx, expected, offer)
	if err != nil {
		return nil, fmt.Errorf("AcceptOffer: %w", err)
	}
	if err := rv.sendBlob(answer); err != nil {
		return nil, err
	}
	return sess, nil
}

// ───────────────────────── сценарии ─────────────────────────

type env struct {
	ctx  context.Context
	sess ports.Session
	st   ports.Stream
	conv *p2pmsg.Conversation
	rv   *rv
}

type scenario_ struct {
	initiator                func(*env) error
	responder                func(*env) error
	corruptPeerKey           bool
	expectNegotiationFailure bool
}

var scenarios map[string]scenario_

func init() {
	// Карта объявляется в init, потому что сценарии ссылаются на общие
	// хелперы, объявленные ниже, — прямая инициализация дала бы цикл.
	scenarios = map[string]scenario_{
		"echo":                {initiator: echoInitiator, responder: echoResponder},
		"zero":                {initiator: zeroInitiator, responder: zeroResponder},
		"max":                 {initiator: maxInitiator, responder: maxResponder},
		"oversize-local":      {initiator: oversizeLocalInitiator, responder: oversizeLocalResponder},
		"oversize-wire":       {initiator: oversizeWireInitiator, responder: oversizeWireResponder},
		"short-length":        {initiator: shortLengthInitiator, responder: brokenFrameResponder},
		"truncated":           {initiator: truncatedInitiator, responder: brokenFrameResponder},
		"disconnect-midframe": {initiator: midframeDisconnectInitiator, responder: midframeDisconnectResponder},
		"clean-shutdown":      {initiator: cleanShutdownInitiator, responder: cleanShutdownResponder},
		"repeated":            {initiator: repeatedInitiator, responder: repeatedResponder},
		"binary":              {initiator: binaryInitiator, responder: binaryResponder},
		"measure":             {initiator: measureInitiator, responder: echoBackResponder},
		"wrong-peer": {
			initiator: unreachable, responder: unreachable,
			corruptPeerKey: true, expectNegotiationFailure: true,
		},
	}
}

func unreachable(*env) error {
	return errors.New("сценарий не должен доходить до потока")
}

// echo — основной acceptance. Проверяется ПОБАЙТОВОЕ совпадение, а не
// «содержит» и не длина.
func echoInitiator(e *env) error {
	if err := e.conv.Send([]byte(msgA)); err != nil {
		return fmt.Errorf("Send: %w", err)
	}
	ok("A отправил %q", msgA)

	got, err := e.conv.Recv()
	if err != nil {
		return fmt.Errorf("Recv ответа: %w", err)
	}
	if !bytes.Equal(got, []byte(msgB)) {
		return fmt.Errorf("A получил % x, ожидалось % x", got, msgB)
	}
	ok("A получил ответ побайтово: %q", got)

	// Разрыв инициирует A закрытием СЕССИИ (ADR-075 §2.10).
	if err := e.sess.Close(); err != nil {
		return fmt.Errorf("Session.Close: %w", err)
	}
	ok("A закрыл сессию")
	return e.rv.awaitSync()
}

func echoResponder(e *env) error {
	got, err := e.conv.Recv()
	if err != nil {
		return fmt.Errorf("Recv: %w", err)
	}
	if !bytes.Equal(got, []byte(msgA)) {
		return fmt.Errorf("B получил % x, ожидалось % x", got, msgA)
	}
	ok("B получил побайтово: %q", got)

	if err := e.conv.Send([]byte(msgB)); err != nil {
		return fmt.Errorf("Send ответа: %w", err)
	}
	ok("B ответил %q", msgB)

	// Разрыв наблюдается СУЩЕСТВУЮЩИМИ средствами: ошибкой на следующей
	// операции. Нового колбэка в замороженных портах не появляется.
	//
	// ЧТО именно наблюдаемо — измеряется, а не предсказывается. Первая
	// редакция спецификации утверждала, что заблокированный AcceptStream
	// вернёт ports.ErrSessionFailed; прогон показал, что при ШТАТНОМ закрытии
	// он не возвращается вовсе и доходит до истечения контекста. Утверждение
	// исправлено по измерению, а не измерение подогнано под утверждение.
	msg, rerr := e.conv.Recv()
	if rerr == nil {
		return fmt.Errorf("после закрытия сессии Recv вернул %d байт", len(msg))
	}
	ok("B заметил разрыв на Recv: %v", rerr)

	// Вторая проба — с КОРОТКИМ сроком: длинный превратил бы «сессия молчит о
	// разрыве» в зависание до потолка всего прогона.
	probe, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	if _, oerr := e.sess.OpenStream(probe); oerr == nil {
		return errors.New("после разрыва сессия открыла новый поток")
	} else {
		ok("B: проба OpenStream после разрыва: %v", oerr)
	}
	return e.rv.sendSync()
}

func zeroInitiator(e *env) error {
	if err := e.conv.Send(nil); err != nil {
		return fmt.Errorf("Send пустого: %w", err)
	}
	if err := e.conv.Send([]byte("after zero")); err != nil {
		return fmt.Errorf("Send следующего: %w", err)
	}
	ok("A отправил пустое сообщение и следом непустое")
	return expectStatus(e)
}

func zeroResponder(e *env) error {
	first, err := e.conv.Recv()
	if err != nil {
		return fmt.Errorf("Recv пустого: %w", err)
	}
	if len(first) != 0 {
		return fmt.Errorf("длина %d, ожидалась 0", len(first))
	}
	// Пустое сообщение обязано отличаться от конца потока без обращения к
	// ошибке — иначе «пришло пустое» неотличимо от «всё кончилось».
	if first == nil {
		return errors.New("пустое сообщение пришло как nil — неотличимо от отказа")
	}
	ok("B принял пустое сообщение, отличимое от конца потока")

	second, err := e.conv.Recv()
	if err != nil {
		return fmt.Errorf("Recv следующего: %w", err)
	}
	if string(second) != "after zero" {
		return fmt.Errorf("после пустого пришло %q", second)
	}
	ok("B принял следующее сообщение — граница кадра не сбилась")
	return sendStatus(e)
}

func maxInitiator(e *env) error {
	if err := e.conv.Send(pattern(p2pmsg.MaxMessageSize)); err != nil {
		return fmt.Errorf("Send ровно MaxMessageSize: %w", err)
	}
	ok("A отправил ровно %d байт", p2pmsg.MaxMessageSize)
	return expectStatus(e)
}

func maxResponder(e *env) error {
	got, err := e.conv.Recv()
	if err != nil {
		return fmt.Errorf("Recv: %w", err)
	}
	want := pattern(p2pmsg.MaxMessageSize)
	if !bytes.Equal(got, want) {
		_ = e.conv.Send([]byte("mismatch"))
		return fmt.Errorf("получено %d байт, ожидалось %d; побайтово не совпало", len(got), len(want))
	}
	ok("B принял %d байт побайтово", len(got))
	return sendStatus(e)
}

// oversize-local: отказ по размеру наступает ДО записи и разговор не ломает.
// Доказывается тем, что следующее годное сообщение доходит неиспорченным.
func oversizeLocalInitiator(e *env) error {
	err := e.conv.Send(make([]byte, p2pmsg.MaxMessageSize+1))
	if !errors.Is(err, p2pmsg.ErrMessageTooLarge) {
		return fmt.Errorf("Send сверх предела дал %v, ожидалась ErrMessageTooLarge", err)
	}
	ok("A: 1 MiB + 1 отвергнут локально: %v", err)

	if err := e.conv.Send(pattern(1000)); err != nil {
		return fmt.Errorf("после локального отказа разговор непригоден: %w", err)
	}
	ok("A: следующее годное сообщение отправлено")
	return expectStatus(e)
}

func oversizeLocalResponder(e *env) error {
	got, err := e.conv.Recv()
	if err != nil {
		return fmt.Errorf("Recv: %w", err)
	}
	// Если бы локальный отказ успел записать хоть байт, здесь пришёл бы мусор.
	if !bytes.Equal(got, pattern(1000)) {
		_ = e.conv.Send([]byte("mismatch"))
		return fmt.Errorf("пришло %d байт, не совпало побайтово — локальный отказ тронул провод", len(got))
	}
	ok("B принял ровно годное сообщение — на провод отвергнутое не ушло")
	return sendStatus(e)
}

// oversize-wire: заголовок с длиной сверх предела кладётся на провод НАПРЯМУЮ,
// минуя Send, — корректный отправитель такого не произвёл бы никогда.
func oversizeWireInitiator(e *env) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], p2pmsg.MaxMessageSize+1)
	if _, err := e.st.Write(hdr[:]); err != nil {
		return fmt.Errorf("запись негодного заголовка: %w", err)
	}
	ok("A положил на провод заголовок длиной %d", p2pmsg.MaxMessageSize+1)
	return e.rv.awaitSync()
}

func oversizeWireResponder(e *env) error {
	_, err := e.conv.Recv()
	if !errors.Is(err, p2pmsg.ErrMessageTooLarge) {
		return fmt.Errorf("Recv дал %v, ожидалась ErrMessageTooLarge", err)
	}
	ok("B отверг негодный заголовок: %v", err)
	if err := assertConversationTerminal(e.conv); err != nil {
		return err
	}
	return e.rv.sendSync()
}

func shortLengthInitiator(e *env) error {
	if _, err := e.st.Write([]byte{0x00, 0x00}); err != nil {
		return fmt.Errorf("запись обрывка заголовка: %w", err)
	}
	// Половинное завершение отправки: получатель увидит конец данных после
	// двух байт, а не разрыв сессии.
	if err := e.conv.Close(); err != nil {
		return fmt.Errorf("Conversation.Close: %w", err)
	}
	ok("A отправил 2 байта заголовка и завершил отправку")
	return e.rv.awaitSync()
}

func truncatedInitiator(e *env) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 64)
	if _, err := e.st.Write(append(hdr[:], pattern(32)...)); err != nil {
		return fmt.Errorf("запись обрезанного кадра: %w", err)
	}
	if err := e.conv.Close(); err != nil {
		return fmt.Errorf("Conversation.Close: %w", err)
	}
	ok("A объявил 64 байта, отправил 32 и завершил отправку")
	return e.rv.awaitSync()
}

// brokenFrameResponder — общий получатель для short-length и truncated:
// обрыв кадрирования обязан дать io.ErrUnexpectedEOF и НЕ io.EOF, иначе
// обрыв посреди кадра выдаётся за корректный конец потока.
func brokenFrameResponder(e *env) error {
	_, err := e.conv.Recv()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("Recv дал %v, ожидался io.ErrUnexpectedEOF", err)
	}
	if errors.Is(err, io.EOF) {
		return errors.New("обрыв кадра выдан за чистый конец потока")
	}
	ok("B отверг оборванный кадр: %v", err)
	if err := assertConversationTerminal(e.conv); err != nil {
		return err
	}
	return e.rv.sendSync()
}

func midframeDisconnectInitiator(e *env) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 64)
	if _, err := e.st.Write(append(hdr[:], pattern(32)...)); err != nil {
		return fmt.Errorf("запись половины кадра: %w", err)
	}
	if err := e.sess.Close(); err != nil {
		return fmt.Errorf("Session.Close: %w", err)
	}
	ok("A оборвал сессию посреди кадра")
	return e.rv.awaitSync()
}

func midframeDisconnectResponder(e *env) error {
	msg, err := e.conv.Recv()
	if err == nil {
		return fmt.Errorf("Recv вернул %d байт при обрыве посреди кадра", len(msg))
	}
	// Конкретное значение не предписывается, и это не слабость проверки, а
	// измеренный факт: аварийный обрыв отбрасывает данные, ещё не ушедшие на
	// провод (ADR-075 §2.9), поэтому B видит либо оборванный кадр, либо конец
	// потока — в зависимости от того, что успело дойти. Инвариант, который
	// проверяется жёстко и всегда: B НИКОГДА не получает сообщение, которого A
	// не дописал.
	ok("B заметил обрыв посреди кадра: %v", err)
	if err := assertRecvSticky(e.conv); err != nil {
		return err
	}
	return e.rv.sendSync()
}

// clean-shutdown проверяет half-close: завершение ОТПРАВКИ не закрывает приём.
// Это свойство было заявлено p2p-session.md §7 и до сих пор не покрыто ни
// одним тестом (§10.1, «Не покрыто тестами»).
func cleanShutdownInitiator(e *env) error {
	if err := e.conv.Send([]byte("bye")); err != nil {
		return fmt.Errorf("Send: %w", err)
	}
	if err := e.conv.Close(); err != nil {
		return fmt.Errorf("Conversation.Close: %w", err)
	}
	ok("A отправил последнее сообщение и завершил отправку (half-close)")

	got, err := e.conv.Recv()
	if err != nil {
		return fmt.Errorf("приём после завершения СВОЕЙ отправки не работает: %w", err)
	}
	if string(got) != "ack" {
		return fmt.Errorf("получено %q, ожидалось \"ack\"", got)
	}
	ok("A принял ответ ПОСЛЕ своего half-close: %q", got)

	// Барьер — ПОСЛЕ приёма и в эту сторону: B не завершается, пока A не
	// подтвердил, что "ack" дошёл. Прежний порядок (B слал барьер сразу за
	// Send и выходил) терял "ack" в очереди отправки SCTP умирающего
	// процесса, и A ждал его вечно — зависание, по разбору которого найден
	// дефект адаптера (потеря собеседника не доходила до сессии).
	if err := e.rv.sendSync(); err != nil {
		return fmt.Errorf("барьер приёма: %w", err)
	}
	if err := e.sess.Close(); err != nil {
		return fmt.Errorf("Session.Close: %w", err)
	}
	ok("A закрыл сессию штатно")
	return nil
}

func cleanShutdownResponder(e *env) error {
	got, err := e.conv.Recv()
	if err != nil {
		return fmt.Errorf("Recv: %w", err)
	}
	if string(got) != "bye" {
		return fmt.Errorf("получено %q", got)
	}
	if _, err := e.conv.Recv(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("после half-close собеседника Recv дал %v, ожидался io.EOF", err)
	}
	ok("B увидел чистый конец приёма: io.EOF ровно на границе кадра")

	// io.EOF приёма НЕ запрещает отправку — иначе ответить на последнее
	// сообщение было бы нечем (ADR-075 §2.10).
	if err := e.conv.Send([]byte("ack")); err != nil {
		return fmt.Errorf("после io.EOF отправка запрещена, а не должна: %w", err)
	}
	ok("B ответил после io.EOF — приём и отправка независимы")
	return e.rv.awaitSync()
}

func repeatedInitiator(e *env) error {
	for i := 0; i < repeatN; i++ {
		if err := e.conv.Send(pattern(i * 37)); err != nil {
			return fmt.Errorf("Send #%d: %w", i, err)
		}
	}
	ok("A отправил %d сообщений подряд", repeatN)
	return expectStatus(e)
}

func repeatedResponder(e *env) error {
	for i := 0; i < repeatN; i++ {
		got, err := e.conv.Recv()
		if err != nil {
			return fmt.Errorf("Recv #%d: %w", i, err)
		}
		if !bytes.Equal(got, pattern(i*37)) {
			_ = e.conv.Send([]byte("mismatch"))
			return fmt.Errorf("сообщение #%d не совпало: %d байт вместо %d", i, len(got), i*37)
		}
	}
	ok("B принял %d сообщений: порядок и содержимое побайтово", repeatN)
	return sendStatus(e)
}

func binaryInitiator(e *env) error {
	for _, p := range binaryPayloads() {
		if err := e.conv.Send(p); err != nil {
			return fmt.Errorf("Send двоичной нагрузки: %w", err)
		}
	}
	ok("A отправил %d двоичных нагрузок (нули, невалидный UTF-8, все 256 значений)", len(binaryPayloads()))
	return expectStatus(e)
}

func binaryResponder(e *env) error {
	for i, want := range binaryPayloads() {
		got, err := e.conv.Recv()
		if err != nil {
			return fmt.Errorf("Recv #%d: %w", i, err)
		}
		if !bytes.Equal(got, want) {
			_ = e.conv.Send([]byte("mismatch"))
			return fmt.Errorf("двоичная нагрузка #%d искажена: % x против % x", i, got, want)
		}
	}
	ok("B принял двоичные нагрузки побайтово — Photon их не интерпретирует")
	return sendStatus(e)
}

func measureInitiator(e *env) error {
	for _, size := range measureSizes {
		var total time.Duration
		for i := 0; i < *rttIters; i++ {
			payload := pattern(size)
			t0 := time.Now()
			if err := e.conv.Send(payload); err != nil {
				return fmt.Errorf("Send %d: %w", size, err)
			}
			got, err := e.conv.Recv()
			if err != nil {
				return fmt.Errorf("Recv %d: %w", size, err)
			}
			total += time.Since(t0)
			if !bytes.Equal(got, payload) {
				return fmt.Errorf("эхо %d байт не совпало", size)
			}
		}
		measure(fmt.Sprintf("rtt_us_payload_%d", size), total.Microseconds()/int64(*rttIters))
		measure(fmt.Sprintf("frame_bytes_payload_%d", size), size+4)
	}
	ok("замеры сняты на %d размерах по %d проходов", len(measureSizes), *rttIters)
	if err := e.conv.Send([]byte(statusDone)); err != nil {
		return err
	}
	return e.rv.awaitSync()
}

// echoBackResponder отражает всё, что приходит, пока не придёт признак конца.
func echoBackResponder(e *env) error {
	for {
		got, err := e.conv.Recv()
		if err != nil {
			return fmt.Errorf("Recv: %w", err)
		}
		if string(got) == statusDone {
			ok("B отразил весь поток замеров")
			return e.rv.sendSync()
		}
		if err := e.conv.Send(got); err != nil {
			return fmt.Errorf("эхо: %w", err)
		}
	}
}

// ───────────────────────── общее ─────────────────────────

const (
	msgA       = "hello from A"
	msgB       = "hello from B"
	statusOK   = "ok"
	statusDone = "done"
	repeatN    = 100
)

var measureSizes = []int{0, 64, 1024, 65536, 1 << 20}

func binaryPayloads() [][]byte {
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	return [][]byte{
		{'a', 0x00, 0x00, 'b', 0x00},
		{0xff, 0xfe, 0xc0, 0x80, 0xed, 0xa0, 0x80},
		make([]byte, 64),
		all,
	}
}

// pattern — детерминированная нагрузка, одинаково вычисляемая обоими
// процессами. Не нули и не текст: нулевая нагрузка прошла бы проверку, даже
// если байты потеряны и заменены нулями.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251 + 1)
	}
	return b
}

func expectStatus(e *env) error {
	got, err := e.conv.Recv()
	if err != nil {
		return fmt.Errorf("Recv подтверждения: %w", err)
	}
	if string(got) != statusOK {
		return fmt.Errorf("отвечающий сообщил %q", got)
	}
	ok("A получил подтверждение от B")
	return e.rv.sendSync()
}

// sendStatus — ответ отвечающего плюс ОЖИДАНИЕ того, что он дошёл.
//
// Выйти сразу после Send нельзя: возврат без ошибки означает, что кадр принят
// транспортом, а НЕ что он доставлен (ADR-075 §2.9). Прогон это и показал —
// отвечающий печатал успех и выходил, а инициатор ждал подтверждения до
// потолка. Сам контракт при этом верен; неверно было ожидание harness'а.
func sendStatus(e *env) error {
	if err := e.conv.Send([]byte(statusOK)); err != nil {
		return err
	}
	return e.rv.awaitSync()
}

// assertConversationTerminal — необратимость терминального отказа: повтор даёт
// ту же ошибку, а не новое чтение потока.
func assertConversationTerminal(c *p2pmsg.Conversation) error {
	_, first := c.Recv()
	_, again := c.Recv()
	if first == nil || again == nil || first.Error() != again.Error() {
		return fmt.Errorf("терминальный отказ не необратим: %v затем %v", first, again)
	}
	if err := c.Send([]byte("x")); err == nil || err.Error() != first.Error() {
		return fmt.Errorf("терминальный отказ не коснулся отправки: %v", err)
	}
	return nil
}

// assertRecvSticky — приём не «чинится» после отказа: повтор даёт ту же
// ошибку и ни разу не выдаёт сообщение.
//
// Отправку не проверяет намеренно: чистый конец ПРИЁМА (io.EOF) по контракту
// отправку не запрещает — иначе ответить на последнее сообщение было бы нечем.
func assertRecvSticky(c *p2pmsg.Conversation) error {
	_, first := c.Recv()
	msg, again := c.Recv()
	if first == nil || again == nil {
		return fmt.Errorf("после отказа Recv выдал сообщение (%d байт)", len(msg))
	}
	if first.Error() != again.Error() {
		return fmt.Errorf("повторный Recv дал другую ошибку: %v затем %v", first, again)
	}
	return nil
}

func flip(k ports.PublicKey) ports.PublicKey {
	out := make(ports.PublicKey, len(k))
	copy(out, k)
	out[0] ^= 0xff
	return out
}

func short(k []byte) string {
	s := hex.EncodeToString(k)
	if len(s) > 16 {
		return s[:16] + "…"
	}
	return s
}

// ───────────────────────── рандеву ─────────────────────────

type rv struct {
	conn net.Conn
	enc  *json.Encoder
	dec  *json.Decoder
	ln   net.Listener
}

type wire struct {
	PublicKey []byte `json:"pubkey,omitempty"`
	Blob      []byte `json:"blob,omitempty"`
	Sync      bool   `json:"sync,omitempty"`
}

func rendezvous() (*rv, error) {
	if *mode == "responder" {
		ln, err := net.Listen("tcp", *signal)
		if err != nil {
			return nil, fmt.Errorf("listen: %w", err)
		}
		// Адрес печатается ПОСЛЕ успешного Listen: порт выбирает ядро (":0"),
		// поэтому фиксированного номера нет и коллизий между параллельными
		// прогонами тоже.
		fmt.Printf("SIGNAL %s\n", ln.Addr().String())
		conn, err := ln.Accept()
		if err != nil {
			_ = ln.Close()
			return nil, fmt.Errorf("accept: %w", err)
		}
		return &rv{conn: conn, enc: json.NewEncoder(conn), dec: json.NewDecoder(conn), ln: ln}, nil
	}
	conn, err := net.DialTimeout("tcp", *signal, 20*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial рандеву: %w", err)
	}
	return &rv{conn: conn, enc: json.NewEncoder(conn), dec: json.NewDecoder(conn)}, nil
}

func (r *rv) Close() {
	_ = r.conn.Close()
	if r.ln != nil {
		_ = r.ln.Close()
	}
}

// exchangeKeys — discovery, лежащий ВНЕ P2P-порта: порт требует знать ключ
// peer'а уже в CreateOffer (ports/p2p.go, ADR-017/018).
func (r *rv) exchangeKeys(self ports.PublicKey) (ports.PublicKey, error) {
	if err := r.enc.Encode(wire{PublicKey: self}); err != nil {
		return nil, err
	}
	var in wire
	if err := r.dec.Decode(&in); err != nil {
		return nil, err
	}
	if len(in.PublicKey) == 0 {
		return nil, errors.New("пустой ключ от собеседника")
	}
	return ports.PublicKey(in.PublicKey), nil
}

func (r *rv) sendBlob(b []byte) error { return r.enc.Encode(wire{Blob: b}) }

func (r *rv) recvBlob() ([]byte, error) {
	var in wire
	if err := r.dec.Decode(&in); err != nil {
		return nil, err
	}
	if len(in.Blob) == 0 {
		return nil, errors.New("пустой блоб")
	}
	return in.Blob, nil
}

// sendSync / awaitSync — барьер ВНЕ P2P-сессии. Именно поэтому им можно
// синхронизировать наблюдение разрыва: канал переживает разрыв сессии, и
// ожидание не превращается в гадание про тайминги.
func (r *rv) sendSync() error { return r.enc.Encode(wire{Sync: true}) }

func (r *rv) awaitSync() error {
	var in wire
	if err := r.dec.Decode(&in); err != nil {
		return fmt.Errorf("ожидание барьера: %w", err)
	}
	if !in.Sync {
		return errors.New("на барьере пришло не то")
	}
	return nil
}
