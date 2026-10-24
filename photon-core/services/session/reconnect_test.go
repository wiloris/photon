package session

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/wiloris/photon/photon-core/ports"
)

// Время жизни соединения при ПОВТОРНОМ Connect.
//
// Дефект: Connect на сессии, потерявшей связь, присваивал s.conn новое
// соединение, не закрывая прежнее. Прежнее оставалось незакрытым и
// недостижимым — Close закрывает только текущее. Для QUIC с включённым
// keep-alive (dispatcher задаёт его по умолчанию) idle-timeout такое
// соединение не уберёт, пока жив сервер, поэтому каждый ленивый реконнект
// action-пути оставлял бы за собой живой сокет.
//
// Насос подписки дефекта не имел: он закрывает оборванную сессию и создаёт
// НОВУЮ (pump.go, «оборванная сессия НЕ переиспользуется»). Ленивый путь
// действий переиспользует ту же сессию — отсюда разница.
//
// Тесты держат четыре свойства: старое закрывается ровно один раз, новое не
// теряется, неудачная замена не трогает действующее соединение, серия
// реконнектов не оставляет висящих соединений.

// handshakeConn — соединение, отвечающее на handshake успехом.
func handshakeConn(t *testing.T) *fakeConn {
	t.Helper()
	return newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
		defer srv.Close()
		var auth, mode map[string]interface{}
		if err := readFrames(t, srv, &auth, &mode); err != nil {
			return
		}
		writeJSON(t, srv, map[string]interface{}{"ok": true})
	})
}

// queueTransport выдаёт соединения по очереди: первый Connect — первое,
// второй — второе и так далее. Одно и то же значение conn не позволило бы
// отличить «старое закрыли» от «новое не открыли».
func queueTransport(conns ...*fakeConn) *fakeTransport {
	var mu sync.Mutex
	i := 0
	tr := &fakeTransport{}
	tr.next = func() (ports.Connection, error) {
		mu.Lock()
		defer mu.Unlock()
		if i >= len(conns) {
			return nil, errors.New("fake: соединения кончились")
		}
		c := conns[i]
		i++
		return c, nil
	}
	return tr
}

func connectedSession(t *testing.T, tr *fakeTransport) *Session {
	t.Helper()
	s, err := New(Config{Transport: tr, Endpoint: "e", SessionToken: "tok", Mode: ModeAction})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("первый Connect: %v", err)
	}
	return s
}

// Повторный Connect после обрыва закрывает прежнее соединение ровно один раз
// и оставляет текущим новое.
func TestReconnectClosesPreviousConnection(t *testing.T) {
	first, second := handshakeConn(t), handshakeConn(t)
	tr := queueTransport(first, second)
	s := connectedSession(t, tr)
	defer s.Close()

	if got := first.closeCount(); got != 0 {
		t.Fatalf("первое соединение закрыто до обрыва (%d)", got)
	}

	// Обрыв: следующий вызов переведёт сессию в Failed.
	first.breakConnection(io.ErrClosedPipe)
	_, err := s.Do(context.Background(), 200, []byte(`{}`))
	if err == nil {
		t.Fatal("Do на оборванном соединении обязан вернуть ошибку")
	}
	if !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("ошибка обрыва = %v, ожидался ErrConnectionLost", err)
	}
	if s.State() != StateFailed {
		t.Fatalf("состояние после обрыва: %v, ожидалось Failed", s.State())
	}

	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("повторный Connect: %v", err)
	}

	if got := first.closeCount(); got != 1 {
		t.Errorf("прежнее соединение закрыто %d раз, ожидался ровно 1 — "+
			"незакрытое остаётся жить при включённом keep-alive", got)
	}
	if got := second.closeCount(); got != 0 {
		t.Errorf("новое соединение закрыто (%d) — реконнект потерял связь", got)
	}
	if s.State() != StateReady {
		t.Errorf("состояние после реконнекта: %v, ожидалось Ready", s.State())
	}

	// Новое соединение — действительно текущее: работа идёт через него.
	if _, err := s.Do(context.Background(), 200, []byte(`{}`)); err != nil {
		t.Errorf("Do после реконнекта: %v", err)
	}
	if second.streamsOpened() == 0 {
		t.Error("после реконнекта стримы открываются не на новом соединении")
	}
}

// Close после реконнекта закрывает только текущее соединение, и суммарно
// каждое закрыто ровно один раз.
func TestReconnectThenCloseClosesEachConnectionOnce(t *testing.T) {
	first, second := handshakeConn(t), handshakeConn(t)
	s := connectedSession(t, queueTransport(first, second))

	first.breakConnection(io.ErrClosedPipe)
	_, _ = s.Do(context.Background(), 200, []byte(`{}`))
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("повторный Connect: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("повторный Close: %v", err)
	}

	if got := first.closeCount(); got != 1 {
		t.Errorf("прежнее соединение закрыто %d раз, ожидался 1", got)
	}
	if got := second.closeCount(); got != 1 {
		t.Errorf("текущее соединение закрыто %d раз, ожидался 1", got)
	}
}

// Неудачная попытка замены не трогает соединение, которое сессия всё ещё
// держит: пока новое не установлено, старое остаётся единственным, и его
// закрытие означало бы потерю связи по инициативе неудачи.
func TestFailedReconnectKeepsExistingConnection(t *testing.T) {
	first := handshakeConn(t)
	tr := queueTransport(first)
	s := connectedSession(t, tr)
	defer s.Close()

	first.breakConnection(io.ErrClosedPipe)
	_, _ = s.Do(context.Background(), 200, []byte(`{}`))

	// Очередь соединений исчерпана — следующий Connect обязан провалиться.
	if err := s.Connect(context.Background()); err == nil {
		t.Fatal("Connect обязан был провалиться: соединений в очереди нет")
	}

	if got := first.closeCount(); got != 0 {
		t.Errorf("неудачная замена закрыла прежнее соединение (%d раз)", got)
	}
	if s.State() != StateFailed {
		t.Errorf("состояние после неудачи: %v, ожидалось Failed", s.State())
	}
}

// Серия реконнектов не оставляет висящих соединений: каждое прежнее закрыто
// ровно один раз, текущее — живо до Close.
func TestRepeatedReconnectLeavesNoOpenConnections(t *testing.T) {
	const n = 5
	conns := make([]*fakeConn, 0, n)
	for i := 0; i < n; i++ {
		conns = append(conns, handshakeConn(t))
	}
	s := connectedSession(t, queueTransport(conns...))

	for i := 0; i < n-1; i++ {
		conns[i].breakConnection(io.ErrClosedPipe)
		_, _ = s.Do(context.Background(), 200, []byte(`{}`))
		if err := s.Connect(context.Background()); err != nil {
			t.Fatalf("реконнект №%d: %v", i+1, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for i, c := range conns {
		if got := c.closeCount(); got != 1 {
			t.Errorf("соединение №%d закрыто %d раз, ожидался ровно 1", i, got)
		}
	}
}

// Конкурентные Connect сохраняют прежний контракт: одновременно идёт не
// более одной попытки, лишние получают ErrConnectInProgress, а соединений
// открывается не больше, чем удачных попыток.
func TestConcurrentReconnectOpensOneConnection(t *testing.T) {
	first, second := handshakeConn(t), handshakeConn(t)
	tr := queueTransport(first, second)
	s := connectedSession(t, tr)
	defer s.Close()

	first.breakConnection(io.ErrClosedPipe)
	_, _ = s.Do(context.Background(), 200, []byte(`{}`))

	const callers = 8
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.Connect(context.Background())
		}()
	}
	wg.Wait()
	close(errs)

	ok, inProgress, other := 0, 0, 0
	for err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrConnectInProgress):
			inProgress++
		default:
			other++
		}
	}
	if other != 0 {
		t.Errorf("неожиданных ошибок: %d", other)
	}
	if ok == 0 {
		t.Fatal("ни один Connect не удался")
	}
	if got := tr.connectCount(); got > 2 {
		t.Errorf("транспорт вызван %d раз: параллельные Connect открыли лишние соединения", got)
	}
	if got := first.closeCount(); got != 1 {
		t.Errorf("прежнее соединение закрыто %d раз, ожидался 1", got)
	}
}
