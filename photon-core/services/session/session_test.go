package session

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	photonpb "github.com/wiloris/photon-core/protobuf"
)

// Тесты T-1…T-16 по photon-core/docs/bridge-session.md §8.
// Failure-oriented тесты обязательны (CLAUDE.md): три реальных бага Phase 9-10
// найдены именно ими, ни один не проявлялся при нормальной работе.

// ── T-1: порядок фреймов handshake ──────────────────────────────────────────

func TestT1_HandshakeFrameOrderAndContent(t *testing.T) {
	type captured struct {
		auth map[string]interface{}
		mode map[string]interface{}
	}
	var got captured
	done := make(chan struct{})

	conn := newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
		defer srv.Close()
		defer close(done)
		if err := readFrames(t, srv, &got.auth, &got.mode); err != nil {
			t.Errorf("read handshake frames: %v", err)
			return
		}
		writeJSON(t, srv, map[string]interface{}{"ok": true})
	})

	s, err := New(Config{
		Transport: &fakeTransport{conn: conn}, Endpoint: "fake://bridge",
		SessionToken: "tok-123", Mode: ModeAction,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	<-done

	// Фрейм 1 — только session_token, без mode: перепутанный порядок сервер
	// отвергнет молча, поэтому проверяется содержимое каждого фрейма.
	if got.auth["session_token"] != "tok-123" {
		t.Errorf("frame 1 session_token = %v, want tok-123", got.auth["session_token"])
	}
	if _, has := got.auth["mode"]; has {
		t.Error("frame 1 must not carry mode — order would be indistinguishable")
	}
	if got.mode["mode"] != "action" {
		t.Errorf("frame 2 mode = %v, want action", got.mode["mode"])
	}
	if v, ok := got.mode["protocol_version"].(float64); !ok || uint32(v) != ProtocolVersion {
		t.Errorf("frame 2 protocol_version = %v, want %d", got.mode["protocol_version"], ProtocolVersion)
	}
	if s.State() != StateReady {
		t.Errorf("state = %v, want ready", s.State())
	}
}

// ── T-2: сервер отверг handshake ────────────────────────────────────────────

func TestT2_HandshakeRejectedByServer(t *testing.T) {
	conn := newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
		defer srv.Close()
		var a, m map[string]interface{}
		readFrames(t, srv, &a, &m)
		writeJSON(t, srv, map[string]interface{}{"ok": false, "error": "bad token"})
	})
	s, _ := New(Config{
		Transport: &fakeTransport{conn: conn}, Endpoint: "e",
		SessionToken: "nope", Mode: ModeAction,
	})
	defer s.Close()

	err := s.Connect(context.Background())
	if !errors.Is(err, ErrHandshakeRejected) {
		t.Fatalf("err = %v, want ErrHandshakeRejected", err)
	}
	if !strings.Contains(err.Error(), "bad token") {
		t.Errorf("err = %v, want server reason preserved", err)
	}
	if s.State() != StateFailed {
		t.Errorf("state = %v, want failed", s.State())
	}
	// Сессия не должна считаться готовой — иначе работа пошла бы по
	// неаутентифицированному соединению.
	if _, err := s.Do(context.Background(), 100, nil); !errors.Is(err, ErrNotReady) {
		t.Errorf("Do after rejected handshake = %v, want ErrNotReady", err)
	}
}

// ── T-3: обрыв между фреймами ───────────────────────────────────────────────

func TestT3_HandshakeConnectionBrokenMidway(t *testing.T) {
	conn := newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
		var a map[string]interface{}
		readFrames(t, srv, &a) // прочитали auth и оборвали
		srv.Close()
	})
	s, _ := New(Config{
		Transport: &fakeTransport{conn: conn}, Endpoint: "e",
		SessionToken: "t", Mode: ModeAction,
	})
	defer s.Close()

	errCh := make(chan error, 1)
	go func() { errCh <- s.Connect(context.Background()) }()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Connect succeeded on broken connection")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect hung on broken connection — must fail, not block")
	}
}

// ── T-4: мусор вместо JSON ──────────────────────────────────────────────────

func TestT4_HandshakeGarbageResponse(t *testing.T) {
	conn := newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
		defer srv.Close()
		var a, m map[string]interface{}
		readFrames(t, srv, &a, &m)
		srv.Write([]byte("this is not json\n"))
	})
	s, _ := New(Config{
		Transport: &fakeTransport{conn: conn}, Endpoint: "e",
		SessionToken: "t", Mode: ModeAction,
	})
	defer s.Close()

	err := s.Connect(context.Background())
	if !errors.Is(err, ErrMalformedFrame) {
		t.Fatalf("err = %v, want ErrMalformedFrame", err)
	}
}

// ── T-5: несовместимая версия протокола отвергается ─────────────────────────
//
// Ключевой тест механизма смены wire-формата (ADR-034 п.5). Обязан падать,
// если реализация начнёт принимать чужую версию.

func TestT5_IncompatibleProtocolVersionRejected(t *testing.T) {
	for _, srvVersion := range []uint32{2, 99, ProtocolVersion + 1} {
		conn := newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
			defer srv.Close()
			var a, m map[string]interface{}
			readFrames(t, srv, &a, &m)
			writeJSON(t, srv, map[string]interface{}{
				"ok": true, "protocol_version": srvVersion,
			})
		})
		s, _ := New(Config{
			Transport: &fakeTransport{conn: conn}, Endpoint: "e",
			SessionToken: "t", Mode: ModeAction,
		})

		err := s.Connect(context.Background())
		if !errors.Is(err, ErrProtocolVersion) {
			t.Fatalf("server version %d: err = %v, want ErrProtocolVersion", srvVersion, err)
		}
		if s.State() == StateReady {
			t.Fatalf("server version %d: session became ready despite incompatible version", srvVersion)
		}
		s.Close()
	}
}

// ── T-6: совместимая и отсутствующая версия принимаются ─────────────────────
//
// Обратная сторона T-5: развёрнутый сегодня Bridge про версии не знает и поля
// не шлёт. Если бы отсутствие поля трактовалось как несовместимость, эта
// проверка сломала бы работающую систему.

func TestT6_CompatibleAndAbsentProtocolVersionAccepted(t *testing.T) {
	cases := []struct {
		name string
		ack  map[string]interface{}
	}{
		{"absent (deployed server)", map[string]interface{}{"ok": true}},
		{"explicit equal", map[string]interface{}{"ok": true, "protocol_version": ProtocolVersion}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ack := tc.ack
			conn := newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
				defer srv.Close()
				var a, m map[string]interface{}
				readFrames(t, srv, &a, &m)
				writeJSON(t, srv, ack)
			})
			s, _ := New(Config{
				Transport: &fakeTransport{conn: conn}, Endpoint: "e",
				SessionToken: "t", Mode: ModeAction,
			})
			defer s.Close()
			if err := s.Connect(context.Background()); err != nil {
				t.Fatalf("Connect: %v", err)
			}
			if s.State() != StateReady {
				t.Errorf("state = %v, want ready", s.State())
			}
		})
	}
}

// ── T-7: action — два фрейма, ответ любой формы ─────────────────────────────

func TestT7_ActionFramesAndResponseShapes(t *testing.T) {
	// data может быть объектом, массивом, null или отсутствовать. Жёсткое
	// map[string]interface{} падало на ответах-списках — реальный баг
	// тест-клиента Bridge, зафиксированный в его же комментарии.
	cases := []struct {
		name     string
		raw      string
		wantData string
	}{
		{"object", `{"ok":true,"data":{"id":"x"}}`, `{"id":"x"}`},
		{"array", `{"ok":true,"data":[{"id":"a"},{"id":"b"}]}`, `[{"id":"a"},{"id":"b"}]`},
		{"null", `{"ok":true,"data":null}`, `null`},
		{"absent", `{"ok":true}`, ``},
		{"error", `{"ok":false,"error":"boom"}`, ``},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.raw
			var hdr, payload map[string]interface{}
			handled := make(chan struct{})

			conn := newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
				defer srv.Close()
				if idx == 0 {
					var a, m map[string]interface{}
					readFrames(t, srv, &a, &m)
					writeJSON(t, srv, map[string]interface{}{"ok": true})
					return
				}
				defer close(handled)
				if err := readFrames(t, srv, &hdr, &payload); err != nil {
					t.Errorf("read action frames: %v", err)
					return
				}
				srv.Write([]byte(raw + "\n"))
			})

			tr := &fakeTransport{conn: conn}
			s, _ := New(Config{Transport: tr, Endpoint: "e", SessionToken: "t", Mode: ModeAction})
			defer s.Close()
			if err := s.Connect(context.Background()); err != nil {
				t.Fatalf("Connect: %v", err)
			}

			resp, err := s.Do(context.Background(), 100, []byte(`{"channel_id":"c","message":"m"}`))
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			<-handled

			if hdr["action"] != float64(100) {
				t.Errorf("header action = %v, want 100", hdr["action"])
			}
			if hdr["resource"] != "PostResource" {
				t.Errorf("header resource = %v, want PostResource", hdr["resource"])
			}
			if payload["channel_id"] != "c" {
				t.Errorf("payload channel_id = %v, want c", payload["channel_id"])
			}
			if string(resp.Data) != tc.wantData {
				t.Errorf("Data = %q, want %q", resp.Data, tc.wantData)
			}
			if tc.name == "error" {
				if resp.OK || resp.Error != "boom" {
					t.Errorf("resp = %+v, want ok=false error=boom", resp)
				}
			}
		})
	}
}

func TestT7_NilPayloadSentAsEmptyObject(t *testing.T) {
	var payload json.RawMessage
	handled := make(chan struct{})
	conn := newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
		defer srv.Close()
		if idx == 0 {
			var a, m map[string]interface{}
			readFrames(t, srv, &a, &m)
			writeJSON(t, srv, map[string]interface{}{"ok": true})
			return
		}
		defer close(handled)
		var hdr map[string]interface{}
		readFrames(t, srv, &hdr, &payload)
		writeJSON(t, srv, map[string]interface{}{"ok": true})
	})
	s, _ := New(Config{Transport: &fakeTransport{conn: conn}, Endpoint: "e", SessionToken: "t", Mode: ModeAction})
	defer s.Close()
	s.Connect(context.Background())

	if _, err := s.Do(context.Background(), 200, nil); err != nil {
		t.Fatalf("Do: %v", err)
	}
	<-handled
	// Протокол ждёт ровно два фрейма; пустого фрейма в нём нет.
	if strings.TrimSpace(string(payload)) != "{}" {
		t.Errorf("payload = %q, want {}", payload)
	}
}

func TestT7_InvalidPayloadRejectedBeforeSending(t *testing.T) {
	s, conn := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeAction)
	before := conn.streamsOpened()

	_, err := s.Do(context.Background(), 100, []byte(`{not json`))
	if !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("err = %v, want ErrInvalidPayload", err)
	}
	// Стрим открылся, но невалидный JSON в сеть не ушёл бы дальше header'а;
	// главное — вызывающий получил внятную ошибку, а не отказ сервера.
	if conn.streamsOpened() < before {
		t.Error("stream accounting broken")
	}
}

// ── T-8: resource по диапазонам ─────────────────────────────────────────────

func TestT8_ResourceForEachRangeAndBoundaries(t *testing.T) {
	// Таблица подтверждена перекрёстной проверкой всех 48 вызовов action(...)
	// в test-client/test_photon.go — клиенте, проверенном против живого
	// сервера.
	cases := []struct {
		action int32
		want   string
	}{
		{100, "PostResource"}, {114, "PostResource"},
		{200, "UserResource"}, {210, "UserResource"},
		{300, "ChannelResource"}, {314, "ChannelResource"},
		{400, "EngramResource"}, {405, "EngramResource"},
		{500, "FileResource"}, {505, "FileResource"},
		{600, "WallResource"}, {602, "WallResource"},
		{219, "UserResource"},
		{900, "ConversationResource"}, {905, "ConversationResource"}, {931, "ConversationResource"},
		{940, "ConversationResource"},
	}
	for _, tc := range cases {
		got, err := ResourceFor(tc.action)
		if err != nil {
			t.Errorf("ResourceFor(%d): %v", tc.action, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ResourceFor(%d) = %q, want %q", tc.action, got, tc.want)
		}
	}
}

func TestT8_UnknownActionRejected(t *testing.T) {
	// 150 лежит В диапазоне Post, но такого action'а не существует.
	// Проверка диапазона без проверки enum пропустила бы его, и сервер
	// отверг бы запрос невнятной ошибкой уже по сети.
	// 700 из этого списка убран: с вводом MediaResource (ADR-049 §8) код
	// существует. Заменён на 750 — он лежит В диапазоне Media, но такого
	// action'а нет, то есть проверяет ровно то же свойство.
	// 950 — то же свойство для ConversationResource (ADR-099 §2.3: 932-999
	// зарезервированы без семантики).
	for _, code := range []int32{0, 1, 99, 150, 250, 750, 950, 9999, -1} {
		if _, err := ResourceFor(code); !errors.Is(err, ErrUnknownAction) {
			t.Errorf("ResourceFor(%d) err = %v, want ErrUnknownAction", code, err)
		}
	}
}

func TestT8_EveryEnumActionHasResource(t *testing.T) {
	// Ловит противоположное: код добавили в enum Action, а диапазон в
	// ResourceFor обновить забыли. Без этого теста ошибка проявилась бы
	// отказом сервера в проде.
	for code, name := range photonpb.Action_name {
		if code == 0 {
			continue
		}
		if _, err := ResourceFor(code); err != nil {
			t.Errorf("action %s (%d) has no resource: %v", name, code, err)
		}
	}
}

func TestT8_DoRejectsUnknownActionWithoutNetwork(t *testing.T) {
	s, conn := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeAction)
	before := conn.streamsOpened()

	if _, err := s.Do(context.Background(), 150, nil); !errors.Is(err, ErrUnknownAction) {
		t.Fatalf("err = %v, want ErrUnknownAction", err)
	}
	if got := conn.streamsOpened(); got != before {
		t.Errorf("opened %d streams for an unknown action, want 0 — validation must precede the network",
			got-before)
	}
}

// ── T-9: подмена кодека не требует правок сессии ────────────────────────────
//
// Проверяет границу ADR-034 п.4: если бы JSON протёк наружу интерфейса, этот
// тест не скомпилировался бы или потребовал бы правки session.go.

type countingCodec struct {
	inner Codec

	mu    sync.Mutex
	calls map[string]int
}

func newCountingCodec() *countingCodec {
	return &countingCodec{inner: JSONCodec{}, calls: map[string]int{}}
}

func (c *countingCodec) note(m string) {
	c.mu.Lock()
	c.calls[m]++
	c.mu.Unlock()
}

func (c *countingCodec) count(m string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[m]
}

func (c *countingCodec) WriteHandshake(w io.Writer, h Handshake) error {
	c.note("WriteHandshake")
	return c.inner.WriteHandshake(w, h)
}
func (c *countingCodec) ReadHandshakeAck(r io.Reader) (HandshakeAck, error) {
	c.note("ReadHandshakeAck")
	return c.inner.ReadHandshakeAck(r)
}
func (c *countingCodec) WriteRequest(w io.Writer, req Request) error {
	c.note("WriteRequest")
	return c.inner.WriteRequest(w, req)
}
func (c *countingCodec) ReadResponse(r io.Reader) (Response, error) {
	c.note("ReadResponse")
	return c.inner.ReadResponse(r)
}
func (c *countingCodec) WriteRequestStream(w io.Writer, req Request, body io.Reader) error {
	c.note("WriteRequestStream")
	return c.inner.WriteRequestStream(w, req, body)
}
func (c *countingCodec) ReadResponseStream(r io.Reader, sink io.Writer) (Response, error) {
	c.note("ReadResponseStream")
	return c.inner.ReadResponseStream(r, sink)
}
func (c *countingCodec) ReadPush(r io.Reader) (Push, error) {
	c.note("ReadPush")
	return c.inner.ReadPush(r)
}

func TestT9_CodecIsSubstitutable(t *testing.T) {
	codec := newCountingCodec()
	conn := newFakeConn(t, okHandshake(map[string]interface{}{"ok": true, "data": map[string]string{"id": "1"}}))
	s, err := New(Config{
		Transport: &fakeTransport{conn: conn}, Endpoint: "e",
		SessionToken: "t", Mode: ModeAction, Codec: codec,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := s.Do(context.Background(), 100, []byte(`{}`)); err != nil {
		t.Fatalf("Do: %v", err)
	}

	for _, m := range []string{"WriteHandshake", "ReadHandshakeAck", "WriteRequest", "ReadResponse"} {
		if codec.count(m) == 0 {
			t.Errorf("%s never called — session bypassed the codec boundary", m)
		}
	}
}

func TestT9_DefaultCodecIsJSON(t *testing.T) {
	s, err := New(Config{
		Transport: &fakeTransport{conn: newFakeConn(t, nil)}, Endpoint: "e",
		SessionToken: "t", Mode: ModeAction,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok := s.codec.(JSONCodec); !ok {
		t.Errorf("default codec = %T, want JSONCodec (ADR-034)", s.codec)
	}
}

// ── T-10: Close идемпотентен из любого состояния ────────────────────────────
//
// Прецедент: double-free в ipc.closeWith при двойном вызове (Phase 9) — не
// проявлялся при нормальной работе.

func TestT10_CloseIsIdempotentFromEveryState(t *testing.T) {
	t.Run("created", func(t *testing.T) {
		s, _ := New(Config{Transport: &fakeTransport{conn: newFakeConn(t, nil)}, Endpoint: "e", Mode: ModeAction})
		for i := 0; i < 3; i++ {
			if err := s.Close(); err != nil {
				t.Fatalf("Close #%d: %v", i+1, err)
			}
		}
	})

	t.Run("ready", func(t *testing.T) {
		s, _ := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeAction)
		for i := 0; i < 3; i++ {
			if err := s.Close(); err != nil {
				t.Fatalf("Close #%d: %v", i+1, err)
			}
		}
		if s.State() != StateClosed {
			t.Errorf("state = %v, want closed", s.State())
		}
	})

	t.Run("failed", func(t *testing.T) {
		conn := newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
			defer srv.Close()
			var a, m map[string]interface{}
			readFrames(t, srv, &a, &m)
			writeJSON(t, srv, map[string]interface{}{"ok": false, "error": "no"})
		})
		s, _ := New(Config{Transport: &fakeTransport{conn: conn}, Endpoint: "e", Mode: ModeAction})
		s.Connect(context.Background())
		for i := 0; i < 2; i++ {
			if err := s.Close(); err != nil {
				t.Fatalf("Close #%d: %v", i+1, err)
			}
		}
	})

	t.Run("concurrent", func(t *testing.T) {
		s, _ := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeAction)
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); s.Close() }()
		}
		wg.Wait()
	})
}

// ── T-11: Close во время Connect ────────────────────────────────────────────
//
// Прецедент: вечное зависание host.Stop на неотвечающем плагине (Phase 9).

func TestT11_CloseDuringConnectDoesNotHang(t *testing.T) {
	tr := &fakeTransport{
		conn:         newFakeConn(t, nil),
		connectBlock: make(chan struct{}), // Connect висит, пока не отменят
	}
	s, _ := New(Config{Transport: tr, Endpoint: "e", SessionToken: "t", Mode: ModeAction})

	errCh := make(chan error, 1)
	go func() { errCh <- s.Connect(context.Background()) }()

	// Дать Connect дойти до блокирующего вызова.
	time.Sleep(50 * time.Millisecond)

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung while Connect was in progress")
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Connect succeeded although the session was closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not return after Close — Close must interrupt it, not wait for it")
	}

	if s.State() != StateClosed {
		t.Errorf("state = %v, want closed", s.State())
	}
}

// ── T-12: после Close сессия не воскресает ──────────────────────────────────
//
// Прецедент: воскрешение плагина после Stop во время backoff (Phase 9).

func TestT12_NoResurrectionAfterClose(t *testing.T) {
	s, conn := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeAction)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	before := conn.streamsOpened()

	if err := s.Connect(context.Background()); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("Connect after Close = %v, want ErrSessionClosed", err)
	}
	if _, err := s.Do(context.Background(), 100, nil); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("Do after Close = %v, want ErrSessionClosed", err)
	}
	if _, err := s.AcceptPush(context.Background()); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("AcceptPush after Close = %v, want ErrSessionClosed", err)
	}
	if got := conn.streamsOpened(); got != before {
		t.Errorf("opened %d streams after Close, want 0", got-before)
	}
	if s.State() != StateClosed {
		t.Errorf("state = %v, want closed", s.State())
	}
}

// ── T-13: отмена ctx во время Do ────────────────────────────────────────────

func TestT13_ContextCancelUnblocksCaller(t *testing.T) {
	// Сервер принимает запрос и не отвечает никогда.
	conn := newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
		if idx == 0 {
			defer srv.Close()
			var a, m map[string]interface{}
			readFrames(t, srv, &a, &m)
			writeJSON(t, srv, map[string]interface{}{"ok": true})
			return
		}
		var hdr, payload map[string]interface{}
		readFrames(t, srv, &hdr, &payload)
		// молчим — стрим не закрываем
	})
	s, _ := New(Config{Transport: &fakeTransport{conn: conn}, Endpoint: "e", SessionToken: "t", Mode: ModeAction})
	defer s.Close()
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := s.Do(ctx, 100, []byte(`{}`))
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Do = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Do did not return after ctx cancel — caller must not wait on a silent server")
	}
	// Полное освобождение стрима здесь недостижимо: ports.Stream.Close —
	// half-close, CancelRead порт не экспортирует. Записано как ограничение
	// (bridge-session.md §6 п.6, §9 п.4), а не проверяется здесь как факт.
}

// ── T-14: параллельные Do ───────────────────────────────────────────────────

func TestT14_ConcurrentDoUsesSeparateStreams(t *testing.T) {
	const n = 16
	conn := newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
		defer srv.Close()
		if idx == 0 {
			var a, m map[string]interface{}
			readFrames(t, srv, &a, &m)
			writeJSON(t, srv, map[string]interface{}{"ok": true})
			return
		}
		var hdr, payload map[string]interface{}
		if err := readFrames(t, srv, &hdr, &payload); err != nil {
			return
		}
		// Эхо: ответ содержит номер, присланный в payload. Смешение ответов
		// между стримами обнаружится как несовпадение.
		writeJSON(t, srv, map[string]interface{}{
			"ok": true, "data": map[string]interface{}{"echo": payload["n"]},
		})
	})

	s, _ := New(Config{Transport: &fakeTransport{conn: conn}, Endpoint: "e", SessionToken: "t", Mode: ModeAction})
	defer s.Close()
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(`{"n":` + itoa(i) + `}`)
			resp, err := s.Do(context.Background(), 100, payload)
			if err != nil {
				errs <- err
				return
			}
			var got struct {
				Echo int `json:"echo"`
			}
			if err := json.Unmarshal(resp.Data, &got); err != nil {
				errs <- err
				return
			}
			if got.Echo != i {
				errs <- errMismatch(i, got.Echo)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// n action-стримов + 1 handshake.
	if got := conn.streamsOpened(); got != n+1 {
		t.Errorf("streams opened = %d, want %d (one per action + handshake)", got, n+1)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func errMismatch(want, got int) error {
	return errors.New("response mismatch: want echo=" + itoa(want) + ", got " + itoa(got))
}

// ── T-15: push ──────────────────────────────────────────────────────────────

func TestT15_AcceptPush(t *testing.T) {
	s, conn := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeSubscribe)
	conn.pushes <- []byte(`{"event":"posted","user_id":"u1","data":{"id":"p1"}}`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := s.AcceptPush(ctx)
	if err != nil {
		t.Fatalf("AcceptPush: %v", err)
	}
	// event — строка, не число: на проводе так, несмотря на enum EventType
	// в схеме (ADR-034).
	if p.Event != "posted" || p.UserID != "u1" {
		t.Errorf("push = %+v, want event=posted user_id=u1", p)
	}
	if string(p.Data) != `{"id":"p1"}` {
		t.Errorf("push data = %q", p.Data)
	}
}

func TestT15_AcceptPushRequiresSubscribeMode(t *testing.T) {
	// На action-соединении сервер UniStream'ов не открывает
	// (bridge-session.md §2.4) — вызов должен быть отвергнут сразу, а не
	// зависнуть в ожидании события, которого не будет.
	s, _ := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeAction)

	done := make(chan error, 1)
	go func() {
		_, err := s.AcceptPush(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "subscribe") {
			t.Fatalf("err = %v, want mode complaint", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AcceptPush hung on an action-mode session")
	}
}

// ── T-17: молчаливый откат режима сервером ──────────────────────────────────
//
// Сервер при ЛЮБОЙ ошибке чтения mode-фрейма подставляет "action" и отвечает
// {"ok":true,"data":{"status":"ready"}} — quic_listener.go:270-275. Клиент,
// запросивший subscribe и не читающий status, считает handshake удавшимся и
// уходит в AcceptPush на соединении, где UniStream'ов не будет никогда.
//
// Тест ловит именно это: подтверждение режима обязано сверяться с
// запрошенным. Проверено сломом правила — при снятой проверке в
// session.go:229 подтест "subscribe downgraded to action" перестаёт падать и
// сессия становится Ready.

func TestT17_SilentModeDowngradeRejected(t *testing.T) {
	ackWith := func(ack map[string]interface{}) streamHandler {
		return func(t *testing.T, idx int, srv io.ReadWriteCloser) {
			defer srv.Close()
			var a, m map[string]interface{}
			if err := readFrames(t, srv, &a, &m); err != nil {
				return
			}
			writeJSON(t, srv, ack)
		}
	}

	cases := []struct {
		name string
		mode Mode
		ack  map[string]interface{}
		want error // nil = handshake обязан пройти
	}{
		{
			name: "subscribe downgraded to action",
			mode: ModeSubscribe,
			ack:  map[string]interface{}{"ok": true, "data": map[string]string{"status": "ready"}},
			want: ErrModeNotAccepted,
		},
		{
			// Обратный случай для полноты: сервер, подтвердивший subscribe
			// на action-запрос, тоже не тот сервер, с которым продолжают.
			name: "action confirmed as subscribe",
			mode: ModeAction,
			ack:  map[string]interface{}{"ok": true, "data": map[string]string{"status": "subscribed"}},
			want: ErrModeNotAccepted,
		},
		{
			name: "subscribe confirmed correctly",
			mode: ModeSubscribe,
			ack:  map[string]interface{}{"ok": true, "data": map[string]string{"status": "subscribed"}},
		},
		{
			name: "action confirmed correctly",
			mode: ModeAction,
			ack:  map[string]interface{}{"ok": true, "data": map[string]string{"status": "ready"}},
		},
		{
			// Регрессия совместимости: сервер вправе не прислать data вовсе.
			// Если бы отсутствие подтверждения трактовалось как отказ, эта
			// проверка сломала бы работающую систему — ровно та же ловушка,
			// что у protocol_version в T-6.
			name: "no data at all",
			mode: ModeSubscribe,
			ack:  map[string]interface{}{"ok": true},
		},
		{
			name: "data without status field",
			mode: ModeSubscribe,
			ack:  map[string]interface{}{"ok": true, "data": map[string]string{"other": "x"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(Config{
				Transport: &fakeTransport{conn: newFakeConn(t, ackWith(tc.ack))},
				Endpoint:  "e", SessionToken: "t", Mode: tc.mode,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer s.Close()

			err = s.Connect(context.Background())
			if tc.want == nil {
				if err != nil {
					t.Fatalf("Connect: %v, want success", err)
				}
				if s.State() != StateReady {
					t.Errorf("state = %v, want ready", s.State())
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if s.State() == StateReady {
				t.Error("session became ready despite a mode the server did not confirm")
			}
		})
	}
}

// data не типизирована (§2.2): её неожиданная форма не должна ронять
// handshake, который в остальном состоялся. Без снисходительного разбора в
// statusFromAckData строгий тип превратил бы каждый такой ответ в
// ErrMalformedFrame — отказ там, где сервер ничего не нарушил.
func TestT17_NonObjectAckDataDoesNotBreakHandshake(t *testing.T) {
	for _, data := range []string{`null`, `[]`, `"ready"`, `42`} {
		t.Run(data, func(t *testing.T) {
			raw := `{"ok":true,"data":` + data + `}`
			conn := newFakeConn(t, func(t *testing.T, idx int, srv io.ReadWriteCloser) {
				defer srv.Close()
				var a, m map[string]interface{}
				if err := readFrames(t, srv, &a, &m); err != nil {
					return
				}
				io.WriteString(srv, raw+"\n")
			})
			s, _ := New(Config{
				Transport: &fakeTransport{conn: conn},
				Endpoint:  "e", SessionToken: "t", Mode: ModeSubscribe,
			})
			defer s.Close()
			if err := s.Connect(context.Background()); err != nil {
				t.Fatalf("Connect: %v, want success (data=%s)", err, data)
			}
		})
	}
}

// ── Прочие контрактные проверки ─────────────────────────────────────────────

func TestConnectIsIdempotentOnReadySession(t *testing.T) {
	conn := newFakeConn(t, okHandshake(map[string]interface{}{"ok": true}))
	tr := &fakeTransport{conn: conn}
	s, _ := New(Config{Transport: tr, Endpoint: "e", SessionToken: "t", Mode: ModeAction})
	defer s.Close()

	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := s.Connect(context.Background()); err != nil {
		t.Fatalf("second Connect = %v, want nil", err)
	}
	// Второе соединение открываться не должно.
	if got := tr.connectCount(); got != 1 {
		t.Errorf("transport.Connect called %d times, want 1", got)
	}
}

func TestDoOnUnconnectedSessionDoesNotConnectImplicitly(t *testing.T) {
	tr := &fakeTransport{conn: newFakeConn(t, nil)}
	s, _ := New(Config{Transport: tr, Endpoint: "e", SessionToken: "t", Mode: ModeAction})
	defer s.Close()

	if _, err := s.Do(context.Background(), 100, nil); !errors.Is(err, ErrNotReady) {
		t.Fatalf("Do = %v, want ErrNotReady", err)
	}
	if got := tr.connectCount(); got != 0 {
		t.Errorf("transport.Connect called %d times — Do must not connect implicitly", got)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	conn := newFakeConn(t, nil)
	cases := []struct {
		name string
		cfg  Config
	}{
		{"no transport", Config{Endpoint: "e", Mode: ModeAction}},
		{"no endpoint", Config{Transport: &fakeTransport{conn: conn}, Mode: ModeAction}},
		{"bad mode", Config{Transport: &fakeTransport{conn: conn}, Endpoint: "e", Mode: "sideways"}},
		{"empty mode", Config{Transport: &fakeTransport{conn: conn}, Endpoint: "e"}},
	}
	for _, tc := range cases {
		if _, err := New(tc.cfg); err == nil {
			t.Errorf("%s: New succeeded, want error", tc.name)
		}
	}
}

func TestNewDoesNotTouchTheNetwork(t *testing.T) {
	// Соответствует правилу, записанному в photon_core.h для Photon_Create:
	// создание инстанса не делает сетевых вызовов.
	tr := &fakeTransport{conn: newFakeConn(t, nil)}
	s, err := New(Config{Transport: tr, Endpoint: "e", SessionToken: "t", Mode: ModeAction})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()
	if got := tr.connectCount(); got != 0 {
		t.Errorf("transport.Connect called %d times during New, want 0", got)
	}
	if s.State() != StateCreated {
		t.Errorf("state = %v, want created", s.State())
	}
}

func TestConnectFailurePropagates(t *testing.T) {
	tr := &fakeTransport{connectErr: io.ErrUnexpectedEOF, conn: newFakeConn(t, nil)}
	s, _ := New(Config{Transport: tr, Endpoint: "e", SessionToken: "t", Mode: ModeAction})
	defer s.Close()

	err := s.Connect(context.Background())
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want wrapped transport error", err)
	}
	if s.State() != StateFailed {
		t.Errorf("state = %v, want failed", s.State())
	}
}

// ── T-18: обрыв соединения отличим от «ещё не подключались» ─────────────────
//
// Прецедент не гипотетический: прогон против живого Bridge показал, что
// subscribe-соединение умирает через 30 с простоя (idle-timeout QUIC), сервер
// снимает клиента с регистрации, а подписчик об этом не узнаёт — у ждущего
// события нет следующей операции, на ошибке которой обрыв обнаружился бы.

func TestT18_AcceptPushReportsConnectionLost(t *testing.T) {
	s, conn := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeSubscribe)

	errCh := make(chan error, 1)
	go func() {
		_, err := s.AcceptPush(context.Background())
		errCh <- err
	}()

	// Даём AcceptPush заблокироваться, затем рвём связь.
	time.Sleep(20 * time.Millisecond)
	conn.breakConnection(io.ErrUnexpectedEOF)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrConnectionLost) {
			t.Fatalf("AcceptPush после обрыва = %v, want ErrConnectionLost", err)
		}
		// Исходная причина обязана сохраниться: без неё диагностика сводится
		// к «связь потеряна», без указания чем именно.
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("исходная причина потеряна: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AcceptPush не разблокировался при обрыве — ровно тот случай, " +
			"ради которого заведён сигнал: подписчик висит, считая себя подписанным")
	}

	if got := s.State(); got != StateFailed {
		t.Errorf("состояние после обрыва = %s, want failed", got)
	}
}

func TestT18_SubsequentCallsSayLostNotNotReady(t *testing.T) {
	s, conn := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeSubscribe)
	conn.breakConnection(io.ErrUnexpectedEOF)

	if _, err := s.AcceptPush(context.Background()); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("первый AcceptPush = %v, want ErrConnectionLost", err)
	}

	// Ключевое различие: «связь была и оборвалась» — не то же, что «ещё не
	// подключались». Это разные решения у вызывающего.
	_, err := s.AcceptPush(context.Background())
	if !errors.Is(err, ErrConnectionLost) {
		t.Errorf("повторный AcceptPush = %v, want ErrConnectionLost", err)
	}
	if errors.Is(err, ErrNotReady) {
		t.Error("обрыв не должен маскироваться под ErrNotReady")
	}

	_, err = s.Do(context.Background(), int32(photonpb.Action_ACTION_GET_ME), nil)
	if !errors.Is(err, ErrConnectionLost) {
		t.Errorf("Do после обрыва = %v, want ErrConnectionLost", err)
	}
}

func TestT18_DoReportsConnectionLost(t *testing.T) {
	s, conn := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeAction)
	conn.breakConnection(io.ErrUnexpectedEOF)

	_, err := s.Do(context.Background(), int32(photonpb.Action_ACTION_GET_ME), nil)
	if !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("Do после обрыва = %v, want ErrConnectionLost", err)
	}
	if got := s.State(); got != StateFailed {
		t.Errorf("состояние = %s, want failed", got)
	}
}

// Отмена ctx вызывающим — не обрыв. Если бы её классифицировали как обрыв,
// сессия ломалась бы от собственного таймаута вызывающего.
func TestT18_ContextCancelIsNotConnectionLost(t *testing.T) {
	s, _ := newReadySession(t, okHandshake(map[string]interface{}{"ok": true}), ModeSubscribe)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := s.AcceptPush(ctx)
	if errors.Is(err, ErrConnectionLost) {
		t.Fatalf("отмена ctx классифицирована как обрыв: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("AcceptPush по отмене = %v, want DeadlineExceeded", err)
	}
	if got := s.State(); got != StateReady {
		t.Errorf("отмена ctx изменила состояние на %s — сессия должна остаться ready", got)
	}
}

// Неудачный Connect и обрыв дают одно состояние Failed, но разные ошибки:
// иначе различие, ради которого всё это заведено, терялось бы на первом же
// повторном вызове.
func TestT18_FailedConnectIsNotConnectionLost(t *testing.T) {
	tr := &fakeTransport{connectErr: io.ErrClosedPipe}
	s, err := New(Config{
		Transport: tr, Endpoint: "fake://bridge", SessionToken: "tok", Mode: ModeAction,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	if err := s.Connect(context.Background()); err == nil {
		t.Fatal("Connect должен был провалиться")
	}
	if got := s.State(); got != StateFailed {
		t.Fatalf("состояние = %s, want failed", got)
	}

	_, err = s.Do(context.Background(), int32(photonpb.Action_ACTION_GET_ME), nil)
	if errors.Is(err, ErrConnectionLost) {
		t.Error("неудачный Connect не должен выглядеть как обрыв связи")
	}
	if !errors.Is(err, ErrNotReady) {
		t.Errorf("Do после неудачного Connect = %v, want ErrNotReady", err)
	}
}
