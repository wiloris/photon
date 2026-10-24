package p2phost

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	edadapter "github.com/wiloris/photon/photon-core/adapters/ed25519"
	pionadapter "github.com/wiloris/photon/photon-core/adapters/pion"
	"github.com/wiloris/photon/photon-core/ports"
)

// Владельцы, Sink и общий адаптер — то, чем пользуется coreops (ADR-076).

type sinkLog struct {
	mu     sync.Mutex
	got    []Event
	reject map[string]bool // тип события → отвергнуть
}

func (l *sinkLog) sink(owner string, ev Event) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.reject[ev.Type] {
		return false
	}
	ev.Owner = owner
	l.got = append(l.got, ev)
	return true
}

func (l *sinkLog) wait(t *testing.T, typ, sid string, d time.Duration) Event {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		l.mu.Lock()
		for i, ev := range l.got {
			if ev.Type == typ && ev.SessionID == sid {
				l.got = append(l.got[:i:i], l.got[i+1:]...)
				l.mu.Unlock()
				return ev
			}
		}
		l.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("Sink: нет %s для %s", typ, sid)
	return Event{}
}

// countingAdapter — общий адаптер композиции; считает Close.
type countingAdapter struct {
	ports.P2P
	mu     sync.Mutex
	closes int
}

func (c *countingAdapter) Close() error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	return nil
}

func sharedPion(t *testing.T) (*countingAdapter, ports.IdentityPair) {
	t.Helper()
	id := edadapter.New()
	p, err := id.Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	a, err := pionadapter.New(pionadapter.Config{Identity: id, Self: p, EstablishTimeout: 15 * time.Second, PeerLossTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return &countingAdapter{P2P: a}, p
}

func newShared(t *testing.T, log *sinkLog, mod func(*Config)) (*Executor, *countingAdapter, ports.IdentityPair) {
	t.Helper()
	a, p := sharedPion(t)
	cfg := Config{Adapter: a, NoHello: true, NoStreams: true, SessionLimit: -1, EstablishTimeout: 15 * time.Second, Sink: log.sink}
	if mod != nil {
		mod(&cfg)
	}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Shutdown)
	return e, a, p
}

// pairAs: инициатор — исполнитель с Sink (владелец owner), отвечающий —
// обычный исполнитель хоста приложения без «привета».
func pairAs(t *testing.T, e *Executor, owner string, pe ports.IdentityPair, peer *Executor, pp ports.IdentityPair) (string, string) {
	t.Helper()
	ctx := context.Background()
	neg, offer, err := e.OfferAs(ctx, owner, pp.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	sp, answer, err := peer.Accept(ctx, pe.Public, offer, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.CompleteAs(ctx, owner, neg, answer)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Await(ctx, owner, s); err != nil {
		t.Fatalf("Await: %v", err)
	}
	waitEvent(t, peer, EventEstablished, sp, 10*time.Second)
	return s, sp
}

func noHelloPeer(t *testing.T) (*Executor, ports.IdentityPair) {
	return newExec(t, func(c *Config) { c.NoHello = true; c.NoStreams = true })
}

func TestSinkDeliversOnlyToOwner(t *testing.T) {
	log := &sinkLog{}
	e, _, pe := newShared(t, log, nil)
	peer, pp := noHelloPeer(t)
	s1, p1 := pairAs(t, e, "one", pe, peer, pp)
	s2, p2 := pairAs(t, e, "two", pe, peer, pp)

	if err := peer.Send(p1, []byte("to-one")); err != nil {
		t.Fatal(err)
	}
	if err := peer.Send(p2, []byte("to-two")); err != nil {
		t.Fatal(err)
	}
	if ev := log.wait(t, EventMessage, s1, 10*time.Second); ev.Owner != "one" || string(ev.Payload) != "to-one" {
		t.Fatalf("сессия one: %+v", ev)
	}
	if ev := log.wait(t, EventMessage, s2, 10*time.Second); ev.Owner != "two" || string(ev.Payload) != "to-two" {
		t.Fatalf("сессия two: %+v", ev)
	}
	if evs := e.Poll(MaxPoll); len(evs) != 0 {
		t.Fatalf("при Sink очередь Poll должна быть пуста: %+v", evs)
	}
}

func TestForeignOwnerIsNotFound(t *testing.T) {
	log := &sinkLog{}
	e, _, pe := newShared(t, log, nil)
	peer, pp := noHelloPeer(t)
	s1, _ := pairAs(t, e, "one", pe, peer, pp)

	if err := e.SendAs("two", s1, []byte("x")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("SendAs чужим: %v", err)
	}
	if err := e.CloseSession("two", s1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CloseSession чужим: %v", err)
	}
	if err := e.Await(context.Background(), "two", s1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Await чужим: %v", err)
	}
	if _, err := e.Peer("two", s1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Peer чужим: %v", err)
	}
	if err := e.Close(s1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Close хоста приложения по чужой сессии: %v", err)
	}
	neg, _, err := e.OfferAs(context.Background(), "one", pp.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.CompleteAs(context.Background(), "two", neg, []byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CompleteAs чужим: %v", err)
	}
	if peerKey, err := e.Peer("one", s1); err != nil || len(peerKey) == 0 {
		t.Fatalf("Peer своим: %v", err)
	}
}

// CloseOwner — своя причина, только свои сессии и согласования.
func TestCloseOwnerClosesOnlyItsSessions(t *testing.T) {
	log := &sinkLog{}
	e, _, pe := newShared(t, log, nil)
	peer, pp := noHelloPeer(t)
	s1, _ := pairAs(t, e, "one", pe, peer, pp)
	s2, p2 := pairAs(t, e, "two", pe, peer, pp)
	neg, _, err := e.OfferAs(context.Background(), "one", pp.Public, nil)
	if err != nil {
		t.Fatal(err)
	}

	e.CloseOwner("one", "plugin_stopped")
	if ev := log.wait(t, EventClosed, s1, 10*time.Second); ev.Reason != "plugin_stopped" || ev.Owner != "one" {
		t.Fatalf("закрытие владельца: %+v", ev)
	}
	if _, err := e.CompleteAs(context.Background(), "one", neg, []byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("согласование владельца не снято: %v", err)
	}
	if err := e.SendAs("two", s2, []byte("alive")); err != nil {
		t.Fatalf("сессия другого владельца: %v", err)
	}
	if ev := waitEvent(t, peer, EventMessage, p2, 10*time.Second); string(ev.Payload) != "alive" {
		t.Fatalf("%q", ev.Payload)
	}
}

// Sink не принял сообщение — сессия закрывается slow_consumer, и
// P2P_CLOSED приходит после всех принятых сообщений.
func TestSinkRejectionClosesWithSlowConsumer(t *testing.T) {
	log := &sinkLog{}
	e, _, pe := newShared(t, log, nil)
	peer, pp := noHelloPeer(t)
	s1, p1 := pairAs(t, e, "one", pe, peer, pp)
	if err := peer.Send(p1, []byte("accepted")); err != nil {
		t.Fatal(err)
	}
	log.wait(t, EventMessage, s1, 10*time.Second)
	log.mu.Lock()
	log.reject = map[string]bool{EventMessage: true}
	log.mu.Unlock()
	if err := peer.Send(p1, []byte("rejected")); err != nil {
		t.Fatal(err)
	}
	if ev := log.wait(t, EventClosed, s1, 10*time.Second); ev.Reason != ReasonSlowConsumer {
		t.Fatalf("причина %q", ev.Reason)
	}
}

// Общий адаптер композиции исполнитель не закрывает: ни при закрытии
// сессии, ни при истечении согласования, ни при Shutdown.
func TestSharedAdapterIsNeverClosedByExecutor(t *testing.T) {
	log := &sinkLog{}
	e, a, pe := newShared(t, log, func(c *Config) { c.EstablishTimeout = 300 * time.Millisecond })
	peer, pp := noHelloPeer(t)
	if _, _, err := e.OfferAs(context.Background(), "one", pp.Public, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond) // согласование истекло
	e2, err := New(Config{Adapter: a, NoHello: true, NoStreams: true, Sink: log.sink, EstablishTimeout: 15 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	s, _ := pairAs(t, e2, "one", pe, peer, pp)
	if err := e2.CloseSession("one", s); err != nil {
		t.Fatal(err)
	}
	log.wait(t, EventClosed, s, 10*time.Second)
	e2.Shutdown()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closes != 0 {
		t.Fatalf("исполнитель закрыл общий адаптер %d раз", a.closes)
	}
}

func TestSharedAdapterRefusesPerNegotiationTURN(t *testing.T) {
	e, _, _ := newShared(t, &sinkLog{}, nil)
	_, pp := noHelloPeer(t)
	if _, _, err := e.OfferAs(context.Background(), "one", pp.Public, &TURN{URLs: []string{"turn:x"}}); !errors.Is(err, ErrBadArgument) {
		t.Fatalf("TURN на согласование при общем адаптере: %v", err)
	}
}

// Без «привета» отвечающий доставляет и пустой первый кадр (провод ADR-076).
func TestNoHelloDeliversEmptyFirstFrame(t *testing.T) {
	log := &sinkLog{}
	e, _, pe := newShared(t, log, nil)
	peer, pp := noHelloPeer(t)
	s1, p1 := pairAs(t, e, "one", pe, peer, pp)
	if err := e.SendAs("one", s1, nil); err != nil {
		t.Fatal(err)
	}
	if ev := waitEvent(t, peer, EventMessage, p1, 10*time.Second); len(ev.Payload) != 0 {
		t.Fatalf("первый кадр: %q", ev.Payload)
	}
}

// P2P_CLOSED — ровно один раз на сессию и последним из её событий, даже
// когда локальное закрытие гонится с входящим потоком сообщений.
func TestExactlyOneClosedAfterAllMessages(t *testing.T) {
	for _, mode := range []string{"queue", "sink"} {
		t.Run(mode, func(t *testing.T) {
			log := &sinkLog{}
			var e *Executor
			var pe ports.IdentityPair
			if mode == "sink" {
				e, _, pe = newShared(t, log, nil)
			} else {
				e, pe = newExec(t, func(c *Config) { c.NoHello = true; c.NoStreams = true })
			}
			peer, pp := noHelloPeer(t)
			s, p := pairAs(t, e, "", pe, peer, pp)
			stop := make(chan struct{})
			// 200 < MaxQueuedEvents: закрывает тест, а не переполнение.
			go func() {
				for i := 0; i < 200; i++ {
					select {
					case <-stop:
						return
					default:
					}
					if peer.Send(p, []byte("m")) != nil {
						return
					}
				}
			}()
			time.Sleep(2 * time.Millisecond)
			if err := e.CloseSession("", s); err != nil {
				t.Fatal(err)
			}
			time.Sleep(300 * time.Millisecond)
			close(stop)

			var evs []Event
			if mode == "sink" {
				log.mu.Lock()
				evs = append(evs, log.got...)
				log.mu.Unlock()
			} else {
				evs = drain(e)
			}
			closed, last := 0, -1
			for i, ev := range evs {
				if ev.SessionID != s {
					continue
				}
				last = i
				if ev.Type == EventClosed {
					closed++
					if ev.Reason != ReasonLocal {
						t.Fatalf("причина %q", ev.Reason)
					}
				}
			}
			if closed != 1 {
				t.Fatalf("P2P_CLOSED %d раз", closed)
			}
			if evs[last].Type != EventClosed {
				t.Fatalf("после P2P_CLOSED есть события сессии: %+v", evs[last])
			}
		})
	}
}
