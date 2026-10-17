package main

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/quic-go/quic-go"
)

// PushEvent — конверт push-уведомления от сервера к клиенту.
type PushEvent struct {
	Event  string          `json:"event"`
	UserID string          `json:"user_id,omitempty"`
	Data   json.RawMessage `json:"data"`

	// ChannelID — канал, к которому относится событие. Служебное поле
	// маршрутизации: аудитория считается по нему (ADR-040), а на провод оно
	// не идёт — форма push для варианта A не меняется. Заполняется хуком, а
	// не разбором Data: §3 ADR-040 требует явного поля.
	ChannelID string `json:"-"`

	// To — адресат события разговора ENGRAM (ADR-123 §2.2): пользователь
	// Mattermost, вычисленный сервером. Служебное поле, на провод не идёт:
	// клиенту ENGRAM mm_user_id не выдаётся (ADR-081 §2.5), в том числе свой.
	To string `json:"-"`
}

// PushClient — один подписанный клиент в subscribe-режиме.
type PushClient struct {
	userID string
	conn   *quic.Conn
	send   chan PushEvent
	done   chan struct{}
	once   sync.Once
}

func newPushClient(userID string, conn *quic.Conn) *PushClient {
	c := &PushClient{
		userID: userID,
		conn:   conn,
		send:   make(chan PushEvent, 64),
		done:   make(chan struct{}),
	}
	go c.writeLoop()
	go c.watchClose()
	return c
}

func (c *PushClient) writeLoop() {
	for {
		select {
		case <-c.done:
			return
		case event, ok := <-c.send:
			if !ok {
				return
			}
			c.sendEvent(event)
		}
	}
}

func (c *PushClient) sendEvent(event PushEvent) {
	stream, err := c.conn.OpenUniStreamSync(context.Background())
	if err != nil {
		c.close()
		return
	}
	defer stream.Close()
	if err := json.NewEncoder(stream).Encode(event); err != nil {
		c.close()
	}
}

func (c *PushClient) watchClose() {
	<-c.conn.Context().Done()
	c.close()
}

func (c *PushClient) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.conn.CloseWithError(0, "client disconnected")
	})
}

// ── PushRegistry ──────────────────────────────────────────────────────────────

type PushRegistry struct {
	mu      sync.RWMutex
	clients map[string][]*PushClient
}

func NewPushRegistry() *PushRegistry {
	return &PushRegistry{
		clients: make(map[string][]*PushClient),
	}
}

func (r *PushRegistry) Register(userID string, conn *quic.Conn) *PushClient {
	client := newPushClient(userID, conn)

	r.mu.Lock()
	r.clients[userID] = append(r.clients[userID], client)
	r.mu.Unlock()

	go func() {
		<-client.done
		r.unregister(userID, client)
	}()

	return client
}

func (r *PushRegistry) unregister(userID string, target *PushClient) {
	r.mu.Lock()
	defer r.mu.Unlock()
	list := r.clients[userID]
	updated := list[:0]
	for _, c := range list {
		if c != target {
			updated = append(updated, c)
		}
	}
	if len(updated) == 0 {
		delete(r.clients, userID)
	} else {
		r.clients[userID] = updated
	}
}

// subscribers — идентификаторы подписанных пользователей. Нужен PushRouter'у
// для вычисления аудитории (ADR-040).
func (r *PushRegistry) subscribers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.clients))
	for userID := range r.clients {
		out = append(out, userID)
	}
	return out
}

// sendTo — доставка одному адресату. Переполненный буфер по-прежнему роняет
// событие молча (см. bridge-session.md §9 п. 6 — открыто, этой правкой не
// закрывается).
func (r *PushRegistry) sendTo(userID string, event PushEvent) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, client := range r.clients[userID] {
		select {
		case client.send <- event:
		default:
		}
	}
}

// Broadcast оставлен только для адресных событий. Ветка «без адресата — всем»
// удалена: ADR-040 требует fail-closed, а не рассылку неизвестной аудитории.
func (r *PushRegistry) Broadcast(event PushEvent) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if event.UserID == "" {
		return // аудитория неизвестна — не рассылаем (ADR-040)
	}
	for _, client := range r.clients[event.UserID] {
		select {
		case client.send <- event:
		default:
		}
	}
}

func (r *PushRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	total := 0
	for _, list := range r.clients {
		total += len(list)
	}
	return total
}
