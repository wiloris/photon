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

func (r *PushRegistry) Broadcast(event PushEvent) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if event.UserID != "" {
		for _, client := range r.clients[event.UserID] {
			select {
			case client.send <- event:
			default:
			}
		}
		return
	}

	for _, clients := range r.clients {
		for _, client := range clients {
			select {
			case client.send <- event:
			default:
			}
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