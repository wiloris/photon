package dispatcher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/wiloris/photon-core/services/localfeed"
	"github.com/wiloris/photon-core/services/outbox"
	"github.com/wiloris/photon-core/services/session"
)

// Действие 813 (ADR-112, контракт §5b.6): очистка ленты канала.

func TestLocalClear_RemovesChannelAndReportsCount(t *testing.T) {
	st := openTestStorage(t)
	seed(t, st, "ch",
		localfeed.Record{ID: "p1", OrderKey: 100, Version: 100, Body: []byte(`{"m":1}`)},
		localfeed.Record{ID: "p2", OrderKey: 200, Version: 200, Body: []byte(`{"m":2}`)})
	seed(t, st, "other", localfeed.Record{ID: "o1", OrderKey: 100, Version: 100, Body: []byte(`{}`)})
	d, br := localDispatcher(t, st)

	var res struct {
		Removed int `json:"removed"`
	}
	call(t, d, actionLocalFeedClear, `{"channel_id":"ch"}`, &res)
	if res.Removed != 2 {
		t.Fatalf("removed = %d, ожидалось 2", res.Removed)
	}
	if n := br.connectCount(); n != 0 {
		t.Fatalf("813 ушёл в Bridge: подключений %d", n)
	}

	resp, err := d.SendAction(actionLocalFeedPage, []byte(`{"channel_id":"ch","limit":10}`))
	if err != nil || !strings.Contains(string(resp), `"records":[]`) {
		t.Fatalf("800 после 813: %s, %v", resp, err)
	}
	var cat struct {
		Channels []struct {
			ChannelID string `json:"channel_id"`
		} `json:"channels"`
	}
	call(t, d, actionLocalFeedCatalog, `{}`, &cat)
	if len(cat.Channels) != 1 || cat.Channels[0].ChannelID != "other" {
		t.Fatalf("каталог после 813: %+v", cat)
	}

	call(t, d, actionLocalFeedClear, `{"channel_id":"ch"}`, &res)
	if res.Removed != 0 {
		t.Fatalf("повтор: removed = %d, ожидалось 0", res.Removed)
	}
}

func TestLocalClear_OutboxIntact(t *testing.T) {
	st := openTestStorage(t)
	seed(t, st, "ch", localfeed.Record{ID: "p1", OrderKey: 100, Version: 100, Body: []byte(`{}`)})
	d, _ := localDispatcher(t, st)
	if _, err := outbox.New(st).Enqueue(context.Background(), "ch", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	var res struct {
		Removed int `json:"removed"`
	}
	call(t, d, actionLocalFeedClear, `{"channel_id":"ch"}`, &res)
	var list struct {
		Ops []wireOp `json:"ops"`
	}
	call(t, d, actionOutboxList, `{"queue":"ch"}`, &list)
	if len(list.Ops) != 1 {
		t.Fatalf("очередь после 813: %+v", list)
	}
}

func TestLocalClear_InvalidPayloadRejected(t *testing.T) {
	st := openTestStorage(t)
	d, _ := localDispatcher(t, st)
	for _, bad := range []string{`{}`, `{"channel_id":""}`, `not json`, "{\"channel_id\":\"a\\u0000b\"}"} {
		_, err := d.SendAction(actionLocalFeedClear, []byte(bad))
		if !errors.Is(err, session.ErrInvalidPayload) {
			t.Fatalf("payload %q: ошибка = %v, ожидался ErrInvalidPayload", bad, err)
		}
	}
}

func TestLocalClear_WithoutStorageFailsExplicitly(t *testing.T) {
	br := newFakeBridge()
	d, err := New(Config{Endpoint: "fake:4443", SessionToken: "t", Transport: br})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	_, err = d.SendAction(actionLocalFeedClear, []byte(`{"channel_id":"ch"}`))
	if !errors.Is(err, ErrNoStorage) {
		t.Fatalf("ошибка = %v, ожидался ErrNoStorage", err)
	}
	if n := br.connectCount(); n != 0 {
		t.Fatalf("подключений %d", n)
	}
}
