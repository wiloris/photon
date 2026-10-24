package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon/photon-core/adapters/bbolt"
	"github.com/wiloris/photon/photon-core/services/localfeed"
	"github.com/wiloris/photon/photon-core/services/outbox"
	"github.com/wiloris/photon/photon-core/services/session"
)

/*
 * Действия 802 (cursor, confirms), 804–812 — контракт docs/local-actions.md,
 * ADR-093, ADR-095. Каждый тест проверяет ответ по контракту, а не «разобралось».
 */

type wireOp struct {
	OpID       string          `json:"op_id"`
	Queue      string          `json:"queue"`
	Seq        uint64          `json:"seq"`
	Body       json.RawMessage `json:"body"`
	State      string          `json:"state"`
	Attempts   int             `json:"attempts"`
	LeaseUntil int64           `json:"lease_until"`
	RetryAt    int64           `json:"retry_at"`
	LastError  string          `json:"last_error"`
}

type opData struct {
	Op      wireOp `json:"op"`
	Changed bool   `json:"changed"`
}

type leaseData struct {
	Leased bool    `json:"leased"`
	Reason string  `json:"reason"`
	Op     *wireOp `json:"op"`
}

func call(t *testing.T, d *Dispatcher, action int32, payload string, dst any) {
	t.Helper()
	resp, err := d.SendAction(action, []byte(payload))
	if err != nil {
		t.Fatalf("SendAction(%d, %s): %v", action, payload, err)
	}
	okData(t, resp, dst)
}

func mustFailArg(t *testing.T, d *Dispatcher, action int32, payload, why string) {
	t.Helper()
	resp, err := d.SendAction(action, []byte(payload))
	if err == nil {
		t.Fatalf("%s: ожидался отказ вызова, пришло %s", why, resp)
	}
	if got := Classify(err); got != ClassInvalidArgument {
		t.Fatalf("%s: класс %v (%v), ожидался ClassInvalidArgument", why, got, err)
	}
}

func enqueue(t *testing.T, d *Dispatcher, queue, body string) wireOp {
	t.Helper()
	var r struct {
		Op wireOp `json:"op"`
	}
	call(t, d, actionOutboxEnqueue, `{"queue":"`+queue+`","body":`+body+`}`, &r)
	return r.Op
}

func lease(t *testing.T, d *Dispatcher, queue string, ms int) leaseData {
	t.Helper()
	var r leaseData
	call(t, d, actionOutboxLease, `{"queue":"`+queue+`","lease_ms":`+itoa(ms)+`}`, &r)
	return r
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func opCall(t *testing.T, d *Dispatcher, action int32, payload string) opData {
	t.Helper()
	var r opData
	call(t, d, action, payload, &r)
	return r
}

func listOps(t *testing.T, d *Dispatcher, queue string) []wireOp {
	t.Helper()
	var r struct {
		Ops []wireOp `json:"ops"`
	}
	call(t, d, actionOutboxList, `{"queue":"`+queue+`"}`, &r)
	return r.Ops
}

// ── 806 / 807 / 812 ─────────────────────────────────────────────────────

func TestOutbox_EnqueueLeaseList(t *testing.T) {
	d, br := localDispatcher(t, openTestStorage(t))

	if ops := listOps(t, d, "conv"); ops == nil || len(ops) != 0 {
		t.Fatalf("пустая очередь: ops = %v, ожидался []", ops)
	}
	a := enqueue(t, d, "conv", `{"t":"привет","n":1757000000123456789}`)
	if a.State != "pending" || a.Attempts != 0 || a.Seq != 1 || len(a.OpID) != 36 {
		t.Fatalf("806: %+v", a)
	}
	if string(a.Body) != `{"t":"привет","n":1757000000123456789}` {
		t.Fatalf("тело изменено: %s", a.Body)
	}
	l := lease(t, d, "conv", 60_000)
	if !l.Leased || l.Op == nil || l.Op.OpID != a.OpID || l.Op.State != "sending" || l.Op.Attempts != 1 || l.Op.LeaseUntil == 0 {
		t.Fatalf("807: %+v", l)
	}
	ops := listOps(t, d, "conv")
	if len(ops) != 1 || ops[0].State != "sending" {
		t.Fatalf("812: %+v", ops)
	}
	if n := br.connectCount(); n != 0 {
		t.Fatalf("действия очереди ушли в Bridge: подключений %d", n)
	}
}

// Пустая очередь и занятая голова — РЕЗУЛЬТАТ 807, не ошибка.
func TestOutbox_LeaseOutcomesAreResults(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))

	if l := lease(t, d, "conv", 60_000); l.Leased || l.Reason != "empty" || l.Op != nil {
		t.Fatalf("пустая очередь: %+v", l)
	}
	a := enqueue(t, d, "conv", `{}`)
	enqueue(t, d, "conv", `{}`)
	lease(t, d, "conv", 60_000)
	l := lease(t, d, "conv", 60_000)
	if l.Leased || l.Reason != "head_leased" || l.Op == nil || l.Op.OpID != a.OpID || l.Op.LeaseUntil == 0 {
		t.Fatalf("занятая голова: %+v", l)
	}
}

func TestOutbox_LeaseExpiryReleasesSameOp(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))
	a := enqueue(t, d, "conv", `{}`)
	lease(t, d, "conv", 50)
	time.Sleep(120 * time.Millisecond)

	if ops := listOps(t, d, "conv"); ops[0].State != "pending" || ops[0].LeaseUntil != 0 {
		t.Fatalf("истёкшая аренда в 812: %+v", ops[0])
	}
	l := lease(t, d, "conv", 60_000)
	if !l.Leased || l.Op.OpID != a.OpID || l.Op.Attempts != 2 {
		t.Fatalf("повторная аренда: %+v — тот же op_id, attempts 2", l)
	}
}

// ── 808–811 ─────────────────────────────────────────────────────────────

func TestOutbox_TransientKeepsHeadPermanentMovesOn(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))
	a := enqueue(t, d, "conv", `{"n":1}`)
	b := enqueue(t, d, "conv", `{"n":2}`)
	lease(t, d, "conv", 60_000)

	r := opCall(t, d, actionOutboxFail, `{"op_id":"`+a.OpID+`","kind":"transient","reason":"network","retry_at_ms":1757000000000}`)
	if !r.Changed || r.Op.State != "pending" || r.Op.RetryAt != 1757000000000 || r.Op.LastError != "network" {
		t.Fatalf("transient: %+v", r)
	}
	if l := lease(t, d, "conv", 60_000); l.Op.OpID != a.OpID {
		t.Fatalf("после transient голова %s, ожидалась %s", l.Op.OpID, a.OpID)
	}
	r = opCall(t, d, actionOutboxFail, `{"op_id":"`+a.OpID+`","kind":"permanent","reason":"forbidden"}`)
	if !r.Changed || r.Op.State != "failed" {
		t.Fatalf("permanent: %+v", r)
	}
	if l := lease(t, d, "conv", 60_000); l.Op.OpID != b.OpID {
		t.Fatalf("после permanent голова %s, ожидалась %s", l.Op.OpID, b.OpID)
	}
	// Повторный permanent — no-op.
	if r := opCall(t, d, actionOutboxFail, `{"op_id":"`+a.OpID+`","kind":"permanent"}`); r.Changed {
		t.Fatalf("повторный permanent изменил: %+v", r)
	}
	// transient из failed — запрещённый переход.
	mustFailArg(t, d, actionOutboxFail, `{"op_id":"`+a.OpID+`","kind":"transient"}`, "transient из failed")
}

func TestOutbox_FailPayloadRules(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))
	a := enqueue(t, d, "conv", `{}`)
	mustFailArg(t, d, actionOutboxFail, `{"op_id":"`+a.OpID+`","kind":"transient_typo"}`, "неизвестный kind")
	mustFailArg(t, d, actionOutboxFail, `{"op_id":"`+a.OpID+`"}`, "kind отсутствует")
	mustFailArg(t, d, actionOutboxFail, `{"op_id":"`+a.OpID+`","kind":"permanent","retry_at_ms":1}`, "retry_at_ms при permanent")
	if ops := listOps(t, d, "conv"); ops[0].State != "pending" {
		t.Fatalf("отвергнутый FAIL изменил операцию: %+v", ops[0])
	}
}

func TestOutbox_RetryFailedKeepsIdentity(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))
	a := enqueue(t, d, "conv", `{}`)
	lease(t, d, "conv", 60_000)
	opCall(t, d, actionOutboxFail, `{"op_id":"`+a.OpID+`","kind":"permanent"}`)

	r := opCall(t, d, actionOutboxRetry, `{"op_id":"`+a.OpID+`"}`)
	if !r.Changed || r.Op.State != "pending" || r.Op.OpID != a.OpID || r.Op.Seq != a.Seq || r.Op.Attempts != 1 {
		t.Fatalf("retry failed: %+v", r)
	}
	if r := opCall(t, d, actionOutboxRetry, `{"op_id":"`+a.OpID+`"}`); r.Changed {
		t.Fatalf("retry pending изменил: %+v", r)
	}
	lease(t, d, "conv", 60_000)
	mustFailArg(t, d, actionOutboxRetry, `{"op_id":"`+a.OpID+`"}`, "retry в аренде")
}

// sent поглощает поздние исходы; CANCEL на sent — отказ.
func TestOutbox_SentAbsorbsLateOutcomes(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))
	a := enqueue(t, d, "conv", `{}`)
	lease(t, d, "conv", 60_000)
	if r := opCall(t, d, actionOutboxComplete, `{"op_id":"`+a.OpID+`"}`); !r.Changed || r.Op.State != "sent" {
		t.Fatalf("808: %+v", r)
	}
	for _, c := range []struct {
		action  int32
		payload string
	}{
		{actionOutboxComplete, `{"op_id":"` + a.OpID + `"}`},
		{actionOutboxFail, `{"op_id":"` + a.OpID + `","kind":"transient","reason":"timeout"}`},
		{actionOutboxFail, `{"op_id":"` + a.OpID + `","kind":"permanent","reason":"forbidden"}`},
		{actionOutboxRetry, `{"op_id":"` + a.OpID + `"}`},
	} {
		if r := opCall(t, d, c.action, c.payload); r.Changed || r.Op.State != "sent" || r.Op.LastError != "" {
			t.Fatalf("%d на sent: %+v — ожидался no-op без ошибки", c.action, r)
		}
	}
	mustFailArg(t, d, actionOutboxCancel, `{"op_id":"`+a.OpID+`"}`, "cancel на sent")
}

func TestOutbox_CancelRules(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))
	never := enqueue(t, d, "conv", `{}`)
	var c struct {
		Cancelled bool `json:"cancelled"`
	}
	call(t, d, actionOutboxCancel, `{"op_id":"`+never.OpID+`"}`, &c)
	if !c.Cancelled {
		t.Fatal("отмена ни разу не выданной")
	}
	mustFailArg(t, d, actionOutboxCancel, `{"op_id":"`+never.OpID+`"}`, "повторная отмена")

	tried := enqueue(t, d, "conv", `{}`)
	lease(t, d, "conv", 50)
	mustFailArg(t, d, actionOutboxCancel, `{"op_id":"`+tried.OpID+`"}`, "cancel в аренде")
	time.Sleep(120 * time.Millisecond)
	mustFailArg(t, d, actionOutboxCancel, `{"op_id":"`+tried.OpID+`"}`, "cancel выданной хотя бы раз")
	opCall(t, d, actionOutboxFail, `{"op_id":"`+tried.OpID+`","kind":"permanent"}`)
	call(t, d, actionOutboxCancel, `{"op_id":"`+tried.OpID+`"}`, &c)
}

func TestOutbox_UnknownOpIDIsArgumentError(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))
	enqueue(t, d, "conv", `{}`)
	const ghost = `{"op_id":"00000000-0000-4000-8000-000000000000","kind":"transient"}`
	for _, a := range []int32{actionOutboxComplete, actionOutboxFail, actionOutboxRetry, actionOutboxCancel} {
		resp, err := d.SendAction(a, []byte(ghost))
		if err == nil || !errors.Is(err, outbox.ErrUnknownOp) || Classify(err) != ClassInvalidArgument {
			t.Fatalf("%d с неизвестным op_id: resp=%s err=%v", a, resp, err)
		}
	}
}

func TestOutbox_EnqueueValidation(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))
	mustFailArg(t, d, actionOutboxEnqueue, `{"queue":"","body":{}}`, "пустая очередь")
	mustFailArg(t, d, actionOutboxEnqueue, `{"queue":"a\u0000b","body":{}}`, "очередь с 0x00")
	mustFailArg(t, d, actionOutboxEnqueue, `{"queue":"q"}`, "без body")
	mustFailArg(t, d, actionOutboxLease, `{"queue":"q","lease_ms":0}`, "аренда без срока")
	mustFailArg(t, d, actionOutboxList, `{"queue":""}`, "812 без очереди")
}

// ── 802 cursor + confirms, 804, 805 ─────────────────────────────────────

func TestIngest_CursorAndConfirmsInOneTransaction(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))
	a := enqueue(t, d, "conv", `{"t":"a"}`)
	lease(t, d, "conv", 60_000)

	var res struct {
		ingestData
		Confirms outbox.ConfirmResult `json:"confirms"`
	}
	call(t, d, actionLocalFeedIngest, `{"channel_id":"dm:x","dir":"newest","at_end":true,
		"records":[{"id":"srv1","order_key":100,"version":100,"body":{"client_msg_id":"`+a.OpID+`"}}],
		"cursor":"c1","confirms":["`+a.OpID+`","ghost-op"]}`, &res)
	if res.Stored != 1 || res.Confirms != (outbox.ConfirmResult{Confirmed: 1, Unknown: 1}) {
		t.Fatalf("802: %+v", res)
	}
	if ops := listOps(t, d, "conv"); ops[0].State != "sent" {
		t.Fatalf("подтверждение не применено: %+v", ops[0])
	}
	var cur struct {
		Found  bool   `json:"found"`
		Cursor string `json:"cursor"`
	}
	call(t, d, actionLocalFeedCursor, `{"channel_id":"dm:x"}`, &cur)
	if !cur.Found || cur.Cursor != "c1" {
		t.Fatalf("805: %+v", cur)
	}
	var cat struct {
		Channels []struct {
			ChannelID string       `json:"channel_id"`
			Newest    wirePosition `json:"newest"`
		} `json:"channels"`
	}
	call(t, d, actionLocalFeedCatalog, `{}`, &cat)
	if len(cat.Channels) != 1 || cat.Channels[0].ChannelID != "dm:x" || cat.Channels[0].Newest != (wirePosition{100, "srv1"}) {
		t.Fatalf("804: %+v", cat)
	}
}

// Отвергнутый блок не оставляет ни записей, ни курсора, ни подтверждений.
func TestIngest_RejectedBlockAppliesNothing(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))
	a := enqueue(t, d, "conv", `{}`)
	lease(t, d, "conv", 60_000)
	// Запись старше якоря при after — ErrRecordCrossesAnchor.
	mustFailArg(t, d, actionLocalFeedIngest, `{"channel_id":"dm:x","dir":"after","anchor":{"order_key":500,"id":"z"},
		"records":[{"id":"old","order_key":100,"version":100,"body":{}}],"cursor":"c1","confirms":["`+a.OpID+`"]}`, "запись по ту сторону якоря")

	if ops := listOps(t, d, "conv"); ops[0].State != "sending" {
		t.Fatalf("подтверждение пережило отказ блока: %+v", ops[0])
	}
	var cur struct {
		Found bool `json:"found"`
	}
	call(t, d, actionLocalFeedCursor, `{"channel_id":"dm:x"}`, &cur)
	if cur.Found {
		t.Fatal("курсор пережил отказ блока")
	}
	var cat struct {
		Channels []any `json:"channels"`
	}
	call(t, d, actionLocalFeedCatalog, `{}`, &cat)
	if cat.Channels == nil || len(cat.Channels) != 0 {
		t.Fatalf("каталог после отказа: %+v", cat)
	}
}

func TestIngest_CursorFieldRules(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))
	blk := func(id string, ok int, extra string) string {
		return `{"channel_id":"ch","dir":"newest","records":[{"id":"` + id + `","order_key":` + itoa(ok) + `,"version":1,"body":{}}]` + extra + `}`
	}
	mustFailArg(t, d, actionLocalFeedIngest, blk("a", 100, `,"cursor":""`), "пустой курсор")
	call(t, d, actionLocalFeedIngest, blk("a", 100, `,"cursor":"c1"`), nil)
	// Без поля — курсор не меняется; confirms без поля — нули.
	var res struct {
		Confirms outbox.ConfirmResult `json:"confirms"`
	}
	call(t, d, actionLocalFeedIngest, blk("b", 200, ``), &res)
	if res.Confirms != (outbox.ConfirmResult{}) {
		t.Fatalf("confirms без поля: %+v", res.Confirms)
	}
	var cur struct {
		Cursor string `json:"cursor"`
	}
	call(t, d, actionLocalFeedCursor, `{"channel_id":"ch"}`, &cur)
	if cur.Cursor != "c1" {
		t.Fatalf("курсор изменился без поля: %q", cur.Cursor)
	}
}

// ADR-095 п. 2.8: channel_id с 0x00 отвергается на 800–805.
func TestLocalFeed_ChannelWithNulRejected(t *testing.T) {
	d, _ := localDispatcher(t, openTestStorage(t))
	const ch = `"a\u0000b"`
	for _, c := range []struct {
		action  int32
		payload string
	}{
		{actionLocalFeedPage, `{"channel_id":` + ch + `}`},
		{actionLocalFeedPut, `{"channel_id":` + ch + `,"records":[{"id":"x","order_key":1,"version":1,"body":{}}]}`},
		{actionLocalFeedIngest, `{"channel_id":` + ch + `,"dir":"newest","records":[{"id":"x","order_key":1,"version":1,"body":{}}]}`},
		{actionLocalFeedGapAnchor, `{"channel_id":` + ch + `}`},
		{actionLocalFeedCursor, `{"channel_id":` + ch + `}`},
	} {
		resp, err := d.SendAction(c.action, []byte(c.payload))
		if err == nil || !errors.Is(err, localfeed.ErrInvalidChannelID) || Classify(err) != ClassInvalidArgument {
			t.Fatalf("%d с 0x00 в channel_id: resp=%s err=%v", c.action, resp, err)
		}
	}
	var cat struct {
		Channels []any `json:"channels"`
	}
	call(t, d, actionLocalFeedCatalog, `{}`, &cat)
	if len(cat.Channels) != 0 {
		t.Fatalf("канал с 0x00 попал в каталог: %+v", cat)
	}
}

// ── хранилище инстанса ──────────────────────────────────────────────────

// Файл v1 (только лента) поднимается до v2 при открытии инстансом; данные
// ленты не теряются.
func TestStorage_V1FileMigratesToV2(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	v1, err := bboltadapter.New(path, []bboltadapter.Migration{{Version: 1, Up: func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists([]byte(localfeed.Namespace))
		return e
	}}})
	if err != nil {
		t.Fatal(err)
	}
	feed := localfeed.New(v1, nil)
	if err := feed.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := feed.Put(context.Background(), "ch", []localfeed.Record{{ID: "old", OrderKey: 1, Version: 1, Body: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	v1.Close(context.Background())

	d, err := New(Config{StoragePath: path})
	if err != nil {
		t.Fatalf("открытие файла v1: %v", err)
	}
	defer d.Close()
	enqueue(t, d, "conv", `{}`)
	var page struct {
		Records []struct {
			ID string `json:"id"`
		} `json:"records"`
	}
	call(t, d, actionLocalFeedPage, `{"channel_id":"ch"}`, &page)
	if len(page.Records) != 1 || page.Records[0].ID != "old" {
		t.Fatalf("данные ленты после миграции: %+v", page)
	}
}

// Подставленное хранилище без миграции v2 — отказ создания, а не отказ на
// первом действии очереди.
func TestStorage_InjectedWithoutOutboxNamespaceFailsCreate(t *testing.T) {
	st, err := bboltadapter.New(filepath.Join(t.TempDir(), "v1only.db"), []bboltadapter.Migration{{Version: 1, Up: func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists([]byte(localfeed.Namespace))
		return e
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close(context.Background())
	_, err = New(Config{Storage: st})
	if !errors.Is(err, ErrStorageUnavailable) || !strings.Contains(err.Error(), outbox.Namespace) {
		t.Fatalf("New без namespace очереди: %v", err)
	}
}

// Каждый sentinel outbox либо ошибка аргумента, либо результат 807 — третьего
// не бывает, и умолчанием ничего не становится.
func TestOutbox_EverySentinelIsClassified(t *testing.T) {
	names := sentinelNames(t, "../outbox/outbox.go")
	byName := map[string]error{}
	for _, e := range outboxArgErrors {
		byName[sentinelName(e)] = e
	}
	for _, e := range outboxResultNotErrors {
		byName[sentinelName(e)] = e
	}
	if len(names) == 0 {
		t.Fatal("в outbox не найдено ни одного Err*")
	}
	for _, n := range names {
		if _, ok := byName[n]; !ok {
			t.Fatalf("sentinel outbox.%s не отнесён ни к outboxArgErrors, ни к outboxResultNotErrors", n)
		}
	}
	if len(byName) != len(names) {
		t.Fatalf("в списках %d sentinel'ов, в исходниках %d", len(byName), len(names))
	}
}

func sentinelName(e error) string {
	for n, v := range map[string]error{
		"ErrInvalidQueue": outbox.ErrInvalidQueue, "ErrInvalidBody": outbox.ErrInvalidBody,
		"ErrInvalidLease": outbox.ErrInvalidLease, "ErrUnknownOp": outbox.ErrUnknownOp,
		"ErrStateConflict": outbox.ErrStateConflict, "ErrQueueEmpty": outbox.ErrQueueEmpty,
		"ErrHeadLeased": outbox.ErrHeadLeased,
	} {
		if v == e {
			return n
		}
	}
	return "?"
}

// Ошибка аргумента очереди — не сбой хранилища: класс берётся из classTable
// и так, но обёртка «storage is unavailable» отправила бы вызывающего
// проверять диск вместо payload (текст GetLastError). Проверяется и на
// обёртке, и сквозь действие.
func TestOutbox_ArgErrorsAreNotStorageFailures(t *testing.T) {
	for _, sentinel := range outboxArgErrors {
		err := wrapOutboxError(fmt.Errorf("ctx: %w", sentinel))
		if errors.Is(err, ErrStorageUnavailable) {
			t.Fatalf("%v обёрнута как сбой хранилища: %v", sentinel, err)
		}
		if !errors.Is(err, session.ErrInvalidPayload) || !errors.Is(err, sentinel) {
			t.Fatalf("%v: обёртка потеряла класс или sentinel: %v", sentinel, err)
		}
	}
	d, _ := localDispatcher(t, openTestStorage(t))
	_, err := d.SendAction(actionOutboxComplete, []byte(`{"op_id":"00000000-0000-4000-8000-000000000000"}`))
	if err == nil || errors.Is(err, ErrStorageUnavailable) || strings.Contains(err.Error(), "storage is unavailable") {
		t.Fatalf("неизвестный op_id описан как сбой хранилища: %v", err)
	}
	// А настоящий сбой — по-прежнему INTERNAL.
	if got := Classify(wrapOutboxError(errors.New("bbolt: database not open"))); got != ClassInternal {
		t.Fatalf("сбой хранилища очереди: класс %v", got)
	}
}
