package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon-core/adapters/bbolt"
	"github.com/wiloris/photon-core/services/localfeed"
)

/*
 * Действия 802 (cursor, confirms) и 804–812 через границу ABI: настоящие
 * Photon_Create / Photon_SendAction, инстанс без секции bridge (ADR-064
 * п. 1.5), файл хранилища — свой на тест. Контракт — photon-core/docs/
 * local-actions.md.
 */

const (
	abiIngest    = 802
	abiCatalog   = 804
	abiCursor    = 805
	abiEnqueue   = 806
	abiLease     = 807
	abiComplete  = 808
	abiFail      = 809
	abiRetry     = 810
	abiCancel    = 811
	abiList      = 812
	abiPage      = 800
	abiPut       = 801
	abiGapAnchor = 803
)

type abiInstance struct {
	t *testing.T
	h unsafe.Pointer
}

func newABIInstance(t *testing.T, path string) *abiInstance {
	t.Helper()
	h, rc := tsCreate(storageConfig(t, path), false)
	if rc != tsOK || h == nil {
		t.Fatalf("Photon_Create: rc = %d", rc)
	}
	t.Cleanup(func() { tsDestroy(h) })
	return &abiInstance{t: t, h: h}
}

// send возвращает код возврата и data конверта (nil при отказе).
func (a *abiInstance) send(action int32, payload string) (int, json.RawMessage) {
	a.t.Helper()
	rc, out := tsSendAction(a.h, action, cstr(a.t, payload), true)
	if rc != tsOK {
		if out != nil {
			a.t.Fatalf("при rc=%d response_out установлен", rc)
		}
		return rc, nil
	}
	defer tsFreeString(out)
	var env struct {
		OK   bool            `json:"ok"`
		Data json.RawMessage `json:"data"`
	}
	body := tsGoString(tsStringData(out))
	if err := json.Unmarshal([]byte(body), &env); err != nil || !env.OK {
		a.t.Fatalf("конверт %d: %s (%v)", action, body, err)
	}
	return rc, env.Data
}

func (a *abiInstance) ok(action int32, payload string, dst any) {
	a.t.Helper()
	rc, data := a.send(action, payload)
	if rc != tsOK {
		a.t.Fatalf("SendAction(%d, %s): rc = %d", action, payload, rc)
	}
	if dst != nil {
		if err := json.Unmarshal(data, dst); err != nil {
			a.t.Fatalf("data %d: %v (%s)", action, err, data)
		}
	}
}

func (a *abiInstance) invalid(action int32, payload, why string) {
	a.t.Helper()
	if rc, data := a.send(action, payload); rc != tsErrInvalidArgument {
		a.t.Fatalf("%s: rc = %d (%s), ожидался PHOTON_ERR_INVALID_ARGUMENT", why, rc, data)
	}
}

type abiOp struct {
	OpID     string `json:"op_id"`
	Seq      int    `json:"seq"`
	State    string `json:"state"`
	Attempts int    `json:"attempts"`
}

func (a *abiInstance) enqueue(queue string) abiOp {
	a.t.Helper()
	var r struct {
		Op abiOp `json:"op"`
	}
	a.ok(abiEnqueue, fmt.Sprintf(`{"queue":%q,"body":{"t":"x"}}`, queue), &r)
	return r.Op
}

type abiLeaseRes struct {
	Leased bool   `json:"leased"`
	Reason string `json:"reason"`
	Op     *abiOp `json:"op"`
}

func (a *abiInstance) lease(queue string, ms int) abiLeaseRes {
	a.t.Helper()
	var r abiLeaseRes
	a.ok(abiLease, fmt.Sprintf(`{"queue":%q,"lease_ms":%d}`, queue, ms), &r)
	return r
}

type abiOpRes struct {
	Op      abiOp `json:"op"`
	Changed bool  `json:"changed"`
}

func (a *abiInstance) op(action int32, payload string) abiOpRes {
	a.t.Helper()
	var r abiOpRes
	a.ok(action, payload, &r)
	return r
}

// Полный цикл: постановка, аренда, занятая голова, истечение, подтверждение,
// следующая голова, пустая очередь.
func TestABI_OutboxFullCycle(t *testing.T) {
	a := newABIInstance(t, filepath.Join(t.TempDir(), "core.db"))

	if l := a.lease("conv", 60_000); l.Leased || l.Reason != "empty" {
		t.Fatalf("пустая очередь: %+v", l)
	}
	first := a.enqueue("conv")
	second := a.enqueue("conv")
	if first.State != "pending" || first.Seq != 1 || second.Seq != 2 {
		t.Fatalf("806: %+v %+v", first, second)
	}
	if l := a.lease("conv", 50); !l.Leased || l.Op.OpID != first.OpID || l.Op.State != "sending" {
		t.Fatalf("807: %+v", l)
	}
	if l := a.lease("conv", 50); l.Leased || l.Reason != "head_leased" || l.Op.OpID != first.OpID {
		t.Fatalf("занятая голова: %+v", l)
	}
	time.Sleep(120 * time.Millisecond)
	l := a.lease("conv", 60_000)
	if !l.Leased || l.Op.OpID != first.OpID || l.Op.Attempts != 2 {
		t.Fatalf("истёкшая аренда: %+v", l)
	}
	if r := a.op(abiComplete, fmt.Sprintf(`{"op_id":%q}`, first.OpID)); !r.Changed || r.Op.State != "sent" {
		t.Fatalf("808: %+v", r)
	}
	if l := a.lease("conv", 60_000); !l.Leased || l.Op.OpID != second.OpID {
		t.Fatalf("следующая голова: %+v", l)
	}
	var list struct {
		Ops []abiOp `json:"ops"`
	}
	a.ok(abiList, `{"queue":"conv"}`, &list)
	if len(list.Ops) != 2 || list.Ops[0].State != "sent" || list.Ops[1].State != "sending" {
		t.Fatalf("812: %+v", list.Ops)
	}
}

// Гонка «ответ потерян → синхронизация подтвердила → поздний исход»: sent
// поглощает FAIL/RETRY/COMPLETE без ошибки; CANCEL — отказ.
func TestABI_SentAbsorbsLateOutcomesAndConfirmIsAtomic(t *testing.T) {
	a := newABIInstance(t, filepath.Join(t.TempDir(), "core.db"))
	op := a.enqueue("conv")
	a.lease("conv", 60_000)

	var res struct {
		Stored   int `json:"stored"`
		Confirms struct {
			Confirmed   int `json:"confirmed"`
			AlreadySent int `json:"already_sent"`
			Unknown     int `json:"unknown"`
		} `json:"confirms"`
	}
	a.ok(abiIngest, fmt.Sprintf(`{"channel_id":"dm:peer","dir":"newest","at_end":true,
		"records":[{"id":"srv1","order_key":1757000000123,"version":1,"body":{"client_msg_id":%q}}],
		"cursor":"next-1","confirms":[%q,"other-device-op"]}`, op.OpID, op.OpID), &res)
	if res.Stored != 1 || res.Confirms.Confirmed != 1 || res.Confirms.Unknown != 1 {
		t.Fatalf("802: %+v", res)
	}
	for _, p := range []struct {
		action  int32
		payload string
	}{
		{abiFail, fmt.Sprintf(`{"op_id":%q,"kind":"transient","reason":"timeout"}`, op.OpID)},
		{abiFail, fmt.Sprintf(`{"op_id":%q,"kind":"permanent"}`, op.OpID)},
		{abiRetry, fmt.Sprintf(`{"op_id":%q}`, op.OpID)},
		{abiComplete, fmt.Sprintf(`{"op_id":%q}`, op.OpID)},
	} {
		if r := a.op(p.action, p.payload); r.Changed || r.Op.State != "sent" {
			t.Fatalf("%d на sent: %+v", p.action, r)
		}
	}
	a.invalid(abiCancel, fmt.Sprintf(`{"op_id":%q}`, op.OpID), "cancel на sent")
	a.invalid(abiComplete, `{"op_id":"00000000-0000-4000-8000-000000000000"}`, "неизвестный op_id")

	var cur struct {
		Found  bool   `json:"found"`
		Cursor string `json:"cursor"`
	}
	a.ok(abiCursor, `{"channel_id":"dm:peer"}`, &cur)
	if !cur.Found || cur.Cursor != "next-1" {
		t.Fatalf("805: %+v", cur)
	}
	var cat struct {
		Channels []struct {
			ChannelID string `json:"channel_id"`
		} `json:"channels"`
	}
	a.ok(abiCatalog, `{}`, &cat)
	if len(cat.Channels) != 1 || cat.Channels[0].ChannelID != "dm:peer" {
		t.Fatalf("804: %+v", cat)
	}
}

// Отвергнутый блок через ABI не применяет ни записей, ни курсора, ни
// подтверждений.
func TestABI_RejectedIngestAppliesNothing(t *testing.T) {
	a := newABIInstance(t, filepath.Join(t.TempDir(), "core.db"))
	op := a.enqueue("conv")
	a.lease("conv", 60_000)
	a.invalid(abiIngest, fmt.Sprintf(`{"channel_id":"dm:peer","dir":"after","anchor":{"order_key":900,"id":"z"},
		"records":[{"id":"old","order_key":1,"version":1,"body":{}}],"cursor":"c","confirms":[%q]}`, op.OpID), "блок против якоря")
	a.invalid(abiIngest, `{"channel_id":"dm:peer","dir":"newest","records":[{"id":"a","order_key":1,"version":1,"body":{}}],"cursor":""}`, "пустой курсор")

	var list struct {
		Ops []abiOp `json:"ops"`
	}
	a.ok(abiList, `{"queue":"conv"}`, &list)
	if list.Ops[0].State != "sending" {
		t.Fatalf("подтверждение пережило отказ: %+v", list.Ops[0])
	}
	var cur struct {
		Found bool `json:"found"`
	}
	a.ok(abiCursor, `{"channel_id":"dm:peer"}`, &cur)
	if cur.Found {
		t.Fatal("курсор пережил отказ")
	}
}

// ADR-095 п. 2.8 через ABI: 0x00 в channel_id — INVALID_ARGUMENT на 800–805.
func TestABI_ChannelWithNulRejected(t *testing.T) {
	a := newABIInstance(t, filepath.Join(t.TempDir(), "core.db"))
	const ch = `"a\u0000b"`
	for _, c := range []struct {
		action  int32
		payload string
	}{
		{abiPage, `{"channel_id":` + ch + `}`},
		{abiPut, `{"channel_id":` + ch + `,"records":[{"id":"x","order_key":1,"version":1,"body":{}}]}`},
		{abiIngest, `{"channel_id":` + ch + `,"dir":"newest","records":[{"id":"x","order_key":1,"version":1,"body":{}}]}`},
		{abiGapAnchor, `{"channel_id":` + ch + `}`},
		{abiCursor, `{"channel_id":` + ch + `}`},
	} {
		a.invalid(c.action, c.payload, fmt.Sprintf("%d с 0x00", c.action))
	}
}

// Файл v1 (только лента) открывается Photon_Create и поднимается до v2:
// очередь работает, лента на месте.
func TestABI_V1StorageFileUpgradesOnCreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	st, err := bboltadapter.New(path, []bboltadapter.Migration{{Version: 1, Up: func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists([]byte(localfeed.Namespace))
		return e
	}}})
	if err != nil {
		t.Fatal(err)
	}
	feed := localfeed.New(st, nil)
	if err := feed.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := feed.Put(context.Background(), "ch", []localfeed.Record{{ID: "old", OrderKey: 1, Version: 1, Body: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	st.Close(context.Background())

	a := newABIInstance(t, path)
	a.enqueue("conv")
	var page struct {
		Records []struct {
			ID string `json:"id"`
		} `json:"records"`
	}
	a.ok(abiPage, `{"channel_id":"ch"}`, &page)
	if len(page.Records) != 1 || page.Records[0].ID != "old" {
		t.Fatalf("лента после подъёма до v2: %+v", page)
	}
}

// Состояние очереди переживает Destroy и повторный Create того же файла.
func TestABI_OutboxSurvivesRecreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	h, rc := tsCreate(storageConfig(t, path), false)
	if rc != tsOK {
		t.Fatalf("Create: %d", rc)
	}
	first := &abiInstance{t: t, h: h}
	op := first.enqueue("conv")
	first.lease("conv", 600_000)
	tsDestroy(h)

	again := newABIInstance(t, path)
	var list struct {
		Ops []abiOp `json:"ops"`
	}
	again.ok(abiList, `{"queue":"conv"}`, &list)
	if len(list.Ops) != 1 || list.Ops[0].OpID != op.OpID || list.Ops[0].State != "sending" || list.Ops[0].Attempts != 1 {
		t.Fatalf("после пересоздания: %+v", list.Ops)
	}
}
