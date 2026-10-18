package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Медиа M1 «Файл в разговоре» против НАСТОЯЩЕГО PostgreSQL (ADR-124 §7;
// implementation contract §8, acceptance §8.11). PHOTON_DM_PG_DSN. Байты —
// MediaService поверх хранилища в памяти; права и привязка — engram_media.

type mediaRig struct {
	*groupRig
	media *MediaService
	store *fakeMediaStore
}

func newMediaRig(t *testing.T) *mediaRig {
	t.Helper()
	st := newFakeMediaStore()
	return &mediaRig{groupRig: newGroupRig(t, 2000), media: NewMediaService(st), store: st}
}

func sha256Of(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func uploadReq(convID, key, name string, data []byte) map[string]any {
	return map[string]any{"conversation_id": convID, "upload_key": key, "kind": "file", "name": name,
		"sha256": sha256Of(data), "body_length": len(data)}
}

func (r *mediaRig) uploadRaw(t *testing.T, callerMM string, req map[string]any, body []byte) Response {
	t.Helper()
	return r.svc.uploadMedia(context.Background(), r.media, callerMM, pl(t, req), bytes.NewReader(body))
}

func (r *mediaRig) upload(t *testing.T, callerMM, convID, key, name string, data []byte) Response {
	t.Helper()
	return r.uploadRaw(t, callerMM, uploadReq(convID, key, name, data), data)
}

func (r *mediaRig) mustUpload(t *testing.T, callerMM, convID, key, name string, data []byte) string {
	t.Helper()
	resp := r.upload(t, callerMM, convID, key, name, data)
	require.True(t, resp.OK, "703: %+v", resp)
	res := resp.Data.(convMediaUploadResult)
	require.Equal(t, int64(len(data)), res.Size)
	require.Equal(t, sha256Of(data), res.SHA256)
	return res.MediaID
}

func sendWithMedia(convID, cid, text string, ids ...string) map[string]any {
	req := sendReq(convID, cid, text)
	atts := make([]map[string]string, len(ids))
	for i, id := range ids {
		atts[i] = map[string]string{"media_id": id}
	}
	req["attachments"] = atts
	return req
}

func (r *mediaRig) sendMedia(t *testing.T, callerMM, convID, cid, text string, ids ...string) Response {
	t.Helper()
	return r.svc.dispatch(context.Background(), callerMM, ActionConversationSend, pl(t, sendWithMedia(convID, cid, text, ids...)))
}

func (r *mediaRig) info(t *testing.T, callerMM, mediaID string) Response {
	t.Helper()
	return r.svc.mediaInfo(context.Background(), callerMM, pl(t, map[string]string{"media_id": mediaID}))
}

// download — 704 на уровне сервиса: проверка доступа, затем байты из хранилища.
func (r *mediaRig) download(t *testing.T, callerMM, mediaID string) ([]byte, string) {
	t.Helper()
	m, code := r.svc.mediaForDownload(context.Background(), callerMM, pl(t, map[string]string{"media_id": mediaID}))
	if code != "" {
		return nil, code
	}
	var buf bytes.Buffer
	_, err := r.media.Download(m.mediaID, &buf)
	require.NoError(t, err)
	return buf.Bytes(), ""
}

func (r *mediaRig) syncWith(t *testing.T, callerMM, convID string, features []string) ConvSyncResponse {
	t.Helper()
	req := map[string]any{"conversation_id": convID}
	if features != nil {
		req["features"] = features
	}
	resp := r.syncRaw(t, callerMM, req)
	require.True(t, resp.OK, "931: %+v", resp)
	return resp.Data.(ConvSyncResponse)
}

func messageByID(t *testing.T, items []any, id string) ConvMessageItem {
	t.Helper()
	for _, it := range items {
		if m, ok := it.(ConvMessageItem); ok && m.ID == id {
			return m
		}
	}
	t.Fatalf("сообщение %s не найдено в ленте", id)
	return ConvMessageItem{}
}

// Сквозной путь сервиса: загрузка → 930 → 931 с признаком → 705 → 704,
// байты совпадают с оригиналом.
func TestM1UploadSendSyncDownload(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	data := []byte(strings.Repeat("photon-m1 ", 1000))

	mid := r.mustUpload(t, a, id, "op1:0", "отчёт.pdf", data)
	msg := msgOf(t, r.sendMedia(t, a, id, newRequestID(), "смотри", mid))
	require.Len(t, msg.Attachments, 1)
	require.Equal(t, ConvAttachment{MediaID: mid, Kind: "file", Mime: convMediaMime, Name: "отчёт.pdf",
		Size: int64(len(data)), SHA256: sha256Of(data)}, msg.Attachments[0])

	it := messageByID(t, r.syncWith(t, b, id, []string{"attachments"}).Items, msg.ID)
	require.Equal(t, "смотри", it.Text)
	require.Equal(t, msg.Attachments, it.Attachments)

	info := r.info(t, b, mid)
	require.True(t, info.OK, "705: %+v", info)
	require.True(t, info.Data.(convMediaInfoResult).Bound)

	got, code := r.download(t, b, mid)
	require.Empty(t, code)
	require.Equal(t, data, got)
}

// Клиент без признака получает текст с заглушкой и без attachments; хранимое
// одинаково (contract §8.8). То же в 902 (last_item) и 941.
func TestM1LegacyClientGetsPlaceholder(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	m1 := r.mustUpload(t, a, id, "k1", "a.txt", []byte("one"))
	m2 := r.mustUpload(t, a, id, "k2", "b.txt", []byte("two"))
	_ = m2
	one := msgOf(t, r.sendMedia(t, a, id, newRequestID(), "", m1))

	items := r.syncWith(t, b, id, nil).Items
	legacy := messageByID(t, items, one.ID)
	require.Equal(t, "[Вложение: a.txt]", legacy.Text)
	require.Nil(t, legacy.Attachments)

	lastLegacy := r.listOpts(t, b, map[string]any{}).Conversations
	lastNew := r.listOpts(t, b, map[string]any{"features": []string{"attachments"}}).Conversations
	require.Len(t, lastLegacy, 1)
	require.Len(t, lastNew, 1)
	require.Equal(t, "[Вложение: a.txt]", lastLegacy[0].LastItem.(ConvMessageItem).Text)
	require.Len(t, lastNew[0].LastItem.(ConvMessageItem).Attachments, 1)
}

// Несколько вложений: порядок запроса сохраняется; заглушка — число.
func TestM1MultipleAttachmentsOrderAndPlaceholder(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	m1 := r.mustUpload(t, a, id, "k1", "a.txt", []byte("one"))
	m2 := r.mustUpload(t, a, id, "k2", "b.txt", []byte("two"))
	msg := msgOf(t, r.sendMedia(t, a, id, newRequestID(), "подпись", m2, m1))
	require.Equal(t, []string{m2, m1}, []string{msg.Attachments[0].MediaID, msg.Attachments[1].MediaID})

	it := messageByID(t, r.syncWith(t, b, id, []string{"attachments"}).Items, msg.ID)
	require.Equal(t, msg.Attachments, it.Attachments)
	legacy := messageByID(t, r.syncWith(t, b, id, nil).Items, msg.ID)
	require.Equal(t, "подпись\n[Вложения: 2]", legacy.Text)

	hits := r.search(t, b, "query", "подпись", "features", []string{"attachments"})
	require.Len(t, hits.Hits, 1)
	require.Len(t, hits.Hits[0].Item.(ConvMessageItem).Attachments, 2)
}

// Повтор загрузки тем же ключом: тот же объект, второго нет; другое
// содержимое под тем же ключом — конфликт (ADR-124 §7 M1-6).
func TestM1UploadIdempotentByKey(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	data := []byte("same bytes")
	m1 := r.mustUpload(t, a, id, "op:0", "x.bin", data)
	m2 := r.mustUpload(t, a, id, "op:0", "x.bin", data)
	require.Equal(t, m1, m2)
	require.Equal(t, 1, r.store.objectCount(), "повтор не создаёт второго объекта")

	resp := r.upload(t, a, id, "op:0", "x.bin", []byte("other bytes"))
	require.Equal(t, convErrIdempotency, resp.Error)
	resp = r.upload(t, a, id, "op:0", "y.bin", data)
	require.Equal(t, convErrIdempotency, resp.Error)
}

// Одновременные загрузки с одним ключом: у всех один и тот же media_id,
// строка одна (частичный уникальный индекс).
func TestM1ConcurrentUploadSameKey(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	data := []byte("concurrent")
	const n = 8
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := r.upload(t, a, id, "race:0", "c.bin", data)
			if resp.OK {
				ids[i] = resp.Data.(convMediaUploadResult).MediaID
			}
		}(i)
	}
	wg.Wait()
	for _, x := range ids {
		require.Equal(t, ids[0], x)
	}
	var rows int
	require.NoError(t, r.db.QueryRow(`SELECT count(*) FROM engram_media WHERE upload_key = 'race:0'`).Scan(&rows))
	require.Equal(t, 1, rows)
}

// Отказы загрузки: форма, вид, пределы, несовпадение длины и хэша, допуск.
func TestM1UploadRejects(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB)
	data := []byte("payload")

	bad := func(mut func(map[string]any)) Response {
		req := uploadReq(id, "k", "f.bin", data)
		mut(req)
		return r.uploadRaw(t, a, req, data)
	}
	for name, mut := range map[string]func(map[string]any){
		"вид image":           func(m map[string]any) { m["kind"] = "image" },
		"ключ с пробелом":     func(m map[string]any) { m["upload_key"] = "a b" },
		"ключ длиннее 100":    func(m map[string]any) { m["upload_key"] = strings.Repeat("k", 101) },
		"имя с /":             func(m map[string]any) { m["name"] = "a/b" },
		"пустое имя":          func(m map[string]any) { m["name"] = "" },
		"sha не hex":          func(m map[string]any) { m["sha256"] = strings.Repeat("z", 64) },
		"длина 0":             func(m map[string]any) { m["body_length"] = 0 },
		"длина больше 100MiB": func(m map[string]any) { m["body_length"] = MaxMediaBytes + 1 },
		"неизвестное поле":    func(m map[string]any) { m["mime"] = "image/png" },
	} {
		require.Equal(t, convErrInvalidRequest, bad(mut).Error, name)
	}
	// Тело короче объявленного и чужой хэш — объект не создаётся.
	short := uploadReq(id, "short", "f.bin", data)
	require.Equal(t, convErrInvalidRequest, r.uploadRaw(t, a, short, data[:3]).Error)
	wrong := uploadReq(id, "wrong", "f.bin", data)
	wrong["sha256"] = sha256Of([]byte("other"))
	require.Equal(t, convErrInvalidRequest, r.uploadRaw(t, a, wrong, data).Error)
	require.Equal(t, 0, r.store.objectCount())
	var rows int
	require.NoError(t, r.db.QueryRow(`SELECT count(*) FROM engram_media`).Scan(&rows))
	require.Zero(t, rows)

	// Посторонний не загружает в чужой разговор.
	require.Equal(t, convErrConversationNF, r.upload(t, c, id, "c", "f.bin", data).Error)
}

// Посторонний и бывший участник не получают ни сведений, ни байтов; ответ
// неотличим от несуществующего объекта.
func TestM1OutsiderAndFormerMemberDenied(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB)
	mid := r.mustUpload(t, a, id, "k", "s.bin", []byte("secret"))
	msgOf(t, r.sendMedia(t, a, id, newRequestID(), "x", mid))

	require.Equal(t, convErrForbidden, r.info(t, c, mid).Error)
	_, code := r.download(t, c, mid)
	require.Equal(t, convErrForbidden, code)
	require.Equal(t, convErrForbidden, r.info(t, c, "no-such-media").Error, "несуществующий — тот же отказ")

	r.mustDo(t, b, ActionConversationLeave, reqOf(id))
	_, code = r.download(t, b, mid)
	require.Equal(t, convErrForbidden, code, "вышедший теряет доступ вместе с лентой")
}

// Непривязанный объект: только загрузивший видит его через 705 (bound=false);
// скачать его нельзя никому.
func TestM1UnboundVisibleOnlyToUploader(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	mid := r.mustUpload(t, a, id, "k", "u.bin", []byte("unbound"))
	info := r.info(t, a, mid)
	require.True(t, info.OK)
	require.False(t, info.Data.(convMediaInfoResult).Bound)
	require.Equal(t, convErrForbidden, r.info(t, b, mid).Error)
	_, code := r.download(t, a, mid)
	require.Equal(t, convErrForbidden, code)
}

// Правила привязки в 930: чужой объект, объект другого разговора и уже
// привязанный — forbidden, позиция не создаётся; форма attachments.
func TestM1BindRules(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	other := r.group(t, a, pidB)
	ofB := r.mustUpload(t, b, id, "k", "b.bin", []byte("of b"))
	inOther := r.mustUpload(t, a, other, "k", "o.bin", []byte("other conv"))
	mine := r.mustUpload(t, a, id, "k2", "m.bin", []byte("mine"))

	h := r.posH(t, id)
	require.Equal(t, convErrForbidden, r.sendMedia(t, a, id, newRequestID(), "x", ofB).Error)
	require.Equal(t, convErrForbidden, r.sendMedia(t, a, id, newRequestID(), "x", inOther).Error)
	require.Equal(t, convErrForbidden, r.sendMedia(t, a, id, newRequestID(), "x", mine, "nosuch").Error)
	require.Equal(t, h, r.posH(t, id), "откат транзакции резерва: позиция не выдана")

	msgOf(t, r.sendMedia(t, a, id, newRequestID(), "x", mine))
	require.Equal(t, convErrForbidden, r.sendMedia(t, a, id, newRequestID(), "y", mine).Error, "повторное использование")

	// Форма: пустой список, дубль, больше 10, пересылка с вложениями,
	// пустой текст без вложений.
	empty := sendReq(id, newRequestID(), "x")
	empty["attachments"] = []any{}
	require.Equal(t, convErrInvalidRequest, r.svc.dispatch(context.Background(), a, ActionConversationSend, pl(t, empty)).Error)
	require.Equal(t, convErrInvalidRequest, r.sendMedia(t, a, id, newRequestID(), "x", mine, mine).Error)
	many := make([]string, 11)
	for i := range many {
		many[i] = "m" + strings.Repeat("x", i+1)
	}
	require.Equal(t, convErrInvalidRequest, r.sendMedia(t, a, id, newRequestID(), "x", many...).Error)
	fwd := sendWithMedia(id, newRequestID(), "x", mine)
	fwd["forwarded_from"] = map[string]any{"author_photon_id": pidB, "created_at": 1}
	require.Equal(t, convErrInvalidRequest, r.svc.dispatch(context.Background(), a, ActionConversationSend, pl(t, fwd)).Error)
	require.Equal(t, errMessageEmpty, r.send(t, a, id, newRequestID(), "  ").Error)
}

// Повтор 930 тем же ключом: тот же ответ, без новых привязок; другой набор —
// idempotency_conflict.
func TestM1SendRepeat(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	m1 := r.mustUpload(t, a, id, "k1", "1.bin", []byte("1"))
	m2 := r.mustUpload(t, a, id, "k2", "2.bin", []byte("2"))
	cid := newRequestID()
	first := msgOf(t, r.sendMedia(t, a, id, cid, "t", m1))
	again := msgOf(t, r.sendMedia(t, a, id, cid, "t", m1))
	require.Equal(t, first, again)
	require.Equal(t, convErrIdempotency, r.sendMedia(t, a, id, cid, "t", m2).Error)
	require.Equal(t, convErrIdempotency, r.send(t, a, id, cid, "t").Error, "тот же ключ без вложений — другое содержание")
	var bound int
	require.NoError(t, r.db.QueryRow(`SELECT count(*) FROM engram_media WHERE bound_pos IS NOT NULL`).Scan(&bound))
	require.Equal(t, 1, bound)
}

// Отказ CreatePost отменяет резерв: объект снова свободен, повтор тем же
// ключом привязывает его к новой позиции.
func TestM1AbortedReservationFreesMedia(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	mid := r.mustUpload(t, a, id, "k", "r.bin", []byte("retry"))
	cid := newRequestID()
	// Однозначный отказ до записи поста (contract §3.4): резерв → aborted.
	r.mm.mu.Lock()
	r.mm.postRejectOnCall = r.mm.postCalls + 1
	r.mm.postRejectID, r.mm.postRejectWhere, r.mm.postRejectStatus = "model.post.is_valid.msg.app_error", "", 400
	r.mm.mu.Unlock()
	require.False(t, r.sendMedia(t, a, id, cid, "t", mid).OK)
	info := r.info(t, a, mid)
	require.True(t, info.OK)
	require.False(t, info.Data.(convMediaInfoResult).Bound, "после отмены резерва объект свободен")

	msg := msgOf(t, r.sendMedia(t, a, id, cid, "t", mid))
	got, code := r.download(t, b, mid)
	require.Empty(t, code)
	require.Equal(t, []byte("retry"), got)
	require.Len(t, msg.Attachments, 1)
}

// Неопределённый исход CreatePost: резерв держит привязку; сверка публикует
// сообщение, и вложение становится доступным.
func TestM1ReconcilePublishesAttachment(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	mid := r.mustUpload(t, a, id, "k", "u.bin", []byte("unknown"))
	r.failNextPost(func(m *fakeConvMM, n int) { m.postCreateThenFailOnCall = n })
	require.False(t, r.sendMedia(t, a, id, newRequestID(), "t", mid).OK)
	_, code := r.download(t, b, mid)
	require.Equal(t, convErrForbidden, code, "до сверки сообщение не опубликовано")
	require.Equal(t, convErrForbidden, r.sendMedia(t, a, id, newRequestID(), "y", mid).Error, "резерв держит привязку")

	res, err := r.svc.reconcileConversation(context.Background(), id, 1<<62)
	require.NoError(t, err)
	require.Equal(t, 1, res.Published)
	_, code = r.download(t, b, mid)
	require.Empty(t, code)
}

// Две отправки разными ключами с одним объектом одновременно: привязка ровно
// одна, вторая отправка — forbidden.
func TestM1ConcurrentBind(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	r.id.person(pidB)
	id := r.group(t, a, pidB)
	mid := r.mustUpload(t, a, id, "k", "c.bin", []byte("contended"))
	const n = 6
	var ok, denied int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := r.sendMedia(t, a, id, newRequestID(), "t", mid)
			mu.Lock()
			defer mu.Unlock()
			if resp.OK {
				ok++
			} else if resp.Error == convErrForbidden {
				denied++
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 1, ok)
	require.Equal(t, n-1, denied)
}

// 933 закрывает доступ к вложению сразу; объект с удалённой привязкой
// повторно не привязывается и загрузившему как свободный не виден.
func TestM1DeleteRevokesAccess(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	mid := r.mustUpload(t, a, id, "k", "d.bin", []byte("deleted"))
	msg := msgOf(t, r.sendMedia(t, a, id, newRequestID(), "t", mid))
	r.mustDel(t, a, id, msg.ID)
	_, code := r.download(t, b, mid)
	require.Equal(t, convErrForbidden, code)
	require.Equal(t, convErrForbidden, r.info(t, a, mid).Error)
	require.Equal(t, convErrForbidden, r.sendMedia(t, a, id, newRequestID(), "t", mid).Error)
	require.Equal(t, 1, r.store.objectCount(), "байты в M1 не удаляются (уборка — M2)")
}

// Пост-носитель удалён в обход Bridge: доступ закрыт без сверки (M1-8).
func TestM1CarrierDeletedOutOfBand(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	mid := r.mustUpload(t, a, id, "k", "o.bin", []byte("oob"))
	msg := msgOf(t, r.sendMedia(t, a, id, newRequestID(), "t", mid))
	_, err := r.db.Exec(`UPDATE posts SET deleteat = 1 WHERE id = $1`, msg.ID)
	require.NoError(t, err)
	_, code := r.download(t, b, mid)
	require.Equal(t, convErrForbidden, code)
}

// since_join: вступивший после сообщения не видит его вложения.
func TestM1SinceJoinHidesEarlierAttachment(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	r.id.person(pidB)
	c := r.id.person(pidC)
	id := r.group(t, a, pidB)
	mid := r.mustUpload(t, a, id, "k", "early.bin", []byte("early"))
	msgOf(t, r.sendMedia(t, a, id, newRequestID(), "до вступления", mid))
	r.mustDo(t, a, ActionConversationMembersAdd, reqOf(id, "photon_ids", []string{pidC}))
	_, code := r.download(t, c, mid)
	require.Equal(t, convErrForbidden, code)
}

// Строка есть, а байтов нет: доступ подтверждается, хранилище отвечает
// «недоступно» — обработчик 704 отдаёт unavailable (contract §8.10 п. 9).
func TestM1BytesMissing(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	mid := r.mustUpload(t, a, id, "k", "lost.bin", []byte("lost"))
	msgOf(t, r.sendMedia(t, a, id, newRequestID(), "t", mid))
	r.store.mu.Lock()
	delete(r.store.objs, mid)
	r.store.mu.Unlock()
	m, code := r.svc.mediaForDownload(context.Background(), b, pl(t, map[string]string{"media_id": mid}))
	require.Empty(t, code)
	_, err := r.media.Info(m.mediaID)
	require.Error(t, err)
}

// Сообщения без вложений не меняются: ни attachments, ни заглушки, ни
// photon_media в props.
func TestM1PlainMessagesUnchanged(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	msg := r.mustSend(t, a, id, newRequestID(), "обычный текст")
	require.Nil(t, msg.Attachments)
	for _, f := range [][]string{nil, {"attachments"}} {
		it := messageByID(t, r.syncWith(t, b, id, f).Items, msg.ID)
		require.Equal(t, "обычный текст", it.Text)
		require.Nil(t, it.Attachments)
	}
	var props string
	require.NoError(t, r.db.QueryRow(`SELECT props::text FROM posts WHERE id = $1`, msg.ID).Scan(&props))
	require.NotContains(t, props, "photon_media")
	raw, err := json.Marshal(msg)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "attachments")
}

// 933 зафиксирован в БД, а удаление поста-носителя не прошло (contract
// §5b.2): пост ещё жив, но позиция — надгробие. Доступ закрывает именно
// отметка позиции, не живость поста.
func TestM1DeleteMarkWithoutCarrierDeletion(t *testing.T) {
	r := newMediaRig(t)
	a := r.id.person(pidA)
	b := r.id.person(pidB)
	id := r.group(t, a, pidB)
	mid := r.mustUpload(t, a, id, "k", "d.bin", []byte("deleted mark"))
	msg := msgOf(t, r.sendMedia(t, a, id, newRequestID(), "t", mid))
	r.mm.mu.Lock()
	r.mm.deletePostFail = 1
	r.mm.mu.Unlock()
	r.mustDel(t, a, id, msg.ID)
	var deleteAt int64
	require.NoError(t, r.db.QueryRow(`SELECT deleteat FROM posts WHERE id = $1`, msg.ID).Scan(&deleteAt))
	require.Zero(t, deleteAt, "пост-носитель остался живым")
	_, code := r.download(t, b, mid)
	require.Equal(t, convErrForbidden, code)
}
