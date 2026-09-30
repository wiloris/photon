package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/plugin/plugintest"
	"github.com/mattermost/mattermost/server/public/pluginapi"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Модульные тесты контракта 217/218 (ADR-092 ред. 3): проверки входа,
// курсор, выбор режима, окно W, форма ответа. Хранилище — дублёр поверх
// постов fakeDM; семантика SQL (порядок, строгое продолжение, advisory
// lock) проверяется на настоящем PostgreSQL — dm_sql_pg_test.go.

// fakeDMStore — посты берутся из fakeDM: CreatePost обработчика и поиск
// видят одно и то же, как в Mattermost.
type fakeDMStore struct {
	mm      *fakeDM
	lockErr error
	calls   []string // "newest", "from:<lower>", "after:<t>/<id>"
}

func (f *fakeDMStore) rows(channelID string, authors dmAuthors, keep func(dmRow) bool) []dmRow {
	var out []dmRow
	for _, p := range f.mm.posts {
		if p.ChannelId != channelID || p.DeleteAt != 0 || p.Type != "" || (p.UserId != authors[0] && p.UserId != authors[1]) {
			continue
		}
		cid, _ := p.GetProp(dmPropClientMsgID).(string)
		r := dmRow{ID: p.Id, UserID: p.UserId, Message: p.Message, CreateAt: p.CreateAt, ClientMsgID: cid}
		if keep(r) {
			out = append(out, r)
		}
	}
	// (CreateAt ASC, Id DESC) — порядок ADR-052.
	sort.Slice(out, func(i, j int) bool {
		a, b := dmPos{out[i].CreateAt, out[i].ID}, dmPos{out[j].CreateAt, out[j].ID}
		return b.newerThan(a)
	})
	return out
}

func (f *fakeDMStore) Newest(_ context.Context, ch string, a dmAuthors, limit int) ([]dmRow, error) {
	f.calls = append(f.calls, "newest")
	all := f.rows(ch, a, func(dmRow) bool { return true })
	if len(all) > limit {
		all = all[len(all)-limit:]
	}
	return all, nil
}

func (f *fakeDMStore) From(_ context.Context, ch string, a dmAuthors, lower int64, limit int) ([]dmRow, error) {
	f.calls = append(f.calls, fmt.Sprintf("from:%d", lower))
	all := f.rows(ch, a, func(r dmRow) bool { return r.CreateAt >= lower })
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func (f *fakeDMStore) After(_ context.Context, ch string, a dmAuthors, pos dmPos, limit int) ([]dmRow, error) {
	f.calls = append(f.calls, fmt.Sprintf("after:%d/%s", pos.CreateAt, pos.ID))
	all := f.rows(ch, a, func(r dmRow) bool { return dmPos{r.CreateAt, r.ID}.newerThan(pos) })
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func (f *fakeDMStore) WithSendLock(_ context.Context, _, _ string, _ time.Duration, fn func(find dmFindFunc) error) error {
	if f.lockErr != nil {
		return f.lockErr
	}
	return fn(func(ch, user, cid string, since int64) (*dmRow, error) {
		for _, p := range f.mm.posts {
			if got, _ := p.GetProp(dmPropClientMsgID).(string); p.ChannelId == ch && p.UserId == user && got == cid && p.CreateAt >= since {
				return &dmRow{ID: p.Id, UserID: p.UserId, Message: p.Message, CreateAt: p.CreateAt, DeleteAt: p.DeleteAt, ClientMsgID: got}, nil
			}
		}
		return nil, nil
	})
}

type v2Rig struct {
	*dmRig
	store *fakeDMStore
	tLink time.Duration
}

func newV2Rig(t *testing.T) *v2Rig {
	r := &v2Rig{dmRig: newDMRig(t), tLink: 5 * time.Second}
	r.store = &fakeDMStore{mm: r.mm}
	return r
}

func (r *v2Rig) deps() directMessageV2Deps {
	return directMessageV2Deps{
		directMessageDeps: r.dmRig.deps(),
		store:             r.store,
		linkTimeout:       func() time.Duration { return r.tLink },
		now:               func() time.Time { return time.UnixMilli(r.mm.clock) },
	}
}

const cidA = "0f8fad5b-d9cb-469f-a165-70867728950e"

func (r *v2Rig) send(t *testing.T, from dmParty, to, text, cid string) Response {
	t.Helper()
	return sendDirectMessageV2(r.deps(), from.mmID, pl(t, map[string]string{"to_photon_id": to, "text": text, "client_msg_id": cid}))
}

func (r *v2Rig) list(t *testing.T, who dmParty, with, cursor string, limit int) DirectMessagesV2 {
	t.Helper()
	resp := listDirectMessagesV2(r.deps(), who.mmID, pl(t, map[string]any{"with_photon_id": with, "cursor": cursor, "limit": limit}))
	require.True(t, resp.OK, "218: %+v", resp)
	return resp.Data.(DirectMessagesV2)
}

func sentV2(t *testing.T, resp Response) DirectMessageV2 {
	t.Helper()
	require.True(t, resp.OK, "217: %+v", resp)
	return resp.Data.(map[string]DirectMessageV2)["message"]
}

// ── 217 ──────────────────────────────────────────────────────────────

func TestDMv2SendRequiresUUIDClientMsgID(t *testing.T) {
	r := newV2Rig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	for _, cid := range []string{"", "not-a-uuid", "0f8fad5bd9cb469fa16570867728950e", cidA + "x"} {
		require.Equal(t, dmFail(errCodeInvalidRequest), r.send(t, a, b.photonID, "привет", cid), cid)
	}
	// Лишнее поле — отказ, а не молчаливый пропуск (именно так 213 терял
	// client_msg_id).
	resp := sendDirectMessageV2(r.deps(), a.mmID, pl(t, map[string]string{"to_photon_id": b.photonID, "text": "x", "client_msg_id": cidA, "user_id": "x"}))
	require.Equal(t, dmFail(errCodeInvalidRequest), resp)
	require.Zero(t, r.mm.createCalls)
}

func TestDMv2SendLinkLimitBeforeMattermost(t *testing.T) {
	r := newV2Rig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	five := "https://a.io http://b.io WWW.c.io ![x](/api/v4/files/1) ![y](https://d.io/i.png)"
	require.Equal(t, 6, countLinks(five), "![y](https://…) считается дважды — консервативно")
	require.Equal(t, dmFail(errMessageTooManyLinks), r.send(t, a, b.photonID, five, cidA))

	ok := "https://a.io http://b.io www.c.io ![x](/f) и текст"
	require.Equal(t, 4, countLinks(ok))
	sentV2(t, r.send(t, a, b.photonID, ok, cidA))

	six := strings.Repeat("https://x.io ", 6)
	calls := r.mm.createCalls
	require.Equal(t, dmFail(errMessageTooManyLinks), r.send(t, a, b.photonID, six, "11111111-1111-4111-8111-111111111111"))
	require.Equal(t, calls, r.mm.createCalls, "отказ N_max — до обращения к Mattermost")
	require.Equal(t, 5, countLinks(strings.Repeat("https://x.io ", 5)))
}

func TestDMv2SendCreatesTaggedPostAndIsIdempotent(t *testing.T) {
	r := newV2Rig(t)
	a, b := r.party(t, "a"), r.party(t, "b")

	// D: первый вызов создал пост, ответ «потерян», повтор — тот же пост.
	first := sentV2(t, r.send(t, a, b.photonID, "одно сообщение", strings.ToUpper(cidA)))
	require.Equal(t, cidA, first.ClientMsgID, "client_msg_id нормализуется к нижнему регистру")
	second := sentV2(t, r.send(t, a, b.photonID, "одно сообщение", cidA))
	require.Equal(t, first, second)
	require.Equal(t, 1, r.mm.createCalls)
	require.Equal(t, cidA, r.mm.posts[0].GetProp(dmPropClientMsgID))

	// E: тот же client_msg_id, другой текст.
	require.Equal(t, dmFail(errIdempotencyConflict), r.send(t, a, b.photonID, "другой текст", cidA))
	require.Equal(t, 1, r.mm.createCalls)

	// Удалённый пост с тем же client_msg_id — не воскрешается.
	r.mm.posts[0].DeleteAt = r.mm.clock
	again := sentV2(t, r.send(t, a, b.photonID, "одно сообщение", cidA))
	require.True(t, again.Deleted)
	require.Equal(t, first.ID, again.ID)
	require.Equal(t, 1, r.mm.createCalls)
}

func TestDMv2SendLockTimeoutAndUnavailable(t *testing.T) {
	r := newV2Rig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	r.store.lockErr = errSendLockTimeout
	require.Equal(t, dmFail(errMessageInProgress), r.send(t, a, b.photonID, "x", cidA))
	r.store.lockErr = errors.New("db down")
	require.Equal(t, dmFail(errMessageFailed), r.send(t, a, b.photonID, "x", cidA))

	d := r.deps()
	d.store, d.storeErr = nil, errors.New("schema mismatch")
	require.Equal(t, dmFail(errSyncUnavailable),
		sendDirectMessageV2(d, a.mmID, pl(t, map[string]string{"to_photon_id": b.photonID, "text": "x", "client_msg_id": cidA})))
	require.Equal(t, dmFail(errSyncUnavailable),
		listDirectMessagesV2(d, a.mmID, pl(t, map[string]any{"with_photon_id": b.photonID})))
	require.Zero(t, r.mm.createCalls)
}

// ── окно W ───────────────────────────────────────────────────────────

func TestDMv2WindowFormula(t *testing.T) {
	// F_max = N_max + LinkMetadataMaxImages + 1 = 5 + 5 + 1 = 11;
	// W = 11 × T_link + 1 с + 30 с.
	require.Equal(t, 11, dmMaxLinks+model.LinkMetadataMaxImages+1)
	require.Equal(t, 86*time.Second, dmWindow(5*time.Second), "стенд: T_link = 5000 мс")
	require.Equal(t, 11*10*time.Second+31*time.Second, dmWindow(10*time.Second), "W следует конфигурации, а не константе")
}

// ── 218: курсор ─────────────────────────────────────────────────────

func TestDMv2CursorRejections(t *testing.T) {
	r := newV2Rig(t)
	a, b, c := r.party(t, "a"), r.party(t, "b"), r.party(t, "c")
	sentV2(t, r.send(t, a, b.photonID, "x", cidA))
	good := r.list(t, a, b.photonID, "", 60).NextCursor
	require.NotEmpty(t, good)

	bind := conversationBinding(a.mmID, b.mmID)
	mk := func(c dmCursor) string { return encodeCursor(c) }
	w := &dmWire{T: 1, ID: "abc"}
	for name, cur := range map[string]string{
		"не base64":            "%%%",
		"не JSON":              base64.RawURLEncoding.EncodeToString([]byte("nope")),
		"лишнее поле":          base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"b":"` + bind + `","x":1}`)),
		"другая версия":        mk(dmCursor{V: 2, B: bind, W: w}),
		"позиция без водяного": mk(dmCursor{V: 1, B: bind, P: w}),
		"негодный id":          mk(dmCursor{V: 1, B: bind, W: &dmWire{T: 1, ID: "A;DROP"}}),
		"отрицательное время":  mk(dmCursor{V: 1, B: bind, W: &dmWire{T: -1, ID: "abc"}}),
	} {
		resp := listDirectMessagesV2(r.deps(), a.mmID, pl(t, map[string]any{"with_photon_id": b.photonID, "cursor": cur}))
		require.Equal(t, dmFail(errInvalidCursor), resp, name)
	}
	// Курсор другого разговора: a↔b предъявлен в разговоре a↔c.
	resp := listDirectMessagesV2(r.deps(), a.mmID, pl(t, map[string]any{"with_photon_id": c.photonID, "cursor": good}))
	require.Equal(t, dmFail(errInvalidCursor), resp)
	// Привязка симметрична: b читает тот же разговор тем же курсором.
	require.True(t, listDirectMessagesV2(r.deps(), b.mmID, pl(t, map[string]any{"with_photon_id": a.photonID, "cursor": good})).OK)
	// В курсоре нет mm_user_id.
	raw, _ := base64.RawURLEncoding.DecodeString(good)
	require.NotContains(t, string(raw), a.mmID)
	require.NotContains(t, string(raw), b.mmID)
}

func TestDMv2Limits(t *testing.T) {
	r := newV2Rig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	for _, l := range []int{0, 201, -1} {
		require.Equal(t, dmFail(errInvalidLimit), listDirectMessagesV2(r.deps(), a.mmID, pl(t, map[string]any{"with_photon_id": b.photonID, "limit": l})))
	}
	for i := 0; i < 70; i++ {
		sentV2(t, r.send(t, a, b.photonID, fmt.Sprint(i), fmt.Sprintf("00000000-0000-4000-8000-%012d", i)))
	}
	got := listDirectMessagesV2(r.deps(), a.mmID, pl(t, map[string]any{"with_photon_id": b.photonID}))
	require.Len(t, got.Data.(DirectMessagesV2).Messages, 60, "по умолчанию 60")
}

// ── 218: режимы и next_cursor (§2.2) ─────────────────────────────────

func TestDMv2PrimarySyncThenPasses(t *testing.T) {
	r := newV2Rig(t)
	a, b := r.party(t, "a"), r.party(t, "b")

	// Канала нет — пустой ответ и курсор с пустым водяным знаком.
	empty := r.list(t, a, b.photonID, "", 2)
	require.Empty(t, empty.Messages)
	require.True(t, empty.AtEnd)
	require.Nil(t, empty.From)

	for i := 0; i < 5; i++ {
		sentV2(t, r.send(t, a, b.photonID, fmt.Sprint(i), fmt.Sprintf("00000000-0000-4000-8000-%012d", i)))
	}
	// Первичная синхронизация: последние limit, от старых к новым, at_end.
	p := r.list(t, a, b.photonID, "", 2)
	require.Equal(t, []string{"3", "4"}, texts(p.Messages))
	require.True(t, p.AtEnd)
	require.Nil(t, p.From)
	require.Equal(t, []string{"newest"}, r.store.calls)

	// Начало прохода: окно от водяного знака, from = L (id пуст).
	r.store.calls = nil
	pass := r.list(t, a, b.photonID, p.NextCursor, 2)
	wm := r.mm.posts[4].CreateAt
	lower := wm - dmWindow(r.tLink).Milliseconds()
	if lower < 0 {
		lower = 0
	}
	require.Equal(t, []string{fmt.Sprintf("from:%d", lower)}, r.store.calls)
	require.Equal(t, &dmWirePosition{CreatedAt: lower, ID: ""}, pass.From)
	require.False(t, pass.AtEnd, "страница полная")

	// Продолжение: строго после последней отданной, без окна.
	r.store.calls = nil
	last := pass.Messages[len(pass.Messages)-1]
	cont := r.list(t, a, b.photonID, pass.NextCursor, 2)
	require.Equal(t, []string{fmt.Sprintf("after:%d/%s", last.CreatedAt, last.ID)}, r.store.calls)
	require.Equal(t, &dmWirePosition{CreatedAt: last.CreatedAt, ID: last.ID}, cont.From)

	// Проход доходит до конца; следующий запрос — снова начало прохода.
	cur := cont.NextCursor
	for i := 0; i < 10 && !cont.AtEnd; i++ {
		cont = r.list(t, a, b.photonID, cur, 2)
		cur = cont.NextCursor
	}
	require.True(t, cont.AtEnd)
	r.store.calls = nil
	r.list(t, a, b.photonID, cur, 2)
	require.True(t, strings.HasPrefix(r.store.calls[0], "from:"), "после at_end — новый проход с окна")
}

func TestDMv2ClientMsgIDOnlyForOwnMessages(t *testing.T) {
	r := newV2Rig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	sentV2(t, r.send(t, a, b.photonID, "от a", cidA))
	sentV2(t, r.send(t, b, a.photonID, "от b", "22222222-2222-4222-8222-222222222222"))

	for who, own := range map[dmParty]string{a: "от a", b: "от b"} {
		other := b
		if who == b {
			other = a
		}
		got := r.list(t, who, other.photonID, "", 60)
		for _, m := range got.Messages {
			if m.Text == own {
				require.NotEmpty(t, m.ClientMsgID, "своё сообщение несёт client_msg_id")
				require.Equal(t, who.photonID, m.AuthorPhotonID)
			} else {
				require.Empty(t, m.ClientMsgID, "чужой client_msg_id не выдаётся")
			}
		}
	}
}

// Удалённый пост, на котором стоит позиция прохода, продолжению не мешает:
// сервер пост по курсору не ищет (§2.2.5).
func TestDMv2DeletedCursorPostDoesNotBreakContinuation(t *testing.T) {
	r := newV2Rig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	for i := 0; i < 4; i++ {
		sentV2(t, r.send(t, a, b.photonID, fmt.Sprint(i), fmt.Sprintf("00000000-0000-4000-8000-%012d", i)))
	}
	p := r.list(t, a, b.photonID, "", 1)
	pass := r.list(t, a, b.photonID, p.NextCursor, 1)
	require.False(t, pass.AtEnd)
	for _, post := range r.mm.posts {
		if post.Id == pass.Messages[0].ID {
			post.DeleteAt = r.mm.clock
		}
	}
	getPostCalls := r.mm.getPostCalls
	cont := listDirectMessagesV2(r.deps(), a.mmID, pl(t, map[string]any{"with_photon_id": b.photonID, "cursor": pass.NextCursor, "limit": 1}))
	require.True(t, cont.OK, "%+v", cont)
	require.Equal(t, getPostCalls, r.mm.getPostCalls, "пост по курсору не ищется")
}

func texts(ms []DirectMessageV2) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.Text)
	}
	return out
}

// Ответ 218 сериализуется в форму photon.proto DirectMessagesV2Response.
func TestDMv2ResponseShape(t *testing.T) {
	r := newV2Rig(t)
	a, b := r.party(t, "a"), r.party(t, "b")
	sentV2(t, r.send(t, a, b.photonID, "x", cidA))
	got := r.list(t, a, b.photonID, "", 60)
	raw, err := json.Marshal(got)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	require.ElementsMatch(t, []string{"messages", "next_cursor", "at_end"}, keys(m), "первичная синхронизация — без from")
	msg := m["messages"].([]any)[0].(map[string]any)
	require.ElementsMatch(t, []string{"id", "author_photon_id", "text", "created_at", "client_msg_id"}, keys(msg))
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Без соединения с БД v2 выключается явно, с причиной, а активация и
// 213/214 не затрагиваются (ADR-092 §5).
func TestDMv2DisabledWithoutDatabase(t *testing.T) {
	api := &plugintest.API{}
	api.On("LogError", "[Photon][DMv2] disabled", "error", mock.MatchedBy(func(s string) bool {
		return strings.Contains(s, "нет соединения с БД")
	})).Once()
	p := &Plugin{}
	p.SetAPI(api)
	p.client = pluginapi.NewClient(api, nil) // драйвера БД нет
	p.initDMv2()
	require.Nil(t, p.dmStore)
	require.ErrorContains(t, p.dmStoreErr, "нет соединения с БД")
	api.AssertExpectations(t)

	h := &UserHandler{plugin: p}
	require.Nil(t, h.directMessageV2Deps().store)
}
