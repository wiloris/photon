package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/stretchr/testify/require"
)

// Контрактные тесты S1 и идемпотентности 217 против НАСТОЯЩЕГО PostgreSQL
// (ADR-092 ред. 3 §5): порядок, строгое продолжение, окно, advisory lock.
//
// Включаются переменной PHOTON_DM_PG_DSN (DSN роли Mattermost). Каждый
// тест работает в собственной схеме photon_dmtest_<rand> с копией
// структуры public.posts (LIKE … INCLUDING ALL — колонки, типы, индексы) и
// удаляет её в конце. public.posts не читается и не пишется: соединения
// теста получают search_path своей схемы, SQL модуля неквалифицирован —
// ровно как у Mattermost.

func pgDSN(t *testing.T) string {
	dsn := os.Getenv("PHOTON_DM_PG_DSN")
	if dsn == "" {
		t.Skip("PHOTON_DM_PG_DSN не задан — контрактные тесты PostgreSQL пропущены")
	}
	return dsn
}

func withSearchPath(dsn, schema string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "search_path=" + schema
}

// pgSchema — временная схема с копией public.posts. like=false — пустая
// схема для негативной проверки схемы.
func pgSchema(t *testing.T, like bool) string {
	t.Helper()
	dsn := pgDSN(t)
	admin, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { admin.Close() })

	var b [6]byte
	_, _ = rand.Read(b[:])
	schema := "photon_dmtest_" + hex.EncodeToString(b[:])
	_, err = admin.Exec(`CREATE SCHEMA ` + schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, e := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		if e != nil {
			t.Errorf("не удалось удалить схему %s: %v", schema, e)
		}
	})
	if like {
		_, err = admin.Exec(`CREATE TABLE ` + schema + `.posts (LIKE public.posts INCLUDING ALL)`)
		require.NoError(t, err)
	}
	return schema
}

func pgOpen(t *testing.T, schema string) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", withSearchPath(pgDSN(t), schema))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	require.NoError(t, db.Ping())
	return db
}

func pgInsert(t *testing.T, db *sql.DB, id, channelID, userID string, createAt int64, msg, cid string) {
	t.Helper()
	props := "{}"
	if cid != "" {
		props = `{"` + dmPropClientMsgID + `":"` + cid + `"}`
	}
	_, err := db.Exec(`INSERT INTO posts (id, channelid, userid, createat, updateat, deleteat, message, type, props)
VALUES ($1, $2, $3, $4, $4, 0, $5, '', $6::jsonb)`, id, channelID, userID, createAt, msg, props)
	require.NoError(t, err)
}

// pgAPI — Mattermost для обработчика: пользователи и каналы из fakeDM,
// CreatePost пишет в PostgreSQL СВОИМ соединением, как это делает
// Mattermost (не в транзакции блокировки).
type pgAPI struct {
	*fakeDM
	db      *sql.DB
	mu      sync.Mutex
	creates int
	delay   time.Duration
}

func (a *pgAPI) CreatePost(p *model.Post) (*model.Post, *model.AppError) {
	time.Sleep(a.delay)
	a.mu.Lock()
	a.creates++
	a.mu.Unlock()
	id := model.NewId()
	cid, _ := p.GetProp(dmPropClientMsgID).(string)
	now := model.GetMillis()
	props := "{}"
	if cid != "" {
		props = `{"` + dmPropClientMsgID + `":"` + cid + `"}`
	}
	if _, err := a.db.Exec(`INSERT INTO posts (id, channelid, userid, createat, updateat, deleteat, message, type, props)
VALUES ($1, $2, $3, $4, $4, 0, $5, '', $6::jsonb)`, id, p.ChannelId, p.UserId, now, p.Message, props); err != nil {
		return nil, model.NewAppError("CreatePost", "store", nil, err.Error(), 500)
	}
	out := p.Clone()
	out.Id, out.CreateAt = id, now
	return out, nil
}

type pgRig struct {
	*dmRig
	api   *pgAPI
	store *pgDMStore
	a, b  dmParty
	ch    string
}

func newPGRig(t *testing.T, schema string) *pgRig {
	r := &pgRig{dmRig: newDMRig(t)}
	db := pgOpen(t, schema)
	r.api = &pgAPI{fakeDM: r.mm, db: pgOpen(t, schema)}
	r.store = newPGDMStore(db)
	r.a, r.b = r.party(t, "a"), r.party(t, "b")
	ch, _ := r.mm.GetDirectChannel(r.a.mmID, r.b.mmID)
	r.ch = ch.Id
	return r
}

func (r *pgRig) deps() directMessageV2Deps {
	return directMessageV2Deps{
		directMessageDeps: directMessageDeps{registry: r.dmRig, api: r.api, logger: func(string, ...any) {}},
		store:             r.store,
		linkTimeout:       func() time.Duration { return 5 * time.Second },
		now:               time.Now,
	}
}

func (r *pgRig) list(t *testing.T, cursor string, limit int) DirectMessagesV2 {
	t.Helper()
	resp := listDirectMessagesV2(r.deps(), r.b.mmID, pl(t, map[string]any{"with_photon_id": r.a.photonID, "cursor": cursor, "limit": limit}))
	require.True(t, resp.OK, "218: %+v", resp)
	return resp.Data.(DirectMessagesV2)
}

// syncPass — один проход до at_end; сторож — от зацикливания (§1.7).
func (r *pgRig) syncPass(t *testing.T, cursor string, limit int) ([]string, string) {
	t.Helper()
	var ids []string
	for i := 0; i < 50; i++ {
		page := r.list(t, cursor, limit)
		for _, m := range page.Messages {
			ids = append(ids, m.ID)
		}
		cursor = page.NextCursor
		if page.AtEnd {
			return ids, cursor
		}
	}
	t.Fatalf("проход не закончился за 50 страниц — зацикливание")
	return nil, ""
}

func set(ids []string) map[string]bool {
	m := map[string]bool{}
	for _, id := range ids {
		m[id] = true
	}
	return m
}

// ── проверка схемы ───────────────────────────────────────────────────

func TestPGSchemaCheck(t *testing.T) {
	good := pgSchema(t, true)
	require.NoError(t, checkDMSchema(context.Background(), pgOpen(t, good), "postgres"))
	require.ErrorContains(t, checkDMSchema(context.Background(), pgOpen(t, good), "mysql"), "только postgres")

	broken := pgSchema(t, false)
	db := pgOpen(t, broken)
	_, err := db.Exec(`CREATE TABLE posts (id varchar(26) PRIMARY KEY, channelid varchar(26), userid varchar(26),
		createat bigint, deleteat bigint, type varchar(26), message varchar(65535), props text)`)
	require.NoError(t, err)
	err = checkDMSchema(context.Background(), db, "postgres")
	require.Error(t, err)
	require.Contains(t, err.Error(), `posts.props: тип "text", ожидался "jsonb"`)
	require.Contains(t, err.Error(), "нет индекса posts(channelid, deleteat, createat)")
}

// ── 218: A, B, C, окно больше limit, порядок, удалённый пост ─────────

func TestPGOrderWithinMillisecondIsIDDesc(t *testing.T) {
	r := newPGRig(t, pgSchema(t, true))
	for _, id := range []string{"aaaa", "bbbb", "cccc"} {
		pgInsert(t, r.api.db, id, r.ch, r.a.mmID, 1000, id, "")
	}
	rows, err := r.store.From(context.Background(), r.ch, dmAuthors{r.a.mmID, r.b.mmID}, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"cccc", "bbbb", "aaaa"}, []string{rows[0].ID, rows[1].ID, rows[2].ID},
		"внутри мс — PostID DESC: от «старых» к «новым» по ADR-052")
}

// A: пост той же мс, что последний отданный, после курсора не теряется.
func TestPGScenarioA_SameCreateAtAsCursor(t *testing.T) {
	r := newPGRig(t, pgSchema(t, true))
	pgInsert(t, r.api.db, "p1", r.ch, r.a.mmID, 100, "1", "")
	pgInsert(t, r.api.db, "p2", r.ch, r.a.mmID, 200, "2", "")
	first, cur := r.syncPass(t, "", 60)
	require.Len(t, first, 2)
	// Той же мс, что p2, и id МЕНЬШЕ — в порядке ADR-052 «новее» p2.
	pgInsert(t, r.api.db, "p0", r.ch, r.a.mmID, 200, "0", "")
	second, _ := r.syncPass(t, cur, 60)
	require.True(t, set(second)["p0"], "пост той же мс потерян: %v", second)
}

// B: граница страницы внутри одной мс — ни одного пропуска.
func TestPGScenarioB_PageBoundaryInsideMillisecond(t *testing.T) {
	r := newPGRig(t, pgSchema(t, true))
	pgInsert(t, r.api.db, "a0", r.ch, r.a.mmID, 100, "a", "")
	for i := 1; i <= 5; i++ {
		pgInsert(t, r.api.db, fmt.Sprintf("p%d", i), r.ch, r.a.mmID, 200, "p", "")
	}
	pgInsert(t, r.api.db, "z9", r.ch, r.a.mmID, 300, "z", "")
	_, cur := r.syncPass(t, "", 1) // первичная: только z9
	got, _ := r.syncPass(t, cur, 2)
	s := set(got)
	for _, id := range []string{"p1", "p2", "p3", "p4", "p5", "a0", "z9"} {
		require.True(t, s[id], "потерян %s: %v", id, got)
	}
}

// C: поздняя фиксация — пост с CreateAt раньше водяного знака, но в окне.
func TestPGScenarioC_LateCommitCaughtByWindow(t *testing.T) {
	r := newPGRig(t, pgSchema(t, true))
	now := model.GetMillis()
	pgInsert(t, r.api.db, "x", r.ch, r.a.mmID, now, "x", "")
	_, cur := r.syncPass(t, "", 60)
	// Фиксируется позже, с CreateAt на 40 с раньше (< W = 86 с).
	pgInsert(t, r.api.db, "y", r.ch, r.a.mmID, now-40_000, "y", "")
	got, _ := r.syncPass(t, cur, 60)
	require.True(t, set(got)["y"], "поздняя фиксация потеряна: %v", got)
}

// §1.7: сообщений в окне больше limit — проход доходит до конца.
func TestPGWindowLargerThanLimitDoesNotLoop(t *testing.T) {
	r := newPGRig(t, pgSchema(t, true))
	now := model.GetMillis()
	for i := 0; i < 10; i++ {
		pgInsert(t, r.api.db, fmt.Sprintf("w%02d", i), r.ch, r.a.mmID, now+int64(i%4), "w", "")
	}
	_, cur := r.syncPass(t, "", 1)
	got, _ := r.syncPass(t, cur, 3) // syncPass падает при зацикливании
	require.Len(t, set(got), 10)
}

// Удалённый пост на позиции прохода — продолжение идёт дальше.
func TestPGDeletedCursorPost(t *testing.T) {
	r := newPGRig(t, pgSchema(t, true))
	for i := 0; i < 4; i++ {
		pgInsert(t, r.api.db, fmt.Sprintf("d%d", i), r.ch, r.a.mmID, int64(100+i), "d", "")
	}
	_, cur := r.syncPass(t, "", 1)
	page := r.list(t, cur, 1)
	require.False(t, page.AtEnd)
	_, err := r.api.db.Exec(`UPDATE posts SET deleteat = 1 WHERE id = $1`, page.Messages[0].ID)
	require.NoError(t, err)
	next := r.list(t, page.NextCursor, 1)
	require.NotEmpty(t, next.Messages, "после удалённого поста-позиции выдача продолжается")
}

// ── 217: D, E, удалённый, параллельные повторы, блокировка ───────────

func (r *pgRig) send(t *testing.T, text, cid string) Response {
	return sendDirectMessageV2(r.deps(), r.a.mmID, pl(t, map[string]string{"to_photon_id": r.b.photonID, "text": text, "client_msg_id": cid}))
}

func (r *pgRig) count(t *testing.T, cid string) int {
	var n int
	require.NoError(t, r.api.db.QueryRow(`SELECT count(*) FROM posts WHERE props->>'`+dmPropClientMsgID+`' = $1`, cid).Scan(&n))
	return n
}

func TestPGScenarioD_E_Idempotency(t *testing.T) {
	r := newPGRig(t, pgSchema(t, true))
	first := sentV2(t, r.send(t, "одно сообщение", cidA))
	// D: ответ «потерян», повтор — тот же пост, строка одна.
	second := sentV2(t, r.send(t, "одно сообщение", cidA))
	require.Equal(t, first.ID, second.ID)
	require.Equal(t, 1, r.count(t, cidA))
	require.Equal(t, 1, r.api.creates)
	// E: другой текст — конфликт, строк не прибавилось.
	require.Equal(t, dmFail(errIdempotencyConflict), r.send(t, "другой", cidA))
	require.Equal(t, 1, r.count(t, cidA))
	// Удалённый — не воскрешается.
	_, err := r.api.db.Exec(`UPDATE posts SET deleteat = $1 WHERE id = $2`, model.GetMillis(), first.ID)
	require.NoError(t, err)
	third := sentV2(t, r.send(t, "одно сообщение", cidA))
	require.True(t, third.Deleted)
	require.Equal(t, first.ID, third.ID)
	require.Equal(t, 1, r.count(t, cidA))
	// Метка видна 218 у своего сообщения.
	_, err = r.api.db.Exec(`UPDATE posts SET deleteat = 0 WHERE id = $1`, first.ID)
	require.NoError(t, err)
	got := listDirectMessagesV2(r.deps(), r.a.mmID, pl(t, map[string]any{"with_photon_id": r.b.photonID}))
	require.Equal(t, cidA, got.Data.(DirectMessagesV2).Messages[0].ClientMsgID)
}

// Два экземпляра Bridge (разные соединения, общая БД) одновременно
// отправляют одну операцию — пост создаётся ровно один.
func TestPGConcurrentSameClientMsgIDAcrossInstances(t *testing.T) {
	schema := pgSchema(t, true)
	r := newPGRig(t, schema)
	r.api.delay = 300 * time.Millisecond // держатель блокировки «долго» создаёт пост
	second := newPGDMStore(pgOpen(t, schema))

	var wg sync.WaitGroup
	results := make([]Response, 2)
	for i, st := range []dmStore{r.store, second} {
		wg.Add(1)
		go func(i int, st dmStore) {
			defer wg.Done()
			d := r.deps()
			d.store = st
			results[i] = sendDirectMessageV2(d, r.a.mmID, pl(t, map[string]string{"to_photon_id": r.b.photonID, "text": "гонка", "client_msg_id": cidA}))
		}(i, st)
	}
	wg.Wait()
	a, b := sentV2(t, results[0]), sentV2(t, results[1])
	require.Equal(t, a.ID, b.ID)
	require.Equal(t, 1, r.count(t, cidA))
	require.Equal(t, 1, r.api.creates)
}

// Блокировка занята дольше ожидания — message_in_progress; освобождается
// концом транзакции держателя.
func TestPGLockTimeoutThenRelease(t *testing.T) {
	schema := pgSchema(t, true)
	r := newPGRig(t, schema)
	holderDB := pgOpen(t, schema)
	tx, err := holderDB.Begin()
	require.NoError(t, err)
	var got bool
	require.NoError(t, tx.QueryRow(`SELECT pg_try_advisory_xact_lock($1)`, sendLockKey(r.a.mmID, cidA)).Scan(&got))
	require.True(t, got)

	d := r.deps()
	d.lockWait = 300 * time.Millisecond
	start := time.Now()
	resp := sendDirectMessageV2(d, r.a.mmID, pl(t, map[string]string{"to_photon_id": r.b.photonID, "text": "x", "client_msg_id": cidA}))
	require.Equal(t, dmFail(errMessageInProgress), resp)
	require.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)
	require.Zero(t, r.api.creates)

	require.NoError(t, tx.Rollback())
	sentV2(t, sendDirectMessageV2(d, r.a.mmID, pl(t, map[string]string{"to_photon_id": r.b.photonID, "text": "x", "client_msg_id": cidA})))
}

// Держатель блокировки «падает» — его соединение закрывается сервером
// (как ShutdownConns Mattermost при перезапуске плагина): блокировка
// освобождается без COMMIT/ROLLBACK.
func TestPGLockReleasedWhenHolderConnectionDies(t *testing.T) {
	schema := pgSchema(t, true)
	r := newPGRig(t, schema)
	holderDB := pgOpen(t, schema)
	holderDB.SetMaxOpenConns(1)
	tx, err := holderDB.Begin()
	require.NoError(t, err)
	var got bool
	var pid int
	require.NoError(t, tx.QueryRow(`SELECT pg_try_advisory_xact_lock($1), pg_backend_pid()`, sendLockKey(r.a.mmID, cidA)).Scan(&got, &pid))
	require.True(t, got)

	killer := pgOpen(t, schema)
	var killed bool
	require.NoError(t, killer.QueryRow(`SELECT pg_terminate_backend($1)`, pid).Scan(&killed))
	require.True(t, killed)

	d := r.deps()
	d.lockWait = 3 * time.Second
	sentV2(t, sendDirectMessageV2(d, r.a.mmID, pl(t, map[string]string{"to_photon_id": r.b.photonID, "text": "после падения", "client_msg_id": cidA})))
	_ = tx.Rollback()
}
