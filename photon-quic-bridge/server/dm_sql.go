package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Прямое чтение Posts для личных сообщений v2 — S1 (ADR-092 ред. 3 §2.4).
//
// ОСОЗНАННАЯ ЗАВИСИМОСТЬ ОТ СХЕМЫ MATTERMOST 9.11 (§5). Весь SQL протокола
// сообщений — в этом файле и больше нигде: при обновлении Mattermost
// перепроверяется он один. Соединение — pluginapi.Store (основная БД через
// RPC-драйвер Mattermost), реплика не используется: задержка репликации
// была бы ещё одной поздней фиксацией.
//
// Имена таблиц и колонок без кавычек: Postgres приводит их к нижнему
// регистру, ровно как их создаёт Mattermost. Схема — текущая (search_path
// соединения), в работе — public.

// dmRow — пост личного канала в объёме, нужном протоколу.
type dmRow struct {
	ID          string
	UserID      string
	Message     string
	CreateAt    int64
	DeleteAt    int64
	ClientMsgID string // Props->>'engram_client_msg_id', пусто — нет метки
}

// dmPos — позиция в порядке (CreateAt, PostID) (ADR-092 §2.1).
type dmPos struct {
	CreateAt int64
	ID       string
}

// newerThan — порядок ADR-052: больший CreateAt новее; при равном —
// МЕНЬШИЙ id. Обязан совпадать с ORDER BY CreateAt ASC, Id DESC ниже.
func (p dmPos) newerThan(q dmPos) bool {
	if p.CreateAt != q.CreateAt {
		return p.CreateAt > q.CreateAt
	}
	return p.ID < q.ID
}

// dmStore — доступ протокола v2 к постам. Интерфейс — ради дублёра в
// модульных тестах обработчика; семантика SQL проверяется контрактными
// тестами против настоящего PostgreSQL (dm_sql_pg_test.go).
type dmStore interface {
	// Newest — последние limit постов, от старых к новым (первичная
	// синхронизация, §2.2.1).
	Newest(ctx context.Context, channelID string, authors dmAuthors, limit int) ([]dmRow, error)
	// From — посты с CreateAt ≥ lower, от старых к новым (начало прохода,
	// §2.2.2).
	From(ctx context.Context, channelID string, authors dmAuthors, lower int64, limit int) ([]dmRow, error)
	// After — посты строго новее pos, от старых к новым (продолжение,
	// §2.2.3).
	After(ctx context.Context, channelID string, authors dmAuthors, pos dmPos, limit int) ([]dmRow, error)
	// WithSendLock — транзакция под advisory lock (sender, client_msg_id)
	// (§2.5.1). fn получает поиск, выполняемый в этой транзакции.
	WithSendLock(ctx context.Context, sender, clientMsgID string, wait time.Duration, fn func(find dmFindFunc) error) error
}

// dmBeforeStore — чтение назад (219, ADR-105). Отдельный интерфейс, чтобы не
// менять dmStore и дублёры 218; реализует *pgDMStore.
type dmBeforeStore interface {
	// Before — не больше limit постов строго старше pos в порядке ADR-092
	// §2.1, ближайшие к pos, от старых к новым. Окна нет (ADR-105 §1.3).
	Before(ctx context.Context, channelID string, authors dmAuthors, pos dmPos, limit int) ([]dmRow, error)
}

// dmFindFunc — поиск своего поста по метке идемпотентности, включая
// удалённые (§2.5.1 п. 2).
type dmFindFunc func(channelID, userID, clientMsgID string, since int64) (*dmRow, error)

// errSendLockTimeout — блокировка не получена за отведённое время:
// одновременный повтор той же операции ещё выполняется.
var errSendLockTimeout = errors.New("dm: send lock wait timed out")

// dmPropClientMsgID — ключ метки в Post.Props (§2.5.1).
const dmPropClientMsgID = "engram_client_msg_id"

type pgDMStore struct {
	db *sql.DB
	// pollEvery — шаг повторной попытки взять блокировку.
	pollEvery time.Duration
}

func newPGDMStore(db *sql.DB) *pgDMStore {
	return &pgDMStore{db: db, pollEvery: 50 * time.Millisecond}
}

const dmSelect = `SELECT Id, UserId, Message, CreateAt, DeleteAt,
	COALESCE(Props->>'` + dmPropClientMsgID + `', '')
FROM Posts
WHERE ChannelId = $1 AND DeleteAt = 0 AND Type = '' AND UserId IN ($2, $3)`

func scanRows(rows *sql.Rows) ([]dmRow, error) {
	defer rows.Close()
	var out []dmRow
	for rows.Next() {
		var r dmRow
		if err := rows.Scan(&r.ID, &r.UserID, &r.Message, &r.CreateAt, &r.DeleteAt, &r.ClientMsgID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// dmAuthors — оба участника личного канала (с собой — один и тот же id
// дважды). Посты прочих авторов в выдачу не попадают (ADR-082 §3.3), и
// фильтр стоит в SQL, а не после LIMIT: иначе неполная страница выглядела
// бы как at_end.
type dmAuthors [2]string

func (s *pgDMStore) Newest(ctx context.Context, channelID string, authors dmAuthors, limit int) ([]dmRow, error) {
	rows, err := s.db.QueryContext(ctx, dmSelect+`
ORDER BY CreateAt DESC, Id ASC LIMIT $4`, channelID, authors[0], authors[1], limit)
	if err != nil {
		return nil, err
	}
	out, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	// От старых к новым: обратный порядок к (CreateAt DESC, Id ASC) —
	// ровно (CreateAt ASC, Id DESC).
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

func (s *pgDMStore) From(ctx context.Context, channelID string, authors dmAuthors, lower int64, limit int) ([]dmRow, error) {
	rows, err := s.db.QueryContext(ctx, dmSelect+` AND CreateAt >= $4
ORDER BY CreateAt ASC, Id DESC LIMIT $5`, channelID, authors[0], authors[1], lower, limit)
	if err != nil {
		return nil, err
	}
	return scanRows(rows)
}

func (s *pgDMStore) After(ctx context.Context, channelID string, authors dmAuthors, pos dmPos, limit int) ([]dmRow, error) {
	rows, err := s.db.QueryContext(ctx, dmSelect+` AND (CreateAt > $4 OR (CreateAt = $4 AND Id < $5))
ORDER BY CreateAt ASC, Id DESC LIMIT $6`, channelID, authors[0], authors[1], pos.CreateAt, pos.ID, limit)
	if err != nil {
		return nil, err
	}
	return scanRows(rows)
}

// Before: порядок ленты (CreateAt ASC, Id DESC), поэтому «строго старше
// (T, id)» — CreateAt < T ИЛИ (CreateAt = T И Id > id): при равном CreateAt
// пост с БОЛЬШИМ id стоит раньше (обратное After).
func (s *pgDMStore) Before(ctx context.Context, channelID string, authors dmAuthors, pos dmPos, limit int) ([]dmRow, error) {
	rows, err := s.db.QueryContext(ctx, dmSelect+` AND (CreateAt < $4 OR (CreateAt = $4 AND Id > $5))
ORDER BY CreateAt DESC, Id ASC LIMIT $6`, channelID, authors[0], authors[1], pos.CreateAt, pos.ID, limit)
	if err != nil {
		return nil, err
	}
	out, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// sendLockKey — 64-битный ключ advisory lock из (отправитель,
// client_msg_id). Префикс отделяет ключи Photon от чужих advisory lock той
// же БД.
func sendLockKey(sender, clientMsgID string) int64 {
	h := sha256.Sum256([]byte("photon:dm-send:" + sender + ":" + clientMsgID))
	return int64(binary.BigEndian.Uint64(h[:8]))
}

// WithSendLock — транзакционная блокировка pg_try_advisory_xact_lock.
//
// Уровень транзакции, а не сессии, выбран намеренно: блокировка снимается
// концом транзакции (COMMIT/ROLLBACK) или закрытием соединения, и забыть
// её снять невозможно. Соединение принадлежит Mattermost (RPC-драйвер
// плагина): при падении или перезапуске плагина supervisor.Shutdown
// вызывает ShutdownConns(pluginID), соединение закрывается — блокировка
// освобождается. БД общая для узлов, поэтому блокировка сериализует
// повторы и между несколькими экземплярами Bridge.
//
// Ожидание — опросом pg_try_*, а не блокирующим pg_advisory_xact_lock с
// lock_timeout: исход «не дождались» различим без разбора текста ошибки,
// который через RPC-драйвер приходит строкой.
func (s *pgDMStore) WithSendLock(ctx context.Context, sender, clientMsgID string, wait time.Duration, fn func(find dmFindFunc) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	key := sendLockKey(sender, clientMsgID)
	deadline := time.Now().Add(wait)
	for {
		var got bool
		if err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock($1)`, key).Scan(&got); err != nil {
			return err
		}
		if got {
			break
		}
		if time.Now().After(deadline) {
			return errSendLockTimeout
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.pollEvery):
		}
	}

	// Поиск — после получения блокировки, отдельным оператором: в READ
	// COMMITTED он видит всё, что зафиксировал предыдущий держатель, включая
	// пост, созданный Mattermost через его собственное соединение.
	find := func(channelID, userID, cid string, since int64) (*dmRow, error) {
		var r dmRow
		e := tx.QueryRowContext(ctx, `SELECT Id, UserId, Message, CreateAt, DeleteAt,
	COALESCE(Props->>'`+dmPropClientMsgID+`', '')
FROM Posts
WHERE ChannelId = $1 AND UserId = $2 AND CreateAt >= $3
  AND Props->>'`+dmPropClientMsgID+`' = $4
ORDER BY CreateAt ASC, Id DESC LIMIT 1`, channelID, userID, since, cid).
			Scan(&r.ID, &r.UserID, &r.Message, &r.CreateAt, &r.DeleteAt, &r.ClientMsgID)
		if errors.Is(e, sql.ErrNoRows) {
			return nil, nil
		}
		if e != nil {
			return nil, e
		}
		return &r, nil
	}

	if err = fn(find); err != nil {
		return err
	}
	return tx.Commit()
}

// ── проверка схемы (§5) ──────────────────────────────────────────────

// dmSchemaColumns — колонки Posts, на которые опирается протокол, и
// ожидаемый тип (information_schema.columns.data_type).
var dmSchemaColumns = map[string]string{
	"id":        "character varying",
	"channelid": "character varying",
	"userid":    "character varying",
	"createat":  "bigint",
	"deleteat":  "bigint",
	"type":      "character varying",
	"message":   "character varying",
	"props":     "jsonb",
}

// dmSchemaIndex — индекс, без которого S1 становится полным обходом канала.
const dmSchemaIndex = "(channelid, deleteat, createat)"

// checkDMSchema проверяет драйвер, колонки, типы и индекс. Ошибка
// перечисляет всё найденное расхождение сразу — чинить по одному пункту
// за перезапуск незачем.
func checkDMSchema(ctx context.Context, db *sql.DB, driverName string) error {
	if driverName != "postgres" {
		return fmt.Errorf("dm v2: SqlSettings.DriverName = %q, поддерживается только postgres", driverName)
	}
	rows, err := db.QueryContext(ctx, `SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = 'posts'`)
	if err != nil {
		return fmt.Errorf("dm v2: чтение схемы posts: %w", err)
	}
	have := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			rows.Close()
			return fmt.Errorf("dm v2: чтение схемы posts: %w", err)
		}
		have[name] = typ
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("dm v2: чтение схемы posts: %w", err)
	}

	var problems []string
	for col, typ := range dmSchemaColumns {
		got, ok := have[col]
		switch {
		case !ok:
			problems = append(problems, "нет колонки posts."+col)
		case got != typ:
			problems = append(problems, fmt.Sprintf("posts.%s: тип %q, ожидался %q", col, got, typ))
		}
	}

	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_indexes
WHERE schemaname = current_schema() AND tablename = 'posts' AND indexdef LIKE '%' || $1 || '%'`,
		dmSchemaIndex).Scan(&n); err != nil {
		return fmt.Errorf("dm v2: чтение индексов posts: %w", err)
	}
	if n == 0 {
		problems = append(problems, "нет индекса posts"+dmSchemaIndex)
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("dm v2: схема Mattermost несовместима: %s", strings.Join(problems, "; "))
	}
	return nil
}

// initDMv2 — подключение S1 и проверка схемы при активации (§5). Отказ не
// роняет активацию: выключается только v2, 213/214 продолжают работать, а
// причина пишется в журнал явно и отдаётся клиенту кодом sync_unavailable.
func (p *Plugin) initDMv2() {
	db, err := p.client.Store.GetMasterDB()
	if err != nil {
		p.dmStoreErr = fmt.Errorf("dm v2: нет соединения с БД: %w", err)
		p.API.LogError("[Photon][DMv2] disabled", "error", p.dmStoreErr.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := checkDMSchema(ctx, db, p.client.Store.DriverName()); err != nil {
		p.dmStoreErr = err
		p.API.LogError("[Photon][DMv2] disabled", "error", err.Error())
		return
	}
	p.dmStore = newPGDMStore(db)
	p.API.LogInfo("[Photon][DMv2] enabled: schema check passed")
}
