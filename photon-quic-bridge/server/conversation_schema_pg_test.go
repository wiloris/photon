package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Контрактные тесты схемы разговоров против НАСТОЯЩЕГО PostgreSQL (ADR-100
// §2.6, conversation-storage.md §2, contract §2). Включаются той же
// переменной PHOTON_DM_PG_DSN, что и dm_sql_pg_test.go. Каждый тест работает
// в собственной схеме photon_convtest_<rand> и удаляет её в конце;
// public не читается и не пишется, кроме копии структуры public.posts для
// проверки Posts (LIKE … INCLUDING ALL, как в dm_sql_pg_test.go).

// convSchema — временная схема; withPosts — с копией структуры public.posts.
func convSchema(t *testing.T, withPosts bool) string {
	t.Helper()
	dsn := pgDSN(t)
	admin, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { admin.Close() })

	var b [6]byte
	_, _ = rand.Read(b[:])
	schema := "photon_convtest_" + hex.EncodeToString(b[:])
	_, err = admin.Exec(`CREATE SCHEMA ` + schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, e := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); e != nil {
			t.Errorf("не удалось удалить схему %s: %v", schema, e)
		}
	})
	if withPosts {
		_, err = admin.Exec(`CREATE TABLE ` + schema + `.posts (LIKE public.posts INCLUDING ALL)`)
		require.NoError(t, err)
	}
	return schema
}

func convMigrated(t *testing.T) *sql.DB {
	t.Helper()
	db := pgOpen(t, convSchema(t, true))
	require.NoError(t, migrateConversationSchema(context.Background(), db, 5*time.Second, 10*time.Millisecond))
	return db
}

func TestConvSchemaFreshMigrationPassesCheck(t *testing.T) {
	db := convMigrated(t)
	require.NoError(t, checkConversationSchema(context.Background(), db, "postgres"))
	v, err := readConvSchemaVersion(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, convSchemaVersion, v)
}

// v8 (ADR-109): прежние строки позиций получают deleted_at NULL и edit_version 0.
func TestConvSchemaV8DefaultsForExistingPositions(t *testing.T) {
	db := convMigrated(t)
	_, err := db.Exec(`INSERT INTO engram_service_team (team_name, state, created_at) VALUES ('t1', 'created', 1)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO engram_conversation (conversation_id, kind, state, title, history_policy, version,
created_at, created_by, member_count, last_activity_at, carrier_state, carrier_team, carrier_name, reserved_at)
VALUES ('c1', 'group', 'active', 'T', 'full', 1, 1, 'p', 1, 1, 'reserved', 't1', 'c-c1', 1)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, sender, reserved_at)
VALUES ('c1', 5, 'message', 'k', 'published', 's', 1)`)
	require.NoError(t, err)
	var del sql.NullInt64
	var ev int
	require.NoError(t, db.QueryRow(`SELECT deleted_at, edit_version FROM engram_position WHERE conversation_id = 'c1'`).Scan(&del, &ev))
	require.False(t, del.Valid)
	require.Equal(t, 0, ev)
}

func TestConvSchemaMigrationIsIdempotent(t *testing.T) {
	db := convMigrated(t)
	require.NoError(t, migrateConversationSchema(context.Background(), db, 5*time.Second, 10*time.Millisecond))
	require.NoError(t, checkConversationSchema(context.Background(), db, "postgres"))
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM engram_schema`).Scan(&n))
	require.Equal(t, 1, n)
}

// Несколько узлов при активации не мигрируют одновременно (ADR-100 §2.6):
// все исполнители успешны, схема применена один раз. Без блокировки
// параллельные CREATE TABLE падают на уже существующих объектах.
func TestConvSchemaConcurrentMigrationsApplyOnce(t *testing.T) {
	schema := convSchema(t, true)
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db, err := sql.Open("postgres", withSearchPath(pgDSN(t), schema))
			if err != nil {
				errs[i] = err
				return
			}
			defer db.Close()
			errs[i] = migrateConversationSchema(context.Background(), db, 10*time.Second, 5*time.Millisecond)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "исполнитель %d", i)
	}
	db := pgOpen(t, schema)
	require.NoError(t, checkConversationSchema(context.Background(), db, "postgres"))
}

// Схема новее кода (откат версии плагина) — отказ, не «миграция назад»
// (ADR-100 §2.6).
func TestConvSchemaNewerThanCodeRejected(t *testing.T) {
	db := convMigrated(t)
	_, err := db.Exec(`UPDATE engram_schema SET version = $1`, convSchemaVersion+1)
	require.NoError(t, err)
	err = migrateConversationSchema(context.Background(), db, 5*time.Second, 10*time.Millisecond)
	require.True(t, errors.Is(err, errConvSchemaNewer), "migrate: %v", err)
	err = checkConversationSchema(context.Background(), db, "postgres")
	require.True(t, errors.Is(err, errConvSchemaNewer), "check: %v", err)
}

// Блокировку миграции держит другой узел дольше бюджета — исход различим.
func TestConvSchemaLockTimeout(t *testing.T) {
	schema := convSchema(t, true)
	holder := pgOpen(t, schema)
	tx, err := holder.Begin()
	require.NoError(t, err)
	defer tx.Rollback()
	var got bool
	require.NoError(t, tx.QueryRow(`SELECT pg_try_advisory_xact_lock($1)`, convSchemaLockKey()).Scan(&got))
	require.True(t, got)

	db := pgOpen(t, schema)
	err = migrateConversationSchema(context.Background(), db, 100*time.Millisecond, 10*time.Millisecond)
	require.True(t, errors.Is(err, errConvSchemaLockTimeout), "migrate: %v", err)
}

func TestConvSchemaCheckDetectsBreakage(t *testing.T) {
	cases := []struct {
		name   string
		break_ string
		want   string
	}{
		{"нет индекса владельца ключа", `DROP INDEX engram_position_key_all`, "нет индекса engram_position_key_all"},
		{"key_live не уникален", `DROP INDEX engram_position_key_live;
CREATE INDEX engram_position_key_live ON engram_position (conversation_id, item_key) WHERE state <> 'aborted'`, "engram_position_key_live: unique=false"},
		{"key_live без условия", `DROP INDEX engram_position_key_live;
CREATE UNIQUE INDEX engram_position_key_live ON engram_position (conversation_id, item_key)`, "engram_position_key_live: условие"},
		{"key_all частичный", `DROP INDEX engram_position_key_all;
CREATE INDEX engram_position_key_all ON engram_position (conversation_id, item_key) WHERE state <> 'aborted'`, "engram_position_key_all частичный"},
		{"нет барьерного индекса", `DROP INDEX engram_position_open`, "нет индекса engram_position_open"},
		{"position_h не bigint", `ALTER TABLE engram_conversation ALTER COLUMN position_h TYPE int`, "position_h"},
		{"нет таблицы резервов", `DROP TABLE engram_position`, "нет таблицы engram_position"},
		{"v8: нет таблицы правок", `DROP TABLE engram_item_edit`, "нет таблицы engram_item_edit"},
		{"v8: нет deleted_at", `ALTER TABLE engram_position DROP COLUMN deleted_at`, "engram_position.deleted_at"},
		{"v8: edit_version не int", `ALTER TABLE engram_position ALTER COLUMN edit_version TYPE bigint`, "engram_position.edit_version"},
		{"v9: нет таблицы индекса текста", `DROP TABLE engram_item_text`, "нет таблицы engram_item_text"},
		{"v9: нет индекса текста", `DROP INDEX engram_item_text_conv`, "нет индекса engram_item_text_conv"},
		{"версия ниже", `UPDATE engram_schema SET version = 0`, "версия схемы 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := convMigrated(t)
			_, err := db.Exec(tc.break_)
			require.NoError(t, err)
			err = checkConversationSchema(context.Background(), db, "postgres")
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestConvSchemaCheckRequiresPosts(t *testing.T) {
	db := pgOpen(t, convSchema(t, false))
	require.NoError(t, migrateConversationSchema(context.Background(), db, 5*time.Second, 10*time.Millisecond))
	err := checkConversationSchema(context.Background(), db, "postgres")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Posts:")
}

func TestConvSchemaCheckRejectsNonPostgres(t *testing.T) {
	err := checkConversationSchema(context.Background(), nil, "mysql")
	require.Error(t, err)
	require.Contains(t, err.Error(), "только postgres")
}

// Ограничения таблиц, на которых стоят инварианты S7–S13 и ADR-101/102.
func TestConvSchemaConstraints(t *testing.T) {
	db := convMigrated(t)
	mustExec := func(q string, args ...any) {
		t.Helper()
		_, err := db.Exec(q, args...)
		require.NoError(t, err)
	}
	// SQLSTATE через интерфейс драйвера: lib/pq в go.mod косвенный, прямой
	// импорт ради одного типа не нужен.
	pgCode := func(err error) string {
		var se interface{ SQLState() string }
		if errors.As(err, &se) {
			return se.SQLState()
		}
		return ""
	}
	mustExec(`INSERT INTO engram_service_team (team_name, state, created_at) VALUES ('t1', 'created', 1)`)
	mustExec(`INSERT INTO engram_conversation (conversation_id, kind, state, title, history_policy, version,
created_at, created_by, member_count, last_activity_at, carrier_state, carrier_team, carrier_name, reserved_at)
VALUES ('c1', 'group', 'active', 'T', 'full', 1, 1, 'p', 1, 1, 'reserved', 't1', 'c-c1', 1)`)

	var h int64
	require.NoError(t, db.QueryRow(`SELECT position_h FROM engram_conversation WHERE conversation_id = 'c1'`).Scan(&h))
	require.Equal(t, int64(0), h, "position_h по умолчанию 0")

	ins := `INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, sender, reserved_at)
VALUES ('c1', $1, 'message', $2, $3, 's', 1)`
	mustExec(ins, 1, "k", "reserved")

	_, err := db.Exec(ins, 1, "other", "reserved")
	require.Equal(t, "23505", pgCode(err), "позиция выдаётся один раз (PK): %v", err)

	_, err = db.Exec(ins, 2, "k", "reserved")
	require.Equal(t, "23505", pgCode(err), "второй неотменённый резерв ключа: %v", err)

	mustExec(`UPDATE engram_position SET state = 'aborted' WHERE pos = 1`)
	mustExec(ins, 2, "k", "reserved") // надгробие не мешает новому резерву ключа
	_, err = db.Exec(ins, 3, "k", "published")
	require.Equal(t, "23505", pgCode(err), "второй неотменённый (published) резерв ключа: %v", err)

	_, err = db.Exec(ins, 4, "x", "unknown")
	require.Equal(t, "23514", pgCode(err), "состояние вне reserved/published/aborted: %v", err)
	_, err = db.Exec(`INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, reserved_at)
VALUES ('c1', 5, 'other', 'x', 'reserved', 1)`)
	require.Equal(t, "23514", pgCode(err), "вид элемента вне message/event: %v", err)
	_, err = db.Exec(`INSERT INTO engram_position (conversation_id, pos, item_kind, item_key, state, reserved_at)
VALUES ('nope', 6, 'message', 'x', 'reserved', 1)`)
	require.Equal(t, "23503", pgCode(err), "резерв несуществующего разговора: %v", err)

	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM engram_position WHERE conversation_id = 'c1' AND item_key = 'k'`).Scan(&n))
	require.Equal(t, 2, n, "индекс key_all не уникален: надгробие и живой резерв одного ключа")
}

// Проверка не спутана с другими объектами: в схеме без таблиц Bridge отказ
// называет недостающие таблицы, а не падает на чтении версии.
func TestConvSchemaCheckOnEmptySchema(t *testing.T) {
	db := pgOpen(t, convSchema(t, true))
	err := checkConversationSchema(context.Background(), db, "postgres")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "нет таблицы engram_schema"), err.Error())
}

// v3 (M2): миграция v2→v3 на базе с данными, вид direct, пара.
func TestConvSchemaV3UpgradeFromV2(t *testing.T) {
	ctx := context.Background()
	db := pgOpen(t, convSchema(t, true))
	_, err := db.Exec(`CREATE TABLE engram_schema (version int NOT NULL)`)
	require.NoError(t, err)
	for v := 0; v < 2; v++ {
		_, err = db.Exec(convMigrations[v])
		require.NoError(t, err)
	}
	_, err = db.Exec(`INSERT INTO engram_schema (version) VALUES (2)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO engram_service_team (team_name, state, created_at) VALUES ('t', 'created', 1)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO engram_conversation (conversation_id, kind, state, title, history_policy, version, created_at,
created_by, member_count, last_activity_at, carrier_state, carrier_team, carrier_name, reserved_at)
VALUES ('g1', 'group', 'active', 'g', 'full', 1, 1, 'p', 1, 1, 'reserved', 't', 'c-g1', 1)`)
	require.NoError(t, err)

	require.NoError(t, migrateConversationSchema(ctx, db, 5*time.Second, 10*time.Millisecond))
	require.NoError(t, checkConversationSchema(ctx, db, "postgres"))
	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM engram_conversation WHERE conversation_id = 'g1'`).Scan(&n))
	require.Equal(t, 1, n, "данные пережили миграцию")

	ins := `INSERT INTO engram_conversation (conversation_id, kind, state, title, history_policy, version, created_at,
created_by, member_count, last_activity_at, carrier_state, carrier_team, carrier_name, reserved_at)
VALUES ($1, $2, 'creating', '', 'full', 0, 1, 'p', 0, 0, 'reserved', 't', $3, 1)`
	_, err = db.Exec(ins, "d1", "direct", "c-d1")
	require.NoError(t, err)
	_, err = db.Exec(ins, "x1", "other", "c-x1")
	require.Error(t, err, "неизвестный вид по-прежнему отвергается")

	_, err = db.Exec(`INSERT INTO engram_direct_pair VALUES ('1', '2', 'd1')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO engram_direct_pair VALUES ('1', '2', 'd2')`)
	require.Error(t, err, "пара — один разговор")
	_, err = db.Exec(`INSERT INTO engram_direct_pair VALUES ('3', '4', 'd1')`)
	require.Error(t, err, "разговор — одна пара")
	_, err = db.Exec(`INSERT INTO engram_direct_pair VALUES ('6', '5', 'd3')`)
	require.Error(t, err, "low < high")

	// Проверка схемы замечает потерю пары и ограничения вида.
	_, err = db.Exec(`ALTER TABLE engram_direct_pair DROP CONSTRAINT engram_direct_pair_conversation_id_key`)
	require.NoError(t, err)
	require.ErrorContains(t, checkConversationSchema(ctx, db, "postgres"), "engram_direct_pair_conversation_id_key")
	_, err = db.Exec(`ALTER TABLE engram_direct_pair ADD CONSTRAINT engram_direct_pair_conversation_id_key UNIQUE (conversation_id)`)
	require.NoError(t, err)
	_, err = db.Exec(`ALTER TABLE engram_conversation DROP CONSTRAINT engram_conversation_kind_check`)
	require.NoError(t, err)
	require.ErrorContains(t, checkConversationSchema(ctx, db, "postgres"), "не допускает direct")
}
