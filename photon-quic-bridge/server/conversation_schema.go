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

	"github.com/mattermost/mattermost/server/public/model"
)

// Таблицы Bridge для разговоров ENGRAM (ADR-100 §2.4–§2.6).
//
// DDL — photon-quic-bridge/docs/conversation-storage.md §2 плюс индекс
// engram_position_key_all (implementation contract 900-931 §2.2). Таблицы
// принадлежат Bridge: Mattermost их не читает и не изменяет, Bridge не
// выполняет DDL над объектами Mattermost (ADR-100 §2.3, §2.6). Имена без
// кавычек и без схемы — текущая схема соединения, как у Mattermost.
//
// Соединение — pluginapi.Store, только основная БД: реплика могла бы не
// увидеть только что записанное (ADR-100 §2.1).

// convSchemaVersion — версия схемы, которую знает этот код.
const convSchemaVersion = 11

// convMigrations[i] переводит схему с версии i на i+1. Только вперёд: правка
// уже выпущенной миграции запрещена, изменение — новой строкой.
var convMigrations = []string{
	// v1 — conversation-storage.md §2 и индекс engram_position_key_all.
	`
CREATE TABLE engram_service_team (
  team_name  text PRIMARY KEY,
  team_id    text UNIQUE,
  state      text NOT NULL CHECK (state IN ('reserved','created','full')),
  carriers   int  NOT NULL DEFAULT 0,
  created_at bigint NOT NULL
);

CREATE TABLE engram_conversation (
  conversation_id    text PRIMARY KEY,
  kind               text NOT NULL CHECK (kind IN ('group')),
  state              text NOT NULL CHECK (state IN ('creating','active','archived')),
  title              text NOT NULL,
  description        text NOT NULL DEFAULT '',
  history_policy     text NOT NULL CHECK (history_policy IN ('full','since_join')),
  version            bigint NOT NULL,
  created_at         bigint NOT NULL,
  created_by         text NOT NULL,
  member_count       int NOT NULL,
  last_activity_at   bigint NOT NULL,
  position_h         bigint NOT NULL DEFAULT 0,
  carrier_state      text NOT NULL CHECK (carrier_state IN ('reserved','bound')),
  carrier_team       text NOT NULL REFERENCES engram_service_team(team_name),
  carrier_name       text NOT NULL UNIQUE,
  carrier_channel_id text UNIQUE,
  reserved_at        bigint NOT NULL,
  CHECK ((carrier_state = 'bound') = (carrier_channel_id IS NOT NULL))
);
CREATE INDEX engram_conversation_creating
  ON engram_conversation (reserved_at) WHERE state = 'creating';

CREATE TABLE engram_member_period (
  conversation_id text NOT NULL REFERENCES engram_conversation,
  photon_id       text NOT NULL,
  period_no       int  NOT NULL,
  role            text NOT NULL CHECK (role IN ('owner','admin','member')),
  state           text NOT NULL CHECK (state IN ('active','left','removed')),
  opened_version  bigint NOT NULL,
  closed_version  bigint,
  open_at         bigint,  open_post_id  text,
  close_at        bigint,  close_post_id text,
  PRIMARY KEY (conversation_id, photon_id, period_no)
);
CREATE UNIQUE INDEX engram_member_one_active
  ON engram_member_period (conversation_id, photon_id) WHERE state = 'active';
CREATE INDEX engram_member_by_user
  ON engram_member_period (photon_id, conversation_id) WHERE state = 'active';

CREATE TABLE engram_request (
  photon_id       text   NOT NULL,
  action          int    NOT NULL,
  request_id      uuid   NOT NULL,
  payload_hash    bytea  NOT NULL,
  conversation_id text,
  state           text   NOT NULL CHECK (state IN ('pending','done')),
  response        jsonb,
  created_at      bigint NOT NULL,
  PRIMARY KEY (photon_id, action, request_id)
);
CREATE INDEX engram_request_age ON engram_request (created_at);

CREATE TABLE engram_event (
  conversation_id text   NOT NULL REFERENCES engram_conversation,
  version         bigint NOT NULL,
  seq             int    NOT NULL,
  event           jsonb  NOT NULL,
  at              bigint NOT NULL,
  state           text   NOT NULL CHECK (state IN ('pending','written')),
  post_id         text,
  PRIMARY KEY (conversation_id, version, seq)
);
CREATE INDEX engram_event_pending ON engram_event (at) WHERE state = 'pending';

CREATE TABLE engram_position (
  conversation_id text   NOT NULL REFERENCES engram_conversation,
  pos             bigint NOT NULL,
  item_kind       text   NOT NULL CHECK (item_kind IN ('message','event')),
  item_key        text   NOT NULL,
  state           text   NOT NULL CHECK (state IN ('reserved','published','aborted')),
  sender          text,
  payload_hash    bytea,
  reserved_at     bigint NOT NULL,
  post_id         text,
  PRIMARY KEY (conversation_id, pos)
);
CREATE UNIQUE INDEX engram_position_key_live
  ON engram_position (conversation_id, item_key) WHERE state <> 'aborted';
CREATE INDEX engram_position_open
  ON engram_position (conversation_id, pos) WHERE state = 'reserved';
CREATE INDEX engram_position_open_age
  ON engram_position (reserved_at) WHERE state = 'reserved';
CREATE INDEX engram_position_key_all
  ON engram_position (conversation_id, item_key);
`,
	// v2 — отметка первой попытки CreateTeam служебной команды
	// (conversation-storage.md §6.0.3). Доказательством владения не является.
	`ALTER TABLE engram_service_team ADD COLUMN create_attempted_at bigint;`,
	// v3 — direct-разговоры (ADR-104 §3.2): вид direct и пара участников.
	// low_pid < high_pid — строки канонического PhotonID; пара принадлежит
	// ровно одному разговору в обе стороны.
	`
ALTER TABLE engram_conversation DROP CONSTRAINT engram_conversation_kind_check;
ALTER TABLE engram_conversation ADD CONSTRAINT engram_conversation_kind_check CHECK (kind IN ('group','direct'));
CREATE TABLE engram_direct_pair (
  low_pid         text NOT NULL,
  high_pid        text NOT NULL,
  conversation_id text NOT NULL UNIQUE,
  PRIMARY KEY (low_pid, high_pid),
  CHECK (low_pid < high_pid)
);
`,
	// v4 — личное состояние участника (ADR-106 §2.2). Не участие и не права;
	// частичный индекс — подсчёт закреплённых одного участника (предел 5).
	`
CREATE TABLE engram_member_state (
  conversation_id text   NOT NULL,
  photon_id       text   NOT NULL,
  hidden_at       bigint,
  pinned_at       bigint,
  muted_until     bigint,
  updated_at      bigint NOT NULL,
  PRIMARY KEY (conversation_id, photon_id)
);
CREATE INDEX engram_member_state_pinned ON engram_member_state (photon_id) WHERE pinned_at IS NOT NULL;
`,
	// v5 — приглашения по ссылке (ADR-107 этап 1). Хранится только SHA-256
	// токена; токен 128 бит виден лишь в ответе 913 и в сохранённом ответе ключа.
	`
CREATE TABLE engram_invite (
  invite_id       text   PRIMARY KEY,
  conversation_id text   NOT NULL,
  token_hash      bytea  NOT NULL UNIQUE,
  created_by      text,
  created_at      bigint,
  expires_at      bigint,
  max_uses        int,
  uses            int    NOT NULL DEFAULT 0,
  revoked_at      bigint
);
CREATE INDEX engram_invite_conversation ON engram_invite (conversation_id);
`,
	// v6 — публичные разговоры (ADR-107 этап 2): правило visibility; прежние
	// строки — private.
	`
ALTER TABLE engram_conversation ADD COLUMN visibility text NOT NULL DEFAULT 'private';
ALTER TABLE engram_conversation ADD CONSTRAINT engram_conversation_visibility_check CHECK (visibility IN ('private','public'));
`,
	// v7 — позиция прочтения участника (ADR-108 §2.1; contract §5b.1):
	// отдельная таблица, а не столбцы engram_member_state — у них разный
	// смысл updated_at.
	`
CREATE TABLE engram_read_state (
  conversation_id text   NOT NULL,
  photon_id       text   NOT NULL,
  read_created_at bigint NOT NULL,
  read_id         text   NOT NULL,
  updated_at      bigint NOT NULL,
  PRIMARY KEY (conversation_id, photon_id)
);
`,
	// v8 — правка и удаление для всех (ADR-109 §2.1–2.2; contract §5b.2):
	// отметка удаления и счётчик правок на позиции сообщения; текущий текст
	// после правки — отдельная таблица (пост-носитель при правке не меняется).
	`
ALTER TABLE engram_position ADD COLUMN deleted_at bigint;
ALTER TABLE engram_position ADD COLUMN edit_version int NOT NULL DEFAULT 0;
CREATE TABLE engram_item_edit (
  conversation_id text   NOT NULL,
  item_id         text   NOT NULL,
  text            text   NOT NULL,
  edit_version    int    NOT NULL,
  updated_at      bigint NOT NULL,
  PRIMARY KEY (conversation_id, item_id)
);
`,
	// v9 — серверный поиск сообщений (ADR-110 Q2; contract §5b.3): индекс
	// нормализованного текста в таблице Bridge. Сам backfill уже
	// опубликованных сообщений — Go-шаг convMigrationSteps[8]: нормализация
	// (регистр, ё) должна быть той же, что у запроса, а lower() PostgreSQL
	// зависит от локали базы.
	`
CREATE TABLE engram_item_text (
  item_id         text   PRIMARY KEY,
  conversation_id text   NOT NULL,
  created_at      bigint NOT NULL,
  text_norm       text   NOT NULL
);
CREATE INDEX engram_item_text_conv ON engram_item_text (conversation_id, created_at DESC, item_id);
`,
	// v10 — блокировки и бан (ADR-111; contract §5b.4): личная блокировка
	// пользователя, бан PhotonID в разговоре и состояние banned периода
	// участия (ограничение state пересоздаётся, как kind в v3).
	`
CREATE TABLE engram_user_block (
  blocker_pid text   NOT NULL,
  blocked_pid text   NOT NULL,
  created_at  bigint NOT NULL,
  PRIMARY KEY (blocker_pid, blocked_pid)
);
CREATE TABLE engram_conversation_ban (
  conversation_id text   NOT NULL,
  photon_id       text   NOT NULL,
  banned_at       bigint NOT NULL,
  banned_by       text   NOT NULL,
  PRIMARY KEY (conversation_id, photon_id)
);
ALTER TABLE engram_member_period DROP CONSTRAINT engram_member_period_state_check;
ALTER TABLE engram_member_period ADD CONSTRAINT engram_member_period_state_check CHECK (state IN ('active','left','removed','banned'));
`,
	// v11 — реакции (ADR-109 §2.3; contract §5b.5): множество
	// (элемент, участник, реакция).
	`
CREATE TABLE engram_reaction (
  item_id    text   NOT NULL,
  photon_id  text   NOT NULL,
  reaction   text   NOT NULL,
  created_at bigint NOT NULL,
  PRIMARY KEY (item_id, photon_id, reaction)
);
`,
}

// convMigrationSteps[i] — Go-шаг, выполняемый в той же транзакции сразу после
// SQL миграции i→i+1.
var convMigrationSteps = map[int]func(ctx context.Context, tx *sql.Tx) error{
	8: backfillItemText,
}

var (
	errConvSchemaNewer       = errors.New("conversation: схема Bridge новее кода плагина")
	errConvSchemaLockTimeout = errors.New("conversation: блокировка миграции не получена в бюджет ожидания")
)

// convSchemaLockKey — ключ advisory-блокировки миграций (conversation-storage.md
// §5: фиксированный ключ photon:schema), та же форма, что sendLockKey.
func convSchemaLockKey() int64 {
	h := sha256.Sum256([]byte("photon:schema"))
	return int64(binary.BigEndian.Uint64(h[:8]))
}

// migrateConversationSchema применяет недостающие миграции одной
// транзакцией под транзакционной advisory-блокировкой (ADR-100 §2.6:
// несколько узлов не мигрируют одновременно). DDL PostgreSQL
// транзакционен: при ошибке схема остаётся прежней целиком.
//
// Блокировка — попыткой с опросом в пределах wait, как WithSendLock: исход
// «не дождались» различим без разбора текста ошибки (conversation-storage.md
// §5). Сессионная блокировка не годится: через pluginapi Store она
// переживает падение плагина (ap1-ordering-research.md, R7).
func migrateConversationSchema(ctx context.Context, db *sql.DB, wait, pollEvery time.Duration) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	key := convSchemaLockKey()
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
			return errConvSchemaLockTimeout
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollEvery):
		}
	}

	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS engram_schema (version int NOT NULL)`); err != nil {
		return fmt.Errorf("conversation: engram_schema: %w", err)
	}
	version, err := readConvSchemaVersion(ctx, tx)
	if err != nil {
		return err
	}
	if version > convSchemaVersion {
		return fmt.Errorf("%w: в базе %d, код знает %d", errConvSchemaNewer, version, convSchemaVersion)
	}
	if version == convSchemaVersion {
		return tx.Commit()
	}
	for v := version; v < convSchemaVersion; v++ {
		if _, err = tx.ExecContext(ctx, convMigrations[v]); err != nil {
			return fmt.Errorf("conversation: миграция %d→%d: %w", v, v+1, err)
		}
		if step := convMigrationSteps[v]; step != nil {
			if err = step(ctx, tx); err != nil {
				return fmt.Errorf("conversation: миграция %d→%d: %w", v, v+1, err)
			}
		}
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM engram_schema`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO engram_schema (version) VALUES ($1)`, convSchemaVersion); err != nil {
		return err
	}
	return tx.Commit()
}

type convQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// readConvSchemaVersion — версия из engram_schema; нет строки — 0. Больше
// одной строки — повреждённая схема, а не «какая-то» версия.
func readConvSchemaVersion(ctx context.Context, q convQueryer) (int, error) {
	rows, err := q.QueryContext(ctx, `SELECT version FROM engram_schema`)
	if err != nil {
		return 0, fmt.Errorf("conversation: чтение engram_schema: %w", err)
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return 0, fmt.Errorf("conversation: чтение engram_schema: %w", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("conversation: чтение engram_schema: %w", err)
	}
	switch len(versions) {
	case 0:
		return 0, nil
	case 1:
		return versions[0], nil
	default:
		return 0, fmt.Errorf("conversation: в engram_schema %d строк, ожидалась одна", len(versions))
	}
}

// convSchemaTables — таблицы схемы (v1–v11).
var convSchemaTables = []string{
	"engram_schema", "engram_service_team", "engram_conversation",
	"engram_member_period", "engram_request", "engram_event", "engram_position",
	"engram_direct_pair", "engram_member_state", "engram_invite", "engram_read_state", "engram_item_edit", "engram_item_text",
	"engram_user_block", "engram_conversation_ban", "engram_reaction",
}

// convIndexSpec — индекс, на который опираются инварианты и запросы
// (conversation-storage.md §2, §3; contract §2).
type convIndexSpec struct {
	table   string
	columns string // как в pg_indexes.indexdef: "(a, b)"
	unique  bool
	where   string // подстрока условия частичного индекса; "" — без условия
}

var convSchemaIndexes = map[string]convIndexSpec{
	"engram_conversation_creating": {"engram_conversation", "(reserved_at)", false, "creating"},
	"engram_member_one_active":     {"engram_member_period", "(conversation_id, photon_id)", true, "active"},
	"engram_member_by_user":        {"engram_member_period", "(photon_id, conversation_id)", false, "active"},
	"engram_request_age":           {"engram_request", "(created_at)", false, ""},
	"engram_event_pending":         {"engram_event", "(at)", false, "pending"},
	// не более одного неотменённого резерва на ключ (ADR-101 §5.6)
	"engram_position_key_live": {"engram_position", "(conversation_id, item_key)", true, "aborted"},
	// барьер (ADR-101 §5.1b)
	"engram_position_open": {"engram_position", "(conversation_id, pos)", false, "reserved"},
	// сверка брошенных резервов
	"engram_position_open_age": {"engram_position", "(reserved_at)", false, "reserved"},
	// владелец ключа во всех состояниях, включая надгробия (ADR-102 §2)
	"engram_position_key_all": {"engram_position", "(conversation_id, item_key)", false, ""},
	// пара direct → один разговор, и разговор → одна пара (ADR-104 §3.2)
	"engram_direct_pair_pkey":                {"engram_direct_pair", "(low_pid, high_pid)", true, ""},
	"engram_direct_pair_conversation_id_key": {"engram_direct_pair", "(conversation_id)", true, ""},
	// закреплённые одного участника (ADR-106 Q4)
	"engram_member_state_pinned": {"engram_member_state", "(photon_id)", false, "pinned_at IS NOT NULL"},
	// приглашения: по разговору и по хэшу токена (ADR-107 этап 1)
	"engram_invite_conversation":   {"engram_invite", "(conversation_id)", false, ""},
	"engram_invite_token_hash_key": {"engram_invite", "(token_hash)", true, ""},
	// поиск сообщений (ADR-110)
	"engram_item_text_conv": {"engram_item_text", "(conversation_id, created_at DESC, item_id)", false, ""},
}

// checkConversationSchema — проверка при активации (ADR-100 §2.6): драйвер,
// версия, таблицы, счётчик позиций, индексы и колонки Posts, на которые
// опираются 930/931 (conversation-storage.md §9 — та же проверка, что
// checkDMSchema). Ошибка перечисляет всё найденное расхождение сразу.
func checkConversationSchema(ctx context.Context, db *sql.DB, driverName string) error {
	if driverName != "postgres" {
		return fmt.Errorf("conversation: SqlSettings.DriverName = %q, поддерживается только postgres", driverName)
	}
	var problems []string

	for _, t := range convSchemaTables {
		var reg sql.NullString
		if err := db.QueryRowContext(ctx, `SELECT to_regclass($1)::text`, t).Scan(&reg); err != nil {
			return fmt.Errorf("conversation: чтение схемы: %w", err)
		}
		if !reg.Valid {
			problems = append(problems, "нет таблицы "+t)
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("conversation: схема Bridge неполна: %s", strings.Join(problems, "; "))
	}

	version, err := readConvSchemaVersion(ctx, db)
	if err != nil {
		return err
	}
	if version != convSchemaVersion {
		if version > convSchemaVersion {
			return fmt.Errorf("%w: в базе %d, код знает %d", errConvSchemaNewer, version, convSchemaVersion)
		}
		problems = append(problems, fmt.Sprintf("версия схемы %d, ожидалась %d", version, convSchemaVersion))
	}

	var attType sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT data_type FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = 'engram_service_team' AND column_name = 'create_attempted_at'`).Scan(&attType); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("conversation: чтение схемы: %w", err)
	}
	if attType.String != "bigint" {
		problems = append(problems, fmt.Sprintf("engram_service_team.create_attempted_at: тип %q, ожидался \"bigint\"", attType.String))
	}

	var hType sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT data_type FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = 'engram_conversation' AND column_name = 'position_h'`).Scan(&hType); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("conversation: чтение схемы: %w", err)
	}
	if hType.String != "bigint" {
		problems = append(problems, fmt.Sprintf("engram_conversation.position_h: тип %q, ожидался \"bigint\"", hType.String))
	}

	// v8: колонки позиции сообщения для правки и удаления (ADR-109).
	for _, col := range []struct{ name, typ string }{{"deleted_at", "bigint"}, {"edit_version", "integer"}} {
		var colType sql.NullString
		if err := db.QueryRowContext(ctx, `SELECT data_type FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = 'engram_position' AND column_name = $1`, col.name).Scan(&colType); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("conversation: чтение схемы: %w", err)
		}
		if colType.String != col.typ {
			problems = append(problems, fmt.Sprintf("engram_position.%s: тип %q, ожидался %q", col.name, colType.String, col.typ))
		}
	}

	var kindDef sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c
WHERE c.conrelid = to_regclass('engram_conversation') AND c.conname = 'engram_conversation_kind_check'`).Scan(&kindDef); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("conversation: чтение схемы: %w", err)
	}
	if !strings.Contains(kindDef.String, "'direct'") {
		problems = append(problems, fmt.Sprintf("engram_conversation.kind: ограничение %q не допускает direct", kindDef.String))
	}

	// v10: состояние banned периода участия (ADR-111).
	var stateDef sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c
WHERE c.conrelid = to_regclass('engram_member_period') AND c.conname = 'engram_member_period_state_check'`).Scan(&stateDef); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("conversation: чтение схемы: %w", err)
	}
	if !strings.Contains(stateDef.String, "'banned'") {
		problems = append(problems, fmt.Sprintf("engram_member_period.state: ограничение %q не допускает banned", stateDef.String))
	}

	type idx struct {
		table, def string
		unique     bool
	}
	have := map[string]idx{}
	rows, err := db.QueryContext(ctx, `SELECT i.tablename, i.indexname, i.indexdef, x.indisunique
FROM pg_indexes i
JOIN pg_class c ON c.relname = i.indexname
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = i.schemaname
JOIN pg_index x ON x.indexrelid = c.oid
WHERE i.schemaname = current_schema() AND i.tablename LIKE 'engram\_%'`)
	if err != nil {
		return fmt.Errorf("conversation: чтение индексов: %w", err)
	}
	for rows.Next() {
		var table, name, def string
		var unique bool
		if err := rows.Scan(&table, &name, &def, &unique); err != nil {
			rows.Close()
			return fmt.Errorf("conversation: чтение индексов: %w", err)
		}
		have[name] = idx{table, def, unique}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("conversation: чтение индексов: %w", err)
	}
	for name, want := range convSchemaIndexes {
		got, ok := have[name]
		switch {
		case !ok:
			problems = append(problems, "нет индекса "+name)
		case got.table != want.table:
			problems = append(problems, fmt.Sprintf("индекс %s на таблице %s, ожидалась %s", name, got.table, want.table))
		case got.unique != want.unique:
			problems = append(problems, fmt.Sprintf("индекс %s: unique=%v, ожидалось %v", name, got.unique, want.unique))
		case !strings.Contains(got.def, want.columns):
			problems = append(problems, fmt.Sprintf("индекс %s: %q не содержит %s", name, got.def, want.columns))
		case want.where == "" && strings.Contains(got.def, " WHERE "):
			problems = append(problems, fmt.Sprintf("индекс %s частичный, ожидался полный", name))
		case want.where != "" && !strings.Contains(got.def, want.where):
			problems = append(problems, fmt.Sprintf("индекс %s: условие не содержит %q", name, want.where))
		}
	}

	if err := checkDMSchema(ctx, db, driverName); err != nil {
		problems = append(problems, "Posts: "+err.Error())
	}

	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("conversation: схема несовместима: %s", strings.Join(problems, "; "))
	}
	return nil
}

// initConversations — миграции и проверка схемы при активации (ADR-100
// §2.6). Отказ не роняет активацию: остальной Bridge работает, а 900–931
// отвечают temporarily_unavailable. Обработчики поверх схемы появляются в
// следующих срезах; до тех пор 900–931 отвечают так в любом случае.
func (p *Plugin) initConversations() {
	driver := p.client.Store.DriverName()
	if driver != "postgres" {
		p.convStoreErr = fmt.Errorf("conversation: SqlSettings.DriverName = %q, поддерживается только postgres", driver)
		p.API.LogError("[Photon][Conv] disabled", "error", p.convStoreErr.Error())
		return
	}
	db, err := p.client.Store.GetMasterDB()
	if err != nil {
		p.convStoreErr = fmt.Errorf("conversation: нет соединения с БД: %w", err)
		p.API.LogError("[Photon][Conv] disabled", "error", p.convStoreErr.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := migrateConversationSchema(ctx, db, 30*time.Second, 100*time.Millisecond); err != nil {
		p.convStoreErr = err
		p.API.LogError("[Photon][Conv] disabled: migration failed", "error", err.Error())
		return
	}
	if err := checkConversationSchema(ctx, db, driver); err != nil {
		p.convStoreErr = err
		p.API.LogError("[Photon][Conv] disabled: schema check failed", "error", err.Error())
		return
	}
	p.convDB = db
	p.API.LogInfo("[Photon][Conv] schema ready", "version", convSchemaVersion)

	// Бот — §6.0.7. Отказ выключает только 900: служебные команды без бота
	// не создаются, а 901/902 от бота не зависят.
	botID, err := p.client.Bot.EnsureBot(&model.Bot{
		Username:    convBotUsername,
		DisplayName: "ENGRAM Bridge",
		Description: "Служебный пользователь Photon Bridge: участник служебных команд носителей ENGRAM.",
	})
	if err != nil {
		p.API.LogError("[Photon][Conv] EnsureBot failed: 900 unavailable", "error", err.Error())
		return
	}
	p.convBotID = botID
}
