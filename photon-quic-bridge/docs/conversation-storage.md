# Conversation Storage — хранилище состояния разговоров ENGRAM в Bridge

**Статус:** ПРЕДЛОЖЕНО. Спецификация до кода; реализации нет. Архитектурная
проблема AP-1 (§13) закрыта **ADR-101** (ПРИНЯТО, `a1468b0`): позиции
элементов ленты группы назначает Bridge по механизму B — счётчик `H`,
резерв на каждую позицию, барьер видимости, надгробие, W = 0.
**Синхронизировано с ADR-101 и ADR-102 (поправка к ADR-101 §5.6, `972cba6`)
2026-10-04**; при расхождении правы они.
Положения, которые описывали прежнюю модель (дописывание незаписанных
событий перед каждой операцией, E5/E6, окно W для групп), помечены
«устарело — ADR-101» и оставлены для истории решения.

Опирается на ADR-101 (порядок ленты группы) и ADR-102 (чужой
`client_msg_id` при 930), ADR-100 (носитель — собственные
таблицы Bridge в PostgreSQL-базе Mattermost через `pluginapi` Store), ADR-099
(протокол 900–931), ADR-098 (§4 участие, §7 лента), ADR-097 (граница
хранения), ADR-093 (исходящие Core), ADR-092 ред. 3 (§2 проход ленты, §2.5
идемпотентность отправки, §5 чтение `Posts`).

Задаёт: схему таблиц Bridge, инварианты, порядок шагов каждой операции,
позицию элементов ленты (по ADR-101), блокировки, SQL ключевых запросов с
индексами, сверку и проверки при активации. **Не задаёт** того, что ADR-100
§5 и ADR-101 §8 оставили открытым (§12).

Все числа ниже — синтетический замер на временном PostgreSQL 18.3
(scratchpad, одна машина, `shared_buffers` 128 МБ, без конкурентной
нагрузки), а не производительность Bridge в Mattermost. Запросы Bridge
идут через RPC-драйвер Mattermost; его накладные расходы не измерены.

---

## 1. Факты, на которых стоит спецификация

Mattermost 9.11 (исходники) и замеры исследований 2026-09-30 — 2026-10-01:

| # | факт | источник |
|---|---|---|
| F1 | `Id` поста назначает только Mattermost: `SaveMultiple` отвергает заданный `Id` у не-удалённого поста | `sqlstore/post_store.go:148`, `model.Post.PreSave` |
| F2 | явный `CreateAt` поста сохраняется как передан (прошлое, будущее, равные значения); `UpdateAt = CreateAt`; `0` → время узла Mattermost. На пути `CreatePost` плагина `CreateAt` присваивается только при нуле (`app/post.go:297–299`, `model.Post.PreSave` `:553–557`); `IsValid` проверяет лишь `CreateAt ≠ 0` (`:405`), границ нет | код; эксперимент на стенде 2026-09-30 (плагин `dev.photon.createat-experiment`: `base`, `base±1`, равные, `+120 с`, `0`) |
| F3 | тип поста вне встроенных допустим только с префиксом `custom_` | `model/post.go:469` |
| F4 | плагинный `CreatePost` пишет и в архивный канал | эксперимент на стенде |
| F5 | `CreateChannel`: COUNT по лимиту команды выполняется **раньше** вставки; конфликт `(Name, TeamId)` возвращает существующий канал с ошибкой | `channel_store.go:666–684` |
| F6 | `CreateTeam` не атомарен (строка команды раньше каналов по умолчанию, 2 открытых канала занимают лимит); конфликт имени не возвращает команду; `GetTeamByName` находит и архивную; восстановить команду или канал плагин не может | `teams/teams.go:12`, `team_store.go:239,365`, `plugin/api.go` |
| F7 | `UpdatePost` не меняет `ChannelId` и `CreateAt` — пост нельзя перенести | `app/post.go:673–683` |
| F8 | транзакции и `pg_try_advisory_xact_lock` через `*sql.DB` из `GetMasterDB()` работают | `dm_sql.go` `WithSendLock`, живой E2E 217 |
| F9 | индекс `Posts (channelid, deleteat, createat)` есть; `id` в нём нет | схема стенда |
| F10 | хук `MessageWillBePosted` **любого** установленного плагина может вернуть заменяющий пост целиком, включая `CreateAt` (`app/post.go:271–283`). Bridge этот хук не реализует; эксперимент F2 прошёл с установленными `calls`, `nps`, `playbooks` — `CreateAt` сохранился | код; стенд |
| F11 | семантика advisory xact lock (PostgreSQL 18.3, проверено, §5.1): снимается при `COMMIT`, `ROLLBACK`, **ошибке любого оператора транзакции** (сразу, до `ROLLBACK`), завершении серверного процесса, обрыве клиента в простое; при обрыве клиента посреди выполняющегося оператора держится до его конца | замер §5.1 |

---

## 2. Схема

Префикс `engram_` (ADR-100 §2.4). Версия схемы — в `engram_schema`.

```sql
CREATE TABLE engram_schema (version int NOT NULL);

-- Служебные команды Mattermost для носителей (ADR-097 §2.4).
CREATE TABLE engram_service_team (
  team_name  text PRIMARY KEY,          -- резервируется ДО CreateTeam
  team_id    text UNIQUE,               -- NULL, пока state = 'reserved'
  state      text NOT NULL CHECK (state IN ('reserved','created','full')),
  carriers   int  NOT NULL DEFAULT 0,   -- резервы и привязки в этой команде
  created_at bigint NOT NULL
);

-- Разговор + привязка носителя (1:1, живут одинаково долго).
CREATE TABLE engram_conversation (
  conversation_id    text PRIMARY KEY,  -- выдаёт Bridge, непрозрачный
  kind               text NOT NULL CHECK (kind IN ('group')),
  state              text NOT NULL CHECK (state IN ('creating','active','archived')),
  title              text NOT NULL,
  description        text NOT NULL DEFAULT '',
  history_policy     text NOT NULL CHECK (history_policy IN ('full','since_join')),
  version            bigint NOT NULL,   -- conversation_version; 0 пока creating
  created_at         bigint NOT NULL,
  created_by         text NOT NULL,     -- PhotonID
  member_count       int NOT NULL,      -- участники в периоде active
  last_activity_at   bigint NOT NULL,   -- CreateAt последнего элемента ленты (902)
  position_h         bigint NOT NULL DEFAULT 0,  -- H: последняя выданная позиция ленты (ADR-101 §5.1); не уменьшается
  carrier_state      text NOT NULL CHECK (carrier_state IN ('reserved','bound')),
  carrier_team       text NOT NULL REFERENCES engram_service_team(team_name),
  carrier_name       text NOT NULL UNIQUE,  -- неизменно; 'c-' || conversation_id
  carrier_channel_id text UNIQUE,           -- ставится один раз
  reserved_at        bigint NOT NULL,
  CHECK ((carrier_state = 'bound') = (carrier_channel_id IS NOT NULL))
);
CREATE INDEX engram_conversation_creating
  ON engram_conversation (reserved_at) WHERE state = 'creating';

-- Периоды участия (ADR-098 §4.2). Роль — у текущего периода.
CREATE TABLE engram_member_period (
  conversation_id text NOT NULL REFERENCES engram_conversation,
  photon_id       text NOT NULL,
  period_no       int  NOT NULL,        -- 1, 2, … на пару (разговор, участник)
  role            text NOT NULL CHECK (role IN ('owner','admin','member')),
  state           text NOT NULL CHECK (state IN ('active','left','removed')),
  opened_version  bigint NOT NULL,
  closed_version  bigint,
  open_at         bigint,  open_post_id  text,   -- позиция открывающего элемента (§4)
  close_at        bigint,  close_post_id text,   -- позиция закрывающего элемента
  PRIMARY KEY (conversation_id, photon_id, period_no)
);
CREATE UNIQUE INDEX engram_member_one_active
  ON engram_member_period (conversation_id, photon_id) WHERE state = 'active';
CREATE INDEX engram_member_by_user
  ON engram_member_period (photon_id, conversation_id) WHERE state = 'active';

-- Ключи идемпотентности изменяющих операций (ADR-099 §10.2).
CREATE TABLE engram_request (
  photon_id       text   NOT NULL,
  action          int    NOT NULL,
  request_id      uuid   NOT NULL,
  payload_hash    bytea  NOT NULL,       -- sha256 канонического содержания
  conversation_id text,
  state           text   NOT NULL CHECK (state IN ('pending','done')),
  response        jsonb,                 -- data первого исполнения
  created_at      bigint NOT NULL,
  PRIMARY KEY (photon_id, action, request_id)
);
CREATE INDEX engram_request_age ON engram_request (created_at);   -- для срока хранения (§12)

-- События ленты: производные от состояния (ADR-098 §4.4, §7.4).
CREATE TABLE engram_event (
  conversation_id text   NOT NULL REFERENCES engram_conversation,
  version         bigint NOT NULL,       -- conversation_version изменения
  seq             int    NOT NULL,       -- порядок событий одного изменения
  event           jsonb  NOT NULL,       -- поля Item ADR-099 §9.3
  at              bigint NOT NULL,       -- CreateAt поста-события = позиция его резерва (§4)
  state           text   NOT NULL CHECK (state IN ('pending','written')),
  post_id         text,
  PRIMARY KEY (conversation_id, version, seq)
);
CREATE INDEX engram_event_pending ON engram_event (at) WHERE state = 'pending';

-- Резервы позиций ленты (ADR-101 §5.1a, §5.10): строка на каждую выданную
-- позицию — и события, и сообщения. Текста элемента нет (ADR-100 §2.2).
CREATE TABLE engram_position (
  conversation_id text   NOT NULL REFERENCES engram_conversation,
  pos             bigint NOT NULL,       -- позиция = CreateAt поста (ADR-101 §5.1)
  item_kind       text   NOT NULL CHECK (item_kind IN ('message','event')),
  item_key        text   NOT NULL,       -- сообщение: Props.engram_client_msg_id;
                                         -- событие: conversation_id:version:seq (E2)
  state           text   NOT NULL CHECK (state IN ('reserved','published','aborted')),
  sender          text,                  -- PhotonID отправителя сообщения — владелец ключа (ADR-102); NULL у события
  payload_hash    bytea,                 -- sha256 текста сообщения: повтор с другим
                                         -- текстом — idempotency_conflict (ADR-099 §9.1)
  reserved_at     bigint NOT NULL,       -- время резерва: возраст для сверки (§8)
  post_id         text,                  -- ставится при переходе в published
  PRIMARY KEY (conversation_id, pos)
);
-- не более одного неотменённого резерва на ключ (ADR-101 §5.6)
CREATE UNIQUE INDEX engram_position_key_live
  ON engram_position (conversation_id, item_key) WHERE state <> 'aborted';
-- барьер: наименьший открытый резерв разговора (ADR-101 §5.1b)
CREATE INDEX engram_position_open
  ON engram_position (conversation_id, pos) WHERE state = 'reserved';
-- сверка брошенных резервов (§8)
CREATE INDEX engram_position_open_age
  ON engram_position (reserved_at) WHERE state = 'reserved';
-- владелец ключа во всех состояниях, включая надгробия (ADR-102 §2;
-- implementation contract §2.2): key_live надгробий не покрывает
CREATE INDEX engram_position_key_all
  ON engram_position (conversation_id, item_key);
```

Тексты сообщений (930) в таблицы Bridge не пишутся: это посты носителя
(ADR-099 §13). Bridge хранит по сообщению только резерв его позиции —
строку `engram_position` без текста (ADR-101 §7 п. 7). Прежняя редакция
утверждала «сообщения в таблицы Bridge не пишутся» без оговорки; для
механизма B это неверно (устарело — ADR-101).

---

## 3. Инварианты хранения

- **S1.** Состояние Bridge меняется только транзакцией; каждое изменение
  метаданных, участия, ролей или состояния увеличивает `version` ровно на 1
  в той же транзакции и пишет свои события `pending` с этой версией.
- **S2.** У пары (разговор, участник) не больше одного периода `active`
  (`engram_member_one_active`).
- **S3.** `carrier_name` задаётся при резерве и не меняется; `carrier_team`
  меняется только при `carrier_state = 'reserved'`; `carrier_channel_id`
  ставится один раз переходом `reserved → bound` и больше не меняется.
- **S4.** Ни одна строка не хранит идентификаторов Mattermost в виде,
  доступном ответам 900–931 (ADR-099 §3): ответы строятся только из
  `conversation_id`, PhotonID и полей ленты.
- **S5.** Разговор в `creating` не видит никто: 901/902/931 его не
  выдают, изменяющие операции отвечают `conversation_not_found`.
- **S6.** Архив ENGRAM — только `state = 'archived'`. Носитель и служебная
  команда в Mattermost не архивируются никогда; периоды при архивации не
  закрываются (ADR-099 §8.1).

Позиции ленты (ADR-101 §5.1, §5.1a, §5.10):

- **S7.** `position_h` (`H`) только растёт: каждая выдача позиции —
  `UPDATE engram_conversation SET position_h = GREATEST(часы Bridge,
  position_h + 1)` в транзакции, которая в ней же пишет резерв этой
  позиции. Удаление поста `H` не уменьшает.
- **S8.** Блокировка строки разговора, взятая этим `UPDATE`, держится **до
  фиксации** транзакции резерва; поэтому резервы разговора фиксируются в
  порядке позиций, и любой снимок видит выданные позиции префиксом.
- **S9.** Последовательности PostgreSQL (`nextval`) и выдача позиций блоками
  для позиций ленты **запрещены** (ADR-101 §5.1).
- **S10.** Позиция выдаётся один раз: `PRIMARY KEY (conversation_id, pos)`;
  пост носителя группы не создаётся ни без предварительного резерва его
  позиции, ни с `CreateAt = 0`.
- **S11.** Переходы резерва — только `reserved → published` и `reserved →
  aborted`, условные (`WHERE state = 'reserved'`); `published` и `aborted`
  конечны. Надгробие (`aborted`) не переиспользуется и хранится постоянно;
  его позиция не выдаётся повторно.
- **S12.** Опубликованная позиция не переназначается: ни повтор 930, ни
  дописывание события, ни сверка не меняют позицию записанного элемента и
  не пересоздают пост ради порядка (ADR-101 I6).
- **S13.** События не отменяются: резерв события закрывается только
  `published` — когда его пост записан (E3–E4).

---

## 4. Позиция событий в ленте

Три принятых требования определяют механизм однозначно:

- состояние Bridge — источник истины, событие — производное; при сбое записи
  события состояние остаётся, событие дописывается идемпотентно (ADR-098
  §4.4, §7.4);
- лента показывает изменения в той последовательности, в какой они
  произошли (ADR-098 §7.2);
- границы периодов `since_join` — позиции открывающего и закрывающего
  событий в порядке `(created_at, id)` (ADR-099 §9.4).

Если бы `CreateAt` события назначал Mattermost в момент записи, отложенная
запись (после сбоя) поставила бы событие **после** сообщений, написанных
уже после изменения: сообщение нового участника оказалось бы раньше его
`member_added` и выпало бы из его собственного периода. Это нарушает §7.2 и
§9.4. Поэтому:

- **E1.** Позиция события назначается Bridge в транзакции изменения из
  счётчика `H` (ADR-101 §5.1, §5.2): изменение с `k` событиями получает
  `P, P+1, …, P+k−1`, где `P = max(часы Bridge, position_h + 1)`, затем
  `position_h := P+k−1`; в той же транзакции пишутся резервы этих позиций
  (`engram_position`, `reserved`, S7–S8) и `engram_event.at`. События
  одного изменения получают разные миллисекунды, поэтому `created` — самый
  ранний элемент ленты (ADR-099 §4) без опоры на случайный порядок `id`.
  *Прежняя редакция E1 («`at = T + seq`, не меньше `at` последнего события
  + 1») сравнивала позицию только с событиями, а не с сообщениями —
  устарело, ADR-101 §5.9.*
- **E2.** Пост-событие пишется `CreatePost` с явным `CreateAt = at`,
  `Type = 'custom_engram_event'` (F3), `UserId` — аккаунт Mattermost
  `actor` (у каждого события ADR-099 §9.3 он есть; служебный пользователь
  не нужен), `Props.engram_event` — поля Item и ключ
  `conversation_id:version:seq`.
- **E3.** Запись идемпотентна: перед `CreatePost` — точечный поиск
  `channelid = носитель AND deleteat = 0 AND createat = at AND
  props->>'engram_event_key' = ключ` (индекс F9, равенство по `createat`).
  Найден — берётся найденный `post_id`.
- **E4.** После записи — транзакция: событие `written` + `post_id`; резерв
  его позиции `reserved → published` (условно, S11); у периода, который
  событие открывает или закрывает, — `open_*` / `close_*`;
  `last_activity_at = GREATEST(…, at)`.
- **E5 (устарело — ADR-101 §5.1a, §5.1b).** *Прежняя редакция: «ни одна
  операция не читает и не пишет ленту разговора, пока у него есть события
  `pending`: сначала они дописываются».* Заменено: записи в носитель не ждут
  дописывания событий, оставшихся незаписанными после сбоя, а 931 их не
  дописывает. Незаписанное событие — открытый резерв; барьер видимости
  (§6.4) не пускает клиента дальше его позиции, пока сверка (§8) не
  допишет пост с сохранённой позицией. Окно W для ленты группы — 0 (ADR-101
  §5.5).
- **E6 (устарело — ADR-101 §5).** *Прежняя редакция: «исключительная
  блокировка снимается только когда время Bridge стало больше `at`
  последнего записанного события: следующее сообщение (время Mattermost)
  окажется позже».* Заменено: сообщение получает позицию из того же `H`
  (§6.3), порядок «событие ↔ сообщение» от часов не зависит, и ожидания
  часов нет. Когда держится X — §5.

**Порядок теперь логический целиком.** Позиции событий и сообщений
выдаются одним счётчиком `H`; часы — только источник «примерно сейчас»
(ADR-101 §5.4: при отставании часов позиция опережает физическое время).
*Прежний абзац этого места объяснял, что порядок «событие ↔ сообщение»
держится на часах узла Mattermost (E1/E6), и называл это проблемой AP-1
(§13); проблема закрыта ADR-101.*

---

## 5. Блокировки

| блокировка | ключ | кто берёт | что защищает |
|---|---|---|---|
| **X(разговор)** — исключительная | advisory, `sha256("photon:conv:" + conversation_id)` | 900 (шаги 2–5: носитель, привязка, финализация, события), 903–921; дописывание `pending` сверкой | изменение состояния, версии, позиций и резервов событий — до фиксации транзакции изменения (ADR-101 §5.2, §5.3); запись постов событий под X сужает окно дубля (ADR-101 §8 / Q8) |
| **S(разговор)** — разделяемая | тот же ключ, `…_shared` | 930, 931 | 930: проверка доступа и права отправителя согласована с резервом позиции — S держится как минимум до фиксации транзакции резерва (ADR-101 §5.3); 931: проверка доступа и периоды вызывающего — из того же состояния, что страница |
| строка `engram_conversation … FOR UPDATE` | — | транзакция изменения; `UPDATE position_h` (S8) | последовательные версии (ADR-099 §10.4); порядок фиксации резервов (S8) |
| отправка | `sha256("photon:conv-send:" + sender + ":" + client_msg_id)` | 930 | повтор одного `client_msg_id` (как 217, ADR-092 §2.5.1) |
| миграции | фиксированный ключ `photon:schema` | активация | одна миграция при нескольких узлах |

- Блокировки берутся **только попыткой с ожиданием по бюджету**
  (`pg_try_advisory_xact_lock*` с опросом, как `WithSendLock`); не
  получена — `temporarily_unavailable`. Бесконечного ожидания нет.
- X держит отдельная «держащая» транзакция на весь цикл: транзакция
  изменения (свой коннект) → `CreatePost` событий → транзакция E4. Порядок
  взятия всегда X/S → строка разговора, поэтому цикла ожидания нет.
  Для порядка и видимости X нужна только до фиксации транзакции изменения
  (позиции уже в `H`, незаписанное событие держит барьер); держать её до
  записи постов событий — текущее правило записи событий (писать события
  без X запрещено, ADR-101 §6), но не условие порядка (ADR-101 §5.3).
- **В держащей транзакции не выполняется ничего, кроме взятия блокировки.**
  Любая ошибка оператора в ней снимает блокировку сразу, хотя транзакция
  формально ещё открыта (§5.1, M3). Если на держащем соединении случилась
  ошибка, блокировка считается потерянной: операция прекращается с
  `temporarily_unavailable`, дальнейших `CreatePost` под «блокировкой» нет.
- *Устарело — ADR-101 §5.1a: «930/931 под S проверяют `pending`; если
  есть — отпускают S, берут X, дописывают, повторяют».* 930 и 931 события
  не дописывают и `pending` не проверяют; незаписанные события дописывает
  сверка (§8) под X, видимость держит барьер (§6.4).
- X берётся также на шаги 2–5 создания (§6.1): без неё возможна сирота
  (§5.2).

### 5.1. Семантика advisory xact lock — проверено

Временный PostgreSQL 18.3; держатель и претендент — разные соединения.

| # | сценарий | результат |
|---|---|---|
| M1 | `COMMIT` держателя | снимается |
| M2 | `ROLLBACK` держателя | снимается |
| M3 | ошибка оператора в транзакции держателя (`SELECT 1/0`, с параметрами и без) | **снимается сразу**: состояние `idle in transaction (aborted)`, в `pg_locks` блокировки нет, претендент её получает. Ожидание исследования («держится до `ROLLBACK`») опровергнуто замером |
| M4 | `pg_terminate_backend` держателя | снимается сразу |
| M5 | клиент убит `SIGKILL`, простаивая в транзакции | снимается через ~1 мс |
| M6 | клиент убит посреди выполняющегося оператора (`pg_sleep(8)`) | держится до конца оператора: 7.7 с (`client_connection_check_interval = 0`) |
| M7 | `SET LOCAL idle_in_transaction_session_timeout = '1s'`, держатель завис | сервер завершает сессию через 1.0 с, блокировка снимается, держатель получает ошибку |
| M8 | `…_shared` двумя сессиями; exclusive при shared; shared при exclusive | S+S совместимы; X при S и S при X не берутся |

**Чего замер не доказывает.** Клиент PostgreSQL для Bridge — сервер
Mattermost (RPC-драйвер), а не процесс плагина. Падение плагина — не обрыв
соединения с базой: снимется ли блокировка, зависит от того, закрывает ли
Mattermost серверную сторону коннекта (§12, Q4). M7 — возможная страховка,
но она разрывает соединение из пула Mattermost; её последствия для
RPC-драйвера не проверены.

**Ответ исследования AP-1** (локальный Mattermost 9.11.0, R7 в
`ap1-ordering-research.md` §2; ADR-101 §5.11): транзакционная блокировка
через `pluginapi` Store после падения процесса плагина снимается через
15–60 с; сессионная (`pg_advisory_lock`) не снимается вовсе — соединение
возвращается в пул Mattermost. Поэтому Bridge использует только
транзакционные блокировки.

### 5.2. Почему два разных канала не становятся носителем одного разговора (C3d)

Гарантия базы — на **привязку**, и она проверена:

| механизм | что запрещает | проверка |
|---|---|---|
| условный `UPDATE … SET carrier_state='bound', carrier_channel_id=$ch WHERE conversation_id=$c AND carrier_state='reserved'` под блокировкой строки | вторую привязку того же резерва: вторая транзакция ждёт строку и после фиксации первой перепроверяет `WHERE` (READ COMMITTED) — 0 строк | K1 |
| `UNIQUE (carrier_channel_id)` | один канал — носитель двух разговоров | K2 |
| `CHECK ((carrier_state='bound') = (carrier_channel_id IS NOT NULL))` + условие `carrier_state='reserved'` | снятие или замену канала у привязанного | K3 |
| `UNIQUE (carrier_name)` + уникальность `(Name, TeamId)` в Mattermost | два канала с одним именем в одной команде | F5 |

**Чего база не гарантирует — дефект прежней редакции.** Созданию второго
канала **в Mattermost** база помешать не может: при смене команды (шаг 2)
параллельный исполнитель, прочитавший старую команду до переезда, создаёт
канал в ней, если там освободилось место (внешняя архивация, Q3).
Воспроизведено (K4): два канала с именем носителя, привязан один, второй —
сирота; прежняя редакция называла такое расхождение «невозможным при S3».
Исправление — шаги 2–5 создания под X(разговор): второй исполнитель ждёт и
видит `bound` (K5: один канал). Без внешнего вмешательства (место в
заполненной команде не освобождается) гонка сходится и без X: повтор
получает ошибку лимита, не находит канал и не может переехать из уже
сменённой команды.

---

## 6. Операции по шагам

Порядок проверок каждого обработчика — ADR-099 §11: повтор по §10.2 →
существование и участие → состояние → право → содержание.

### 6.1. 900 Create

1. **T1 (резерв):** `INSERT engram_request … state='pending' ON CONFLICT DO
   NOTHING`. Вставлено — выдать `conversation_id`; выбрать команду
   (`engram_service_team`, `state='created' AND carriers < лимит`, `FOR
   UPDATE`), `carriers + 1`; вставить разговор `creating` с резервом
   `(carrier_team, carrier_name)`; записать `conversation_id` в ключ. Не
   вставлено (ключ есть) — сравнить `payload_hash` (иначе
   `idempotency_conflict`); `done` → вернуть `response`; `pending` →
   продолжить с сохранённым `conversation_id` с шага 2.
2. **Носитель — под X(разговор) до шага 5 включительно** (§5.2).
   `CreateChannel(carrier_team, carrier_name)` (приватный,
   `CreatorId` пуст). Успех или конфликт имени → привязка (шаг 3). Ошибка
   лимита → **сначала** `GetChannelByName(team, name, includeDeleted=true)`
   (F5: лимит проверяется раньше уникальности): найден — привязка; нет —
   команда `full`, резерв переезжает в другую команду (только при
   `carrier_state='reserved'`), повтор шага 2.
3. **Привязка:** `UPDATE … SET carrier_state='bound', carrier_channel_id=$ch
   WHERE conversation_id=$c AND carrier_state='reserved'`. 0 строк —
   перечитать: привязан тот же канал — продолжить; другой — возможен
   только при внешнем вмешательстве (под X гонка исключена, K5):
   `temporarily_unavailable` и запись в журнал; найденный непривязанный
   канал — сирота (§8).
4. **T2 (X уже взята):** разговор `FOR UPDATE`; если ещё `creating` — периоды
   `active` (создатель `owner`, остальные `member`), `version = 1`,
   `member_count`, `state='active'`, события `created` и `member_added`
   (`pending`) с позициями из `H` и их резервами (E1).
5. **События** — E2–E4, затем снять X. *(Прежняя ссылка на E6 — снимать X
   только после того, как часы прошли `at`, — устарела, ADR-101.)*
6. **T3:** ключ `done`, `response`. Ответ — только после шага 5
   (ADR-099 §4: «разговор, участие и начальные события записаны»).

Повтор с тем же `request_id` на любом шаге продолжает с первого
незавершённого; параллельные повторы сходятся (тест C1).

### 6.2. Изменения 903, 904, 910, 911, 912, 920, 921

Под X, одной транзакцией: ключ идемпотентности (`ON CONFLICT DO NOTHING`;
есть и `done` — вернуть `response`), разговор `FOR UPDATE`, проверки §11,
`expected_version` (ADR-099 §10.3), изменение периодов/ролей/метаданных,
`version + 1`, события `pending` с позициями из `H` и их резервами (E1),
ключ `done` с `response`. Затем E2–E4 и снятие X. Ответ — после записи
событий.

«Нет изменения» (`already`, `changed:false`) — без события и без роста
версии, но ключ записывается (повтор отвечает тем же).

### 6.3. 930 Send

По ADR-101 §5.1a, §5.3, §5.6, §5.10 и ADR-102; блокировка отправки
ADR-092 §2.5.1 (§5) сохраняется.

1. **Под S:** доступ, состояние, право — из таблиц Bridge. S держится как
   минимум до фиксации транзакции резерва (шаг 3).
2. **Проверка отправителя, затем поиск.**
   - **Сначала (ADR-102 §2):** есть ли у разговора резерв сообщения с
     `item_key = client_msg_id` **другого** `sender` — в любом состоянии:
     `reserved`, `published` или `aborted` (надгробие). Если есть — это
     **не** повтор: ответ `idempotency_conflict` (ADR-099 §12), новая
     позиция не резервируется, `CreatePost` не вызывается, чужой резерв или
     надгробие не изменяется (ни переходов, ни сверки в ходе этого
     запроса). Поиск — по `engram_position_key_all` (§2): индекс
     `engram_position_key_live` надгробий не покрывает. План не замерен
     (как L3/L4, §10).
   - Иначе — **поиск** среди резервов разговора с `item_key =
     client_msg_id` **не в состоянии `aborted`** (`engram_position_key_live`);
     все они принадлежат этому же `sender` (ADR-102 §2):
     - `published` — тот же текст (`payload_hash`) → ответ с его позицией
       (I6); другой текст → `idempotency_conflict` (ADR-099 §9.1);
     - `reserved` → `message_in_progress`;
     - нет (нет вовсе или есть только свои надгробия) → шаг 3. Пост с тем
       же ключом на позиции с надгробием **не считается найденным**.
   *Прежняя редакция этого шага: «найденный резерв другого `sender` — не
   определено (§12, Q8)» — закрыто ADR-102.*
3. **Резерв** — одна транзакция: `UPDATE engram_conversation SET position_h
   = GREATEST(часы Bridge, position_h + 1) … RETURNING position_h` → `P`;
   `INSERT engram_position (P, 'message', client_msg_id, 'reserved', sender,
   payload_hash, reserved_at)`; `COMMIT` (S8). Нарушение
   `engram_position_key_live` (параллельный повтор или одновременная
   отправка другого `sender` с тем же ключом) → повторить шаг 2: свой
   резерв найдётся поиском, чужой даст `idempotency_conflict` (ADR-102 §2
   п. 4).
4. **Публикация:** `CreatePost` с явным `CreateAt = P` и
   `Props.engram_client_msg_id`. `PendingPostId` не задаётся либо задаётся
   с привязкой к позиции (`client_msg_id` + `P`). `CreateAt` возвращённого
   поста сверяется с `P`; расхождение → резерв `reserved → aborted`, успех
   не сообщается (ADR-101 §5.6: `deduplicateCreatePost` 30 с возвращает
   прежний пост с тем же `PendingPostId` как успех). Однозначная
   подтверждённая ошибка создания (пост не создан) → так же `aborted`.
   **Неопределённый результат** — тайм-аут, потерянный ответ, обрыв RPC,
   смерть исполнителя после отправки `CreatePost` — к `aborted` у владельца
   **не** ведёт: резерв остаётся `reserved` и закрывается сверкой (§8), которая
   сначала ищет пост с `CreateAt = P` и ключом (ADR-101 §5.10; implementation
   contract §3.4, §3.6). *Прежняя редакция: «расхождение или ошибка
   `CreatePost` → `aborted`» — сужено, так как допускала отмену при
   неопределённом исходе.*
5. **Закрытие:** условный переход `reserved → published` с `post_id`. Не
   прошёл (резерв уже отменён сверкой) → успех **не** сообщается, клиент
   повторяет; повтор получает новую позицию (шаг 2 → 3).
6. `last_activity_at = GREATEST(…)` (подсказка сортировки 902; при сбое
   чинится сверкой).

Пост элемента определяется парой `(CreateAt, ключ)`, а не ключом. Клиент
никогда не получает отменённый элемент; сообщение доходит повтором
исходящих Core (ADR-093) — at-least-once на стороне отправителя.
*Прежняя редакция: «`pending` нет (иначе §5)… поиск своего поста по
`Props.engram_client_msg_id` в горизонте идемпотентности, `CreatePost`
(время Mattermost)» — устарело, ADR-101.*

### 6.4. 931 Sync

По ADR-101 §5.1b, §5.5. Под S: доступ; периоды вызывающего; затем **один
оператор SQL через master** (`Store.GetMasterDB`, не реплика), который в
одном снимке:

- вычисляет **барьер** — наименьшую позицию резерва разговора в состоянии
  `reserved` (`engram_position_open`), а если открытых резервов нет —
  `position_h + 1`;
- выбирает страницу `Posts` носителя формой §7.2 (видимая лента: `full` —
  вся, `since_join` — периоды) только с `CreateAt < барьер`;
- исключает посты на позициях с надгробием (`engram_position.state =
  'aborted'`);
- оставляет один пост на позицию (наименьший `PostID`: дубль поста-события
  или поста одной попытки).

Окно W для ленты группы — **0**: механика прохода, курсор и `at_end` —
ADR-092 §2.2 и ADR-099 §9.2 по видимой ленте без окна; курсор ответа не
проходит барьер. 931 события **не дописывает** и `pending` не проверяет.
Барьер и страница разными операторами, разными соединениями или с реплики
не читаются (ADR-101 §5.1b, §5.12 п. 1).
Типы: `''` — сообщение, `custom_engram_event` — событие; прочие
(системные посты Mattermost) не выдаются.
*Прежняя редакция: «под S: `pending` нет; … механика прохода, окно W, курсор
и `at_end` — ADR-092 §2.2» — устарело, ADR-101.*

### 6.5. 901 Get, 902 List

Только таблицы Bridge; блокировки не нужны. 902 — keyset по
`(last_activity_at DESC, conversation_id DESC)`. Ключ изменяемый: разговор,
поднявшийся во время постраничного прохода, в этом проходе не повторяется
и может быть пропущен; он будет первым на следующей первой странице (§12,
Q5).

---

## 7. Запросы и индексы

Объём замера: 110 служебных команд; 200 000 разговоров (97% `active`, 2%
`archived`, 1% `creating`); 2 082 000 периодов (по 10 участников, 5% с
двумя периодами); «тяжёлый» пользователь в 2000 группах; 1 000 000 ключей
идемпотентности; 400 000 событий (1000 `pending`); `Posts` — 2 583 000
строк: носитель на 300 000 элементов (события каждые 500, пары с равным
`CreateAt` каждые 50), 198 000 носителей по 10 постов, канал двух авторов
на 300 000, канал на 3000 постов с группами по 5 равных `CreateAt`.
Индексы `Posts` — btree-индексы Mattermost 9.11 (GIN полнотекстового поиска
на эти планы не влияют и опущены). Время — `EXPLAIN (ANALYZE)` после
одного прогревающего выполнения.

### 7.1. Таблицы Bridge

| запрос | SQL (суть) | индекс | план | время |
|---|---|---|---|---|
| доступ §11 | `… FROM engram_member_period WHERE conversation_id=$1 AND photon_id=$2 ORDER BY period_no DESC LIMIT 1` | PK | Index Scan Backward | 0.03 мс |
| периоды для `since_join` | то же, `ORDER BY period_no` | PK | Index Scan | 0.03 мс |
| 901 снимок | `… FROM engram_conversation WHERE conversation_id=$1` | PK | Index Scan | 0.04 мс |
| 901 состав | `… WHERE conversation_id=$1 AND state='active'` | `engram_member_one_active` | Bitmap | 0.05 мс |
| 902, обычный пользователь | §7.3 | `engram_member_by_user` + PK | Index Scan + Nested Loop | 0.25 мс |
| 902, 2000 групп | §7.3 | те же | Bitmap + LATERAL по PK | 15–17 мс |
| ключ идемпотентности: поиск | `… WHERE photon_id=$1 AND action=$2 AND request_id=$3` | PK | Index Scan | 0.03 мс |
| ключ: вставка | `INSERT … ON CONFLICT DO NOTHING` | PK | — | 0.18 мс |
| изменение: сериализация | `SELECT version, state … WHERE conversation_id=$1 FOR UPDATE` | PK | Index Scan | 0.08 мс |
| привязка по разговору | `SELECT carrier_state, carrier_channel_id … WHERE conversation_id=$1` | PK | Index Scan | 0.05 мс |
| обратный поиск по каналу | `… WHERE carrier_channel_id=$1` | UNIQUE `carrier_channel_id` | Index Scan | 0.03 мс |
| сверка созданий | `… WHERE state='creating' AND reserved_at < $1 ORDER BY reserved_at LIMIT 100` | `engram_conversation_creating` | Index Scan | 0.17 мс |
| незаписанные события, все | `… WHERE state='pending' ORDER BY at LIMIT 100` | `engram_event_pending` | Index Scan | 0.13 мс |
| незаписанные события разговора | `… WHERE conversation_id=$1 AND state='pending' ORDER BY version, seq` | PK | Index Scan | 0.03 мс |
| выбор служебной команды | `… WHERE state='created' AND carriers < $1 ORDER BY team_name LIMIT 1 FOR UPDATE` (замер — с `SKIP LOCKED`, план тот же) | PK | Index Scan | 0.10 мс |
| выдача позиции (930, изменение) | `UPDATE engram_conversation SET position_h = GREATEST($t, position_h + 1) WHERE conversation_id=$1 RETURNING position_h` + `INSERT engram_position` | PK | — | не замерено здесь; потолок сериализации — ADR-101 §5.8 |
| барьер (внутри оператора 931) | `min(pos) … WHERE conversation_id=$1 AND state='reserved'`, иначе `position_h + 1` | `engram_position_open` | — | не замерено здесь |
| повтор 930 | `… WHERE conversation_id=$1 AND item_key=$2 AND state <> 'aborted'` | `engram_position_key_live` | — | не замерено здесь |
| владелец ключа 930 (ADR-102) | `… WHERE conversation_id=$1 AND item_kind='message' AND item_key=$2 AND sender <> $3 LIMIT 1` | `engram_position_key_all` | — | не замерено здесь (contract O1) |
| брошенные резервы (сверка) | `… WHERE state='reserved' AND reserved_at < $1 ORDER BY reserved_at LIMIT 100` | `engram_position_open_age` | — | не замерено здесь |

### 7.2. Лента 931: форма «диапазон `CreateAt` + группа равных»

**Под ADR-101 (действует).** Страница 931 читается **одним оператором** вместе
с барьером (§6.4): барьер — подзапрос того же оператора, верхняя граница
диапазона — `min(барьер, закрытие периода)`, посты на позициях с
надгробием отфильтрованы, один пост на позицию:

```sql
WITH b AS (
  SELECT COALESCE(
    (SELECT min(pos) FROM engram_position WHERE conversation_id = $c AND state = 'reserved'),
    (SELECT position_h + 1 FROM engram_conversation WHERE conversation_id = $c)) AS bar)
SELECT DISTINCT ON (p.createat) p.id, p.createat, …
FROM b, posts p
WHERE p.channelid = $1 AND p.deleteat = 0 AND p.type IN ('', 'custom_engram_event')
  AND p.createat > $2 AND p.createat < LEAST(b.bar, $3)      -- $3 = закрытие периода или +∞
  AND NOT EXISTS (SELECT 1 FROM engram_position r
                  WHERE r.conversation_id = $c AND r.pos = p.createat AND r.state = 'aborted')
ORDER BY p.createat ASC, p.id ASC
LIMIT $4;
-- оператор возвращает и b.bar: курсор ответа не проходит барьер
-- первичная синхронизация — тем же оператором, зеркально:
--   p.createat >= $2 (открытие периода) AND p.createat < LEAST(b.bar, $3)
--   ORDER BY p.createat DESC, p.id ASC
```

Позиции элементов разговора уникальны (S10): равный `CreateAt` у постов
ленты бывает только у повторов одного элемента (дубль поста-события,
дубль одной попытки отправки), и они схлопываются в один пост. Дочитывание
«группы равных» отдельным запросом (1) поэтому не нужно, а отдельные
операторы для страницы и барьера запрещены (ADR-101 §5.1b). `since_join`
по нескольким периодам — тоже внутри одного оператора. **План и время
этой формы на объёме §7 не замерены**: замеры и проверка L1/L2 ниже
выполнены для прежней формы из трёх запросов (acceptance L3–L4, §10).
Корректность барьера одним оператором проверена на стенде AP-1 (ADR-101
§5.12 п. 1).

*Ниже — прежняя форма (до ADR-101) и её замеры: остаются основанием выбора
условий индекса; для ленты группы три отдельных запроса заменены одним
оператором выше.*

Индекс `Posts (channelid, deleteat, createat)` не содержит `id`, а порядок
ленты — `(createat ASC, id DESC)`. Предикат «строго новее позиции» в
точной форме — `createat > $t OR (createat = $t AND id < $x)` — планировщик
**не превращает в условие индекса** и читает канал с начала (§7.4). Индекс
в `Posts` добавить нельзя (ADR-100 §2.3). Поэтому лента читается тремя
простыми запросами, каждый из которых целиком — условие индекса, а порядок
`id` внутри равного `CreateAt` досортировывается в Bridge:

```sql
-- (1) группа равного CreateAt на границе (позиции курсора, открытия или закрытия периода)
SELECT p.id, p.createat, … FROM posts p
WHERE p.channelid = $1 AND p.deleteat = 0 AND p.type IN ('', 'custom_engram_event')
  AND p.createat = $2;

-- (2) продолжение: следующий диапазон по возрастанию
… AND p.createat > $2 AND p.createat <= $3        -- $3 = закрытие периода или +∞
ORDER BY p.createat ASC LIMIT $4;

-- (3) первичная синхронизация: последние по убыванию
… AND p.createat >= $2 AND p.createat < $3        -- $2 = открытие периода
ORDER BY p.createat DESC LIMIT $4;
```

Алгоритм страницы (для каждого видимого периода по порядку; `full` — один
период от `created` без закрытия):

1. начало — более новая из позиции курсора и открывающего элемента;
   группа (1) на `CreateAt` начала, отфильтрованная точными предикатами
   ADR-052 и границами периода;
2. диапазон (2) с `LIMIT` на недостающее; если строк ровно `LIMIT` —
   последняя группа равного `CreateAt` дочитывается целиком запросом (1)
   (иначе порядок `id` в ней неполон);
3. фильтр по границам периода, сортировка `(createat ASC, id DESC)`,
   повтор с последнего `CreateAt`, пока страница не заполнена или период
   не исчерпан; затем следующий период.

Первичная синхронизация — зеркально, запросом (3), от последнего периода.

| составляющая | план (литерально и после 6 `EXECUTE` — одинаково) | время |
|---|---|---|
| (1) группа равных | Index Scan `idx_posts_create_at` (равенство) | 0.03–0.06 мс |
| (2) продолжение, `LIMIT 60` | Index Scan `idx_posts_channel_id_delete_at_create_at`, без сортировки | 0.08 мс |
| (2) из разрыва к открытию периода | то же | 0.08 мс |
| (3) первичная, `LIMIT 60` | Index Scan Backward, без сортировки | 0.10 мс |

**Корректность** (перебор по правилам ADR-052 и ADR-099 §9.4, канал с
группами по 5 равных `CreateAt`, три набора периодов, 89 позиций, лимиты
1–7, 11, 60): **2403 сравнения, 0 расхождений**. Отрицательный контроль —
без дочитывания последней группы: **1300 расхождений**. Первый вариант
проверки (пары равных раз в 50 постов, 57 сравнений) такой ошибки **не
ловил** — контроль давал 0 расхождений; поэтому проверка обязана
использовать данные, где граница страницы попадает внутрь группы.

### 7.3. 902 List

```sql
SELECT c.conversation_id, c.title, c.state, c.version, c.member_count, m.role, c.last_activity_at
FROM engram_member_period m
CROSS JOIN LATERAL (SELECT * FROM engram_conversation c
                    WHERE c.conversation_id = m.conversation_id LIMIT 1) c
WHERE m.photon_id = $1 AND m.state = 'active' AND c.state IN ('active','archived')
  AND (c.last_activity_at, c.conversation_id) < ($2, $3)      -- курсор, если есть
ORDER BY c.last_activity_at DESC, c.conversation_id DESC LIMIT $4;
```

`LATERAL … LIMIT 1` закрепляет форму плана «участия пользователя → поиск
разговора по PK»: цена растёт с числом групп пользователя, а не с числом
разговоров. Обычное `JOIN` в одном из прогонов на странице по курсору
перешло на Hash Join с полным проходом `engram_conversation` (200 000
строк, 82 мс). Измерено: 0.25 мс (10 групп), 15–17 мс (2000 групп).

### 7.4. Отвергнутые формы ленты

| форма | где | время |
|---|---|---|
| точная OR-форма «новее позиции» без границы (как `After` в `dm_sql.go`) | носитель 300 000, позиция 150 000 | 59 мс |
| та же + избыточная `createat >= $t` — литерально | то же | 0.15 мс |
| `since_join` одной OR-формой по двум периодам, из разрыва | то же | 98 мс |
| `UNION ALL` по периодам с точными предикатами — литерально | то же | 0.13–0.22 мс; корректна (2403/0, контроль с границей `<` вместо `<=` — 452 расхождения) |

`UNION ALL` корректна, но зависит от выбора плана: с двумя отдельными
нижними границами `createat` тот же запрос ушёл в Bitmap Heap Scan со
сортировкой 100 000 строк (84 мс); подготовленная форма этого варианта в
замере проверена некорректно (в тест передавалась позиция, а не
`max(открытие, позиция)`) — и потому не выбрана. Форма §7.2 проверена в
обоих режимах.

### 7.5. 930: поиск своего поста

**Под ADR-101 (действует).** Повтор 930 ищет резерв, а не пост: `…
FROM engram_position WHERE conversation_id=$1 AND item_key=$2 AND state <>
'aborted'` по `engram_position_key_live` (§6.3 шаг 2). Пост элемента
определяется парой `(CreateAt = pos, ключ)`; сверка (§8) находит его
точечным поиском `channelid = носитель AND deleteat = 0 AND createat = pos
AND props->>'engram_client_msg_id' = ключ` (индекс F9, равенство по
`createat`, как E3). Пост с тем же ключом на позиции с надгробием найденным
не считается.

*Прежняя редакция (до ADR-101) — механика 217:* `… WHERE channelid=$1 AND
userid=$2 AND createat >= now − 30 дней AND props->>'engram_client_msg_id'
= $3`. Горизонт 30 дней на синтетике покрывает весь носитель: 6–29 мс
(Bitmap по `idx_posts_user_id`, цена — посты автора за горизонт). Индекса
по `Props` нет и добавить его нельзя. Для 930 групп этот поиск заменён
поиском резерва; он находил бы и пост на позиции с надгробием (ADR-101
§5.6). См. §12, Q7.

---

## 8. Сверка и восстановление

Что оставляет сбой и чем находится:

| состояние после сбоя | обнаружение | сходится |
|---|---|---|
| ключ `pending`, разговор `creating`, канала нет / канал есть без привязки | `engram_conversation_creating` (`reserved_at < T`) | повтором 900 с тем же `request_id` (§6.1) |
| изменение зафиксировано, событие `pending` (резерв `reserved`) | `engram_event_pending` | дописыванием сверкой под X с сохранённой позицией (E2–E4); до этого барьер не пускает клиентов дальше позиции события (§6.4). *Прежнее «любой следующей операцией над разговором (E5)» — устарело, ADR-101* |
| событие записано в носитель, `written` не отмечено | E3: точечный поиск по `createat = at` и ключу | тем же дописыванием; резерв → `published` |
| резерв сообщения `reserved`, владелец не подтвердил публикацию (умер, потерял блокировку) | `engram_position_open_age` | одним условным оператором (ADR-101 §5.10): пост с `CreateAt = pos` и ключом найден → `published`; иначе → `aborted` (надгробие) |
| пост долетел после отмены (R1: `CreatePost` доводится сервером после смерти плагина) | — | не выдаётся никогда: позиция с надгробием (§6.4); судьба самого поста в носителе — ADR-101 §8 / Q8 |
| `CreatePost` успешен, переход `→ published` у владельца не прошёл | — | владелец успеха не сообщает; клиент повторяет и получает новую позицию (§6.3) |
| `last_activity_at` отстал | сравнение с `max(createat)` носителя | обновлением |
| в Mattermost есть канал с именем носителя, не привязанный (внешнее вмешательство, §5.2) | `GetChannelByName` по `carrier_name` в командах, кроме `carrier_team` | не привязывается; судьба канала — Q3 |
| восстановление базы из снимка между записью Bridge и постом | те же состояния | теми же механизмами (ADR-100 §4) |

Кто и когда запускает проход по брошенным созданиям и событиям без
операций — **открыто** (§12, Q2). Когда отменять брошенный резерв —
вопрос живости, не безопасности: отменить можно в любой момент (живой
владелец не сможет сообщить успех), но лента разговора стоит за открытым
резервом до отмены (ADR-101 §5.11; на настоящем Mattermost сигнал смерти
владельца — снятие транзакционной блокировки через 15–60 с).

---

## 9. Проверки при активации и миграции

- `DriverName = postgres`, иначе 900–931 → `temporarily_unavailable`
  (ADR-100 §2.7).
- Миграции Bridge — под блокировкой `photon:schema`, идемпотентные, только
  вперёд; версия в `engram_schema`.
- Схема новее кода, отсутствует объект, не прошла проверка →
  900–931 отвечают `temporarily_unavailable`, остальной Bridge работает
  (ADR-100 §2.6).
- Проверка колонок и индексов `Posts`, на которые опираются 930/931, —
  как `checkDMSchema` (тип `props = jsonb`, индекс
  `(channelid, deleteat, createat)`).

---

## 10. Acceptance (реализации)

Каждая проверка имеет отрицательный контроль: правило ломается нарочно,
и проверка обязана упасть. Результаты — прогон исследования на
синтетическом PostgreSQL (фиктивный Mattermost повторяет F5: COUNT до
вставки, конфликт имени возвращает канал). Реализация обязана пройти те же
сценарии против настоящего PostgreSQL.

| # | сценарий | ожидание | исследование | контроль |
|---|---|---|---|---|
| C1 | 32 параллельных 900 с одним ключом | один разговор, один ключ, 3 участника, один канал в Mattermost | PASS | без PK «проверить-затем-вставить»: 2 записи одного ключа |
| C1b | тот же ключ, другое содержание | `idempotency_conflict` | PASS | — |
| C2 | 64 параллельных 903 | версия 65, 64 события с версиями 2..65 | PASS | без `FOR UPDATE`: версия 6–9 при 64 изменениях (три прогона) |
| C2b | 64 параллельных 903 с одним `expected_version` | 1 успех, 63 `version_conflict` | PASS | — |
| C3a | сбой после `CreateChannel` до привязки, повтор 900 | тот же разговор, `active`, один канал | PASS | — |
| C3b | команда заполнена до создания канала | резерв переезжает, канал только в новой команде | PASS | — |
| C3c | канал создан, затем команда заполнилась, повтор получает ошибку лимита | привязка к существующему каналу, один канал | PASS | смена команды по одной ошибке лимита: 2 канала (сирота) |
| C3d | две привязки разных каналов к одному резерву | одна успешна, вторая — расхождение привязки | PASS | — |
| C4 | 16 параллельных 910 одного участника | один период, версия +1 | PASS | без `FOR UPDATE`: дубль отвергнут ограничением схемы (PK/уникальный индекс) |
| C5 | add → remove → add | `no_access` → `active`; никогда не участвовавший — `conversation_not_found`; 2 периода | PASS | — |
| K1 | две параллельные привязки разных каналов к одному резерву (SQL) | вторая ждёт строку, после фиксации первой — 0 строк | PASS | — |
| K2 | привязать канал, уже привязанный к другому разговору | `UNIQUE (carrier_channel_id)` | PASS | — |
| K3 | перепривязать или снять канал у `bound` | 0 строк; `CHECK` отвергает | PASS | — |
| K5 | смена команды при параллельном исполнителе и освободившемся месте, шаги 2–5 под X | один канал, оба исполнителя получают его | PASS | без X (K4): 2 канала, сирота |
| M1–M8 | семантика advisory xact lock (§5.1) | как в таблице §5.1 | PASS | — |
| L1 | видимая лента §7.2 против перебора (группы равных `CreateAt`) | 0 расхождений | 2403/0 | без дочитывания группы: 1300 расхождений |
| L2 | планы §7.2 после 6 `EXECUTE` | Index Scan по `(channelid, deleteat, createat)` / `createat`, без Bitmap и полной сортировки | PASS | — |
| L3 | лента одним оператором с барьером (§7.2, ADR-101) против перебора, включая `since_join` | 0 расхождений | не выполнено | без барьера / страница и барьер разными операторами |
| L4 | план оператора L3 после 6 `EXECUTE` на объёме §7 | условия индекса, без полной сортировки | не выполнено | — |
| P1 | барьер и страница одним оператором, конкурентные отправители со сбоями (ADR-101 §5.12 п. 1) | 0 пропусков ниже курсора, 0 постов на надгробии | PASS (стенд AP-1) | «страница, затем барьер»: 918–1237 пропусков; без барьера: ≈2500 |
| P2 | порядок фиксации резервов (S8, S9) | вторая транзакция резерва ждёт первую | PASS (стенд AP-1) | `nextval`: позиция теряется (3 из 3) |
| P3 | надгробие: поздний пост через 3 и 8 с, задержка в хуках, потеря X посреди записи события (ADR-101 §9) | поздний пост не выдан; клиент получает один пост-событие | PASS (стенд AP-1) | отмена без фильтра надгробия: поздний пост выдан |
| P4 | повтор 930 после отмены: 7 сценариев и конкурентный прогон (ADR-101 §5.12 п. 2) | 0 потерь при ответе «успех», ≤ 1 опубликованной позиции на ключ | PASS (стенд AP-1) | поиск мимо надгробий: 87–373 потери; `PendingPostId` = ключ без сверки `CreateAt`: 131–337 |
| P5 | конкурентный прогон с W = 0 (W = 1 мс) | все инварианты | PASS (стенд AP-1) | прежний механизм с дописыванием: I7 нарушен (10–30) |

Дополнительно для реализации (в исследовании не выполнялось — нужен
настоящий Mattermost):

- сбой между фиксацией изменения и записью события → событие дописывается
  сверкой с тем же `at`, лента клиента содержит его на месте (E1–E4,
  барьер §6.4);
- сбой между `CreatePost` события и отметкой `written` → повторная запись
  находит пост (E3), дубля нет;
- при событии `pending` 931 не выдаёт ничего за его позицией, пока оно не
  дописано *(прежнее «930/931 при событии `pending` сначала дописывают
  его» — устарело, ADR-101)*;
- барьер и страница 931 через RPC-драйвер настоящего Mattermost и через
  master — одним оператором (ADR-101 §5.1b, §5.12: проверено по коду, не
  прогоном);
- `PendingPostId` и сверка `CreateAt` против настоящего `CreatePost`
  (§6.3 шаг 4);
- ни один пост носителя группы не создан с `CreateAt = 0` или без резерва
  (S10);
- ни один ответ 900–931 не содержит идентификаторов Mattermost (S4);
- ошибка SQL на держащем соединении X прекращает операцию до любого
  `CreatePost` (§5, M3);
- 900 при `DriverName ≠ postgres` и при непроверенной схеме —
  `temporarily_unavailable`, активация не падает.

---

## 11. Находка вне рамок: форма продолжения 218

Действующий `After` протокола 218 (`server/dm_sql.go:143`) использует
точную OR-форму без границы, которую планировщик не превращает в условие
индекса. Замер на синтетическом канале двух авторов (300 000 постов,
позиция посередине):

| форма | литерально | подготовленно (как драйвер, после 6 `EXECUTE`) |
|---|---|---|
| как в `dm_sql.go` (только OR) | 72 мс | 176 мс, Bitmap по `channel_id, update_at` + сортировка 180 000 строк |
| + `createat >= $2` | 0.13 мс | 136 мс, Bitmap + сортировка |
| форма §7.2 | 0.08 мс | 0.08 мс |

Цена растёт с длиной переписки до позиции курсора. Это не часть данной
спецификации: исправление 218 — отдельная задача над принятым протоколом
(форма §7.2 его семантику не меняет). Здесь зафиксировано, что проблема
измерена, и почему лента групп не повторяет форму `After`. Поправка к
ADR-092 §2.4 записана как необходимая в ADR-101 §7 п. 9 (текст —
`ap1-ordering-research.md` §11.6); семантика курсора 218 и окно W `direct`
не меняются.

---

## 12. OPEN QUESTIONS

Не решаются этой спецификацией; реализация не выбирает их молча.

- **Q1. Срок хранения ключей идемпотентности** (ADR-100 §5, ADR-099
  §10.2 — не короче горизонта автоповторов клиента). Индекс
  `engram_request_age` готов; значение и механизм удаления не выбраны. Сюда
  же — срок хранения опубликованных резервов `engram_position`: повтор 930
  ищет по ним (§6.3), поэтому срок связан с горизонтом повторов; надгробия
  хранятся постоянно (ADR-101 §8). От этого же срока зависит, как долго
  определён владелец ключа сообщения (§12 Q8 / ADR-102; ADR-102 §3).
- **Q2. Уборка брошенных созданий и незаписанных событий** (ADR-100 §5):
  кто запускает проход (фоновая задача плагина, при активации, по
  операции), с каким возрастом, что делать с разговором `creating`, к
  которому повтор уже не придёт (завершить или оставить; носитель по S6 в
  любом случае остаётся). Тот же проход отменяет брошенные резервы
  сообщений (§8); порог отмены — эксплуатационный параметр живости
  (ADR-101 §5.11, §8).
- **Q3. Внешняя архивация или удаление носителя и служебной команды**
  (ADR-100 §5): администратор Mattermost может заархивировать канал или
  команду; Bridge это не предотвращает (F4 — писать в архивный канал
  плагин может). Политика обнаружения и реакции не выбрана.
- **Q4. Снятие advisory lock при падении процесса плагина.** Семантика
  PostgreSQL проверена (§5.1): обрыв клиента снимает блокировку, если
  оператор не выполняется. Но клиент базы — сервер Mattermost, а не плагин;
  закрывает ли Mattermost серверную сторону коннекта RPC-драйвера при
  падении плагина — не проверено. От этого зависит, сколько разговор
  остаётся недоступным (`temporarily_unavailable` по бюджету ожидания, не
  зависание). Возможная страховка — `SET LOCAL
  idle_in_transaction_session_timeout` в держащей транзакции (M7); её
  влияние на пул Mattermost не проверено. **Частичный ответ** — §5.1
  (R7): транзакционная блокировка снимается через 15–60 с, сессионная — не
  снимается; страховка M7 по-прежнему не проверена.
- **Q5. Семантика курсора 902 по изменяемому `last_activity_at`.** ADR-099
  §6 задаёт порядок и непрозрачный курсор, но не поведение при изменении
  ключа во время прохода. Предложено в §6.5; требует подтверждения как
  уточнение протокола.
- **Q6 → AP-1 (§13). Закрыт ADR-101.** Порядок «событие ↔ сообщение»
  зависел от физических часов; теперь позиции событий и сообщений выдаёт
  один счётчик `H` (§4, §6.3).
- **Q7. Стоимость 930 в большом носителе.** Поиск по
  `Props.engram_client_msg_id` в горизонте 30 дней (как 217) проходил все
  посты автора в носителе за горизонт (§7.5). **Для 930 групп снят
  ADR-101:** повтор ищет резерв по индексу `engram_position_key_live`
  (§6.3), а пост — точечно по `(CreateAt, ключ)`; отличие от механики
  ADR-092 §2.5 записано ADR-101 §7 п. 2. Остаётся связь со сроком хранения
  опубликованных резервов (Q1).
- **Q8 (§12 Q8 / ADR-102). Область уникальности ключа сообщения в резервах. Закрыт
  ADR-102** (поправка к ADR-101 §5.6, `972cba6`). Неопределённость,
  отмеченная при синхронизации с ADR-101: индекс ADR-101 §5.6 —
  `(разговор, ключ)`, а ADR-092 §2.5.1 определяет отправку парой
  «отправитель + `client_msg_id`»; что делать с резервом другого `sender`,
  ADR-101 не говорил. Теперь: резерв того же `(разговор, client_msg_id)`
  другого отправителя в любом состоянии, включая надгробие, — не повтор,
  ответ `idempotency_conflict`, без резерва, без `CreatePost`, без
  изменения чужого резерва (§6.3 шаг 2); все резервы сообщений одного
  ключа принадлежат одному отправителю.
  **Связь с Q1** — не новое решение, а следствие ADR-101 §8 и ADR-102 §3:
  владелец ключа определён, пока существует хотя бы один резерв этого
  ключа. Надгробия хранятся постоянно, поэтому после отмены владелец
  определён всегда. Опубликованный резерв удаляется только по сроку
  хранения Q1; после удаления последнего опубликованного резерва ключа (и
  при отсутствии надгробий) владелец исторически больше не определяется —
  та же граница, что горизонт повторов Q1. Срок Q1 не выбран и остаётся
  открытым (Q1).

---

## 13. Архитектурная проблема AP-1: порядок «событие ↔ сообщение» зависит от физических часов

**Статус: решена ADR-101 (ПРИНЯТО, `a1468b0`).** Выбран класс B с
механизмом «резерв на каждую позицию + барьер видимости + надгробие», W = 0
(ADR-101 §5; исследование — `ap1-ordering-research.md`). Действующая модель
изложена в §2–§8 этого документа. **Раздел ниже сохранён как исторический
материал выбора класса** и описывает состояние до ADR-101; места,
опровергнутые или заменённые решением, помечены «устарело — ADR-101».
Прежний статус: «не решена… до решения реализация 930/931 заблокирована».

### 13.1. Проблема

Позиция элемента ленты — `(CreateAt, PostID)` поста носителя; при равном
`CreateAt` новее меньший `PostID` (ADR-092 §2.1, ADR-052, ADR-098 §7.2).
Позиции назначают **два разных источника**:

| элемент | кто назначает `CreateAt` | основание |
|---|---|---|
| событие | Bridge: `at = T + seq`, строго больше `at` предыдущего события (E1) | ADR-098 §7.3, ADR-099 §13: способ хранения события выбирает Bridge |
| сообщение 930 | Mattermost: `GetMillis()` узла, выполняющего `CreatePost`, после хуков `MessageWillBePosted` (F2, F10; ADR-092 §1.3) | ADR-099 §9.1: механика ADR-092 §2.5 |

События упорядочены между собой логически. Порядок **событие ↔ сообщение**
сравнивает значение, назначенное Bridge, со значением часов Mattermost, и
E6 («X снимается, когда часы прошли `at`») делает его верным только при
условии: **часы Bridge и часы узла Mattermost, пишущего сообщение, — одни
и те же и не идут назад.**

Условие нарушается:

- **несколько узлов Mattermost:** каждый узел выполняет `CreatePost` своего
  экземпляра плагина по своим часам; расхождение `δ` переставляет событие
  и сообщение, записанные с интервалом меньше `δ`;
- **один узел, шаг часов назад** (коррекция шагом): то же на величину шага.

Последствие для `since_join` (ADR-099 §9.4): участник добавлен (событие
`member_added` с `at = T`), сразу пишет сообщение; его `CreateAt < T` —
сообщение раньше собственного открывающего события, вне его периода, и
**невидимо ему самому**. Симметрично: сообщение, отправленное до удаления
участника, может встать после `member_removed` и выпасть из его прошлого
периода. Оба случая нарушают ADR-098 §7.2 («клиент видит их в той же
последовательности, в какой они произошли»).

### 13.2. Инварианты, которые обязан обеспечить механизм

Для одного разговора. «Операция A завершилась раньше начала B» — ответ A
получен до вызова B (порядок в реальном времени).

- **I1.** Открывающий элемент периода (`created` или `member_added`)
  предшествует каждому сообщению участника, отправленному в этом периоде.
- **I2.** Закрывающий элемент (`member_left`, `member_removed`) следует за
  каждым сообщением, завершившимся до изменения, и предшествует каждому
  сообщению, начатому после его завершения.
- **I3.** События разговора упорядочены детерминированно и совпадают с
  порядком `conversation_version`, внутри одной версии — с `seq`;
  `created` — самый ранний элемент ленты (ADR-099 §4).
- **I4.** Равные позиции разрешаются детерминированно правилом ADR-052
  (при равном `CreateAt` новее меньший `PostID`); равенство допустимо только
  между элементами, порядок которых не связан I1–I3 (конкурентные
  сообщения).
- **I5.** Безопасность семантики ADR-098/099 (I1–I4, границы периодов
  §9.4) не зависит от синхронизации и монотонности физических часов; часы
  могут влиять только на живость (задержку, `temporarily_unavailable`).
- **I6.** Позиция элемента назначается один раз при записи и не меняется
  (пост нельзя перенести, F7); повтор 930/900–921 и дописывание события
  после сбоя не меняют позиций уже записанных элементов.
- **I7.** Порядок 931 для любого элемента однозначен относительно событий:
  клиентский курсор, прошедший позицию события, не может позже встретить
  более раннее по I1/I2 сообщение, и наоборот (W ADR-092 §2.3 — только
  для поздней фиксации конкурентных сообщений). *Устарело — ADR-101 §2:
  принятая формулировка I7 окна W не использует; элемент не появляется
  задним числом за курсором клиента.*
- **I8.** `direct` (217/218, ADR-092) не меняется.

### 13.3. Классы решений (без выбора)

Сейчас в Mattermost нет ни одного поста групп: у всех классов нет миграции
существующих данных групп; ниже «цена миграции» — цена **смены** класса
после появления данных.

#### A. Физический `CreateAt`, ограничение: один узел и часы без шагов назад

- **Модель:** как в §4 (E1–E6); ограничение записывается как предусловие
  корректности.
- **ADR:** новый ADR фиксирует предусловие; ADR-077 §5 (HA не требование)
  совместим; ADR-092, 098, 099 не меняются. Появление HA требует
  пересмотра (ADR-077 §5.2 допускает HA в будущем — для групп он станет
  несовместимым без нового решения).
- **ADR-092 §1.3:** без изменений; сообщение получает время Mattermost.
- **`since_join`:** верен при соблюдении предусловия; I5 **не выполняется**
  — безопасность зависит от часов.
- **Равные позиции:** события — разные мс (E1); событие ↔ сообщение —
  строго различны по E6; сообщения между собой — ADR-052.
- **Retry / сверка:** как §4, §8; отложенное событие пишется со своим `at`
  (E5 исключает чтение между фиксацией и записью).
- **Посты Mattermost:** обычные, время — Mattermost.
- **Цена смены класса позже:** A → B/D — только для новых записей, старые
  позиции остаются; ошибки порядка, случившиеся при нарушении предусловия,
  исправить нельзя (F7).
- **Эксплуатация:** режим часов без шагов (подстройка), наблюдение;
  нарушение не обнаруживается протоколом.

#### B. Bridge назначает `CreateAt` сообщений групп (логическая позиция)

- **Модель:** каждая запись в носитель получает `max(часы, последняя
  позиция + 1)`: событие под X — `max(T, max CreateAt носителя + 1, at
  предыдущего события + 1)`; сообщение под S — `CreateAt = max(часы, at
  последнего события + 1)`, передаётся в `CreatePost` явно (F2).
  *Устарело — ADR-101 §5.1: позиция и событий, и сообщений — `max(часы,
  H + 1)` из одного счётчика `H`, а не по постам носителя (H4).*
- **ADR:** новый ADR, уточняющий ADR-099 §3 («время — Unix ms сервера»),
  §9.1 (930 задаёт `CreateAt`), §9.2 (окно W для групп), §16 (строка
  «ADR-092 … механика используется по ссылке»); ADR-098 §7.2 не меняется;
  ADR-092 не меняется (§13.4).
- **ADR-092 §1.3:** текст остаётся верным для `direct`; для групп момент
  присвоения `CreateAt` переносится из `App.CreatePost` в Bridge — вывод
  окна W §2.3 для групп теряет основание (§13.4).
- **`since_join`:** I1/I2 выполняются логически; I5 — да для порядка;
  `created_at` сообщения становится логической меткой (≥ часов, при шаге
  часов назад или расхождении узлов — впереди физического времени).
- **Равные позиции:** событие ↔ сообщение строго различны (+1); сообщения
  под S конкурентны и могут совпасть — ADR-052. *Устарело — ADR-101 §5.3:
  продвижение `H` сериализует выдачу, равных позиций нет вовсе.*
- **Retry / сверка:** повтор 930 находит пост по `client_msg_id` и
  возвращает его исходный `CreateAt` (I6); до `CreatePost` значение не
  сохраняется — повтор после сбоя вычисляет новое; событие — как §4.
  *Устарело — ADR-101 §5.6: позиция резервируется до `CreatePost`, повтор
  ищет резерв мимо надгробий (§6.3).*
- **Посты Mattermost:** посты с явным `CreateAt`, возможен `CreateAt`
  впереди часов (сдвигает `Channels.LastPostAt`, F2).
- **Цена смены класса позже:** B → A/D — новые сообщения снова получают
  время Mattermost; старые позиции остаются.
- **Остаток:** окно W для групп нужно вывести заново: `D` включает RPC
  плагин → сервер и хуки `MessageWillBePosted` **всех** плагинов (F10),
  которые выполняются после присвоения. *Устарело — ADR-101 §5.5: окно не
  выводится, W = 0; позднюю фиксацию закрывают барьер и надгробие.*

#### C. Отдельная логическая последовательность поверх `CreateAt`

- **Модель:** Bridge выдаёт каждому элементу ленты (событию и сообщению)
  номер `seq` в разговоре; порядок ленты — по `seq`, `CreateAt` — только
  время для показа. Номер хранится у Bridge (индекс «номер → пост»), так
  как индекса по `Props` в `Posts` нет и его нельзя добавить (ADR-100
  §2.3).
- **ADR:** ADR-098 §7.2 (порядок `(created_at, id)`), ADR-099 §9.2–§9.4
  (ключ порядка, курсор, границы периодов, форма Item), ADR-100 §2.2
  («не хранятся сообщения и события ленты» — появляется строка Bridge на
  каждое сообщение); ADR-052 не меняется (`OrderKey` — `int64`
  приложения, Core его не толкует); ADR-092 не меняется.
- **ADR-092 §1.3:** не затрагивается — `CreateAt` сообщения остаётся
  временем Mattermost и в порядок не входит.
- **`since_join`:** границы периодов — номера; I1–I5 выполняются логически,
  если номер сообщения выдаётся после проверки участия под S.
- **Равные позиции:** нет — номера уникальны.
- **Retry / сверка:** номер выдаётся до `CreatePost` → появляется частичное
  состояние **на каждое сообщение** (номер без поста, пост без индекса) и
  сверка по сообщениям, а не только по событиям; пропуски номеров
  допустимы, если номер — только порядок.
- **Посты Mattermost:** обычные; порядок живёт вне них.
- **Цена смены класса позже:** в C — выдать номера всем существующим
  элементам по `(CreateAt, PostID)` и сменить протокол 931 (новый ключ
  порядка, пересинхронизация клиентов); из C — обратная смена протокола.
  Дороже всех.

#### D. Физический `CreateAt` сообщений с логической «оградой» (не документирован в ADR)

Документированного в текущих ADR механизма, кроме A–C, не найдено. Из
документированных частей (E1, E5 блокировок §5, F2, ADR-098 §7.3)
собирается ещё один класс:

- **Модель:** события — логические позиции, как в B (`at = max(T, max
  CreateAt носителя + 1, at предыдущего + 1)`, под X видны все
  зафиксированные сообщения). Сообщения — время Mattermost, но 930 под S
  **ждёт**, пока часы Bridge на своём узле не станут больше `at`
  последнего события (или отвечает `temporarily_unavailable` по бюджету),
  и только потом вызывает `CreatePost` на том же узле.
- **ADR:** ADR-099 §9.1 получает предусловие отправки (ожидание,
  возможный `temporarily_unavailable`); ADR-092, 098 не меняются.
- **ADR-092 §1.3:** без изменений — `CreateAt` присваивает `App.CreatePost`,
  вывод W §2.3 остаётся верным.
- **`since_join`:** I1/I2 выполняются, пока часы одного узла не шагнули
  назад **в окне** между проверкой и присвоением `CreateAt` (RPC и хуки,
  F10) — остаточная зависимость от часов, узкая; расхождение узлов влияет
  только на живость (ожидание до `δ`). I5 выполняется не полностью.
- **Равные позиции:** событие ↔ сообщение строго различны; сообщения —
  ADR-052.
- **Retry / сверка:** как A; нарушение обнаружимо после записи (у
  возвращённого поста `CreateAt ≤ at` события), но не исправимо (F7).
- **Посты Mattermost:** обычные, время — Mattermost.
- **Цена смены класса позже:** как A.

#### Сводка

| | A | B | C | D |
|---|---|---|---|---|
| I5 (безопасность без часов) | нет | да | да | почти (узкое окно) |
| кто назначает `CreateAt` сообщения | Mattermost | Bridge | Mattermost | Mattermost |
| ключ порядка ленты | `(CreateAt, PostID)` | `(CreateAt, PostID)` | `seq` | `(CreateAt, PostID)` |
| ADR к изменению | новый (предусловие) | 099 §3, §9.1, §9.2, §16 | 098 §7.2, 099 §9.2–§9.4, 100 §2.2 | 099 §9.1 |
| ADR-092 | не меняется | не меняется; вывод W не переносится на группы | не меняется | не меняется |
| HA Mattermost | несовместим | совместим | совместим | совместим, ценой ожидания |
| частичные состояния на сообщение | нет | нет (*устарело — ADR-101: механизм B пишет резерв на каждый элемент, §2*) | да | нет |
| цена смены класса позже | низкая | низкая | высокая | низкая |

### 13.4. Отходит ли B от ADR-092 §1.3 — конкретные места

Проверено по тексту ADR-092 ред. 3 и ADR-099:

1. **ADR-092 §1.3 — не норма, а факт.** Раздел «1. Факты» описывает
   `App.CreatePost`: «хуки `MessageWillBePosted` → **`CreateAt =
   GetMillis()`** → `getEmbedsAndImages` → `Save`». В коде присваивание
   условное: `if post.CreateAt == 0` (`app/post.go:297–299`; §1.3 условие
   не упоминает). Пост с явным `CreateAt` в эту ветку не входит; §1.3
   остаётся верным описанием для постов без явного `CreateAt`, то есть для
   `direct`. **Буквального противоречия B с §1.3 нет.**
2. **ADR-092 §2.3 — конфликт с выводом окна W.** «`W` — страховка от
   поздней фиксации (§1.3): задержка `D` между присвоением `CreateAt` и
   фиксацией поста»; `W_min = F_max × T_link + T_db_max` считает только
   синхронные загрузки предпросмотра и запись. При B `CreateAt`
   присваивается в Bridge **до** RPC к серверу и **до** хуков
   `MessageWillBePosted` всех плагинов; `D` получает слагаемые, которых
   формула не содержит и которые не ограничены (время чужих хуков). Для
   групп гарантия §2.3 перестаёт выводиться.
3. **ADR-099 §9.2 и §16 — ссылка «по механике ADR-092».** §9.2: проход
   «ровно ADR-092 §2.2.1–2.2.4»; §16: «ADR-092 ред. 3 | не меняется;
   механика §2.2 и §2.5 используется по ссылке». Проход опирается на W
   (§2.3); при B ссылка становится неверной для групп (п. 2).
4. **ADR-099 §9.1 — механика отправки.** «Send — механика ADR-092 §2.5
   по `conversation_id`»; ADR-092 §2.5.1 шаг 4: «не найден → `CreatePost` с
   `Props.engram_client_msg_id`». B добавляет явный `CreateAt` — это
   расширение шага, а не противоречие, но ссылка на механику §2.5 без
   оговорок становится неточной.
5. **ADR-099 §3 — «Время — Unix ms сервера».** При B `created_at`
   сообщения — логическая метка, которая при шаге часов назад или
   расхождении узлов опережает время сервера.

**Итог:** ADR-092 можно сохранить **буквально** — он описывает `direct`, а
его текст для постов без явного `CreateAt` остаётся верным. Изменение B
ложится на **ADR-099** (§3, §9.1, §9.2, §16): окно W для групп должно
получить собственный вывод, а использование механики ADR-092 «по ссылке» —
явные отличия. Утверждение прежней редакции этого раздела («отход от
модели ADR-092 §1.3») было неточным: конфликт — в выводе W (§2.3) и в
ссылках ADR-099, а не в §1.3. *Устарело в части окна — ADR-101 §5.5: для
групп W = 0, собственный вывод окна не нужен; поправки ADR-099 перечислены
в ADR-101 §7.*

### 13.5. Что нужно решить отдельным ADR

*Решено ADR-101: класс B; смысл `created_at` — ADR-101 §5.4; вывод окна W
для групп не нужен (W = 0, §5.5). Перечень ниже — исторический.*

- класс решения (A, B, C, D или иной);
- для A — формулировка предусловия (один узел, режим часов) и что
  происходит при его нарушении;
- для B — собственный вывод окна W для групп и смысл `created_at`
  сообщения;
- для C — новый ключ порядка в протоколе 931 и место индекса номеров;
- для D — бюджет ожидания 930 и реакция на обнаруженное нарушение.
