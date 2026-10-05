# Implementation contract: ADR-104 … ADR-107 (Messaging v2)

**Статус:** P1, выведен orchestrator из принятых ADR-104–107 (решение владельца 2026-10-05: «не запрашивай
дополнительные решения, если они следуют из принятых ADR»). Каждое решение ниже либо цитирует ADR, либо
отмечено «реализация» — выбор, который ADR оставил исполнителю и который не меняет семантику.
База: `messaging/adr-104-107` @ `540830d` (линия slice-6 `957ded3` + fix215 `d51ac8b`).
Образец формы и правил: `conversation-implementation-contract.md` (900–931) — всё, что здесь не сказано, —
как там (порядок проверок ADR-099 §11, идемпотентность §10.2, X/S-блокировки ADR-101 §5.2/5.3, ошибки §12,
`temporarily_unavailable` при сбое хранилища, никаких идентификаторов Mattermost в ответах).

## 0. Неизменяемое

217/218, 930/931, 900–904, 910–912, 920–921 — без изменений поведения (diff тестов этих кодов = 0, кроме
явно названных ниже ответов на новый `kind`). `photon-core/ports/*` не меняются.

## 1. Срезы

| срез | содержание | ADR | схема |
|---|---|---|---|
| M1 | коды 905, 906, 907, 913, 914, 915, 916 (Conversation), 219 (User), 940 (Conversation) в `enum Action` без oneof-payload (как срез 1, ADR-099 §14); `ResourceFor` (219 → UserResource, 9xx — уже); правило authz «аутентифицирован» на каждый код; обработчики-заглушки `temporarily_unavailable`; `TestAuthzCoversEveryAction` зелёный | все | — |
| M2 | `direct`: 905, вид в 900-семействе, запреты, 902 все виды + `last_item` (+ `peer_photon_id` у `direct`) | 104 | v3 |
| M3 | 940 (Conversation, назад) и 219 (DM legacy, назад) | 105 | — |
| M4 | 906 разархивирование; 907 личное состояние; 902 порядок/скрытие | 106 | v4 |
| M5 | 913–916 приглашения, событие `member_joined`, `invite_invalid` | 107 этап 1 | v5 |
| M6 | 917/918 в `enum Action` и authz; `visibility` в 900/903/Snapshot; каталог 917; вступление 918 | 107 этап 2 | v6 |

Каждый срез — один commit на своей ветке от вершины предыдущего; PG-тесты на настоящем PostgreSQL
(`tools/pgtest`), `-race` на изменённых пакетах, `go vet`, `gofmt`.

## 2. M2 — `direct` (ADR-104)

- **Схема v3:** `engram_direct_pair (low_pid text, high_pid text, conversation_id text NOT NULL UNIQUE,
  PRIMARY KEY (low_pid, high_pid))`; `low_pid < high_pid` (строки канонического PhotonID).
- **905 OPEN_DIRECT** `{request_id, peer_photon_id}` → `{conversation: Snapshot, created: bool}`
  (ADR-104 §3.2). Порядок: формат PhotonID → `invalid_photon_id`; свой → `invalid_request`; peer не найден /
  деактивирован → `photon_id_not_found` (как `resolveExisting`). Идемпотентность — **по паре**: под
  транзакционной advisory-блокировкой ключа пары (реализация) найти строку пары → вернуть разговор
  (`created:false`); иначе создать как 900 (носитель — распределитель §6.0 контракта 900–931; `kind:"direct"`,
  `title:""`, `history_policy:"full"`, оба участника `member`, события `created` + `member_added` пира) и
  вставить пару. `request_id` записывается (§10.2), но на результат не влияет: повтор и встречный вызов
  дают тот же `conversation_id`.
- **Права `direct`** (ADR-104 §3.1): обоим — `messages.send`, `messages.read`; 903, 904, 910, 911, 912, 920, 921
  на `direct` → `invalid_request` (после проверки доступа, ADR-099 §11). 900 с `kind:"direct"` →
  `invalid_request` (создаёт только 905).
- **902**: возвращает все виды; ListItem получает `last_item` (последний **видимый** вызывающему элемент ленты,
  форма Item ADR-099 §9.3, `null` если ленты нет) и для `direct` — `peer_photon_id` (реализация: форма, без
  новой семантики — второй участник ADR-104 §3.1). Порядок и курсор — как были (ADR-099 §6).
- **Тесты:** гонка двух встречных 905 → один разговор (PG, параллельно); повтор; свой/неизвестный/
  деактивированный; 930/931 на `direct` (P-тесты как у групп); запреты; 902 со смесью видов, `last_item`
  при since_join не раскрывает чужие периоды.

## 3. M3 — история назад (ADR-105)

- **940** `{conversation_id, before:{created_at,id}, limit?}` → `{items, at_start}`: доступ как 931 (S-блокировка,
  `callerAccess`, архив §8.1), видимость — периоды since_join; элементы строго старше якоря, ближайшие к нему,
  в порядке ленты; `limit` 1..200 (иначе `invalid_limit`), по умолчанию 60; `at_start` — нет видимых элементов
  старше выданных. Барьер не нужен (ADR-105 §1.4), но надгробия и неопубликованные резервы не выдаются.
- **219** `{with_photon_id, before:{created_at,id}, limit?}` → `{items, at_start}` по DM-каналу пары
  (как 218: S1-чтение, порядок ADR-092 §2.1, форма сообщения 218, `client_msg_id` только своих); окна нет
  (ADR-105 §1.3).
- **Тесты:** граница якоря (равный `created_at`, разные id), `at_start`, since_join-разрывы не раскрываются,
  `limit` вне диапазона, поздний пост в окне (219) находит проход 218 вперёд.

## 4. M4 — жизненный цикл (ADR-106)

- **Схема v4:** `engram_member_state (conversation_id, photon_id, hidden_at bigint NULL, pinned_at bigint NULL,
  muted_until bigint NULL, updated_at bigint NOT NULL, PRIMARY KEY (conversation_id, photon_id))`.
- **906 UNARCHIVE** `{request_id, conversation_id, expected_version?}` → `{conversation: Snapshot}`: под X, право
  `conversation.archive`; не архивный → `changed`-эквивалент: возвращается снимок без события и без версии
  (реализация, как «нет изменения» §6.2); иначе событие `unarchived`, версия +1. Разговор без участников в
  периоде `active` разархивировать некому (ADR-106 §2.1): любой вызывающий получает `no_access` /
  `conversation_not_found` по порядку проверок ADR-099 §11 — отдельного кода нет.
- **907 MEMBER_STATE_SET** `{conversation_id, hidden?, pinned?, muted_until?}` → `{member_state}`; доступ —
  участник `active` или бывший участник архивного разговора; без X (не меняет разговор); `pinned:true` при уже
  5 других закреплённых → `invalid_request` (ADR-106 Q4; отдельного кода в ADR нет — клиент знает число из 902).
- **«Возврат скрытого» — правило чтения, а не запись в 930** (реализация; 930 заморожен): разговор скрыт, если
  `hidden_at IS NOT NULL AND (last_activity_at <= hidden_at OR muted_until > now)`.
- **902**: `member_state` в ListItem; порядок — закреплённые первыми (`pinned_at` убыв.), затем
  `last_activity_at`; скрытые не возвращаются без `include_hidden:true`. Курсор keyset расширяется
  закреплением (реализация: закреплённые — первая страница целиком, ≤5).
- **Тесты:** 906 право/событие/версия/повтор; 907 идемпотентность, невидимость другим, версия не меняется;
  скрытый возвращается после нового сообщения и не возвращается при `muted_until`; предел 5.

## 5. M5 — приглашения (ADR-107 этап 1)

- **Схема v5:** `engram_invite (invite_id text PK, conversation_id text NOT NULL, token_hash bytea NOT NULL UNIQUE,
  created_by text, created_at bigint, expires_at bigint NULL, max_uses int NULL, uses int NOT NULL DEFAULT 0,
  revoked_at bigint NULL)`; хранится SHA-256 токена.
- **913** создать (`members.add`, ключ §10.2) → токен 128 бит, base64url, только в этом ответе и в сохранённом
  ответе ключа идемпотентности (повтор отдаёт тот же токен); **914** отозвать; **915** список без токенов.
- **916 JOIN_BY_INVITE** `{request_id, token}`: найти по хэшу под X разговора; недействителен (нет / отозван /
  истёк / `uses >= max_uses`) → `invite_invalid` (один код); архив → `conversation_archived`; уже участник →
  снимок без события; иначе новый период `member`, `uses+1`, событие `member_joined` (`actor` — вступивший,
  `invite_id`), версия +1.
- **Ограничение частоты** 916: 20 попыток в минуту на пользователя в памяти процесса (реализация,
  эксплуатационный параметр ADR-107 §3).
- **Тесты:** гонка двух вступающих при `max_uses=1` → ровно один; все причины → один код; токен не в логах
  (проверка журнала теста); хэш в таблице; since_join для вступившего.

## 5a. M6 — публичные разговоры (ADR-107 этап 2)

Добавлено 2026-10-06 по решению владельца ADR-107 Q1 («этап 2 — следующим»), Q3, Q4; выведено orchestrator, новых
решений не содержит. Отмечено «реализация» — то, что ADR оставил исполнителю.

- **Коды:** 917 `DIRECTORY_SEARCH`, 918 `JOIN_PUBLIC` в `enum Action` (резерв ADR-099 §2.3, свободны; как M1 —
  без oneof-payload, перегенерация `photon.pb.go` тем же protoc), `ResourceFor` → ConversationResource, authz
  «аутентифицирован», `TestAuthzCoversEveryAction` зелёный.
- **Схема v6:** `engram_conversation.visibility text NOT NULL DEFAULT 'private'`, ограничение
  `visibility IN ('private','public')`; существующие строки — `private`.
- **900:** необязательное `visibility` (`private` по умолчанию); при `public` поле `history_policy` можно опустить —
  тогда `full` (ADR-107 Q3); при `private` `history_policy` обязательно, как было (запросы без `visibility` —
  байт в байт прежнее поведение, тесты 900 без изменений). `visibility` входит в хэш ключа §10.2 только когда
  передано (реализация: хэш прежних запросов не меняется).
- **903:** необязательное `visibility`; право `conversation.update`; изменение — в событии `metadata_changed`
  (поле `visibility`), версия +1; на `direct` 903 уже `invalid_request` (M2). **Snapshot** и ListItem 902
  получают `visibility`. 905 создаёт `private`.
- **917** `{query, cursor?, limit?}` → `{conversations:[{conversation_id, title, description, member_count}],
  next_cursor?}`: только `public` и не архивные; без состава и ленты (ADR-107 §2.5); подстрока названия без учёта
  регистра (Q4); `query` после обрезки 1..64 символа, иначе `invalid_request`; `limit` 1..50, по умолчанию 20,
  иначе `invalid_limit`; порядок `(created_at DESC, conversation_id)`, курсор непрозрачный keyset (реализация).
  Вызывающему участие не нужно; закрытые не видны никогда.
- **918** `{request_id, conversation_id}` → `{conversation: Snapshot}`: как 916 (ключ §10.2, X, новый период
  `member`, событие `member_joined` без `invite_id`, версия +1, уже участник — снимок без события), но для
  `public`; `private` или нет такого → `conversation_not_found` (ADR-107 §2.6, ADR-098 §8); архив →
  `conversation_archived`. Ограничение частоты — общее с 916 (ADR-107 §3).
- **Тесты:** 900/903 без `visibility` — прежние тесты без изменений; `public` без `history_policy` → `full`;
  917 не возвращает `private`/архивные, регистр, курсор без дублей; 918 на `private` неотличим от несуществующего;
  гонка двух 918 — два разных периода, `member_count` +2; since_join для вступившего по 918.

## 6. Клиент (engram-android, слой данных; экраны — в фазе UI)

| срез | содержание |
|---|---|
| A1 | кодек 905/902(`last_item`, `peer_photon_id`); `direct` через существующий Conversation-слой (лента `grp:<own>:<cid>` — префикс сохраняется для всех видов, переименование тронуло бы замороженные группы); модель единого списка = 902 (все виды) + legacy-пары 215 без `direct`; отправка новых личных — 930 через outbox |
| A2 | 219/940 → 802 `dir:"before"` (Core уже умеет, ADR-065 §2) |
| A3 | кодек 906/907, порядок и скрытие в модели списка |
| A4 | кодек 913–916 |
| A5 | кодек 917/918, `visibility` в 900/903/Snapshot/ListItem |

## 7. Найдено при выводе контракта

- **Очистка локальной ленты** («удалить у себя», ADR-106 §2.5) требует локального действия Core, которого нет
  (`localfeed` не удаляет; действия 800–812 вводились ADR-064/065). Нужна поправка — отдельное решение D11
  (проект ADR). До него A3 реализует скрытие, но не очистку.
- **Live acceptance** требует выкладки Bridge на стенд — отдельное разрешение владельца.
