# Локальные действия 804–812: каталог, курсор, исходящие

Спецификация среза «Core → диспетчер» для ADR-093 (исходящие) и ADR-095
(поправка к ADR-065). **Написана до кода** и принята владельцем 2026-09-30.
Номера 806–812 ADR-093 §2.7 оставил на реализацию — здесь они и
закрепляются.

Основание: ADR-093 (§2.7 — имена, номера при реализации), ADR-095 (804/805,
802), ADR-064 §2 (сотня 800–899, явные отказы в Bridge), ADR-065 §8
(локальный отказ — отказ вызова, не `{ok:false}`), ADR-038 (конверт).

## Общее

- Конверт успеха: `{"ok":true,"data":{…}}`. Вердикта `ok:false` у локальных
  действий нет (ADR-065 §8.2): отказ — код возврата `Photon_SendAction` +
  `Photon_GetLastError`.
- Отказы вызова:
  | причина | класс | код ABI |
  |---|---|---|
  | payload не JSON; нет/неверное поле; `queue` пуст или с `0x00`; `body` отсутствует; `lease_ms ≤ 0`; неизвестный `kind`; `retry_at_ms` при `permanent`; неизвестный `op_id`; запрещённый переход | `ClassInvalidArgument` | `PHOTON_ERR_INVALID_ARGUMENT` |
  | нет секции `storage` | `ClassNotConnected` | ADR-064 п. 1.5 |
  | отказ транзакции | `ClassInternal` | `PHOTON_ERR_INTERNAL` |
- Целые (`seq`, `*_ms`, `*_at`) — int64, разбор типизированный (ADR-065 §2.2).
- `body` — любое JSON-значение, проходит побайтово (ADR-034).
- Время — Unix ms по часам устройства; для порядка не используется.
- **Действующее состояние**: операция в `sending` с `lease_until ≤ now`
  везде показывается и обрабатывается как `pending` (ADR-093 §2.2).

### Операция на проводе (`Op`)

```
{"op_id":"<uuid v4>", "queue":"<строка>", "seq":<int64>,
 "body":<JSON>, "state":"pending"|"sending"|"sent"|"failed",
 "attempts":<int>, "lease_until":<ms>,   // только при state=sending
 "retry_at":<ms>,                        // если задан вызывающим
 "last_error":"<строка>",                // если был отказ
 "created_at":<ms>, "updated_at":<ms>}
```

## 804 ACTION_LOCAL_FEED_CATALOG — ADR-095 §2.9

- payload `{}`
- ответ `{"channels":[{"channel_id":"…","newest":{"order_key":…,"id":"…"}}, …]}`
  свежие первыми; пусто — `{"channels":[]}`.

## 805 ACTION_LOCAL_FEED_CURSOR — ADR-095 §2.9

- payload `{"channel_id":"…"}` (непустой, без `0x00`)
- ответ `{"found":true,"cursor":"…"}` | `{"found":false}`.

## 802 — дополнение ADR-095 §2.4–2.6

- payload + `"cursor":"<непустая>"?`, `"confirms":["<op_id>",…]?`
- ответ + `"confirms":{"confirmed":N,"already_sent":N,"unknown":N}`
  (всегда присутствует; без поля `confirms` в payload — нули).

## 806 OUTBOX_ENQUEUE

- payload `{"queue":"…","body":<JSON>}`
- ответ `{"op":Op}` — `state:"pending"`, `attempts:0`, новый `op_id`, `seq`
  = последний в очереди + 1.

## 807 OUTBOX_LEASE

- payload `{"queue":"…","lease_ms":<int64 > 0>}`
- голова — первая операция очереди в действующем `pending` либо `sending`
  (`sent` и `failed` пропускаются).

| ситуация | ответ |
|---|---|
| голова в `pending` | `{"leased":true,"op":Op}` — `state:"sending"`, `attempts`+1, `lease_until = now + lease_ms` |
| **истёкшая аренда** головы | то же: `leased:true`, **тот же `op_id`**, `attempts`+1 (признак повтора — `attempts > 1`) |
| **голова в действующей аренде** | `{"leased":false,"reason":"head_leased","op":Op}` — голова с её `lease_until`; следующая не выдаётся (FIFO) |
| **пустая очередь** (нет `pending`/`sending`) | `{"leased":false,"reason":"empty"}` |

Пустая очередь и занятая голова — **результаты, не ошибки** (`ok:true`):
это штатные состояния цикла отправки. `retry_at` аренду не ограничивает —
когда пытаться, решает вызывающий (ADR-093 §2.3).

## 808 OUTBOX_COMPLETE

- payload `{"op_id":"…"}`
- ответ `{"op":Op,"changed":bool}`

| из | итог |
|---|---|
| `sending` / `pending` (в т.ч. истёкшая аренда — поздний ответ) | `sent`, `changed:true` |
| `failed` | `sent`, `changed:true` — утверждение сервера об успехе сильнее прежней классификации |
| **`sent` (повторный Complete)** | `changed:false`, `ok:true`, ничего не меняется |
| **неизвестный `op_id`** | отказ вызова `PHOTON_ERR_INVALID_ARGUMENT` |

## 809 OUTBOX_FAIL

- payload `{"op_id":"…","kind":"transient"|"permanent","reason":"…"?,"retry_at_ms":<int64>?}`
  (`retry_at_ms` — только при `transient`)
- ответ `{"op":Op,"changed":bool}`

| kind \ из | `pending`/`sending` | `failed` | `sent` |
|---|---|---|---|
| `transient` | `pending`, аренда снята, `retry_at`, `last_error`; **голова остаётся головой** | отказ вызова (переход запрещён) | `changed:false` (поглощение `sent`) |
| `permanent` | `failed`, `last_error`; очередь идёт дальше | `changed:false` | `changed:false` (поглощение `sent`) |

## 810 OUTBOX_RETRY

- payload `{"op_id":"…"}`
- ответ `{"op":Op,"changed":bool}`

| из | итог |
|---|---|
| **`failed`** | `pending`, **тот же `op_id` и `seq`** (прежнее место в очереди; снова становится головой, если раньше остальных незавершённых), `attempts` сохраняется |
| `pending` | `changed:false` |
| `sending` (действующая аренда) | отказ вызова — операция в полёте |
| `sent` | `changed:false` (поглощение `sent`) |

## 811 OUTBOX_CANCEL

- payload `{"op_id":"…"}`
- ответ `{"cancelled":true}` — операция и её индекс удалены.

| из | итог |
|---|---|
| `failed` | удалена |
| `pending` с `attempts = 0` (ни разу не выдана) | удалена |
| `pending` с `attempts > 0` (в т.ч. истёкшая аренда) | отказ вызова — сервер мог её получить |
| `sending` | отказ вызова — в полёте |
| `sent` | отказ вызова — уже доставлена |
| неизвестный `op_id` (в т.ч. повторная отмена) | отказ вызова |

## 812 OUTBOX_LIST

- payload `{"queue":"…"}`
- ответ `{"ops":[Op,…]}` по возрастанию `seq`, в действующем состоянии,
  включая `sent` (до уборки) и `failed`; пусто — `{"ops":[]}`.

Уборка `sent` (ADR-093 §2.6) действием в этом срезе не выводится.

## `sent` поглощает поздние исходы (принято 2026-09-30)

`sent` — терминальное состояние, установленное подтверждением сервера, и
оно сильнее любого позднего исхода:

| действие на `sent` | итог |
|---|---|
| 808 `COMPLETE` | `changed:false`, `ok:true` |
| 809 `FAIL transient` | `changed:false`, `ok:true` |
| 809 `FAIL permanent` | `changed:false`, `ok:true` |
| 810 `RETRY` | `changed:false`, `ok:true` |
| 811 `CANCEL` | **отказ вызова** — отменить доставленную операцию нельзя, и вызывающий обязан это узнать |

Основание — штатная гонка: ответ 213 потерян → вызывающий шлёт `FAIL
transient`, а синхронизация (214 → 802 `confirms`) уже подтвердила тот же
`op_id`. Ошибка вызова в этом месте была бы ложной тревогой. Это меняет
реализацию `8a9e71b`, где такие переходы возвращали `ErrStateConflict`.
