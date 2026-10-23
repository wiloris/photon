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

---

# P2P-исполнитель хоста: 860–865 (ADR-116 §3)

Исполнитель строится, только если в `config_json` есть секция
`"p2p": {"device_key_path": …, "peer_loss_timeout_ms": …,
"establish_timeout_ms": …}`. `device.key` — существующий (ADR-086),
Core его только читает. Без секции каждое действие отвечает вердиктом
`p2p_not_configured`.

Ключи, блобы и `payload` — **base64** (так `encoding/json` кодирует байты).
Негодный JSON или отсутствие обязательного поля — **ошибка вызова**
(`PHOTON_ERR_INVALID_ARGUMENT`); доменный отказ — **вердикт** `ok:false`
(ADR-038): `not_found`, `not_ready`, `too_many_sessions`,
`payload_too_large`, `negotiation_failed`. Чужой и несуществующий
`session_id`/`negotiation_id` неотличимы — `not_found`.

| код | payload | data |
|---|---|---|
| 860 `P2P_OFFER` | `{peer_public_key, turn?}` | `{negotiation_id, offer}` |
| 861 `P2P_ACCEPT` | `{peer_public_key, offer, turn?}` | `{session_id, answer}` |
| 862 `P2P_COMPLETE` | `{negotiation_id, answer}` | `{session_id}` |
| 863 `P2P_SEND` | `{session_id, payload}` (≤ 64 KiB) | `{}` |
| 864 `P2P_CLOSE` | `{session_id}` (или `negotiation_id`) | `{}` |
| 865 `P2P_EVENTS` | `{max?}` (≤ 64) | `{events: [...]}` |

`turn` — `{urls, username, credential}`, краткоживущие учётные данные
Bridge (1006). `peer_public_key` — **ожидаемый** ключ собеседника; его
подтверждает handshake ADR-041, несовпадение даёт `P2P_CLOSED failed`.

`negotiation_id` потребляется одним `P2P_COMPLETE`; незавершённое
согласование освобождается по `establish_timeout_ms`. Согласование живёт
дольше вызова 860: контекст действия отменяется сразу после ответа, и
привязка к нему снимала бы согласование до прихода answer (ловушка
ADR-044 §6.1, найдена первым прогоном через `SendAction`).

## События (865)

| `type` | поля | когда |
|---|---|---|
| `P2P_ESTABLISHED` | `session_id`, `path` (`direct`/`relay`) | handshake прошёл, поток разговора готов |
| `P2P_MESSAGE` | `session_id`, `payload` | сообщение собеседника |
| `P2P_CLOSED` | `session_id`, `reason` | ровно один раз на сессию |

`reason`: `local` — закрыл вызывающий (864 или закрытие инстанса);
`remote` — собеседник закрыл штатно; `failed` — согласование, handshake
или канал отказали, включая исчезновение собеседника без закрытия;
`slow_consumer` — очередь событий (1024) переполнена: сообщения молча не
теряются, сессия закрывается.

События — опросом, а не колбэком `Photon_SetEventCallback`: колбэк
вызывает ровно один поток — насос подписки Bridge (ADR-045), и на этом
держатся реентерабельный `Photon_Destroy` и определение потока колбэка.

# Служебные потоки P2P-сессии: 866–868

**Статус: реализовано в рабочей ветке; запись не принята** (проект записи о
служебных потоках к ADR-075 §2.3). Сессия может нести несколько разговоров,
каждый — в своём bidi-потоке. **Поток по умолчанию (863, `P2P_MESSAGE`) не
меняется ни на байт**: и старый, и новый узел говорят по нему как раньше.

Новый узел объявляет поддержку одним uni-потоком (кадр
`photon-streams/<N>`); старый uni-потоки не принимает, поэтому не видит
объявления вовсе. Служебный поток открывается только после объявления
собеседника — к старому узлу он не уходит никогда, и `866` отвечает
`peer_unsupported` не позже чем через `AnnounceWait` (3 с). Подробности
протокола — `services/p2phost/streams.go`.

| код | payload | data |
|---|---|---|
| 866 `P2P_STREAM_OPEN` | `{session_id, service}` | `{stream_id}` |
| 867 `P2P_STREAM_SEND` | `{stream_id, payload}` (≤ 64 KiB) | `{}` |
| 868 `P2P_STREAM_CLOSE` | `{stream_id}` | `{}` |

`service` — 1–64 символа `[a-z0-9._:/-]`, первый — буква (`photon.call/1`,
`app:<extension>/<name>`); Core проверяет только синтаксис. Вердикты:
`not_found` (чужой и несуществующий неотличимы), `not_ready`,
`peer_unsupported`, `stream_refused`, `too_many_streams` (16 на сессию в обе
стороны вместе), `invalid_service`, `payload_too_large`, `failed`.

| событие 865 | поля | когда |
|---|---|---|
| `P2P_STREAMS_READY` | `session_id` | собеседник объявил служебные потоки |
| `P2P_STREAM_OPENED` | `session_id`, `stream_id`, `service` | собеседник открыл поток |
| `P2P_STREAM_MESSAGE` | `session_id`, `stream_id`, `payload` | сообщение потока |
| `P2P_STREAM_CLOSED` | `session_id`, `stream_id`, `reason` | `local`, `remote`, `failed`, `slow_consumer` |

Закрытие потока сессию не закрывает. Переполнение очереди сообщением потока
закрывает **этот поток** (`slow_consumer`). При закрытии сессии её потоки
снимаются без отдельных событий у закрывшего; у собеседника конец потоков
может проявиться `P2P_STREAM_CLOSED` **до** `P2P_CLOSED` (на уровне потока он
неотличим от закрытия потока), но **после** `P2P_CLOSED` событий о её потоках
нет — по построению.

# Цель подписки: 814 (ADR-123 §2.3)

`ACTION_LOCAL_EVENTS_TARGET` — задать, заменить или снять endpoint и токен
QUIC-подписки работающего инстанса. Полный контракт —
`photon-quic-bridge/docs/conversation-push.md` §4.

| payload | ответ |
|---|---|
| `{"endpoint":"host:port","session_token":"…"}` | `{"target":"set"}` |
| `{}` или оба поля пустые | `{"target":"cleared"}` |
| одно поле без другого; endpoint не `host:port`; не JSON-объект нужной формы | отказ вызова `PHOTON_ERR_INVALID_ARGUMENT` |
| после `Photon_Destroy` | отказ вызова |

- Хранилища и секции `bridge` не требует. Action-соединения не создаёт.
- Колбэк, установленный до первой цели, запоминается; первая цель запускает
  насос. Смена цели прерывает подписку и паузу восстановления, новая цель
  подключается без паузы; одно событие, уже принятое старой подпиской, может
  быть доставлено после возврата 814 (вызов допустим из колбэка события,
  поэтому ждать насос он не может). Снятие цели закрывает подписку; насос ждёт
  новой цели без переподключений.
- Токен не попадает ни в ответ, ни в журналы.

## 815 LOCAL_MEDIA_UPLOAD_FILE и 816 LOCAL_MEDIA_DOWNLOAD_FILE (ADR-124 M1, ПРЕДЛОЖЕНО)

Передача файла вложения разговора ENGRAM через Core (ADR-124 §2.13, профиль M1
§7). Байты идут по **медиасоединению** — отдельному action-соединению QUIC к
цели, заданной 814; подписка событий и HTTP-запросы ENGRAM им не затрагиваются.
Контракт Bridge — `photon-quic-bridge/docs/conversation-implementation-contract.md`
§8 (703/704). Использование цели 814 для медиа — поправка ADR-123 §2.3.1,
предложенная ADR-124; до его принятия действия существуют только на ветке M1.

| действие | payload | успех (`PHOTON_OK`, конверт `ok:true`) |
|---|---|---|
| 815 | `{"conversation_id","upload_key","path","name","kind":"file"}` | `{"media_id","sha256","size"}` |
| 816 | `{"media_id","path"}` | `{"media_id","kind","mime","name","size","sha256"}` |

- **815.** Core открывает `path` (обычный файл, читаемый процессом), первым
  проходом вычисляет размер и SHA-256, вторым — передаёт ровно `size` байт
  действием 703. Файл в память целиком не читается. Если файл изменился между
  проходами, Bridge отвергает тело по длине или хэшу (`invalid_request`).
  Ответ Bridge сверяется с вычисленными `size` и `sha256`.
- **816.** Тело действия 704 пишется во временный файл `path + ".part"` в том
  же каталоге; после полного тела, совпадения длины с `body_length` и SHA-256
  с объявленным файл синхронизируется на диск и атомарно переименовывается в
  `path`. При любом отказе временный файл удаляется, `path` не создаётся и не
  меняется.
- **Вердикт Bridge** (`forbidden`, `invalid_request`, `idempotency_conflict`,
  `unavailable`, `temporarily_unavailable`, коды допуска 930) — конверт
  `{"ok":false,"error":…}` при `PHOTON_OK` (ADR-038), как у удалённых действий.
- **Отказ вызова:** неверный payload, пустые поля, `kind ≠ "file"`, файл не
  открывается, путь назначения недоступен — `PHOTON_ERR_INVALID_ARGUMENT`;
  цели 814 нет — `PHOTON_ERR_NOT_CONNECTED`; обрыв соединения, недобор
  длины, несовпадение SHA-256 у принятого тела — класс сети или внутренний
  отказ по таблице `errors.go`. Прерванная передача никогда не сообщает успех.
- **Медиасоединение** создаётся лениво первым вызовом, переиспользуется,
  закрывается при смене или снятии цели 814 и при `Photon_Destroy`; после
  обрыва следующий вызов подключается заново. Повтор после обрыва безопасен:
  815 — тем же `upload_key` (Bridge вернёт тот же объект), 816 — с первого
  байта.
- Хранилища не требуют. События прогресса и действия отмены в M1 нет (M3).

## 817 LOCAL_PROFILE_MEDIA_UPLOAD_FILE и 818 LOCAL_PROFILE_MEDIA_DOWNLOAD_FILE (ADR-127, ПРЕДЛОЖЕНО)

Передача файла галереи профиля ENGRAM через Core — те же медиасоединение,
два прохода по файлу, проверка длины и SHA-256 и атомарная запись, что у
815/816; действия Bridge — 706/707 (`conversation-implementation-contract.md`
§13). Общий код с 815/816 — `uploadFile`/`downloadFile` диспетчера и
`uploadStream`/`downloadStream` клиента `services/media`.

| действие | payload | успех (`PHOTON_OK`, конверт `ok:true`) |
|---|---|---|
| 817 | `{"upload_key","path","role":"original"\|"thumb","mime"}` | `{"media_id","sha256","size"}` |
| 818 | `{"media_id","path"}` | `{"media_id","role","mime","size","sha256"}` |

- Пустые поля, иная роль, файл не открывается, путь назначения недоступен —
  `PHOTON_ERR_INVALID_ARGUMENT`; вердикт Bridge (`forbidden`,
  `profile_hidden`, `gallery_limit`, `invalid_request`, `unavailable`, …) —
  в конверте при `PHOTON_OK`, как у 815/816.

## 819 LOCAL_DIAG_SNAPSHOT и 820 LOCAL_DIAG_EVENTS (ADR-130 §2.4, §2.7, §2.8)

Встроенная диагностика инстанса. **Написано до кода** (2026-10-23) по
принятому ADR-130 ред. 2.1. Схема записей и снимка — `services/diag`.

| действие | payload | успех (`PHOTON_OK`, конверт `ok:true`) |
|---|---|---|
| 819 | `{}` | снимок: `instance_id`, `instance_start_wall_ms`, `uptime_ms`, `connections` (`action`/`subscribe`/`media`: `state`, `in_state_ms`, `connects`, `connect_errors`, `reconnects`, `lost`), `calls`, `events`, `journal`, `storage`, `config` |
| 820 | `{"after_seq": n, "limit": m}` | `{"instance_id", "instance_start_wall_ms", "events": [...], "next_seq", "dropped"}` |

- Запись `events[]`: `{"seq","at_mono_ms","component","kind","conn"?,"fields":{…}}`
  — имена, не числа; поля — закрытый перечень ключей ADR-130 §2.3.
- **Время** (ADR-130 §2.3, поправка 2026-10-23): `at_mono_ms` — монотонное
  время события от старта инстанса, мс; не зависит от перевода системных
  часов. `instance_start_wall_ms` — календарное Unix-время старта инстанса,
  мс, снятое один раз при создании. Календарного времени события в ответе
  НЕТ: его можно лишь оценить как `instance_start_wall_ms + at_mono_ms` и
  показывать как оценку — после перевода системных часов оценка расходится с
  настоящим временем. Порядок событий задаёт `seq`. Длительности (`dur_us`,
  `wait_us`, `exec_us`, `commit_us`) — монотонные.
- 820: `after_seq` — 0 или `next_seq` прошлого ответа; `limit` — 1…512,
  обязателен. Отдаются записи с `seq > after_seq` по порядку. `next_seq` —
  последний отданный или `after_seq`, если отдавать нечего. `dropped` —
  сколько записей в `(after_seq, первый отданный)` уже вытеснено; отличен от
  нуля только вместе с записями. Повтор запроса журнал не меняет.
- Новый `instance_id` у читателя — перезапуск инстанса: читать заново с
  `after_seq = 0`.
- Отказы вызова (ADR-065 §8.2 — не `{ok:false}`):
  | причина | класс | код ABI |
  |---|---|---|
  | payload не JSON; `limit` вне 1…512; `after_seq` больше последнего выданного номера (`invalid_cursor`) | `ClassInvalidArgument` | `PHOTON_ERR_INVALID_ARGUMENT` |
- **Хранилище и Bridge не нужны**: оба действия исполняются на инстансе
  без `storage` и без `bridge`.
- **Диагностика выключена** (`diagnostics.enabled: false`): 819 — снимок с
  `config.enabled = false` и пустыми разделами; 820 — пустая страница,
  `instance_id = ""`, `instance_start_wall_ms = 0`, `next_seq = after_seq` (курсор при этом не
  проверяется — журнала нет). Отказом это не считается: клиент отличает
  «выключено» по `config.enabled`.
- **Чтение диагностики в журнал не пишется и в счётчики вызовов не входит**
  (ADR-130 §2.8): 819/820 не дают ни `call_failed`, ни `call_slow`, ни
  строки в `calls`.
- **Локальность — два рубежа.** Диспетчер исполняет их сам, до сессии;
  `session.Do`/`DoStream` отвергают всю сотню 800–899 (`ErrLocalAction`).
  Bridge отвечает отказом (`notServedHere`, ADR-064 §2).
- Содержимое — никогда: payload вызовов, тексты сообщений, токен, PhotonID,
  ключи, пути к файлам (ADR-130 §2.8).
