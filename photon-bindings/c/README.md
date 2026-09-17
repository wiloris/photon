# photon-bindings/c

Реализация `photon_core.h` (замороженный ABI v1 — ADR-020/026/027/028)
через cgo, `-buildmode=c-shared`.

## Файлы

| Файл | Назначение |
|---|---|
| `photon_core.h` | **Публичный ABI.** Замороженный контракт, единственное, что видят потребители `.so` |
| `photon.go` | Реализация всех 12 экспортируемых функций |
| `internal_cgo_types.h` | Непубличный. Объявления для cgo без `const` — см. комментарий в файле |
| `handle_registry.go` | Реестр живых handle: эпохи и отложенное освобождение ячеек (ADR-059). Без cgo |
| `typesafety_test.go` | Контракт ADR-061: разрушающая функция на handle чужого типа — no-op |
| `testsupport.go` | Go-обёртки над C-типами (Go не поддерживает cgo в `_test.go`) |
| `photon_test.go` | Тесты контракта: lifecycle, ownership, action-коды, ABI version |
| `events_test.go`, `fake_transport_test.go` | Доставка событий, реентерабельный и конкурентный `Destroy` |
| `handle_registry_test.go` | Модульные тесты реестра с контролируемым аллокатором (ADR-059) |
| `IMPLEMENTATION_LOG.md` | Почему решения приняты так; с ADR-059 |
| `leakcheck_handles.c` | Leak-check: 400k циклов `Create → SendAction → GetLastError → Destroy` (один `Destroy` на handle), различает арены Go от утечки |
| `leakcheck_strings.c` | Leak-check: 550k перезаписей `lastError` |
| `livecheck_bridge.c` | **Ручной** сквозной прогон через `.so` против живого Bridge — см. ниже |
| `go.mod` | `replace` на `../../photon-core`, путь относительный. Закоммичен: тесты идут из репозитория |

## Сборка

```sh
go build -buildmode=c-shared -o build/photon_core.so .
```

**Собирать только в отдельный каталог.** `-buildmode=c-shared` всегда
генерирует C-заголовок рядом с выходным файлом и **называет его по имени
`-o`**: `-o photon_core.so` даёт `photon_core.h`, `-o build/photon_core.so` —
`build/photon_core.h`. Отключить генерацию нечем. Поэтому команда
`go build -buildmode=c-shared -o photon_core.so .` **молча затирает
`photon_core.h`** — замороженный публичный контракт ABI (ADR-008) — сгенерённым
cgo заголовком. Сборка при этом завершается успешно, и потеря обнаруживается
только по `git diff`, а при невнимательном коммите не обнаруживается вовсе.

Так здесь и было записано до Phase 13.5: команда из этого README уничтожала
заголовок при первом же выполнении.

## Тесты

```sh
go test .          # 55 тестов
go test -race .    # race detector
go test -asan .    # AddressSanitizer (LeakSanitizer здесь не срабатывает — IMPLEMENTATION_LOG.md)

# leak-check (после сборки .so)
gcc -o build/leakcheck_handles leakcheck_handles.c -Lbuild -l:photon_core.so -Wl,-rpath,'$ORIGIN' && ./build/leakcheck_handles
gcc -o build/leakcheck_strings leakcheck_strings.c -Lbuild -l:photon_core.so -Wl,-rpath,'$ORIGIN' && ./build/leakcheck_strings
```

`//go:build ignore` в начале `leakcheck_*.c` и `livecheck_bridge.c` — не
украшение: без него cgo подхватывает их как часть пакета, и `go test` падает на
«повторном определении `main`». Именно из-за этого тесты пакета раньше не
запускались из репозитория.

## Сквозной прогон против живого Bridge

**Не автотест** — требует развёрнутого `photon-quic-bridge` и действующего
Session ID, поэтому запускается руками и в `go test` не участвует.

```sh
go build -buildmode=c-shared -o build/photon_core.so .
gcc -o build/livecheck_bridge livecheck_bridge.c -Lbuild -l:photon_core.so -Wl,-rpath,'$ORIGIN'
./build/livecheck_bridge <host:port> <Session ID>
```

Зачем отдельно от `photon-core/tests/livecheck`: тот прогон — Go, вызывающий
Go, границу C он не пересекает. Здесь проверяется то, что получит настоящий
клиент: код возврата `PhotonResult` и строка `response_json` из `.so`.

Проверяется: `Photon_Create` без обращения к сети; `getMe` → `PHOTON_OK` +
`ok:true`; **отказ живого Bridge → `PHOTON_OK` + `ok:false` в конверте**
(ADR-038 — главное свойство фазы); неизвестный код → `PHOTON_ERR_INVALID_ACTION`
с нетронутым `response_out`; инстанс без секции `bridge` →
`PHOTON_ERR_NOT_CONNECTED`. Любое нарушенное ожидание даёт выход с кодом 1.

`Session ID` — это `Session.Id`, а не токен из `/users/login`: имя поля
`session_token` на проводе историческое.

## Результаты проверки (Go 1.22.2, linux/amd64)

> Исторический прогон, до ADR-059. Актуальные числа — после ADR-061
> (2026-09-17): `go test .` — 55/55, `-race` чисто, `go test -asan` — чисто,
> оба leak-check без тренда. Числа прогона ADR-059 (48/48, 2026-09-14) и
> подробности обоих решений — `IMPLEMENTATION_LOG.md`.

- `go build -buildmode=c-shared` — OK, 12 символов `Photon_*` экспортированы
- `go test .` — **35/35 PASS** (было 32; +3 в Phase 13.5)
- `go test -race .` — PASS
- `go vet` — чисто (0 предупреждений)
- `gofmt -l` — чисто
- Leak-check handles: рост RSS выравнивается (164 → 240 → 24 → −160 КБ за интервал) — арены Go-рантайма, не утечка
- Leak-check strings: 212 КБ суммарно за 400k перезаписей `lastError`, интервалы без тренда (224 → 52 → −8 → −56 КБ)
- `livecheck_bridge` против живого Bridge (`138.124.91.123.sslip.io:4443`, Mattermost 9.11) — все ожидания выполнены

Оба leak-check'а проверены **мутацией**, а не только зелёным прогоном: с
убранным `C.free` в `setError` вердикт строк становится `LEAKS` — 4,8 МБ на
каждые 100k итераций, 19 280 КБ суммарно против порога 1024. Разница с чистой
сборкой (212 КБ, интервалы колеблются вокруг нуля) — два порядка, так что
критерий отличает утечку от шума с большим запасом.

Критерий строк переписан на интервальный в ходе этой проверки: прежний мерил
одно окно и после Phase 13.5 начал выдавать `LEAKS` при полном отсутствии
утечки — путь ошибки стал аллоцировать на стороне Go, и куча выходит на
устойчивый уровень не мгновенно. Измерено на 2 млн итераций: весь рост — в
первых 100k, дальше RSS колеблется в коридоре 14 872–15 528 КБ без тренда.

## Найденный и исправленный баг

> **Контракт handle — ADR-059 и ADR-061.** Handle действителен до снятия с
> учёта; наблюдаемая граница — возврат первого не-реентерабельного
> `Photon_Destroy`, дальше UB. Разрушающая функция, получившая handle чужого
> типа (`Destroy` на строке или байтах, `FreeString`/`FreeBytes` на чужом
> объекте), не делает ничего: объект остаётся действительным и освобождается
> своей функцией. Двойной `Free*` своего типа и вызов за окном остаются UB.
> Формулировки контракта — в `photon_core.h`.

> **Механизм ниже заменён ADR-059.** Повторный `Photon_Destroy` на уже
> уничтоженном handle больше не поддерживаемый сценарий, а UB: handle валиден
> до снятия с учёта, наблюдаемая граница — возврат первого не-реентерабельного
> `Destroy`. Метка `0` в ячейке читалась из освобождённой памяти (нашёл ASan);
> вместо неё — реестр живых handle, `handle_registry.go`. Раздел оставлен как
> история находки; подробности — `IMPLEMENTATION_LOG.md`.

Первый прогон тестов поймал **double free** в `Photon_Destroy` при повторном
вызове на уже уничтоженном handle. Причина: `cgo.Handle` переиспользует
освобождённые слоты, поэтому повторный `resolveOpaqueHandle` мог зарезолвить
старое значение в **чужой** объект и освободить чужую память.

Исправление: `releaseOpaqueHandle` зануляет ячейку C-памяти **до** `C.free`,
а `resolveOpaqueHandle` трактует 0 как sentinel "уже освобождён"
(`cgo.Handle` нумеруется с 1, поэтому 0 безопасен как маркер).

## Ограничение: Valgrind

> Пакет прогоняется под ASan (`go test -asan .`, чисто; впервые — с ADR-059,
> сейчас 55 тестов). LeakSanitizer при этом **не** срабатывает на завершении
> Go-процесса, поэтому утечки по-прежнему проверяются только RSS — см.
> `IMPLEMENTATION_LOG.md`.

Valgrind в среде разработки недоступен, поэтому leak-check выполнен через
измерение RSS на большом числе итераций с разделением одноразового роста
(арены рантайма) и линейного (утечка). Прогон под Valgrind/ASan на машине,
где они доступны, остаётся желательным дополнительным шагом.
