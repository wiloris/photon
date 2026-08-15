# photon-bindings/c

Реализация `photon_core.h` (замороженный ABI v1 — ADR-020/026/027/028)
через cgo, `-buildmode=c-shared`.

## Файлы

| Файл | Назначение |
|---|---|
| `photon_core.h` | **Публичный ABI.** Замороженный контракт, единственное, что видят потребители `.so` |
| `photon.go` | Реализация всех 12 экспортируемых функций |
| `internal_cgo_types.h` | Непубличный. Объявления для cgo без `const` — см. комментарий в файле |
| `testsupport.go` | Go-обёртки над C-типами (Go не поддерживает cgo в `_test.go`) |
| `photon_test.go` | 32 теста контракта: lifecycle, ownership, action-коды, ABI version |
| `leakcheck_handles.c` | Leak-check: 400k циклов create/destroy, различает арены Go от утечки |
| `leakcheck_strings.c` | Leak-check: 550k перезаписей `lastError` |
| `go.mod.example` | Пример go.mod (пути `replace` — из среды разработки, заменить на свои) |

## Сборка

```sh
go build -buildmode=c-shared -o photon_core.so .
```

## Тесты

```sh
go test .          # 32 теста контракта
go test -race .    # race detector

# leak-check (после сборки .so)
gcc -o leakcheck_handles leakcheck_handles.c -L. -l:photon_core.so -Wl,-rpath,. && ./leakcheck_handles
gcc -o leakcheck_strings leakcheck_strings.c -L. -l:photon_core.so -Wl,-rpath,. && ./leakcheck_strings
```

## Результаты проверки (Go 1.22.2, linux/amd64)

- `go build -buildmode=c-shared` — OK, 12 символов `Photon_*` экспортированы
- `go test .` — **32/32 PASS**
- `go test -race .` — PASS
- `go vet` — чисто (0 предупреждений)
- `gofmt -l` — чисто
- Leak-check handles: рост RSS выравнивается (1036 → 4 → 8 → −384 КБ за интервал) — арены Go-рантайма, не утечка
- Leak-check strings: 64 КБ на 500k перезаписей `lastError` — фактически ноль

## Найденный и исправленный баг

Первый прогон тестов поймал **double free** в `Photon_Destroy` при повторном
вызове на уже уничтоженном handle. Причина: `cgo.Handle` переиспользует
освобождённые слоты, поэтому повторный `resolveOpaqueHandle` мог зарезолвить
старое значение в **чужой** объект и освободить чужую память.

Исправление: `releaseOpaqueHandle` зануляет ячейку C-памяти **до** `C.free`,
а `resolveOpaqueHandle` трактует 0 как sentinel "уже освобождён"
(`cgo.Handle` нумеруется с 1, поэтому 0 безопасен как маркер).

## Ограничение: Valgrind

Valgrind в среде разработки недоступен, поэтому leak-check выполнен через
измерение RSS на большом числе итераций с разделением одноразового роста
(арены рантайма) и линейного (утечка). Прогон под Valgrind/ASan на машине,
где они доступны, остаётся желательным дополнительным шагом.
