/*
 * internal_cgo_types.h
 *
 * НЕ публичный ABI. Используется только внутри реализации (photon.go)
 * для cgo-генерации Go<->C сигнатур.
 *
 * Причина существования: cgo для экспортируемых Go-функций (//export)
 * всегда генерирует C-объявления без `const` на параметрах-указателях —
 * у Go-типа *C.char нет понятия const. Если в этой же единице
 * трансляции одновременно подключить photon_core.h (где параметры
 * объявлены как `const char*`, `const PhotonBytes*` и т.п.) и
 * автоматически сгенерированные cgo-объявления (`char*`, `PhotonBytes*`
 * без const) — компилятор C считает это конфликтующими повторными
 * объявлениями одной функции (различие в квалификации верхнего уровня
 * параметра — ошибка переобъявления в C).
 *
 * Это не расхождение ABI: `const` — compile-time аннотация, стирается в
 * объектном коде; экспортируемый символ и calling convention идентичны
 * что с const, что без. Поэтому решение — не менять замороженный
 * (ADR-020/026/027/028) внешний photon_core.h, а дать cgo-стороне
 * собственный, непубличный набор объявлений с той же семантикой, без
 * const. Внешние потребители .so видят только photon_core.h и
 * слинкованный бинарник — никогда этот файл.
 */

#ifndef PHOTON_INTERNAL_CGO_TYPES_H
#define PHOTON_INTERNAL_CGO_TYPES_H

#include <stddef.h>
#include <stdint.h>

typedef struct PhotonHandle PhotonHandle;
typedef struct PhotonString PhotonString;
typedef struct PhotonBytes  PhotonBytes;

/* Значения обязаны буквально совпадать с photon_core.h — источник
 * истины для самих значений остаётся там, это только повторное
 * объявление типа для cgo. */
typedef enum {
    PHOTON_OK = 0,
    PHOTON_ERR_TIMEOUT,
    PHOTON_ERR_INVALID_ARGUMENT,
    PHOTON_ERR_PERMISSION_DENIED,
    PHOTON_ERR_NOT_FOUND,
    PHOTON_ERR_NOT_CONNECTED,
    PHOTON_ERR_ALREADY_EXISTS,
    PHOTON_ERR_INTERNAL,
    PHOTON_ERR_ABI_MISMATCH,
    PHOTON_ERR_INVALID_ACTION,
} PhotonResult;

#define PHOTON_ABI_MAJOR 1
#define PHOTON_ABI_MINOR 0

typedef void (*PhotonEventCallback)(const char* event_json, void* user_data);

#endif /* PHOTON_INTERNAL_CGO_TYPES_H */
