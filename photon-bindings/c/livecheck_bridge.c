//go:build ignore

/* Сквозной прогон границы ABI против ЖИВОГО Bridge (Phase 13.5).

   Отдельная программа по той же причине, что leakcheck_*.c: своя main, и без
   ограничения сборки cgo подхватил бы её вместе с пакетом.

   Зачем она нужна отдельно от photon-core/tests/livecheck: тот прогон — Go,
   вызывающий Go, и границу C он не пересекает. Здесь проверяется именно то,
   что получит настоящий клиент: код возврата PhotonResult и строка
   response_json из .so.

   Главное проверяемое свойство — ADR-038: отказ сервера приходит с PHOTON_OK
   и ok:false ВНУТРИ конверта. Соблазн вернуть на ok:false ненулевой код
   существует ровно до того момента, как окажется, что различить «вызов не
   состоялся» и «сервер отказал» станет нечем.

   Сборка и запуск — см. README.md.
*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "photon_core.h"

#define ACTION_GET_ME         200
#define ACTION_GET_MY_CHANNELS 302
#define ACTION_UNKNOWN     424242

static int failures = 0;

static void ok(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void bad(const char *fmt, ...) __attribute__((format(printf, 1, 2)));

#include <stdarg.h>
static void ok(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    printf("  OK   "); vprintf(fmt, ap); printf("\n");
    va_end(ap);
}
static void bad(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    failures++;
    printf("  FAIL "); vprintf(fmt, ap); printf("\n");
    va_end(ap);
}

static const char *result_name(PhotonResult r) {
    switch (r) {
    case PHOTON_OK:                    return "PHOTON_OK";
    case PHOTON_ERR_TIMEOUT:           return "PHOTON_ERR_TIMEOUT";
    case PHOTON_ERR_INVALID_ARGUMENT:  return "PHOTON_ERR_INVALID_ARGUMENT";
    case PHOTON_ERR_PERMISSION_DENIED: return "PHOTON_ERR_PERMISSION_DENIED";
    case PHOTON_ERR_NOT_FOUND:         return "PHOTON_ERR_NOT_FOUND";
    case PHOTON_ERR_NOT_CONNECTED:     return "PHOTON_ERR_NOT_CONNECTED";
    case PHOTON_ERR_ALREADY_EXISTS:    return "PHOTON_ERR_ALREADY_EXISTS";
    case PHOTON_ERR_INTERNAL:          return "PHOTON_ERR_INTERNAL";
    case PHOTON_ERR_ABI_MISMATCH:      return "PHOTON_ERR_ABI_MISMATCH";
    case PHOTON_ERR_INVALID_ACTION:    return "PHOTON_ERR_INVALID_ACTION";
    }
    return "неизвестный PhotonResult";
}

/* Обрезка для печати: ответы Mattermost — сотни байт. */
static void print_head(const char *s, int n) {
    printf("       %.*s%s\n", n, s, (int)strlen(s) > n ? "…" : "");
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "использование: %s <endpoint host:port> <session id>\n", argv[0]);
        return 2;
    }

    char config[1024];
    snprintf(config, sizeof config,
             "{\"bridge\":{\"endpoint\":\"%s\",\"session_token\":\"%s\"}}",
             argv[1], argv[2]);

    PhotonHandle *h = NULL;
    PhotonResult r = Photon_Create(config, &h);
    if (r != PHOTON_OK || h == NULL) {
        fprintf(stderr, "  FAIL Photon_Create: %s\n", result_name(r));
        return 1;
    }
    ok("Photon_Create — инстанс с адресом Bridge создан, сети не касался");

    /* 1. Успешный вызов: PHOTON_OK и ok:true в конверте. */
    PhotonString *resp = NULL;
    r = Photon_SendAction(h, ACTION_GET_ME, "{}", &resp);
    if (r != PHOTON_OK) {
        bad("getMe: %s (%s)", result_name(r), Photon_GetLastError(h));
    } else if (strstr(Photon_StringData(resp), "\"ok\":true") == NULL) {
        bad("getMe: PHOTON_OK, но в конверте нет ok:true");
        print_head(Photon_StringData(resp), 160);
    } else {
        ok("getMe: PHOTON_OK, ok:true, %zu байт", Photon_StringLen(resp));
        print_head(Photon_StringData(resp), 120);
    }
    Photon_FreeString(resp);
    resp = NULL;

    /* 2. ГЛАВНОЕ (ADR-038): отказ сервера — это PHOTON_OK и ok:false внутри.
       getMyChannels без team_id — настоящий отказ настоящего Bridge. */
    r = Photon_SendAction(h, ACTION_GET_MY_CHANNELS, "{}", &resp);
    if (r != PHOTON_OK) {
        bad("отказ сервера пришёл кодом %s, а обязан конвертом с PHOTON_OK (ADR-038): %s",
            result_name(r), Photon_GetLastError(h));
    } else if (resp == NULL) {
        bad("PHOTON_OK, но response_out не заполнен");
    } else if (strstr(Photon_StringData(resp), "\"ok\":false") == NULL) {
        bad("PHOTON_OK, но ok:false в конверте нет — проверка не состоялась");
        print_head(Photon_StringData(resp), 160);
    } else {
        ok("отказ сервера: PHOTON_OK + ok:false в конверте (ADR-038)");
        print_head(Photon_StringData(resp), 160);
    }
    Photon_FreeString(resp);
    resp = NULL;

    /* 3. Неизвестный код — отдельный код возврата, не INVALID_ARGUMENT. */
    r = Photon_SendAction(h, ACTION_UNKNOWN, "{}", &resp);
    if (r != PHOTON_ERR_INVALID_ACTION) {
        bad("неизвестный код дал %s, ожидался PHOTON_ERR_INVALID_ACTION", result_name(r));
    } else if (resp != NULL) {
        bad("response_out заполнен на пути ошибки — нарушение photon_core.h");
    } else {
        ok("неизвестный код: PHOTON_ERR_INVALID_ACTION, response_out не тронут");
    }

    Photon_Destroy(h);
    ok("Photon_Destroy вернул управление");

    /* 4. Инстанс без секции bridge законен, но адресата у него нет:
       PHOTON_ERR_NOT_CONNECTED, и это не то же самое, что ошибка аргументов. */
    PhotonHandle *h2 = NULL;
    r = Photon_Create("{}", &h2);
    if (r != PHOTON_OK) {
        bad("Photon_Create(\"{}\") дал %s — инстанс без Bridge законен", result_name(r));
    } else {
        r = Photon_SendAction(h2, ACTION_GET_ME, "{}", &resp);
        if (r != PHOTON_ERR_NOT_CONNECTED) {
            bad("инстанс без Bridge: %s, ожидался PHOTON_ERR_NOT_CONNECTED", result_name(r));
        } else {
            ok("инстанс без Bridge: PHOTON_ERR_NOT_CONNECTED (%s)", Photon_GetLastError(h2));
        }
        Photon_Destroy(h2);
    }

    if (failures > 0) {
        printf("\n  ИТОГ: нарушенных ожиданий — %d\n", failures);
        return 1;
    }
    printf("\n  ИТОГ: все ожидания выполнены\n");
    return 0;
}
