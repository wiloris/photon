//go:build ignore

/* Живой прогон доставки событий через границу ABI (Phase 15.5).

   Отдельная программа по той же причине, что livecheck_bridge.c и
   leakcheck_*.c: своя main, и без ограничения сборки cgo подхватил бы её
   вместе с пакетом.

   Зачем отдельно от photon-core/tests/eventcheck: тот прогон — Go, вызывающий
   Go, и границу C он не пересекает. Здесь проверяется ровно то, что получит
   настоящий клиент, и ровно те свойства, которых нет по ту сторону границы:
   реентерабельные вызовы Photon_* из колбэка и уничтожение изнутри колбэка.

   Сборка и запуск — см. README.md.
*/

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include "photon_core.h"

#define ACTION_GET_ME 200

static int failures = 0;

static void ok(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    printf("  OK   "); vprintf(fmt, ap); printf("\n"); fflush(stdout);
    va_end(ap);
}
static void bad(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    failures++;
    printf("  FAIL "); vprintf(fmt, ap); printf("\n"); fflush(stdout);
    va_end(ap);
}
static void note(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    printf("  .    "); vprintf(fmt, ap); printf("\n"); fflush(stdout);
    va_end(ap);
}

/* ── Наблюдение ─────────────────────────────────────────────────────── */

static PhotonHandle *g_handle;
static int  g_events;
static int  g_connected;
static char g_first[512];

static int  g_reentrant_action_rc = -12345;
static int  g_reentrant_lasterr_done;
static int  g_destroy_returned;
static int  g_events_after_destroy;
static int  g_destroy_called;

static pthread_t g_cb_thread;
static int       g_cb_thread_set;
static int       g_cb_thread_stable = 1;

static void observe(const char *json) {
    if (g_destroy_called) { g_events_after_destroy++; return; }
    g_events++;
    if (g_events == 1 && json) {
        strncpy(g_first, json, sizeof(g_first) - 1);
        g_first[sizeof(g_first) - 1] = 0;
    }
    if (json && strstr(json, "\"CONNECTED\"")) g_connected++;

    /* Поток доставки обязан быть ОДНИМ И ТЕМ ЖЕ на всём жизненном цикле:
       на этом держится определение реентерабельного Destroy. */
    if (!g_cb_thread_set) { g_cb_thread = pthread_self(); g_cb_thread_set = 1; }
    else if (!pthread_equal(g_cb_thread, pthread_self())) g_cb_thread_stable = 0;
}

static void cb_plain(const char *json, void *ud) { (void)ud; observe(json); }

/* Пункт 7: колбэк зовёт SendAction и GetLastError на ЖИВОМ Bridge. */
static void cb_reentrant(const char *json, void *ud) {
    (void)ud;
    observe(json);
    if (g_reentrant_action_rc == -12345) {
        PhotonString *resp = NULL;
        g_reentrant_action_rc = (int)Photon_SendAction(g_handle, ACTION_GET_ME, "{}", &resp);
        if (resp) {
            if (strstr(Photon_StringData(resp), "\"ok\":true")) g_reentrant_lasterr_done |= 2;
            Photon_FreeString(resp);
        }
        (void)Photon_GetLastError(g_handle);
        g_reentrant_lasterr_done |= 1;
    }
}

/* Пункт 6: колбэк зовёт Photon_Destroy. */
static void cb_destroy(const char *json, void *ud) {
    (void)ud;
    observe(json);
    if (!g_destroy_called) {
        g_destroy_called = 1;
        Photon_Destroy(g_handle);
        g_destroy_returned = 1;
    }
}

static void reset(void) {
    g_handle = NULL; g_events = 0; g_connected = 0; g_first[0] = 0;
    g_reentrant_action_rc = -12345; g_reentrant_lasterr_done = 0;
    g_destroy_returned = 0; g_events_after_destroy = 0; g_destroy_called = 0;
    g_cb_thread_set = 0; g_cb_thread_stable = 1;
}

static PhotonHandle *make(const char *config) {
    PhotonHandle *h = NULL;
    PhotonResult r = Photon_Create(config, &h);
    if (r != PHOTON_OK) { bad("Photon_Create: %d", (int)r); return NULL; }
    return h;
}

/* Ждёт n событий не дольше sec секунд. */
static int wait_events(int n, int sec) {
    for (int i = 0; i < sec * 20; i++) {
        if (g_events >= n) return 1;
        usleep(50 * 1000);
    }
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <config_json>\n", argv[0]);
        return 2;
    }
    const char *config = argv[1];
    printf("\n=== livecheck_events (граница ABI, живой Bridge) ===\n");

    /* ── 1. CONNECTED доходит до C-колбэка ──────────────────────────── */
    printf("\n-- CONNECTED через C ABI --\n");
    reset();
    g_handle = make(config);
    if (!g_handle) return 1;
    Photon_SetEventCallback(g_handle, cb_plain, NULL);
    if (!wait_events(1, 30)) {
        bad("событие не пришло за 30 с");
    } else if (!strstr(g_first, "\"type\":\"CONNECTED\"")) {
        bad("первое событие не CONNECTED: %s", g_first);
    } else {
        ok("CONNECTED получен C-колбэком: %s", g_first);
    }
    Photon_Destroy(g_handle);

    /* ── 7. Колбэк зовёт SendAction и GetLastError ──────────────────── */
    printf("\n-- реентерабельные SendAction/GetLastError из колбэка --\n");
    reset();
    g_handle = make(config);
    if (!g_handle) return 1;
    Photon_SetEventCallback(g_handle, cb_reentrant, NULL);
    if (!wait_events(1, 30)) {
        bad("событие не пришло — реентерабельность проверить нечем");
    } else {
        for (int i = 0; i < 600 && g_reentrant_lasterr_done == 0; i++) usleep(50 * 1000);
        if (g_reentrant_action_rc == -12345) {
            bad("SendAction из колбэка не вернулся (дедлок)");
        } else if (g_reentrant_action_rc != PHOTON_OK) {
            bad("SendAction из колбэка вернул %d", g_reentrant_action_rc);
        } else if ((g_reentrant_lasterr_done & 2) == 0) {
            bad("SendAction из колбэка вернул не ok:true");
        } else if ((g_reentrant_lasterr_done & 1) == 0) {
            bad("GetLastError из колбэка не вернулся (дедлок)");
        } else {
            ok("SendAction из колбэка: PHOTON_OK, ok:true на живом Bridge");
            ok("GetLastError из колбэка вернулся");
        }
    }
    Photon_Destroy(g_handle);

    /* ── 6+9. Destroy изнутри колбэка, событий после — нет ──────────── */
    printf("\n-- Photon_Destroy из колбэка --\n");
    reset();
    g_handle = make(config);
    if (!g_handle) return 1;
    Photon_SetEventCallback(g_handle, cb_destroy, NULL);
    if (!wait_events(1, 30)) {
        bad("событие не пришло — Destroy из колбэка проверить нечем");
    } else {
        for (int i = 0; i < 200 && !g_destroy_returned; i++) usleep(50 * 1000);
        if (!g_destroy_returned) {
            bad("реентерабельный Photon_Destroy не вернулся (дедлок)");
        } else {
            ok("реентерабельный Photon_Destroy вернулся немедленно");
            sleep(5);
            if (g_events_after_destroy != 0)
                bad("после Destroy пришло %d событий", g_events_after_destroy);
            else
                ok("после Destroy колбэк не вызывался");
        }
    }

    /* ── 6. Destroy в ожидании push и повторный Destroy ─────────────── */
    printf("\n-- Destroy в ожидании push, затем повторный --\n");
    reset();
    g_handle = make(config);
    if (!g_handle) return 1;
    Photon_SetEventCallback(g_handle, cb_plain, NULL);
    if (!wait_events(1, 30)) {
        bad("подписка не поднялась");
    } else {
        sleep(2); /* насос стоит в ожидании push */
        g_destroy_called = 1;
        Photon_Destroy(g_handle);
        ok("Destroy в ожидании push вернулся");
        Photon_Destroy(g_handle);   /* повторный — обязан быть безопасным */
        ok("повторный Photon_Destroy безопасен");
        sleep(3);
        if (g_events_after_destroy != 0)
            bad("после Destroy пришло %d событий", g_events_after_destroy);
        else
            ok("после Destroy колбэк не вызывался");
    }

    /* ── стабильность потока доставки ──────────────────────────────── */
    printf("\n-- поток доставки --\n");
    if (!g_cb_thread_stable) bad("колбэк вызывался из РАЗНЫХ OS-потоков");
    else ok("колбэк всегда вызывался из одного OS-потока");

    printf("\n");
    if (failures) { printf("ИТОГ: нарушенных ожиданий - %d\n\n", failures); return 1; }
    printf("ИТОГ: все ожидания выполнены\n\n");
    return 0;
}
