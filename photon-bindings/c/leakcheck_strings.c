/* Проверка ownership PhotonString/PhotonBytes: если Photon_FreeString
   не освобождает кэшированную C-строку, рост будет линейным. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "photon_core.h"

/* Эти функции не в публичном ABI (создание строк — внутреннее дело
   Core), поэтому дергаем их косвенно: через повторные ошибочные вызовы,
   которые заполняют lastError, и через сам жизненный цикл handle. */

static long rss_kb(void) {
    FILE *f = fopen("/proc/self/status", "r");
    char line[256]; long kb = -1;
    while (fgets(line, sizeof line, f))
        if (strncmp(line, "VmRSS:", 6) == 0) { sscanf(line + 6, "%ld", &kb); break; }
    fclose(f);
    return kb;
}

int main(void) {
    /* Один handle, много ошибок подряд: каждая перезаписывает lastError.
       Если старая C-строка не освобождается — линейный рост. */
    PhotonHandle *h = NULL;
    if (Photon_Create("{}", &h) != PHOTON_OK) return 1;

    for (int i = 0; i < 50000; i++) {
        PhotonString *r = NULL;
        Photon_SendAction(h, 999999, "{}", &r);
    }
    long r0 = rss_kb();
    for (int i = 0; i < 500000; i++) {
        PhotonString *r = NULL;
        Photon_SendAction(h, 999999, "{}", &r);
    }
    long r1 = rss_kb();

    Photon_Destroy(h);

    printf("after 50k errors:   %ld KB\n", r0);
    printf("after 550k errors:  %ld KB  (delta %ld KB over 500k more)\n", r1, r1 - r0);
    if (r1 - r0 > 512) {
        printf("VERDICT: lastError string LEAKS\n");
        return 1;
    }
    printf("VERDICT: lastError properly freed on overwrite\n");
    return 0;
}
