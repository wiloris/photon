/* Отличаем одноразовый рост (арены Go-рантайма) от линейной утечки:
   меряем RSS после каждых 100k циклов. Утечка => каждый интервал даёт
   примерно одинаковый прирост. Арены => прирост падает почти до нуля. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "photon_core.h"

static long rss_kb(void) {
    FILE *f = fopen("/proc/self/status", "r");
    char line[256]; long kb = -1;
    while (fgets(line, sizeof line, f))
        if (strncmp(line, "VmRSS:", 6) == 0) { sscanf(line + 6, "%ld", &kb); break; }
    fclose(f);
    return kb;
}

static void cycle(int n) {
    for (int i = 0; i < n; i++) {
        PhotonHandle *h = NULL;
        Photon_Create("{}", &h);
        PhotonString *resp = NULL;
        Photon_SendAction(h, 999999, "{}", &resp);
        Photon_GetLastError(h);
        Photon_Destroy(h);
        Photon_Destroy(h);
    }
}

int main(void) {
    cycle(10000);
    long r0 = rss_kb();
    cycle(100000); long r1 = rss_kb();
    cycle(100000); long r2 = rss_kb();
    cycle(100000); long r3 = rss_kb();
    cycle(100000); long r4 = rss_kb();

    printf("baseline: %ld KB\n", r0);
    printf("+100k:    %ld KB  (delta %ld)\n", r1, r1 - r0);
    printf("+200k:    %ld KB  (delta %ld)\n", r2, r2 - r1);
    printf("+300k:    %ld KB  (delta %ld)\n", r3, r3 - r2);
    printf("+400k:    %ld KB  (delta %ld)\n", r4, r4 - r3);

    long last = r4 - r3;
    if (last > 512) { printf("VERDICT: LEAK (steady growth %ld KB per 100k)\n", last); return 1; }
    printf("VERDICT: no leak (growth flattens - Go runtime arenas, not a leak)\n");
    return 0;
}
