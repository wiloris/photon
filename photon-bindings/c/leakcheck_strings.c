//go:build ignore

/* Отдельная программа, а не часть пакета: у неё своя main, и без этого
   ограничения сборки cgo подхватил бы её вместе с пакетом — линковщик тогда
   падает на «повторном определении main». Собирается вручную против готовой
   photon_core.so, см. README.md.
*/

/* Проверка ownership lastError: если setError не освобождает прежнюю
   C-строку, рост RSS будет линейным и не остановится.

   Критерий — интервальный, как в leakcheck_handles.c, и это исправление, а
   не стилистика. Прежний вариант мерил ОДНО окно (RSS на 50k против RSS на
   550k) и падал, если разница превышала 512 КБ. После Phase 13.5 путь ошибки
   стал аллоцировать на стороне Go (fmt.Errorf в диспетчере вместо
   константной строки-заглушки), куча вырастает до нового устойчивого уровня
   не мгновенно — и тест начал выдавать «VERDICT: LEAKS» на 1248 КБ при
   полном отсутствии утечки.

   Измерено на 2 млн итераций: весь рост приходится на первые 100k
   (9548 → ~15300 КБ), дальше RSS колеблется в коридоре 14872–15528 КБ без
   тренда, отдельные интервалы дают и +624, и −620 КБ. Отсюда два следствия,
   заложенные ниже: прогрев 100k, а не 50k, и порог по СУММАРНОМУ приросту за
   окно, а не по одному интервалу — колебание в 620 КБ иначе роняло бы тест
   случайно.

   Чувствительности это не стоит: настоящая утечка lastError — это ~40-60
   байт на итерацию, то есть 16-24 МБ за 400k, на четыре порядка больше
   порога. Проверено мутацией: с убранным C.free в setError вердикт
   становится LEAK. */
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

static void errors(int n) {
    /* Один handle на весь прогон создаётся в main; сюда он не передаётся —
       строку перезаписывает любой ошибочный вызов на нём. */
    extern PhotonHandle *g_h;
    for (int i = 0; i < n; i++) {
        PhotonString *r = NULL;
        Photon_SendAction(g_h, 999999, "{}", &r);
    }
}

PhotonHandle *g_h = NULL;

int main(void) {
    /* Один handle, много ошибок подряд: каждая перезаписывает lastError.
       Если старая C-строка не освобождается — линейный рост. */
    if (Photon_Create("{}", &g_h) != PHOTON_OK) return 1;

    errors(100000);            /* прогрев: куча выходит на устойчивый уровень */
    long r0 = rss_kb();
    errors(100000); long r1 = rss_kb();
    errors(100000); long r2 = rss_kb();
    errors(100000); long r3 = rss_kb();
    errors(100000); long r4 = rss_kb();

    Photon_Destroy(g_h);

    printf("после прогрева 100k: %ld KB\n", r0);
    printf("+100k:               %ld KB  (delta %ld)\n", r1, r1 - r0);
    printf("+200k:               %ld KB  (delta %ld)\n", r2, r2 - r1);
    printf("+300k:               %ld KB  (delta %ld)\n", r3, r3 - r2);
    printf("+400k:               %ld KB  (delta %ld)\n", r4, r4 - r3);

    long net = r4 - r0;
    printf("суммарно за 400k:    %ld KB\n", net);
    if (net > 1024) {
        printf("VERDICT: lastError string LEAKS\n");
        return 1;
    }
    printf("VERDICT: lastError properly freed on overwrite\n");
    return 0;
}
