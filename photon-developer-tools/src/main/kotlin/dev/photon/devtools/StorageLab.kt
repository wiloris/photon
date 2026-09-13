package dev.photon.devtools

import dev.photon.core.Photon
import java.util.Locale

/*
 * Storage Lab — объём Phase 16, три выполнимых буллета.
 *
 * ЧТО ЭТО ИЗМЕРЯЕТ И ЧТО НЕТ — различие определяет весь режим.
 *
 * Через публичный путь (Kotlin → JNI → C ABI → Core → Bridge) наблюдаемы
 * ДОМЕННЫЕ действия и ничего сверх. Значит измеряется чтение ленты как его
 * видит приложение: страница, курсор, глубина, число вызовов, поведение при
 * возврате и догрузке.
 *
 * НЕ измеряется — и не может быть измерено без нового API:
 *
 *   - локальная лента `services/localfeed`. Разведка перед срезом: её
 *     импортируют РОВНО два инструмента внутри репозитория (cmd/feedslice,
 *     cmd/feedreconcile) и её собственные тесты. Ни dispatcher, ни
 *     photon-bindings/c о ней не знают. То есть на публичном пути локального
 *     хранилища НЕТ ВООБЩЕ: каждая страница — сетевой запрос;
 *   - сегменты и разрывы. Это понятия localfeed (ADR-052/053): утверждение о
 *     полноте живёт в локальном хранилище, которого на этом пути нет. Клиент
 *     наблюдает не «разрыв», а только то, что сервер вернул;
 *   - namespace, ключи, префиксы, ForEachPrefixFrom, key layout, транзакции
 *     bbolt — ports.Storage в C ABI не выведен (ROADMAP, Phase 16).
 *
 * Заводить ради этих замеров действие или диагностический API — прямо
 * запрещено постановкой фазы: инструмент обязан ходить тем же путём, что
 * приложения. Поэтому здесь они честно объявлены неизмеримыми, а не
 * подменены похожими числами.
 */

private fun f1(v: Double): String = String.format(Locale.ROOT, "%.1f", v)
private fun f2(v: Double): String = String.format(Locale.ROOT, "%.2f", v)

/** Страница ленты, как её видит клиент: порядок и число записей. */
data class FeedPage(val order: List<String>, val millis: Double, val outcome: Outcome)

private val orderBlock = Regex("\"order\"\\s*:\\s*\\[([^\\]]*)\\]")
private val quoted = Regex("\"([^\"]+)\"")

/**
 * Разбор `order` из ответа getFeed.
 *
 * Чистая функция намеренно: порядок записей — то единственное, на чём стоят
 * все выводы ниже (глубина, дубли, пропуски), и проверить его надо без сети.
 */
fun parseOrder(response: String): List<String> {
    val block = orderBlock.find(response)?.groupValues?.get(1) ?: return emptyList()
    return quoted.findAll(block).map { it.groupValues[1] }.toList()
}

/**
 * Дубли между страницами обхода.
 *
 * Курсор обязан исключать саму опорную запись (контракт getFeed: «курсор — id
 * записи, ОТ которой листаем; сама она в выдачу не входит»). Дубль означал бы,
 * что обход топчется, и цена обхода перестала бы быть тем, чем кажется.
 */
fun duplicatesAcross(pages: List<List<String>>): List<String> {
    val seen = HashSet<String>()
    val dup = LinkedHashSet<String>()
    pages.forEach { page -> page.forEach { if (!seen.add(it)) dup.add(it) } }
    return dup.toList()
}

/**
 * Следующий якорь догрузки: САМАЯ СВЕЖАЯ запись блока, то есть первая в
 * порядке выдачи.
 *
 * Это ловушка, на которой первый прогон этого режима и споткнулся. `after`
 * отдаёт записи, БЛИЖАЙШИЕ к якорю (измерено Phase 12), а `order` идёт от
 * свежих к старым — значит ближний к якорю край блока стоит ПОСЛЕДНИМ, а
 * дальний ПЕРВЫМ. Якорь, двигаемый по ближнему краю, продвигается на одну
 * запись за запрос: блок почти целиком повторяется.
 *
 * Измерено на живом Bridge, страница 5:
 *
 *   блок после якоря W        → [k5ix, wxdn, ehcd, tmwd, qrig]
 *   якорь = qrig (ближний)    → [iypi, k5ix, wxdn, ehcd, tmwd]  ← 4 повтора
 *   якорь = k5ix (дальний)    → [bkrp, 79fi, iixd, r4xf, iypi]  ← ни одного
 *
 * Отсюда: цикл сходится только при движении по дальнему краю.
 */
fun nextAnchor(order: List<String>): String? = order.firstOrNull()

/**
 * Отношение задержки на глубине к задержке первой страницы.
 *
 * Именно ОТНОШЕНИЕ, а не разность: абсолютные величины определяются сетью до
 * стенда и между машинами несравнимы, а рост с глубиной — свойство сервера.
 */
fun depthRatio(first: Double, deep: Double): Double = if (first <= 0.0) 0.0 else deep / first

/** Сводка серии задержек. Перцентили берутся у Latencies — второй реализации нет. */
private fun summary(samples: List<Double>): String {
    if (samples.isEmpty()) return "нет замеров"
    val l = Latencies(samples)
    return "min ${f1(l.percentile(0))} / p50 ${f1(l.percentile(50))} / " +
        "p95 ${f1(l.percentile(95))} / max ${f1(l.percentile(100))} мс"
}

class StorageLab(private val endpoint: String, private val token: String) {

    /** Счётчик СЕТЕВЫХ вызовов: на публичном пути каждый getFeed — запрос. */
    private var feedCalls = 0

    private fun config() =
        """{"bridge":{"endpoint":"$endpoint","session_token":"$token",""" +
            """"connect_timeout_ms":15000,"call_timeout_ms":30000}}"""

    private fun page(p: Photon, channel: String, limit: Int,
                     before: String? = null, after: String? = null): FeedPage {
        val cursor = when {
            before != null -> ""","before":"$before""""
            after != null -> ""","after":"$after""""
            else -> ""
        }
        feedCalls++
        val r = call(p, UsedActions.GET_FEED, """{"channel_id":"$channel","limit":$limit$cursor}""")
        return FeedPage(
            order = r.response?.let { parseOrder(it) } ?: emptyList(),
            millis = r.millis,
            outcome = r.outcome,
        )
    }

    fun run(channelArg: String?, seed: Int, limit: Int, depthPages: Int, returns: Int) {
        println("Storage Lab — чтение ленты через публичный путь")
        println("страница $limit, глубина $depthPages страниц, возвратов $returns\n")

        Photon.create(config()).use { p ->
            val channel = channelArg ?: selfChannel(p)
            if (channel == null) {
                System.err.println("канал не получен — дальше мерить нечего")
                return
            }
            println("канал: $channel\n")

            if (seed > 0) seedFeed(p, channel, seed)

            val firstPage = openChannel(p, channel, limit)
            if (firstPage.order.isEmpty()) {
                System.err.println("лента пуста: сеять нечего или -seed 0. Замеры глубины пропущены")
                return
            }

            val walk = depth(p, channel, limit, depthPages, firstPage)
            repeats(p, channel, limit, returns, firstPage.millis)
            catchUp(p, channel, limit)
            requests(walk)
            comparison()
            unmeasurable()
        }
    }

    // ── канал и заготовка ────────────────────────────────────────────────────

    private fun selfChannel(p: Photon): String? {
        val me = call(p, UsedActions.GET_ME, "{}")
        val id = Regex("\"id\"\\s*:\\s*\"([^\"]+)\"").find(me.response ?: "")?.groupValues?.get(1)
            ?: return null
        val ch = call(p, UsedActions.GET_OR_CREATE_DIRECT_CHANNEL, """{"target_user_id":"$id"}""")
        return Regex("\"id\"\\s*:\\s*\"([^\"]+)\"").find(ch.response ?: "")?.groupValues?.get(1)
    }

    /**
     * Заготовка длинной ленты.
     *
     * Посты создаются ТЕМ ЖЕ публичным действием: иначе лента была бы набита
     * мимо пути, который меряется, и числа относились бы к другому сценарию.
     */
    private fun seedFeed(p: Photon, channel: String, n: Int) {
        println("── заготовка ленты: $n постов ──────────────────")
        val started = System.nanoTime()
        var ok = 0
        repeat(n) { i ->
            val r = call(p, UsedActions.CREATE_POST,
                """{"channel_id":"$channel","message":"storage-lab seed #$i"}""")
            if (r.outcome == Outcome.OK) ok++
        }
        val secs = (System.nanoTime() - started) / 1e9
        println("  создано $ok из $n за ${f1(secs)} с (${f1(ok / secs)} постов/с)\n")
    }

    // ── 1. открытие канала ───────────────────────────────────────────────────

    private fun openChannel(p: Photon, channel: String, limit: Int): FeedPage {
        println("── открытие канала (страница без курсора) ──────")
        val page = openMeasured(p, channel, limit, 5)
        println("  задержка: ${summary(page.second)}")
        println("  записей на странице: ${page.first.order.size}\n")
        return page.first
    }

    private fun openMeasured(p: Photon, channel: String, limit: Int, times: Int): Pair<FeedPage, List<Double>> {
        var last = page(p, channel, limit)   // прогрев: первый вызов несёт установление соединения
        val samples = mutableListOf<Double>()
        repeat(times) {
            last = page(p, channel, limit)
            samples.add(last.millis)
        }
        return last to samples
    }

    // ── 2. глубина через before ──────────────────────────────────────────────

    private fun depth(p: Photon, channel: String, limit: Int,
                      pages: Int, first: FeedPage): List<FeedPage> {
        println("── глубина: обход назад курсором before ────────")
        val walk = mutableListOf(first)
        var cursor = first.order.lastOrNull()
        var n = 1
        while (n < pages && cursor != null) {
            val next = page(p, channel, limit, before = cursor)
            if (next.order.isEmpty()) {
                println("  лента кончилась на странице ${n + 1}")
                break
            }
            walk.add(next)
            cursor = next.order.lastOrNull()
            n++
        }

        // Повторные замеры на КАЖДОЙ глубине. Одиночный замер здесь ничего не
        // значит: первый прогон дал разброс 0.95–2.01 без всякой связи с
        // глубиной — двухмодальный шум сети, а не свойство сервера. Вывод
        // «растёт/не растёт» можно делать только по медиане повторов.
        val repeatsPerDepth = 3
        val cursors = listOf<String?>(null) + walk.dropLast(1).map { it.order.lastOrNull() }
        val medians = cursors.mapIndexed { i, cursor ->
            val samples = (1..repeatsPerDepth).map { page(p, channel, limit, before = cursor).millis }
            Latencies(samples).percentile(50)
        }
        medians.forEachIndexed { i, med ->
            println("  страница ${i + 1} (глубина ${i * limit}): медиана ${f1(med)} мс " +
                "по $repeatsPerDepth замерам, записей ${walk[i].order.size}, " +
                "отношение к первой ${f2(depthRatio(medians[0], med))}")
        }
        val half = medians.size / 2
        if (half >= 2) {
            val shallow = Latencies(medians.take(half)).percentile(50)
            val deep = Latencies(medians.drop(half)).percentile(50)
            println("  медиана мелких страниц ${f1(shallow)} мс, глубоких ${f1(deep)} мс, " +
                "отношение ${f2(depthRatio(shallow, deep))}")
        }

        val dup = duplicatesAcross(walk.map { it.order })
        if (dup.isEmpty()) {
            println("  дублей между страницами нет: курсор исключает опорную запись")
        } else {
            println("  ⚠️ ДУБЛИ между страницами: ${dup.size} — обход топчется")
        }
        println("  задержка по страницам (медианы глубин): ${summary(medians)}\n")
        return walk
    }

    // ── 3. повторные возвраты ────────────────────────────────────────────────

    private fun repeats(p: Photon, channel: String, limit: Int, times: Int, firstMs: Double) {
        if (times <= 0) return
        println("── повторные возвраты в канал ──────────────────")
        val samples = mutableListOf<Double>()
        repeat(times) { samples.add(page(p, channel, limit).millis) }
        println("  ${times} возвратов: ${summary(samples)}")
        println("  отношение p50 возврата к первому открытию: " +
            f2(depthRatio(firstMs, Latencies(samples).percentile(50))))
        println("  КАЖДЫЙ возврат — сетевой запрос: локального кеша на публичном пути нет\n")
    }

    // ── 4. догрузка после offline ────────────────────────────────────────────

    /**
     * Сценарий: клиент запомнил водяной знак, «ушёл», лента выросла, клиент
     * вернулся и добирает новое курсором after.
     *
     * Проверяется не только число запросов, но и СХОДИМОСТЬ: Phase 12
     * измерила, что after отдаёт записи, БЛИЖАЙШИЕ к якорю, — иначе цикл
     * закрытия дыры не сходился бы. Здесь то же свойство наблюдается
     * клиентским путём.
     */
    private fun catchUp(p: Photon, channel: String, limit: Int) {
        println("── догрузка после offline (курсор after) ───────")
        val head = page(p, channel, limit)
        val watermark = head.order.firstOrNull()
        if (watermark == null) {
            println("  водяной знак не получен — сценарий пропущен\n")
            return
        }

        val newCount = limit * 2 + 3   // заведомо больше страницы: догрузка обязана идти циклом
        println("  водяной знак: $watermark")
        println("  создаётся новых записей: $newCount")
        val created = mutableListOf<String>()
        repeat(newCount) { i ->
            val r = call(p, UsedActions.CREATE_POST,
                """{"channel_id":"$channel","message":"storage-lab catchup #$i"}""")
            Regex("\"id\"\\s*:\\s*\"([^\"]+)\"").find(r.response ?: "")
                ?.groupValues?.get(1)?.let { created.add(it) }
        }

        val callsBefore = feedCalls
        val fetched = LinkedHashSet<String>()
        var anchor = watermark
        var rounds = 0
        while (rounds < 20) {
            val pg = page(p, channel, limit, after = anchor)
            rounds++
            if (pg.order.isEmpty()) break
            fetched.addAll(pg.order)
            val next = nextAnchor(pg.order) ?: break
            if (next == anchor) break
            anchor = next
            if (pg.order.size < limit) break
        }
        val calls = feedCalls - callsBefore

        val missing = created.filterNot { fetched.contains(it) }
        println("  запросов на закрытие: $calls (страница $limit, новых $newCount)")
        println("  получено уникальных: ${fetched.size}")
        println("  из созданных не добрано: ${missing.size}")
        println(if (missing.isEmpty())
                    "  цикл СОШЁЛСЯ за $rounds раундов: якорь идёт по ДАЛЬНЕМУ краю блока"
                else "  ⚠️ цикл не сошёлся за $rounds раундов")
        println()
    }

    // ── 5. число сетевых запросов ────────────────────────────────────────────

    private fun requests(walk: List<FeedPage>) {
        println("── число сетевых запросов ──────────────────────")
        println("  запросов getFeed за прогон: $feedCalls")
        println("  страниц в обходе назад: ${walk.size}, записей отдано: ${walk.sumOf { it.order.size }}")
        println("  ровно ОДИН запрос на страницу — иначе и быть не может:")
        println("  локального кеша на публичном пути нет, повтор той же страницы")
        println("  стоит столько же, сколько первый показ\n")
    }

    // ── 6. сверка с эталонами Phase 12 ───────────────────────────────────────

    private fun comparison() {
        println("── сверка с эталонами Phase 12 ─────────────────")
        println("  СОПОСТАВИМО (обе величины — сетевые, getFeed через Bridge):")
        println("    Phase 12, второй срез: 3 запроса, 732 мс, в среднем 244 мс;")
        println("    первый (с установлением соединения) 577–615 мс, последующие 58–83 мс.")
        println("    Замеры выше — то же действие тем же путём, плюс JNI и C ABI (9 мкс, Performance).")
        println()
        println("  НЕ СОПОСТАВИМО, и подменять нельзя:")
        println("    «50 просмотренных / 0 выброшенных на любой глубине» и «8.7 мкс вместо 137 мс»")
        println("    относятся к ЛОКАЛЬНОЙ ленте localfeed поверх bbolt. Здесь её нет:")
        println("    сервер листает своим GetPostsBefore/After по своей базе.")
        println("    Сравнивать эти числа с сетевыми — сравнивать разные машины и разные хранилища.\n")
    }

    // ── 7. что через публичный путь измерить нельзя ──────────────────────────

    private fun unmeasurable() {
        println("── неизмеримое через публичный путь ────────────")
        println("  Ни один пункт ниже не закрывается новым действием или API — это")
        println("  запрещено постановкой фазы. Они объявлены, а не обойдены.\n")
        println("  1. Сегменты и разрывы локальной ленты (ADR-052/053). Понятие")
        println("     принадлежит services/localfeed; на публичном пути его нет —")
        println("     localfeed не импортируют ни dispatcher, ни photon-bindings/c.")
        println("  2. Просмотр namespace, ключей, значений, поиск по префиксу,")
        println("     ForEachPrefixFrom, key layout, транзакции bbolt: ports.Storage")
        println("     в C ABI не выведен, действий для него в каталоге нет.")
        println("  3. Разделение «сеть против работы сервера»: клиент видит только")
        println("     сумму. Та же оговорка, что у Performance.")
        println("  4. Стоимость страницы в просмотренных записях: сервер её не")
        println("     сообщает, а вывести из ответа нечем.")
    }
}
