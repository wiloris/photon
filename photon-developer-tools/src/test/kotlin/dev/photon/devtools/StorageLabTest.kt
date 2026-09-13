package dev.photon.devtools

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/*
 * Тесты чистых функций Storage Lab.
 *
 * Проверяется то, на чём стоят выводы прогона: разбор порядка записей,
 * обнаружение дублей обхода, отношение задержек — и структурные свойства,
 * которые прогон подтвердить не может, потому что зелёный отчёт на живом
 * сервере одинаково выглядит и когда измеряется не то.
 */
class StorageLabTest {

    // ── разбор порядка ───────────────────────────────────────────────────────

    @Test
    fun `порядок записей читается из ответа getFeed`() {
        val resp = """{"ok":true,"data":{"order":["p3","p2","p1"],""" +
            """"posts":{"p3":{"id":"p3"},"p2":{"id":"p2"},"p1":{"id":"p1"}}}}"""
        assertEquals(listOf("p3", "p2", "p1"), parseOrder(resp))
    }

    @Test
    fun `пустая лента даёт пустой порядок, а не выдуманные записи`() {
        assertEquals(emptyList(), parseOrder("""{"ok":true,"data":{"order":[],"posts":{}}}"""))
        assertEquals(emptyList(), parseOrder("""{"ok":false,"error":"forbidden"}"""))
        assertEquals(emptyList(), parseOrder("не json вовсе"))
    }

    /**
     * Ключевое: идентификаторы берутся ИЗ order, а не из posts.
     *
     * posts — карта без порядка; собрав идентификаторы из неё, обход получил бы
     * произвольную последовательность и «глубина» перестала бы быть глубиной.
     * Мутация «читать ключи posts» проходит предыдущий тест и падает здесь.
     */
    @Test
    fun `порядок берётся из order, а не из карты posts`() {
        val resp = """{"ok":true,"data":{"order":["b"],"posts":{"a":{"id":"a"},"b":{"id":"b"}}}}"""
        assertEquals(listOf("b"), parseOrder(resp),
            "в order одна запись — значит и на странице одна, сколько бы ни лежало в posts")
    }

    // ── дубли обхода ─────────────────────────────────────────────────────────

    @Test
    fun `обход без дублей признаётся чистым`() {
        val pages = listOf(listOf("p5", "p4"), listOf("p3", "p2"), listOf("p1"))
        assertTrue(duplicatesAcross(pages).isEmpty())
    }

    /**
     * Курсор обязан исключать опорную запись. Если сервер вернёт её снова,
     * обход будет топтаться, и «страниц на ленту» станет неверным числом.
     */
    @Test
    fun `повтор опорной записи на следующей странице виден как дубль`() {
        val pages = listOf(listOf("p5", "p4"), listOf("p4", "p3"))
        assertEquals(listOf("p4"), duplicatesAcross(pages))
    }

    @Test
    fun `дубль внутри одной страницы тоже виден`() {
        assertEquals(listOf("p1"), duplicatesAcross(listOf(listOf("p2", "p1", "p1"))))
    }

    // ── направление якоря догрузки ───────────────────────────────────────────

    /**
     * Регрессия на дефект ПЕРВОГО прогона этого режима.
     *
     * Петля брала якорем последний элемент блока — ближний к прежнему якорю, —
     * и продвигалась на одну запись за запрос: из 43 новых записей за 20
     * раундов добралось 39. Измерение на живом Bridge (страница 5) показало,
     * что при ближнем крае блок повторяется на 4 из 5 записей, а при дальнем
     * повторов нет вовсе.
     *
     * Мутация «вернуть last» роняет этот тест — и только его: отчёт о
     * догрузке на живом сервере выглядел бы просто «медленнее».
     */
    @Test
    fun `якорь догрузки идёт по дальнему краю блока`() {
        // order: от свежих к старым; ближний к прежнему якорю — последний.
        val block = listOf("самая-свежая", "средняя", "ближняя-к-якорю")
        assertEquals("самая-свежая", nextAnchor(block),
            "якорь по ближнему краю продвигает цикл на одну запись за запрос")
    }

    @Test
    fun `пустой блок не даёт якоря и останавливает цикл`() {
        assertEquals(null, nextAnchor(emptyList()))
    }

    // ── отношение задержек ───────────────────────────────────────────────────

    @Test
    fun `отношение глубины считается к первой странице`() {
        assertEquals(2.0, depthRatio(50.0, 100.0))
        assertEquals(1.0, depthRatio(80.0, 80.0))
    }

    /**
     * Нулевая или отрицательная опора не должна давать бесконечность: это
     * означало бы, что в отчёт попало число, которого измерение не давало.
     */
    @Test
    fun `нулевая опора не даёт бесконечности`() {
        assertEquals(0.0, depthRatio(0.0, 100.0))
    }

    // ── структурные свойства ─────────────────────────────────────────────────

    @Test
    fun `перцентили берутся из общей реализации Network Lab`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "StorageLab.kt").readText()
        assertTrue(src.contains("Latencies("),
            "Storage Lab обязан использовать Latencies, а не заводить свой расчёт")
        assertTrue(!src.contains("fun percentile("),
            "в Storage Lab не должно быть второй реализации перцентилей")
    }

    /**
     * Инструмент не имеет права дотянуться до внутренностей ядра мимо C ABI
     * (ROADMAP, Phase 16, «Public API constraint»). Для Storage Lab соблазн
     * самый сильный: локальная лента лежит в services/localfeed, и импорт
     * решил бы задачу «инспектора» одной строкой — превратив инструмент во
     * второе ядро.
     */
    @Test
    fun `Storage Lab не обходит публичный путь импортом внутренних пакетов`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "StorageLab.kt").readText()
        listOf("services/localfeed", "adapters/bbolt", "ports.Storage", "photon-core/services").forEach {
            assertTrue(!src.contains("import $it"), "прямого импорта $it быть не должно")
        }
        assertTrue(src.contains("dev.photon.core.Photon"),
            "единственный вход в платформу — публичный мост")
    }

    /**
     * Неизмеримое обязано быть объявлено, а не обойдено похожими числами.
     * Тест держит именно это свойство отчёта: без него ничто не мешает тихо
     * выдать серверную пагинацию за локальную ленту.
     */
    @Test
    fun `отчёт объявляет, чего через публичный путь измерить нельзя`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "StorageLab.kt").readText()
        assertTrue(src.contains("неизмеримое через публичный путь") ||
            src.contains("НЕ СОПОСТАВИМО"),
            "в отчёте обязан быть раздел о неизмеримом")
        assertTrue(src.contains("localfeed"),
            "отсутствие локальной ленты на публичном пути должно быть названо прямо")
        assertTrue(src.contains("НЕ СОПОСТАВИМО"),
            "сверка с Phase 12 обязана отделять сетевые величины от локальных")
    }

    /**
     * Коды действий берутся из общего перечня. Магическое число здесь
     * означало бы вторую копию каталога — ровно то, чего избегает Inspector.
     */
    @Test
    fun `Storage Lab шлёт действия из общего перечня, а не магические числа`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "StorageLab.kt").readText()
        assertTrue(src.contains("UsedActions.GET_FEED"))
        assertTrue(!Regex("""call\(p,\s*\d+""").containsMatchIn(src),
            "числовой код действия в вызове означает вторую копию каталога")
        assertEquals(101, UsedActions.GET_FEED)
        assertTrue(UsedActions.sentByTool.values.contains(UsedActions.GET_FEED),
            "действие обязано быть объявлено в перечне, который сверяется с каталогом")
    }
}
