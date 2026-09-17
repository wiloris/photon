package dev.photon.devtools

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/*
 * Тесты Dashboard.
 *
 * Без native и без сети: часы и вывод подставляются, а исход вызова
 * передаётся готовым CallResult — сам вызов и его классификация проверены в
 * NetworkLabTest. Здесь проверяется то, что делает сам Dashboard: состояние
 * подписки по событиям, журнал и его формат, разбор версии ABI, конфиг.
 *
 * Жизненный цикл моста (колбэков после close нет, доставки по одной) — не
 * логика Dashboard: он проверен PhotonEventsTest и живым прогоном Reconnect Lab.
 */
class DashboardTest {

    /** Часы, выдающие t1, t2, … — видно, какая отметка куда попала. */
    private class Ticks {
        var n = 0
        fun next(): String = "t${++n}"
    }

    private fun dashboard(stamp: () -> String = { "12:00:00.000" }, out: MutableList<String> = mutableListOf()) =
        Dashboard(stamp = stamp, out = { out.add(it) })

    private fun result(outcome: Outcome, detail: String, millis: Double, response: String? = null) =
        CallResult(outcome, response, detail, (millis * 1_000_000).toLong())

    // ── состояние подписки ───────────────────────────────────────────────────

    @Test
    fun `до событий подписки нет, журналы пусты`() {
        val out = mutableListOf<String>()
        val d = dashboard(out = out)
        assertEquals("нет подписки", d.subscription)
        d.printActions()
        d.printEvents()
        assertTrue("  (пусто)" in out && "  (ни одного)" in out, "$out")
    }

    @Test
    fun `CONNECTED и NETWORK_CHANGED меняют состояние подписки`() {
        val d = dashboard()
        d.onEvent("""{"type":"CONNECTED"}""")
        assertEquals("CONNECTED (с 12:00:00.000)", d.subscription)
        d.onEvent("""{"type":"NETWORK_CHANGED","state":"disconnected"}""")
        assertEquals("NETWORK_CHANGED — связь потеряна (12:00:00.000)", d.subscription)
    }

    /** Ровно та последовательность, которую показал живой прогон Reconnect Lab. */
    @Test
    fun `CONNECTED после NETWORK_CHANGED возвращает состояние связи`() {
        val t = Ticks()
        val d = dashboard(stamp = t::next)
        d.onEvent("""{"type":"CONNECTED"}""")
        d.onEvent("""{"type":"NETWORK_CHANGED","state":"disconnected"}""")
        d.onEvent("""{"type":"CONNECTED"}""")
        assertTrue(d.subscription.startsWith("CONNECTED (с "), d.subscription)
        assertEquals(3, d.eventLog().size)
        assertEquals(listOf("CONNECTED", "NETWORK_CHANGED", "CONNECTED"),
            d.eventLog().map { it.split("  ")[1] })
    }

    /**
     * MESSAGE — событие, а не состояние связи. Неизвестный тип и битый JSON тоже
     * не должны ни сбрасывать, ни подменять состояние, но и не теряются.
     */
    @Test
    fun `MESSAGE, неизвестный тип и битый JSON не меняют состояние, но видны в журнале`() {
        val d = dashboard()
        d.onEvent("""{"type":"CONNECTED"}""")
        val connected = d.subscription
        d.onEvent("""{"type":"MESSAGE","data":{"event":"posted"}}""")
        d.onEvent("""{"type":"PLUGIN_EVENT"}""")
        d.onEvent("""не json""")
        assertEquals(connected, d.subscription)
        assertEquals(listOf("CONNECTED", "MESSAGE", "PLUGIN_EVENT", "?"),
            d.eventLog().map { it.split("  ")[1] })
    }

    @Test
    fun `пробелы в event_json не мешают разбору`() {
        val d = dashboard()
        d.onEvent("""{ "type" : "NETWORK_CHANGED" }""")
        assertTrue(d.subscription.startsWith("NETWORK_CHANGED"), d.subscription)
    }

    @Test
    fun `событие печатается строкой с молнией и попадает в журнал усечённым`() {
        val out = mutableListOf<String>()
        val d = dashboard(out = out)
        val long = """{"type":"MESSAGE","data":"${"x".repeat(300)}"}"""
        d.onEvent(long)
        assertEquals(listOf("  ⚡ 12:00:00.000  MESSAGE"), out)
        assertEquals("12:00:00.000  MESSAGE  ${long.take(160)}", d.eventLog().single())
    }

    // ── журнал действий ──────────────────────────────────────────────────────

    /**
     * Три исхода записываются как есть (ADR-038): вердикт ok:false — вызов
     * состоялся, это НЕ ошибка вызова.
     */
    @Test
    fun `журнал хранит три исхода раздельно и в порядке отправки`() {
        val d = dashboard()
        val body = """{"ok":true,"data":{}}"""
        assertEquals(body, d.record(200, "{}", result(Outcome.OK, "OK", 81.6, body)))
        d.record(308, "{}", result(Outcome.VERDICT_FALSE, "ВЕРДИКТ ok:false — нет поля", 90.2, """{"ok":false}"""))
        assertEquals(null, d.record(999999, "{}", result(Outcome.CALL_ERROR, "ОШИБКА ВЫЗОВА code=9", 0.4)))

        val log = d.actionLog()
        assertEquals(listOf(200, 308, 999999), log.map { it.action })
        assertEquals(listOf(Outcome.OK, Outcome.VERDICT_FALSE, Outcome.CALL_ERROR), log.map { it.kind })
        assertEquals(listOf(81L, 90L, 0L), log.map { it.millis })
    }

    /** Формат печати — часть поведения CLI; рефакторинг его не меняет. */
    @Test
    fun `журнал печатается в прежнем формате`() {
        val out = mutableListOf<String>()
        val d = dashboard(out = out)
        d.record(200, """{"k":"${"v".repeat(200)}"}""", result(Outcome.CALL_ERROR, "ОШИБКА ВЫЗОВА code=5 x", 13.7))
        d.printActions()
        assertEquals(listOf(
            "\n── журнал действий ───────────────────────────────",
            "  12:00:00.000  action=200  13 мс",
            "      payload: ${"""{"k":"${"v".repeat(200)}"}""".take(90)}",
            "      исход:   ОШИБКА ВЫЗОВА code=5 x",
        ), out)
    }

    // ── ABI и конфиг ─────────────────────────────────────────────────────────

    /** MINOR клиент вправе игнорировать (ADR-026, п. 4): совместимость решает MAJOR. */
    @Test
    fun `совместимость ABI решает только MAJOR`() {
        assertEquals(AbiInfo(1, 0, true), abiInfo((1 shl 16) or 0, 1))
        assertEquals(AbiInfo(1, 7, true), abiInfo((1 shl 16) or 7, 1))
        assertEquals(AbiInfo(2, 0, false), abiInfo(2 shl 16, 1))
        assertEquals(AbiInfo(0, 1, false), abiInfo(1, 1))
    }

    @Test
    fun `конфиг онлайн-режима несёт адрес и сессию в объекте bridge`() {
        val cfg = dashboardConfig("host.example:4443", "tok")
        assertTrue(cfg.startsWith("""{"bridge":{"""), cfg)
        assertTrue(cfg.contains(""""endpoint":"host.example:4443"""") && cfg.contains(""""session_token":"tok""""), cfg)
    }

    // ── структурные границы ──────────────────────────────────────────────────

    private fun src(name: String) = java.io.File(System.getProperty("photon.devtools.src"), name).readText()

    @Test
    fun `Dashboard классифицирует исход общим Outcome, а не своей копией`() {
        val dash = src("Dashboard.kt")
        assertTrue(dash.contains("call(p, action, payload)"), "Dashboard обязан звать общий call()")
        assertFalse(dash.contains("\"ok\\\"") || Regex("""catch\s*\(\s*\w+\s*:\s*PhotonException""").containsMatchIn(dash),
            "в Dashboard не должно быть своей классификации ok:false или ошибки вызова")
        assertFalse(src("Main.kt").contains("class Dashboard"), "Dashboard должен жить в одном месте")
    }

    /**
     * Разбор "type" живёт в одном месте. Копии расходятся молча: одна
     * лаборатория считала бы событие известным, соседняя — нет, и разница
     * выглядела бы дефектом ядра. Проверяется структурно — поведение четырёх
     * копий одинаково по определению, пока они одинаковы.
     */
    @Test
    fun `разбор type событий берётся из общего eventType`() {
        val srcDir = System.getProperty("photon.devtools.src")
        assertNotNull(srcDir, "-Dphoton.devtools.src задаёт задача Gradle")
        val offenders = java.io.File(srcDir).walkTopDown()
            .filter { it.isFile && it.extension == "kt" && it.name != "EventType.kt" }
            // Маркер копии — экранированный литерал поля в исходнике: Regex("\"type\"...
            .filter { it.readText().contains("type\\\"") }
            .map { it.name }.toList()
        assertTrue(offenders.isEmpty(), "своя копия разбора \"type\": $offenders")

        assertEquals("CONNECTED", eventType("""{"type":"CONNECTED"}"""))
        assertEquals("NETWORK_CHANGED", eventType("""{ "type" : "NETWORK_CHANGED" }"""))
        assertEquals(null, eventType("""{"x":1}"""))
    }

    @Test
    fun `коды действий Dashboard берутся из UsedActions`() {
        val sends = Regex("""dash\.send\(photon,\s*([^,]+),""").findAll(src("Main.kt")).map { it.groupValues[1].trim() }.toList()
        assertTrue(sends.isNotEmpty(), "вызовы Dashboard не найдены")
        assertTrue(sends.all { it.startsWith("UsedActions.") }, "код действия вписан числом: $sends")
    }
}
