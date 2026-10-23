package dev.photon.devtools

import dev.photon.sdk.DiagnosticEvent
import dev.photon.sdk.DiagnosticValue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/*
 * Панель «Путь» — чистые функции: что журнал подтверждает, то и показано;
 * чего нет — названо отсутствующим, а не достроено.
 */
class ConnectionPathTest {

    private fun ev(seq: Long, comp: String, kind: String, conn: String?, vararg f: Pair<String, Any>) =
        DiagnosticEvent(seq, seq * 10, comp, kind, conn, f.associate { (k, v) ->
            k to if (v is String) DiagnosticValue.Text(v) else DiagnosticValue.Num((v as Number).toLong())
        })

    private fun okDial(seq: Long, conn: String, transport: String?, family: String?, remote: String?) =
        ev(seq, "transport", "dial", conn, *listOfNotNull(
            "endpoint" to "bridge.example:4443", "dur_us" to 2100, "class" to "ok",
            transport?.let { "transport" to it }, family?.let { "family" to it }, remote?.let { "remote" to it },
        ).toTypedArray())

    private fun failDial(seq: Long, conn: String, cls: String = "timeout") =
        ev(seq, "transport", "dial", conn, "endpoint" to "bridge.example:4443", "dur_us" to 2_000_800,
            "class" to cls, "transport" to "quic")

    @Test
    fun `установленное соединение ставится на свою ступень`() {
        val v = pathFrom(listOf(
            okDial(1, "action", "quic", "IPv6", "[2001:db8::7]:4443"),
            okDial(2, "subscribe", "quic", "IPv6", "[2001:db8::7]:4443"),
        ), 0)
        val q6 = v.rungs.single { it.rung == Rung.QUIC_V6 }
        assertEquals(2, q6.count)
        assertEquals(listOf("action", "subscribe"), q6.conns)
        assertTrue(v.rungs.filter { it.rung != Rung.QUIC_V6 }.all { it.count == 0 })
        val text = renderPath(v).joinToString("\n")
        assertTrue("quic/IPv6  → [2001:db8::7]:4443 (endpoint bridge.example:4443), подключение 2.1 мс" in text, text)
        assertTrue("✔ IPv6 QUIC: подключений 2 (action, subscribe)" in text, text)
        assertTrue("· IPv4 QUIC: в журнале не наблюдалась" in text, text)
        assertTrue("· IPv6 TCP: в журнале не наблюдалась" in text, text)
        assertTrue("· транспорт с маскировкой: в журнале не наблюдалась" in text, text)
    }

    @Test
    fun `семейство берётся из журнала, а не из адреса — IPv4 не путается с IPv6`() {
        val v = pathFrom(listOf(okDial(1, "action", "quic", "IPv4", "127.0.0.1:4443")), 0)
        assertEquals(1, v.rungs.single { it.rung == Rung.QUIC_V4 }.count)
        assertEquals(0, v.rungs.single { it.rung == Rung.QUIC_V6 }.count)
        assertEquals(Rung.QUIC_V4, rungOf(dialOf(okDial(1, "action", "quic", "IPv4", "127.0.0.1:1"))!!))
        assertEquals(Rung.TCP_V6, rungOf(dialOf(okDial(1, "action", "tcp", "IPv6", "[::1]:1"))!!))
    }

    @Test
    fun `неудачная попытка не ставится ни на какую ступень и не получает адреса`() {
        val v = pathFrom(listOf(failDial(1, "action"), failDial(2, "action")), 0)
        assertTrue(v.rungs.all { it.count == 0 }, "отказ попал на ступень: ${v.rungs}")
        assertEquals(mapOf("quic" to 2), v.failedByTransport)
        val c = v.conns.single { it.conn == "action" }
        assertNull(c.lastOk)
        assertEquals(2, c.failures)
        val text = renderPath(v).joinToString("\n")
        assertTrue("соединение не установлено: попыток 2, все неудачные" in text, text)
        assertTrue("последняя попытка #2: timeout за 2000.8 мс (quic) — соединения нет, адреса и семейства нет" in text, text)
        assertFalse("✔" in text, "у отказов не бывает использованной ступени: $text")
    }

    @Test
    fun `отказ даже с полем family не делает ступень использованной`() {
        // Журнал так не пишет (у отказа нет соединения), но панель не должна
        // полагаться на это: использованная ступень — только class=ok.
        val odd = ev(1, "transport", "dial", "action", "class" to "error", "transport" to "quic", "family" to "IPv6")
        assertNull(rungOf(dialOf(odd)!!))
        assertTrue(pathFrom(listOf(odd), 0).rungs.all { it.count == 0 })
    }

    @Test
    fun `без transport или family соединение не относится к ступени — показано как неопределённое`() {
        val v = pathFrom(listOf(
            okDial(1, "action", null, "IPv4", "127.0.0.1:1"),
            okDial(2, "subscribe", "quic", null, "bridge:1"),
        ), 0)
        assertTrue(v.rungs.all { it.count == 0 })
        assertEquals(2, v.unplaced.size)
        val text = renderPath(v).joinToString("\n")
        assertTrue("транспорт не сообщён/IPv4" in text, text)
        assertTrue("quic/семейство неизвестно" in text, text)
        assertTrue("к ступени не отнесено" in text, text)
    }

    @Test
    fun `роли без dial названы, вытеснение — предупреждение`() {
        val v = pathFrom(listOf(okDial(5, "action", "quic", "IPv4", "127.0.0.1:1")), dropped = 7)
        val text = renderPath(v).joinToString("\n")
        assertTrue("⚠ вытеснено 7 записей до чтения" in text, text)
        assertTrue(Regex("subscribe +dial в прочитанном журнале нет — запись могла быть вытеснена").containsMatchIn(text), text)
        assertTrue(Regex("media +dial в прочитанном журнале нет — запись могла быть вытеснена").containsMatchIn(text), text)
        val clean = renderPath(pathFrom(emptyList(), 0)).joinToString("\n")
        assertFalse("вытеснен" in clean, clean)
        assertTrue(Regex("action +dial в прочитанном журнале нет$", RegexOption.MULTILINE).containsMatchIn(clean), clean)
    }

    @Test
    fun `последнее установленное соединение и последующая неудача показаны оба`() {
        val v = pathFrom(listOf(
            okDial(1, "subscribe", "quic", "IPv4", "127.0.0.1:1"),
            failDial(2, "subscribe"),
        ), 0)
        val c = v.conns.single { it.conn == "subscribe" }
        assertEquals(1L, c.lastOk?.seq)
        assertEquals(2L, c.lastFailure?.seq)
        // Неудача, за которой снова успех, «последней попыткой» не показывается.
        val healed = pathFrom(listOf(okDial(1, "subscribe", "quic", "IPv4", "a:1"), failDial(2, "subscribe"),
            okDial(3, "subscribe", "quic", "IPv4", "a:1")), 0).conns.single { it.conn == "subscribe" }
        assertNull(healed.lastFailure)
        assertEquals(3, healed.attempts)
    }

    @Test
    fun `перебор ступеней виден только по событиям autodial`() {
        val none = renderPath(pathFrom(listOf(okDial(1, "action", "quic", "IPv4", "a:1")), 0)).joinToString("\n")
        assertTrue("перебора ступеней ядро не вело: событий autodial (route_*) в журнале нет" in none, none)
        val some = pathFrom(listOf(ev(1, "autodial", "route_attempt", null, "class" to "error")), 0)
        assertEquals(1, some.routeEvents)
        assertTrue("событий autodial (route_*) в журнале: 1" in renderPath(some).joinToString("\n"))
    }

    @Test
    fun `трекер Inspector Live — разрыв копится, перезапуск инстанса сбрасывает`() {
        val t = PathTracker()
        val e1 = okDial(1, "action", "quic", "IPv4", "127.0.0.1:1")
        t.accept(listOf(LiveLine.Gap(3, ""), LiveLine.Event(e1, 0, "")))
        assertEquals(3, t.view().dropped)
        assertEquals(1, t.view().rungs.single { it.rung == Rung.QUIC_V4 }.count)
        t.accept(listOf(LiveLine.Restart("a", "b", "")))
        assertEquals(0, t.view().dropped)
        assertTrue(t.view().rungs.all { it.count == 0 }, "подключения прежнего инстанса перенесены в новый")
    }

    @Test
    fun `не dial — не запись пути`() {
        assertNull(dialOf(ev(1, "dispatcher", "conn_result", "action", "class" to "ok")))
        assertNull(dialOf(ev(1, "transport", "conn_closed", "action", "class" to "local")))
    }
}
