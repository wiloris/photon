package dev.photon.devtools

import dev.photon.sdk.DiagnosticEvent
import dev.photon.sdk.DiagnosticValue
import dev.photon.sdk.DiagnosticsPage
import dev.photon.sdk.DiagnosticsSnapshot
import dev.photon.sdk.DurationHistogram
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Журнал ядра в лабораториях (ADR-130 §2.7): разбор времён и классов по
 * событиям ядра, сверка исходов, работа против настоящего инстанса.
 */
class CoreJournalTest {

    private fun ev(seq: Long, mono: Long, comp: String, kind: String, conn: String? = null, vararg f: Pair<String, Any>) =
        DiagnosticEvent(seq, mono, comp, kind, conn, f.associate { (k, v) ->
            k to if (v is String) DiagnosticValue.Text(v) else DiagnosticValue.Num((v as Number).toLong())
        })

    private val twoCycles = listOf(
        ev(1, 900, "dispatcher", "conn_result", "subscribe", "class" to "ok"),
        // цикл 1: обрыв в 1000, восстановление в 50000
        ev(2, 1500, "dispatcher", "call_failed", null, "class" to "timeout", "action" to 200),
        ev(3, 44_000, "session", "failed", "subscribe", "class" to "lost"),
        ev(4, 44_001, "events", "event_sent", "subscribe", "event" to "NETWORK_CHANGED"),
        ev(5, 50_300, "dispatcher", "conn_result", "subscribe", "class" to "ok"),
        ev(6, 50_301, "events", "event_sent", "subscribe", "event" to "CONNECTED"),
        ev(7, 50_700, "dispatcher", "conn_result", "action", "class" to "ok"),
        // цикл 2: обрыв в 60000, восстановление в 100000 — подписка не вернулась
        ev(8, 90_000, "session", "failed", "subscribe", "class" to "lost"),
        ev(9, 90_001, "events", "event_sent", "subscribe", "event" to "NETWORK_CHANGED"),
    )

    @Test
    fun `времена циклов считаются от команд по часам ядра`() {
        val c = journalCycles(twoCycles, listOf(1000, 60_000), listOf(50_000, 100_000))
        assertEquals(JournalCycle(1, 43_000, 43_001, 300, 700, 301, listOf("timeout")), c[0])
        assertEquals(30_000L, c[1].detectMs)
        assertNull(c[1].transportSubMs, "подписка во втором цикле не вернулась — null, а не ноль")
        assertNull(c[1].sessionConnectedMs)
    }

    @Test
    fun `проверки ловят нарушения`() {
        val ok = journalChecks(journalCycles(twoCycles, listOf(1000), listOf(50_000)), 0)
        assertTrue(ok.all { it.ok }, ok.filterNot { it.ok }.toString())
        assertTrue(ok.any { "запас 2000 мс" in it.detail })

        val late = listOf(ev(1, 46_500, "session", "failed", "subscribe", "class" to "lost"))
        val slow = journalChecks(journalCycles(late, listOf(1000), listOf(60_000)), 0)
        assertFalse(slow.first { "≤ 45 с" in it.name }.ok, "T_detect 45.5 с не должен проходить")

        val internal = listOf(ev(1, 2000, "dispatcher", "call_failed", null, "class" to "internal"))
        assertFalse(journalChecks(journalCycles(internal, listOf(1000), listOf(5000)), 0).first { "классифицированы" in it.name }.ok)
        assertFalse(journalChecks(emptyList(), 7).first().ok, "разрыв журнала обязан быть виден")
    }

    @Test
    fun `сверка исходов и среднее время в ядре`() {
        assertTrue(outcomeCrossCheck(10, 2, mapOf("ok" to 8L, "timeout" to 2L)).ok)
        assertFalse(outcomeCrossCheck(10, 1, mapOf("ok" to 8L, "timeout" to 2L)).ok)
        assertFalse(outcomeCrossCheck(11, 2, mapOf("ok" to 8L, "timeout" to 2L)).ok)
        assertEquals(150.0, meanCoreMicros(DurationHistogram(emptyList(), emptyList(), 300, 2)))
        assertNull(meanCoreMicros(DurationHistogram(emptyList(), emptyList(), 0, 0)))
    }

    @Test
    fun `перезапуск инстанса сбрасывает накопленное`() {
        var id = "i1"
        var events = listOf(ev(1, 10, "dispatcher", "call_failed", null, "class" to "x"))
        val src = object : DiagnosticsSource {
            override fun snapshot() = DiagnosticsSnapshot(id, 0, 777, true, emptyMap(), null, null, null, null, null, "{}")
            override fun events(afterSeq: Long, limit: Int) =
                events.filter { it.seq > afterSeq }.let { DiagnosticsPage(id, 0, it, it.lastOrNull()?.seq ?: afterSeq, 0) }
        }
        val j = CoreJournal(src)
        assertEquals(1, j.pull().size)
        assertEquals(777L, j.now())
        id = "i2"
        events = listOf(ev(1, 5, "session", "state", "action"))
        val after = j.pull()
        assertEquals(1, after.size)
        assertEquals("session", after.single().component, "события прежнего инстанса не смешиваются с новыми")
    }
}
