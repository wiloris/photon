package dev.photon.devtools

import dev.photon.core.PhotonException
import dev.photon.core.PhotonNative
import dev.photon.sdk.DiagnosticEvent
import dev.photon.sdk.DiagnosticValue
import dev.photon.sdk.DiagnosticsPage
import dev.photon.sdk.DiagnosticsSnapshot
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Inspector Live — логика ленты на подставном источнике (`sdk-api.md` §12.5):
 * курсор, перезапуск инстанса, разрывы, имена действий, подпись времени.
 */
class LiveFeedTest {

    /** Журнал «ядра» с ёмкостью: хранит последние [capacity] записей. */
    private class FakeJournal(var instanceId: String, val capacity: Int = 1000, val startWall: Long = 1_792_735_000_000) {
        val all = mutableListOf<DiagnosticEvent>()
        var lastSeq = 0L
        fun add(kind: String, fields: Map<String, DiagnosticValue> = emptyMap()) {
            lastSeq++
            all += DiagnosticEvent(lastSeq, lastSeq * 10, "dispatcher", kind, null, fields)
        }
        fun restart(newId: String) { instanceId = newId; all.clear(); lastSeq = 0 }
    }

    private class FakeSource(val j: FakeJournal) : DiagnosticsSource {
        val reads = mutableListOf<Long>()
        override fun snapshot(): DiagnosticsSnapshot = error("не нужен")
        override fun events(afterSeq: Long, limit: Int): DiagnosticsPage {
            reads += afterSeq
            if (afterSeq > j.lastSeq) throw PhotonException(PhotonNative.PHOTON_ERR_INVALID_ARGUMENT, "diag: cursor is ahead of the journal")
            val oldest = maxOf(1, j.lastSeq - j.capacity + 1)
            var first = afterSeq + 1
            var dropped = 0L
            if (first < oldest) { dropped = oldest - first; first = oldest }
            val evs = j.all.filter { it.seq >= first }.take(limit)
            return DiagnosticsPage(j.instanceId, j.startWall, evs, evs.lastOrNull()?.seq ?: afterSeq, if (evs.isEmpty()) 0 else dropped)
        }
    }

    private fun feed(j: FakeJournal, limit: Int = 4) =
        LiveFeed(FakeSource(j), mapOf(200 to "ACTION_GET_ME")::get, limit, ZoneId.of("UTC"))

    private fun seqs(lines: List<LiveLine>) = lines.filterIsInstance<LiveLine.Event>().map { it.event.seq }

    @Test
    fun `курсор продолжает без пропусков и повторов`() {
        val j = FakeJournal("i1")
        val f = feed(j)
        repeat(10) { j.add("call_failed") }
        assertEquals((1L..10L).toList(), seqs(f.poll()))
        assertEquals(emptyList(), seqs(f.poll()))
        repeat(3) { j.add("call_failed") }
        assertEquals(listOf(11L, 12L, 13L), seqs(f.poll()))
        assertEquals(13L, f.cursor)
    }

    @Test
    fun `вытесненные события — явная строка разрыва`() {
        val j = FakeJournal("i1", capacity = 5)
        val f = feed(j)
        repeat(3) { j.add("call_failed") }
        f.poll()
        repeat(9) { j.add("call_failed") } // 4..12; живы 8..12
        val lines = f.poll()
        val gap = lines.first()
        assertIs<LiveLine.Gap>(gap)
        assertEquals(4L, gap.dropped)
        assertTrue("вытеснено 4" in gap.text)
        assertEquals((8L..12L).toList(), seqs(lines))
        assertEquals(4L, f.droppedTotal)
    }

    @Test
    fun `смена instance_id — перезапуск, чтение заново с нуля`() {
        val j = FakeJournal("i1")
        val src = FakeSource(j)
        val f = LiveFeed(src, { null }, 4, ZoneId.of("UTC"))
        repeat(3) { j.add("call_failed") }
        f.poll()
        j.restart("i2")
        repeat(6) { j.add("conn_start") }
        val lines = f.poll()
        val r = lines.first()
        assertIs<LiveLine.Restart>(r)
        assertEquals("i1", r.previous)
        assertEquals("i2", r.current)
        assertEquals((1L..6L).toList(), seqs(lines), "после перезапуска — с первой записи нового инстанса")
        assertEquals(1, f.restarts)
        assertEquals("i2", f.instanceId)
    }

    @Test
    fun `курсор впереди журнала — сброс и обнаружение перезапуска`() {
        val j = FakeJournal("i1")
        val f = feed(j)
        repeat(8) { j.add("call_failed") }
        f.poll() // курсор 8
        j.restart("i2")
        repeat(2) { j.add("dial") } // у нового журнала last=2 < 8
        val lines = f.poll()
        assertIs<LiveLine.Restart>(lines.first())
        assertEquals(listOf(1L, 2L), seqs(lines))
    }

    @Test
    fun `имена действий из каталога, время — с пометкой оценки`() {
        val j = FakeJournal("i1", startWall = 1_792_735_000_000)
        val f = feed(j)
        j.add("call_failed", mapOf("action" to DiagnosticValue.Num(200), "class" to DiagnosticValue.Text("not_connected")))
        j.add("call_failed", mapOf("action" to DiagnosticValue.Num(999999)))
        val (a, b) = f.poll().filterIsInstance<LiveLine.Event>()
        assertTrue("action=200(ACTION_GET_ME)" in a.text, a.text)
        assertTrue("action=999999(нет в каталоге)" in b.text, b.text)
        assertEquals(1_792_735_000_000 + 10, a.estimatedWallMs)
        assertTrue(a.text.startsWith("≈"), "оценка времени не помечена: ${a.text}")
        assertTrue("+10мс" in a.text)
        assertTrue("ОЦЕНКА" in LiveFeed.TIME_NOTE && "монотонным" in LiveFeed.TIME_NOTE)
    }

    @Test
    fun `выключенная диагностика в снимке`() {
        val s = DiagnosticsSnapshot("", 0, 0, false, emptyMap(), null, null, null, null, null, "{}")
        assertEquals(listOf("диагностика выключена конфигурацией (diagnostics.enabled=false)"), renderSnapshot(s))
    }

    @Test
    fun `без каталога коды — числами, без утверждения «нет в каталоге»`() {
        val j = FakeJournal("i1")
        j.add("call_failed", mapOf("action" to DiagnosticValue.Num(200)))
        val noCatalog = LiveFeed(FakeSource(j), { null }, 4, ZoneId.of("UTC"), catalogLoaded = false)
        val line = (noCatalog.poll().single() as LiveLine.Event).text
        assertTrue("action=200" in line && "нет в каталоге" !in line, line)
        val withCatalog = LiveFeed(FakeSource(j), { null }, 4, ZoneId.of("UTC"))
        assertTrue("action=200(нет в каталоге)" in (withCatalog.poll().single() as LiveLine.Event).text)
    }
}
