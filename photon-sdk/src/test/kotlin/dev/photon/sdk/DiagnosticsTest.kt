package dev.photon.sdk

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Диагностика — `sdk-api.md` §12.6, ADR-130 §2.7–§2.8 (с поправкой §2.3).
 */
class DiagnosticsTest {

    private fun clientWith(response: String): Pair<PhotonClient, FakeNativeSession> {
        val native = FakeNativeSession()
        native.nextResponse = response
        return PhotonClient.createWith(PhotonConfig()) { native } to native
    }

    private fun ok(data: String) = """{"ok":true,"data":$data}"""

    private val page = ok(
        """{"instance_id":"abc","instance_start_wall_ms":1792735000123,
           "events":[
             {"seq":9007199254740993,"at_mono_ms":15030,"component":"session","kind":"state","conn":"action",
              "fields":{"from":"connecting","to":"ready"}},
             {"seq":9007199254740994,"at_mono_ms":15031,"component":"dispatcher","kind":"call_failed",
              "fields":{"action":424242,"dur_us":9007199254740995,"class":"invalid_action"}}],
           "next_seq":9007199254740994,"dropped":3}"""
    )

    @Test
    fun `общий sendAction отвергает 819 и 820 без вызова ABI`() {
        val (c, n) = clientWith(ok("{}"))
        for (code in listOf(819, 820)) {
            val e = assertFailsWith<IllegalArgumentException> { c.sendAction(code, "{}") }
            assertTrue("diagnostics()" in e.message!!, e.message)
        }
        assertTrue(n.actions.isEmpty(), "нативная сессия вызвана: ${n.actions}")
    }

    @Test
    fun `остальные коды sendAction пропускает как прежде`() {
        val (c, n) = clientWith(ok("{}"))
        for (code in listOf(800, 803, 818, 821, 860, 200)) c.sendAction(code, "{}")
        assertEquals(listOf(800, 803, 818, 821, 860, 200), n.actions.map { it.first })
    }

    @Test
    fun `коды совпадают с photon proto`() {
        assertEquals(819, DiagnosticsCodec.ACTION_SNAPSHOT)
        assertEquals(820, DiagnosticsCodec.ACTION_EVENTS)
        val proto = File("../photon-core/protobuf/photon.proto").readText()
        assertTrue(Regex("""ACTION_LOCAL_DIAG_SNAPSHOT\s*=\s*819;""").containsMatchIn(proto))
        assertTrue(Regex("""ACTION_LOCAL_DIAG_EVENTS\s*=\s*820;""").containsMatchIn(proto))
    }

    @Test
    fun `events уходит кодом 820 с курсором и лимитом`() {
        val (c, n) = clientWith(page)
        c.diagnostics().events(9007199254740993L, 512)
        assertEquals(820, n.actions.single().first)
        assertEquals("""{"after_seq":9007199254740993,"limit":512}""", n.actions.single().second)
    }

    @Test
    fun `страница разбирается без потери Long и без календарного времени события`() {
        val (c, _) = clientWith(page)
        val p = c.diagnostics().events(0, 10)
        assertEquals("abc", p.instanceId)
        assertEquals(1792735000123L, p.instanceStartWallMs)
        assertEquals(9007199254740994L, p.nextSeq)
        assertEquals(3L, p.dropped)
        val (a, b) = p.events
        assertEquals(9007199254740993L, a.seq)
        assertEquals("action", a.conn)
        assertNull(b.conn)
        assertEquals(DiagnosticValue.Text("ready"), a.fields["to"])
        assertEquals(DiagnosticValue.Num(9007199254740995L), b.fields["dur_us"])
        assertEquals(1792735000123L + 15030L, p.estimatedWallMs(a))
    }

    @Test
    fun `в формах SDK нет поля календарного времени события`() {
        val src = File("src/main/kotlin/dev/photon/sdk/Diagnostics.kt").readText()
        assertFalse("at_wall_ms" in src, "SDK читает at_wall_ms")
        assertFalse(Regex("""val atWall""").containsMatchIn(src), "у события календарное поле")
    }

    @Test
    fun `snapshot уходит кодом 819 и разбирается`() {
        val (c, n) = clientWith(
            ok(
                """{"instance_id":"abc","instance_start_wall_ms":1792735000123,"uptime_ms":42,
                   "connections":{"action":{"state":"ready","in_state_ms":5,"connects":2,"connect_errors":1,"reconnects":1,"lost":1},
                                  "media":{"state":"none","connects":0,"connect_errors":0,"reconnects":0,"lost":0}},
                   "calls":{"in_flight":0,"total":7,"slow":1,"by_class":{"ok":6,"timeout":1},
                            "duration":{"le_us":[10,50],"counts":[1,2,4],"sum_us":900,"n":7}},
                   "events":{"connected":1,"network_changed":0,"message":12},
                   "journal":{"capacity":1024,"len":30,"last_seq":30,"evicted":0},
                   "config":{"enabled":true,"capacity":1024,"slow_tx_ms":100,"slow_call_ms":5000,
                             "bridge_configured":true,"storage_configured":false,"p2p_configured":false},
                   "future_section":{"x":1}}"""
            )
        )
        val s = c.diagnostics().snapshot()
        assertEquals(819, n.actions.single().first)
        assertEquals("{}", n.actions.single().second)
        assertTrue(s.enabled)
        assertEquals(1792735000123L, s.instanceStartWallMs)
        assertEquals(ConnectionDiagnostics("ready", 5, 2, 1, 1, 1), s.connections["action"])
        assertEquals("none", s.connections["media"]!!.state)
        assertEquals(mapOf("ok" to 6L, "timeout" to 1L), s.calls!!.byClass)
        assertEquals(listOf(1L, 2L, 4L), s.calls!!.duration.counts)
        assertEquals(12L, s.subscriptionEvents!!.message)
        assertEquals(30L, s.journal!!.lastSeq)
        assertNull(s.storage, "хранилища нет — null, а не пустой раздел")
        assertTrue(s.config!!.bridgeConfigured)
        assertTrue("future_section" in s.rawJson, "незнакомый раздел потерян")
    }

    @Test
    fun `выключенная диагностика — не исключение`() {
        val (c, _) = clientWith(
            ok(
                """{"instance_id":"","instance_start_wall_ms":0,"uptime_ms":0,"connections":null,
                   "calls":{"in_flight":0,"total":0,"slow":0,"by_class":null,"duration":{"le_us":null,"counts":null,"sum_us":0,"n":0}},
                   "events":{"connected":0,"network_changed":0,"message":0},
                   "journal":{"capacity":0,"len":0,"last_seq":0,"evicted":0},
                   "config":{"enabled":false,"capacity":0,"slow_tx_ms":0,"slow_call_ms":0,
                             "bridge_configured":false,"storage_configured":false,"p2p_configured":false}}"""
            )
        )
        val s = c.diagnostics().snapshot()
        assertFalse(s.enabled)
        assertTrue(s.connections.isEmpty())
        assertNull(s.calls)
        assertNull(s.config)
    }

    @Test
    fun `ответ не по контракту — IllegalStateException с сырым ответом`() {
        val (c, _) = clientWith(ok("""{"instance_id":"abc","events":[],"next_seq":0}"""))
        val e = assertFailsWith<IllegalStateException> { c.diagnostics().events(0, 10) }
        assertTrue("instance_start_wall_ms" in e.message!!, e.message)
        val (c2, _) = clientWith("""{"ok":false,"error":"x"}""")
        assertFailsWith<IllegalStateException> { c2.diagnostics().snapshot() }
    }

    @Test
    fun `отказ вызова ядра проходит как PhotonException`() {
        val (c, n) = clientWith(ok("{}"))
        n.nextThrow = dev.photon.core.PhotonException(dev.photon.core.PhotonNative.PHOTON_ERR_INVALID_ARGUMENT, "diag: cursor is ahead of the journal")
        assertIs<dev.photon.core.PhotonException>(runCatching { c.diagnostics().events(99, 10) }.exceptionOrNull())
    }

    @Test
    fun `закрытый клиент`() {
        val (c, _) = clientWith(page)
        val d = c.diagnostics()
        c.close()
        assertFailsWith<IllegalStateException> { c.diagnostics() }
        assertFailsWith<IllegalStateException> { d.events(0, 1) }
    }
}
