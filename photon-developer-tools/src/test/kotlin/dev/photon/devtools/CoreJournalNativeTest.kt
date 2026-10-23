package dev.photon.devtools

import dev.photon.core.Photon
import dev.photon.sdk.PhotonClient
import dev.photon.sdk.PhotonConfig
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/*
 * CoreJournal (photon-devtools-core) на НАСТОЯЩЕМ ядре: SDK → JNI → C ABI.
 * Чистая часть — в photon-devtools-core (CoreJournalTest); здесь — то, что
 * требует собранных .so, а они собираются этим проектом.
 */
class CoreJournalNativeTest {

    // ── настоящее ядро ───────────────────────────────────────────────────

    private fun ensureNative() {
        val dir = File(requireNotNull(System.getProperty("photon.native.dir")) { "photon.native.dir не задан" })
        Photon.loadFrom(File(dir, "libphoton_jni.so").absolutePath)
    }

    @Test
    fun `журнал настоящего инстанса — время и классы отказов`() {
        ensureNative()
        PhotonClient.create(PhotonConfig()).use { c ->
            val j = CoreJournal(c.diagnosticsSource())
            val t0 = j.now()
            runCatching { c.sendAction(200, "{}") }   // нет Bridge — not_connected
            runCatching { c.sendAction(UsedActions.DELIBERATELY_UNKNOWN, "{}") }
            val first = assertNotNull(j.await(t0, 2000, CoreJournal.callFailed))
            assertTrue(first.atMonoMs >= t0)
            val classes = j.pull().filter(CoreJournal.callFailed).map { CoreJournal.text(it, "class") }
            assertEquals(listOf("not_connected", "invalid_action"), classes)
            assertTrue(j.now() >= first.atMonoMs, "часы инстанса идут вперёд")
            val byClass = c.diagnostics().snapshot().calls!!.byClass
            assertTrue(outcomeCrossCheck(2, 2, byClass).ok, byClass.toString())
        }
    }
}
