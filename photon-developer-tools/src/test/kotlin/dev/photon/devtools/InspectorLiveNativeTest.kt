package dev.photon.devtools

import dev.photon.core.Photon
import dev.photon.sdk.BridgeConfig
import dev.photon.sdk.DiagnosticsPage
import dev.photon.sdk.DiagnosticsSnapshot
import dev.photon.sdk.FeedRecord
import dev.photon.sdk.PhotonClient
import dev.photon.sdk.PhotonConfig
import dev.photon.sdk.StorageConfig
import java.io.File
import java.nio.file.Files
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/*
 * Inspector Live на НАСТОЯЩЕМ ядре: SDK → JNI → C ABI → services/diag.
 * Без подмен — события и снимки идут из журнала Photon Core, а не из
 * инструмента. Bridge в этих тестах недостижим намеренно: диагностика по
 * сети не ходит (ADR-130 §2.8), а отказы подключения порождают события.
 */
class InspectorLiveNativeTest {

    private companion object {
        @Volatile private var loaded = false

        @Synchronized
        fun ensureNative() {
            val dir = File(requireNotNull(System.getProperty("photon.native.dir")) {
                "photon.native.dir не задан: тест обязан идти задачей Gradle, собирающей .so"
            })
            val jni = File(dir, "libphoton_jni.so")
            require(jni.isFile && File(dir, "libphoton_core.so").isFile) { "нет собранных .so в $dir" }
            if (!loaded) {
                Photon.loadFrom(jni.absolutePath)
                loaded = true
            }
        }

        val catalog: Inspector by lazy { Inspector.fromSystemProperty() }
        val names: Map<Int, String> by lazy {
            catalog.enum("Action")!!.valueList.associate { it.number to it.name }
        }

        const val SECRET_TOKEN = "SECRET-TOKEN-7f3a9c"
        const val SECRET_TEXT = "СЕКРЕТНЫЙ-ТЕКСТ-сообщения-41"
    }

    private fun drain(client: PhotonClient): Pair<LiveFeed, List<LiveLine>> {
        val f = LiveFeed(client.diagnosticsSource(), { names[it] })
        return f to f.poll()
    }

    @Test
    fun `живые события ядра с именами действий из каталога`() {
        ensureNative()
        PhotonClient.create(PhotonConfig()).use { c ->
            runCatching { c.sendAction(200, "{}") }        // нет Bridge — not_connected
            runCatching { c.sendAction(999999, "{}") }     // неизвестный код — invalid_action
            val (f, lines) = drain(c)
            val events = lines.filterIsInstance<LiveLine.Event>()
            assertEquals(2, events.size, lines.joinToString("\n") { it.text() })
            assertTrue("dispatcher.call_failed" in events[0].text && "action=200(ACTION_GET_ME)" in events[0].text, events[0].text)
            assertTrue("class=not_connected" in events[0].text, events[0].text)
            assertTrue("action=999999(нет в каталоге)" in events[1].text, events[1].text)
            assertTrue(events.all { it.text.startsWith("≈") })

            val snap = c.diagnostics().snapshot()
            assertEquals(f.instanceId, snap.instanceId)
            assertEquals(2L, snap.calls!!.total, "чтение диагностики попало в учёт вызовов")
            assertTrue(renderSnapshot(snap).any { "вызовы: в полёте 0, всего 2" in it }, renderSnapshot(snap).toString())
            // Оценка времени события — старт инстанса + монотонное время.
            val page = c.diagnostics().events(0, 10)
            assertEquals(page.instanceStartWallMs + page.events[0].atMonoMs, events[0].estimatedWallMs)
            assertTrue(page.instanceStartWallMs > 1_700_000_000_000)
        }
    }

    @Test
    fun `общий sendAction не пропускает 819 и 820 к ядру`() {
        ensureNative()
        PhotonClient.create(PhotonConfig()).use { c ->
            assertFailsWith<IllegalArgumentException> { c.sendAction(819, "{}") }
            assertFailsWith<IllegalArgumentException> { c.sendAction(820, """{"after_seq":0,"limit":1}""") }
            // Ни один из отвергнутых вызовов не дошёл до ядра: журнал и счётчики пусты.
            val s = c.diagnostics().snapshot()
            assertEquals(0L, s.calls!!.total)
            assertEquals(0L, s.journal!!.lastSeq)
        }
    }

    @Test
    fun `перезапуск инстанса виден по instance_id`() {
        ensureNative()
        PhotonClient.create(PhotonConfig()).use { first ->
            PhotonClient.create(PhotonConfig()).use { second ->
                runCatching { first.sendAction(999999, "{}") }
                repeat(3) { runCatching { second.sendAction(999999, "{}") } }
                var current: DiagnosticsSource = first.diagnosticsSource()
                val switching = object : DiagnosticsSource {
                    override fun snapshot(): DiagnosticsSnapshot = current.snapshot()
                    override fun events(afterSeq: Long, limit: Int): DiagnosticsPage = current.events(afterSeq, limit)
                }
                val f = LiveFeed(switching, { names[it] })
                assertEquals(1, f.poll().filterIsInstance<LiveLine.Event>().size)
                current = second.diagnosticsSource() // «тот же» инстанс перезапущен
                val lines = f.poll()
                assertIs<LiveLine.Restart>(lines.first(), lines.joinToString("\n") { it.text() })
                assertEquals(3, lines.filterIsInstance<LiveLine.Event>().size)
                assertEquals(1, f.restarts)
            }
        }
    }

    /**
     * Ни токен сессии, ни текст сообщения, ни путь к файлу хранилища не
     * попадают ни в журнал, ни в снимок, ни в вывод Inspector Live
     * (ADR-130 §2.8). Источники «секретов» настоящие: токен уходит в
     * handshake недостижимого Bridge, текст — в тело записи ленты.
     */
    @Test
    fun `нет утечки токена, текста сообщения и пути`() {
        ensureNative()
        val dir = Files.createTempDirectory("inspector-live").toFile()
        try {
            val db = File(dir, "secret-store-path.db")
            val cfg = PhotonConfig(
                bridge = BridgeConfig(
                    endpoint = "127.0.0.1:1",
                    sessionToken = SECRET_TOKEN,
                    connectTimeout = Duration.ofMillis(300),
                    callTimeout = Duration.ofSeconds(2),
                ),
                storage = StorageConfig(path = db.absolutePath),
            )
            PhotonClient.create(cfg).use { c ->
                c.localFeedPut("канал-1", listOf(FeedRecord("r1", 1, 1, """{"message":"$SECRET_TEXT"}""")))
                runCatching { c.sendAction(200, """{"text":"$SECRET_TEXT"}""") } // не подключится
                runCatching { c.sendAction(801, """{"channel_id":"","records":[{"body":"$SECRET_TEXT"}]}""") }

                val (_, lines) = drain(c)
                val snapshot = c.diagnostics().snapshot()
                val page = c.diagnostics().events(0, 512)
                val everything = buildString {
                    lines.forEach { appendLine(it.text()) }
                    renderSnapshot(snapshot).forEach { appendLine(it) }
                    appendLine(snapshot.rawJson)
                    page.events.forEach { appendLine(it.toString()) }
                }
                assertTrue(lines.filterIsInstance<LiveLine.Event>().any { "transport.dial" in it.text },
                    "попытка подключения не попала в журнал:\n$everything")
                for (secret in listOf(SECRET_TOKEN, SECRET_TEXT, db.absolutePath, "secret-store-path", "канал-1")) {
                    assertTrue(secret !in everything, "утечка «$secret»:\n$everything")
                }
            }
        } finally {
            dir.listFiles()?.forEach { it.delete() }
            dir.delete()
        }
    }
}
