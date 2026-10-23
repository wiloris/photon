package dev.photon.devtools

import dev.photon.core.Photon
import dev.photon.sdk.BridgeConfig
import dev.photon.sdk.PhotonClient
import dev.photon.sdk.PhotonConfig
import dev.photon.sdk.TlsConfig
import java.io.File
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/*
 * Панель «Путь» на НАСТОЯЩЕМ ядре: SDK → JNI → C ABI → dispatcher +
 * adapters/quic → bridgesim (quic-go v0.59) на 127.0.0.1 и [::1]. Поля пути
 * приходят из журнала ядра через 820, а не из инструмента.
 *
 * Чего этот тест НЕ доказывает: выбор семейства для имени с A- и
 * AAAA-записями — адреса здесь литералы; имени с обеими записями в этом
 * окружении нет, системная конфигурация не меняется.
 */
class ConnectionPathNativeTest {

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
    }

    /** bridgesim на адресе [listen]; закрывается закрытием stdin. */
    private class Sim(listen: String) : AutoCloseable {
        private val proc: Process
        val addr: String

        init {
            val bin = File(requireNotNull(System.getProperty("photon.lab.bridgesim")) {
                "photon.lab.bridgesim не задан: тест обязан идти задачей Gradle, собирающей bridgesim"
            })
            require(bin.canExecute()) { "нет собранного bridgesim: $bin" }
            proc = ProcessBuilder(bin.absolutePath, listen).redirectError(ProcessBuilder.Redirect.DISCARD).start()
            val first = proc.inputStream.bufferedReader().readLine() ?: ""
            if (!first.startsWith("LISTEN ")) {
                proc.destroyForcibly()
                fail("bridgesim не сообщил адрес на $listen: «$first»")
            }
            addr = first.removePrefix("LISTEN ")
            // stdout дальше («ACTION n») читать не нужно, но и копиться он не должен.
            Thread { runCatching { proc.inputStream.copyTo(java.io.OutputStream.nullOutputStream()) } }
                .apply { isDaemon = true; start() }
        }

        override fun close() {
            runCatching { proc.outputStream.close() }
            if (!proc.waitFor(3, TimeUnit.SECONDS)) proc.destroyForcibly().waitFor()
        }
    }

    private fun client(endpoint: String, connect: Duration = Duration.ofSeconds(3)) = PhotonClient.create(PhotonConfig(
        bridge = BridgeConfig(
            endpoint = endpoint, sessionToken = "tok", connectTimeout = connect, callTimeout = Duration.ofSeconds(3),
            tls = TlsConfig(insecureSkipVerify = true, serverName = "localhost"),
        ),
    ))

    /** Ждёт, пока в журнале появятся dial всех [roles], и возвращает панель. */
    private fun awaitPath(p: PhotonClient, roles: Set<String>): PathView {
        val journal = CoreJournal(p.diagnosticsSource())
        val deadline = System.currentTimeMillis() + 10_000
        while (true) {
            val v = journal.path()
            if (v.conns.filter { it.conn in roles }.all { it.attempts > 0 } || System.currentTimeMillis() > deadline) return v
            Thread.sleep(25)
        }
    }

    private fun checkFamily(listen: String, rung: Rung) {
        ensureNative()
        Sim(listen).use { sim ->
            client(sim.addr).use { p ->
                p.addEventListener { } // подписка: второе соединение
                p.sendAction(UsedActions.GET_ME, "{}") // bridgesim отвечает — action подключён
                val v = awaitPath(p, setOf("action", "subscribe"))
                for (role in listOf("action", "subscribe")) {
                    val ok = assertNotNull(v.conns.single { it.conn == role }.lastOk, "$role: нет установленного соединения")
                    assertEquals("quic", ok.transport, "$role: транспорт")
                    assertEquals(rung.family, ok.family, "$role: семейство")
                    assertEquals(sim.addr, ok.remote, "$role: фактический адрес")
                    assertEquals(rung, rungOf(ok))
                }
                val used = v.rungs.filter { it.count > 0 }.map { it.rung }
                assertEquals(listOf(rung), used, "использована ровно одна ступень")
                val text = renderPath(v).joinToString("\n")
                assertTrue("✔ ${rung.label}" in text, text)
                assertTrue("· IPv4 TCP: в журнале не наблюдалась" in text, text)
                assertTrue("· транспорт с маскировкой: в журнале не наблюдалась" in text, text)
                assertTrue("событий autodial (route_*) в журнале нет" in text, text)
            }
        }
    }

    @Test
    fun `IPv4 QUIC — путь из журнала настоящего ядра`() = checkFamily("127.0.0.1:0", Rung.QUIC_V4)

    @Test
    fun `IPv6 QUIC — путь из журнала настоящего ядра`() = checkFamily("[::1]:0", Rung.QUIC_V6)

    @Test
    fun `отказ подключения — транспорт есть, адреса, семейства и ступени нет`() {
        ensureNative()
        val dead = Sim("[::1]:0").use { it.addr } // слушатель закрыт — порт пуст
        client(dead, connect = Duration.ofMillis(800)).use { p ->
            runCatching { p.sendAction(UsedActions.GET_ME, "{}") }
            val v = awaitPath(p, setOf("action"))
            val c = v.conns.single { it.conn == "action" }
            assertNull(c.lastOk)
            val f = assertNotNull(c.lastFailure, "неудачная попытка не попала в панель")
            assertEquals("quic", f.transport)
            assertNull(f.remote, "у неудачной попытки нет адреса соединения")
            assertNull(f.family, "у неудачной попытки нет семейства")
            assertTrue(v.rungs.all { it.count == 0 }, "отказ поставлен на ступень: ${v.rungs}")
            assertTrue((v.failedByTransport["quic"] ?: 0) >= 1)
        }
    }
}
