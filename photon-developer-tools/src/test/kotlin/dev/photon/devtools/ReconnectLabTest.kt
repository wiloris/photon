package dev.photon.devtools

import dev.photon.sdk.PhotonEvent

import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketTimeoutException
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/*
 * Тесты Reconnect Lab.
 *
 * Ретранслятор проверяется на loopback против эхо-сервера — сети не нужно,
 * но пакеты настоящие. Выводы прогона проверяются на эталонном таймлайне и на
 * его порче: каждое правило обязано падать на своей мутации, иначе зелёный
 * отчёт живого прогона ничего не значил бы.
 *
 * Сам обрыв через JNI и живой Bridge здесь не воспроизводится: это прогон, а
 * не тест, и выдавать одно за другое нельзя.
 */
class ReconnectLabTest {

    // ── эхо-сервер и клиенты ─────────────────────────────────────────────────

    /** Эхо, отвечающее с префиксом адреса отправителя, — видно, откуда пришло. */
    private class Echo : AutoCloseable {
        val socket = DatagramSocket(InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0))
        val seenPorts = java.util.concurrent.ConcurrentLinkedQueue<Int>()

        init {
            thread(isDaemon = true) {
                val buf = ByteArray(2048)
                while (true) {
                    val p = DatagramPacket(buf, buf.size)
                    try { socket.receive(p) } catch (e: Exception) { return@thread }
                    seenPorts.add(p.port)
                    val reply = "echo:" + String(p.data, p.offset, p.length)
                    socket.send(DatagramPacket(reply.toByteArray(), reply.length, p.socketAddress))
                }
            }
        }

        val address get() = InetSocketAddress(InetAddress.getByName("127.0.0.1"), socket.localPort)
        override fun close() = socket.close()
    }

    private fun client() = DatagramSocket().apply { soTimeout = 1000 }

    private fun ask(c: DatagramSocket, port: Int, msg: String): String? {
        c.send(DatagramPacket(msg.toByteArray(), msg.length, InetAddress.getByName("127.0.0.1"), port))
        val buf = ByteArray(2048)
        val p = DatagramPacket(buf, buf.size)
        return try {
            c.receive(p); String(p.data, p.offset, p.length)
        } catch (e: SocketTimeoutException) {
            null
        }
    }

    // ── ретранслятор ─────────────────────────────────────────────────────────

    @Test
    fun `ретранслятор передаёт пакеты в обе стороны`() {
        Echo().use { echo ->
            UdpRelay(echo.address).start().use { relay ->
                val c = client()
                assertEquals("echo:ping", ask(c, relay.port, "ping"))
                val s = relay.stats()
                assertEquals(1, s.upForwarded)
                assertEquals(1, s.downForwarded)
                c.close()
            }
        }
    }

    /**
     * Вынужденное отличие от ретранслятора Phase 15.5: через ретранслятор идут
     * ДВА соединения инстанса. Ретранслятор с одним клиентом отдал бы ответ
     * второго соединения первому — этот тест такую реализацию роняет.
     */
    @Test
    fun `два клиента получают каждый свои ответы`() {
        Echo().use { echo ->
            UdpRelay(echo.address).start().use { relay ->
                val a = client()
                val b = client()
                assertEquals("echo:from-a", ask(a, relay.port, "from-a"))
                assertEquals("echo:from-b", ask(b, relay.port, "from-b"))
                assertEquals("echo:again-a", ask(a, relay.port, "again-a"))
                assertEquals(2, relay.stats().flowsActive)
                a.close(); b.close()
            }
        }
    }

    @Test
    fun `обрыв роняет пакеты и считает их, восстановление возвращает путь`() {
        Echo().use { echo ->
            UdpRelay(echo.address).start().use { relay ->
                val c = client()
                assertEquals("echo:1", ask(c, relay.port, "1"))
                val portBefore = echo.seenPorts.last()

                relay.breakPath()
                assertNull(ask(c, relay.port, "lost"), "при оборванном пути ответа быть не может")
                val broken = relay.stats()
                assertTrue(broken.broken)
                assertEquals(1, broken.upDropped)
                assertEquals(0, broken.flowsActive, "обрыв обязан забыть потоки")

                relay.healPath()
                assertEquals("echo:2", ask(c, relay.port, "2"))
                // Новый исходящий сокет — новый порт со стороны сервера: для
                // QUIC это другой путь, а не продолжение старого.
                assertTrue(echo.seenPorts.last() != portBefore, "после восстановления поток обязан быть новым")
                c.close()
            }
        }
    }

    /** Ретранслятор — отдельный процесс; уход родителя (EOF) обязан его завершать. */
    @Test
    fun `ретранслятор-процесс управляется трубами и выходит по EOF`() {
        Echo().use { echo ->
            val relay = RelayProcess.start("127.0.0.1:${echo.socket.localPort}")
            try {
                assertTrue(relay.pid != ProcessHandle.current().pid(), "ретранслятор обязан быть другим процессом")
                val c = client()
                assertEquals("echo:x", ask(c, relay.port, "x"))
                val br = relay.breakPath()
                assertTrue(br.ok && br.stats!!.broken)
                assertNull(ask(c, relay.port, "y"))
                assertEquals(1, relay.stats().stats!!.upDropped)
                assertFalse(relay.healPath().stats!!.broken)
                assertEquals("echo:z", ask(c, relay.port, "z"))
                c.close()
            } finally {
                relay.close()
            }
            // Принудительное завершение в close() — страховка, а не механизм:
            // тест требует, чтобы процесс вышел САМ, иначе страховка скрыла бы
            // ретранслятор, переживающий родителя.
            assertTrue(relay.exitedOnEof, "ретранслятор не вышел по EOF — его пришлось убить")
            val handle = ProcessHandle.of(relay.pid)
            assertTrue(handle.isEmpty || !handle.get().isAlive, "ретранслятор пережил закрытие трубы")
        }
    }

    @Test
    fun `ответ ретранслятора разбирается, отказ не выдаётся за успех`() {
        val ok = parseRelayReply(relayStatsJson("break", RelayStats(5, 4, 3, 2, 2, 0, true)))
        assertTrue(ok.ok)
        assertEquals("break", ok.cmd)
        assertEquals(RelayStats(5, 4, 3, 2, 2, 0, true), ok.stats)
        val bad = parseRelayReply("""{"ok":false,"cmd":"boom","error":"unknown command"}""")
        assertFalse(bad.ok)
        assertEquals("unknown command", bad.error)
        assertEquals(7, parseRelayReply("""{"ok":true,"cmd":"ready","port":7}""").port)
    }

    // ── выводы прогона ───────────────────────────────────────────────────────

    private fun s(sec: Double) = (sec * 1e9).toLong()

    private fun goodMarks() = listOf(
        Mark(s(0.6), MarkKind.CONNECTED),
        Mark(s(10.0), MarkKind.BREAK),
        Mark(s(40.0), MarkKind.NETWORK_CHANGED),
        Mark(s(41.0), MarkKind.HEAL),
        Mark(s(43.0), MarkKind.CONNECTED),
        Mark(s(50.0), MarkKind.BREAK),
        Mark(s(80.0), MarkKind.NETWORK_CHANGED),
        Mark(s(81.0), MarkKind.HEAL),
        Mark(s(83.0), MarkKind.CONNECTED),
        Mark(s(90.0), MarkKind.CLOSE_RETURNED),
    )

    private fun probe(label: String, cycle: Int, phase: ProbePhase, outcome: Outcome?) =
        ActionProbe(label, cycle, phase, outcome, "", 0, 1.0)

    private fun goodProbes() = listOf(
        probe("до", 1, ProbePhase.BEFORE, Outcome.OK),
        probe("во время", 1, ProbePhase.DURING, Outcome.CALL_ERROR),
        probe("после-1", 1, ProbePhase.AFTER, Outcome.OK),
        probe("после-2", 2, ProbePhase.AFTER, Outcome.OK),
    )

    private fun analyze(
        marks: List<Mark> = goodMarks(),
        probes: List<ActionProbe> = goodProbes(),
        dropped: List<Long> = listOf(12, 9),
        concurrent: Int = 1,
        close: Long? = s(0.2),
    ) = analyzeReconnect(marks, probes, dropped, concurrent, close, s(15.0))

    private fun failed(checks: List<Check>) = checks.filter { !it.ok }.map { it.name }

    @Test
    fun `эталонный прогон проходит все проверки`() {
        val checks = analyze()
        assertEquals(emptyList(), failed(checks))
        assertTrue(checks.first { it.name == "цикл 1: обрыв обнаружен до восстановления пути" }.detail.contains("30000 мс"))
    }

    @Test
    fun `без NETWORK_CHANGED обрыв не считается обнаруженным`() {
        val f = failed(analyze(marks = goodMarks().filterNot { it.kind == MarkKind.NETWORK_CHANGED && it.atNanos == s(40.0) }))
        assertTrue("цикл 1: обрыв обнаружен до восстановления пути" in f, "$f")
    }

    @Test
    fun `без CONNECTED после восстановления цикл не закрыт`() {
        val f = failed(analyze(marks = goodMarks().filterNot { it.atNanos == s(43.0) }))
        assertTrue("цикл 1: CONNECTED после восстановления" in f, "$f")
    }

    /** CONNECTED при оборванном пути означает, что обрыв ненастоящий. */
    @Test
    fun `CONNECTED во время обрыва разоблачает фиктивный обрыв`() {
        val f = failed(analyze(marks = goodMarks() + Mark(s(20.0), MarkKind.CONNECTED)))
        assertTrue("цикл 1: через оборванный путь CONNECTED не приходил" in f, "$f")
    }

    @Test
    fun `NETWORK_CHANGED вне обрыва — ложное обнаружение`() {
        val f = failed(analyze(marks = goodMarks() + Mark(s(5.0), MarkKind.NETWORK_CHANGED)))
        assertTrue("NETWORK_CHANGED только при оборванном пути" in f, "$f")
    }

    /** Ноль отброшенных пакетов — клиент молчал, и обрыв ничего не проверил. */
    @Test
    fun `обрыв без отброшенных пакетов не засчитывается`() {
        val f = failed(analyze(dropped = listOf(0, 9)))
        assertTrue("цикл 1: путь действительно оборван" in f, "$f")
    }

    @Test
    fun `ответ сервера во время обрыва не принимается`() {
        for (o in listOf(Outcome.OK, Outcome.VERDICT_FALSE)) {
            val probes = goodProbes().map { if (it.phase == ProbePhase.DURING) it.copy(outcome = o) else it }
            val f = failed(analyze(probes = probes))
            assertTrue("цикл 1: во время обрыва ответа сервера не было" in f, "$o: $f")
        }
    }

    @Test
    fun `зависший вызов и неуспех после восстановления видны`() {
        val probes = goodProbes().map { if (it.label == "после-1") it.copy(outcome = null) else it }
        val f = failed(analyze(probes = probes))
        assertTrue("ни один вызов не завис" in f, "$f")
        assertTrue("цикл 1: действие после восстановления выполнено" in f, "$f")
    }

    @Test
    fun `параллельная доставка нарушает D1`() {
        assertTrue("доставки событий не параллельны (jni-events D1)" in failed(analyze(concurrent = 2)))
    }

    @Test
    fun `колбэк после close и зависший close видны`() {
        val late = failed(analyze(marks = goodMarks() + Mark(s(91.0), MarkKind.NETWORK_CHANGED)))
        assertTrue("после close() колбэков нет" in late, "$late")
        assertTrue("close() вернулся в срок" in failed(analyze(close = null)))
        assertTrue("close() вернулся в срок" in failed(analyze(close = s(20.0))))
    }

    /** Типизированное событие SDK → метка прогона. Разбора конверта здесь нет. */
    @Test
    fun `тип события отображается в метку прогона`() {
        assertEquals(MarkKind.CONNECTED, eventKind(PhotonEvent.Connected("{}")))
        assertEquals(MarkKind.NETWORK_CHANGED, eventKind(PhotonEvent.NetworkChanged("disconnected", "{}")))
        assertEquals(MarkKind.MESSAGE, eventKind(PhotonEvent.Message("posted", null, null, "{}")))
        assertEquals(MarkKind.OTHER_EVENT, eventKind(PhotonEvent.Unknown("PLUGIN_EVENT", "{}")))
        assertEquals(MarkKind.OTHER_EVENT, eventKind(PhotonEvent.Unknown(null, "не json")))
    }

    // ── структурные границы ──────────────────────────────────────────────────

    private fun src(name: String) = java.io.File(System.getProperty("photon.devtools.src"), name).readText()

    /** Ретранслятор не смешивается с измеряемым процессом. */
    @Test
    fun `лаборатория запускает ретранслятор отдельным процессом`() {
        val lab = src("ReconnectLab.kt")
        assertTrue(lab.contains("RelayProcess.start("), "ретранслятор обязан порождаться процессом")
        assertTrue(!lab.contains("UdpRelay("), "ретранслятор в измеряемой JVM смешал бы путь и стенд")
    }

    @Test
    fun `инстанс создаётся публичным API SDK и смотрит в ретранслятор`() {
        val lab = src("ReconnectLab.kt")
        assertTrue(lab.contains("PhotonClient.create("), "инстанс обязан создаваться через SDK")
        assertTrue(lab.contains("serverName = host()"), "имя для TLS — настоящий хост Bridge")
        assertNotNull(Regex("""endpoint = "127\.0\.0\.1:""").find(lab),
            "адрес обязан указывать на локальный ретранслятор")
    }

    /** Границы проверки обязаны быть названы в отчёте, а не подразумеваться. */
    @Test
    fun `отчёт называет, чем проверка НЕ является`() {
        val all = RECONNECT_BOUNDARIES.joinToString("\n")
        listOf("ИСКУССТВЕННЫЙ обрыв UDP-пути", "НЕ проверка смены сетевого интерфейса",
            "НЕ проверка убийства сервера", "НЕ NAT rebinding", "НЕ симметричный NAT")
            .forEach { assertTrue(all.contains(it), "не названо: $it") }
        assertTrue(src("ReconnectLab.kt").contains("RECONNECT_BOUNDARIES.forEach"))
    }
}
