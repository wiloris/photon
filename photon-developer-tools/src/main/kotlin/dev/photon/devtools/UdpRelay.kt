package dev.photon.devtools

import java.io.Closeable
import java.io.File
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

/*
 * Локальный UDP-ретранслятор для проверки обрыва и восстановления связи.
 *
 * Методика взята из Phase 15.5 (photon-core/tests/eventcheck/proxy.go), а не
 * придумана заново: разрыв ПУТИ пакетов — единственный способ получить
 * настоящий обрыв без доступа к серверу. Отодвинутый keep-alive не работает
 * (keep-alive шлёт сам Bridge), закрытие инстанса — не потеря сети.
 *
 * ОДНО ОТЛИЧИЕ ОТ PHASE 15.5, И ОНО ВЫНУЖДЕННОЕ. Ретранслятор eventcheck
 * держит ровно одного клиента: через него шла только подписка, действия —
 * напрямую. Здесь инстанс создаётся через C ABI с ОДНИМ bridge.endpoint, и
 * через ретранслятор идут оба соединения: action и подписка. adapters/quic
 * открывает сокет на каждое соединение (DialAddr), то есть у клиента два
 * разных исходных порта. Ретранслятор с одним клиентом отдавал бы ответы
 * второго соединения первому — поэтому здесь таблица потоков по адресу
 * клиента, и это закреплено тестом.
 *
 * Обрыв: пакеты перестают ходить в обе стороны, исходящие сокеты закрываются,
 * таблица забывается — как breakPath в Phase 15.5. После восстановления новые
 * пакеты клиента открывают НОВЫЕ исходящие сокеты с новыми портами.
 *
 * Живёт ОТДЕЛЬНЫМ ПРОЦЕССОМ ([RelayProcess]) — не в измеряемой JVM: иначе
 * потоки ретранслятора делили бы процесс с путём, задержку которого мы
 * измеряем.
 */

data class RelayStats(
    val upForwarded: Long,
    val downForwarded: Long,
    val upDropped: Long,
    val downDropped: Long,
    val flowsOpened: Long,
    val flowsActive: Int,
    val broken: Boolean,
)

class UdpRelay(private val remote: InetSocketAddress) : Closeable {

    private val listen = DatagramSocket(InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0))
    val port: Int get() = listen.localPort

    /** Защищает broken и flows вместе: проверка «оборван ли путь» и открытие
     *  потока обязаны быть одним шагом, иначе пакет, проверенный до обрыва,
     *  открыл бы поток уже после него. */
    private val lock = Any()
    private var broken = false
    private val flows = HashMap<SocketAddress, Flow>()

    private val upForwarded = AtomicLong()
    private val downForwarded = AtomicLong()
    private val upDropped = AtomicLong()
    private val downDropped = AtomicLong()
    private val flowsOpened = AtomicLong()

    @Volatile private var closed = false

    private inner class Flow(val client: SocketAddress) {
        val up = DatagramSocket()

        init {
            up.connect(remote)
            thread(isDaemon = true, name = "relay-up-$client") { pumpUp() }
        }

        /** От сервера к клиенту. Закрытие сокета при обрыве выводит из цикла. */
        private fun pumpUp() {
            val buf = ByteArray(65535)
            while (true) {
                val p = DatagramPacket(buf, buf.size)
                try {
                    up.receive(p)
                } catch (e: Exception) {
                    return
                }
                val drop = synchronized(lock) { broken }
                if (drop) {
                    downDropped.incrementAndGet()
                    continue
                }
                try {
                    listen.send(DatagramPacket(p.data, p.offset, p.length, client))
                    downForwarded.incrementAndGet()
                } catch (e: Exception) {
                    return
                }
            }
        }
    }

    fun start(): UdpRelay {
        thread(isDaemon = true, name = "relay-down") { pumpDown() }
        return this
    }

    /** От клиентов к серверу. */
    private fun pumpDown() {
        val buf = ByteArray(65535)
        while (!closed) {
            val p = DatagramPacket(buf, buf.size)
            try {
                listen.receive(p)
            } catch (e: Exception) {
                return
            }
            val flow = synchronized(lock) {
                if (broken) null
                else flows.getOrPut(p.socketAddress) {
                    flowsOpened.incrementAndGet()
                    Flow(p.socketAddress)
                }
            }
            if (flow == null) {
                upDropped.incrementAndGet()
                continue
            }
            try {
                flow.up.send(DatagramPacket(p.data, p.offset, p.length))
                upForwarded.incrementAndGet()
            } catch (e: Exception) {
                // Сокет закрыт обрывом между выборкой потока и отправкой:
                // пакет не ушёл — значит, отброшен.
                upDropped.incrementAndGet()
            }
        }
    }

    /** Рвёт путь: пакеты не ходят ни в одну сторону, исходящие сокеты закрыты. */
    fun breakPath() {
        val old = synchronized(lock) {
            broken = true
            val v = flows.values.toList()
            flows.clear()
            v
        }
        old.forEach { it.up.close() }
    }

    /** Восстанавливает путь. Новые пакеты откроют новые потоки. */
    fun healPath() {
        synchronized(lock) { broken = false }
    }

    fun stats(): RelayStats = synchronized(lock) {
        RelayStats(upForwarded.get(), downForwarded.get(), upDropped.get(), downDropped.get(),
            flowsOpened.get(), flows.size, broken)
    }

    override fun close() {
        closed = true
        breakPath()
        listen.close()
    }
}

// ── управляющий канал дочернего процесса ─────────────────────────────────────

fun relayStatsJson(cmd: String, s: RelayStats): String =
    """{"ok":true,"cmd":"$cmd","broken":${s.broken},"up_forwarded":${s.upForwarded},""" +
        """"down_forwarded":${s.downForwarded},"up_dropped":${s.upDropped},""" +
        """"down_dropped":${s.downDropped},"flows_opened":${s.flowsOpened},"flows_active":${s.flowsActive}}"""

data class RelayReply(val ok: Boolean, val cmd: String, val error: String?, val port: Int?, val stats: RelayStats?)

private fun longField(json: String, name: String): Long? =
    Regex("\"$name\"\\s*:\\s*(-?\\d+)").find(json)?.groupValues?.get(1)?.toLongOrNull()

/** Разбор ответа ретранслятора. Чистая функция — проверяется без процессов. */
fun parseRelayReply(line: String): RelayReply {
    val ok = Regex("\"ok\"\\s*:\\s*true").containsMatchIn(line)
    val cmd = Regex("\"cmd\"\\s*:\\s*\"([^\"]*)\"").find(line)?.groupValues?.get(1) ?: "?"
    val error = Regex("\"error\"\\s*:\\s*\"([^\"]*)\"").find(line)?.groupValues?.get(1)
    val up = longField(line, "up_forwarded")
    val stats = if (up == null) null else RelayStats(
        upForwarded = up,
        downForwarded = longField(line, "down_forwarded") ?: 0,
        upDropped = longField(line, "up_dropped") ?: 0,
        downDropped = longField(line, "down_dropped") ?: 0,
        flowsOpened = longField(line, "flows_opened") ?: 0,
        flowsActive = (longField(line, "flows_active") ?: 0).toInt(),
        broken = Regex("\"broken\"\\s*:\\s*true").containsMatchIn(line),
    )
    return RelayReply(ok, cmd, error, longField(line, "port")?.toInt(), stats)
}

/**
 * Точка входа дочернего процесса.
 *
 * Протокол — строка на команду: `break`, `heal`, `stats`. Ответ — JSON в
 * строку. Первая строка — готовность с портом. EOF на stdin означает уход
 * родителя: ретранслятор закрывается и процесс выходит, пережить измеряемый
 * процесс он не должен.
 */
object UdpRelayMain {
    @JvmStatic
    fun main(args: Array<String>) {
        val target = args.getOrNull(0) ?: run {
            System.err.println("нужно: <host:port Bridge>")
            kotlin.system.exitProcess(2)
        }
        val relay = UdpRelay(resolveUdp4(target)).start()
        val out = System.out
        out.println("""{"ok":true,"cmd":"ready","port":${relay.port}}""")
        out.flush()
        val input = System.`in`.bufferedReader()
        while (true) {
            val line = input.readLine() ?: break
            val reply = when (val cmd = line.trim()) {
                "break" -> { relay.breakPath(); relayStatsJson(cmd, relay.stats()) }
                "heal" -> { relay.healPath(); relayStatsJson(cmd, relay.stats()) }
                "stats" -> relayStatsJson(cmd, relay.stats())
                else -> """{"ok":false,"cmd":"${cmd.replace("\"", "")}","error":"unknown command"}"""
            }
            out.println(reply)
            out.flush()
        }
        relay.close()
    }
}

/** host:port → IPv4-адрес. IPv4 — как udp4 в ретрансляторе Phase 15.5. */
fun resolveUdp4(hostPort: String): InetSocketAddress {
    val i = hostPort.lastIndexOf(':')
    require(i > 0) { "ожидается host:port, получено $hostPort" }
    val host = hostPort.substring(0, i)
    val port = hostPort.substring(i + 1).toInt()
    val v4 = InetAddress.getAllByName(host).firstOrNull { it is Inet4Address }
        ?: error("у $host нет IPv4-адреса")
    return InetSocketAddress(v4, port)
}

/**
 * Ретранслятор в отдельном процессе — JVM с тем же classpath.
 *
 * Управляется трубами, как runtime Plugin Lab. Процесс чужой измеряемому:
 * его потоки, GC и планирование на задержку клиента не накладываются.
 */
class RelayProcess private constructor(private val process: Process, val port: Int) : Closeable {
    private val reader = process.inputStream.bufferedReader()
    private val writer = process.outputStream.bufferedWriter()

    private fun command(cmd: String): RelayReply {
        writer.write(cmd)
        writer.newLine()
        writer.flush()
        val line = reader.readLine() ?: return RelayReply(false, cmd, "канал закрыт", null, null)
        return parseRelayReply(line)
    }

    fun breakPath(): RelayReply = command("break")
    fun healPath(): RelayReply = command("heal")
    fun stats(): RelayReply = command("stats")

    val pid: Long get() = process.pid()
    val isAlive: Boolean get() = process.isAlive

    /** Вышел ли процесс САМ по закрытию трубы, без принудительного завершения. */
    @Volatile var exitedOnEof: Boolean = false
        private set

    /** Закрывает трубу и ждёт выхода; не дождался — убивает. */
    override fun close() {
        runCatching { writer.close() }
        exitedOnEof = process.waitFor(5, TimeUnit.SECONDS)
        if (!exitedOnEof) process.destroyForcibly()
    }

    companion object {
        fun start(target: String): RelayProcess {
            val java = ProcessHandle.current().info().command().orElse(
                File(System.getProperty("java.home"), "bin/java").path)
            val p = ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                UdpRelayMain::class.java.name, target)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start()
            Runtime.getRuntime().addShutdownHook(Thread { if (p.isAlive) p.destroyForcibly() })
            // Готовность читается побайтово, а не через BufferedReader: тот
            // съел бы в свой буфер и следующие строки, которые затем читает
            // reader экземпляра.
            val ready = parseRelayReply(readLineRaw(p.inputStream))
            val port = ready.port
            if (!ready.ok || port == null) {
                p.destroyForcibly()
                error("ретранслятор не объявил готовность")
            }
            return RelayProcess(p, port)
        }

        private fun readLineRaw(input: java.io.InputStream): String {
            val sb = StringBuilder()
            while (true) {
                val b = input.read()
                if (b < 0 || b == '\n'.code) break
                sb.append(b.toChar())
            }
            return sb.toString()
        }
    }
}
