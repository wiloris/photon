package dev.photon.devtools

import dev.photon.core.Photon
import dev.photon.sdk.BridgeConfig
import dev.photon.sdk.PhotonClient
import dev.photon.sdk.PhotonConfig
import java.time.Duration
import java.io.File
import java.util.Locale
import java.util.concurrent.CountDownLatch

/*
 * Performance — воспроизводимые измерения пути
 * Desktop → JNI → C ABI → Photon Core (→ Bridge).
 *
 * Ничего нового у платформы не просит: используются уже существующие символы
 * (Photon_GetABIVersion, Photon_SendAction) и обычные средства ОС и JVM.
 *
 * Разделение слоёв опирается на факт из кода, а не на догадку: неизвестный
 * код действия отсекается ЛОКАЛЬНО, в session/resource.go, до того как
 * что-либо уходит в сеть. Поэтому вызов с заведомо неизвестным кодом
 * измеряет путь без сети, а разность с живым вызовом даёт ПРОИЗВОДНУЮ оценку
 * доли сети и сервера. Производную — и так она подписана: измеренным RTT она
 * не является.
 */

private fun f1(v: Double) = String.format(Locale.ROOT, "%.1f", v)
private fun f2(v: Double) = String.format(Locale.ROOT, "%.2f", v)
private fun f3(v: Double) = String.format(Locale.ROOT, "%.3f", v)

/** Слой пути и его измеренная задержка. */
data class Probe(val name: String, val includes: String, val network: Boolean, val latency: Latencies)

/**
 * Оценка доли сети и сервера как разности медиан.
 *
 * Отрицательной быть не может: если локальный путь оказался «дороже»
 * полного, это шум измерения, а не отрицательная сеть. Такой случай
 * возвращает 0 и обязан быть виден в отчёте как шум.
 */
fun derivedNetworkShare(fullP50: Double, localP50: Double): Double =
    (fullP50 - localP50).coerceAtLeast(0.0)

/** Во сколько раз выросла пропускная способность относительно одного потока. */
fun scaling(oneThread: Double, nThreads: Double): Double =
    if (oneThread <= 0.0) 0.0 else nThreads / oneThread

/** Замер памяти: куча JVM и RSS процесса — величины РАЗНЫЕ и не взаимозаменяемые. */
data class MemorySnapshot(val jvmUsedBytes: Long, val rssKb: Long, val hwmKb: Long) {
    companion object {
        private fun proc(field: String): Long =
            runCatching {
                File("/proc/self/status").readLines()
                    .firstOrNull { it.startsWith("$field:") }
                    ?.filter { it.isDigit() }?.toLong() ?: 0L
            }.getOrDefault(0L)

        fun take(): MemorySnapshot {
            val rt = Runtime.getRuntime()
            return MemorySnapshot(rt.totalMemory() - rt.freeMemory(), proc("VmRSS"), proc("VmHWM"))
        }
    }
}

class PerfLab(private val endpoint: String?, private val token: String?) {

    private val online = endpoint != null && token != null

    private fun config() =
        if (online) PhotonConfig(
            BridgeConfig(
                endpoint = endpoint,
                sessionToken = token,
                connectTimeout = Duration.ofSeconds(15),
                callTimeout = Duration.ofSeconds(20),
            )
        )
        // Инстанс без адреса Bridge законен: сети он не касается, и локальный
        // путь меряется именно на нём.
        else PhotonConfig()

    /**
     * Прогрев обязателен и вынесен в отдельный шаг.
     *
     * Без него первые вызовы измеряют работу JIT и загрузку классов, а не
     * путь. Числа без прогрева выглядят правдоподобно и означают не то —
     * это и делает их опаснее отсутствующих.
     */
    private fun warmup(times: Int, body: () -> Unit) = repeat(times) { body() }

    private fun measure(times: Int, body: () -> Unit): Latencies {
        val samples = ArrayList<Double>(times)
        repeat(times) {
            val t = System.nanoTime()
            body()
            samples.add((System.nanoTime() - t) / 1_000_000.0)
        }
        return Latencies(samples)
    }

    private fun probes(p: PhotonClient?, n: Int): List<Probe> {
        val out = ArrayList<Probe>()

        // 1. Граница JVM↔native в чистом виде: константа, ни замка, ни состояния.
        warmup(n) { Photon.abiVersion() }
        out.add(Probe("abiVersion()", "JNI + cgo", false, measure(n) { Photon.abiVersion() }))

        if (p != null) {
            // 2. Тот же путь плюс мьютекс инстанса, диспетчер и валидация кода.
            //    Сети не касается: ResourceFor отвергает код раньше.
            warmup(n) { call(p, UsedActions.DELIBERATELY_UNKNOWN, "{}") }
            out.add(Probe("sendAction(неизвестный код)", "JNI + cgo + Core до валидации", false,
                measure(n) { call(p, UsedActions.DELIBERATELY_UNKNOWN, "{}") }))

            if (online) {
                // 3. Полный путь.
                warmup(minOf(n, 20)) { call(p, UsedActions.GET_ME, "{}") }
                out.add(Probe("sendAction(getMe)", "весь путь + QUIC + Bridge", true,
                    measure(n) { call(p, UsedActions.GET_ME, "{}") }))
            }
        }
        return out
    }

    private fun throughputSweep(p: PhotonClient, threadCounts: List<Int>, perThread: Int): List<Pair<Int, Double>> =
        threadCounts.map { threads ->
            val ready = CountDownLatch(threads)
            val go = CountDownLatch(1)
            val wall = System.nanoTime()
            val pool = (1..threads).map {
                Thread {
                    ready.countDown(); go.await()
                    repeat(perThread) { call(p, UsedActions.GET_ME, "{}") }
                }.apply { start() }
            }
            ready.await(); go.countDown(); pool.forEach { it.join() }
            val ms = (System.nanoTime() - wall) / 1_000_000.0
            threads to (threads * perThread) * 1000.0 / ms
        }

    fun run(samples: Int, perThread: Int) {
        println("Performance — путь Desktop → JNI → C ABI → Photon Core")
        println(if (online) "режим: с Bridge ($endpoint)" else "режим: БЕЗ Bridge — только локальные слои")
        println()

        val before = MemorySnapshot.take()

        PhotonClient.create(config()).use { p ->
            println("── задержка по слоям, $samples измерений после прогрева ".padEnd(64, '─'))
            val list = probes(p, samples)
            list.forEach { pr ->
                val net = if (pr.network) "включает сеть" else "без сети"
                println("\n  ${pr.name}  [$net]")
                println("    проходит: ${pr.includes}")
                println("    min ${f3(pr.latency.min)}  p50 ${f3(pr.latency.p50)}  " +
                        "p95 ${f3(pr.latency.p95)}  p99 ${f3(pr.latency.p99)}  max ${f3(pr.latency.max)}  (мс)")
            }

            printCore(p, list)

            val local = list.lastOrNull { !it.network }
            val full = list.firstOrNull { it.network }
            if (local != null && full != null) {
                val derived = derivedNetworkShare(full.latency.p50, local.latency.p50)
                val share = if (full.latency.p50 > 0) derived / full.latency.p50 * 100 else 0.0
                println("\n── доля сети и сервера — ПРОИЗВОДНАЯ, не измерение ".padEnd(64, '─'))
                println("    p50 полного пути      ${f3(full.latency.p50)} мс")
                println("    p50 локального пути   ${f3(local.latency.p50)} мс")
                println("    разность              ${f3(derived)} мс  (${f1(share)}% полного)")
                println("    Это арифметика над медианами двух РАЗНЫХ вызовов, а не")
                println("    измеренный RTT: отделить сеть от работы сервера нечем.")
            }

            if (online) {
                println("\n── пропускная способность, развёртка потоков ".padEnd(64, '─'))
                val sweep = throughputSweep(p, listOf(1, 2, 4, 8), perThread)
                val base = sweep.first().second
                sweep.forEach { (threads, tp) ->
                    println("    %2d поток(ов): %7s оп/с   масштабирование ×%s"
                        .format(threads, f1(tp), f2(scaling(base, tp))))
                }
                println("    Идеальное масштабирование недостижимо: вызовы делят одно")
                println("    QUIC-соединение и один инстанс.")
            }

            val after = MemorySnapshot.take()
            println("\n── память ".padEnd(64, '─'))
            println("    куча JVM:   ${f1(before.jvmUsedBytes / 1048576.0)} → " +
                    "${f1(after.jvmUsedBytes / 1048576.0)} МиБ")
            println("    RSS:        ${before.rssKb / 1024} → ${after.rssKb / 1024} МиБ " +
                    "(пик ${after.hwmKb / 1024} МиБ)")
            println("    RSS включает И кучу JVM, И память Go-ядра: оно живёт в ЭТОМ же")
            println("    процессе. Разделить их без нового API нечем — попытка")
            println("    приписать разность Go была бы догадкой, а не измерением.")

            printLimits()
        }
    }

    /**
     * Класс исхода и время внутри ядра — по журналу ЯДРА (ADR-130 §2.7).
     * Время вызова внутри ядра (гистограмма `calls.duration`) показано рядом
     * с полным временем пути из JVM: разность — JNI, cgo и граница ABI.
     */
    private fun printCore(p: PhotonClient, list: List<Probe>) {
        println("\n── по журналу ядра (ADR-130 §2.7) ─────────────────")
        val s = p.diagnostics().snapshot()
        if (!s.enabled) {
            println("  диагностика выключена конфигурацией — данных ядра нет")
            return
        }
        val c = s.calls ?: return
        println("  вызовы: всего ${c.total}, по классам ${c.byClass.toSortedMap()}")
        val journal = CoreJournal(p.diagnosticsSource())
        val failed = journal.pull().filter(CoreJournal.callFailed)
        val unknown = failed.lastOrNull { CoreJournal.num(it, "action") == UsedActions.DELIBERATELY_UNKNOWN.toLong() }
        println("  класс исхода зонда «неизвестный код» по журналу: " +
            (unknown?.let { CoreJournal.text(it, "class") } ?: "в журнале нет (вытеснен?)"))
        val core = meanCoreMicros(c.duration)
        val local = list.lastOrNull { !it.network && it.name.startsWith("sendAction") }
        if (core != null && local != null) {
            println("  время внутри ядра (среднее по всем вызовам): ${f3(core / 1000)} мс; " +
                "полный путь из JVM (p50 зонда): ${f3(local.latency.p50)} мс")
            println("  разность — JNI, cgo и граница ABI; ядро считает время своими монотонными часами")
        }
    }

    private fun printLimits() {
        println("\n── что этим НЕ измерено ".padEnd(64, '─'))
        println("  • сетевой RTT сам по себе: доля сети получена вычитанием и")
        println("    включает работу сервера. Отдельно они не разделимы клиентом.")
        println("  • внутренности ядра: pprof, GC и счётчики горутин Go наружу не")
        println("    выведены — это потребовало бы диагностического API.")
        println("  • распределение памяти между JVM и Go внутри одного процесса.")
        println("  • измерения зависят от машины и сети: воспроизводимы условия")
        println("    (прогрев, число измерений, метод перцентилей), а не числа.")
    }
}
