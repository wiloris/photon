package dev.photon.devtools

import dev.photon.core.Photon
import dev.photon.core.PhotonEventListener
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.Locale
import kotlin.math.ceil
import kotlin.math.roundToLong

/*
 * Network Lab — диагностический режим Photon Developer Tools.
 *
 * Ходит ТЕМ ЖЕ публичным путём, что и Dashboard: Kotlin → JNI → C ABI →
 * Photon Core. Ни нового действия, ни новой возможности ABI здесь нет и не
 * требуется — измеряется то, что видно клиенту (ROADMAP.md, Phase 16,
 * «Public API constraint»).
 *
 * Что измеряется на самом деле, а что нет, написано в отчёте прогона явно:
 * выдавать имитацию за проверку хуже, чем не проверять.
 */

private val labClock = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")

/*
 * Форматирование чисел ЯВНО в Locale.ROOT.
 *
 * Найдено тестом: String.format зависит от локали JVM, и в русской «4.00»
 * превращается в «4,00». Числа прогона перестали бы быть сравнимыми между
 * машинами — а сравнимость и есть смысл этих чисел.
 */
private fun fmt1(v: Double): String = String.format(Locale.ROOT, "%.1f", v)
private fun fmt2(v: Double): String = String.format(Locale.ROOT, "%.2f", v)

private fun now(): String = LocalTime.now().format(labClock)

/** Отметка события подписки в хронологическом порядке. */
data class EventMark(val at: String, val type: String, val sinceStartMs: Long)

/**
 * Сводка задержек.
 *
 * Перцентили — методом nearest-rank: p_k равен ⌈k/100·N⌉-му по возрастанию.
 * Метод назван намеренно: у перцентиля нет одного определения, и молча
 * выбранное сделало бы числа несравнимыми между прогонами.
 */
class Latencies(samples: List<Double>) {
    private val sorted = samples.sorted()
    val count: Int get() = sorted.size

    fun percentile(k: Int): Double {
        if (sorted.isEmpty()) return 0.0
        val rank = ceil(k / 100.0 * sorted.size).toInt().coerceIn(1, sorted.size)
        return sorted[rank - 1]
    }

    val min: Double get() = if (sorted.isEmpty()) 0.0 else sorted.first()
    val max: Double get() = if (sorted.isEmpty()) 0.0 else sorted.last()
    val p50: Double get() = percentile(50)
    val p95: Double get() = percentile(95)
    val p99: Double get() = percentile(99)
}

/** Итог одного сценария. */
class ScenarioResult(
    val name: String,
    val note: String,
    val wallMillis: Long,
    results: List<CallResult>,
) {
    val ok = results.count { it.outcome == Outcome.OK }
    val verdictFalse = results.count { it.outcome == Outcome.VERDICT_FALSE }
    val callError = results.count { it.outcome == Outcome.CALL_ERROR }
    val total = results.size

    /** Задержки считаются по СОСТОЯВШИМСЯ вызовам: у несостоявшегося измеряется
     *  время до отказа, и смешивать их значило бы усреднять разные величины. */
    val latency = Latencies(results.filter { it.outcome != Outcome.CALL_ERROR }.map { it.millis })

    val throughput: Double get() = if (wallMillis == 0L) 0.0 else total * 1000.0 / wallMillis

    fun print() {
        println("\n── сценарий: $name ".padEnd(52, '─'))
        if (note.isNotEmpty()) println("  $note")
        println("  операций:        $total за $wallMillis мс")
        println("  ok:true          $ok")
        println("  вердикт ok:false $verdictFalse   ← вызов СОСТОЯЛСЯ, отказал сервер")
        println("  ошибка вызова    $callError   ← вызов НЕ состоялся")
        println("  пропускная спос. ${fmt1(throughput)} оп/с")
        if (latency.count > 0) {
            println("  задержка, мс (по ${latency.count} состоявшимся вызовам):")
            println("    min ${f(latency.min)}   p50 ${f(latency.p50)}   p95 ${f(latency.p95)}" +
                    "   p99 ${f(latency.p99)}   max ${f(latency.max)}")
        } else {
            println("  задержка: не измерена — ни один вызов не состоялся")
        }
    }

    private fun f(v: Double) = fmt1(v)
}

/**
 * Сравнение начала и конца серии — признак деградации соединения.
 *
 * Порога здесь нет и быть не должно: сеть шумит, и жёсткий порог давал бы
 * ложные срабатывания. Отношение показывается как ИЗМЕРЕНИЕ, вывод делает
 * человек.
 */
fun degradation(samples: List<Double>): String {
    if (samples.size < 20) return "серия короче 20 вызовов — сравнивать нечего"
    val slice = samples.size / 10
    val head = samples.take(slice).sorted()
    val tail = samples.takeLast(slice).sorted()
    val hp50 = head[head.size / 2]
    val tp50 = tail[tail.size / 2]
    val ratio = if (hp50 == 0.0) 0.0 else tp50 / hp50
    return "p50 первых $slice = ${fmt1(hp50)} мс, последних $slice = " +
           "${fmt1(tp50)} мс, отношение ${fmt2(ratio)}"
}

class NetworkLab(private val endpoint: String, private val token: String) {

    private val events = ConcurrentLinkedQueue<EventMark>()
    private val startedAt = System.nanoTime()

    private fun config() =
        """{"bridge":{"endpoint":"$endpoint","session_token":"$token",""" +
        """"connect_timeout_ms":15000,"call_timeout_ms":20000}}"""

    private fun listener() = object : PhotonEventListener {
        override fun onEvent(eventJson: String) {
            val type = eventType(eventJson) ?: "?"
            events.add(EventMark(now(), type, (System.nanoTime() - startedAt) / 1_000_000))
        }
    }

    /**
     * Серия последовательных действий на ОДНОМ соединении.
     *
     * Действие 200 (`getMe`) выбрано как самое дешёвое из доступных и не
     * меняющее состояние: сценарий можно гонять сколько угодно раз, не
     * оставляя следов на сервере.
     */
    private fun serial(p: Photon, n: Int, name: String, note: String): Pair<ScenarioResult, List<Double>> {
        val results = ArrayList<CallResult>(n)
        val wall = System.nanoTime()
        repeat(n) { results.add(call(p, UsedActions.GET_ME, "{}")) }
        val ms = (System.nanoTime() - wall) / 1_000_000
        val r = ScenarioResult(name, note, ms, results)
        return r to results.filter { it.outcome != Outcome.CALL_ERROR }.map { it.millis }
    }

    /** Конкурентные действия на одном инстансе: путь не должен становиться последовательным. */
    private fun concurrent(p: Photon, threads: Int, perThread: Int): ScenarioResult {
        val results = ConcurrentLinkedQueue<CallResult>()
        val ready = CountDownLatch(threads)
        val go = CountDownLatch(1)
        val wall = System.nanoTime()
        val pool = (1..threads).map {
            Thread {
                ready.countDown()
                go.await()
                repeat(perThread) { results.add(call(p, UsedActions.GET_ME, "{}")) }
            }.apply { start() }
        }
        ready.await()
        go.countDown()
        pool.forEach { it.join() }
        val ms = (System.nanoTime() - wall) / 1_000_000
        return ScenarioResult("конкурентный, $threads×$perThread",
            "все потоки работают с ОДНИМ инстансом", ms, results.toList())
    }

    fun run(serialCount: Int, threads: Int, perThread: Int, soakSeconds: Int) {
        println("Network Lab — $endpoint")
        println("путь: Kotlin → JNI → C ABI → Photon Core; новых действий и возможностей ABI нет\n")

        Photon.create(config()).use { p ->
            p.setEventListener(listener())
            println("  слушатель установлен, подписка запущена")

            // 1. Базовая серия.
            val (base, baseSamples) = serial(p, serialCount, "последовательный, $serialCount вызовов",
                "одно соединение, действие 200 (getMe) — не меняет состояние")
            base.print()
            println("  деградация: ${degradation(baseSamples)}")

            // 2. Регрессия ADR-050: кредит MAX_STREAMS.
            //
            // Воспроизводится клиентским путём БЕЗ нового API: каждое действие
            // открывает стрим на том же соединении, и до исправления 4215b1d
            // предел в 100 вёл себя как пожизненный — сотое действие вставало
            // навсегда. 130 > 100, значит сценарий действительно проходит через
            // прежнюю границу отказа.
            val (credit, _) = serial(p, 130, "кредит MAX_STREAMS, 130 вызовов",
                "регрессия ADR-050: >100 стримов на ОДНОМ соединении без переподключения")
            credit.print()
            println("  вывод: " + if (credit.callError == 0)
                "кредит возвращается — все 130 прошли"
            else
                "ОТКАЗ на ${credit.callError} вызовах — похоже на деградацию кредита")

            // 3. Конкурентность.
            concurrent(p, threads, perThread).print()

            // 4. Длительный прогон.
            if (soakSeconds > 0) {
                val results = ArrayList<CallResult>()
                val samples = ArrayList<Double>()
                val deadline = System.nanoTime() + soakSeconds * 1_000_000_000L
                val wall = System.nanoTime()
                while (System.nanoTime() < deadline) {
                    val r = call(p, UsedActions.GET_ME, "{}")
                    results.add(r)
                    if (r.outcome != Outcome.CALL_ERROR) samples.add(r.millis)
                    Thread.sleep(50)
                }
                val ms = (System.nanoTime() - wall) / 1_000_000
                val soak = ScenarioResult("длительный, $soakSeconds с",
                    "вызов раз в 50 мс — поиск деградации соединения со временем", ms, results)
                soak.print()
                println("  деградация: ${degradation(samples)}")
            }

            printEvents()
            printLimits()
        }
    }

    private fun printEvents() {
        println("\n── события подписки, хронологически ──────────────")
        if (events.isEmpty()) {
            println("  ни одного (подписка не поднялась либо прогон короче установления связи)")
        } else {
            events.forEach { println("  ${it.at}  +${it.sinceStartMs} мс  ${it.type}") }
        }
    }

    /**
     * Границы честности прогона.
     *
     * Обрыв связи изнутри приложения не имитируется намеренно: закрыть
     * инстанс — не то же самое, что потерять сеть, и выдавать одно за другое
     * значило бы получить зелёную строку там, где ничего не проверено.
     */
    private fun printLimits() {
        println("\n── что этим прогоном НЕ проверено ────────────────")
        println("  • преднамеренный обрыв и восстановление связи: требует внешнего")
        println("    средства (в Phase 15.5 для этого поднимался UDP-ретранслятор).")
        println("    Внутри приложения не имитируется — имитация не является проверкой.")
        println("    NETWORK_CHANGED показывается, если случится по-настоящему.")
        println("  • внутренняя статистика транспорта (пакеты, окна, повторы),")
        println("    маршруты Autodial, состояние P2P — публичного пути нет.")
        println("  • задержка измеряется КЛИЕНТОМ: в неё входит весь путь,")
        println("    включая JNI, cgo и работу сервера. Это не сетевой RTT.")
    }
}
