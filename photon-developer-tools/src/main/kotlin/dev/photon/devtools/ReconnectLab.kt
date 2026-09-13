package dev.photon.devtools

import dev.photon.core.Photon
import dev.photon.core.PhotonEventListener
import java.io.File
import java.util.Locale
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

/*
 * Reconnect Lab — обрыв и восстановление связи через публичный путь.
 *
 * Kotlin → JNI → C ABI → Photon Core → локальный UDP-ретранслятор → живой
 * Bridge. Инстанс создаётся обычным Photon_Create: bridge.endpoint указывает
 * на ретранслятор, bridge.tls.server_name — на настоящее имя Bridge. Ни
 * нового действия, ни новой возможности ABI.
 *
 * Методика — Phase 15.5 (eventcheck: 4-disconnect, 5-independence,
 * 8-two-reconnects): CONNECTED → разрыв пути → ожидание NETWORK_CHANGED до
 * 90 с → восстановление пути → CONNECTED до 60 с; два цикла подряд. Путь
 * восстанавливается ТОЛЬКО после того, как обрыв обнаружен: иначе старое
 * соединение пережило бы разрыв через новый исходный порт ретранслятора, и
 * проверялась бы миграция пути, а не восстановление.
 *
 * Здесь добавлено то, чего Phase 15.5 через Go не видела: граница JNI
 * (последовательность доставок, отсутствие колбэков после close) и действия
 * во время и после обрыва — тем же вызовом, которым пользуется приложение.
 */

val RECONNECT_BOUNDARIES: List<String> = listOf(
    "это ИСКУССТВЕННЫЙ обрыв UDP-пути локальным ретранслятором: пакеты перестают ходить, QUIC обнаруживает это сам",
    "это НЕ проверка смены сетевого интерфейса (Wi-Fi ↔ сотовая, down/up интерфейса)",
    "это НЕ проверка убийства сервера или перезапуска Bridge",
    "это НЕ NAT rebinding: путь восстанавливается только после обнаруженного обрыва, старое соединение к тому моменту мертво",
    "это НЕ симметричный NAT и вообще не traversal: клиент и ретранслятор на одной машине",
)

// ── модель прогона ───────────────────────────────────────────────────────────

enum class MarkKind { CONNECTED, NETWORK_CHANGED, MESSAGE, OTHER_EVENT, BREAK, HEAL, ACTION, CLOSE_RETURNED }

/** Отметка таймлайна. Время — наносекунды от начала прогона, монотонные. */
data class Mark(val atNanos: Long, val kind: MarkKind, val detail: String = "")

enum class ProbePhase { BEFORE, DURING, AFTER }

/** Действие, отправленное лабораторией. [outcome] == null — вызов не вернулся в срок. */
data class ActionProbe(
    val label: String,
    val cycle: Int,
    val phase: ProbePhase,
    val outcome: Outcome?,
    val detail: String,
    val startedNanos: Long,
    val millis: Double,
) {
    val hung: Boolean get() = outcome == null
}

data class Check(val name: String, val ok: Boolean, val detail: String)

private fun ms(nanos: Long): String = String.format(Locale.ROOT, "%.0f мс", nanos / 1_000_000.0)

val EVENT_KINDS = setOf(MarkKind.CONNECTED, MarkKind.NETWORK_CHANGED, MarkKind.MESSAGE, MarkKind.OTHER_EVENT)

/**
 * Выводы прогона из фактов. Чистая функция: проверяется без сети, и каждое
 * правило ломается своей мутацией.
 *
 * Правила не задают, КАКОЙ код ошибки обязан вернуть вызов во время обрыва:
 * это наблюдение, а не контракт. Задано только то, что следует из смысла
 * обрыва: ответа сервера на оборванном пути быть не может.
 */
fun analyzeReconnect(
    marks: List<Mark>,
    probes: List<ActionProbe>,
    droppedPerCycle: List<Long>,
    maxConcurrentCallbacks: Int,
    closeNanos: Long?,
    closeBudgetNanos: Long,
): List<Check> {
    val checks = mutableListOf<Check>()
    val sorted = marks.sortedBy { it.atNanos }
    val breaks = sorted.filter { it.kind == MarkKind.BREAK }.map { it.atNanos }
    val heals = sorted.filter { it.kind == MarkKind.HEAL }.map { it.atNanos }
    val closeAt = sorted.firstOrNull { it.kind == MarkKind.CLOSE_RETURNED }?.atNanos
    fun events(kind: MarkKind, from: Long, to: Long) =
        sorted.filter { it.kind == kind && it.atNanos > from && it.atNanos < to }

    if (breaks.isEmpty()) {
        checks.add(Check("обрыв выполнялся", false, "ни одного разрыва пути в прогоне"))
    }

    for ((i, b) in breaks.withIndex()) {
        val n = i + 1
        val prevHeal = heals.getOrNull(i - 1) ?: Long.MIN_VALUE
        val heal = heals.getOrNull(i) ?: Long.MAX_VALUE
        val nextBoundary = breaks.getOrNull(i + 1) ?: closeAt ?: Long.MAX_VALUE

        val before = events(MarkKind.CONNECTED, prevHeal, b)
        checks.add(Check("цикл $n: CONNECTED до обрыва", before.isNotEmpty(),
            if (before.isEmpty()) "подписка не была поднята — обрывать нечего" else "есть"))

        val dropped = droppedPerCycle.getOrNull(i) ?: 0
        checks.add(Check("цикл $n: путь действительно оборван", dropped > 0,
            "ретранслятор отбросил пакетов клиента: $dropped" +
                if (dropped == 0L) " — клиент ничего не слал, обрыв не проверен" else ""))

        val lost = events(MarkKind.NETWORK_CHANGED, b, heal).firstOrNull()
        checks.add(Check("цикл $n: обрыв обнаружен до восстановления пути", lost != null,
            if (lost == null) "NETWORK_CHANGED не пришёл" else "NETWORK_CHANGED через ${ms(lost.atNanos - b)} после разрыва"))

        val fake = events(MarkKind.CONNECTED, b, heal)
        checks.add(Check("цикл $n: через оборванный путь CONNECTED не приходил", fake.isEmpty(),
            if (fake.isEmpty()) "нет" else "CONNECTED при оборванном пути — обрыв не настоящий"))

        if (heal != Long.MAX_VALUE) {
            val back = events(MarkKind.CONNECTED, heal, nextBoundary).firstOrNull()
            checks.add(Check("цикл $n: CONNECTED после восстановления", back != null,
                if (back == null) "подписка не восстановилась" else "через ${ms(back.atNanos - heal)} после восстановления пути"))
        } else {
            checks.add(Check("цикл $n: путь восстанавливался", false, "восстановления не было"))
        }

        val during = probes.filter { it.cycle == n && it.phase == ProbePhase.DURING }
        val answered = during.filter { it.outcome == Outcome.OK || it.outcome == Outcome.VERDICT_FALSE }
        if (during.isNotEmpty()) {
            checks.add(Check("цикл $n: во время обрыва ответа сервера не было", answered.isEmpty(),
                if (answered.isEmpty()) "все ${during.size} вызова — ошибка вызова"
                else "ответ сервера на оборванном пути: ${answered.map { it.label }}"))
        }

        val after = probes.filter { it.cycle == n && it.phase == ProbePhase.AFTER }
        checks.add(Check("цикл $n: действие после восстановления выполнено",
            after.any { it.outcome == Outcome.OK },
            after.joinToString { "${it.label}=${it.outcome ?: "ЗАВИС"}" }.ifEmpty { "действий не было" }))
    }

    checks.add(Check("до обрыва действие выполнено",
        probes.any { it.phase == ProbePhase.BEFORE && it.outcome == Outcome.OK },
        probes.filter { it.phase == ProbePhase.BEFORE }.joinToString { "${it.outcome}" }.ifEmpty { "действий не было" }))

    val stray = sorted.filter { m ->
        m.kind == MarkKind.NETWORK_CHANGED &&
            breaks.indices.none { i -> m.atNanos > breaks[i] && m.atNanos < (heals.getOrNull(i) ?: Long.MAX_VALUE) }
    }
    checks.add(Check("NETWORK_CHANGED только при оборванном пути", stray.isEmpty(),
        if (stray.isEmpty()) "да" else "вне обрыва: ${stray.size} — ложное обнаружение или нестабильный reconnect"))

    val hung = probes.filter { it.hung }
    checks.add(Check("ни один вызов не завис", hung.isEmpty(),
        if (hung.isEmpty()) "все ${probes.size} вернулись в срок" else "не вернулись: ${hung.map { it.label }}"))

    checks.add(Check("доставки событий не параллельны (jni-events D1)", maxConcurrentCallbacks <= 1,
        "одновременно в слушателе максимум: $maxConcurrentCallbacks"))

    checks.add(Check("close() вернулся в срок", closeNanos != null && closeNanos <= closeBudgetNanos,
        if (closeNanos == null) "не вернулся" else "за ${ms(closeNanos)}"))

    if (closeAt != null) {
        val late = sorted.filter { it.kind in EVENT_KINDS && it.atNanos > closeAt }
        checks.add(Check("после close() колбэков нет", late.isEmpty(),
            if (late.isEmpty()) "нет" else "пришло после close: ${late.map { it.kind }}"))
    }
    return checks
}

/** Тип события из event_json. Тот же разбор, что у Dashboard и Logging. */
fun eventKind(json: String): MarkKind =
    when (Regex("\"type\"\\s*:\\s*\"([A-Z_]+)\"").find(json)?.groupValues?.get(1)) {
        "CONNECTED" -> MarkKind.CONNECTED
        "NETWORK_CHANGED" -> MarkKind.NETWORK_CHANGED
        "MESSAGE" -> MarkKind.MESSAGE
        else -> MarkKind.OTHER_EVENT
    }

/** Число открытых дескрипторов процесса — наблюдение, а не порог. */
fun openDescriptors(): Int? = File("/proc/self/fd").list()?.size

// ── прогон ───────────────────────────────────────────────────────────────────

class ReconnectLab(private val endpoint: String, private val sessionId: String) {

    private val t0 = System.nanoTime()
    private fun now() = System.nanoTime() - t0

    private val marks = java.util.Collections.synchronizedList(mutableListOf<Mark>())
    private val probes = java.util.Collections.synchronizedList(mutableListOf<ActionProbe>())
    private val inCallback = AtomicInteger()
    private val maxInCallback = AtomicInteger()
    private val pool = Executors.newCachedThreadPool { r -> Thread(r, "reconnect-probe").apply { isDaemon = true } }

    private val connectTimeoutMs = 15_000L
    private val callTimeoutMs = 20_000L
    /** Предел, после которого вызов считается зависшим: дольше не ждёт даже
     *  вызов, которому пришлось переподключаться. */
    private val probeBudgetMs = connectTimeoutMs + callTimeoutMs + 10_000L
    private val closeBudgetNanos = TimeUnit.SECONDS.toNanos(15)

    private fun host(): String = endpoint.substring(0, endpoint.lastIndexOf(':'))

    private fun config(relayPort: Int) =
        """{"bridge":{"endpoint":"127.0.0.1:$relayPort","session_token":"$sessionId",""" +
            """"connect_timeout_ms":$connectTimeoutMs,"call_timeout_ms":$callTimeoutMs,""" +
            """"event_backoff_min_ms":1000,"event_backoff_max_ms":5000,""" +
            """"tls":{"server_name":"${host()}"}}}"""

    private fun mark(kind: MarkKind, detail: String = "") {
        marks.add(Mark(now(), kind, detail))
        println("  ${stamp(now())}  ${kind.name.padEnd(15)} $detail")
    }

    private fun stamp(n: Long) = String.format(Locale.ROOT, "+%8.3f с", n / 1e9)

    private fun count(kind: MarkKind) = synchronized(marks) { marks.count { it.kind == kind } }

    private fun waitFor(kind: MarkKind, n: Int, seconds: Long): Boolean {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds)
        while (System.nanoTime() < deadline) {
            if (count(kind) >= n) return true
            Thread.sleep(50)
        }
        return count(kind) >= n
    }

    private fun startProbe(p: Photon, label: String, cycle: Int, phase: ProbePhase): Pair<Long, Future<CallResult>> {
        val started = now()
        mark(MarkKind.ACTION, "→ $label (getMe)")
        return started to pool.submit(Callable { call(p, UsedActions.GET_ME, "{}") })
    }

    private fun finishProbe(f: Pair<Long, Future<CallResult>>, label: String, cycle: Int, phase: ProbePhase): ActionProbe {
        val probe = try {
            val r = f.second.get(probeBudgetMs, TimeUnit.MILLISECONDS)
            ActionProbe(label, cycle, phase, r.outcome, r.detail, f.first, r.millis)
        } catch (e: TimeoutException) {
            ActionProbe(label, cycle, phase, null, "не вернулся за $probeBudgetMs мс", f.first, probeBudgetMs.toDouble())
        }
        probes.add(probe)
        mark(MarkKind.ACTION, "← $label: ${probe.outcome ?: "ЗАВИС"} за ${String.format(Locale.ROOT, "%.1f", probe.millis)} мс — ${probe.detail.take(110)}")
        return probe
    }

    private fun probe(p: Photon, label: String, cycle: Int, phase: ProbePhase) =
        finishProbe(startProbe(p, label, cycle, phase), label, cycle, phase)

    fun run(cycles: Int = 2) {
        println("Reconnect Lab — обрыв и восстановление через публичный путь")
        println("Kotlin → JNI → C ABI → Core → локальный UDP-ретранслятор (отдельный процесс) → $endpoint\n")

        val relay = RelayProcess.start(endpoint)
        println("  ретранслятор: pid ${relay.pid}, 127.0.0.1:${relay.port} → ${resolveUdp4(endpoint)}")
        println("  конфиг: endpoint=127.0.0.1:${relay.port}, server_name=${host()}, connect ${connectTimeoutMs} мс, call ${callTimeoutMs} мс, backoff 1000..5000 мс\n")

        val dropped = mutableListOf<Long>()
        val relayLog = mutableListOf<String>()
        var closeNanos: Long? = null
        var fdAfter: Int? = null

        try {
            val photon = Photon.create(config(relay.port))
            photon.setEventListener(object : PhotonEventListener {
                override fun onEvent(eventJson: String) {
                    val c = inCallback.incrementAndGet()
                    maxInCallback.accumulateAndGet(c) { a, b -> maxOf(a, b) }
                    try {
                        val kind = eventKind(eventJson)
                        // Содержимое MESSAGE не печатается: там данные
                        // пользователя, а лаборатории нужен только факт.
                        mark(kind, if (kind == MarkKind.MESSAGE) "" else eventJson.take(120))
                    } finally {
                        inCallback.decrementAndGet()
                    }
                }
            })

            println("── подписка и действие до обрыва ────────────────")
            if (!waitFor(MarkKind.CONNECTED, 1, 30)) println("  ❌ CONNECTED не пришёл за 30 с")
            probe(photon, "до обрыва #1", 1, ProbePhase.BEFORE)
            probe(photon, "до обрыва #2", 1, ProbePhase.BEFORE)
            val s0 = relay.stats().stats
            // Ожидается 2 активных (action + подписка); больше — значит, были
            // повторные попытки подключения, каждая с нового сокета.
            relayLog.add("до обрыва: потоков открыто ${s0?.flowsOpened}, активно ${s0?.flowsActive}")
            fdBeforeHolder = openDescriptors()

            for (cycle in 1..cycles) {
                println("\n── цикл $cycle: разрыв пути ──────────────────────────")
                val droppedBefore = relay.stats().stats?.upDropped ?: 0
                relay.breakPath()
                mark(MarkKind.BREAK, "ретранслятор роняет пакеты в обе стороны")

                // Сразу после разрыва: соединение для клиента ещё «живое», и
                // вызов уходит в него. Что вернётся — наблюдение.
                val immediate = if (cycle == 1) startProbe(photon, "сразу после разрыва", cycle, ProbePhase.DURING) else null

                val detected = waitFor(MarkKind.NETWORK_CHANGED, cycle, 90)
                if (!detected) println("  ❌ NETWORK_CHANGED не пришёл за 90 с")

                if (cycle == 1) {
                    // После обнаружения обрыва: соединение уже мёртвое.
                    probe(photon, "после NETWORK_CHANGED, путь оборван", cycle, ProbePhase.DURING)
                    immediate?.let { finishProbe(it, "сразу после разрыва", cycle, ProbePhase.DURING) }
                }
                val droppedAfter = relay.stats().stats?.upDropped ?: 0
                dropped.add(droppedAfter - droppedBefore)
                relayLog.add("цикл $cycle: за обрыв отброшено пакетов клиента ${droppedAfter - droppedBefore}")

                relay.healPath()
                mark(MarkKind.HEAL, "путь восстановлен")
                if (!waitFor(MarkKind.CONNECTED, cycle + 1, 60)) println("  ❌ CONNECTED после восстановления не пришёл за 60 с")

                if (cycle == 1) {
                    // Последовательно, пока не выполнится: первый вызов может
                    // узнать об обрыве action-соединения, следующий — поднять новое.
                    for (k in 1..3) {
                        if (probe(photon, "после восстановления #$k", cycle, ProbePhase.AFTER).outcome == Outcome.OK) break
                    }
                } else {
                    // Конкурентно: несколько вызовов на восстанавливающемся
                    // соединении не должны ни зависнуть, ни поднять по соединению каждый.
                    val flowsBefore = relay.stats().stats?.flowsOpened ?: 0
                    val started = (1..4).map { k -> startProbe(photon, "конкурентно #$k", cycle, ProbePhase.AFTER) }
                    started.forEachIndexed { k, f -> finishProbe(f, "конкурентно #${k + 1}", cycle, ProbePhase.AFTER) }
                    probe(photon, "после конкурентных", cycle, ProbePhase.AFTER)
                    val flowsAfter = relay.stats().stats?.flowsOpened ?: 0
                    relayLog.add("цикл $cycle: новых потоков за 4 конкурентных + 1 вызов: ${flowsAfter - flowsBefore}")
                }
            }

            fdAfter = openDescriptors()
            println("\n── close() ─────────────────────────────────────")
            val closer = Thread { photon.close() }
            val closeStart = System.nanoTime()
            closer.start()
            closer.join(TimeUnit.NANOSECONDS.toMillis(closeBudgetNanos))
            if (!closer.isAlive) {
                closeNanos = System.nanoTime() - closeStart
                mark(MarkKind.CLOSE_RETURNED, "за ${ms(closeNanos!!)}")
            } else {
                println("  ❌ close() не вернулся за 15 с")
            }
            Thread.sleep(2000) // окно, в котором запоздалый колбэк успел бы прийти
            val s = relay.stats().stats
            relayLog.add("итого: вперёд ${s?.upForwarded}, назад ${s?.downForwarded}, отброшено ${s?.upDropped}/${s?.downDropped}, потоков открыто ${s?.flowsOpened}")
        } finally {
            relay.close()
            pool.shutdownNow()
        }

        report(analyzeReconnect(marks.toList(), probes.toList(), dropped, maxInCallback.get(),
            closeNanos, closeBudgetNanos), relayLog, fdBeforeHolder, fdAfter)
    }

    private var fdBeforeHolder: Int? = null

    private fun report(checks: List<Check>, relayLog: List<String>, fdBefore: Int?, fdAfter: Int?) {
        println("\n── ретранслятор ────────────────────────────────")
        relayLog.forEach { println("  $it") }

        println("\n── таймлайн событий подписки ───────────────────")
        marks.toList().sortedBy { it.atNanos }.filter { it.kind != MarkKind.ACTION }
            .forEach { println("  ${stamp(it.atNanos)}  ${it.kind}") }

        println("\n── действия ────────────────────────────────────")
        probes.toList().forEach {
            println("  ${it.label.padEnd(36)} ${(it.outcome?.name ?: "ЗАВИС").padEnd(13)} " +
                String.format(Locale.ROOT, "%9.1f мс", it.millis) + "  ${it.detail.take(100)}")
        }

        println("\n── дескрипторы процесса (наблюдение, не порог) ──")
        println("  до обрывов: $fdBefore, после циклов: $fdAfter")

        println("\n── проверки ────────────────────────────────────")
        checks.forEach { println("  ${if (it.ok) "✅" else "❌"} ${it.name}: ${it.detail}") }
        val failed = checks.count { !it.ok }
        println("\n  ИТОГ: проверок ${checks.size}, нарушено $failed")

        println("\n── границы проверки ────────────────────────────")
        RECONNECT_BOUNDARIES.forEach { println("  • $it") }
    }
}
