package dev.photon.devtools

import dev.photon.sdk.DiagnosticEvent
import dev.photon.sdk.DiagnosticValue
import dev.photon.sdk.DiagnosticsSnapshot
import dev.photon.sdk.DurationHistogram

/*
 * Журнал ядра для лабораторий — ADR-130 §2.7: «Network/Reconnect/Perf Lab
 * берут время обнаружения обрыва, переподключения и класс исхода из журнала
 * ядра». Источник — диагностика ядра через SDK (819/820); своих событий
 * лаборатории не заводят и ничего не симулируют.
 *
 * Время — монотонное время ИНСТАНСА: момент команды лаборатории берётся как
 * `uptime_ms` снимка (база та же, что у `at_mono_ms` событий), поэтому
 * разности «команда → событие» считаются по одним часам — часам ядра.
 */
class CoreJournal(private val source: DiagnosticsSource) {

    private val feed = LiveFeed(source, { null })
    private val log = mutableListOf<DiagnosticEvent>()

    /** Записи, вытесненные до чтения: ненулевое значение — времена ниже неполны. */
    var dropped: Long = 0
        private set

    /** Текущее монотонное время инстанса, мс (`uptime_ms` снимка). */
    fun now(): Long = source.snapshot().uptimeMs

    fun snapshot(): DiagnosticsSnapshot = source.snapshot()

    /** Дочитывает журнал. Перезапуск инстанса сбрасывает накопленное. */
    @Synchronized
    fun pull(): List<DiagnosticEvent> {
        for (line in feed.poll()) when (line) {
            is LiveLine.Event -> log += line.event
            is LiveLine.Gap -> dropped += line.dropped
            is LiveLine.Restart -> log.clear()
        }
        return log.toList()
    }

    /** Панель «Путь» по всему прочитанному журналу (ConnectionPath.kt). */
    fun path(): PathView = pathFrom(pull(), dropped)

    /** Первое событие с `at_mono_ms ≥ from`, удовлетворяющее условию. */
    fun first(from: Long, pred: (DiagnosticEvent) -> Boolean): DiagnosticEvent? =
        pull().firstOrNull { it.atMonoMs >= from && pred(it) }

    /** Ждёт такое событие не дольше [timeoutMs]. */
    fun await(from: Long, timeoutMs: Long, pred: (DiagnosticEvent) -> Boolean): DiagnosticEvent? {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (true) {
            first(from, pred)?.let { return it }
            if (System.currentTimeMillis() > deadline) return null
            Thread.sleep(25)
        }
    }

    companion object {
        fun text(e: DiagnosticEvent, key: String): String? = (e.fields[key] as? DiagnosticValue.Text)?.value
        fun num(e: DiagnosticEvent, key: String): Long? = (e.fields[key] as? DiagnosticValue.Num)?.value

        /** Сессия соединения [conn] потеряна (не «не подключилась»). */
        fun lost(conn: String): (DiagnosticEvent) -> Boolean =
            { it.component == "session" && it.kind == "failed" && it.conn == conn && text(it, "class") == "lost" }

        /** Подключение соединения [conn] удалось. */
        fun connected(conn: String): (DiagnosticEvent) -> Boolean =
            { it.component == "dispatcher" && it.kind == "conn_result" && it.conn == conn && text(it, "class") == "ok" }

        /** Клиенту отдано событие подписки [type] (CONNECTED / NETWORK_CHANGED). */
        fun sent(type: String): (DiagnosticEvent) -> Boolean =
            { it.component == "events" && it.kind == "event_sent" && text(it, "event") == type }

        /** Вызов завершился отказом. */
        val callFailed: (DiagnosticEvent) -> Boolean = { it.component == "dispatcher" && it.kind == "call_failed" }
    }
}

/** Времена одного цикла «обрыв → восстановление» по журналу ядра, мс; null — события нет. */
data class JournalCycle(
    val cycle: Int,
    val detectMs: Long?,          // обрыв → потеря подписки (session.failed lost)
    val networkChangedMs: Long?,  // обрыв → NETWORK_CHANGED отдан клиенту
    val transportSubMs: Long?,    // восстановление → подписка подключена
    val transportActionMs: Long?, // восстановление → action подключён (по первому вызову)
    val sessionConnectedMs: Long?, // восстановление → CONNECTED отдан клиенту
    val failedClasses: List<String>, // классы отказавших вызовов в окне обрыва
)

/**
 * Разбор циклов по журналу: [breaks] и [heals] — моменты команд ретранслятору
 * в монотонном времени инстанса (`CoreJournal.now()`), по одному на цикл.
 */
fun journalCycles(events: List<DiagnosticEvent>, breaks: List<Long>, heals: List<Long>): List<JournalCycle> =
    breaks.indices.map { i ->
        val b = breaks[i]
        val h = heals.getOrNull(i) ?: Long.MAX_VALUE
        val next = breaks.getOrNull(i + 1) ?: Long.MAX_VALUE
        fun firstIn(from: Long, to: Long, pred: (DiagnosticEvent) -> Boolean) =
            events.firstOrNull { it.atMonoMs in from until to && pred(it) }?.let { it.atMonoMs - from }
        JournalCycle(
            cycle = i + 1,
            detectMs = firstIn(b, h, CoreJournal.lost("subscribe")),
            networkChangedMs = firstIn(b, h, CoreJournal.sent("NETWORK_CHANGED")),
            transportSubMs = if (h == Long.MAX_VALUE) null else firstIn(h, next, CoreJournal.connected("subscribe")),
            transportActionMs = if (h == Long.MAX_VALUE) null else firstIn(h, next, CoreJournal.connected("action")),
            sessionConnectedMs = if (h == Long.MAX_VALUE) null else firstIn(h, next, CoreJournal.sent("CONNECTED")),
            failedClasses = events.filter { it.atMonoMs in b until next && CoreJournal.callFailed(it) }
                .mapNotNull { CoreJournal.text(it, "class") },
        )
    }

/** Предел обнаружения обрыва по ADR-130 §5.1. */
const val DETECT_LIMIT_MS: Long = 45_000

/** Проверки по журналу ядра для Reconnect Lab. */
fun journalChecks(cycles: List<JournalCycle>, journalDropped: Long): List<Check> {
    val out = mutableListOf<Check>()
    out += Check("журнал ядра прочитан без разрывов", journalDropped == 0L,
        if (journalDropped == 0L) "вытеснено 0" else "вытеснено $journalDropped — времена неполны")
    for (c in cycles) {
        out += Check("цикл ${c.cycle}: журнал — потеря подписки ≤ 45 с", c.detectMs != null && c.detectMs <= DETECT_LIMIT_MS,
            c.detectMs?.let { "T_detect $it мс (запас ${DETECT_LIMIT_MS - it} мс)" } ?: "session.failed lost в журнале нет")
        out += Check("цикл ${c.cycle}: журнал — NETWORK_CHANGED отдан клиенту", c.networkChangedMs != null,
            c.networkChangedMs?.let { "через $it мс после обрыва" } ?: "event_sent NETWORK_CHANGED нет")
        out += Check("цикл ${c.cycle}: журнал — подписка переподключена", c.transportSubMs != null,
            c.transportSubMs?.let { "T_transport подписки $it мс" } ?: "conn_result ok подписки нет")
        out += Check("цикл ${c.cycle}: журнал — отказы вызовов классифицированы, не internal",
            c.failedClasses.none { it == "internal" },
            if (c.failedClasses.isEmpty()) "отказов нет" else c.failedClasses.groupingBy { it }.eachCount().toString())
    }
    return out
}

/** Среднее время вызова внутри ядра по гистограмме, мкс; null — вызовов нет. */
fun meanCoreMicros(h: DurationHistogram): Double? = if (h.n == 0L) null else h.sumUs.toDouble() / h.n

/** Сверка счёта исходов инструмента с классами вызовов ядра. */
fun outcomeCrossCheck(toolCalls: Int, toolCallErrors: Int, coreByClass: Map<String, Long>): Check {
    val coreTotal = coreByClass.values.sum()
    val coreFailed = coreTotal - (coreByClass["ok"] ?: 0L)
    val ok = coreTotal == toolCalls.toLong() && coreFailed == toolCallErrors.toLong()
    return Check("исходы: инструмент сходится с журналом ядра", ok,
        "инструмент: вызовов $toolCalls, отказов вызова $toolCallErrors; ядро: всего $coreTotal, по классам $coreByClass")
}

/** Проверка с вердиктом — общий формат отчётов лабораторий и раздела ENGRAM. */
data class Check(val name: String, val ok: Boolean, val detail: String)
