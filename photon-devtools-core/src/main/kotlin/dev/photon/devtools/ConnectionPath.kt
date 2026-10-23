package dev.photon.devtools

import dev.photon.sdk.DiagnosticEvent
import dev.photon.sdk.DiagnosticValue
import java.util.Locale

/*
 * Панель «Путь» — по какой ступени лестницы подключения (ARCHITECTURE.md §5:
 * IPv6 QUIC → IPv4 QUIC → IPv6 TCP → IPv4 TCP → транспорт с маскировкой)
 * ядро ДЕЙСТВИТЕЛЬНО подключилось.
 *
 * Источник — только событие `transport.dial` журнала ядра (ADR-130 §2.3,
 * дополнение 2026-10-23): `transport` сообщает сам адаптер, `remote` и
 * `family` есть только у установленного соединения. Чего в журнале нет, того
 * панель не показывает и не достраивает:
 *  - неудачная попытка соединения не установила — ни адреса, ни семейства у
 *    неё нет, и к ступени она не относится;
 *  - ступень, по которой подключения не было, — «не наблюдалась», а не
 *    «не работает» и не «работает»;
 *  - перебор ступеней (Autodial) виден только по событиям `route_*`; пока их
 *    нет, панель так и говорит — показан фактический путь, а не лестница.
 */

/** Одна запись `transport.dial`. */
data class DialRecord(
    val seq: Long,
    val atMonoMs: Long,
    val conn: String?,
    val endpoint: String?,
    val transport: String?,
    val family: String?,
    val remote: String?,
    val durUs: Long?,
    val cls: String?,
) {
    val ok: Boolean get() = cls == "ok"
}

/** `transport.dial` → [DialRecord]; любое другое событие — `null`. */
fun dialOf(e: DiagnosticEvent): DialRecord? {
    if (e.component != "transport" || e.kind != "dial") return null
    fun text(k: String) = (e.fields[k] as? DiagnosticValue.Text)?.value
    fun num(k: String) = (e.fields[k] as? DiagnosticValue.Num)?.value
    return DialRecord(e.seq, e.atMonoMs, e.conn, text("endpoint"), text("transport"), text("family"),
        text("remote"), num("dur_us"), text("class"))
}

/** Событие выбора или перебора маршрута Autodial. */
fun isRouteEvent(e: DiagnosticEvent): Boolean = e.component == "autodial"

/** Ступени лестницы в порядке приоритета. */
enum class Rung(val label: String, val transport: String?, val family: String?) {
    QUIC_V6("IPv6 QUIC", "quic", "IPv6"),
    QUIC_V4("IPv4 QUIC", "quic", "IPv4"),
    TCP_V6("IPv6 TCP", "tcp", "IPv6"),
    TCP_V4("IPv4 TCP", "tcp", "IPv4"),
    MASKED("транспорт с маскировкой", null, null),
}

/** Ступень установленного соединения; `null` — по журналу не определить. */
fun rungOf(d: DialRecord): Rung? =
    if (!d.ok) null else Rung.entries.firstOrNull { it.transport != null && it.transport == d.transport && it.family == d.family }

/** Соединение роли `action` / `subscribe` / `media`. */
data class ConnPath(
    val conn: String,
    /** Последнее установленное соединение. */
    val lastOk: DialRecord?,
    /** Последняя попытка — если она неудачная и позже [lastOk]. */
    val lastFailure: DialRecord?,
    val attempts: Int,
    val failures: Int,
)

data class RungStatus(val rung: Rung, val conns: List<String>, val count: Int)

data class PathView(
    val conns: List<ConnPath>,
    val rungs: List<RungStatus>,
    /** Установленные соединения, ступень которых по журналу не определить. */
    val unplaced: List<DialRecord>,
    /** Неудачные попытки по транспорту: адреса и семейства у них нет. */
    val failedByTransport: Map<String, Int>,
    val routeEvents: Int,
    /** Вытеснено записей до чтения — панель может быть неполной. */
    val dropped: Long,
)

/** Роли соединений ядра, которые панель показывает всегда — с данными или без. */
val PATH_ROLES: List<String> = listOf("action", "subscribe", "media")

fun buildPathView(dials: List<DialRecord>, routeEvents: Int, dropped: Long): PathView {
    val sorted = dials.sortedBy { it.seq }
    val conns = (PATH_ROLES + sorted.mapNotNull { it.conn }.filter { it !in PATH_ROLES }.distinct()).map { role ->
        val own = sorted.filter { it.conn == role }
        val lastOk = own.lastOrNull { it.ok }
        val last = own.lastOrNull()
        ConnPath(role, lastOk, last?.takeIf { !it.ok }, own.size, own.count { !it.ok })
    }
    val ok = sorted.filter { it.ok }
    val rungs = Rung.entries.map { r ->
        val hits = ok.filter { rungOf(it) == r }
        RungStatus(r, hits.mapNotNull { it.conn }.distinct(), hits.size)
    }
    return PathView(
        conns = conns,
        rungs = rungs,
        unplaced = ok.filter { rungOf(it) == null },
        failedByTransport = sorted.filter { !it.ok }.groupingBy { it.transport ?: "не сообщён" }.eachCount(),
        routeEvents = routeEvents,
        dropped = dropped,
    )
}

private fun ms(us: Long?): String = us?.let { String.format(Locale.ROOT, "%.1f мс", it / 1000.0) } ?: "длительность —"

private fun step(d: DialRecord): String {
    val tr = d.transport ?: "транспорт не сообщён"
    val fam = d.family ?: "семейство неизвестно"
    return "$tr/$fam"
}

fun renderPath(v: PathView): List<String> {
    val out = mutableListOf<String>()
    out += "путь подключения — по событиям transport.dial журнала ядра"
    if (v.dropped > 0) {
        out += "  ⚠ вытеснено ${v.dropped} записей до чтения — часть подключений могла не попасть в панель"
    }
    for (c in v.conns) {
        val head = "  %-9s".format(c.conn)
        val ok = c.lastOk
        out += when {
            ok != null -> "$head ${step(ok)}  → ${ok.remote ?: "адрес не сообщён"}" +
                (ok.endpoint?.let { " (endpoint $it)" } ?: "") +
                ", подключение ${ms(ok.durUs)}, #${ok.seq}"
            c.attempts > 0 -> "$head соединение не установлено: попыток ${c.attempts}, все неудачные"
            v.dropped > 0 -> "$head dial в прочитанном журнале нет — запись могла быть вытеснена"
            else -> "$head dial в прочитанном журнале нет"
        }
        c.lastFailure?.let { f ->
            out += "            последняя попытка #${f.seq}: ${f.cls ?: "класс —"} за ${ms(f.durUs)}" +
                " (${f.transport ?: "транспорт не сообщён"}) — соединения нет, адреса и семейства нет"
        }
        if (c.attempts > 1 || c.failures > 0) {
            out += "            попыток ${c.attempts}, неудачных ${c.failures}"
        }
    }
    out += "  лестница (ARCHITECTURE.md §5) — только наблюдённое:"
    for (r in v.rungs) {
        out += if (r.count > 0) "    ✔ ${r.rung.label}: подключений ${r.count} (${r.conns.joinToString()})"
        else "    · ${r.rung.label}: в журнале не наблюдалась"
    }
    for (d in v.unplaced) {
        out += "    ? #${d.seq} [${d.conn ?: "—"}] ${step(d)} → ${d.remote ?: "адрес не сообщён"} — к ступени не отнесено"
    }
    if (v.failedByTransport.isNotEmpty()) {
        out += "    неудачные попытки (ступень неизвестна — соединения нет): " +
            v.failedByTransport.toSortedMap().entries.joinToString { "${it.key} ×${it.value}" }
    }
    out += if (v.routeEvents == 0) {
        "  перебора ступеней ядро не вело: событий autodial (route_*) в журнале нет — показан фактический путь"
    } else {
        "  событий autodial (route_*) в журнале: ${v.routeEvents}"
    }
    return out
}

/**
 * Накопитель панели для Inspector Live: те же строки, что идут в ленту.
 * Перезапуск инстанса сбрасывает накопленное — прежние подключения к новому
 * инстансу не относятся.
 */
class PathTracker {
    private val dials = mutableListOf<DialRecord>()
    private var routes = 0
    private var dropped = 0L

    fun accept(lines: List<LiveLine>) {
        for (l in lines) when (l) {
            is LiveLine.Restart -> { dials.clear(); routes = 0; dropped = 0 }
            is LiveLine.Gap -> dropped += l.dropped
            is LiveLine.Event -> {
                dialOf(l.event)?.let { dials += it }
                if (isRouteEvent(l.event)) routes++
            }
        }
    }

    fun view(): PathView = buildPathView(dials, routes, dropped)
}

/** Панель по событиям, уже прочитанным [CoreJournal]. */
fun pathFrom(events: List<DiagnosticEvent>, dropped: Long): PathView =
    buildPathView(events.mapNotNull(::dialOf), events.count(::isRouteEvent), dropped)
