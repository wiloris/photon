package dev.photon.devtools

import dev.photon.core.PhotonException
import dev.photon.core.PhotonNative
import dev.photon.sdk.DiagnosticEvent
import dev.photon.sdk.DiagnosticValue
import dev.photon.sdk.DiagnosticsPage
import dev.photon.sdk.DiagnosticsSnapshot
import dev.photon.sdk.PhotonClient
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/*
 * Лента Inspector Live и снимок ядра — платформонезависимая часть
 * Developer-Tools (photon-devtools-core): ею пользуются и консольный
 * инструмент (photon-developer-tools), и раздел «Для разработчиков» ENGRAM.
 *
 * Источник — ТОЛЬКО диагностика ядра через SDK (`PhotonClient.diagnostics()`,
 * коды 819/820, ADR-130 §2.7): своих счётчиков и своих событий здесь нет.
 *
 * Время события показывается как ОЦЕНКА по монотонным часам: у записи только
 * `at_mono_ms`, календарное время — instance_start_wall_ms + at_mono_ms, и
 * после перевода системных часов оно расходится с настоящим
 * (ADR-130 §2.3, поправка 2026-10-23).
 */

/** Источник диагностики. Рабочий — [PhotonClient.diagnostics]; в тестах — подставной. */
interface DiagnosticsSource {
    fun snapshot(): DiagnosticsSnapshot
    fun events(afterSeq: Long, limit: Int): DiagnosticsPage
}

fun PhotonClient.diagnosticsSource(): DiagnosticsSource {
    val d = diagnostics()
    return object : DiagnosticsSource {
        override fun snapshot() = d.snapshot()
        override fun events(afterSeq: Long, limit: Int) = d.events(afterSeq, limit)
    }
}

/** Строка ленты Inspector Live. */
sealed interface LiveLine {
    /** Событие журнала ядра. */
    data class Event(val event: DiagnosticEvent, val estimatedWallMs: Long, val text: String) : LiveLine

    /** Явный разрыв: столько событий вытеснено до того, как их прочитали. */
    data class Gap(val dropped: Long, val text: String) : LiveLine

    /** Сменился instance_id — инстанс перезапущен; чтение начато заново. */
    data class Restart(val previous: String?, val current: String, val text: String) : LiveLine
}

/**
 * Чтение журнала по курсору с правилами `sdk-api.md` §12.5.
 *
 * - продолжает с `nextSeq` прошлой страницы, без пропусков и повторов;
 * - смена `instanceId` — перезапуск инстанса: строка [LiveLine.Restart] и
 *   чтение с `afterSeq = 0`;
 * - `dropped > 0` — строка [LiveLine.Gap] перед записями страницы;
 * - отказ ядра «курсор впереди журнала» (`PHOTON_ERR_INVALID_ARGUMENT`) —
 *   курсор сбрасывается в 0, следующая страница покажет новый instance_id.
 *
 * @param actionName имя кода действия по каталогу; `null` — кода в каталоге нет.
 * @param catalogLoaded каталог действий загружен; `false` — коды показываются
 *   числами без пометки «нет в каталоге» (её нельзя утверждать без каталога).
 */
class LiveFeed(
    private val source: DiagnosticsSource,
    private val actionName: (Int) -> String?,
    private val limit: Int = 512,
    private val zone: ZoneId = ZoneId.systemDefault(),
    private val catalogLoaded: Boolean = true,
) {
    var instanceId: String? = null
        private set
    var cursor: Long = 0
        private set
    var restarts: Int = 0
        private set
    var droppedTotal: Long = 0
        private set

    /**
     * Читает всё, что накопилось с прошлого вызова (не больше `maxPages`
     * страниц за раз, чтобы опрос не застревал на непрерывном потоке).
     */
    fun poll(maxPages: Int = 16): List<LiveLine> {
        val out = mutableListOf<LiveLine>()
        repeat(maxPages) {
            val page = try {
                source.events(cursor, limit)
            } catch (e: PhotonException) {
                if (e.code != PhotonNative.PHOTON_ERR_INVALID_ARGUMENT || cursor == 0L) throw e
                cursor = 0 // курсор впереди журнала — журнал не тот, что был
                return@repeat
            }
            val previous = instanceId
            if (previous == null) {
                instanceId = page.instanceId // первое чтение
            } else if (page.instanceId != previous) {
                // Перезапуск: прочитанное по старому курсору к новому
                // инстансу не относится — страница отбрасывается, если она
                // была прочитана не с нуля.
                restarts++
                instanceId = page.instanceId
                out += LiveLine.Restart(previous, page.instanceId,
                    "── инстанс перезапущен: $previous → ${page.instanceId}; чтение заново ──")
                if (cursor != 0L) {
                    cursor = 0
                    return@repeat
                }
            }
            if (page.dropped > 0) {
                droppedTotal += page.dropped
                out += LiveLine.Gap(page.dropped, "── вытеснено ${page.dropped} событий до чтения (журнал переполнен) ──")
            }
            for (e in page.events) {
                val wall = page.estimatedWallMs(e)
                out += LiveLine.Event(e, wall, render(e, wall))
            }
            cursor = page.nextSeq
            if (page.events.isEmpty()) return out
        }
        return out
    }

    /** Строка события: оценка времени, монотонное время, компонент, вид, поля. */
    fun render(e: DiagnosticEvent, estimatedWallMs: Long): String {
        val time = TIME.withZone(zone).format(Instant.ofEpochMilli(estimatedWallMs))
        val conn = e.conn?.let { " [$it]" } ?: ""
        val fields = e.fields.entries.joinToString(" ") { (k, v) -> "$k=${value(k, v)}" }
        return "≈$time +${e.atMonoMs}мс  #${e.seq} ${e.component}.${e.kind}$conn  $fields"
    }

    private fun value(key: String, v: DiagnosticValue): String = when (v) {
        is DiagnosticValue.Text -> v.value
        is DiagnosticValue.Num ->
            if (key == "action" && catalogLoaded) "${v.value}(${actionName(v.value.toInt()) ?: "нет в каталоге"})"
            else v.value.toString()
    }

    companion object {
        private val TIME: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")

        /** Подпись к колонке времени — в выводе всегда рядом с первой строкой. */
        const val TIME_NOTE: String = "время «≈» — ОЦЕНКА по монотонным часам: старт инстанса + at_mono_ms; " +
            "после перевода системных часов расходится с настоящим"
    }
}

/** Снимок состояния в несколько строк: соединения, вызовы, события, журнал, хранилище. */
fun renderSnapshot(s: DiagnosticsSnapshot): List<String> {
    if (!s.enabled) return listOf("диагностика выключена конфигурацией (diagnostics.enabled=false)")
    val out = mutableListOf<String>()
    out += "инстанс ${s.instanceId}, работает ${s.uptimeMs / 1000} с"
    for ((name, c) in s.connections.toSortedMap()) {
        out += "  %-9s %-10s %6d мс в состоянии; попыток %d, неудач %d, переподключений %d, обрывов %d"
            .format(name, c.state, c.inStateMs, c.connects, c.connectErrors, c.reconnects, c.lost)
    }
    s.calls?.let { c ->
        out += "  вызовы: в полёте ${c.inFlight}, всего ${c.total}, медленных ${c.slow}, по классам ${c.byClass.toSortedMap()}"
    }
    s.subscriptionEvents?.let { e ->
        out += "  события подписки: CONNECTED ${e.connected}, NETWORK_CHANGED ${e.networkChanged}, MESSAGE ${e.message}"
    }
    s.journal?.let { j ->
        out += "  журнал: ${j.len}/${j.capacity}, последний seq ${j.lastSeq}, вытеснено ${j.evicted}"
    }
    out += s.storage?.let { st ->
        "  хранилище: ${if (st.open) "открыто" else "закрыто"}, схема ${st.schemaVersion}; " +
            "View ${st.viewTx} (ошибок ${st.viewErrors}), Update ${st.updateTx} (ошибок ${st.updateErrors}, " +
            "откатов ${st.rollbacks}), медленных ${st.slowTx}" +
            (st.bolt?.get("file_size")?.let { ", файл $it Б" } ?: "")
    } ?: "  хранилище: нет"
    return out
}

fun LiveLine.text(): String = when (this) {
    is LiveLine.Event -> text
    is LiveLine.Gap -> text
    is LiveLine.Restart -> text
}
