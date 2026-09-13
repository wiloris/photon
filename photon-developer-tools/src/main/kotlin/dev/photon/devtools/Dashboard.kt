package dev.photon.devtools

import dev.photon.core.Photon
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentLinkedQueue

/*
 * Dashboard — логика режима по умолчанию, объём Phase 16.
 *
 * Вынесена из Main.kt ради тестов и БЕЗ изменения поведения: строки вывода,
 * порядок записей и вызовы часов те же. Параметрами стали только часы и
 * вывод — со значениями по умолчанию, равными прежним, — чтобы переходы
 * состояния и журнал проверялись без native и без сети, как у остальных
 * лабораторий.
 *
 * Разбор "type" из event_json здесь свой. Такой же разбор есть в LogView,
 * NetworkLab и ReconnectLab (eventKind): это записанный технический долг,
 * а не решение — сводить их в один вне объёма A2.
 */

private val clock = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")

private fun wallStamp(): String = LocalTime.now().format(clock)

/** Строка журнала действий: код, payload, исход, задержка. */
data class ActionRecord(
    val at: String,
    val action: Int,
    val payload: String,
    /** Класс исхода — то, что Dashboard обязан различать (ADR-038). В вывод
     *  не попадает: печатается [outcome], как и прежде. */
    val kind: Outcome,
    val outcome: String,
    val millis: Long,
)

/**
 * Журнал действий и событий.
 *
 * События приходят СИНХРОННО на потоке доставки ядра (jni-events.md, D1),
 * поэтому очередь здесь — не буфер доставки, а хранилище для показа.
 */
class Dashboard(
    private val stamp: () -> String = ::wallStamp,
    private val out: (String) -> Unit = { println(it) },
) {
    private val actions = ConcurrentLinkedQueue<ActionRecord>()
    private val events = ConcurrentLinkedQueue<String>()

    @Volatile var subscription: String = "нет подписки"
        private set

    fun onEvent(json: String) {
        val type = Regex("\"type\"\\s*:\\s*\"([A-Z_]+)\"").find(json)?.groupValues?.get(1) ?: "?"
        // CONNECTED и NETWORK_CHANGED описывают ТОЛЬКО подписное соединение:
        // action-соединение подключается лениво и своего фонового жизненного
        // цикла не имеет (photon_core.h).
        when (type) {
            "CONNECTED" -> subscription = "CONNECTED (с ${stamp()})"
            "NETWORK_CHANGED" -> subscription = "NETWORK_CHANGED — связь потеряна (${stamp()})"
        }
        events.add("${stamp()}  $type  ${json.take(160)}")
        out("  ⚡ ${stamp()}  $type")
    }

    /**
     * Выполняет действие и записывает исход.
     *
     * Различение двух разных вещей — суть пункта Dashboard (ADR-038):
     *  - PhotonException  — ИСХОД ВЫЗОВА: до сервера не дошло или дошло, но
     *                       C ABI вернул не PHOTON_OK;
     *  - ok:false в ответе — ВЕРДИКТ ОПЕРАЦИИ: вызов состоялся, сервер отказал.
     * Смешивать их значит терять различие между «связь сломана» и «сервер
     * сказал нет».
     */
    fun send(p: Photon, action: Int, payload: String): String? =
        // Классификация живёт в Outcome.kt и только там: Network Lab
        // пользуется той же, и две копии разошлись бы.
        record(action, payload, call(p, action, payload))

    /** Запись исхода уже состоявшегося [call]. Отделена от вызова ради тестов. */
    fun record(action: Int, payload: String, r: CallResult): String? {
        actions.add(ActionRecord(stamp(), action, payload, r.outcome, r.detail, r.millis.toLong()))
        return r.response
    }

    fun actionLog(): List<ActionRecord> = actions.toList()

    fun eventLog(): List<String> = events.toList()

    fun printActions() {
        out("\n── журнал действий ───────────────────────────────")
        if (actions.isEmpty()) out("  (пусто)")
        actions.forEach {
            out("  ${it.at}  action=${it.action}  ${it.millis} мс")
            out("      payload: ${it.payload.take(90)}")
            out("      исход:   ${it.outcome}")
        }
    }

    fun printEvents() {
        out("\n── события ───────────────────────────────────────")
        if (events.isEmpty()) out("  (ни одного)")
        events.forEach { out("  $it") }
    }
}

/** Версия ABI, разобранная из упакованного `(MAJOR shl 16) or MINOR`. */
data class AbiInfo(val major: Int, val minor: Int, val compatible: Boolean)

/**
 * Совместимость решает только MAJOR: MINOR клиент вправе игнорировать
 * (ADR-026, п. 4).
 */
fun abiInfo(packed: Int, expectedMajor: Int): AbiInfo {
    val major = packed shr 16
    val minor = packed and 0xFFFF
    return AbiInfo(major, minor, major == expectedMajor)
}

fun dashboardConfig(endpoint: String, token: String) =
    """{"bridge":{"endpoint":"$endpoint","session_token":"$token","connect_timeout_ms":15000,"call_timeout_ms":20000}}"""
