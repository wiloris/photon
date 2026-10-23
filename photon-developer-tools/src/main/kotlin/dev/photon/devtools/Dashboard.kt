package dev.photon.devtools

import dev.photon.sdk.BridgeConfig
import dev.photon.sdk.PhotonClient
import dev.photon.sdk.PhotonConfig
import dev.photon.sdk.PhotonEvent
import java.time.Duration
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
 * События приходят типизированными: SDK отдаёт PhotonEvent, и разбора
 * event_json здесь нет вовсе. Прежде тут была своя копия разбора "type" —
 * как в LogView, NetworkLab и ReconnectLab; сначала долг свели в один
 * источник, затем переход на SDK убрал и его (ADR-062 п. 7).
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

    fun onEvent(event: PhotonEvent) {
        val type = label(event)
        // CONNECTED и NETWORK_CHANGED описывают ТОЛЬКО подписное соединение:
        // action-соединение подключается лениво и своего фонового жизненного
        // цикла не имеет (photon_core.h).
        when (event) {
            is PhotonEvent.Connected -> subscription = "CONNECTED (с ${stamp()})"
            is PhotonEvent.NetworkChanged -> subscription = "NETWORK_CHANGED — связь потеряна (${stamp()})"
            else -> {}
        }
        events.add("${stamp()}  $type  ${event.raw.take(160)}")
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
    fun send(client: PhotonClient, action: Int, payload: String): String? =
        // Классификация живёт в Outcome.kt и только там: Network Lab
        // пользуется той же, и две копии разошлись бы.
        record(action, payload, call(client, action, payload))

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

/**
 * Конфигурация онлайн-режима.
 *
 * Собирается типизированно (ADR-062 п. 6): строковой сборки `config_json` в
 * инструменте больше нет ни в одном месте — её было семь. Значения по
 * умолчанию SDK не подставляет, поэтому заданные здесь таймауты — осознанный
 * выбор инструмента, а не копия умолчаний ядра.
 */
fun dashboardConfig(endpoint: String, token: String): PhotonConfig =
    PhotonConfig(
        BridgeConfig(
            endpoint = endpoint,
            sessionToken = token,
            connectTimeout = Duration.ofSeconds(15),
            callTimeout = Duration.ofSeconds(20),
        )
    )
