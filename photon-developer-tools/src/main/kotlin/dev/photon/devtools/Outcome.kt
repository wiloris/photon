package dev.photon.devtools

import dev.photon.core.Photon
import dev.photon.core.PhotonException

/*
 * Единственное место, где вызов Photon измеряется и классифицируется.
 *
 * Вынесено сюда, чтобы Dashboard и Network Lab пользовались ОДНОЙ
 * классификацией: две копии этой логики разошлись бы, и тогда «успех» на
 * одном экране означал бы не то же, что на другом.
 */

/**
 * Три различимых исхода, а не два.
 *
 * ADR-038: `PhotonResult` — исход ВЫЗОВА, вердикт операции живёт в конверте.
 * Смешивать их значит терять разницу между «связь сломана» и «сервер сказал
 * нет» — а для сетевой диагностики это ровно та разница, ради которой она
 * существует.
 */
enum class Outcome {
    /** Вызов состоялся, сервер согласился. */
    OK,

    /** Вызов состоялся, сервер отказал: `ok:false` в конверте. НЕ сетевой сбой. */
    VERDICT_FALSE,

    /** Вызов не состоялся: C ABI вернул не `PHOTON_OK`. */
    CALL_ERROR,
}

data class CallResult(
    val outcome: Outcome,
    val response: String?,
    val detail: String,
    val nanos: Long,
) {
    val millis: Double get() = nanos / 1_000_000.0
}

private val verdictFalse = Regex("\"ok\"\\s*:\\s*false")
private val errorField = Regex("\"error\"\\s*:\\s*\"([^\"]*)\"")

/**
 * Классификация ответа, пришедшего БЕЗ ошибки вызова.
 *
 * Отдельная чистая функция намеренно: её можно проверить тестом без сети и
 * без native — а значит, можно доказать, что `ok:false` не считается
 * сетевым сбоем.
 */
fun classifyResponse(response: String): Outcome =
    if (verdictFalse.containsMatchIn(response)) Outcome.VERDICT_FALSE else Outcome.OK

fun verdictReason(response: String): String =
    errorField.find(response)?.groupValues?.get(1) ?: "причина не указана"

/** Выполняет действие, измеряет задержку, классифицирует исход. */
fun call(p: Photon, action: Int, payload: String): CallResult {
    val started = System.nanoTime()
    return try {
        val response = p.sendAction(action, payload)
        val nanos = System.nanoTime() - started
        when (classifyResponse(response)) {
            Outcome.OK -> CallResult(Outcome.OK, response,
                "OK (вызов состоялся, вердикт положительный)", nanos)
            else -> CallResult(Outcome.VERDICT_FALSE, response,
                "ВЕРДИКТ ok:false — ${verdictReason(response)} (вызов состоялся!)", nanos)
        }
    } catch (e: PhotonException) {
        CallResult(Outcome.CALL_ERROR, null,
            "ОШИБКА ВЫЗОВА code=${e.code} ${e.nativeMessage ?: ""}", System.nanoTime() - started)
    }
}
