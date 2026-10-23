package dev.photon.devtools

import dev.photon.core.PhotonException
import dev.photon.sdk.PhotonClient
import dev.photon.sdk.PhotonResponse

/*
 * Единственное место, где вызов Photon измеряется и классифицируется ДЛЯ
 * ОТЧЁТА инструмента.
 *
 * После перехода на SDK (ADR-062 п. 7) классификация исхода больше не живёт
 * здесь: её делает сам SDK, возвращая `PhotonResponse` и бросая
 * `PhotonException`. Своего разбора конверта (`"ok"\s*:\s*false` регулярным
 * выражением) в инструменте не осталось.
 *
 * Что здесь осталось и почему — это и есть ответ на вопрос, ради которого
 * ADR-062 п. 7 затевался:
 *
 *  1. **измерение задержки.** `Duration`/`nanos` в публичный результат SDK
 *     намеренно не входят (`sdk-api.md` §9.2 п. 7): они нужны Performance Lab
 *     и Network Lab, а не всякому приложению. Инструмент меряет сам;
 *  2. **метка для отчёта.** Таблицам и журналу нужна одна колонка с исходом;
 *     [Outcome] — ярлык показа, а не модель протокола.
 */

/**
 * Исход вызова в терминах отчёта инструмента.
 *
 * ADR-038 различает две разные вещи, и SDK выражает их двумя механизмами:
 * исключение (вызов не состоялся) и `PhotonResponse` (вердикт сервера).
 * Здесь они сводятся в одну шкалу — потому что отчёту нужна одна колонка.
 */
enum class Outcome {
    /** Вызов состоялся, сервер согласился (`ok:true`). */
    OK,

    /** Вызов состоялся, сервер отказал: `ok:false` в конверте. НЕ сетевой сбой. */
    VERDICT_FALSE,

    /**
     * Вызов состоялся, но конверт не распознан: нет булева `ok`, не объект
     * либо не разбирается.
     *
     * Появился вместе с переходом на SDK. Прежняя классификация инструмента
     * искала регулярным выражением ТОЛЬКО `ok:false`, поэтому испорченный
     * ответ засчитывался успехом — SDK такой ответ называет
     * `Unrecognized`, и прятать это обратно в OK значило бы вернуть дефект.
     */
    UNRECOGNIZED,

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

/**
 * Выполняет действие, измеряет задержку, приводит исход к [Outcome].
 *
 * Чистая функция отображения `PhotonResponse` → [Outcome] вынесена отдельно
 * ([outcomeOf]): её можно проверить тестом без сети и без native.
 */
fun call(client: PhotonClient, action: Int, payload: String): CallResult {
    val started = System.nanoTime()
    return try {
        val response = client.sendAction(action, payload)
        val nanos = System.nanoTime() - started
        CallResult(outcomeOf(response), response.raw, detailOf(response), nanos)
    } catch (e: PhotonException) {
        CallResult(Outcome.CALL_ERROR, null,
            "ОШИБКА ВЫЗОВА code=${e.code} ${e.nativeMessage ?: ""}", System.nanoTime() - started)
    }
}

/** Ярлык исхода по ответу SDK. */
fun outcomeOf(response: PhotonResponse): Outcome = when (response) {
    is PhotonResponse.Ok -> Outcome.OK
    is PhotonResponse.Rejected -> Outcome.VERDICT_FALSE
    is PhotonResponse.Unrecognized -> Outcome.UNRECOGNIZED
}

/** Человекочитаемое описание исхода для журнала и таблиц. */
fun detailOf(response: PhotonResponse): String = when (response) {
    is PhotonResponse.Ok ->
        "OK (вызов состоялся, вердикт положительный)"
    is PhotonResponse.Rejected ->
        "ВЕРДИКТ ok:false — ${response.error ?: "причина не указана"} (вызов состоялся!)"
    is PhotonResponse.Unrecognized ->
        "КОНВЕРТ НЕ РАСПОЗНАН (вызов состоялся, ответ без булева ok)"
}
