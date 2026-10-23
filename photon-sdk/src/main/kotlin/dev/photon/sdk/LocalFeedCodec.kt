package dev.photon.sdk

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/**
 * Сборка payload и разбор результатов локальной ленты (ADR-066 §3.2).
 *
 * Тем же инструментом, что весь SDK: дерево kotlinx **без компиляторного
 * плагина** (`sdk-api.md` §7.1). Сборка — как в [PhotonConfig], разбор — как в
 * [ResponseParser] и [EventParser].
 *
 * `internal`: публичная поверхность перечислена ADR-066 и этого объекта не
 * содержит.
 */
internal object LocalFeedCodec {

    private val json = Json

    /**
     * Коды действий.
     *
     * Вписаны числами осознанно и с оговоркой. Канонический каталог —
     * `photon-core/protobuf/photon.proto` (ADR-025), и машинно читать его SDK
     * нечем: `FileDescriptorSet` разбирает Protobuf Inspector в Developer
     * Tools, а тянуть protobuf-зависимость в SDK ради трёх чисел значило бы
     * платить несоразмерно.
     *
     * Публичного перечисления действий при этом НЕ появляется — `sdk-api.md`
     * §9 запрещает его прямо: «каталог живёт в `photon.proto`… своей копии нет
     * ни у ядра, ни у Developer Tools». Три константы внутри `internal`-объекта
     * копией каталога не являются и наружу не видны; расхождение с
     * `photon.proto` ловит тест, пиннингующий номера.
     */
    const val ACTION_PUT: Int = 801
    const val ACTION_INGEST: Int = 802
    const val ACTION_GAP_ANCHOR: Int = 803

    // ── Сборка payload ──────────────────────────────────────────────────

    fun putPayload(channelId: String, records: List<FeedRecord>): String =
        buildJsonObject {
            put("channel_id", channelId)
            put("records", recordsArray(records))
        }.toString()

    fun ingestPayload(channelId: String, block: FeedBlock): String =
        buildJsonObject {
            put("channel_id", channelId)
            put("records", recordsArray(block.records))
            put("dir", direction(block))
            anchorOf(block)?.let { put("anchor", positionObject(it)) }
            put("at_end", block.atEnd)
        }.toString()

    fun gapAnchorPayload(channelId: String): String =
        buildJsonObject { put("channel_id", channelId) }.toString()

    /**
     * Имя направления с провода.
     *
     * Берётся ИЗ ТИПА блока, а не из отдельного поля: другого источника у
     * него нет, и рассогласовать имя с якорем невозможно по построению.
     */
    private fun direction(block: FeedBlock): String = when (block) {
        is FeedBlock.Newest -> "newest"
        is FeedBlock.After -> "after"
        is FeedBlock.Before -> "before"
    }

    /**
     * Якорь блока, если он у этого направления есть.
     *
     * У [FeedBlock.Newest] якоря нет как поля — не «есть и равен null».
     * Поэтому `anchor` в payload для него не появляется вовсе, и
     * `ErrAnchorNotAllowed` через SDK недостижим.
     */
    private fun anchorOf(block: FeedBlock): FeedPosition? = when (block) {
        is FeedBlock.Newest -> null
        is FeedBlock.After -> block.anchor
        is FeedBlock.Before -> block.anchor
    }

    private fun positionObject(p: FeedPosition): JsonObject = buildJsonObject {
        put("order_key", JsonPrimitive(p.orderKey))
        put("id", p.id)
    }

    private fun recordsArray(records: List<FeedRecord>): JsonArray = buildJsonArray {
        for (r in records) {
            add(
                buildJsonObject {
                    put("id", r.id)
                    // JsonPrimitive(Number) хранит литерал строкой, поэтому
                    // Long доезжает точным при любом значении. Именно здесь
                    // теряется точность, если подставить Double.
                    put("order_key", JsonPrimitive(r.orderKey))
                    put("version", JsonPrimitive(r.version))
                    put("body", parseBody(r.id, r.body))
                }
            )
        }
    }

    /**
     * Тело записи как JSON-значение.
     *
     * Разбирается РОВНО ДЛЯ ТОГО, чтобы вложить в дерево, и ни для чего
     * больше: содержимое SDK не интерпретирует и схемы не вводит.
     *
     * Негодное тело — ошибка построения, а не ответ ядра: payload из него
     * собрать нечем. [IllegalArgumentException] того же рода, что
     * `IllegalStateException` у закрытого клиента, — нарушение предусловия
     * вызова. Собственного типа исключения SDK не заводит (ADR-066 §8).
     */
    private fun parseBody(recordId: String, body: String) =
        runCatching { json.parseToJsonElement(body) }.getOrElse {
            throw IllegalArgumentException(
                "record $recordId: body is not valid JSON", it
            )
        }

    // ── Разбор результатов ──────────────────────────────────────────────

    fun parsePut(response: PhotonResponse): FeedPutResult {
        val data = dataObject(response) ?: return FeedPutResult.Unavailable(response)
        val stored = intField(data, "stored") ?: return FeedPutResult.Unavailable(response)
        return FeedPutResult.Stored(stored, response)
    }

    /**
     * Семь полей, и **все семь обязательны**.
     *
     * Отсутствие любого делает результат [FeedIngestResult.Unavailable], а не
     * нулём: подставленный ноль неотличим от настоящего нуля, и «поле не
     * пришло» превратилось бы в «ничего не изменилось» — худший вид потери,
     * потому что выглядит как нормальный ответ.
     */
    fun parseIngest(response: PhotonResponse): FeedIngestResult {
        val data = dataObject(response) ?: return FeedIngestResult.Unavailable(response)
        return FeedIngestResult.Applied(
            stored = intField(data, "stored") ?: return FeedIngestResult.Unavailable(response),
            segments = intField(data, "segments") ?: return FeedIngestResult.Unavailable(response),
            updated = intField(data, "updated") ?: return FeedIngestResult.Unavailable(response),
            unchanged = intField(data, "unchanged") ?: return FeedIngestResult.Unavailable(response),
            stale = intField(data, "stale") ?: return FeedIngestResult.Unavailable(response),
            vanished = intField(data, "vanished") ?: return FeedIngestResult.Unavailable(response),
            gapsClosed = intField(data, "gaps_closed") ?: return FeedIngestResult.Unavailable(response),
            response = response,
        )
    }

    fun parseGapAnchor(response: PhotonResponse): FeedGapAnchor {
        val data = dataObject(response) ?: return FeedGapAnchor.Unavailable(response)

        val found = (data["found"] as? JsonPrimitive)
            ?.takeIf { !it.isString }?.content?.toBooleanStrictOrNull()
            ?: return FeedGapAnchor.Unavailable(response)

        if (!found) return FeedGapAnchor.None(response)

        val anchor = data["anchor"] as? JsonObject ?: return FeedGapAnchor.Unavailable(response)
        val orderKey = longField(anchor, "order_key") ?: return FeedGapAnchor.Unavailable(response)
        val id = (anchor["id"] as? JsonPrimitive)?.takeIf { it.isString }?.content
            ?: return FeedGapAnchor.Unavailable(response)

        return FeedGapAnchor.Found(FeedPosition(orderKey, id), response)
    }

    /** `data` успешного конверта как объект, либо null. */
    private fun dataObject(response: PhotonResponse): JsonObject? {
        val ok = response as? PhotonResponse.Ok ?: return null
        val data = ok.data ?: return null
        return runCatching { json.parseToJsonElement(data) }.getOrNull() as? JsonObject
    }

    /**
     * Целое поле. Дробное и строковое не принимаются: «12.5» и «"12"» —
     * не число записей, а испорченный ответ.
     */
    private fun intField(obj: JsonObject, name: String): Int? =
        (obj[name] as? JsonPrimitive)?.takeIf { !it.isString }?.content?.toIntOrNull()

    /**
     * Длинное целое.
     *
     * `toLongOrNull` по строке литерала, БЕЗ промежуточного Double: значения
     * выше 2^53 обязаны доезжать без искажения (ADR-065 п. 2.2).
     */
    private fun longField(obj: JsonObject, name: String): Long? =
        (obj[name] as? JsonPrimitive)?.takeIf { !it.isString }?.content?.toLongOrNull()
}
