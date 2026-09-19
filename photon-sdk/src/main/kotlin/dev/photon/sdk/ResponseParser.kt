package dev.photon.sdk

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Разбор конверта ответа (`sdk-api.md` §9.2).
 *
 * Те же два правила, что у [EventParser], и по тем же причинам:
 *
 *  1. **наружу ничего не бросается** — негодный конверт становится
 *     [PhotonResponse.Unrecognized], а не исключением: вызов состоялся, и
 *     исключение заявило бы обратное;
 *  2. **`data` не пересобирается** — вырезается из исходной строки, потому
 *     что `JsonElement.toString()` нормализует форматирование (измерено).
 */
internal object ResponseParser {

    private val json = Json

    fun parse(raw: String): PhotonResponse {
        val obj = runCatching { json.parseToJsonElement(raw) }.getOrNull() as? JsonObject
            ?: return PhotonResponse.Unrecognized(raw)

        // "ok" обязан быть БУЛЕВЫМ. Строка "true" вердиктом не является:
        // booleanOrNull у kotlinx принимает и строковый примитив, поэтому
        // isString проверяется отдельно — иначе {"ok":"false"} стал бы
        // успехом.
        val okPrimitive = obj["ok"] as? JsonPrimitive
        val ok = okPrimitive?.takeIf { !it.isString }?.content?.toBooleanStrictOrNull()
            ?: return PhotonResponse.Unrecognized(raw)

        val data = if (obj.containsKey("data")) rawTopLevelValue(raw, "data") else null

        return if (ok) {
            PhotonResponse.Ok(data = data, raw = raw)
        } else {
            PhotonResponse.Rejected(
                error = (obj["error"] as? JsonPrimitive)?.takeIf { it.isString }?.content,
                data = data,
                raw = raw,
            )
        }
    }
}
