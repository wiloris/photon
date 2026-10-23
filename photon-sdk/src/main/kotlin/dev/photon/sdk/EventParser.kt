package dev.photon.sdk

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject

/**
 * Разбор конверта события (`sdk-api.md` §1–§2).
 *
 * Два правила, каждое из которых легко нарушить «улучшением»:
 *
 *  1. **Наружу ничего не бросается.** Разбор идёт внутри нативного колбэка,
 *     и исключение отсюда пересекло бы границу JNI. Любой отказ разбора —
 *     это [PhotonEvent.Unknown] с `type = null`.
 *  2. **`data` не пересобирается.** `JsonElement.toString()` нормализует
 *     форматирование (измерено: `{"a": 1 ,  "b" : [1,  2]}` → `{"a":1,"b":[1,2]}`),
 *     поэтому поддерево вырезается ИЗ ИСХОДНОЙ строки, а не печатается
 *     заново. Совпадение на плотном JSON, который шлёт ядро сегодня,
 *     случайно и опираться на него нельзя.
 *
 * Разделение труда: библиотека отвечает за корректность (валидность JSON,
 * экранирование, юникод), собственный код — только за границы одного
 * поддерева, и работает он уже на проверенном вводе.
 */
internal object EventParser {

    private val json = Json

    fun parse(raw: String): PhotonEvent {
        val obj = runCatching { json.parseToJsonElement(raw).jsonObject }.getOrNull()
            ?: return PhotonEvent.Unknown(null, raw)

        // Отсутствие "type" — тоже неразобранный конверт, и проверяется
        // ОТДЕЛЬНО от исключения: у некоторых разборщиков пустой ввод не
        // бросает вовсе (измерено на gson и jackson), и опора только на
        // исключение сделала бы правило зависящим от библиотеки.
        val type = obj.stringField("type") ?: return PhotonEvent.Unknown(null, raw)

        return when (type) {
            TYPE_CONNECTED -> PhotonEvent.Connected(raw)

            TYPE_NETWORK_CHANGED -> {
                val state = obj.stringField("state")
                    ?: return PhotonEvent.Unknown(type, raw)
                PhotonEvent.NetworkChanged(state, raw)
            }

            TYPE_MESSAGE -> {
                val event = obj.stringField("event")
                    ?: return PhotonEvent.Unknown(type, raw)
                PhotonEvent.Message(
                    event = event,
                    userId = obj.stringField("user_id"),
                    data = if (obj.containsKey("data")) rawTopLevelValue(raw, "data") else null,
                    raw = raw,
                )
            }

            else -> PhotonEvent.Unknown(type, raw)
        }
    }

    private const val TYPE_CONNECTED = "CONNECTED"
    private const val TYPE_MESSAGE = "MESSAGE"
    private const val TYPE_NETWORK_CHANGED = "NETWORK_CHANGED"

    /**
     * Значение строкового поля, либо null — включая случай «поле есть, но
     * не строка». Число или объект в поле `type` типом события не являются.
     */
    private fun JsonObject.stringField(name: String): String? =
        (this[name] as? JsonPrimitive)?.takeIf { it.isString }?.content
}

/**
 * Текст значения поля верхнего уровня, вырезанный из исходной строки.
 *
 * Вызывается ТОЛЬКО после успешного разбора той же строки библиотекой,
 * поэтому здесь нет ни проверок корректности, ни декодирования: задача —
 * найти границы значения, а не понять его. Строки пропускаются с учётом
 * экранирования — иначе `}` или `"` внутри значения оборвали бы подсчёт
 * вложенности.
 *
 * Возвращает null, если поля нет: для разобранного объекта это значит, что
 * его там действительно не было.
 */
internal fun rawTopLevelValue(source: String, field: String): String? {
    var i = 0
    while (i < source.length) {
        when (source[i]) {
            '"' -> {
                val keyStart = i
                val keyEnd = skipString(source, i) ?: return null
                var j = keyEnd
                while (j < source.length && source[j].isWhitespace()) j++
                if (j < source.length && source[j] == ':') {
                    var v = j + 1
                    while (v < source.length && source[v].isWhitespace()) v++
                    val end = skipValue(source, v) ?: return null
                    if (source.substring(keyStart + 1, keyEnd - 1) == field) {
                        return source.substring(v, end)
                    }
                    // Значение пропускается ЦЕЛИКОМ — отсюда и свойство
                    // «только верхний уровень»: во вложенный объект обход
                    // не заходит, и одноимённый ключ внутри него недостижим
                    // по построению. Отдельной проверки глубины поэтому нет:
                    // она была бы кодом, который никакая мутация не ломает
                    // (проверено — тесты проходили и без неё).
                    i = end
                } else {
                    i = keyEnd
                }
            }
            else -> i++
        }
    }
    return null
}

/** Индекс сразу за закрывающей кавычкой строки, начинающейся в [start]. */
private fun skipString(s: String, start: Int): Int? {
    var i = start + 1
    while (i < s.length) {
        when (s[i]) {
            '\\' -> i += 2   // экранированный символ пропускается целиком
            '"' -> return i + 1
            else -> i++
        }
    }
    return null
}

/** Индекс сразу за значением, начинающимся в [start]. */
private fun skipValue(s: String, start: Int): Int? {
    var i = start
    while (i < s.length && s[i].isWhitespace()) i++
    if (i >= s.length) return null
    return when (s[i]) {
        '"' -> skipString(s, i)
        '{', '[' -> {
            var depth = 0
            var j = i
            while (j < s.length) {
                when (s[j]) {
                    '"' -> { j = skipString(s, j) ?: return null; continue }
                    '{', '[' -> depth++
                    '}', ']' -> { depth--; if (depth == 0) return j + 1 }
                }
                j++
            }
            null
        }
        // Число, true/false/null — до разделителя.
        else -> {
            var j = i
            while (j < s.length && s[j] != ',' && s[j] != '}' && s[j] != ']' && !s[j].isWhitespace()) j++
            j
        }
    }
}
