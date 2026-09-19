package dev.photon.sdk

import java.time.Duration
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/**
 * Конфигурация инстанса Photon (`sdk-api.md` §4–§6).
 *
 * Два **взаимоисключающих** способа задать её, и смешивать их нельзя:
 *
 * ```
 * PhotonConfig(BridgeConfig(endpoint = "host:4443", …))   // типизированный
 * PhotonConfig.raw("""{"storage_path":"/tmp/photon"}""")   // всё остальное
 * ```
 *
 * Перегрузки, принимающей и то и другое, не существует намеренно: она
 * потребовала бы правил приоритета («что если `raw` содержит `bridge`») и
 * завела бы второе место, где собирается JSON.
 *
 * [raw] — **временный** путь для полей, которых SDK ещё не знает. Ядро
 * неизвестные поля игнорирует намеренно (`dispatcher/config.go:139-142`),
 * и `{"storage_path":…}` — законный `config_json`, отсутствующий в схеме
 * `wireConfig`. Как только поле входит в схему ядра, оно типизируется здесь.
 */
public class PhotonConfig {

    private val bridge: BridgeConfig?
    private val rawJson: String?

    /** Типизированный путь. `PhotonConfig()` даёт `{}` — инстанс без Bridge законен. */
    @JvmOverloads
    public constructor(bridge: BridgeConfig? = null) {
        this.bridge = bridge
        this.rawJson = null
    }

    private constructor(rawJson: String) {
        this.bridge = null
        this.rawJson = rawJson
    }

    /**
     * `config_json` для `Photon_Create`.
     *
     * Незаданные поля **не сериализуются вовсе**. Хотя ядро трактует
     * отсутствие и ноль одинаково (`withDefaults`), выбран один вариант:
     * два способа выразить одно состояние начали бы означать разное при
     * первой же попытке отличить «пользователь задал ноль» от «не задавал».
     *
     * Значений по умолчанию SDK **не подставляет**: они живут в ядре
     * (15 с / 30 с / 15 с / 500 мс / 30 с), и копия здесь разошлась бы с
     * оригиналом молча.
     */
    public fun toJson(): String = rawJson ?: buildJsonObject {
        val b = bridge ?: return@buildJsonObject
        put("bridge", b.toJsonObject())
    }.toString()

    override fun toString(): String =
        if (rawJson != null) "PhotonConfig(raw)" else "PhotonConfig(bridge=$bridge)"

    public companion object {
        /**
         * Конфигурация, заданная готовым JSON целиком.
         *
         * Строка уходит в ABI **нетронутой**: SDK её не разбирает и не
         * проверяет. Валидация — в ядре, и ошибка приходит оттуда
         * (`PHOTON_ERR_INVALID_ARGUMENT`).
         */
        @JvmStatic
        public fun raw(json: String): PhotonConfig = PhotonConfig(json)
    }
}

/**
 * Секция `bridge` — девять полей, ровно те же, что у ядра
 * (`dispatcher/config.go:113-129`), и ничего сверх.
 *
 * `null` означает «не задано»: поле не попадёт в JSON, и значение возьмёт
 * ядро. Длительности — [Duration], а не миллисекунды: единицы границы ABI
 * не должны протекать в публичный API. Перевод — один, в момент
 * сериализации.
 *
 * **Проверок здесь нет.** Отрицательная длительность уезжает в ядро и
 * возвращается как `PHOTON_ERR_INVALID_ARGUMENT` — единственная валидация
 * живёт там (`config.go:157-172`), и дублировать её значит завести вторую
 * точку истины.
 */
public data class BridgeConfig @JvmOverloads constructor(
    public val endpoint: String? = null,
    public val sessionToken: String? = null,
    public val connectTimeout: Duration? = null,
    public val callTimeout: Duration? = null,
    public val keepAlive: Duration? = null,
    public val eventBackoffMin: Duration? = null,
    public val eventBackoffMax: Duration? = null,
    public val tls: TlsConfig? = null,
) {
    internal fun toJsonObject(): JsonObject = buildJsonObject {
        endpoint?.let { put("endpoint", it) }
        sessionToken?.let { put("session_token", it) }
        connectTimeout?.let { put("connect_timeout_ms", it.millis()) }
        callTimeout?.let { put("call_timeout_ms", it.millis()) }
        keepAlive?.let { put("keepalive_ms", it.millis()) }
        eventBackoffMin?.let { put("event_backoff_min_ms", it.millis()) }
        eventBackoffMax?.let { put("event_backoff_max_ms", it.millis()) }
        tls?.let { put("tls", it.toJsonObject()) }
    }
}

/** Секция `bridge.tls`. */
public data class TlsConfig @JvmOverloads constructor(
    public val insecureSkipVerify: Boolean? = null,
    public val serverName: String? = null,
) {
    internal fun toJsonObject(): JsonObject = buildJsonObject {
        insecureSkipVerify?.let { put("insecure_skip_verify", JsonPrimitive(it)) }
        serverName?.let { put("server_name", it) }
    }
}

/**
 * Длительность в миллисекундах границы ABI.
 *
 * Округление вниз — документированное поведение `Duration.toMillis()`, а не
 * дефект: субмиллисекундная точность на границе не выражается.
 */
private fun Duration.millis(): Long = toMillis()
