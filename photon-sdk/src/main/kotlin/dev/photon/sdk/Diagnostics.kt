package dev.photon.sdk

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

/*
 * Диагностика инстанса Photon Core — `sdk-api.md` §12, ADR-130 §2.7–§2.8.
 *
 * Типизированы формы самой диагностики: это схема ядра (ADR-130 §2.3–§2.5),
 * а не доменные данные Mattermost — запрет §6 на них не распространяется.
 * Время — по поправке ADR-130 §2.3: у события только монотонное `atMonoMs`,
 * календарное время события — лишь оценка ([DiagnosticsPage.estimatedWallMs]).
 */

/**
 * Диагностика клиента: снимок (819) и журнал по курсору (820).
 *
 * Своего состояния не держит — курсор хранит вызывающий (§12.5).
 */
public class PhotonDiagnostics internal constructor(private val client: PhotonClient) {

    /**
     * Снимок состояния инстанса (`ACTION_LOCAL_DIAG_SNAPSHOT`).
     *
     * @throws dev.photon.core.PhotonException если вызов не состоялся
     * @throws IllegalStateException если клиент закрыт или ответ не
     *   соответствует контракту
     */
    public fun snapshot(): DiagnosticsSnapshot =
        DiagnosticsCodec.parseSnapshot(client.sendDiagnostic(DiagnosticsCodec.ACTION_SNAPSHOT, "{}"))

    /**
     * Записи журнала с `seq > afterSeq`, не больше `limit`
     * (`ACTION_LOCAL_DIAG_EVENTS`). Пределы `limit` (1…512) и курсора
     * проверяет ядро (§12.4).
     *
     * @throws dev.photon.core.PhotonException если вызов не состоялся — в том
     *   числе `PHOTON_ERR_INVALID_ARGUMENT` за курсор или лимит
     * @throws IllegalStateException если клиент закрыт или ответ не
     *   соответствует контракту
     */
    public fun events(afterSeq: Long, limit: Int): DiagnosticsPage =
        DiagnosticsCodec.parsePage(
            client.sendDiagnostic(DiagnosticsCodec.ACTION_EVENTS, DiagnosticsCodec.eventsPayload(afterSeq, limit))
        )
}

/** Значение поля записи журнала: число или строка (перечень ключей закрыт, ADR-130 §2.3). */
public sealed interface DiagnosticValue {
    public data class Num(public val value: Long) : DiagnosticValue
    public data class Text(public val value: String) : DiagnosticValue
}

/**
 * Запись журнала.
 *
 * @property atMonoMs монотонное время от старта инстанса, мс. Календарного
 *   времени у записи нет — см. [DiagnosticsPage.estimatedWallMs].
 * @property conn соединение (`action`/`subscribe`/`media`) или `null`.
 */
public data class DiagnosticEvent(
    public val seq: Long,
    public val atMonoMs: Long,
    public val component: String,
    public val kind: String,
    public val conn: String?,
    public val fields: Map<String, DiagnosticValue>,
)

/**
 * Страница журнала.
 *
 * @property instanceStartWallMs календарное Unix-время старта инстанса, мс,
 *   снятое один раз при создании.
 * @property nextSeq курсор следующего чтения.
 * @property dropped сколько записей между прошлым курсором и первой записью
 *   страницы уже вытеснено — явный разрыв.
 */
public data class DiagnosticsPage(
    public val instanceId: String,
    public val instanceStartWallMs: Long,
    public val events: List<DiagnosticEvent>,
    public val nextSeq: Long,
    public val dropped: Long,
) {
    /**
     * ОЦЕНКА календарного времени события: старт инстанса + монотонное время
     * события. Не измеренное значение: после перевода системных часов
     * расходится с настоящим временем на величину перевода (ADR-130 §2.3).
     */
    public fun estimatedWallMs(event: DiagnosticEvent): Long = instanceStartWallMs + event.atMonoMs
}

/** Распределение длительностей: `counts[i]` — наблюдений ≤ `leUs[i]`, последний — больше. */
public data class DurationHistogram(
    public val leUs: List<Long>,
    public val counts: List<Long>,
    public val sumUs: Long,
    public val n: Long,
)

/** Одно из соединений инстанса. `state` — `none`, если соединение не поднималось. */
public data class ConnectionDiagnostics(
    public val state: String,
    public val inStateMs: Long,
    public val connects: Long,
    public val connectErrors: Long,
    public val reconnects: Long,
    public val lost: Long,
)

/** Вызовы `Photon_SendAction` (чтение диагностики в них не входит). */
public data class CallDiagnostics(
    public val inFlight: Long,
    public val total: Long,
    public val slow: Long,
    public val byClass: Map<String, Long>,
    public val duration: DurationHistogram,
)

/** События подписки, отданные клиенту. */
public data class SubscriptionEventCounts(
    public val connected: Long,
    public val networkChanged: Long,
    public val message: Long,
)

/** Состояние журнала. */
public data class JournalDiagnostics(
    public val capacity: Long,
    public val len: Long,
    public val lastSeq: Long,
    public val evicted: Long,
)

/** Локальное хранилище. `bolt` — статистика файла, `null`, если недоступна. */
public data class StorageDiagnostics(
    public val open: Boolean,
    public val schemaVersion: Long,
    public val viewTx: Long,
    public val updateTx: Long,
    public val viewErrors: Long,
    public val updateErrors: Long,
    public val rollbacks: Long,
    public val slowTx: Long,
    public val viewWait: DurationHistogram,
    public val viewExec: DurationHistogram,
    public val updateWait: DurationHistogram,
    public val updateExec: DurationHistogram,
    public val updateCommit: DurationHistogram,
    public val bolt: Map<String, Long>?,
)

/** Сводка конфигурации без секретов. */
public data class DiagnosticsConfigSummary(
    public val capacity: Long,
    public val slowTxMs: Long,
    public val slowCallMs: Long,
    public val bridgeConfigured: Boolean,
    public val storageConfigured: Boolean,
    public val p2pConfigured: Boolean,
)

/**
 * Снимок состояния инстанса (ADR-130 §2.5).
 *
 * @property enabled `false` — диагностика выключена конфигурацией; тогда
 *   разделы пусты, а это не ошибка (§12.4).
 * @property storage `null`, если хранилища у инстанса нет.
 * @property rawJson `data` ответа целиком — для полей, которых эта версия SDK
 *   не знает.
 */
public data class DiagnosticsSnapshot(
    public val instanceId: String,
    public val instanceStartWallMs: Long,
    public val uptimeMs: Long,
    public val enabled: Boolean,
    public val connections: Map<String, ConnectionDiagnostics>,
    public val calls: CallDiagnostics?,
    public val subscriptionEvents: SubscriptionEventCounts?,
    public val journal: JournalDiagnostics?,
    public val storage: StorageDiagnostics?,
    public val config: DiagnosticsConfigSummary?,
    public val rawJson: String,
)

/**
 * Сборка payload и разбор ответов 819/820. Тем же инструментом, что весь SDK
 * (§7.1): дерево kotlinx без компиляторного плагина.
 */
internal object DiagnosticsCodec {

    private val json = Json

    /**
     * Коды действий — с той же оговоркой, что у [LocalFeedCodec]: канонический
     * каталог — `photon.proto`, расхождение ловит тест, пиннингующий номера.
     */
    const val ACTION_SNAPSHOT: Int = 819
    const val ACTION_EVENTS: Int = 820

    fun isDiagnostic(action: Int): Boolean = action == ACTION_SNAPSHOT || action == ACTION_EVENTS

    fun eventsPayload(afterSeq: Long, limit: Int): String =
        buildJsonObject {
            put("after_seq", JsonPrimitive(afterSeq))
            put("limit", JsonPrimitive(limit))
        }.toString()

    // ── Разбор ──────────────────────────────────────────────────────────

    fun parsePage(response: PhotonResponse): DiagnosticsPage {
        val data = dataObject(response)
        val events = (data["events"] as? JsonArray ?: malformed(response, "events")).map { el ->
            val e = el as? JsonObject ?: malformed(response, "events[]")
            val fields = (e["fields"] as? JsonObject ?: malformed(response, "fields")).mapValues { (_, v) ->
                val p = v as? JsonPrimitive ?: malformed(response, "fields value")
                if (p.isString) DiagnosticValue.Text(p.content)
                else DiagnosticValue.Num(p.content.toLongOrNull() ?: malformed(response, "fields number"))
            }
            DiagnosticEvent(
                seq = long(e, "seq") ?: malformed(response, "seq"),
                atMonoMs = long(e, "at_mono_ms") ?: malformed(response, "at_mono_ms"),
                component = str(e, "component") ?: malformed(response, "component"),
                kind = str(e, "kind") ?: malformed(response, "kind"),
                conn = str(e, "conn"),
                fields = fields,
            )
        }
        return DiagnosticsPage(
            instanceId = str(data, "instance_id") ?: malformed(response, "instance_id"),
            instanceStartWallMs = long(data, "instance_start_wall_ms") ?: malformed(response, "instance_start_wall_ms"),
            events = events,
            nextSeq = long(data, "next_seq") ?: malformed(response, "next_seq"),
            dropped = long(data, "dropped") ?: malformed(response, "dropped"),
        )
    }

    fun parseSnapshot(response: PhotonResponse): DiagnosticsSnapshot {
        val data = dataObject(response)
        val config = data["config"] as? JsonObject ?: malformed(response, "config")
        val enabled = bool(config, "enabled") ?: malformed(response, "config.enabled")
        val connections = (data["connections"] as? JsonObject)?.mapValues { (_, v) ->
            val c = v as? JsonObject ?: malformed(response, "connections")
            ConnectionDiagnostics(
                state = str(c, "state") ?: malformed(response, "state"),
                inStateMs = long(c, "in_state_ms") ?: 0,
                connects = long(c, "connects") ?: 0,
                connectErrors = long(c, "connect_errors") ?: 0,
                reconnects = long(c, "reconnects") ?: 0,
                lost = long(c, "lost") ?: 0,
            )
        } ?: emptyMap()
        val calls = (data["calls"] as? JsonObject)?.let { c ->
            CallDiagnostics(
                inFlight = long(c, "in_flight") ?: 0,
                total = long(c, "total") ?: 0,
                slow = long(c, "slow") ?: 0,
                byClass = (c["by_class"] as? JsonObject)?.mapValues { (_, v) ->
                    (v as? JsonPrimitive)?.content?.toLongOrNull() ?: 0L
                } ?: emptyMap(),
                duration = histogram(c["duration"]),
            )
        }
        val events = (data["events"] as? JsonObject)?.let { e ->
            SubscriptionEventCounts(long(e, "connected") ?: 0, long(e, "network_changed") ?: 0, long(e, "message") ?: 0)
        }
        val journal = (data["journal"] as? JsonObject)?.let { j ->
            JournalDiagnostics(long(j, "capacity") ?: 0, long(j, "len") ?: 0, long(j, "last_seq") ?: 0, long(j, "evicted") ?: 0)
        }
        val storage = (data["storage"] as? JsonObject)?.let { s ->
            StorageDiagnostics(
                open = bool(s, "open") ?: false,
                schemaVersion = long(s, "schema_version") ?: 0,
                viewTx = long(s, "view_tx") ?: 0,
                updateTx = long(s, "update_tx") ?: 0,
                viewErrors = long(s, "view_errors") ?: 0,
                updateErrors = long(s, "update_errors") ?: 0,
                rollbacks = long(s, "rollbacks") ?: 0,
                slowTx = long(s, "slow_tx") ?: 0,
                viewWait = histogram(s["view_wait"]),
                viewExec = histogram(s["view_exec"]),
                updateWait = histogram(s["update_wait"]),
                updateExec = histogram(s["update_exec"]),
                updateCommit = histogram(s["update_commit"]),
                bolt = (s["bolt"] as? JsonObject)?.mapValues { (_, v) -> (v as? JsonPrimitive)?.content?.toLongOrNull() ?: 0L },
            )
        }
        val summary = if (!enabled) null else DiagnosticsConfigSummary(
            capacity = long(config, "capacity") ?: 0,
            slowTxMs = long(config, "slow_tx_ms") ?: 0,
            slowCallMs = long(config, "slow_call_ms") ?: 0,
            bridgeConfigured = bool(config, "bridge_configured") ?: false,
            storageConfigured = bool(config, "storage_configured") ?: false,
            p2pConfigured = bool(config, "p2p_configured") ?: false,
        )
        return DiagnosticsSnapshot(
            instanceId = str(data, "instance_id") ?: "",
            instanceStartWallMs = long(data, "instance_start_wall_ms") ?: 0,
            uptimeMs = long(data, "uptime_ms") ?: 0,
            enabled = enabled,
            connections = connections,
            calls = if (enabled) calls else null,
            subscriptionEvents = if (enabled) events else null,
            journal = if (enabled) journal else null,
            storage = storage,
            config = summary,
            rawJson = data.toString(),
        )
    }

    private fun histogram(el: JsonElement?): DurationHistogram {
        val h = el as? JsonObject ?: return DurationHistogram(emptyList(), emptyList(), 0, 0)
        fun longs(name: String) = (h[name] as? JsonArray)?.mapNotNull { (it as? JsonPrimitive)?.content?.toLongOrNull() }
            ?: emptyList()
        return DurationHistogram(longs("le_us"), longs("counts"), long(h, "sum_us") ?: 0, long(h, "n") ?: 0)
    }

    private fun dataObject(response: PhotonResponse): JsonObject {
        val ok = response as? PhotonResponse.Ok ?: malformed(response, "ok")
        val data = ok.data ?: malformed(response, "data")
        return runCatching { json.parseToJsonElement(data) }.getOrNull() as? JsonObject
            ?: malformed(response, "data")
    }

    /** Ответ не по контракту: ядро так не отвечает — рассогласование версий. */
    private fun malformed(response: PhotonResponse, what: String): Nothing =
        throw IllegalStateException("diagnostics response does not match the contract ($what): ${response.raw}")

    /** Длинное целое без промежуточного Double (значения выше 2^53 точны). */
    private fun long(obj: JsonObject, name: String): Long? =
        (obj[name] as? JsonPrimitive)?.takeIf { !it.isString }?.content?.toLongOrNull()

    private fun str(obj: JsonObject, name: String): String? =
        (obj[name] as? JsonPrimitive)?.takeIf { it.isString && it !is JsonNull }?.content

    private fun bool(obj: JsonObject, name: String): Boolean? =
        (obj[name] as? JsonPrimitive)?.takeIf { !it.isString }?.content?.toBooleanStrictOrNull()
}
