package dev.photon.devtools

import dev.photon.sdk.BridgeConfig
import dev.photon.sdk.PhotonClient
import dev.photon.sdk.PhotonConfig
import dev.photon.sdk.PhotonEvent
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

/*
 * Logging — единый хронологический поток того, что ВИДНО клиенту.
 *
 * Границы записаны здесь, а не в отчёте, потому что их легко потерять:
 *
 *  • централизованного лога ЯДРА нет и взять его неоткуда. Разведка
 *    2026-09-11: во всём photon-core ни одного импорта log/slog/logrus/zap
 *    (кроме тестов), и среди портов нет порта логирования. Дело не в том,
 *    что лог есть и не выведен наружу, — писать нечем. Появление такого
 *    потока потребовало бы завести логирование в ядре, то есть отдельное
 *    архитектурное решение;
 *
 *  • поэтому здесь — журнал ДЕЙСТВИЙ инструмента и СОБЫТИЯ подписки,
 *    и ничего сверх. Называть это «логами Photon» было бы неверно.
 */

/** Формат без зависимости от локали — тот же довод, что в Network Lab. */
private fun fmtMs(v: Double): String = String.format(java.util.Locale.ROOT, "%.1f", v)

private val logClock = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault())

/** Уровень выводится из исхода, а не назначается вручную. */
enum class Level { INFO, WARN, ERROR }

enum class Source { ACTION, EVENT }

data class LogEntry(
    val seq: Long,
    val epochNanos: Long,
    val source: Source,
    val level: Level,
    val component: String,
    val text: String,
    /** Идентификатор цепочки — только у действия; у события его нет. */
    val correlation: String? = null,
    /**
     * Доменный ключ записи (id поста), по которому цепочка собирается ПОСЛЕ
     * прогона.
     *
     * Найдено живым прогоном: push прилетает в ту же миллисекунду, что и
     * ответ на действие, и иногда раньше, чем ответ разобран, — подписное
     * соединение независимо от action-соединения. Поэтому искать связь в
     * момент прихода события нельзя: это гонка по построению. Ключ
     * запоминается у обеих сторон, а соединяются они при отчёте.
     */
    val key: String? = null,
) {
    val at: String get() = logClock.format(Instant.ofEpochMilli(epochNanos / 1_000_000))
}

/** Уровень исхода: вердикт сервера — предупреждение, несостоявшийся вызов — ошибка. */
fun levelOf(outcome: Outcome): Level = when (outcome) {
    Outcome.OK -> Level.INFO
    Outcome.VERDICT_FALSE -> Level.WARN
    // Вызов состоялся, но ответ не распознан: не успех и не сбой связи.
    Outcome.UNRECOGNIZED -> Level.WARN
    Outcome.CALL_ERROR -> Level.ERROR
}

/**
 * Хронологический поток с фильтрацией.
 *
 * Порядок задаёт seq, а не отметка времени: у двух записей она может
 * совпасть с точностью до миллисекунды, и тогда сортировка по времени
 * переставляла бы их между прогонами.
 */
class LogStream {
    private val entries = ConcurrentLinkedQueue<LogEntry>()
    private val seq = AtomicLong(0)

    fun add(
        source: Source,
        level: Level,
        component: String,
        text: String,
        correlation: String? = null,
        key: String? = null,
    ): LogEntry {
        val e = LogEntry(seq.incrementAndGet(), System.nanoTime(), source, level, component, text, correlation, key)
        entries.add(e)
        return e
    }

    fun all(): List<LogEntry> = entries.sortedBy { it.seq }

    fun filter(
        source: Source? = null,
        minLevel: Level = Level.INFO,
        component: String? = null,
        contains: String? = null,
    ): List<LogEntry> = all().filter { e ->
        (source == null || e.source == source) &&
        e.level.ordinal >= minLevel.ordinal &&
        (component == null || e.component.equals(component, ignoreCase = true)) &&
        (contains == null || e.text.contains(contains, ignoreCase = true))
    }

    /**
     * Цепочки «действие → событие», собранные ПОСЛЕ прогона.
     *
     * Соединение по доменному ключу и только по нему: сквозного
     * идентификатора запроса протокол не передаёт (ADR-046), и придумывать
     * его здесь нельзя.
     */
    fun chains(): Map<String, List<LogEntry>> {
        val byKey = all().filter { it.source == Source.ACTION && it.key != null }
            .associate { it.key!! to it.correlation!! }
        val out = LinkedHashMap<String, MutableList<LogEntry>>()
        all().forEach { e ->
            val id = when {
                e.source == Source.ACTION && e.correlation != null && e.key != null -> e.correlation
                e.source == Source.EVENT && e.key != null -> byKey[e.key]
                else -> null
            } ?: return@forEach
            out.getOrPut(id) { mutableListOf() }.add(e)
        }
        return out.filterValues { it.size > 1 }
    }

    /** Записи одной цепочки. */
    fun correlated(id: String): List<LogEntry> = chains()[id] ?: emptyList()
}

/**
 * Единственная связь между действием и событием, доступная клиенту.
 *
 * Ядро не разбирает data события (ADR-025/034), поэтому корреляция строится
 * по идентификатору поста: он есть и в ответе createPost, и в теле пришедшего
 * MESSAGE. Никакой сквозной идентификатор запроса протоколом не передаётся —
 * в конверте push нет ни номера, ни версии (ADR-046), — и выдумывать его
 * здесь нельзя.
 */
fun correlationKeyOf(response: String?): String? =
    response?.let { Regex("\"id\"\\s*:\\s*\"([a-z0-9]{20,})\"").find(it)?.groupValues?.get(1) }

class LoggingView(
    private val endpoint: String,
    private val token: String,
    private val catalog: Inspector?,
) {
    private val stream = LogStream()

    private fun actionName(code: Int): String {
        // Имя действия берётся из канонического каталога, а не из таблицы в
        // коде: своей копии каталога у инструмента нет (вариант C1).
        val e = catalog?.enum("Action") ?: return "action=$code"
        return e.valueList.firstOrNull { it.number == code }?.name ?: "НЕИЗВЕСТНЫЙ код $code"
    }

    private fun config() = PhotonConfig(
        BridgeConfig(
            endpoint = endpoint,
            sessionToken = token,
            connectTimeout = Duration.ofSeconds(15),
            callTimeout = Duration.ofSeconds(20),
        )
    )

    private fun listener() = dev.photon.sdk.PhotonEventListener { event ->
        run {
            val type = label(event)
            // Только запоминаем ключ. Связь ищется при отчёте: событие может
            // прийти раньше, чем разобран ответ на вызвавшее его действие.
            stream.add(Source.EVENT, Level.INFO, type, event.raw.take(140),
                key = correlationKeyOf(event.raw))
        }
    }

    fun run(channelId: String?) {
        println("Logging — поток действий и событий")
        println("источник имён действий: канонический каталог (FileDescriptorSet)\n")

        PhotonClient.create(config()).use { p ->
            p.addEventListener(listener())
            stream.add(Source.ACTION, Level.INFO, "подписка", "слушатель установлен, подписка запущена")

            fun send(code: Int, payload: String, note: String): String? {
                val r = call(p, code, payload)
                val id = "act-${stream.all().size + 1}"
                stream.add(Source.ACTION, levelOf(r.outcome), actionName(code),
                    "$note — ${r.detail}, ${fmtMs(r.millis)} мс", id,
                    key = correlationKeyOf(r.response))
                return r.response
            }

            send(UsedActions.GET_ME, "{}", "состояние субъекта")
            send(UsedActions.DELIBERATELY_UNKNOWN, "{}", "заведомо неизвестный код")
            send(UsedActions.GET_OR_CREATE_DIRECT_CHANNEL, "{}", "payload без обязательного поля")

            if (channelId != null) {
                // Единственная цепочка, которую можно проследить целиком:
                // созданный пост возвращает id, и он же приходит в MESSAGE.
                val resp = send(UsedActions.CREATE_POST,
                    """{"channel_id":"$channelId","message":"logging-probe"}""",
                    "пост для проверки корреляции")
                Thread.sleep(4000)
                correlationKeyOf(resp)?.let { id ->
                    send(UsedActions.DELETE_POST, """{"post_id":"$id"}""", "уборка")
                }
            } else {
                Thread.sleep(4000)
            }

            printAll()
            printFilters()
            printLimits()
        }
    }

    private fun printAll() {
        println("── поток, хронологически ─────────────────────────")
        stream.all().forEach { e ->
            val mark = when (e.level) { Level.INFO -> "  "; Level.WARN -> "⚠ "; Level.ERROR -> "✖ " }
            val corr = e.correlation?.let { " [$it]" } ?: ""
            println("  ${e.at} $mark${e.source.name.padEnd(6)} ${e.component.padEnd(34)} ${e.text}$corr")
        }
    }

    private fun printFilters() {
        println("\n── фильтры ───────────────────────────────────────")
        println("  только события:        ${stream.filter(source = Source.EVENT).size}")
        println("  только действия:       ${stream.filter(source = Source.ACTION).size}")
        println("  уровень WARN и выше:   ${stream.filter(minLevel = Level.WARN).size}")
        println("  уровень ERROR:         ${stream.filter(minLevel = Level.ERROR).size}")
        val chains = stream.chains()
        println("  цепочек действие→событие: ${chains.size}")
        chains.forEach { (id, entries) ->
            println("    $id:")
            entries.forEach { println("      ${e2s(it)}") }
        }
    }

    private fun e2s(e: LogEntry) = "${e.at}  ${e.source.name.padEnd(6)} ${e.component} — ${e.text.take(70)}"

    private fun printLimits() {
        println("\n── что этим НЕ является ──────────────────────────")
        println("  • это НЕ логи ядра. Централизованного лога Photon Core не")
        println("    существует: во всём photon-core нет ни одного логгера и нет")
        println("    порта логирования. Его появление — отдельное архитектурное")
        println("    решение, а не отсутствующая реализация инструмента.")
        println("  • сквозного идентификатора запроса в протоколе нет: в конверте")
        println("    push нет ни номера, ни версии (ADR-046). Корреляция строится")
        println("    по id поста — единственной связи, видимой клиенту.")
    }
}
