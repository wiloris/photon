package dev.photon.devtools

import dev.photon.core.Photon
import dev.photon.core.PhotonEventListener
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentLinkedQueue

/*
 * Photon Developer Tools — Dashboard, объём Phase 16.
 *
 * Первый настоящий клиент Photon: он ходит ТЕМ ЖЕ публичным путём, которым
 * пойдут приложения — Kotlin → JNI → C ABI → Photon Core. Прямых импортов
 * внутренних пакетов ядра здесь нет ни одного, и появиться они не могут:
 * инструмент видит ровно то, что видит любое приложение.
 *
 * Сознательно НЕ содержит Compose и UI-архитектуры: на этом шаге нужен
 * технический потребитель, а не продукт. Что показывает — определено
 * ROADMAP.md, Phase 16, раздел Dashboard, и ничего сверх.
 *
 * Адрес Bridge и Session ID — аргументы. В коде их нет и не будет.
 */

private val clock = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")

private fun stamp(): String = LocalTime.now().format(clock)

/** Строка журнала действий: код, payload, исход, задержка. */
private data class ActionRecord(
    val at: String,
    val action: Int,
    val payload: String,
    val outcome: String,
    val millis: Long,
)

/**
 * Журнал действий и событий.
 *
 * События приходят СИНХРОННО на потоке доставки ядра (jni-events.md, D1),
 * поэтому очередь здесь — не буфер доставки, а хранилище для показа.
 */
private class Dashboard {
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
        println("  ⚡ ${stamp()}  $type")
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
    fun send(p: Photon, action: Int, payload: String): String? {
        // Классификация живёт в Outcome.kt и только там: Network Lab
        // пользуется той же, и две копии разошлись бы.
        val r = call(p, action, payload)
        actions.add(ActionRecord(stamp(), action, payload, r.detail, r.millis.toLong()))
        return r.response
    }

    fun printActions() {
        println("\n── журнал действий ───────────────────────────────")
        if (actions.isEmpty()) println("  (пусто)")
        actions.forEach {
            println("  ${it.at}  action=${it.action}  ${it.millis} мс")
            println("      payload: ${it.payload.take(90)}")
            println("      исход:   ${it.outcome}")
        }
    }

    fun printEvents() {
        println("\n── события ───────────────────────────────────────")
        if (events.isEmpty()) println("  (ни одного)")
        events.forEach { println("  $it") }
    }
}

private fun config(endpoint: String, token: String) =
    """{"bridge":{"endpoint":"$endpoint","session_token":"$token","connect_timeout_ms":15000,"call_timeout_ms":20000}}"""

fun main(args: Array<String>) {
    // Network Lab — отдельный диагностический режим, а не часть Dashboard:
    // у него другой объём работы и другой отчёт.
    if (args.isNotEmpty() && args[0] == "--network-lab") {
        if (args.size < 3) {
            System.err.println("нужно: --network-lab <endpoint> <sessionId> " +
                "[серия] [потоки] [на поток] [секунд]")
            kotlin.system.exitProcess(2)
        }
        Photon.loadLibrary()
        NetworkLab(args[1], args[2]).run(
            serialCount = args.getOrNull(3)?.toIntOrNull() ?: 200,
            threads = args.getOrNull(4)?.toIntOrNull() ?: 8,
            perThread = args.getOrNull(5)?.toIntOrNull() ?: 25,
            soakSeconds = args.getOrNull(6)?.toIntOrNull() ?: 20,
        )
        return
    }

    if (args.isNotEmpty() && args[0] == "--protobuf-inspector") {
        // Сети и native не требует вовсе: каталог читается из дескриптора.
        Inspector.fromSystemProperty().printCatalog(
            showMessages = args.contains("-messages") || args.contains("-all"),
            messageLimit = if (args.contains("-all")) Int.MAX_VALUE else 8,
        )
        return
    }

    if (args.isNotEmpty() && args[0] == "--logging") {
        if (args.size < 3) {
            System.err.println("нужно: --logging <endpoint> <sessionId> [channelId]")
            kotlin.system.exitProcess(2)
        }
        Photon.loadLibrary()
        // Каталог нужен, чтобы показывать ИМЕНА действий, а не голые числа.
        // Недоступен — поток всё равно работает, просто без имён.
        val catalog = runCatching { Inspector.fromSystemProperty() }.getOrNull()
        LoggingView(args[1], args[2], catalog).run(args.getOrNull(3))
        return
    }

    // Crypto Lab — наблюдение следов криптографии. Ключей у инструмента нет
    // и не будет (ADR-021): он смотрит на исходы обычных действий.
    if (args.isNotEmpty() && args[0] == "--crypto-lab") {
        if (args.size < 3) {
            System.err.println("нужно: --crypto-lab <endpoint> <sessionId> " +
                "[тот же сервер по IP — для сверки имени в сертификате]")
            kotlin.system.exitProcess(2)
        }
        // Каталог нужен стражу ADR-021: список действий берётся канонический,
        // своей копии у инструмента нет.
        val catalog = runCatching { Inspector.fromSystemProperty() }.getOrNull()
        CryptoLab(args[1], args[2], catalog).run(args.getOrNull(3)?.takeIf { it.isNotBlank() })
        return
    }

    // Storage Lab — отдельный диагностический режим. Меряет ЧТЕНИЕ ЛЕНТЫ
    // через доменные действия; локального хранилища на публичном пути нет, и
    // режим это показывает, а не обходит.
    if (args.isNotEmpty() && args[0] == "--storage-lab") {
        if (args.size < 3) {
            System.err.println("нужно: --storage-lab <endpoint> <sessionId> " +
                "[channelId] [seed] [limit] [страниц вглубь] [возвратов]")
            kotlin.system.exitProcess(2)
        }
        Photon.loadLibrary()
        StorageLab(args[1], args[2]).run(
            channelArg = args.getOrNull(3)?.takeIf { it.isNotBlank() && it != "-" },
            seed = args.getOrNull(4)?.toIntOrNull() ?: 0,
            limit = args.getOrNull(5)?.toIntOrNull() ?: 20,
            depthPages = args.getOrNull(6)?.toIntOrNull() ?: 10,
            returns = args.getOrNull(7)?.toIntOrNull() ?: 5,
        )
        return
    }

    if (args.isNotEmpty() && args[0] == "--performance") {
        Photon.loadLibrary()
        PerfLab(args.getOrNull(1), args.getOrNull(2)).run(
            samples = args.getOrNull(3)?.toIntOrNull() ?: 200,
            perThread = args.getOrNull(4)?.toIntOrNull() ?: 20,
        )
        return
    }

    println("Photon Developer Tools — Dashboard (Phase 16)")
    println("путь: Kotlin → JNI → C ABI → Photon Core\n")

    // java.library.path выставляет Gradle; ручной LD_LIBRARY_PATH не нужен —
    // libphoton_jni.so слинкована с rpath $ORIGIN и находит libphoton_core.so
    // соседом (ADR-051).
    Photon.loadLibrary()

    println("── ядро ──────────────────────────────────────────")
    val abi = Photon.abiVersion()
    val major = abi shr 16
    val minor = abi and 0xFFFF
    println("  версия ABI:      $major.$minor")
    println("  ожидается MAJOR: ${Photon.EXPECTED_ABI_MAJOR}")
    println("  совместимость:   ${if (major == Photon.EXPECTED_ABI_MAJOR) "да" else "НЕТ — клиент собран под другой MAJOR"}")

    val online = args.size >= 2
    val cfg = if (online) config(args[0], args[1]) else "{}"
    if (!online) {
        println("\n  режим: БЕЗ Bridge (адрес и Session ID не переданы).")
        println("  запуск с ними:  ./gradlew run --args=\"<endpoint> <sessionId>\"")
    }

    val dash = Dashboard()
    Photon.create(cfg).use { photon ->
        println("\n── инстанс ───────────────────────────────────────")
        println("  создан:          да")
        println("  последняя ошибка: ${photon.lastError() ?: "нет"}")

        // Первый ненулевой слушатель ЗАПУСКАЕТ подписку: это единственный
        // вызов ABI, выражающий такое намерение (photon_core.h).
        println("\n── подписка ──────────────────────────────────────")
        photon.setEventListener(object : PhotonEventListener {
            override fun onEvent(eventJson: String) = dash.onEvent(eventJson)
        })
        println("  слушатель установлен, подписка запущена")

        dash.send(photon, UsedActions.GET_ME, "{}")   // доказывает весь путь
        dash.send(photon, UsedActions.DELIBERATELY_UNKNOWN, "{}")  // ошибка ВЫЗОВА

        if (online) {
            // Отказ СЕРВЕРА при состоявшемся вызове: payload без обязательного
            // поля. Именно эта строка показывает разницу между исходом вызова
            // и вердиктом операции — без неё различение видно наполовину.
            dash.send(photon, UsedActions.GET_OR_CREATE_DIRECT_CHANNEL, "{}")
            Thread.sleep(6000)                  // окно, в котором приходит CONNECTED
        }

        println("\n  состояние подписки: ${dash.subscription}")
        dash.printActions()
        dash.printEvents()
        println("\n── завершение ────────────────────────────────────")
        println("  close(): после возврата колбэк не будет вызван ни разу")
    }
    println("  инстанс закрыт")
}
