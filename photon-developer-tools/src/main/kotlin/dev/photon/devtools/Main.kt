package dev.photon.devtools

import dev.photon.core.Photon
import dev.photon.core.PhotonEventListener

/*
 * Photon Developer Tools — точка входа и Dashboard, объём Phase 16.
 *
 * Первый настоящий клиент Photon: он ходит ТЕМ ЖЕ публичным путём, которым
 * пойдут приложения — Kotlin → JNI → C ABI → Photon Core. Прямых импортов
 * внутренних пакетов ядра здесь нет ни одного, и появиться они не могут:
 * инструмент видит ровно то, что видит любое приложение.
 *
 * Сознательно НЕ содержит Compose и UI-архитектуры: на этом шаге нужен
 * технический потребитель, а не продукт. Что показывает — определено
 * ROADMAP.md, Phase 16, раздел Dashboard, и ничего сверх. Логика Dashboard —
 * в Dashboard.kt; здесь только выбор режима и порядок шагов.
 *
 * Адрес Bridge и Session ID — аргументы. В коде их нет и не будет.
 */

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

    // Обрыв и восстановление — через локальный UDP-ретранслятор отдельным
    // процессом (методика Phase 15.5). Искусственный разрыв пути, не смена
    // интерфейса и не убитый сервер: границы печатаются в отчёте.
    if (args.isNotEmpty() && args[0] == "--reconnect-lab") {
        if (args.size < 3) {
            System.err.println("нужно: --reconnect-lab <endpoint host:port> <sessionId> [циклов]")
            kotlin.system.exitProcess(2)
        }
        Photon.loadLibrary()
        ReconnectLab(args[1], args[2]).run(cycles = args.getOrNull(3)?.toIntOrNull() ?: 2)
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

    // Plugin Lab — собственный экземпляр runtime (ADR-057). Native-путь
    // Photon здесь не задействован вовсе: это отдельный дочерний процесс.
    if (args.isNotEmpty() && args[0] == "--plugin-lab") {
        val rt = System.getProperty("photon.lab.runtime")
        val hello = System.getProperty("photon.lab.fixture.hello")
        val sdk = System.getProperty("photon.lab.fixture.sdkscenario")
        if (rt == null || hello == null || sdk == null) {
            System.err.println("не заданы photon.lab.runtime и photon.lab.fixture.* — " +
                "запускать через `gradle run --args=\"--plugin-lab [сценарий]\"`")
            kotlin.system.exitProcess(2)
        }
        // Сценарий — id из каталога runtime; без него runtime запускает свой
        // сценарий по умолчанию. Своего списка у инструмента нет.
        PluginLab(java.io.File(rt), java.io.File(hello), java.io.File(sdk)).run(args.getOrNull(1))
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
    val abi = abiInfo(Photon.abiVersion(), Photon.EXPECTED_ABI_MAJOR)
    println("  версия ABI:      ${abi.major}.${abi.minor}")
    println("  ожидается MAJOR: ${Photon.EXPECTED_ABI_MAJOR}")
    println("  совместимость:   ${if (abi.compatible) "да" else "НЕТ — клиент собран под другой MAJOR"}")

    val online = args.size >= 2
    val cfg = if (online) dashboardConfig(args[0], args[1]) else "{}"
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
