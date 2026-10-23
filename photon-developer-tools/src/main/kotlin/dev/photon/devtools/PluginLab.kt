package dev.photon.devtools

import java.io.BufferedReader
import java.io.File
import java.util.Locale
import java.util.concurrent.TimeUnit

/*
 * Plugin Lab — собственный экземпляр runtime (ADR-057) на настоящем
 * исполнителе coreops (ADR-058, photon-plugin-runtime/docs/plugin-lab.md).
 *
 * Инструмент ВЛАДЕЕТ собственным экземпляром Plugin Runtime: запускает его
 * дочерним процессом и управляет им по stdin/stdout. Чужой runtime он не ищет
 * и найти не может — обнаружения нет и не заводится.
 *
 * ГРАНИЦА ДОВЕРИЯ — РОДИТЕЛЬСТВО ПРОЦЕССА. Ни сокета, ни порта, ни токена:
 * канал недостижим ни для кого, кроме родителя. Поэтому здесь нет и не должно
 * появиться ни аутентификации, ни авторизации управляющей стороны — они
 * защищали бы то, до чего никто снаружи не дотягивается.
 *
 * УПРАВЛЯЮЩИЙ КАНАЛ — НЕ PLUGIN IPC. Механика похожа (родитель владеет трубами
 * потомка), собеседник противоположен: у Plugin IPC он недоверенный, здесь —
 * свой же процесс. Кодек поэтому свой, и общего с Plugin IPC у него ничего.
 *
 * КАТАЛОГ СЦЕНАРИЕВ ПРИХОДИТ ИЗ КАНАЛА. Своей копии здесь нет: ни id
 * сценариев, ни идентичностей плагинов, ни прав — выбирается только id, а
 * всё остальное runtime выводит сам. Лаборатория НАБЛЮДАЕТ результат;
 * свойства доказывает тестовый срез B4a, а не этот отчёт.
 *
 * Чего здесь нет по построению: C ABI не участвует вовсе (`Photon` не
 * загружается), Photon Core о плагинах по-прежнему не знает (ADR-030),
 * управление чужим или production runtime невозможно.
 */

private fun ms(v: Long): String = String.format(Locale.ROOT, "%.1f", v / 1_000_000.0)

/** Граница лаборатории, как её сообщает runtime. Ключей здесь нет. */
data class LabInfo(
    val keyMaterial: String?,
    val storage: String?,
    val storageDir: String?,
    val productionComposition: Boolean,
)

/** Строка каталога сценариев, как её отдаёт runtime. */
data class ScenarioInfo(
    val id: String,
    val fixture: String?,
    val pluginId: String?,
    val granted: List<String>,
    val namespace: String?,
    val description: String?,
)

/** Ответ управляющего канала. Разбор нарочно свой и минимальный. */
data class ControlReply(
    val raw: String,
    val ok: Boolean,
    val cmd: String,
    val error: String?,
    val runtimeState: String?,
    val pluginId: String?,
    val pluginState: String?,
    val granted: List<String>,
    val operations: List<String>,
    val isolationPlatform: String?,
    val scenario: String? = null,
    val fixture: String? = null,
    val namespace: String? = null,
    val results: Map<String, String> = emptyMap(),
    val scenarios: List<ScenarioInfo> = emptyList(),
    val lab: LabInfo? = null,
)

private fun field(json: String, name: String): String? =
    Regex("\"$name\"\\s*:\\s*\"([^\"]*)\"").find(json)?.groupValues?.get(1)

private fun boolField(json: String, name: String): Boolean =
    Regex("\"$name\"\\s*:\\s*(true|false)").find(json)?.groupValues?.get(1) == "true"

private fun stringArray(json: String, name: String): List<String> {
    val block = Regex("\"$name\"\\s*:\\s*\\[([^\\]]*)\\]").find(json)?.groupValues?.get(1) ?: return emptyList()
    return Regex("\"([^\"]+)\"").findAll(block).map { it.groupValues[1] }.toList()
}

/** Плоский объект "имя":"значение" по ключу. Вложенных объектов в нём нет. */
private fun flatObject(json: String, name: String): String? =
    Regex("\"$name\"\\s*:\\s*\\{([^{}]*)\\}").find(json)?.groupValues?.get(1)

private fun parseResults(json: String): Map<String, String> {
    val block = flatObject(json, "results") ?: return emptyMap()
    return Regex("\"([^\"]+)\"\\s*:\\s*\"([^\"]*)\"").findAll(block)
        .associate { it.groupValues[1] to it.groupValues[2] }
}

private fun parseLab(json: String): LabInfo? {
    val block = flatObject(json, "lab") ?: return null
    return LabInfo(
        keyMaterial = field(block, "key_material"),
        storage = field(block, "storage"),
        storageDir = field(block, "storage_dir"),
        productionComposition = boolField(block, "production_composition"),
    )
}

private fun parseScenarios(json: String): List<ScenarioInfo> {
    val block = Regex("\"scenarios\"\\s*:\\s*\\[(.*)\\]", RegexOption.DOT_MATCHES_ALL)
        .find(json)?.groupValues?.get(1) ?: return emptyList()
    return Regex("\\{[^{}]*\\}").findAll(block).mapNotNull { m ->
        val o = m.value
        val id = field(o, "id") ?: return@mapNotNull null
        ScenarioInfo(id, field(o, "fixture"), field(o, "plugin_id"), stringArray(o, "granted"),
            field(o, "namespace"), field(o, "description"))
    }.toList()
}

/**
 * Разбор ответа. Чистая функция: проверяется без процессов и без сети — а
 * значит выводы лаборатории («плагин загружен», «операция авторизована»)
 * можно проверить, не поднимая runtime.
 */
fun parseControlReply(line: String): ControlReply = ControlReply(
    raw = line,
    ok = boolField(line, "ok"),
    cmd = field(line, "cmd") ?: "?",
    error = field(line, "error"),
    runtimeState = field(line, "runtime_state"),
    pluginId = if (line.contains("\"scenarios\"")) null else field(line, "plugin_id"),
    pluginState = field(line, "plugin_state"),
    granted = if (line.contains("\"scenarios\"")) emptyList() else stringArray(line, "granted"),
    // Операции лежат объектами; имена достаточно вытащить по ключу "op".
    operations = Regex("\"op\"\\s*:\\s*\"([^\"]+)\"").findAll(line).map { it.groupValues[1] }.toList(),
    isolationPlatform = field(line, "platform"),
    scenario = field(line, "scenario"),
    fixture = if (line.contains("\"scenarios\"")) null else field(line, "fixture"),
    namespace = if (line.contains("\"scenarios\"")) null else field(line, "namespace"),
    results = parseResults(line),
    scenarios = parseScenarios(line),
    lab = parseLab(line),
)

/**
 * Граница лаборатории — печатается в каждом отчёте (plugin-lab.md §9.2).
 * Отдельная функция, чтобы её полнота проверялась тестом, а не на глаз.
 */
fun labBoundaryLines(lab: LabInfo?): List<String> = listOf(
    "ключи устройства и мастер-ключ одноразовые (${lab?.keyMaterial ?: "?"}): созданы при старте, на диск не пишутся",
    "хранилище временное (${lab?.storage ?: "?"}): ${lab?.storageDir ?: "?"}; удаляется при штатном завершении, после kill -9 может остаться",
    "это не продакшн-композиция и не образец для cmd/photon-core-server",
    "свойства доказаны B4a: tests/lifecycle/sdkscenario_test.go",
)

/** Подпись к значению, которое легко принять за доказательство (§8). */
fun resultCaption(name: String): String? = when (name) {
    "verify_raw" -> "наблюдение, не доказательство; см. TestSDKScenario_SignIsDomainSeparated"
    else -> null
}

/** Кадр load. Id экранируется: он приходит от пользователя, проверяет его runtime. */
fun loadFrame(seq: Int, scenario: String?): String =
    if (scenario == null) """{"id":$seq,"cmd":"load"}"""
    else """{"id":$seq,"cmd":"load","scenario":"${scenario.replace("\\", "\\\\").replace("\"", "\\\"")}"}"""

/**
 * Шаг жизненного цикла — то, что лаборатория обязана показать по пунктам, а не
 * одной строкой «работает».
 */
data class LifecycleStep(val name: String, val ok: Boolean, val detail: String, val millis: Long)

class PluginLab(
    private val runtimeBinary: File,
    private val helloFixture: File,
    private val sdkFixture: File,
) {

    private var process: Process? = null
    private var reader: BufferedReader? = null
    private var seq = 0
    private val steps = mutableListOf<LifecycleStep>()
    private var lab: LabInfo? = null
    private var last: ControlReply? = null

    fun run(scenario: String? = null) {
        println("Plugin Lab — собственный экземпляр runtime (ADR-057) на настоящем coreops (ADR-058)")
        println("runtime: ${runtimeBinary.name}, фикстуры: ${helloFixture.name}, ${sdkFixture.name}\n")

        if (!runtimeBinary.canExecute() || !helloFixture.canExecute() || !sdkFixture.canExecute()) {
            System.err.println("не собраны артефакты лаборатории: ${runtimeBinary.path}, ${helloFixture.path}, ${sdkFixture.path}")
            System.err.println("собрать: gradle pluginLabNative")
            return
        }

        try {
            start()
            catalog()
            // Отвергнутый load — детерминированный отказ: диагностировать
            // нечего, и ожидание завершения сценария не начинается вовсе.
            if (load(scenario)) diagnose() else skipDiagnose()
            shutdown()
        } finally {
            // Страховка на любой выход, включая исключение: процесс не должен
            // пережить лабораторию. Основная гарантия — на стороне runtime
            // (EOF на stdin), это вторая линия.
            process?.let { if (it.isAlive) it.destroy() }
        }
        report()
    }

    // ── шаги ─────────────────────────────────────────────────────────────────

    private fun start() {
        val t = System.nanoTime()
        val p = ProcessBuilder(runtimeBinary.absolutePath,
            "-fixture-hello", helloFixture.absolutePath,
            "-fixture-sdkscenario", sdkFixture.absolutePath)
            .redirectErrorStream(false)
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start()
        process = p
        reader = p.inputStream.bufferedReader()

        // Если JVM уйдёт штатно — закрыть трубу немедленно, не дожидаясь GC.
        Runtime.getRuntime().addShutdownHook(Thread { if (p.isAlive) p.destroy() })

        val ready = parseControlReply(reader!!.readLine() ?: "")
        lab = ready.lab
        step("start → runtime ready", ready.ok && ready.runtimeState == "ready",
            if (ready.ok) "временное хранилище: ${ready.lab?.storageDir}" else "отказ: ${ready.error}", t)
        println("  ⚙ runtime поднят, pid ${p.pid()}")
    }

    /** Каталог приходит из runtime — выбирать можно только из него. */
    private fun catalog() {
        val t = System.nanoTime()
        val r = call("scenarios")
        step("closed scenario catalog", r.ok && r.scenarios.isNotEmpty(), "сценариев: ${r.scenarios.size}", t)
        r.scenarios.forEach {
            println("  📋 ${it.id}: ${it.description} — ${it.pluginId}, права ${it.granted}, namespace ${it.namespace}")
        }
    }

    /** Возвращает, загружен ли плагин: от этого зависит, есть ли что диагностировать. */
    private fun load(scenario: String?): Boolean {
        val t = System.nanoTime()
        val r = send(loadFrame(++seq, scenario))
        val loaded = r.ok && r.pluginState == "running"
        loadError = r.error
        step("plugin loaded", loaded,
            if (r.ok) "сценарий ${r.scenario}: ${r.pluginId}, права ${r.granted}, namespace ${r.namespace}"
            else "отказ: ${r.error}", t)
        return loaded
    }

    private var loadError: String? = null

    /** Шаг диагностики при отвергнутом load: сразу неуспех, без опроса. */
    private fun skipDiagnose() {
        step("diagnostic operation", false,
            "не выполнялась: плагин не загружен (${loadError ?: "причина не сообщена"})", System.nanoTime())
    }

    /**
     * Диагностическая операция — read-only и над собственным экземпляром.
     *
     * Показывает не «плагин жив», а путь авторизации: операция, дошедшая до
     * исполнителя, прошла Authorizer, потому что проверка прав стоит в сессии
     * ДО вызова исполнителя. Ждёт, пока плагин сообщит о завершении сценария.
     */
    private fun diagnose() {
        val t = System.nanoTime()
        var r = call("diagnose")
        val deadline = System.currentTimeMillis() + 15_000
        while (!finished(r) && System.currentTimeMillis() < deadline) {
            Thread.sleep(100)
            r = call("diagnose")
        }
        last = r
        step("diagnostic operation", r.ok && r.operations.isNotEmpty() && finished(r),
            "состояние ${r.pluginState}, права ${r.granted}, операций ${r.operations.size}", t)
        println("  🔬 изоляция: ${r.isolationPlatform}")
        println("  🔬 операции плагина (дошли до исполнителя ⇒ авторизованы): ${r.operations}")
    }

    private fun finished(r: ControlReply) = r.results.containsKey("done") || r.results.containsKey("ready")

    private fun shutdown() {
        val t = System.nanoTime()
        val r = call("shutdown")
        val exited = process?.waitFor(10, TimeUnit.SECONDS) ?: false
        step("graceful shutdown", r.ok && exited,
            "состояние ${r.runtimeState}, процесс завершился: $exited", t)
    }

    // ── канал ────────────────────────────────────────────────────────────────

    private fun call(cmd: String): ControlReply = send("""{"id":${++seq},"cmd":"$cmd"}""")

    private fun send(frame: String): ControlReply {
        val p = process ?: error("runtime не запущен")
        p.outputStream.write(frame.toByteArray())
        p.outputStream.write('\n'.code)
        p.outputStream.flush()
        val line = reader?.readLine() ?: return ControlReply(
            "", false, "?", "канал закрыт", null, null, null, emptyList(), emptyList(), null)
        return parseControlReply(line)
    }

    private fun step(name: String, ok: Boolean, detail: String, startedNanos: Long) {
        val s = LifecycleStep(name, ok, detail, System.nanoTime() - startedNanos)
        steps.add(s)
        println("  ${if (ok) "✅" else "❌"} $name: $detail (${ms(s.millis)} мс)")
    }

    private fun report() {
        println("\n── жизненный цикл ──────────────────────────────")
        steps.forEach { println("  ${if (it.ok) "✅" else "❌"} ${it.name}") }
        val failed = steps.count { !it.ok }
        println("\n  шагов ${steps.size}, неуспешных $failed")

        println("\n── результаты плагина (наблюдение) ─────────────")
        val results = last?.results.orEmpty()
        if (results.isEmpty()) println("  (нет)")
        results.forEach { (name, value) ->
            val caption = resultCaption(name)?.let { "  — $it" } ?: ""
            println("  $name → $value$caption")
        }

        println("\n── граница лаборатории ─────────────────────────")
        labBoundaryLines(lab).forEach { println("  • $it") }

        println("\n── границы среза ───────────────────────────────")
        println("  • управляется ТОЛЬКО собственный экземпляр: обнаружения чужого")
        println("    runtime нет и не заводится (ADR-057 §6);")
        println("  • Grant/Revoke происходят ВНУТРИ этого экземпляра и наружу не")
        println("    выведены — управляющий канал их не предлагает (§7);")
        println("  • C ABI не участвует: библиотека Photon здесь не загружается,")
        println("    photon-core о плагинах по-прежнему не знает (ADR-030);")
        println("  • это не production management API и не удалённое управление.")
    }
}
