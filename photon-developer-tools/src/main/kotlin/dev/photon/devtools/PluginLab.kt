package dev.photon.devtools

import java.io.BufferedReader
import java.io.File
import java.util.Locale
import java.util.concurrent.TimeUnit

/*
 * Plugin Lab — первый вертикальный срез по ADR-057.
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
 * Чего здесь нет по построению: C ABI не участвует вовсе (`Photon` не
 * загружается), Photon Core о плагинах по-прежнему не знает (ADR-030),
 * управление чужим или production runtime невозможно.
 */

private fun ms(v: Long): String = String.format(Locale.ROOT, "%.1f", v / 1_000_000.0)

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
)

private fun field(json: String, name: String): String? =
    Regex("\"$name\"\\s*:\\s*\"([^\"]*)\"").find(json)?.groupValues?.get(1)

private fun boolField(json: String, name: String): Boolean =
    Regex("\"$name\"\\s*:\\s*(true|false)").find(json)?.groupValues?.get(1) == "true"

private fun stringArray(json: String, name: String): List<String> {
    val block = Regex("\"$name\"\\s*:\\s*\\[([^\\]]*)\\]").find(json)?.groupValues?.get(1) ?: return emptyList()
    return Regex("\"([^\"]+)\"").findAll(block).map { it.groupValues[1] }.toList()
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
    pluginId = field(line, "plugin_id"),
    pluginState = field(line, "plugin_state"),
    granted = stringArray(line, "granted"),
    // Операции лежат объектами; имена достаточно вытащить по ключу "op".
    operations = Regex("\"op\"\\s*:\\s*\"([^\"]+)\"").findAll(line).map { it.groupValues[1] }.toList(),
    isolationPlatform = field(line, "platform"),
)

/**
 * Шаг жизненного цикла — то, что лаборатория обязана показать по пунктам, а не
 * одной строкой «работает».
 */
data class LifecycleStep(val name: String, val ok: Boolean, val detail: String, val millis: Long)

class PluginLab(private val runtimeBinary: File, private val pluginBinary: File) {

    private var process: Process? = null
    private var reader: BufferedReader? = null
    private var seq = 0
    private val steps = mutableListOf<LifecycleStep>()

    fun run() {
        println("Plugin Lab — собственный экземпляр runtime (ADR-057)")
        println("runtime: ${runtimeBinary.name}, фикстура: ${pluginBinary.name}\n")

        if (!runtimeBinary.canExecute() || !pluginBinary.canExecute()) {
            System.err.println("не собраны native-артефакты: ${runtimeBinary.path}, ${pluginBinary.path}")
            System.err.println("собрать: gradle pluginLabNative")
            return
        }

        try {
            start()
            load()
            diagnose()
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
        val p = ProcessBuilder(runtimeBinary.absolutePath, "-plugin", pluginBinary.absolutePath)
            .redirectErrorStream(false)
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start()
        process = p
        reader = p.inputStream.bufferedReader()

        // Если JVM уйдёт штатно — закрыть трубу немедленно, не дожидаясь GC.
        Runtime.getRuntime().addShutdownHook(Thread { if (p.isAlive) p.destroy() })

        val ready = parseControlReply(reader!!.readLine() ?: "")
        step("start → runtime ready", ready.ok && ready.runtimeState == "ready",
            "состояние: ${ready.runtimeState}", t)
        println("  ⚙ runtime поднят, pid ${p.pid()}")
    }

    private fun load() {
        val t = System.nanoTime()
        val r = call("load")
        step("plugin loaded", r.ok && r.pluginState == "running",
            if (r.ok) "плагин ${r.pluginId}: ${r.pluginState}" else "отказ: ${r.error}", t)
    }

    /**
     * Диагностическая операция — read-only и над собственным экземпляром.
     *
     * Показывает не «плагин жив», а путь авторизации: операция, дошедшая до
     * исполнителя, прошла Authorizer, потому что проверка прав стоит в сессии
     * ДО вызова исполнителя. Пустой список означал бы, что плагин ничего не
     * успел или не смог.
     */
    private fun diagnose() {
        val t = System.nanoTime()
        var r = call("diagnose")
        val deadline = System.currentTimeMillis() + 10_000
        while (r.operations.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100)
            r = call("diagnose")
        }
        step("diagnostic operation", r.ok && r.operations.isNotEmpty(),
            "состояние ${r.pluginState}, права ${r.granted}, операции ${r.operations}", t)
        println("  🔬 изоляция: ${r.isolationPlatform}")
        println("  🔬 выданные права: ${r.granted}")
        println("  🔬 операции плагина (дошли до исполнителя ⇒ авторизованы): ${r.operations}")
    }

    private fun shutdown() {
        val t = System.nanoTime()
        val r = call("shutdown")
        val exited = process?.waitFor(10, TimeUnit.SECONDS) ?: false
        step("graceful shutdown", r.ok && exited,
            "состояние ${r.runtimeState}, процесс завершился: $exited", t)
    }

    // ── канал ────────────────────────────────────────────────────────────────

    private fun call(cmd: String): ControlReply {
        val p = process ?: error("runtime не запущен")
        seq++
        p.outputStream.write("""{"id":$seq,"cmd":"$cmd"}""".toByteArray())
        p.outputStream.write('\n'.code)
        p.outputStream.flush()
        val line = reader?.readLine() ?: return ControlReply(
            "", false, cmd, "канал закрыт", null, null, null, emptyList(), emptyList(), null)
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
        println()
        println("── границы среза ───────────────────────────────")
        println("  • управляется ТОЛЬКО собственный экземпляр: обнаружения чужого")
        println("    runtime нет и не заводится (ADR-057 §6);")
        println("  • Grant/Revoke происходят ВНУТРИ этого экземпляра и наружу не")
        println("    выведены — управляющий канал их не предлагает (§7);")
        println("  • C ABI не участвует: библиотека Photon здесь не загружается,")
        println("    photon-core о плагинах по-прежнему не знает (ADR-030);")
        println("  • это не production management API и не удалённое управление.")
    }
}
