package dev.photon.devtools

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/*
 * Тесты Plugin Lab.
 *
 * Проверяется разбор управляющего канала и структурные границы ADR-057.
 * Жизненный цикл и отсутствие сирот проверяются НАСТОЯЩИМИ ПРОЦЕССАМИ на
 * стороне Go (cmd/devtools-runtime/lifecycle_test.go): здесь для этого нет ни
 * процессов, ни плагина, и имитация дала бы зелёный отчёт ни о чём.
 */
class PluginLabTest {

    // ── разбор управляющего канала ───────────────────────────────────────────

    @Test
    fun `готовность runtime читается из первой строки`() {
        val r = parseControlReply("""{"id":0,"ok":true,"cmd":"ready","runtime_state":"ready"}""")
        assertTrue(r.ok)
        assertEquals("ready", r.cmd)
        assertEquals("ready", r.runtimeState)
    }

    @Test
    fun `состояние плагина читается из ответа на load`() {
        val r = parseControlReply(
            """{"id":1,"ok":true,"cmd":"load","plugin_id":"com.engram.test.hello","plugin_state":"running"}""")
        assertTrue(r.ok)
        assertEquals("com.engram.test.hello", r.pluginId)
        assertEquals("running", r.pluginState)
    }

    @Test
    fun `отказ канала не выдаётся за успех`() {
        val r = parseControlReply("""{"id":2,"ok":false,"cmd":"load","error":"plugin already loaded"}""")
        assertTrue(!r.ok)
        assertEquals("plugin already loaded", r.error)
    }

    /**
     * Права и операции — то, ради чего диагностика существует. Операция,
     * дошедшая до исполнителя, прошла Authorizer: проверка прав стоит в сессии
     * ДО вызова исполнителя. Разбор обязан доставать и то, и другое.
     */
    @Test
    fun `диагностика отдаёт права, операции и изоляцию`() {
        val line = """{"id":3,"ok":true,"cmd":"diagnose","plugin_state":"running",""" +
            """"granted":["storage.read"],""" +
            """"isolation":{"platform":"none","properties":{"separate_process":true}},""" +
            """"operations":[{"op":"storage.get","authorized":true},{"op":"log.write","authorized":true}]}"""
        val r = parseControlReply(line)
        assertEquals(listOf("storage.read"), r.granted)
        assertEquals(listOf("storage.get", "log.write"), r.operations)
        assertEquals("none", r.isolationPlatform)
    }

    /**
     * Пустая диагностика — не успех. Плагин, не выполнивший ни одной операции,
     * означает, что путь авторизации не пройден, и отчёт обязан это показать,
     * а не написать «работает».
     */
    @Test
    fun `диагностика без операций не считается пройденной`() {
        val r = parseControlReply("""{"id":4,"ok":true,"cmd":"diagnose","plugin_state":"running","operations":[]}""")
        assertTrue(r.ok)
        assertTrue(r.operations.isEmpty(), "пустой список обязан оставаться пустым, а не пропадать")
    }

    @Test
    fun `мусор в канале не разбирается в успех`() {
        val r = parseControlReply("не json вовсе")
        assertTrue(!r.ok)
        assertTrue(r.granted.isEmpty() && r.operations.isEmpty())
    }

    // ── структурные границы ADR-057 ──────────────────────────────────────────

    /**
     * Инструмент управляет ТОЛЬКО собственным экземпляром. Обнаружения чужого
     * runtime нет: ни сокета, ни порта, ни поиска по файловой системе.
     */
    @Test
    fun `Plugin Lab не ищет и не подключается к чужому runtime`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "PluginLab.kt").readText()
        listOf("Socket(", "ServerSocket", "InetAddress", "URL(", "HttpClient", "connect(")
            .forEach { assertTrue(!src.contains(it), "$it означал бы подключение к чужому runtime") }
        assertTrue(src.contains("ProcessBuilder"),
            "единственный способ получить runtime — породить его самому")
    }

    /**
     * Grant/Revoke наружу не выведены: управляющий канал их не предлагает, и
     * лаборатория их не шлёт. Появление такой команды здесь означало бы
     * внешний API управления политикой — прямо запрещённый ADR-057 §7.
     */
    @Test
    fun `управляющий канал не предлагает Grant и Revoke`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "PluginLab.kt").readText()
        listOf("\"grant\"", "\"revoke\"", "grantPermission", "revokePermission")
            .forEach { assertTrue(!src.contains(it), "$it — вынос политики во внешний API") }
    }

    /**
     * C ABI в этом пути не участвует вовсе. Загрузка нативной библиотеки
     * Photon здесь означала бы, что управление плагинами поехало в сторону
     * ядра — ровно то, что запрещает ADR-030.
     */
    @Test
    fun `Plugin Lab не трогает C ABI и Photon Core`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "PluginLab.kt").readText()
        listOf("Photon.loadLibrary", "Photon.create", "dev.photon.core.Photon", "sendAction")
            .forEach { assertTrue(!src.contains(it), "$it втягивает C ABI в управление плагинами") }
    }

    /**
     * Смерть родителя не должна оставлять сирот. Основная гарантия — на
     * стороне runtime (EOF на stdin), здесь вторая линия: закрыть процесс при
     * любом выходе, включая исключение.
     */
    @Test
    fun `лаборатория гасит дочерний процесс при любом выходе`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "PluginLab.kt").readText()
        assertTrue(src.contains("finally"), "уборка обязана стоять в finally, а не на счастливом пути")
        assertTrue(src.contains("addShutdownHook"), "штатный выход JVM обязан закрывать трубу немедленно")
        assertTrue(src.contains("destroy()"))
    }
}
