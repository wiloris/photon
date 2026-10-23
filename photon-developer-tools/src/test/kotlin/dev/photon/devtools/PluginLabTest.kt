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

    // ── B4b: каталог, результаты, граница лаборатории (plugin-lab.md §11.2) ──

    /** K1 — каталог, результаты и граница лаборатории разбираются из канала. */
    @Test
    fun `K1 разбор каталога, результатов и границы лаборатории`() {
        val catalog = parseControlReply(
            """{"id":1,"ok":true,"cmd":"scenarios","scenarios":[""" +
                """{"id":"s1","fixture":"f1","plugin_id":"p.one","granted":["storage.read"],"namespace":"lab.f1","description":"первый"},""" +
                """{"id":"s2","fixture":"f2","plugin_id":"p.two","granted":["storage.read","identity.read"],"namespace":"lab.f2","description":"второй"}]}""")
        assertEquals(listOf("s1", "s2"), catalog.scenarios.map { it.id })
        assertEquals(listOf("storage.read", "identity.read"), catalog.scenarios[1].granted)
        assertEquals("lab.f2", catalog.scenarios[1].namespace)
        assertEquals(null, catalog.pluginId, "plugin_id строки каталога не должен стать plugin_id ответа")

        val diag = parseControlReply(
            """{"id":2,"ok":true,"cmd":"diagnose","scenario":"s2","plugin_id":"p.two","plugin_state":"running",""" +
                """"results":{"pub":"QUJD","sig":"len=64 b64=QUJD…","verify_raw":"false","done":"sign"},""" +
                """"lab":{"key_material":"ephemeral","storage":"temporary","storage_dir":"/tmp/photon-plugin-lab-1","production_composition":false}}""")
        assertEquals("s2", diag.scenario)
        assertEquals(mapOf("pub" to "QUJD", "sig" to "len=64 b64=QUJD…", "verify_raw" to "false", "done" to "sign"), diag.results)
        assertEquals(LabInfo("ephemeral", "temporary", "/tmp/photon-plugin-lab-1", false), diag.lab)
    }

    /** K2 — все четыре строки границы лаборатории в отчёте (§9.2). */
    @Test
    fun `K2 отчёт печатает все четыре строки границы лаборатории`() {
        val lines = labBoundaryLines(LabInfo("ephemeral", "temporary", "/tmp/photon-plugin-lab-7", false))
        assertEquals(4, lines.size)
        assertTrue(lines[0].contains("одноразовые") && lines[0].contains("на диск не пишутся"), lines[0])
        assertTrue(lines[1].contains("/tmp/photon-plugin-lab-7") && lines[1].contains("kill -9 может остаться"), lines[1])
        assertTrue(lines[2].contains("не продакшн-композиция") && lines[2].contains("cmd/photon-core-server"), lines[2])
        assertTrue(lines[3].contains("B4a") && lines[3].contains("tests/lifecycle/sdkscenario_test.go"), lines[3])
        val src = java.io.File(System.getProperty("photon.devtools.src"), "PluginLab.kt").readText()
        assertTrue(src.contains("labBoundaryLines(lab).forEach"), "отчёт обязан печатать границу, а не только уметь её построить")
    }

    /** K3 — своей копии каталога нет: ни id сценариев, ни идентичностей. */
    @Test
    fun `K3 в Plugin Lab нет литералов сценариев и идентичностей`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "PluginLab.kt").readText()
        listOf("com.engram.test", "\"hello\"", "\"sdk-storage-roundtrip\"", "\"sdk-sign\"", "\"sdk-sign-forbidden\"")
            .forEach { assertTrue(!src.contains(it), "$it в PluginLab.kt — вторая копия каталога runtime") }
        assertTrue(src.contains("call(\"scenarios\")"), "каталог обязан запрашиваться у runtime")
    }

    /** K4 — лаборатория не показывает verify_domain и не утверждает доказательств. */
    @Test
    fun `K4 в Plugin Lab нет verify_domain и утверждений о доказательстве`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "PluginLab.kt").readText()
        listOf("verify_domain", "ADR-037 доказан", "изоляция доказана", "приватный ключ не выходит")
            .forEach { assertTrue(!src.contains(it), "«$it» — утверждение, которое лаборатория делать не вправе") }
        assertEquals("наблюдение, не доказательство; см. TestSDKScenario_SignIsDomainSeparated", resultCaption("verify_raw"))
    }

    @Test
    fun `кадр load экранирует id и без него не шлёт scenario`() {
        assertEquals("""{"id":3,"cmd":"load"}""", loadFrame(3, null))
        assertEquals("""{"id":4,"cmd":"load","scenario":"a\"b"}""", loadFrame(4, "a\"b"))
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
