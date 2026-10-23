package dev.photon.devtools

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/*
 * Тесты Protobuf Inspector.
 *
 * Проверяется СОДЕРЖИМОЕ дескриптора, а не факт его существования: тест
 * «файл есть» прошёл бы и на пустышке, и на дескрипторе чужой схемы.
 *
 * Ожидаемые значения берутся из самого photon.proto простым разбором текста,
 * независимым от protoc: дескриптор обязан совпасть с исходником схемы ТОЧНО —
 * тем же набором имён и номеров. Прежде здесь стояли зашитые числа (64 действия,
 * 8 ресурсов, 75 сообщений), и тест краснел при каждом новом действии, хотя
 * Inspector показывал каталог верно; такая проверка ловила только «стало больше»,
 * но не подмену или перенумерацию. Опорные точки диапазонов остаются — они не
 * дают пройти совпадению двух одинаково испорченных источников.
 */
class InspectorTest {

    /** photon.proto как текст — источник ожидаемых значений. */
    private val protoText: String by lazy {
        val path = System.getProperty("photon.proto")
        assertNotNull(path, "тест запускается через Gradle: -Dphoton.proto задаёт задача")
        val f = File(path)
        assertTrue(f.isFile, "photon.proto не найден: $path")
        f.readText()
    }

    /** Значения enum [name] из текста схемы: имя → номер. */
    private fun protoEnum(name: String): Map<String, Int> {
        val body = Regex("""(?ms)^enum\s+$name\s*\{(.*?)^\}""").find(protoText)?.groupValues?.get(1)
        assertNotNull(body, "enum $name не найден в photon.proto")
        val values = Regex("""(?m)^\s*([A-Z][A-Z0-9_]*)\s*=\s*(\d+)\s*;""").findAll(body)
            .map { it.groupValues[1] to it.groupValues[2].toInt() }.toList()
        assertEquals(values.size, values.map { it.first }.toSet().size, "повтор имени в enum $name")
        return values.toMap()
    }

    /** Имена сообщений верхнего уровня из текста схемы. */
    private fun protoMessages(): Set<String> =
        Regex("""(?m)^message\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{""").findAll(protoText).map { it.groupValues[1] }.toSet()

    private val inspector: Inspector by lazy {
        val path = System.getProperty("photon.descriptor")
        assertNotNull(path, "тест запускается через Gradle: -Dphoton.descriptor задаёт задача")
        val f = File(path)
        assertTrue(f.isFile, "дескриптор не сгенерирован: $path")
        Inspector(f)
    }

    @Test
    fun `дескриптор происходит из photon proto`() {
        assertEquals("photon.proto", inspector.sourceName)
        assertEquals("photon", inspector.packageName)
    }

    @Test
    fun `enum Action содержит ожидаемые значения и номера`() {
        val action = inspector.enum("Action")
        assertNotNull(action)
        val byName = action.valueList.associate { it.name to it.number }

        // Опорные точки из каждого диапазона ресурсов — если каталог подменён
        // или дескриптор чужой, совпасть они не могут.
        assertEquals(0, byName["ACTION_UNSPECIFIED"])
        assertEquals(100, byName["ACTION_CREATE_POST"])
        assertEquals(101, byName["ACTION_GET_FEED"])
        assertEquals(200, byName["ACTION_GET_ME"])
        assertEquals(308, byName["ACTION_GET_OR_CREATE_DIRECT_CHANNEL"])
        assertEquals(700, byName["ACTION_MEDIA_UPLOAD"])
        assertEquals(701, byName["ACTION_MEDIA_DOWNLOAD"])
        assertEquals(702, byName["ACTION_MEDIA_GET_INFO"])

        // Локальное действие в том же каноническом каталоге (ADR-064): коды
        // 800-899 исполняются хранилищем инстанса и на провод не выходят, но
        // источник кодов остаётся один, и Inspector обязан их показывать.
        assertEquals(800, byName["ACTION_LOCAL_FEED_PAGE"])

        // Каталог совпадает со схемой ТОЧНО: те же имена с теми же номерами, без
        // лишних и без пропущенных; UNSPECIFIED ровно один.
        assertEquals(protoEnum("Action"), byName)
        assertEquals(action.valueCount, byName.size, "в дескрипторе повторяются имена Action")
        assertEquals(1, action.valueList.count { it.number == 0 })
        assertEquals(action.valueCount, action.valueList.map { it.number }.toSet().size, "номера Action не уникальны")
    }

    @Test
    fun `enum Resource содержит ожидаемые значения`() {
        val r = inspector.enum("Resource")
        assertNotNull(r)
        val byName = r.valueList.associate { it.name to it.number }
        assertEquals(protoEnum("Resource"), byName)
        assertEquals(0, byName["RESOURCE_UNSPECIFIED"])
        assertEquals(1, byName["RESOURCE_POST"])
        assertEquals(2, byName["RESOURCE_USER"])
        assertEquals(3, byName["RESOURCE_CHANNEL"])
        assertEquals(7, byName["RESOURCE_MEDIA"])
    }

    @Test
    fun `комментарии схемы действительно доступны через source info`() {
        assertTrue(inspector.sourceInfoAvailable,
            "дескриптор собран без --include_source_info: комментариев в нём нет")

        val action = inspector.enum("Action")!!
        val withComment = action.valueList.indices
            .mapNotNull { inspector.enumValueComment("Action", it) }
        assertTrue(withComment.isNotEmpty(), "ни одного комментария в enum Action")
        assertTrue(withComment.any { it.contains("PostResource") },
            "заголовок диапазона PostResource не найден; получено: ${withComment.take(3)}")
    }

    @Test
    fun `сообщения и поля читаются с типами и номерами`() {
        val msgs = inspector.messages()
        assertEquals(protoMessages(), msgs.map { it.name }.toSet())
        assertEquals(msgs.size, msgs.map { it.name }.toSet().size)

        val req = msgs.firstOrNull { it.name == "PhotonRequest" }
        assertNotNull(req, "PhotonRequest отсутствует в дескрипторе")
        assertTrue(req.fieldCount > 0)
        assertTrue(req.fieldList.all { it.number > 0 && it.name.isNotEmpty() })
    }

    @Test
    fun `EventType отличается от словаря событий подписки C ABI`() {
        val e = inspector.enum("EventType")
        assertNotNull(e)
        val names = e.valueList.map { it.name }.toSet()
        // Ловушка, ради которой проверка и написана: показать EventType как
        // типы событий подписки значило бы ввести в заблуждение.
        assertTrue(names.contains("POST_CREATED"))
        listOf("CONNECTED", "MESSAGE", "NETWORK_CHANGED").forEach {
            assertTrue(it !in names, "$it не принадлежит enum EventType схемы")
        }
    }

    @Test
    fun `действия инструмента подтверждены каталогом`() {
        val report = inspector.verifyUsedActions()
        assertTrue(report.isNotEmpty())
        assertTrue(report.none { it.contains("В КАТАЛОГЕ НЕТ") },
            "код, который шлёт инструмент, отсутствует в каноническом каталоге: $report")
    }

    /**
     * Структурная проверка: у инструмента НЕТ своей копии каталога.
     *
     * Сканируются его исходники — тот же приём, которым ADR-031 проверяется
     * структурно, а не тестом поведения. Имена значений вида ACTION_* обязаны
     * встречаться только в дескрипторе, а не в коде.
     */
    @Test
    fun `в исходниках инструмента нет собственной копии каталога`() {
        val srcDir = System.getProperty("photon.devtools.src")
        assertNotNull(srcDir, "-Dphoton.devtools.src задаёт задача Gradle")
        val offenders = File(srcDir).walkTopDown()
            .filter { it.isFile && it.extension == "kt" }
            .mapNotNull { f ->
                val hits = Regex("\\bACTION_[A-Z_]+\\b").findAll(f.readText()).map { it.value }.toList()
                if (hits.isEmpty()) null else "${f.name}: ${hits.distinct()}"
            }.toList()
        assertTrue(offenders.isEmpty(),
            "имена из каталога вписаны в код инструмента — это вторая копия истины: $offenders")
    }
}
