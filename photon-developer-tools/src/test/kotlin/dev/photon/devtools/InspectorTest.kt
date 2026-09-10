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
 */
class InspectorTest {

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

        // 60 значений, из них одно UNSPECIFIED — 59 реальных кодов.
        assertEquals(60, action.valueCount)
        assertEquals(59, action.valueList.count { it.number != 0 })
    }

    @Test
    fun `enum Resource содержит ожидаемые значения`() {
        val r = inspector.enum("Resource")
        assertNotNull(r)
        val byName = r.valueList.associate { it.name to it.number }
        assertEquals(8, r.valueCount)
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
        assertEquals(75, msgs.size)

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
