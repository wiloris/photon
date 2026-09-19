package dev.photon.sdk

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/*
 * Границы модуля — проверяются СТРУКТУРНО, а не поведением.
 *
 * Тот же приём, что у ADR-031 в photon-plugin-sdk: правило, которое можно
 * нарушить одной строкой импорта, должно ломать прогон, а не ревью.
 */
class ModuleBoundaryTest {

    private val sources: List<File> =
        File("src/main/kotlin/dev/photon/sdk").listFiles()!!.filter { it.extension == "kt" }

    private fun bodies(): List<Pair<String, String>> =
        sources.map { it.name to it.readText() }

    @Test
    fun `исходники SDK на месте`() {
        // Без этого остальные проверки прошли бы на пустом списке.
        assertTrue(sources.size >= 8, "найдено файлов: ${sources.size}")
    }

    @Test
    fun `SDK не импортирует Developer Tools`() {
        for ((name, src) in bodies()) {
            assertTrue(
                "dev.photon.devtools" !in src,
                "$name импортирует Developer Tools: зависимость направлена в обратную сторону"
            )
        }
    }

    @Test
    fun `в SDK нет копии Outcome-CallResult`() {
        // ADR-062 п. 6 переносит классификацию исхода, но §9 решил, что
        // форма у неё другая: две оси — исключение и PhotonResponse.
        for ((name, src) in bodies()) {
            for (forbidden in listOf("class Outcome", "enum class Outcome", "class CallResult", "VERDICT_FALSE")) {
                assertTrue(forbidden !in src, "$name содержит $forbidden — модель инструмента скопирована в API")
            }
        }
    }

    @Test
    fun `в SDK нет доменного перечисления действий`() {
        // Каталог живёт в photon.proto и читается машинно; копия здесь была
        // бы третьей (Inspector.kt:132-150 про вторую).
        for ((name, src) in bodies()) {
            for (forbidden in listOf("enum class Action", "enum class PhotonAction", "GET_ME", "CREATE_POST")) {
                assertTrue(forbidden !in src, "$name содержит $forbidden — доменный словарь протокола")
            }
        }
    }

    @Test
    fun `SDK не типизирует data ответа и события`() {
        // data объявлена String? в обоих местах; любой другой тип означал бы
        // разбор доменной формы Mattermost (ADR-062 п. 5).
        val event = File("src/main/kotlin/dev/photon/sdk/PhotonEvent.kt").readText()
        val response = File("src/main/kotlin/dev/photon/sdk/PhotonResponse.kt").readText()
        assertTrue("val data: String?" in event, "PhotonEvent.Message.data перестала быть сырым текстом")
        assertTrue("val data: String?" in response, "PhotonResponse.data перестала быть сырым текстом")
    }

    @Test
    fun `SDK не дублирует значения по умолчанию из ядра`() {
        // Умолчания живут в withDefaults ядра: 15 с / 30 с / 15 с / 500 мс.
        val config = File("src/main/kotlin/dev/photon/sdk/PhotonConfig.kt").readText()
        val code = config.lineSequence().filterNot { it.trimStart().startsWith("*") }
            .filterNot { it.trimStart().startsWith("//") }.joinToString("\n")
        for (default in listOf("15000", "30000", "500", "ofSeconds(15)", "ofSeconds(30)")) {
            assertTrue(default !in code, "PhotonConfig подставляет умолчание ядра: $default")
        }
    }

    @Test
    fun `SDK не заводит второго механизма нативных колбэков`() {
        // Единственное обращение к мосту — BridgeSession; JNI, native и
        // загрузка библиотек остаются в dev.photon.core.
        for ((name, src) in bodies()) {
            for (forbidden in listOf("external fun", "System.loadLibrary", "System.load(", "@JvmStatic external")) {
                assertTrue(forbidden !in src, "$name объявляет собственную нативную поверхность: $forbidden")
            }
        }
        // Учитывается только КОД: KDoc ссылается на мост во многих местах и
        // границы не нарушает.
        val touchingBridge = bodies().filter { (_, src) ->
            src.lineSequence()
                .map { it.trim() }
                .filterNot { it.startsWith("*") || it.startsWith("//") || it.startsWith("/*") }
                .any { "dev.photon.core" in it }
        }.map { it.first }.sorted()

        assertEquals(
            listOf("NativeSession.kt", "PhotonClient.kt"),
            touchingBridge,
            "мост используется не только швом и фабрикой: одна реализация, а не две (ADR-062 п. 2)"
        )
    }

    @Test
    fun `внутренние типы не попали в публичную поверхность`() {
        for (internalType in listOf("EventDispatcher", "EventParser", "ResponseParser", "NativeSession", "BridgeSession")) {
            val src = sources.first { it.nameWithoutExtension == internalType || it.readText().contains("class $internalType") }
                .readText()
            assertTrue(
                Regex("(?m)^(public )?(object|class|interface) $internalType").containsMatchIn(src) ||
                    "internal object $internalType" in src || "internal class $internalType" in src ||
                    "internal interface $internalType" in src,
                "$internalType найден, но объявление не распознано"
            )
            assertTrue(
                "public class $internalType" !in src && "public object $internalType" !in src &&
                    "public interface $internalType" !in src,
                "$internalType объявлен публичным"
            )
        }
    }
}
