package dev.photon.sdk

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

/*
 * Разбор конверта ответа — §9.2.
 *
 * Конверт берётся из канонической схемы (PhotonResponse в photon.proto:
 * ok/error/data), а не выдумывается. Главное свойство, которое здесь
 * держится: испорченный ответ НЕ засчитывается успехом.
 */
class ResponseParserTest {

    @Test
    fun `ok true даёт Ok`() {
        val r = ResponseParser.parse("""{"ok":true,"data":{"id":"p1"}}""")
        val ok = assertIs<PhotonResponse.Ok>(r)
        assertEquals("""{"id":"p1"}""", ok.data)
    }

    @Test
    fun `ok true без data даёт null, а не пустую строку`() {
        assertNull((ResponseParser.parse("""{"ok":true}""") as PhotonResponse.Ok).data)
    }

    @Test
    fun `ok false даёт Rejected с текстом error как есть`() {
        val r = ResponseParser.parse("""{"ok":false,"error":"team_id is required"}""")
        val rejected = assertIs<PhotonResponse.Rejected>(r)
        assertEquals("team_id is required", rejected.error)
    }

    @Test
    fun `ok false без error даёт null — провод наличия поля не гарантирует`() {
        assertNull((ResponseParser.parse("""{"ok":false}""") as PhotonResponse.Rejected).error)
    }

    @Test
    fun `ok false с data сохраняет и data`() {
        val r = ResponseParser.parse("""{"ok":false,"error":"нет","data":{"missing":["a"]}}""")
        val rejected = assertIs<PhotonResponse.Rejected>(r)
        assertEquals("""{"missing":["a"]}""", rejected.data)
    }

    @Test
    fun `текст error не классифицируется`() {
        // Мутация: сопоставить текст коду ошибки (ADR-033 это запрещает,
        // и в ADR-038 записан прецедент двух реальных дефектов).
        for (text in listOf("permission denied", "not found", "timeout", "")) {
            val r = ResponseParser.parse("""{"ok":false,"error":"$text"}""")
            assertEquals(text, (r as PhotonResponse.Rejected).error)
        }
    }

    // ── Unrecognized: испорченное не становится успехом ──────────────────

    @Test
    fun `конверт без ok даёт Unrecognized, а не Ok`() {
        // Ровно то, что сегодня делает инструмент: classifyResponse ищет
        // только "ok":false, поэтому {"data":…} у него — успех.
        assertIs<PhotonResponse.Unrecognized>(ResponseParser.parse("""{"data":{"id":"p1"}}"""))
    }

    @Test
    fun `нечитаемый JSON даёт Unrecognized`() {
        for (bad in listOf("", "не json", """{"ok":tru""", "{")) {
            assertIs<PhotonResponse.Unrecognized>(ResponseParser.parse(bad), "ввод: $bad")
        }
    }

    @Test
    fun `не объект даёт Unrecognized`() {
        for (bad in listOf("[]", """[{"ok":true}]""", "null", "17", "\"строка\"", "true")) {
            assertIs<PhotonResponse.Unrecognized>(ResponseParser.parse(bad), "ввод: $bad")
        }
    }

    @Test
    fun `небулево ok даёт Unrecognized`() {
        // Строка "true" вердиктом не является: booleanOrNull у kotlinx
        // принял бы её, поэтому isString проверяется отдельно.
        for (bad in listOf(
            """{"ok":"true"}""", """{"ok":"false"}""", """{"ok":1}""",
            """{"ok":0}""", """{"ok":null}""", """{"ok":{}}""",
        )) {
            assertIs<PhotonResponse.Unrecognized>(ResponseParser.parse(bad), "ввод: $bad")
        }
    }

    @Test
    fun `raw сохраняется у всех подтипов байт в байт`() {
        for (raw in listOf(
            """{ "ok" : true , "data" : { "a" : 1 } }""",
            """{ "ok" : false , "error" : "нет" }""",
            """{"нет-ok":1}""",
            "совсем не json",
        )) {
            assertEquals(raw, ResponseParser.parse(raw).raw, "raw изменён для: $raw")
        }
    }

    @Test
    fun `data вырезается, а не пересобирается`() {
        // Мутация: вернуть JsonElement.toString(). Измерено, что он
        // нормализует форматирование.
        val data = """{"a": 1 ,  "b" : [1,  2] , "s":"два  пробела"}"""
        val r = ResponseParser.parse("""{"ok":true,"data":$data}""")
        assertEquals(data, (r as PhotonResponse.Ok).data)
    }

    @Test
    fun `ключ data внутри вложенного объекта не принимается за поле верхнего уровня`() {
        val r = ResponseParser.parse("""{"ok":true,"other":{"data":"чужое"},"data":"своё"}""")
        assertEquals("\"своё\"", (r as PhotonResponse.Ok).data)
    }

    @Test
    fun `в публичном результате нет измерения времени`() {
        // §9.2 п. 7: Duration/nanos в результат не входят. CallResult.nanos
        // инструмента существует ради Performance Lab, а не ради приложений;
        // в публичном типе он стал бы обязательством навсегда.
        //
        // Мутация: добавить `nanos` или `duration` в любой из подтипов.
        val types = listOf(
            PhotonResponse.Ok::class.java,
            PhotonResponse.Rejected::class.java,
            PhotonResponse.Unrecognized::class.java,
        )
        for (t in types) {
            val timing = t.declaredFields.filterNot { it.isSynthetic }.filter { f ->
                f.name.lowercase().let { n -> "nano" in n || "duration" in n || "millis" in n || "elapsed" in n } ||
                    f.type == java.time.Duration::class.java
            }
            assertEquals(
                emptyList(), timing.map { it.name },
                "в ${t.simpleName} появилось измерение времени"
            )
        }
    }

    @Test
    fun `порядок полей не важен`() {
        val r = ResponseParser.parse("""{"data":{"a":1},"error":null,"ok":false}""")
        assertIs<PhotonResponse.Rejected>(r)
    }
}
