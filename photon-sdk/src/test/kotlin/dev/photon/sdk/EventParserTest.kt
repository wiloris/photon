package dev.photon.sdk

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/*
 * Разбор конверта события — §1, §2 спецификации.
 *
 * Конверты здесь не выдуманы: это ровно то, что собирает ядро
 * (photon-core/services/dispatcher/events.go:36-90). Тест, написанный на
 * воображаемой форме, проверял бы согласие SDK с самим собой.
 *
 * Каждое утверждение проверялось мутацией — сломать правило и убедиться, что
 * тест падает; перечень мутаций — sdk-api.md §8.
 */
class EventParserTest {

    // ── Три типа, которые ядро действительно эмитирует ───────────────────

    @Test
    fun `CONNECTED разбирается и не несёт полей`() {
        val raw = """{"type":"CONNECTED"}"""
        val e = EventParser.parse(raw)
        assertIs<PhotonEvent.Connected>(e)
        assertEquals(raw, e.raw)
    }

    @Test
    fun `NETWORK_CHANGED несёт state`() {
        val raw = """{"type":"NETWORK_CHANGED","state":"disconnected"}"""
        val e = EventParser.parse(raw)
        assertIs<PhotonEvent.NetworkChanged>(e)
        assertEquals("disconnected", e.state)
        assertEquals(raw, e.raw)
    }

    @Test
    fun `MESSAGE несёт event, user_id и data`() {
        val raw = """{"type":"MESSAGE","event":"posted","user_id":"u1","data":{"id":"p1"}}"""
        val e = EventParser.parse(raw)
        assertIs<PhotonEvent.Message>(e)
        assertEquals("posted", e.event)
        assertEquals("u1", e.userId)
        assertEquals("""{"id":"p1"}""", e.data)
    }

    @Test
    fun `user_id отсутствует чаще всего и это не ошибка`() {
        // omitempty на сервере: поле заполняет один хук из восьми
        // (bridge-session.md §2.3).
        val e = EventParser.parse("""{"type":"MESSAGE","event":"posted","data":{"id":"p1"}}""")
        assertIs<PhotonEvent.Message>(e)
        assertNull(e.userId)
    }

    @Test
    fun `MESSAGE без data даёт null, а не пустую строку`() {
        val e = EventParser.parse("""{"type":"MESSAGE","event":"posted"}""")
        assertIs<PhotonEvent.Message>(e)
        assertNull(e.data)
    }

    // ── Неизвестное доставляется, а не отбрасывается ─────────────────────

    @Test
    fun `неизвестный тип конверта даёт Unknown с сохранённым типом`() {
        // Зарезервированы в photon_core.h:346-350, ядро их не эмитирует —
        // но обязано быть определено, что случится, когда начнёт.
        for (type in listOf("PLUGIN_EVENT", "STORAGE_CHANGED", "ЧЕГО_ЕЩЁ_НЕТ")) {
            val raw = """{"type":"$type","что-то":1}"""
            val e = EventParser.parse(raw)
            assertIs<PhotonEvent.Unknown>(e, "тип $type")
            assertEquals(type, e.type)
            assertEquals(raw, e.raw)
        }
    }

    @Test
    fun `незнакомый MESSAGE-event остаётся Message, а не становится Unknown`() {
        // Словарь event принадлежит Bridge; SDK его не знает и знать не
        // обязан. Смешение этих двух уровней — самая вероятная ошибка.
        val e = EventParser.parse("""{"type":"MESSAGE","event":"такого_хука_ещё_нет"}""")
        assertIs<PhotonEvent.Message>(e)
        assertEquals("такого_хука_ещё_нет", e.event)
    }

    // ── Неразбираемое: Unknown(null), без исключения наружу ──────────────

    @Test
    fun `неразбираемый конверт даёт Unknown с типом null и не бросает`() {
        for (bad in listOf("", "не json", """{"type":"MESSAGE""", "[]", "null", "17")) {
            val e = EventParser.parse(bad)
            assertIs<PhotonEvent.Unknown>(e, "ввод: $bad")
            assertNull(e.type, "ввод: $bad")
            assertEquals(bad, e.raw)
        }
    }

    @Test
    fun `нестроковый тип не считается типом события`() {
        val e = EventParser.parse("""{"type":17}""")
        assertIs<PhotonEvent.Unknown>(e)
        assertNull(e.type)
    }

    @Test
    fun `конверт нужного типа без обязательного поля даёт Unknown с СОХРАНЁННЫМ типом`() {
        // Ядро так не шлёт; правило существует, чтобы форма отказа была
        // определена, а не выяснялась на живом сервере.
        //
        // Различие трёх исходов держится именно здесь:
        //   Unknown(null)      — разобрать до типа не удалось;
        //   Unknown("MESSAGE") — тип известен, форма конверта не совпала;
        //   Message(...)       — обязательные поля на месте.
        //
        // Пустая строка вместо отсутствующего event запрещена отдельно: это
        // значение, которого ядро не присылает, и оно спрятало бы порченый
        // конверт за видимостью нормального.
        for ((raw, type) in listOf(
            """{"type":"NETWORK_CHANGED"}""" to "NETWORK_CHANGED",
            """{"type":"NETWORK_CHANGED","state":17}""" to "NETWORK_CHANGED",
            """{"type":"MESSAGE","data":{}}""" to "MESSAGE",
            """{"type":"MESSAGE","event":null}""" to "MESSAGE",
        )) {
            val e = EventParser.parse(raw)
            assertIs<PhotonEvent.Unknown>(e, "конверт: $raw")
            assertEquals(type, e.type, "тип потерян для: $raw")
            assertEquals(raw, e.raw, "raw изменён для: $raw")
        }
    }

    // ── raw и data: дословность ──────────────────────────────────────────

    @Test
    fun `raw у каждого подтипа равен исходной строке байт в байт`() {
        val raws = listOf(
            """{"type":"CONNECTED"}""",
            """{ "type" : "NETWORK_CHANGED" , "state" : "disconnected" }""",
            """{"type":"MESSAGE","event":"posted","data":{"a":1}}""",
            """{"type":"НЕТ_ТАКОГО"}""",
            "не json вовсе",
        )
        for (raw in raws) assertEquals(raw, EventParser.parse(raw).raw, "raw изменён для: $raw")
    }

    @Test
    fun `data равен исходному фрагменту при произвольном форматировании`() {
        // Мутация, которую этот тест обязан ловить: вернуть
        // JsonElement.toString() вместо вырезания подстроки. Измерено —
        // toString нормализует пробелы: {"a": 1 ,  "b" : [1,  2]} даёт
        // {"a":1,"b":[1,2]}, и на плотном JSON ядра подмена незаметна.
        val data = """{"a": 1 ,  "b" : [1,  2] , "s":"два  пробела"}"""
        val e = EventParser.parse("""{"type":"MESSAGE","event":"posted","data":$data}""")
        assertIs<PhotonEvent.Message>(e)
        assertEquals(data, e.data)
    }

    @Test
    fun `data сохраняет точность чисел`() {
        // Ядро вставляет data сырыми байтами именно поэтому
        // (events.go:58-60): пересборка через double потеряла бы длинный
        // идентификатор или экспоненту.
        val data = """{"create_at":1758181234567,"big":123456789012345678901234567890,"e":1e3}"""
        val e = EventParser.parse("""{"type":"MESSAGE","event":"posted","data":$data}""")
        assertEquals(data, (e as PhotonEvent.Message).data)
    }

    @Test
    fun `data с вложенностью, массивами и скобками внутри строк`() {
        val data = """{"nested":{"a":[1,2,"}"]},"s":"с \" кавычкой и \\ слешем","arr":[{"x":"]"}]}"""
        val e = EventParser.parse("""{"type":"MESSAGE","event":"posted","data":$data,"user_id":"u"}""")
        assertIs<PhotonEvent.Message>(e)
        assertEquals(data, e.data)
        assertEquals("u", e.userId)
    }

    @Test
    fun `ключ data внутри вложенного объекта не принимается за поле верхнего уровня`() {
        // Поле ищется строго на верхнем уровне: иначе "data" внутри чужого
        // объекта увела бы разбор в произвольное место конверта.
        val raw = """{"type":"MESSAGE","event":"posted","other":{"data":"чужое"},"data":"своё"}"""
        val e = EventParser.parse(raw)
        assertIs<PhotonEvent.Message>(e)
        assertEquals("\"своё\"", e.data)
    }

    @Test
    fun `data нестрокового и непредметного типа тоже отдаётся дословно`() {
        for (value in listOf("null", "17", "true", "\"строка\"", "[1,2]")) {
            val e = EventParser.parse("""{"type":"MESSAGE","event":"posted","data":$value}""")
            assertEquals(value, (e as PhotonEvent.Message).data, "значение: $value")
        }
    }

    @Test
    fun `порядок полей в конверте не важен`() {
        val e = EventParser.parse("""{"data":{"a":1},"user_id":"u","event":"posted","type":"MESSAGE"}""")
        assertIs<PhotonEvent.Message>(e)
        assertEquals("""{"a":1}""", e.data)
        assertTrue(e.event == "posted" && e.userId == "u")
    }
}
