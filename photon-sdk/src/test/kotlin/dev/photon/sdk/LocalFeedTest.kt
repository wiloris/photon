package dev.photon.sdk

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

/*
 * Типизированная поверхность локальной ленты — ADR-066 §§3–8.
 *
 * Проверяется не «JSON собрался», а свойства, которые легко потерять
 * правкой: тот ли код действия уходит в ABI; переживает ли Long значение
 * выше 2^53; остаётся ли gaps_closed знаковым; отдаются ли все семь полей;
 * различимы ли «пропуска нет» и «результата нет»; и остаётся ли
 * противоречивый блок невыразимым.
 *
 * Стенд — FakeNativeSession: он записывает пару (код, payload), поэтому
 * ошибка в коде действия видна прямо, а не через форму ответа.
 */
class LocalFeedTest {

    private fun clientWith(response: String): Pair<PhotonClient, FakeNativeSession> {
        val native = FakeNativeSession()
        native.nextResponse = response
        val client = PhotonClient.createWith(PhotonConfig()) { native }
        return client to native
    }

    private fun okEnvelope(data: String) = """{"ok":true,"data":$data}"""

    private val rec = FeedRecord(id = "a", orderKey = 100, version = 100, body = """{"n":1}""")
    private val anchor = FeedPosition(orderKey = 500, id = "m")

    // ── Коды действий ───────────────────────────────────────────────────

    @Test
    fun `каждый примитив уходит своим кодом действия`() {
        // Мутация, которую тест обязан ловить: перепутанные 801/802/803.
        // Форма ответа её не поймала бы — стенд отвечает одинаково.
        val (c1, n1) = clientWith(okEnvelope("""{"stored":1}"""))
        c1.localFeedPut("ch", listOf(rec))
        assertEquals(801, n1.actions.single().first)

        val (c2, n2) = clientWith(okEnvelope(fullIngestData()))
        c2.localFeedIngest("ch", FeedBlock.Newest(listOf(rec), atEnd = false))
        assertEquals(802, n2.actions.single().first)

        val (c3, n3) = clientWith(okEnvelope("""{"found":false}"""))
        c3.localFeedGapAnchor("ch")
        assertEquals(803, n3.actions.single().first)
    }

    @Test
    fun `коды совпадают с каноническим перечнем photon proto`() {
        // Пиннинг: SDK не читает FileDescriptorSet, поэтому расхождение с
        // photon.proto ловится здесь, а не при первом живом вызове.
        assertEquals(801, LocalFeedCodec.ACTION_PUT)
        assertEquals(802, LocalFeedCodec.ACTION_INGEST)
        assertEquals(803, LocalFeedCodec.ACTION_GAP_ANCHOR)
    }

    // ── Сборка payload ──────────────────────────────────────────────────

    @Test
    fun `put собирает payload сам, вызывающий строк не пишет`() {
        val (c, n) = clientWith(okEnvelope("""{"stored":1}"""))
        c.localFeedPut("ch", listOf(rec))
        assertEquals(
            """{"channel_id":"ch","records":[{"id":"a","order_key":100,"version":100,"body":{"n":1}}]}""",
            n.actions.single().second
        )
    }

    @Test
    fun `направление берётся из типа блока, а якорь появляется только с ним`() {
        val cases = listOf(
            FeedBlock.Newest(listOf(rec), atEnd = true) to
                """"dir":"newest","at_end":true""",
            FeedBlock.After(anchor, listOf(rec), atEnd = false) to
                """"dir":"after","anchor":{"order_key":500,"id":"m"},"at_end":false""",
            FeedBlock.Before(anchor, listOf(rec), atEnd = false) to
                """"dir":"before","anchor":{"order_key":500,"id":"m"},"at_end":false""",
        )
        for ((block, expected) in cases) {
            val (c, n) = clientWith(okEnvelope(fullIngestData()))
            c.localFeedIngest("ch", block)
            val payload = n.actions.single().second
            assertTrue(payload.contains(expected), "в payload нет «$expected»: $payload")
        }
    }

    @Test
    fun `у newest якоря в payload нет вовсе`() {
        // Не «anchor:null», а отсутствие поля: ErrAnchorNotAllowed через SDK
        // недостижим по построению (ADR-066 §4).
        val (c, n) = clientWith(okEnvelope(fullIngestData()))
        c.localFeedIngest("ch", FeedBlock.Newest(listOf(rec), atEnd = false))
        assertFalse(n.actions.single().second.contains("anchor"), n.actions.single().second)
    }

    @Test
    fun `противоречивый блок невыразим — структурная проверка`() {
        // Поведенческого теста на «Newest + anchor» не существует и
        // существовать не может: такой объект не собирается. Проверяется то,
        // что действительно можно нарушить, — появление у Newest поля или
        // конструктора с якорем.
        val newest = FeedBlock.Newest::class.java

        assertTrue(
            newest.declaredFields.none { it.type == FeedPosition::class.java },
            "у Newest появилось поле-якорь: " + newest.declaredFields.joinToString { it.name }
        )
        assertTrue(
            newest.constructors.filterNot { it.isSynthetic }
                .none { ctor -> ctor.parameterTypes.any { it == FeedPosition::class.java } },
            "у Newest появился конструктор с якорем"
        )
        // И обратная сторона: у After/Before якорь ОБЯЗАТЕЛЕН, то есть не
        // nullable-поле, которое можно не задать.
        for (withAnchor in listOf(FeedBlock.After::class.java, FeedBlock.Before::class.java)) {
            assertTrue(
                withAnchor.declaredFields.any { it.type == FeedPosition::class.java },
                "${withAnchor.simpleName} потерял якорь"
            )
        }
    }

    @Test
    fun `тело записи уходит как JSON-значение, а не как строка`() {
        val (c, n) = clientWith(okEnvelope("""{"stored":1}"""))
        c.localFeedPut("ch", listOf(rec.copy(body = """{"message":"привет","tags":[1,2]}""")))
        val payload = n.actions.single().second
        assertTrue(payload.contains(""""body":{"message":"привет","tags":[1,2]}"""), payload)
    }

    @Test
    fun `негодное тело — ошибка построения, а не ответ ядра`() {
        val (c, n) = clientWith(okEnvelope("""{"stored":1}"""))
        assertFailsWith<IllegalArgumentException> {
            c.localFeedPut("ch", listOf(rec.copy(body = "не json")))
        }
        // Главное: до ABI вызов не дошёл. Иначе ядро получило бы payload,
        // который SDK уже признал негодным.
        assertTrue(n.actions.isEmpty(), "негодная запись всё равно ушла в ABI")
    }

    // ── Long ────────────────────────────────────────────────────────────

    @Test
    fun `order_key и version переживают значение выше 2^53`() {
        // 2^53+1: Double округлил бы его до 2^53. Миллисекунды эпохи (1.7e12)
        // такой мутации не ловят — они меньше порога, проверено на ядре.
        val big = 9007199254740993L
        val (c, n) = clientWith(okEnvelope("""{"stored":1}"""))
        c.localFeedPut("ch", listOf(rec.copy(orderKey = big, version = big)))
        val payload = n.actions.single().second
        assertTrue(payload.contains(""""order_key":9007199254740993"""), payload)
        assertTrue(payload.contains(""""version":9007199254740993"""), payload)
    }

    @Test
    fun `якорь в ответе разбирается точным Long`() {
        val big = 9007199254740993L
        val (c, _) = clientWith(okEnvelope("""{"found":true,"anchor":{"order_key":$big,"id":"m"}}"""))
        val got = c.localFeedGapAnchor("ch")
        assertIs<FeedGapAnchor.Found>(got)
        assertEquals(big, got.anchor.orderKey)
    }

    // ── Результат заливки ───────────────────────────────────────────────

    @Test
    fun `все семь полей доезжают до вызывающего`() {
        val (c, _) = clientWith(
            okEnvelope(
                """{"stored":1,"segments":2,"updated":3,"unchanged":4,
                    "stale":5,"vanished":6,"gaps_closed":7}"""
            )
        )
        val got = c.localFeedIngest("ch", FeedBlock.Newest(listOf(rec), atEnd = false))
        assertIs<FeedIngestResult.Applied>(got)
        assertEquals(listOf(1, 2, 3, 4, 5, 6, 7), listOf(
            got.stored, got.segments, got.updated, got.unchanged,
            got.stale, got.vanished, got.gapsClosed,
        ))
    }

    @Test
    fun `gaps_closed остаётся отрицательным`() {
        // Отрицательное значение законно и означает НОВЫЙ пропуск. Приведение
        // к нулю «чтобы не пугать» потеряло бы ровно тот случай, ради
        // которого поле и заведено.
        val (c, _) = clientWith(
            okEnvelope(
                """{"stored":1,"segments":2,"updated":0,"unchanged":0,
                    "stale":0,"vanished":0,"gaps_closed":-1}"""
            )
        )
        val got = c.localFeedIngest("ch", FeedBlock.Newest(listOf(rec), atEnd = false))
        assertIs<FeedIngestResult.Applied>(got)
        assertEquals(-1, got.gapsClosed)
    }

    @Test
    fun `отсутствие любого из семи полей даёт Unavailable, а не ноль`() {
        // Подставленный ноль неотличим от настоящего нуля: «поле не пришло»
        // превратилось бы в «ничего не изменилось».
        val names = listOf("stored", "segments", "updated", "unchanged", "stale", "vanished", "gaps_closed")
        for (missing in names) {
            val data = names.filter { it != missing }.joinToString(",") { """"$it":1""" }
            val (c, _) = clientWith(okEnvelope("{$data}"))
            val got = c.localFeedIngest("ch", FeedBlock.Newest(listOf(rec), atEnd = false))
            assertIs<FeedIngestResult.Unavailable>(got, "без поля $missing результат не стал Unavailable")
        }
    }

    @Test
    fun `нечисловое поле результата даёт Unavailable`() {
        for (bad in listOf(""""stored":"1"""", """"stored":1.5""")) {
            val (c, _) = clientWith(
                okEnvelope(
                    """{$bad,"segments":1,"updated":1,"unchanged":1,
                        "stale":1,"vanished":1,"gaps_closed":1}"""
                )
            )
            val got = c.localFeedIngest("ch", FeedBlock.Newest(listOf(rec), atEnd = false))
            assertIs<FeedIngestResult.Unavailable>(got, "поле $bad принято за число")
        }
    }

    // ── GapAnchor ───────────────────────────────────────────────────────

    @Test
    fun `found false — это None, а не Unavailable и не нулевая позиция`() {
        val (c, _) = clientWith(okEnvelope("""{"found":false}"""))
        val got = c.localFeedGapAnchor("ch")
        assertIs<FeedGapAnchor.None>(got)
    }

    @Test
    fun `found true отдаёт позицию`() {
        val (c, _) = clientWith(okEnvelope("""{"found":true,"anchor":{"order_key":100,"id":"o1"}}"""))
        val got = c.localFeedGapAnchor("ch")
        assertIs<FeedGapAnchor.Found>(got)
        assertEquals(FeedPosition(100, "o1"), got.anchor)
    }

    @Test
    fun `found true без якоря — Unavailable, а не выдуманная позиция`() {
        val (c, _) = clientWith(okEnvelope("""{"found":true}"""))
        assertIs<FeedGapAnchor.Unavailable>(c.localFeedGapAnchor("ch"))
    }

    @Test
    fun `нечитаемый found — Unavailable`() {
        // "false" строкой вердиктом не является — то же правило, что у
        // ResponseParser для "ok".
        for (bad in listOf("""{"found":"false"}""", """{"found":1}""", """{}""")) {
            val (c, _) = clientWith(okEnvelope(bad))
            assertIs<FeedGapAnchor.Unavailable>(c.localFeedGapAnchor("ch"), "принято: $bad")
        }
    }

    // ── Конверт: отказ и неразобранное ──────────────────────────────────

    @Test
    fun `отказ и неразобранный конверт остаются различимы через response`() {
        // Второй таксономии конверта SDK не заводит: Rejected и Unrecognized
        // различает PhotonResponse, и он же доступен из результата.
        val (c1, _) = clientWith("""{"ok":false,"error":"no storage"}""")
        val r1 = c1.localFeedIngest("ch", FeedBlock.Newest(listOf(rec), atEnd = false))
        assertIs<FeedIngestResult.Unavailable>(r1)
        assertIs<PhotonResponse.Rejected>(r1.response)

        val (c2, _) = clientWith("не json")
        val r2 = c2.localFeedIngest("ch", FeedBlock.Newest(listOf(rec), atEnd = false))
        assertIs<FeedIngestResult.Unavailable>(r2)
        assertIs<PhotonResponse.Unrecognized>(r2.response)
    }

    @Test
    fun `исходный конверт доступен из любого результата`() {
        val raw = okEnvelope("""{"stored":2}""")
        val (c, _) = clientWith(raw)
        val got = c.localFeedPut("ch", listOf(rec))
        assertIs<FeedPutResult.Stored>(got)
        assertEquals(raw, got.response.raw)
        assertEquals(2, got.stored)
    }

    // ── Границы, которые срез не переходит ──────────────────────────────

    @Test
    fun `действие 800 не типизируется этим срезом`() {
        // ADR-066 п. 3.6. Появление localFeedPage означало бы расширение
        // поверхности сверх принятого решения.
        assertTrue(
            PhotonClient::class.java.methods.none { it.name.contains("Page", ignoreCase = true) },
            "появился типизированный примитив страницы: " +
                PhotonClient::class.java.methods.joinToString { it.name }
        )
    }

    @Test
    fun `sendAction остаётся публичным`() {
        // Он общий для всех действий и не объявляется устаревшим (ADR-066
        // п. 3.5): типизируются три, остальное по-прежнему через него.
        val m = PhotonClient::class.java.methods.single { it.name == "sendAction" }
        assertTrue(m.annotations.none { it.annotationClass == Deprecated::class })
    }

    @Test
    fun `закрытый клиент отвергает типизированные вызовы так же, как sendAction`() {
        val (c, _) = clientWith(okEnvelope("""{"stored":1}"""))
        c.close()
        assertFailsWith<IllegalStateException> { c.localFeedPut("ch", listOf(rec)) }
        assertFailsWith<IllegalStateException> {
            c.localFeedIngest("ch", FeedBlock.Newest(listOf(rec), atEnd = false))
        }
        assertFailsWith<IllegalStateException> { c.localFeedGapAnchor("ch") }
    }

    private fun fullIngestData() =
        """{"stored":1,"segments":1,"updated":0,"unchanged":0,"stale":0,"vanished":0,"gaps_closed":0}"""
}
