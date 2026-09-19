package dev.photon.sdk

import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/*
 * Семантика слушателей — §3 спецификации.
 *
 * Нативная библиотека здесь не нужна: правила §3 живут выше ABI. Это
 * сознательный выбор дизайна — правила, проверяемые только на живом
 * сервере, проверяются редко.
 *
 * Обработчик необработанных исключений — глобальное состояние JVM, поэтому
 * каждый тест ставит свой и возвращает прежний в @AfterTest.
 */
class EventDispatcherTest {

    private val saved: Thread.UncaughtExceptionHandler? =
        Thread.getDefaultUncaughtExceptionHandler()

    @AfterTest
    fun restore() {
        Thread.setDefaultUncaughtExceptionHandler(saved)
    }

    private val connected = """{"type":"CONNECTED"}"""
    private val message = """{"type":"MESSAGE","event":"posted","data":{"a":1}}"""

    private fun recorder(into: MutableList<String>, name: String) =
        PhotonEventListener { into.add(name) }

    // ── Доставка и порядок ───────────────────────────────────────────────

    @Test
    fun `событие доходит до всех слушателей в порядке регистрации`() {
        val calls = mutableListOf<String>()
        val d = EventDispatcher()
        d.add(recorder(calls, "первый"))
        d.add(recorder(calls, "второй"))
        d.add(recorder(calls, "третий"))

        d.dispatch(connected)

        // Порядок именно регистрации, а не произвольный: недетерминированный
        // порядок нельзя проверить тестом вовсе.
        assertEquals(listOf("первый", "второй", "третий"), calls)
    }

    @Test
    fun `слушатели получают разобранное событие, а не строку`() {
        var got: PhotonEvent? = null
        val d = EventDispatcher()
        d.add(PhotonEventListener { got = it })

        d.dispatch(message)

        val e = assertIs<PhotonEvent.Message>(got)
        assertEquals("posted", e.event)
        assertEquals("""{"a":1}""", e.data)
        assertEquals(message, e.raw)
    }

    @Test
    fun `неразбираемый конверт доставляется как Unknown, а не теряется`() {
        var got: PhotonEvent? = null
        val d = EventDispatcher()
        d.add(PhotonEventListener { got = it })

        d.dispatch("вовсе не json")

        val e = assertIs<PhotonEvent.Unknown>(got)
        assertNull(e.type)
    }

    @Test
    fun `без слушателей доставка не падает`() {
        EventDispatcher().dispatch(connected)
    }

    // ── §3.1–3.2: изменение набора во время доставки ─────────────────────

    @Test
    fun `remove из тела слушателя не бросает и не влияет на текущее событие`() {
        val calls = mutableListOf<String>()
        val d = EventDispatcher()
        lateinit var second: PhotonEventListener

        d.add(PhotonEventListener {
            calls.add("первый")
            d.remove(second)          // снимаем ИДУЩЕГО СЛЕДОМ
        })
        second = recorder(calls, "второй")
        d.add(second)

        d.dispatch(connected)

        // Слушатель уже попал в снимок — текущее событие он получает.
        assertEquals(listOf("первый", "второй"), calls)
        // А в следующих снимках его уже нет.
        calls.clear()
        d.dispatch(connected)
        assertEquals(listOf("первый"), calls)
    }

    @Test
    fun `add из тела слушателя не влияет на текущее событие`() {
        val calls = mutableListOf<String>()
        val d = EventDispatcher()
        d.add(PhotonEventListener {
            calls.add("первый")
            d.add(recorder(calls, "добавленный"))
        })

        d.dispatch(connected)
        assertEquals(listOf("первый"), calls, "добавленный во время доставки получил текущее событие")

        calls.clear()
        d.dispatch(connected)
        assertEquals(listOf("первый", "добавленный"), calls)
    }

    @Test
    fun `слушатель, снявший сам себя, текущее событие получает`() {
        val calls = mutableListOf<String>()
        val d = EventDispatcher()
        lateinit var self: PhotonEventListener
        self = PhotonEventListener {
            calls.add("сам")
            d.remove(self)
        }
        d.add(self)

        d.dispatch(connected)
        d.dispatch(connected)

        assertEquals(listOf("сам"), calls)
        assertEquals(0, d.count())
    }

    // ── §3.4: отказ слушателя ────────────────────────────────────────────

    @Test
    fun `исключение одного слушателя не отменяет доставку остальным`() {
        val calls = mutableListOf<String>()
        Thread.setDefaultUncaughtExceptionHandler { _, _ -> }
        val d = EventDispatcher()
        d.add(recorder(calls, "до"))
        d.add(PhotonEventListener { throw IllegalStateException("отказ") })
        d.add(recorder(calls, "после"))

        d.dispatch(connected)

        assertEquals(listOf("до", "после"), calls)
    }

    @Test
    fun `перехватывается Throwable, а не только Exception`() {
        // Мутация, которую тест обязан ловить: сузить catch до Exception.
        // Тогда Error вышел бы из dispatch и пересёк границу JNI.
        val reported = CopyOnWriteArrayList<Throwable>()
        Thread.setDefaultUncaughtExceptionHandler { _, e -> reported.add(e) }
        val calls = mutableListOf<String>()
        val d = EventDispatcher()
        d.add(PhotonEventListener { throw StackOverflowError("искусственный Error") })
        d.add(recorder(calls, "после Error"))

        d.dispatch(connected)   // не бросает

        assertEquals(listOf("после Error"), calls)
        assertEquals(1, reported.size)
        assertIs<StackOverflowError>(reported.first())
    }

    @Test
    fun `отказ подаётся обработчику текущего потока ровно один раз`() {
        val reported = CopyOnWriteArrayList<Pair<String, String>>()
        Thread.setDefaultUncaughtExceptionHandler { t, e ->
            reported.add(t.name to (e.message ?: ""))
        }
        val d = EventDispatcher()
        d.add(PhotonEventListener { throw IllegalStateException("отказ-1") })

        d.dispatch(connected)

        assertEquals(1, reported.size)
        assertEquals("отказ-1", reported.first().second)
        assertEquals(Thread.currentThread().name, reported.first().first,
            "обработчик получил не тот поток, на котором шла доставка")
    }

    @Test
    fun `каждый отказавший слушатель подаётся отдельно`() {
        val reported = CopyOnWriteArrayList<String>()
        Thread.setDefaultUncaughtExceptionHandler { _, e -> reported.add(e.message ?: "") }
        val d = EventDispatcher()
        d.add(PhotonEventListener { throw IllegalStateException("первый") })
        d.add(PhotonEventListener { throw IllegalStateException("второй") })

        d.dispatch(connected)

        assertEquals(listOf("первый", "второй"), reported)
    }

    @Test
    fun `исключение самого обработчика подавляется и не подаётся повторно`() {
        val entered = CopyOnWriteArrayList<String>()
        Thread.setDefaultUncaughtExceptionHandler { _, e ->
            entered.add(e.message ?: "")
            throw RuntimeException("обработчик сам сломался")
        }
        val calls = mutableListOf<String>()
        val d = EventDispatcher()
        d.add(PhotonEventListener { throw IllegalStateException("отказ") })
        d.add(recorder(calls, "следующий"))

        d.dispatch(connected)   // не бросает

        assertEquals(listOf("отказ"), entered, "обработчик вызван повторно своим же исключением")
        assertEquals(listOf("следующий"), calls, "поломка обработчика остановила доставку")
    }

    @Test
    fun `dispatch не бросает даже когда обработчик не установлен`() {
        // ThreadGroup остаётся обработчиком по умолчанию и печатает
        // трассировку; проверяется здесь то, что наружу ничего не выходит.
        Thread.setDefaultUncaughtExceptionHandler(null)
        val calls = mutableListOf<String>()
        val d = EventDispatcher()
        d.add(PhotonEventListener { throw IllegalStateException("отказ без обработчика") })
        d.add(recorder(calls, "следующий"))

        d.dispatch(connected)

        assertEquals(listOf("следующий"), calls)
    }

    @Test
    fun `без установленного обработчика отказ всё равно виден в System_err`() {
        // Это и есть причина брать currentThread().uncaughtExceptionHandler,
        // а не getDefaultUncaughtExceptionHandler(): второй вернул бы null,
        // и отказ стал бы невидим совсем — хуже сегодняшнего положения, где
        // мост печатает трассировку сам.
        //
        // Мутация, которую тест обязан ловить: заменить источник обработчика
        // на getDefaultUncaughtExceptionHandler().
        Thread.setDefaultUncaughtExceptionHandler(null)
        val captured = java.io.ByteArrayOutputStream()
        val realErr = System.err
        System.setErr(java.io.PrintStream(captured, true))
        try {
            val d = EventDispatcher()
            d.add(PhotonEventListener { throw IllegalStateException("МАРКЕР-запасного-пути") })
            d.dispatch(connected)
        } finally {
            System.setErr(realErr)
        }

        assertTrue(
            captured.toString().contains("МАРКЕР-запасного-пути"),
            "отказ не виден нигде: в System.err пусто (${captured.toString().take(120)})"
        )
    }

    // ── SDK не трогает глобальный обработчик ─────────────────────────────

    @Test
    fun `SDK не устанавливает default-обработчик`() {
        Thread.setDefaultUncaughtExceptionHandler(null)
        val d = EventDispatcher()
        d.add(PhotonEventListener { throw IllegalStateException("отказ") })

        d.dispatch(connected)

        assertNull(
            Thread.getDefaultUncaughtExceptionHandler(),
            "SDK установил глобальный обработчик — это захват ресурса приложения"
        )
    }

    @Test
    fun `count отражает набор`() {
        val d = EventDispatcher()
        val l = PhotonEventListener { }
        assertEquals(0, d.count())
        d.add(l)
        assertEquals(1, d.count())
        d.remove(l)
        assertEquals(0, d.count())
        d.remove(l)   // повторное снятие безопасно
        assertTrue(d.count() == 0)
    }
}
