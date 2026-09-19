package dev.photon.sdk

import dev.photon.core.PhotonException
import dev.photon.core.PhotonNative
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/*
 * PhotonClient — §8 (время жизни колбэка и слушателей) и §9 (действия).
 *
 * Нативная библиотека не нужна: сессия подставляется швом NativeSession.
 * Свойства самого ABI, которые шов воспроизводит (Destroy ждёт колбэк,
 * колбэк один), доказаны отдельным живым прогоном — здесь проверяется
 * поведение SDK поверх них.
 */
class PhotonClientTest {

    private val saved: Thread.UncaughtExceptionHandler? =
        Thread.getDefaultUncaughtExceptionHandler()

    @AfterTest
    fun restore() {
        Thread.setDefaultUncaughtExceptionHandler(saved)
    }

    private fun client(fake: FakeNativeSession = FakeNativeSession()): Pair<PhotonClient, FakeNativeSession> =
        PhotonClient.createWith(PhotonConfig()) { fake } to fake

    private val connected = """{"type":"CONNECTED"}"""
    private val message = """{"type":"MESSAGE","event":"posted","data":{"a":1}}"""

    // ── Создание ─────────────────────────────────────────────────────────

    @Test
    fun `создание с типизированной конфигурацией передаёт config_json`() {
        val seen = AtomicReference<String>()
        val cfg = PhotonConfig(
            BridgeConfig(endpoint = "host:4443", callTimeout = Duration.ofSeconds(30))
        )
        PhotonClient.createWith(cfg) { json -> seen.set(json); FakeNativeSession() }

        assertEquals(
            """{"bridge":{"endpoint":"host:4443","call_timeout_ms":30000}}""",
            seen.get()
        )
    }

    @Test
    fun `создание с raw-конфигурацией передаёт строку без изменений`() {
        val seen = AtomicReference<String>()
        val src = """{ "storage_path" : "/tmp/photon" , "bridge":{"endpoint":"h:1"} }"""
        PhotonClient.createWith(PhotonConfig.raw(src)) { json -> seen.set(json); FakeNativeSession() }

        assertEquals(src, seen.get())
    }

    @Test
    fun `создание сети не касается и колбэка не ставит`() {
        val (c, fake) = client()
        assertEquals(0, fake.installs.get())
        assertFalse(c.callbackInstalled())
    }

    // ── §8 L1–L2: установка колбэка ──────────────────────────────────────

    @Test
    fun `первый addEventListener устанавливает нативный колбэк`() {
        val (c, fake) = client()
        c.addEventListener { }
        assertEquals(1, fake.installs.get())
        assertTrue(fake.hasCallback())
    }

    @Test
    fun `второй addEventListener колбэк НЕ переустанавливает`() {
        val (c, fake) = client()
        c.addEventListener { }
        c.addEventListener { }
        c.addEventListener { }
        assertEquals(1, fake.installs.get(), "колбэк переустанавливался")
        assertEquals(3, c.listenerCount())
    }

    @Test
    fun `removeEventListener нативного колбэка не касается`() {
        val (c, fake) = client()
        val l = PhotonEventListener { }
        c.addEventListener(l)
        c.removeEventListener(l)
        assertEquals(1, fake.installs.get(), "remove тронул native")
        assertTrue(fake.hasCallback(), "remove снял нативный колбэк")
    }

    @Test
    fun `снятие ПОСЛЕДНЕГО слушателя колбэк не снимает и подписку не трогает`() {
        val (c, fake) = client()
        val l = PhotonEventListener { }
        c.addEventListener(l)
        c.removeEventListener(l)

        assertEquals(0, c.listenerCount())
        assertTrue(fake.hasCallback(), "колбэк снят вместе с последним слушателем")

        // §8 L4: доставка в пустой набор законна.
        fake.emit(connected)

        // И новый слушатель начинает получать события БЕЗ переустановки.
        val got = AtomicInteger()
        c.addEventListener { got.incrementAndGet() }
        fake.emit(connected)
        assertEquals(1, got.get())
        assertEquals(1, fake.installs.get(), "колбэк переустановили после пустого набора")
    }

    // ── §3 поверх настоящего клиента ─────────────────────────────────────

    @Test
    fun `событие доходит до нескольких слушателей в порядке регистрации`() {
        val (c, fake) = client()
        val calls = mutableListOf<String>()
        c.addEventListener { calls.add("первый") }
        c.addEventListener { calls.add("второй") }

        fake.emit("""{"type":"MESSAGE","event":"posted","data":{"a":1}}""")

        assertEquals(listOf("первый", "второй"), calls)
    }

    @Test
    fun `слушатели получают разобранное событие`() {
        val (c, fake) = client()
        val got = AtomicReference<PhotonEvent>()
        c.addEventListener { got.set(it) }

        val raw = """{"type":"MESSAGE","event":"posted","user_id":"u1","data":{ "a" : 1 }}"""
        fake.emit(raw)

        val e = assertIs<PhotonEvent.Message>(got.get())
        assertEquals("posted", e.event)
        assertEquals("u1", e.userId)
        assertEquals("""{ "a" : 1 }""", e.data, "data пересобран, а не вырезан")
        assertEquals(raw, e.raw)
    }

    @Test
    fun `снятие во время доставки на текущее событие не влияет`() {
        val (c, fake) = client()
        val calls = mutableListOf<String>()
        lateinit var second: PhotonEventListener
        c.addEventListener {
            calls.add("первый")
            c.removeEventListener(second)
        }
        second = PhotonEventListener { calls.add("второй") }
        c.addEventListener(second)

        fake.emit(connected)
        assertEquals(listOf("первый", "второй"), calls, "снимок не соблюдён")

        calls.clear()
        fake.emit(connected)
        assertEquals(listOf("первый"), calls)
    }

    @Test
    fun `исключение слушателя не отменяет доставку остальным`() {
        Thread.setDefaultUncaughtExceptionHandler { _, _ -> }
        val (c, fake) = client()
        val calls = mutableListOf<String>()
        c.addEventListener { throw IllegalStateException("отказ") }
        c.addEventListener { calls.add("после") }

        fake.emit(connected)

        assertEquals(listOf("после"), calls)
    }

    // ── §8 L7–L10: закрытие ──────────────────────────────────────────────

    @Test
    fun `addEventListener после close бросает — колбэк НЕ ставился`() {
        val (c, _) = client()
        c.close()
        assertFailsWith<IllegalStateException> { c.addEventListener { } }
    }

    @Test
    fun `addEventListener после close бросает — колбэк уже был установлен`() {
        // Ровно тот случай, ради которого L7 сформулирован «всегда»:
        // здесь add моста не касается, и без явной проверки он молча
        // добавил бы слушателя в мёртвый клиент.
        val (c, fake) = client()
        c.addEventListener { }
        c.close()

        assertFailsWith<IllegalStateException> { c.addEventListener { } }
        assertEquals(1, fake.installs.get())
    }

    @Test
    fun `removeEventListener после close — no-op, не бросает`() {
        val (c, _) = client()
        val l = PhotonEventListener { }
        c.addEventListener(l)
        c.close()

        c.removeEventListener(l)                 // ранее зарегистрированный
        c.removeEventListener(PhotonEventListener { })  // никогда не бывший
    }

    @Test
    fun `close очищает набор слушателей`() {
        val (c, _) = client()
        c.addEventListener { }
        c.addEventListener { }
        assertEquals(2, c.listenerCount())

        c.close()
        assertEquals(0, c.listenerCount(), "слушатели не сняты при закрытии")
    }

    @Test
    fun `набор очищается ПОСЛЕ Photon_Destroy, а не до`() {
        // Мутация, которую тест обязан ловить: очистить список раньше
        // native.close(). Тогда доставка, идущая во время Destroy, потеряла
        // бы адресатов, которым событие обещано снимком.
        val fake = FakeNativeSession()
        val gate = CountDownLatch(1)
        val countDuringDestroy = AtomicInteger(-1)
        val c = PhotonClient.createWith(PhotonConfig()) { fake }
        c.addEventListener { }

        fake.closeGate = gate
        val closer = Thread { c.close() }
        closer.start()
        Thread.sleep(200)
        countDuringDestroy.set(c.listenerCount())   // Destroy ещё идёт
        gate.countDown()
        closer.join(5000)

        assertEquals(1, countDuringDestroy.get(), "набор очищен ДО возврата из Destroy")
        assertEquals(0, c.listenerCount())
    }

    @Test
    fun `повторный close безопасен и Destroy выполняется один раз`() {
        val (c, fake) = client()
        c.addEventListener { }
        c.close()
        c.close()
        c.close()
        assertEquals(1, fake.closes.get(), "Photon_Destroy вызван повторно")
    }

    @Test
    fun `close во время доставки не даёт дедлока, даже если слушатель зовёт клиента`() {
        // Замок клиента НЕ должен удерживаться на время native.close():
        // иначе слушатель, обратившийся к клиенту, повис бы, а Destroy ждал
        // бы его — классический дедлок (та же ловушка, что ctxLock в мосте).
        val fake = FakeNativeSession()
        val c = PhotonClient.createWith(PhotonConfig()) { fake }

        val inDelivery = CountDownLatch(1)
        val mayFinish = CountDownLatch(1)
        val addThrew = AtomicBoolean(false)

        c.addEventListener {
            inDelivery.countDown()
            // Пока идёт доставка, другой поток закрывает клиента. Обращение
            // отсюда обязано ВЕРНУТЬСЯ (исключением), а не заблокироваться.
            mayFinish.await(10, TimeUnit.SECONDS)
            addThrew.set(runCatching { c.addEventListener { } }.exceptionOrNull() is IllegalStateException)
        }

        val emitter = Thread { fake.emit(connected) }
        emitter.start()
        assertTrue(inDelivery.await(5, TimeUnit.SECONDS))

        val closer = Thread { c.close() }
        closer.start()
        Thread.sleep(200)
        mayFinish.countDown()

        closer.join(10_000)
        emitter.join(10_000)
        assertFalse(closer.isAlive, "close() не вернулся — дедлок")
        assertFalse(emitter.isAlive, "доставка не завершилась — дедлок")
        assertTrue(addThrew.get(), "add из колбэка закрываемого клиента обязан бросить IllegalStateException")
    }

    // ── §3.4 п. 7: close() из обработчика ошибок ─────────────────────────

    /**
     * Спецификация §3.4 п. 7 объявляет `close()` из обработчика допустимым, а
     * §8 L6 — что после возврата из `Destroy` доставок нет. До этого теста
     * оба свойства держались только живым прогоном 2026-09-18 (возврат за
     * 25 мс), оснастка которого в репозиторий не вошла.
     *
     * Шов моделирует реентерабельный `Photon_Destroy`: вызов из колбэка
     * возвращается сразу, не дожидаясь сам себя.
     */
    @Test
    fun `close из обработчика ошибок возвращает управление и не даёт новых доставок`() {
        val fake = FakeNativeSession()
        val client = PhotonClient.createWith(PhotonConfig()) { fake }

        val handlerReturned = AtomicBoolean(false)
        val closeThrew = AtomicReference<Throwable?>(null)
        Thread.setDefaultUncaughtExceptionHandler { _, _ ->
            closeThrew.set(runCatching { client.close() }.exceptionOrNull())
            handlerReturned.set(true)
        }

        val afterClose = AtomicInteger(0)
        client.addEventListener { if (it is PhotonEvent.Message) throw IllegalStateException("отказ") }
        client.addEventListener { afterClose.incrementAndGet() }

        // Доставка идёт на этом же потоке: если close() внутри обработчика
        // повиснет, тест не вернётся вовсе — и это правильный способ
        // обнаружить дедлок, а не таймаут с мягким сообщением.
        fake.emit(message)

        assertTrue(handlerReturned.get(), "обработчик не вернул управление — close() повис")
        assertNull(closeThrew.get(), "close() из обработчика бросил: ${closeThrew.get()}")
        assertEquals(1, fake.closes.get(), "Photon_Destroy вызван не один раз")

        // Остаток снимка отработал: отменить идущую доставку ядро не может и
        // не обещает (§3.4 п. 7).
        assertEquals(1, afterClose.get(), "слушатель после бросившего не получил событие")

        // А новых доставок больше нет — клиент закрыт.
        val before = afterClose.get()
        fake.emit(message)
        assertEquals(before, afterClose.get(), "после close() пришла новая доставка")
    }

    /**
     * §3.4, конец: слушатель, идущий следом за закрытием, обращается к уже
     * закрытому клиенту и получает `IllegalStateException` — это ОБЫЧНОЕ
     * исключение слушателя, и оно идёт тем же путём п. 1–6, то есть попадает
     * тому же обработчику. Цепочка конечна.
     */
    @Test
    fun `отказ слушателя на уже закрытом инстансе идёт тем же путём`() {
        val fake = FakeNativeSession()
        val client = PhotonClient.createWith(PhotonConfig()) { fake }

        val reported = CopyOnWriteArrayList<Throwable>()
        Thread.setDefaultUncaughtExceptionHandler { _, e ->
            reported.add(e)
            // Закрываем ровно один раз, на первом отказе.
            if (reported.size == 1) client.close()
        }

        val lastRan = AtomicBoolean(false)
        client.addEventListener { if (it is PhotonEvent.Message) throw IllegalStateException("первый отказ") }
        client.addEventListener {
            // Клиент уже закрыт обработчиком — вызов обязан бросить.
            client.sendAction(200, "{}")
        }
        client.addEventListener { lastRan.set(true) }

        fake.emit(message)

        assertEquals(2, reported.size, "обработчик получил не оба отказа: $reported")
        assertIs<IllegalStateException>(reported[0])
        val second = assertIs<IllegalStateException>(reported[1])
        assertTrue(
            second.message!!.contains("Photon client is closed"),
            "второй отказ пришёл не от закрытого клиента: ${second.message}"
        )
        assertTrue(lastRan.get(), "цепочка оборвала доставку остатку снимка")
        assertEquals(1, fake.closes.get(), "close() вызван повторно — цепочка не конечна")
    }

    // ── §9: действия ─────────────────────────────────────────────────────

    @Test
    fun `sendAction передаёт код и payload и возвращает Ok`() {
        val (c, fake) = client()
        fake.nextResponse = """{"ok":true,"data":{"id":"p1"}}"""

        val r = c.sendAction(100, """{"channel_id":"c1"}""")

        assertEquals(listOf(100 to """{"channel_id":"c1"}"""), fake.actions.toList())
        val ok = assertIs<PhotonResponse.Ok>(r)
        assertEquals("""{"id":"p1"}""", ok.data)
    }

    @Test
    fun `sendAction с ok false даёт Rejected, а НЕ исключение`() {
        val (c, fake) = client()
        fake.nextResponse = """{"ok":false,"error":"team_id is required"}"""

        val r = c.sendAction(200, "{}")

        val rejected = assertIs<PhotonResponse.Rejected>(r)
        assertEquals("team_id is required", rejected.error)
        assertNull(rejected.data)
    }

    @Test
    fun `несостоявшийся вызов даёт PhotonException с кодом и текстом моста`() {
        val (c, fake) = client()
        fake.nextThrow = PhotonException(PhotonNative.PHOTON_ERR_NOT_CONNECTED, "no connection")

        val e = assertFailsWith<PhotonException> { c.sendAction(200, "{}") }
        assertEquals(PhotonNative.PHOTON_ERR_NOT_CONNECTED, e.code)
        assertEquals("no connection", e.nativeMessage)
        assertTrue(e.message!!.contains("PHOTON_ERR_NOT_CONNECTED"))
    }

    @Test
    fun `исключение моста не превращается в Unrecognized`() {
        // Мутация: поймать PhotonException и вернуть Unrecognized. Тогда
        // «связи нет» стало бы неотличимо от «сервер ответил мусором».
        val (c, fake) = client()
        fake.nextThrow = PhotonException(PhotonNative.PHOTON_ERR_TIMEOUT, null)
        assertFailsWith<PhotonException> { c.sendAction(200, "{}") }
    }

    @Test
    fun `sendAction после close бросает IllegalStateException и до моста не доходит`() {
        // Проверка принадлежит SDK, а не наследуется от моста: шов здесь
        // НИЧЕГО не бросает, поэтому тест падает, если убрать check(!closed)
        // из PhotonClient.sendAction.
        val fake = FakeNativeSession()
        val c = PhotonClient.createWith(PhotonConfig()) { fake }
        c.sendAction(200, "{}")          // до закрытия вызов проходит
        assertEquals(1, fake.actions.size)

        c.close()

        val e = assertFailsWith<IllegalStateException> { c.sendAction(200, "{}") }
        assertTrue(
            e.message!!.contains("Photon client is closed"),
            "сообщение: ${e.message}"
        )
        assertEquals(1, fake.actions.size, "вызов ушёл в мост на закрытом клиенте")
    }

    @Test
    fun `конкурентные sendAction не смешивают результаты`() {
        // Код результата в мосте — thread-local (photon_jni.c:53-54).
        // Проверяется, что SDK не заводит поверх него общего состояния.
        val fake = object : NativeSession {
            override fun sendAction(action: Int, payloadJson: String): String {
                Thread.sleep(2)
                return """{"ok":true,"data":{"echo":$action}}"""
            }
            override fun setEventListener(listener: dev.photon.core.PhotonEventListener?) {}
            override fun close() {}
        }
        val c = PhotonClient.createWith(PhotonConfig()) { fake }

        val threads = (1..16).map { i ->
            Thread {
                repeat(20) {
                    val r = c.sendAction(i, "{}")
                    assertEquals("""{"echo":$i}""", (r as PhotonResponse.Ok).data)
                }
            }
        }
        threads.forEach { it.start() }
        threads.forEach { it.join(20_000) }
        threads.forEach { assertFalse(it.isAlive) }
    }

    // ── §9: raw и data ───────────────────────────────────────────────────

    @Test
    fun `raw ответа сохраняется байт в байт при форматировании, которое пересборка изменила бы`() {
        val (c, fake) = client()
        val raw = """{ "ok" : true ,  "data" : { "a": 1 , "b":[1,  2] } }"""
        fake.nextResponse = raw

        val r = c.sendAction(200, "{}")

        assertEquals(raw, r.raw)
        assertEquals("""{ "a": 1 , "b":[1,  2] }""", (r as PhotonResponse.Ok).data)
    }

    @Test
    fun `data ответа сохраняет точность чисел`() {
        val (c, fake) = client()
        val data = """{"create_at":1758181234567,"big":123456789012345678901234567890,"e":1e3}"""
        fake.nextResponse = """{"ok":true,"data":$data}"""

        assertEquals(data, (c.sendAction(200, "{}") as PhotonResponse.Ok).data)
    }
}
