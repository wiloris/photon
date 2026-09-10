import dev.photon.core.Photon
import dev.photon.core.PhotonEventListener
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean

/*
 * Тесты моста событий (photon-bindings/docs/jni-events.md §10).
 *
 * Требуют ЖИВОГО Bridge: без подписки событий не существует, а тест,
 * который «тихо пропускается» при отсутствии сервера, хуже отсутствующего
 * (CLAUDE.md, «Метод работы»). Поэтому адрес и Session ID — обязательные
 * аргументы, и без них прогон падает, а не проходит.
 *
 *   java -cp ... PhotonEventsTestKt <libphoton_jni.so> <endpoint> <sessionId>
 *
 * Session ID и адрес в коде НЕ хранятся.
 */

private var passed = 0
private var failed = 0

private fun check(name: String, ok: Boolean, detail: String = "") {
    if (ok) { passed++; println("PASS: $name") }
    else { failed++; println("FAIL: $name -- $detail") }
}

private fun config(endpoint: String, token: String) =
    """{"bridge":{"endpoint":"$endpoint","session_token":"$token",
       "connect_timeout_ms":15000,"call_timeout_ms":20000}}"""
        .trimIndent().replace("\n", "")

private fun listener(body: (String) -> Unit) = object : PhotonEventListener {
    override fun onEvent(eventJson: String) = body(eventJson)
}

private fun typeOf(json: String): String =
    Regex("\"type\"\\s*:\\s*\"([A-Z_]+)\"").find(json)?.groupValues?.get(1) ?: ""

/**
 * Идентификатор из ответа. Отсутствие — не NPE, а внятный отказ с текстом
 * ответа: иначе дефект самого теста неотличим от дефекта моста.
 */
private fun idFrom(what: String, response: String): String =
    Regex("\"id\"\\s*:\\s*\"([a-z0-9]+)\"").find(response)?.groupValues?.get(1)
        ?: error("$what: в ответе нет id -- ${response.take(200)}")

/** Самопереписка субъекта: канал, в который тест сам себе пишет. */
private fun selfChannel(p: Photon): String {
    val id = idFrom("getMe", p.sendAction(200, "{}"))
    // Поле называется target_user_id (server/handler_channel.go), и это
    // единственная форма, которую принимает Bridge.
    val ch = p.sendAction(308, """{"target_user_id":"$id"}""")
    return idFrom("getOrCreateDirectChannel", ch)
}

private fun post(p: Photon, channel: String, text: String): String =
    idFrom("createPost", p.sendAction(100, """{"channel_id":"$channel","message":"$text"}"""))

// ── 1. Подписка запускается установкой слушателя; CONNECTED доходит ──────

private fun testConnected(cfg: String) {
    val latch = CountDownLatch(1)
    val seen = mutableListOf<String>()
    Photon.create(cfg).use { p ->
        p.setEventListener(listener { json ->
            synchronized(seen) { seen.add(json) }
            if (typeOf(json) == "CONNECTED") latch.countDown()
        })
        val got = latch.await(30, TimeUnit.SECONDS)
        check("установка слушателя запускает подписку: CONNECTED получен", got,
              "за 30 с события нет; получено: ${seen.size}")
        if (got) {
            val first = synchronized(seen) { seen.first() }
            check("event_json содержит поле type", typeOf(first).isNotEmpty(), first.take(80))
        }
    }
}

// ── 2. Порядок и отсутствие параллельности ──────────────────────────────

private fun testOrderAndSerial(cfg: String) {
    val n = 5
    val order = mutableListOf<String>()
    val concurrent = AtomicInteger(0)
    val maxConcurrent = AtomicInteger(0)
    val connected = CountDownLatch(1)
    val messages = CountDownLatch(n)

    Photon.create(cfg).use { p ->
        p.setEventListener(listener { json ->
            val cur = concurrent.incrementAndGet()
            maxConcurrent.updateAndGet { m -> if (cur > m) cur else m }
            // Удержание делает нарушение «не параллельны» наблюдаемым:
            // без паузы гонка могла бы не проявиться.
            Thread.sleep(40)
            if (typeOf(json) == "CONNECTED") connected.countDown()
            if (typeOf(json) == "MESSAGE") {
                synchronized(order) { order.add(json) }
                messages.countDown()
            }
            concurrent.decrementAndGet()
        })
        check("подписка поднялась", connected.await(30, TimeUnit.SECONDS))

        val ch = selfChannel(p)
        val sent = (1..n).map { post(p, ch, "evt-$it") }
        val got = messages.await(60, TimeUnit.SECONDS)
        check("получены все $n событий MESSAGE", got,
              "получено ${n - messages.count} из $n")

        check("медленный слушатель: доставки не параллельны (max=1)",
              maxConcurrent.get() == 1, "наблюдалось ${maxConcurrent.get()}")

        if (got) {
            val arrived = synchronized(order) { order.toList() }
            val ok = sent.indices.all { i -> arrived[i].contains(sent[i]) }
            check("порядок событий совпал с порядком отправки", ok)
        }
        sent.forEach { runCatching { p.sendAction(102, """{"post_id":"$it"}""") } }
    }
}

// ── 3. Исключение слушателя не останавливает поток ──────────────────────

private fun testListenerException(cfg: String) {
    val afterThrow = CountDownLatch(1)
    val threw = AtomicBoolean(false)
    val connected = CountDownLatch(1)

    Photon.create(cfg).use { p ->
        p.setEventListener(listener { json ->
            if (typeOf(json) == "CONNECTED") connected.countDown()
            if (typeOf(json) == "MESSAGE") {
                if (threw.compareAndSet(false, true)) {
                    throw RuntimeException("намеренное исключение слушателя")
                }
                afterThrow.countDown()
            }
        })
        check("подписка поднялась (exception-тест)", connected.await(30, TimeUnit.SECONDS))

        val ch = selfChannel(p)
        val ids = listOf(post(p, ch, "boom-1"), post(p, ch, "boom-2"))
        val survived = afterThrow.await(60, TimeUnit.SECONDS)

        check("исключение слушателя не уронило процесс", true)
        check("после исключения поток событий продолжился", survived,
              "второе событие не пришло за 60 с")
        ids.forEach { runCatching { p.sendAction(102, """{"post_id":"$it"}""") } }
    }
}

// ── 4. close() из тела слушателя: без дедлока ───────────────────────────

private fun testCloseFromCallback(cfg: String) {
    val returned = CountDownLatch(1)
    val p = Photon.create(cfg)
    p.setEventListener(listener {
        // Реентерабельный Photon_Destroy: возврат обязан быть немедленным,
        // иначе насос ждал бы сам себя (event-delivery.md §10.2).
        p.close()
        returned.countDown()
    })
    val ok = returned.await(30, TimeUnit.SECONDS)
    check("close() из тела слушателя вернул управление (нет дедлока)", ok,
          "за 30 с не вернулся")
    runCatching { p.close() }
}

// ── 5. После close() слушатель не вызывается ────────────────────────────

private fun testNoCallbackAfterClose(cfg: String) {
    val connected = CountDownLatch(1)
    val afterClose = AtomicInteger(0)
    val closed = AtomicBoolean(false)

    val p = Photon.create(cfg)
    p.setEventListener(listener { json ->
        if (closed.get()) afterClose.incrementAndGet()
        if (typeOf(json) == "CONNECTED") connected.countDown()
    })
    check("подписка поднялась (after-close тест)", connected.await(30, TimeUnit.SECONDS))

    p.close()
    closed.set(true)
    // Окно, в котором нарушение проявилось бы: подписка была живой, обрыв
    // соединения при уничтожении мог бы породить NETWORK_CHANGED.
    Thread.sleep(3000)
    check("после close() слушатель не вызван ни разу", afterClose.get() == 0,
          "вызовов после close: ${afterClose.get()}")
}

// ── 6. Смена слушателя во время работы ──────────────────────────────────

private fun testReplaceListener(cfg: String) {
    val first = CountDownLatch(1)
    val second = CountDownLatch(1)
    val oldAfterSwap = AtomicInteger(0)
    val swapped = AtomicBoolean(false)

    Photon.create(cfg).use { p ->
        p.setEventListener(listener {
            if (swapped.get()) oldAfterSwap.incrementAndGet()
            first.countDown()
        })
        check("подписка поднялась (replace-тест)", first.await(30, TimeUnit.SECONDS))

        p.setEventListener(listener { second.countDown() })
        swapped.set(true)

        val ch = selfChannel(p)
        val id = post(p, ch, "replaced")
        check("события идут НОВОМУ слушателю", second.await(60, TimeUnit.SECONDS))
        check("старый слушатель после замены не вызывается", oldAfterSwap.get() == 0,
              "вызовов: ${oldAfterSwap.get()}")
        runCatching { p.sendAction(102, """{"post_id":"$id"}""") }
    }
}

// ── 7. setEventListener на закрытом инстансе ────────────────────────────

private fun testSetOnClosed(cfg: String) {
    val p = Photon.create(cfg)
    p.close()
    val threw = runCatching { p.setEventListener(listener { }) }.isFailure
    check("setEventListener на закрытом инстансе бросает, не падая", threw)
}

// ── 8. Жизненный цикл ctx при замене слушателя ──────────────────────────
//
// Оракул — WeakReference. GlobalRef удерживает объект живым, поэтому
// «собран сборщиком» ТОЖДЕСТВЕННО «GlobalRef освобождён»: собственной
// диагностики в native не заводится и ABI не расширяется.
//
// Сеть здесь не нужна: выделение и освобождение контекста — свойство моста,
// а не подписки. Конфигурация без bridge законна (Photon_Create принимает
// "{}"), поэтому эти проверки герметичны.

private fun gcCampaign(rounds: Int = 12) {
    repeat(rounds) {
        System.gc()
        // Давление на кучу: одного System.gc() недостаточно, чтобы считать
        // отсутствие сборки доказательством утечки.
        val junk = ByteArray(1 shl 20); junk[0] = 1
        Thread.sleep(15)
    }
}

private fun testListenerCtxLifetime() {
    val cfg = "{}"

    // (а) замена: прежний GlobalRef жив до close и освобождён после
    run {
        val p = Photon.create(cfg)
        var a: PhotonEventListener? = listener { }
        val weakA = java.lang.ref.WeakReference(a)
        p.setEventListener(a)
        a = null

        p.setEventListener(listener { })
        gcCampaign(3)
        check("до close() GlobalRef удерживает прежнего слушателя",
              weakA.get() != null,
              "объект собран до close — оракул не имел бы зубов")

        p.close()
        gcCampaign()
        check("после close() прежний GlobalRef освобождён", weakA.get() == null,
              "объект не собран: GlobalRef остался")
    }

    // (б) listener → null → listener → close: освобождаются оба контекста
    run {
        val p = Photon.create(cfg)
        var l1: PhotonEventListener? = listener { }
        var l2: PhotonEventListener? = listener { }
        val w1 = java.lang.ref.WeakReference(l1)
        val w2 = java.lang.ref.WeakReference(l2)

        p.setEventListener(l1)
        p.setEventListener(null)     // отцепление: контекст не создаётся
        p.setEventListener(l2)
        l1 = null; l2 = null

        p.close()
        gcCampaign()
        check("listener → null → listener → close: освобождены оба GlobalRef",
              w1.get() == null && w2.get() == null,
              "жив первый=${w1.get() != null}, второй=${w2.get() != null}")
    }

    // (в) массовая замена: контексты копятся, но освобождаются все
    run {
        val n = 200
        val p = Photon.create(cfg)
        val refs = ArrayList<java.lang.ref.WeakReference<PhotonEventListener>>(n)
        repeat(n) {
            val l = listener { }
            refs.add(java.lang.ref.WeakReference(l))
            p.setEventListener(l)
        }
        check("$n замен слушателя подряд выполнены без отказа", true)

        gcCampaign(3)
        val aliveBefore = refs.count { it.get() != null }
        check("до close() удерживаются все $n GlobalRef", aliveBefore == n,
              "живых до close: $aliveBefore из $n — часть освободилась раньше времени")

        p.close()
        gcCampaign()
        val aliveAfter = refs.count { it.get() != null }
        check("после close() освобождены все $n GlobalRef", aliveAfter == 0,
              "осталось живых: $aliveAfter")
    }

    // (г) повторный close идемпотентен и не освобождает контексты дважды
    run {
        val p = Photon.create(cfg)
        p.setEventListener(listener { })
        p.close()
        p.close()
        p.close()
        check("тройной close() после setEventListener: без падения", true)
    }
}

fun main(args: Array<String>) {
    if (args.size < 3) {
        System.err.println("нужно: <libphoton_jni.so> <endpoint> <sessionId>")
        kotlin.system.exitProcess(2)
    }
    System.load(java.io.File(args[0]).absolutePath)
    val cfg = config(args[1], args[2])
    println("Loaded: ${args[0]}  →  ${args[1]}")
    println()

    testConnected(cfg)
    testOrderAndSerial(cfg)
    testListenerException(cfg)
    testCloseFromCallback(cfg)
    testNoCallbackAfterClose(cfg)
    testReplaceListener(cfg)
    testSetOnClosed(cfg)
    testListenerCtxLifetime()

    println()
    println("=== PASSED: $passed, FAILED: $failed ===")
    if (failed > 0) kotlin.system.exitProcess(1)
}
