package dev.photon.sdk

import dev.photon.core.PhotonException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Фиктивная сессия вместо нативной.
 *
 * Воспроизводит те свойства ABI, которые уже доказаны живым прогоном
 * 2026-09-18 и на которые опирается §8:
 *  - `setEventListener` заменяет единственного адресата;
 *  - `close()` идемпотентен и ЖДЁТ завершения идущей доставки;
 *  - `sendAction` бросает `PhotonException` при `rc != PHOTON_OK`.
 *
 * Нужна не «вместо живого прогона», а чтобы правила §8–§9 проверялись при
 * каждом `gradle test`, а не изредка против сервера.
 */
internal class FakeNativeSession : NativeSession {

    /** Сколько раз ставился нативный колбэк. Ключевое наблюдение §8 L1–L2. */
    val installs = AtomicInteger(0)

    /** Последний установленный адресат, null — снят. */
    private val callback = AtomicReference<dev.photon.core.PhotonEventListener?>(null)

    val closes = AtomicInteger(0)
    val actions = CopyOnWriteArrayList<Pair<Int, String>>()

    /** Ответ на следующий sendAction либо исключение. */
    @Volatile var nextResponse: String = """{"ok":true}"""
    @Volatile var nextThrow: RuntimeException? = null

    /** Признак «идёт доставка» — им close() и ждёт, как настоящий Destroy. */
    private val inDelivery = AtomicInteger(0)

    /** Если задан, close() ждёт его, имитируя ожидание колбэка. */
    @Volatile var closeGate: CountDownLatch? = null

    @Volatile var closedAtListenerCount: Int = -1

    override fun sendAction(action: Int, payloadJson: String): String {
        actions.add(action to payloadJson)
        nextThrow?.let { throw it }
        return nextResponse
    }

    override fun setEventListener(listener: dev.photon.core.PhotonEventListener?) {
        installs.incrementAndGet()
        callback.set(listener)
    }

    override fun close() {
        closes.incrementAndGet()
        // Настоящий Photon_Destroy возвращается только после завершения
        // идущего колбэка — воспроизводим это, иначе тест на дедлок ничего
        // бы не проверял.
        closeGate?.await()
        while (inDelivery.get() > 0) Thread.sleep(5)
    }

    /** Доставка события так, как это делает насос ядра. */
    fun emit(eventJson: String) {
        val cb = callback.get() ?: return
        inDelivery.incrementAndGet()
        try {
            cb.onEvent(eventJson)
        } finally {
            inDelivery.decrementAndGet()
        }
    }

    fun hasCallback(): Boolean = callback.get() != null

    companion object {
        fun photonException(code: Int, message: String?) = PhotonException(code, message)
    }
}
