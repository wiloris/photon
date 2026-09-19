package dev.photon.sdk

import dev.photon.core.Photon

/**
 * Узкий шов вокруг существующего JVM/JNI-моста.
 *
 * **Второй реализации JNI не появляется** (ADR-062 п. 2): единственная
 * реализация — [BridgeSession], и она только делегирует. Интерфейс нужен
 * ради одного: семантику §8 и §9 можно проверить тестами без нативной
 * библиотеки, а значит проверять её будут при каждом прогоне, а не изредка
 * против живого сервера.
 *
 * `internal` намеренно: публичная поверхность SDK перечислена в §4 списка
 * задач и этого типа не содержит.
 */
internal interface NativeSession {
    /**
     * Действие. Бросает `dev.photon.core.PhotonException` при
     * `rc != PHOTON_OK` — ровно так, как это делает мост.
     */
    fun sendAction(action: Int, payloadJson: String): String

    /** Устанавливает единственный нативный колбэк инстанса. */
    fun setEventListener(listener: dev.photon.core.PhotonEventListener?)

    /** `Photon_Destroy`: ждёт завершения идущего колбэка, идемпотентен. */
    fun close()
}

/** Единственная настоящая реализация: делегирование, без собственной логики. */
internal class BridgeSession(private val photon: Photon) : NativeSession {

    override fun sendAction(action: Int, payloadJson: String): String =
        photon.sendAction(action, payloadJson)

    override fun setEventListener(listener: dev.photon.core.PhotonEventListener?) {
        photon.setEventListener(listener)
    }

    override fun close() {
        photon.close()
    }
}
