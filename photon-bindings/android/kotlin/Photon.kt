package dev.photon.core

/**
 * Идиоматичная Kotlin-обёртка над Photon C ABI v1.
 *
 * Границы ответственности:
 *  - [PhotonNative] — тонкий JNI-мост, повторяет C ABI один в один.
 *  - Этот класс — только Kotlin-удобства: типизированные результаты,
 *    исключения вместо кодов, [AutoCloseable], безопасность повторного
 *    close.
 *
 * Чего этот класс НЕ делает (сознательно):
 *  - не управляет памятью Photon — этим занимается C ABI
 *    (Photon_Destroy / Photon_Free*), см. ADR-028;
 *  - не знает, что находится внутри handle (cgo.Handle, ячейка
 *    C-памяти или иное) — для Kotlin это непрозрачный Long;
 *  - не кэширует и не интерпретирует payload/response — только
 *    передаёт JSON насквозь.
 */
class Photon private constructor(private var handle: Long) : java.io.Closeable {

    @Volatile
    private var closed = false

    /**
     * Выполняет действие Photon Protocol.
     *
     * @param action     код из enum Action (photon-core/protobuf)
     * @param payloadJson payload в JSON
     * @return JSON-ответ
     * @throws PhotonException при любом результате, отличном от PHOTON_OK
     * @throws IllegalStateException если инстанс уже закрыт
     */
    fun sendAction(action: Int, payloadJson: String): String {
        check(!closed) { "Photon instance is closed" }

        val response = PhotonNative.sendAction(handle, action, payloadJson)
        val rc = PhotonNative.lastSendActionResult()
        if (rc != PhotonNative.PHOTON_OK) {
            throw PhotonException(rc, PhotonNative.getLastError(handle))
        }
        return response ?: ""
    }

    /**
     * Текст последней ошибки на этом инстансе, либо null.
     * Для закрытого инстанса всегда null.
     */
    fun lastError(): String? =
        if (closed) null else PhotonNative.getLastError(handle)

    /**
     * Освобождает инстанс. Идемпотентен: повторный вызов безопасен —
     * как из-за флага [closed] здесь, так и из-за гарантии C ABI
     * (Photon_Destroy на уже уничтоженном handle — no-op, ADR-028).
     * Двойная защита сознательна: Kotlin-флаг документирует намерение,
     * гарантия C ABI страхует от гонок между потоками.
     */
    @Synchronized
    override fun close() {
        if (closed) return
        closed = true
        PhotonNative.destroy(handle)
        handle = 0
    }

    companion object {
        /** MAJOR-версия ABI, с которой собран этот Kotlin-слой. */
        const val EXPECTED_ABI_MAJOR = 1

        /**
         * Загружает нативные библиотеки. На Android — по имени
         * (System.loadLibrary), на desktop-JVM для тестов удобнее
         * [loadFrom] с абсолютным путём.
         */
        @JvmStatic
        fun loadLibrary(name: String = "photon_jni") {
            System.loadLibrary(name)
        }

        /** Загружает библиотеку по абсолютному пути (тесты, desktop). */
        @JvmStatic
        fun loadFrom(absolutePath: String) {
            System.load(absolutePath)
        }

        /** Упакованная версия ABI: (MAJOR shl 16) or MINOR. */
        @JvmStatic
        fun abiVersion(): Int = PhotonNative.getAbiVersion()

        @JvmStatic
        fun abiMajor(): Int = abiVersion() ushr 16

        @JvmStatic
        fun abiMinor(): Int = abiVersion() and 0xFFFF

        /**
         * Создаёт инстанс Photon.
         *
         * Перед созданием сверяет MAJOR-версию ABI: несовпадение
         * означает, что нативная библиотека несовместима с этим
         * Kotlin-слоем, и продолжать нельзя (ADR-026, п.4 — MAJOR
         * обязателен к проверке клиентом, MINOR можно игнорировать).
         *
         * @throws PhotonException если создание не удалось
         * @throws IllegalStateException при несовпадении MAJOR ABI
         */
        @JvmStatic
        fun create(configJson: String): Photon {
            val major = abiMajor()
            check(major == EXPECTED_ABI_MAJOR) {
                "Photon ABI major mismatch: native=$major, expected=$EXPECTED_ABI_MAJOR"
            }

            val handle = PhotonNative.create(configJson)
            val rc = PhotonNative.lastCreateResult()
            if (rc != PhotonNative.PHOTON_OK || handle == 0L) {
                // Photon_GetLastError требует существующий handle,
                // которого при неудачном create нет (ADR-027) — поэтому
                // здесь доступен только код результата, без текста.
                throw PhotonException(rc, null)
            }
            return Photon(handle)
        }
    }
}

/**
 * Ошибка Photon. [code] — значение PhotonResult из photon_core.h.
 */
class PhotonException(
    val code: Int,
    val nativeMessage: String?
) : RuntimeException(buildMessage(code, nativeMessage)) {

    companion object {
        private fun buildMessage(code: Int, nativeMessage: String?): String {
            val name = codeName(code)
            return if (nativeMessage.isNullOrEmpty()) {
                "Photon error: $name ($code)"
            } else {
                "Photon error: $name ($code): $nativeMessage"
            }
        }

        fun codeName(code: Int): String = when (code) {
            PhotonNative.PHOTON_OK -> "PHOTON_OK"
            PhotonNative.PHOTON_ERR_TIMEOUT -> "PHOTON_ERR_TIMEOUT"
            PhotonNative.PHOTON_ERR_INVALID_ARGUMENT -> "PHOTON_ERR_INVALID_ARGUMENT"
            PhotonNative.PHOTON_ERR_PERMISSION_DENIED -> "PHOTON_ERR_PERMISSION_DENIED"
            PhotonNative.PHOTON_ERR_NOT_FOUND -> "PHOTON_ERR_NOT_FOUND"
            PhotonNative.PHOTON_ERR_NOT_CONNECTED -> "PHOTON_ERR_NOT_CONNECTED"
            PhotonNative.PHOTON_ERR_ALREADY_EXISTS -> "PHOTON_ERR_ALREADY_EXISTS"
            PhotonNative.PHOTON_ERR_INTERNAL -> "PHOTON_ERR_INTERNAL"
            PhotonNative.PHOTON_ERR_ABI_MISMATCH -> "PHOTON_ERR_ABI_MISMATCH"
            PhotonNative.PHOTON_ERR_INVALID_ACTION -> "PHOTON_ERR_INVALID_ACTION"
            else -> "UNKNOWN"
        }
    }
}
