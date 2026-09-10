package dev.photon.core;

/**
 * Тонкий JNI-мост к Photon C ABI v1 (photon_core.h).
 *
 * <p>Правило границы (ARCHITECTURE.md, раздел 7): этот класс — и стоящий
 * за ним нативный код — обращаются ИСКЛЮЧИТЕЛЬНО к функциям
 * {@code Photon_*} из photon_core.h через libphoton_core.so. Никаких
 * обращений к Go-символам, внутренним структурам photon-core или
 * деталям реализации cgo. Именно эта граница проверена тестами
 * bindings/c (32 теста, race detector, leak-check), и обход её обнулил
 * бы всю проверку.
 *
 * <p>Ownership: Photon владеет всеми ресурсами, на которые ссылается
 * handle. JNI ничего не освобождает самостоятельно — только передаёт
 * вызовы в C ABI, где освобождение делает {@code Photon_Destroy} /
 * {@code Photon_Free*}. Java-сторона видит handle как непрозрачный
 * {@code long} и не знает (и не должна знать), что внутри — cgo.Handle,
 * ячейка C-памяти или что-либо иное.
 *
 * <p>Прикладной код должен работать через Kotlin-обёртку {@code Photon},
 * а не вызывать эти native-методы напрямую.
 */
public final class PhotonNative {

    private PhotonNative() {
        // Утилитарный класс, экземпляры не нужны.
    }

    // ── ABI versioning ──────────────────────────────────────────────

    /**
     * @return упакованная версия ABI: (MAJOR &lt;&lt; 16) | MINOR.
     */
    public static native int getAbiVersion();

    // ── Жизненный цикл ──────────────────────────────────────────────

    /**
     * Создаёт инстанс Photon.
     *
     * @param configJson конфигурация в JSON
     * @return handle (&ne; 0) при успехе, 0 при ошибке. Ноль возвращается
     *         именно потому, что C ABI гарантирует NULL в *handle при
     *         любом результате, отличном от PHOTON_OK (ADR-027/028) —
     *         Java-сторона транслирует эту гарантию в 0.
     */
    public static native long create(String configJson);

    /**
     * Код результата последнего вызова {@link #create(String)} в этом
     * потоке. Отдельный метод, потому что create возвращает handle, а не
     * PhotonResult, а знать точную причину ошибки инициализации нужно
     * ({@code Photon_GetLastError} требует уже существующий handle,
     * которого при неудачном create нет).
     */
    public static native int lastCreateResult();

    /**
     * Уничтожает инстанс. Безопасно вызывать с handle == 0 и повторно
     * на уже уничтоженном handle — C ABI гарантирует no-op (ADR-028).
     */
    public static native void destroy(long handle);

    // ── Photon Protocol ─────────────────────────────────────────────

    /**
     * Выполняет действие.
     *
     * @param handle      handle из {@link #create(String)}
     * @param actionCode  код из enum Action (photon-core/protobuf)
     * @param payloadJson payload в JSON
     * @return JSON-ответ при успехе, либо null. Код результата —
     *         через {@link #lastSendActionResult()}.
     */
    public static native String sendAction(long handle, int actionCode, String payloadJson);

    /**
     * Код результата последнего вызова {@link #sendAction} в этом потоке.
     */
    public static native int lastSendActionResult();

    // ── Диагностика ─────────────────────────────────────────────────

    /**
     * @return текст последней ошибки на этом handle, либо null.
     */
    public static native String getLastError(long handle);

    // ── События ─────────────────────────────────────────────────────

    /**
     * Устанавливает слушателя событий и ЗАПУСКАЕТ подписку: это единственный
     * вызов ABI, выражающий намерение получать события ({@code Photon_Create}
     * сети не касается). Повторный вызов заменяет адресата, {@code null}
     * отцепляет его, подписку не останавливая.
     *
     * @return указатель на контекст доставки. Вызывающий обязан передать его
     *         в {@link #releaseEventListener(long)} СТРОГО ПОСЛЕ возврата из
     *         {@link #destroy(long)}: до этого момента доставка может идти с
     *         этим самым контекстом. Для {@code listener == null} — 0.
     */
    public static native long setEventListener(long handle, PhotonEventListener listener);

    /**
     * Освобождает контекст, полученный от {@link #setEventListener}.
     *
     * Безопасно только после возврата из {@link #destroy(long)} — тогда C ABI
     * уже гарантировал, что колбэк не будет вызван ни разу и ни из какого
     * потока. Вызов с 0 — no-op.
     */
    public static native void releaseEventListener(long ctx);

    // ── Коды результата (зеркалят PhotonResult из photon_core.h) ────

    public static final int PHOTON_OK = 0;
    public static final int PHOTON_ERR_TIMEOUT = 1;
    public static final int PHOTON_ERR_INVALID_ARGUMENT = 2;
    public static final int PHOTON_ERR_PERMISSION_DENIED = 3;
    public static final int PHOTON_ERR_NOT_FOUND = 4;
    public static final int PHOTON_ERR_NOT_CONNECTED = 5;
    public static final int PHOTON_ERR_ALREADY_EXISTS = 6;
    public static final int PHOTON_ERR_INTERNAL = 7;
    public static final int PHOTON_ERR_ABI_MISMATCH = 8;
    public static final int PHOTON_ERR_INVALID_ACTION = 9;
}
