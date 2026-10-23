package dev.photon.sdk

import dev.photon.core.Photon

/**
 * Клиент Photon — публичная поверхность SDK.
 *
 * Спецификация: `photon-bindings/docs/sdk-api.md` §8 (время жизни колбэка и
 * слушателей) и §9 (действия). Написана до этого кода.
 *
 * Поверх существующего JVM/JNI-моста, не вместо него: маршалинг, загрузка
 * библиотек, коды результата и время жизни handle остаются в
 * `dev.photon.core` (ADR-062 п. 2).
 *
 * Пример:
 * ```
 * dev.photon.core.Photon.loadLibrary()
 * PhotonClient.create(PhotonConfig(BridgeConfig(endpoint = "host:4443"))).use { client ->
 *     client.addEventListener { event -> … }
 *     when (val r = client.sendAction(200, "{}")) {
 *         is PhotonResponse.Ok           -> …
 *         is PhotonResponse.Rejected     -> …
 *         is PhotonResponse.Unrecognized -> …
 *     }
 * }
 * ```
 */
public class PhotonClient internal constructor(private val native: NativeSession) : AutoCloseable {

    private val dispatcher = EventDispatcher()

    /**
     * Замок ТОЛЬКО для флага закрытия и однократной установки колбэка.
     *
     * Отдельный, а не монитор инстанса, по той же причине, по какой в мосте
     * заведён `ctxLock` (`Photon.kt:72-79`): `close()` ждёт возврата из
     * идущего колбэка, и если бы колбэк, обратившийся к клиенту, ждал тот же
     * замок, получился бы дедлок. Поэтому `native.close()` вызывается ВНЕ
     * замка, а `removeEventListener` его не берёт вовсе.
     */
    private val lock = Any()

    @Volatile
    private var closed = false

    private var installed = false

    /**
     * Добавляет слушателя событий.
     *
     * Первый вызов **устанавливает единственный нативный колбэк** и тем
     * самым запускает подписку (§8 L1): установка колбэка — единственный
     * вызов ABI, выражающий намерение получать события. Последующие вызовы
     * границы ABI не пересекают (L2).
     *
     * @throws IllegalStateException если клиент закрыт — всегда, независимо
     *   от того, устанавливался ли колбэк раньше (L7). Без этого правила
     *   поведение зависело бы от истории вызовов.
     */
    public fun addEventListener(listener: PhotonEventListener) {
        synchronized(lock) {
            check(!closed) { "Photon client is closed" }
            dispatcher.add(listener)
            if (!installed) {
                installed = true
                // Ровно одна установка за жизнь клиента. Диспетчер получает
                // сырой конверт и сам решает, кому и что доставить.
                native.setEventListener { eventJson -> dispatcher.dispatch(eventJson) }
            }
        }
    }

    /**
     * Снимает слушателя.
     *
     * Нативного колбэка не касается — включая снятие последнего слушателя
     * (§8 L3): подписку остановить нечем, `NULL` в ABI лишь отцепляет
     * адресата. Доставка в пустой набор законна (L4).
     *
     * После `close()` — **безопасный no-op** (L8): снятие есть уборка, и
     * требовать порядка «сначала отписаться, потом закрыть» значило бы
     * заставлять каждого потребителя оборачивать уборку в `try/catch`.
     */
    public fun removeEventListener(listener: PhotonEventListener) {
        dispatcher.remove(listener)
    }

    /**
     * Выполняет действие Photon Protocol.
     *
     * @param action числовой код из `photon-core/protobuf/photon.proto`
     * @param payloadJson payload строкой
     * @return вердикт сервера: [PhotonResponse.Ok], [PhotonResponse.Rejected]
     *   либо [PhotonResponse.Unrecognized]
     * @throws dev.photon.core.PhotonException если **вызов не состоялся**
     *   (`rc != PHOTON_OK`): нет связи, таймаут, неизвестный код действия.
     *   Отказ сервера исключением НЕ является (ADR-038).
     * @throws IllegalStateException если клиент закрыт
     * @throws IllegalArgumentException для кодов диагностики 819 и 820: они
     *   доступны только через [diagnostics] (ADR-130 §2.8, `sdk-api.md`
     *   §12.1); нативная сессия при этом не вызывается
     *
     * Проверка закрытия — **своя**, а не унаследованная от моста
     * (`Photon.kt:35` делает то же самое). Так контракт SDK не зависит от
     * того, сохранит ли мост своё правило, и сообщение остаётся
     * единообразным с [addEventListener].
     *
     * Гонку это не убирает и не обещает убрать: вызов, начавшийся ДО
     * закрытия и заставший его в полёте, будет отменён и завершится
     * [dev.photon.core.PhotonException] с кодом отказа вызова — конкретный
     * код частью контракта не является (§9).
     */
    public fun sendAction(action: Int, payloadJson: String): PhotonResponse {
        check(!closed) { "Photon client is closed" }
        require(!DiagnosticsCodec.isDiagnostic(action)) {
            "action $action is diagnostics: use PhotonClient.diagnostics()"
        }
        return ResponseParser.parse(native.sendAction(action, payloadJson))
    }

    // ── Диагностика инстанса: 819, 820 (ADR-130) ────────────────────────

    /**
     * Диагностика инстанса: снимок состояния и журнал событий ядра
     * (`sdk-api.md` §12). Единственный путь к кодам 819/820 — общий
     * [sendAction] их отвергает.
     *
     * @throws IllegalStateException если клиент закрыт
     */
    public fun diagnostics(): PhotonDiagnostics {
        check(!closed) { "Photon client is closed" }
        return PhotonDiagnostics(this)
    }

    /** Путь 819/820 в обход запрета [sendAction] — только для [PhotonDiagnostics]. */
    internal fun sendDiagnostic(action: Int, payloadJson: String): PhotonResponse {
        check(!closed) { "Photon client is closed" }
        check(DiagnosticsCodec.isDiagnostic(action)) { "not a diagnostics action: $action" }
        return ResponseParser.parse(native.sendAction(action, payloadJson))
    }

    // ── Локальная лента: 801, 802, 803 (ADR-066) ────────────────────────

    /*
     * Три типизированных примитива. Действие 800 (страница) СРЕДИ НИХ НЕТ:
     * оно остаётся доступным через [sendAction], и ADR-066 п. 3.6 говорит об
     * этом прямо. Цикл догрузки у вызывающего собирается из трёх
     * типизированных вызовов и одного общего.
     *
     * Цикла синхронизации здесь нет и не появляется: он живёт у приложения
     * (ADR-066 §9). SDK даёт примитивы, последовательность вызовов,
     * повторение и сходимость — забота вызывающего.
     */

    /**
     * Кладёт записи в локальную ленту канала (`ACTION_LOCAL_FEED_PUT`).
     *
     * **О непрерывности не утверждает ничего** и сегментов не создаёт: «эти
     * пять записей» и «эти пять — всё, что было между X и Y» — разные
     * утверждения, и второе делает [localFeedIngest].
     *
     * @throws IllegalArgumentException если `body` какой-либо записи не
     *   является валидным JSON: payload из него собрать нечем
     * @throws dev.photon.core.PhotonException если вызов не состоялся — в том
     *   числе когда у инстанса нет секции `storage` либо блок отвергнут
     *   инвариантами ядра (ADR-065 п. 3). Причина приходит общим кодом и
     *   текстом; типизации причин нет (ADR-066 §8)
     * @throws IllegalStateException если клиент закрыт
     */
    public fun localFeedPut(channelId: String, records: List<FeedRecord>): FeedPutResult =
        LocalFeedCodec.parsePut(
            sendAction(LocalFeedCodec.ACTION_PUT, LocalFeedCodec.putPayload(channelId, records))
        )

    /**
     * Заливает блок и то, что этот блок доказал о полноте
     * (`ACTION_LOCAL_FEED_INGEST`).
     *
     * **Полноту диапазона утверждает вызывающий.** Core проверяет структуру и
     * внутреннюю непротиворечивость блока, но не истинность утверждения о
     * том, что сервер вернул диапазон целиком; SDK этого утверждения в
     * гарантию не превращает. То, что блок собрался в Kotlin, не означает,
     * что диапазон полон (ADR-066 §10).
     *
     * @throws IllegalArgumentException если `body` какой-либо записи не
     *   является валидным JSON
     * @throws dev.photon.core.PhotonException если вызов не состоялся
     * @throws IllegalStateException если клиент закрыт
     */
    public fun localFeedIngest(channelId: String, block: FeedBlock): FeedIngestResult =
        LocalFeedCodec.parseIngest(
            sendAction(LocalFeedCodec.ACTION_INGEST, LocalFeedCodec.ingestPayload(channelId, block))
        )

    /**
     * Позиция, от которой закрывать самый верхний пропуск
     * (`ACTION_LOCAL_FEED_GAP_ANCHOR`).
     *
     * Производная от сегментов, вычисляемая ядром при каждом вызове: своего
     * состояния SDK не заводит и второго источника истины не создаёт.
     *
     * @throws dev.photon.core.PhotonException если вызов не состоялся
     * @throws IllegalStateException если клиент закрыт
     */
    public fun localFeedGapAnchor(channelId: String): FeedGapAnchor =
        LocalFeedCodec.parseGapAnchor(
            sendAction(LocalFeedCodec.ACTION_GAP_ANCHOR, LocalFeedCodec.gapAnchorPayload(channelId))
        )

    /**
     * Закрывает клиента. Идемпотентен (§8 L10).
     *
     * Порядок обязателен и не подлежит перестановке: сначала
     * `Photon_Destroy` — только после его возврата C ABI гарантирует, что
     * колбэк не будет вызван ни разу, — и лишь затем очистка набора
     * слушателей (L9). Очистка раньше означала бы, что идущая доставка
     * теряет адресатов, которым событие обещано снимком.
     */
    override fun close() {
        synchronized(lock) {
            if (closed) return
            closed = true
        }
        // ВНЕ замка: Photon_Destroy ждёт возврата из идущего колбэка, и
        // слушатель, обратившийся в это время к клиенту, не должен упереться
        // в замок, который держит закрывающий поток.
        native.close()
        dispatcher.clear()
    }

    /** Число зарегистрированных слушателей — для тестов и диагностики. */
    internal fun listenerCount(): Int = dispatcher.count()

    /** Установлен ли нативный колбэк. */
    internal fun callbackInstalled(): Boolean = synchronized(lock) { installed }

    public companion object {
        /**
         * Создаёт клиента.
         *
         * Нативные библиотеки должны быть загружены заранее
         * (`dev.photon.core.Photon.loadLibrary()` либо `loadFrom`): загрузку
         * SDK на себя не берёт — это свойство процесса, а не клиента.
         *
         * Сети этот вызов не касается: `Photon_Create` не подключается, а
         * подписку запускает первый [addEventListener].
         *
         * @throws dev.photon.core.PhotonException если создание не удалось
         */
        @JvmStatic
        public fun create(config: PhotonConfig): PhotonClient =
            createWith(config) { json -> BridgeSession(Photon.create(json)) }

        /**
         * То же создание, но с подставляемым источником сессии.
         *
         * Существует ради тестов: путь «конфигурация → `config_json` →
         * инстанс» проверяется без нативной библиотеки. Публичной
         * поверхностью не является.
         */
        internal fun createWith(config: PhotonConfig, open: (String) -> NativeSession): PhotonClient =
            PhotonClient(open(config.toJson()))
    }
}
