package dev.photon.sdk

/**
 * Событие Photon в типизированном виде.
 *
 * Спецификация — `photon-bindings/docs/sdk-api.md` §1. Три подтипа отвечают
 * трём значениям, которые эмитирует ядро; всё остальное приходит как
 * [Unknown].
 *
 * **Появление нового типа события в ядре не требует обновления SDK и не
 * ломает существующий обработчик:** новое значение `"type"` становится
 * [Unknown], нового Kotlin-подтипа при этом не возникает, и исчерпывающий
 * `when` у потребителя остаётся исчерпывающим. Подтипы для зарезервированных
 * `PLUGIN_EVENT` и `STORAGE_CHANGED` (`photon_core.h:346-350`) намеренно НЕ
 * заведены: их полей никто не наблюдал, и придуманная форма была бы
 * обещанием за ядро.
 *
 * [raw] есть у каждого подтипа и равен исходному `event_json` байт в байт.
 * Это единственное, что делает [Unknown] пригодным к употреблению, и
 * страховка на случай, когда типизация чего-то не довезла.
 */
public sealed interface PhotonEvent {

    /** Исходный `event_json`, как его отдал C ABI. */
    public val raw: String

    /**
     * Подписное соединение подтверждено сервером.
     *
     * Полей нет, потому что ядро их не пишет (`events.go:36-38`). Относится
     * ТОЛЬКО к подписному соединению: action-соединение подключается лениво
     * и своего фонового жизненного цикла не имеет.
     */
    public data class Connected(override val raw: String) : PhotonEvent

    /**
     * Входящее сообщение (push от Bridge).
     *
     * @property event строка с провода Bridge (`"posted"` и подобные), НЕ
     *   перечисление: доменный словарь принадлежит Bridge, и ни ядро, ни SDK
     *   его не знают. Незнакомое значение остаётся [Message], а не
     *   становится [Unknown] — [Unknown] описывает неизвестный тип КОНВЕРТА.
     * @property userId обычно `null`: сервер объявил поле с `omitempty` и
     *   заполняет его один хук из восьми.
     * @property data сырой JSON-текст полезной нагрузки, дословно, как он
     *   пришёл. Не разобранный объект: доменную форму Mattermost SDK не
     *   знает (ADR-062 п. 5).
     */
    public data class Message(
        public val event: String,
        public val userId: String?,
        public val data: String?,
        override val raw: String,
    ) : PhotonEvent

    /**
     * Состояние подписного соединения изменилось.
     *
     * @property state единственное значение на сегодня — `"disconnected"`.
     *   Строка, а не перечисление: словарь состояний ядра может вырасти.
     */
    public data class NetworkChanged(
        public val state: String,
        override val raw: String,
    ) : PhotonEvent

    /**
     * Конверт, который SDK не распознал.
     *
     * @property type значение поля `"type"`, если его удалось прочитать, и
     *   `null`, если конверт не разобрался до этого поля. Различие
     *   существенно: непустая строка означает «тип известен ядру, но не
     *   этой версии SDK», `null` — «прочитать не удалось вовсе».
     */
    public data class Unknown(
        public val type: String?,
        override val raw: String,
    ) : PhotonEvent
}
