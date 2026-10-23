package dev.photon.sdk

/**
 * Типы локальной ленты (ADR-066 §§3–7).
 *
 * Формы повторяют принятые **ADR-065 п. 2** один в один и ничего к ним не
 * добавляют. Доменной модели здесь нет и быть не может: `body` — непрозрачный
 * JSON, и ни `Post`, ни ENGRAM SDK не знает (ADR-062 п. 5, ADR-034).
 *
 * Типы сгруппированы в одном файле по образцу [PhotonConfig], где рядом живут
 * `BridgeConfig`, `TlsConfig` и `StorageConfig`: у них один предмет и один
 * повод меняться.
 *
 * **Что эти типы НЕ делают.** Они не проверяют полноту и не превращают
 * утверждение вызывающего в гарантию. Невыразимость противоречивой пары
 * «направление + якорь» — construction safety, а не валидация: арбитром
 * остаётся Core, и payload, собранный мимо SDK, он отвергнет так же
 * (ADR-066 §4.3).
 */

/**
 * Позиция записи в ленте — пара, замороженная **ADR-052**.
 *
 * @property orderKey ключ порядка у источника. `Long`, а не число с плавающей
 *   точкой: миллисекунды эпохи и любое значение выше 2^53 обязаны доезжать без
 *   искажения (ADR-065 п. 2.2).
 * @property id идентификатор записи у источника. Второй компонент ключа:
 *   `orderKey` не уникален.
 */
public data class FeedPosition(
    public val orderKey: Long,
    public val id: String,
)

/**
 * Запись локальной ленты.
 *
 * @property id идентификатор у источника. Пустым быть не может — это отвергнет
 *   Core (ADR-065 п. 3.6); SDK такой записи не строит и проверку не дублирует.
 * @property orderKey ключ порядка, сообщённый вызывающим.
 * @property version версия записи у источника. Меньшая, чем локальная, не
 *   применяется — это решает Core (ADR-053).
 * @property body **непрозрачный JSON-текст**. SDK его не разбирает и схемы не
 *   вводит; проверяется ровно одно — что это синтаксически JSON, иначе payload
 *   собрать нечем.
 *
 *   Незначащее форматирование (пробелы, переводы строк) при сборке payload
 *   нормализуется деревом kotlinx. **Значения не меняются**: числа переносятся
 *   литералом, порядок ключей сохраняется. Побайтовое равенство тела
 *   гарантируется хранилищем и чтением, а не отправкой.
 */
public data class FeedRecord(
    public val id: String,
    public val orderKey: Long,
    public val version: Long,
    public val body: String,
)

/**
 * Блок заливки — **утверждение о полноте**, а не просто набор записей
 * (ADR-065 п. 2).
 *
 * Направление и якорь выражены **взаимоисключающими путями** (ADR-066 §4):
 * «самые свежие» якоря не имеют по построению, «после» и «до» — требуют его
 * обязательно. Противоречивая пара `Newest` + якорь невыразима, и это
 * единственный инвариант ADR-065, который SDK повторяет, — повторяет формой,
 * а не второй проверкой.
 *
 * Та же форма, что у [PhotonConfig]: два взаимоисключающих пути вместо одной
 * перегрузки с правилами приоритета.
 *
 * [atEnd] **обязателен во всех трёх подтипах и умолчания не имеет**. Это не
 * строгость ради строгости: «сервер вернул меньше запрошенного» знает только
 * тот, кто знал запрошенное, и вызывающий отвечает за истинность этого
 * утверждения (ADR-066 §10). Умолчание `false` молча превратило бы забытый
 * параметр в утверждение.
 */
public sealed interface FeedBlock {

    /** Записи блока. Порядок безразличен: Core берёт крайние позиции. */
    public val records: List<FeedRecord>

    /** Сервер вернул МЕНЬШЕ запрошенного, то есть блок дошёл до края. */
    public val atEnd: Boolean

    /** Ответ на запрос без курсора — «самые свежие». Якоря не имеет. */
    public data class Newest(
        override val records: List<FeedRecord>,
        override val atEnd: Boolean,
    ) : FeedBlock

    /**
     * Ответ на запрос «новее якоря».
     *
     * Записи старше якоря Core отвергнет (ADR-065 п. 3.9); равная якорю
     * запись законна — её включение в блок обязательно, когда сегмент не
     * должен обещать полноту до позиции, записи которой в хранилище нет.
     */
    public data class After(
        public val anchor: FeedPosition,
        override val records: List<FeedRecord>,
        override val atEnd: Boolean,
    ) : FeedBlock

    /** Ответ на запрос «старше якоря». Зеркально [After]. */
    public data class Before(
        public val anchor: FeedPosition,
        override val records: List<FeedRecord>,
        override val atEnd: Boolean,
    ) : FeedBlock
}

/*
 * Результаты трёх действий.
 *
 * У каждого — sealed-иерархия с открытым дном и ссылкой на исходный
 * [PhotonResponse]. Это тот же приём, что у [PhotonEvent] и [PhotonResponse]:
 * типизированная модель плюс страховка на случай, когда типизация чего-то не
 * довезла.
 *
 * Второй таксономии конверта при этом НЕ заводится. `ok:false` и
 * неразобранный конверт остаются различимы через `response`, потому что
 * различает их уже [PhotonResponse]; копировать сюда `Rejected`/`Unrecognized`
 * значило бы завести второе место, где описан один и тот же конверт.
 *
 * Локальные действия сегодня `ok:false` не возвращают — вердикта у локальной
 * операции не бывает, она либо выполнилась, либо отказала вызовом. Но
 * «сегодня не бывает» в тип не закладывается: ADR-038 описывает конверт, а не
 * подмножество действий.
 */

/** Результат `ACTION_LOCAL_FEED_PUT`. */
public sealed interface FeedPutResult {

    /** Ответ целиком, как его вернул `sendAction`. */
    public val response: PhotonResponse

    /**
     * Записи положены.
     *
     * @property stored сколько записей принято. `Put` версий не сверяет и
     *   пишет каждую, поэтому число равно числу отправленных.
     */
    public data class Stored(
        public val stored: Int,
        override val response: PhotonResponse,
    ) : FeedPutResult

    /**
     * Типизированного результата нет: конверт не `ok:true` либо `data` не
     * разобралась. Причина различима по [response] — не исключение, потому
     * что вызов состоялся.
     */
    public data class Unavailable(override val response: PhotonResponse) : FeedPutResult
}

/** Результат `ACTION_LOCAL_FEED_INGEST`. */
public sealed interface FeedIngestResult {

    /** Ответ целиком, как его вернул `sendAction`. */
    public val response: PhotonResponse

    /**
     * Блок применён. **Семь полей — все, что вычислил Core**, без отбора:
     * отбор означал бы решение за приложение, какие из уже посчитанных чисел
     * ему нужны (ADR-066 §6.3).
     *
     * @property stored сколько записей записано.
     * @property segments сколько непрерывных участков у канала ПОСЛЕ слияния.
     * @property updated пришла версия старше локальной, тело перезаписано.
     * @property unchanged версия совпала.
     * @property stale пришла версия МЛАДШЕ локальной — запись НЕ применена.
     *   Не ошибка: защита от переупорядоченной доставки (ADR-053).
     * @property vanished локальные записи внутри диапазона блока, которых в
     *   блоке нет.
     *
     *   **Только сигнал.** Ни удаления, ни требования его выполнить отсюда не
     *   следует: политика закрыта ADR-054 и ADR-060 (вариант A) —
     *   автоматического удаления не происходит никогда.
     * @property gapsClosed на сколько пропусков стало меньше.
     *
     *   **Знаковое число.** Отрицательное значение — законный результат,
     *   означающий появление НОВОГО пропуска: так выглядит первый шаг
     *   возвращения после offline. Приводить его к нулю нельзя; прежний
     *   комментарий ядра утверждал, что отрицательным оно не бывает, и был
     *   опровергнут первым же живым прогоном.
     */
    public data class Applied(
        public val stored: Int,
        public val segments: Int,
        public val updated: Int,
        public val unchanged: Int,
        public val stale: Int,
        public val vanished: Int,
        public val gapsClosed: Int,
        override val response: PhotonResponse,
    ) : FeedIngestResult

    /** Типизированного результата нет — см. [FeedPutResult.Unavailable]. */
    public data class Unavailable(override val response: PhotonResponse) : FeedIngestResult
}

/**
 * Результат `ACTION_LOCAL_FEED_GAP_ANCHOR`.
 *
 * Позиция, от которой закрывать САМЫЙ ВЕРХНИЙ пропуск, — **производная** от
 * сегментов, вычисляемая при каждом вызове. Persisted watermark отсюда не
 * возникает, второго источника истины не появляется (ADR-066 §7).
 */
public sealed interface FeedGapAnchor {

    /** Ответ целиком, как его вернул `sendAction`. */
    public val response: PhotonResponse

    /**
     * Пропуск есть, и вот позиция для его закрытия.
     *
     * Это `Newest` сегмента, лежащего ПОД пропуском, а не самая свежая
     * локальная запись: запрос от неё вернул бы уже известное и оставил дыру
     * навсегда.
     */
    public data class Found(
        public val anchor: FeedPosition,
        override val response: PhotonResponse,
    ) : FeedGapAnchor

    /**
     * Пропусков нет.
     *
     * Отдельный подтип, а не нулевая позиция: нулевая предложила бы закрывать
     * пропуск от начала ленты. И не ошибка — «лента непрерывна» законный
     * ответ.
     */
    public data class None(override val response: PhotonResponse) : FeedGapAnchor

    /** Типизированного результата нет — см. [FeedPutResult.Unavailable]. */
    public data class Unavailable(override val response: PhotonResponse) : FeedGapAnchor
}
