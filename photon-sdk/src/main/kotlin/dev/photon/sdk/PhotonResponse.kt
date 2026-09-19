package dev.photon.sdk

/**
 * Ответ на действие (`sdk-api.md` §9).
 *
 * Две оси ADR-038 выражены двумя разными механизмами, а не одним
 * перечислением: исключение отвечает на вопрос «состоялся ли вызов»,
 * [PhotonResponse] — «что сказал сервер». Отдельного `Outcome`/`CallResult`
 * в SDK нет.
 *
 * Отказ сервера (`ok:false`) — **нормальный ответ состоявшегося вызова**, а
 * не исключение: кодов ошибок на проводе не существует, и вернуть отказ
 * кодом значило бы потерять тело вместе с единственной причиной отказа
 * (ADR-038 п. 2).
 *
 * [raw] есть у каждого подтипа и равен исходному конверту байт в байт.
 */
public sealed interface PhotonResponse {

    /** Конверт ответа целиком, как его вернул C ABI. */
    public val raw: String

    /**
     * `ok:true`.
     *
     * @property data сырой JSON-текст поля `data`, дословно, или `null`, если
     *   поля нет. Не разобранный объект: доменную форму SDK не знает
     *   (ADR-062 п. 5) — то же правило, что для `MESSAGE.data` (§1).
     *
     * Внимание: `ok:true` **не означает**, что операция выполнена полностью.
     * `getUsersByIds`, `batchStore` и `batchQuery` отвечают успехом при
     * частичном отказе, пряча отказавшую часть внутрь `data`
     * (`photon_core.h:422-426`).
     */
    public data class Ok(
        public val data: String?,
        override val raw: String,
    ) : PhotonResponse

    /**
     * `ok:false` — вызов состоялся, сервер отказал.
     *
     * @property error текст `error`, **как есть**. По тексту ничего не
     *   классифицируется (ADR-033); `null` — поле отсутствует, провод его
     *   наличия не гарантирует.
     * @property data сырой JSON-текст `data`, если сервер его приложил.
     */
    public data class Rejected(
        public val error: String?,
        public val data: String?,
        override val raw: String,
    ) : PhotonResponse

    /**
     * Конверт без читаемого булева `ok`: не объект, не разбирается, поле
     * отсутствует либо не булево.
     *
     * Не `Ok`: испорченный ответ, засчитанный успехом, — то же, что §2
     * запретил для событий. Не исключение: вызов состоялся и байты есть, а
     * исключение заявило бы обратное.
     */
    public data class Unrecognized(override val raw: String) : PhotonResponse
}
