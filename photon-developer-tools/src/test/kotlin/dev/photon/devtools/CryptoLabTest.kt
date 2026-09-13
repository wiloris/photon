package dev.photon.devtools

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/*
 * Тесты Crypto Lab.
 *
 * Проверяется классификация исходов и страж ADR-021 — то, что должно
 * работать одинаково на любом сервере. Тексты отказов сервера здесь НЕ
 * закрепляются: они принадлежат серверу, и тест, повторяющий их дословно,
 * ломался бы от смены формулировки, ничего не проверяя по существу.
 */
class CryptoLabTest {

    private fun obs(outcome: Outcome, detail: String) =
        CryptoObservation("зонд", outcome, detail, 1.0)

    // ── аутентификация против авторизации ────────────────────────────────────

    /**
     * Главное различение лаборатории. `unauthorized` и `forbidden` — разные
     * ответы о разном: первый про то, КТО зовёт, второй про то, ЧТО ему
     * позволено. Слить их значило бы объявить отказ прав провалом
     * аутентификации — и наоборот.
     */
    @Test
    fun `аутентификация и авторизация различаются`() {
        assertEquals("субъект не аутентифицирован",
            authKind(obs(Outcome.VERDICT_FALSE, "ВЕРДИКТ ok:false — unauthorized")))
        assertEquals("аутентифицирован, прав нет",
            authKind(obs(Outcome.VERDICT_FALSE, "ВЕРДИКТ ok:false — forbidden")))
    }

    /**
     * Несостоявшийся вызов — не отказ сервера. На этом стоит весь вывод о
     * TLS: сверка имени в сертификате обрывает соединение, а не приходит
     * вердиктом (ADR-038).
     */
    @Test
    fun `несостоявшийся вызов не выдаётся за отказ сервера`() {
        assertEquals("соединение не состоялось",
            authKind(obs(Outcome.CALL_ERROR, "ОШИБКА ВЫЗОВА code=2 tls: bad certificate")))
        assertTrue(authKind(obs(Outcome.CALL_ERROR, "что угодно")) != "субъект не аутентифицирован")
    }

    /**
     * Регрессия на находку живого прогона.
     *
     * Негодная сессия отвергается на handshake и приходит ошибкой ВЫЗОВА, а
     * не вердиктом; причина `unauthorized` при этом сохраняется в тексте:
     *
     *   session: malformed frame from server: handshake ack:
     *   Application error 0x0 (remote): unauthorized
     *
     * Классификатор обязан удержать ОБА факта — что аутентификация провалена
     * и что соединение не поднялось. Мутация «сначала смотреть на исход»
     * сливает этот случай с обрывом сети и роняет тест.
     */
    @Test
    fun `отказ аутентификации на handshake отличим от обрыва сети`() {
        val rejected = obs(Outcome.CALL_ERROR,
            "ОШИБКА ВЫЗОВА code=7 session: malformed frame from server: " +
                "handshake ack: Application error 0x0 (remote): unauthorized")
        assertEquals("не аутентифицирован — отказ на handshake, соединение не поднялось",
            authKind(rejected))
        assertTrue(authKind(rejected) != authKind(obs(Outcome.CALL_ERROR, "timeout")),
            "отвергнутая сессия и упавшая сеть обязаны различаться")
    }

    @Test
    fun `иная причина отказа не приписывается аутентификации`() {
        assertEquals("отказ сервера по иной причине",
            authKind(obs(Outcome.VERDICT_FALSE, "ВЕРДИКТ ok:false — invalid payload")))
    }

    @Test
    fun `принятый вызов классифицируется как принятый`() {
        assertEquals("принято", authKind(obs(Outcome.OK, "OK")))
    }

    // ── страж ADR-021 ────────────────────────────────────────────────────────

    /**
     * Каталог без криптографических действий — нормальное состояние; тест
     * фиксирует, что страж на нём молчит, иначе он кричал бы всегда и его
     * перестали бы читать.
     */
    @Test
    fun `доменный каталог не содержит криптографических действий`() {
        val domain = listOf(
            "ACTION_CREATE_POST", "ACTION_GET_FEED", "ACTION_DELETE_POST",
            "ACTION_GET_ME", "ACTION_UPLOAD_FILE", "ACTION_MEDIA_UPLOAD",
            "ACTION_GET_OR_CREATE_DIRECT_CHANNEL",
        )
        assertTrue(cryptoLikeActions(domain).isEmpty())
    }

    /**
     * Мутация «вынести подпись наружу» обязана быть замеченной. Именно ради
     * этого страж существует: ADR-021 запрещает выдачу ключа НАВСЕГДА, а не
     * только сегодня.
     */
    @Test
    fun `появление действия про ключи или подпись замечается`() {
        assertEquals(listOf("ACTION_IDENTITY_SIGN"),
            cryptoLikeActions(listOf("ACTION_GET_ME", "ACTION_IDENTITY_SIGN")))
        assertEquals(listOf("ACTION_EXPORT_PRIVATE_KEY"),
            cryptoLikeActions(listOf("ACTION_EXPORT_PRIVATE_KEY", "ACTION_GET_FEED")))
        assertEquals(listOf("ACTION_DECRYPT_BLOB"),
            cryptoLikeActions(listOf("ACTION_DECRYPT_BLOB")))
    }

    /**
     * Страж сторожит КЛАСС, а не одну формулировку: «получить приватный ключ»
     * можно назвать как угодно, и список маркеров должен переживать
     * переименование.
     */
    @Test
    fun `страж ловит класс действий, а не одно имя`() {
        listOf("SIGN", "VERIFY", "KEY", "IDENTITY", "CRYPTO", "ENCRYPT", "DECRYPT", "SECRET", "SEED")
            .forEach { marker ->
                assertTrue(cryptoLikeActions(listOf("ACTION_${marker}_SOMETHING")).isNotEmpty(),
                    "маркер $marker обязан срабатывать")
            }
    }

    // ── структурные свойства ─────────────────────────────────────────────────

    /**
     * Лаборатория не имеет права обзавестись ключами: ни своей криптографии,
     * ни импорта внутренних пакетов. Иначе она проверяла бы себя, а не
     * платформу.
     */
    @Test
    fun `Crypto Lab не подписывает и не проверяет сам`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "CryptoLab.kt").readText()
        listOf("java.security", "javax.crypto", "Signature.getInstance", "KeyPairGenerator")
            .forEach { assertTrue(!src.contains(it), "$it в лаборатории быть не должно") }
        assertTrue(src.contains("dev.photon.core.Photon"),
            "единственный вход в платформу — публичный мост")
    }

    /**
     * Каталог берётся канонический. Свой список действий здесь означал бы
     * вторую копию — ровно то, чего избегает Inspector.
     */
    @Test
    fun `страж читает канонический каталог, а не свой список`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "CryptoLab.kt").readText()
        assertTrue(src.contains("catalog.enum(\"Action\")"),
            "список действий обязан приходить из дескриптора")
        assertTrue(!Regex("""ACTION_[A-Z_]+\s*=\s*\d""").containsMatchIn(src),
            "объявление кодов действий в лаборатории — вторая копия каталога")
    }

    /**
     * Неизмеримое обязано быть объявлено с ОСНОВАНИЕМ. Без этого теста ничто
     * не мешает заменить честное «нечем наблюдать» на похожий замер.
     */
    @Test
    fun `ADR-037 объявлен ненаблюдаемым с основанием`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "CryptoLab.kt").readText()
        assertTrue(src.contains("НЕ НАБЛЮДАЕМО"))
        assertTrue(src.contains("services/account") && src.contains("ports.Identity"),
            "основание — отсутствие этих пакетов на публичном пути, и оно должно быть названо")
        assertTrue(src.contains("identity.sign"),
            "вторая половина основания — операция плагина, которой на этом пути нет")
    }

    /**
     * Вывод об аутентификации делается ПАРОЙ. Один отказ ничего не значит:
     * сервер, отвергающий всё, выглядел бы так же — та же ошибка, что нашли
     * в Phase 13.5 у проверки «отвергнут без сети».
     */
    @Test
    fun `вывод об аутентификации требует и принятого вызова`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "CryptoLab.kt").readText()
        assertTrue(src.contains("good.outcome == Outcome.OK && junk.outcome != Outcome.OK"),
            "вывод обязан требовать успеха валидной сессии, а не только отказа негодной")
    }
}
