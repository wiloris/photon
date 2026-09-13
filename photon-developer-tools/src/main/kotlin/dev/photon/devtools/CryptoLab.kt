package dev.photon.devtools

import dev.photon.core.Photon
import java.util.Locale

/*
 * Crypto Lab — единственный выполнимый буллет Phase 16.
 *
 * Формулировка пункта: «наблюдение за тем, как криптография проявляется в
 * ПУБЛИЧНОМ ПУТИ: аутентификация устройства при подключении, доменное
 * разделение подписей (ADR-037) — по исходу действий и текстам отказов».
 *
 * Ключевое слово — «проявляется». Лаборатория ничего не подписывает и не
 * проверяет сама: у неё нет и не будет доступа к ключам. Она наблюдает СЛЕДЫ
 * криптографии в исходах обычных действий.
 *
 * Что здесь принципиально НЕ появится:
 *
 *   - прямой вызов Ed25519/X25519, просмотр Identity, операции над ключами:
 *     публичного пути нет (ROADMAP, Phase 16);
 *   - операция «получить приватный ключ»: ADR-021 запрещает её навсегда, и
 *     §3 ниже сторожит именно это свойство, а не просто его отсутствие
 *     сегодня.
 *
 * Ни одного нового действия, порта или диагностического API ради этой
 * лаборатории не заведено.
 */

private fun c1(v: Double): String = String.format(Locale.ROOT, "%.1f", v)

/** Наблюдение: что делали, что получили, и как это классифицировано. */
data class CryptoObservation(
    val what: String,
    val outcome: Outcome,
    val detail: String,
    val millis: Double,
)

/**
 * Имена действий, которые означали бы вынос криптографии в публичный каталог.
 *
 * Список намеренно шире, чем «ADR-021 запрещает выдачу ключа»: сторожить надо
 * весь класс, иначе завтра появится действие вида «export identity», формально
 * не названное «get private key».
 *
 * Маркеры — фрагменты имён, а не имена: вписать сюда имя из каталога значило бы
 * завести его вторую копию, что запрещено отдельным тестом Inspector.
 */
val cryptoActionMarkers: List<String> = listOf(
    "SIGN", "VERIFY", "KEY", "IDENTITY", "CRYPTO", "ENCRYPT", "DECRYPT", "SECRET", "SEED",
)

/**
 * Действия каталога, похожие на криптографические.
 *
 * Чистая функция: проверяется без сети и без native, и именно она делает §3
 * регрессионным стражем, а не разовым наблюдением.
 */
fun cryptoLikeActions(actionNames: List<String>): List<String> =
    actionNames.filter { name -> cryptoActionMarkers.any { name.contains(it) } }

/**
 * Различение исходов аутентификации.
 *
 * Случаев четыре, и смешивать их нельзя:
 *
 *   - отказ ВЫЗОВА, в котором сервер всё же назвал `unauthorized`: сессия
 *     отвергнута на handshake, соединение не поднялось;
 *   - отказ вызова без такой причины — TLS, сеть, недоступный адрес;
 *   - вердикт `ok:false` с `unauthorized` — вызов состоялся, субъект отвергнут;
 *   - вердикт `ok:false` с `forbidden` — субъект известен, прав не хватает.
 *
 * ПОРЯДОК ВЕТОК СУЩЕСТВЕН, и это измеренный факт, а не предосторожность.
 * Живой прогон: негодный токен приходит НЕ вердиктом, а ошибкой вызова —
 * `session: malformed frame from server: handshake ack: Application error
 * 0x0 (remote): unauthorized`. Если сначала спрашивать про исход, а уже
 * потом про причину, отказ аутентификации сольётся с обрывом сети, и клиент
 * потеряет единственный признак, по которому «перелогинься» отличается от
 * «сеть упала».
 *
 * Аутентификация и авторизация тоже разные вещи: первое про то, КТО зовёт,
 * второе про то, что ему позволено.
 */
fun authKind(o: CryptoObservation): String = when {
    o.detail.contains("unauthorized") && o.outcome == Outcome.CALL_ERROR ->
        "не аутентифицирован — отказ на handshake, соединение не поднялось"
    o.outcome == Outcome.CALL_ERROR -> "соединение не состоялось"
    o.outcome == Outcome.VERDICT_FALSE && o.detail.contains("unauthorized") -> "субъект не аутентифицирован"
    o.outcome == Outcome.VERDICT_FALSE && o.detail.contains("forbidden") -> "аутентифицирован, прав нет"
    o.outcome == Outcome.VERDICT_FALSE -> "отказ сервера по иной причине"
    else -> "принято"
}

class CryptoLab(
    private val endpoint: String,
    private val token: String,
    private val catalog: Inspector?,
) {

    private fun config(ep: String, tok: String, insecure: Boolean) =
        """{"bridge":{"endpoint":"$ep","session_token":"$tok",""" +
            """"connect_timeout_ms":15000,"call_timeout_ms":20000""" +
            (if (insecure) ""","tls":{"insecure_skip_verify":true}""" else "") + "}}"

    /** Один зонд: создать инстанс, сделать один обычный вызов, записать исход. */
    private fun probe(what: String, ep: String, tok: String, insecure: Boolean): CryptoObservation {
        val started = System.nanoTime()
        return try {
            Photon.create(config(ep, tok, insecure)).use { p ->
                val r = call(p, UsedActions.GET_ME, "{}")
                CryptoObservation(what, r.outcome, r.detail, r.millis)
            }
        } catch (e: Exception) {
            // Отказ на создании инстанса — тоже наблюдение, а не сбой прогона.
            CryptoObservation(what, Outcome.CALL_ERROR,
                "${e::class.simpleName}: ${e.message}", (System.nanoTime() - started) / 1e6)
        }
    }

    fun run(ipEndpoint: String?) {
        println("Crypto Lab — следы криптографии в публичном пути")
        println("лаборатория ничего не подписывает: ключей у неё нет и не будет\n")

        Photon.loadLibrary()
        transportCrypto(ipEndpoint)
        authentication()
        adr021Guard()
        adr037NotObservable()
    }

    // ── 1. криптография транспорта ───────────────────────────────────────────

    /**
     * Проверка сертификата — самое заметное проявление криптографии на этом
     * пути, и наблюдается оно ОДНИМ И ТЕМ ЖЕ сервером под двумя именами.
     *
     * Сертификат стенда выдан на доменное имя. Обращение к тому же серверу по
     * IP обязано провалиться на сверке имени: содержимое, права и токен при
     * этом не меняются — меняется только то, чему клиент доверяет.
     */
    private fun transportCrypto(ipEndpoint: String?) {
        println("── 1. криптография транспорта ──────────────────")
        println("  TLS 1.3 задан минимумом в ядре и вниз не опускается")

        val byName = probe("подключение по имени из сертификата", endpoint, token, insecure = false)
        report(byName)

        if (ipEndpoint == null) {
            println("  ⏭  сверка имени не выполнена: второй адрес того же сервера не задан")
            println("     (нужен тот же Bridge по IP — тогда имя в сертификате не совпадёт)\n")
            return
        }

        val byIp = probe("тот же сервер по IP, проверка включена", ipEndpoint, token, insecure = false)
        report(byIp)
        val byIpInsecure = probe("тот же сервер по IP, проверка ВЫКЛЮЧЕНА", ipEndpoint, token, insecure = true)
        report(byIpInsecure)

        println()
        when {
            byIp.outcome == Outcome.CALL_ERROR && byIpInsecure.outcome == Outcome.OK ->
                println("  ВЫВОД: проверка сертификата включена по умолчанию и работает —\n" +
                    "  один и тот же сервер принят по имени и отвергнут по IP.\n" +
                    "  insecure_skip_verify снимает именно её, и цена флага теперь видна.")
            byIp.outcome == Outcome.OK ->
                println("  ⚠️ ВНИМАНИЕ: обращение по IP ПРИНЯТО. Либо сертификат покрывает IP,\n" +
                    "  либо проверка имени не выполняется — второе было бы дефектом.")
            else ->
                println("  вывод не сделан: оба обращения по IP отвергнуты, различить\n" +
                    "  проверку имени и недоступность адреса этим замером нельзя.")
        }
        println()
    }

    // ── 2. аутентификация ────────────────────────────────────────────────────

    /**
     * Аутентификация наблюдается ПАРОЙ: валидный токен против заведомо
     * негодного. Один только отказ ничего не значил бы — сервер, отвергающий
     * всё подряд, выглядел бы точно так же.
     */
    private fun authentication() {
        println("── 2. аутентификация при подключении ───────────")
        val good = probe("валидная сессия", endpoint, token, insecure = false)
        report(good)
        val junk = probe("несуществующая сессия", endpoint, "0123456789abcdef0123456789", insecure = false)
        report(junk)
        val empty = probe("пустой токен", endpoint, "", insecure = false)
        report(empty)

        println()
        println("  различение исходов (аутентификация ≠ авторизация):")
        listOf(good, junk, empty).forEach { println("    ${it.what}: ${authKind(it)}") }
        if (good.outcome == Outcome.OK && junk.outcome != Outcome.OK) {
            println("  ВЫВОД: подтверждение сессии выполняется и наблюдаемо — валидный")
            println("  токен принят, негодный отвергнут тем же путём и тем же вызовом.")
        } else {
            println("  ⚠️ пара не различилась: вывод об аутентификации не делается.")
        }
        if (junk.outcome == Outcome.CALL_ERROR && junk.detail.contains("unauthorized")) {
            println()
            println("  НАБЛЮДЕНИЕ ДЛЯ ПРИЛОЖЕНИЙ: отказ аутентификации приходит НЕ вердиктом,")
            println("  а ошибкой ВЫЗОВА — сессия проверяется на handshake, и соединение не")
            println("  поднимается вовсе. Причина `unauthorized` в тексте сохраняется, но")
            println("  обёрнута как `malformed frame from server`: по коду исхода отличить")
            println("  «сессия истекла» от «сеть упала» нельзя, только по подстроке.")
            println("  Это свойство пути, а не дефект лаборатории; для клиента, которому")
            println("  нужно решать «перелогиниться или повторить», различение существенно.")
        }
        println()
    }

    // ── 3. ADR-021: страж, а не наблюдение ───────────────────────────────────

    /**
     * Здесь лаборатория не измеряет, а СТОРОЖИТ.
     *
     * ADR-021 запрещает операцию «получить приватный ключ» навсегда, а ROADMAP
     * требует, чтобы любая будущая диагностика криптографии это свойство
     * сохраняла. Проверка идёт по КАНОНИЧЕСКОМУ каталогу (FileDescriptorSet),
     * а не по списку внутри инструмента: своей копии каталога у него нет.
     */
    private fun adr021Guard() {
        println("── 3. ADR-021: приватный ключ наружу не выходит ─")
        if (catalog == null) {
            println("  ⏭  каталог недоступен: проверка не выполнена (нужен дескриптор)\n")
            return
        }
        val action = catalog.enum("Action")
        if (action == null) {
            println("  ⏭  enum Action в дескрипторе отсутствует\n")
            return
        }
        val names = action.valueList.map { it.name }
        val suspicious = cryptoLikeActions(names)
        println("  действий в каноническом каталоге: ${names.size}")
        println("  похожих на криптографические: ${suspicious.size}")
        if (suspicious.isEmpty()) {
            println("  ВЫВОД: ни одного действия про ключи, подпись или identity —")
            println("  криптография в доменный каталог не вынесена. Появление такого")
            println("  действия обязано быть архитектурным решением, а не правкой enum.")
        } else {
            println("  ⚠️ НАЙДЕНЫ: ${suspicious.joinToString(", ")}")
            println("  Проверить по ADR-021: подпись выполняется на стороне владельца")
            println("  ключа, наружу уходит результат, а не секрет.")
        }
        println()
    }

    // ── 4. ADR-037: честно необнаружимо ──────────────────────────────────────

    /**
     * Второй половине буллета ответить нечем, и это ответ, а не пропуск.
     */
    private fun adr037NotObservable() {
        println("── 4. доменное разделение подписей (ADR-037) ───")
        println("  НЕ НАБЛЮДАЕМО через публичный путь. Основание, а не впечатление:")
        println()
        println("  ADR-037 разделяет домены в двух местах — операции identity.sign")
        println("  плагина (Plugin Runtime) и services/account.AuthenticateDevice.")
        println("  Ни то, ни другое на этом пути не лежит:")
        println("    • в каталоге нет действия ни для подписи, ни для аутентификации")
        println("      устройства — все 59 кодов доменные (user, channel, post, file,")
        println("      engram, wall, media);")
        println("    • services/dispatcher и photon-bindings/c не ссылаются ни на")
        println("      services/account, ни на ports.Identity;")
        println("    • аутентификация, которую клиент действительно проходит, —")
        println("      сессия Mattermost (§2), а не challenge устройства.")
        println()
        println("  Значит наблюдать домены нечем: они разделяют подписи, которых на")
        println("  публичном пути не возникает. Заводить ради проверки действие или")
        println("  диагностический API запрещено постановкой фазы — поэтому пункт")
        println("  объявлен неизмеримым, а не закрыт похожим замером.")
    }

    private fun report(o: CryptoObservation) {
        val mark = when (o.outcome) {
            Outcome.OK -> "✅"
            Outcome.VERDICT_FALSE -> "🟡"
            Outcome.CALL_ERROR -> "⛔"
        }
        println("  $mark ${o.what}: ${o.detail} (${c1(o.millis)} мс)")
    }
}
