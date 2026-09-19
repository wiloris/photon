package dev.photon.sdk

import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/*
 * Конфигурация — §4–§6 спецификации.
 *
 * Проверяется не «JSON собрался», а четыре правила, каждое из которых легко
 * нарушить «улучшением»: незаданное не сериализуется, умолчаний SDK не
 * подставляет, длительности переводятся в миллисекунды один раз, raw и
 * типизированный путь не смешиваются.
 */
class PhotonConfigTest {

    // ── Незаданное отсутствует ───────────────────────────────────────────

    @Test
    fun `пустая конфигурация даёт пустой объект`() {
        // Инстанс без Bridge законен: {} принимается Photon_Create
        // (config.go:143-147).
        assertEquals("{}", PhotonConfig().toJson())
    }

    @Test
    fun `задано только одно поле — в JSON только оно`() {
        val json = PhotonConfig(BridgeConfig(endpoint = "host:4443")).toJson()
        assertEquals("""{"bridge":{"endpoint":"host:4443"}}""", json)
    }

    @Test
    fun `незаданные поля не сериализуются ни нулями, ни null`() {
        val json = PhotonConfig(BridgeConfig(endpoint = "h:1")).toJson()
        for (absent in listOf(
            "session_token", "connect_timeout_ms", "call_timeout_ms", "keepalive_ms",
            "event_backoff_min_ms", "event_backoff_max_ms", "tls",
        )) {
            assertFalse(json.contains(absent), "в JSON попало незаданное поле $absent: $json")
        }
        assertFalse(json.contains("null"), "в JSON попал null: $json")
        assertFalse(json.contains(":0"), "в JSON попал ноль вместо отсутствия: $json")
    }

    @Test
    fun `SDK не подставляет умолчаний ядра`() {
        // Умолчания живут в withDefaults (15 с / 30 с / 15 с / 500 мс / 30 с).
        // Копия здесь разошлась бы с оригиналом молча и незаметно.
        val json = PhotonConfig(BridgeConfig(endpoint = "h:1")).toJson()
        for (default in listOf("15000", "30000", "500")) {
            assertFalse(json.contains(default), "SDK подставил умолчание $default: $json")
        }
    }

    // ── Все девять полей ─────────────────────────────────────────────────

    @Test
    fun `девять полей отображаются в те же имена, что у ядра`() {
        val json = PhotonConfig(
            BridgeConfig(
                endpoint = "bridge.example:4443",
                sessionToken = "s",
                connectTimeout = Duration.ofSeconds(15),
                callTimeout = Duration.ofSeconds(30),
                keepAlive = Duration.ofSeconds(15),
                eventBackoffMin = Duration.ofMillis(500),
                eventBackoffMax = Duration.ofSeconds(30),
                tls = TlsConfig(insecureSkipVerify = true, serverName = "bridge.example"),
            )
        ).toJson()

        // Имена — из wireConfig (config.go:113-129). Расхождение здесь ядро
        // не заметит: неизвестные поля оно игнорирует, и опечатка тихо
        // превратится в «не задано».
        assertEquals(
            """{"bridge":{"endpoint":"bridge.example:4443","session_token":"s",""" +
            """"connect_timeout_ms":15000,"call_timeout_ms":30000,"keepalive_ms":15000,""" +
            """"event_backoff_min_ms":500,"event_backoff_max_ms":30000,""" +
            """"tls":{"insecure_skip_verify":true,"server_name":"bridge.example"}}}""",
            json
        )
    }

    @Test
    fun `tls сериализуется частично, если задано одно поле`() {
        val json = PhotonConfig(BridgeConfig(tls = TlsConfig(serverName = "s"))).toJson()
        assertEquals("""{"bridge":{"tls":{"server_name":"s"}}}""", json)
    }

    @Test
    fun `insecure_skip_verify false пишется, если ЗАДАН явно`() {
        // «Не задано» и «задано false» — разные состояния: null исчезает,
        // false остаётся.
        assertEquals(
            """{"bridge":{"tls":{"insecure_skip_verify":false}}}""",
            PhotonConfig(BridgeConfig(tls = TlsConfig(insecureSkipVerify = false))).toJson()
        )
    }

    // ── Длительности ─────────────────────────────────────────────────────

    @Test
    fun `Duration переводится в миллисекунды`() {
        val json = PhotonConfig(BridgeConfig(callTimeout = Duration.ofSeconds(90))).toJson()
        assertEquals("""{"bridge":{"call_timeout_ms":90000}}""", json)
    }

    @Test
    fun `субмиллисекундная часть округляется вниз`() {
        // Мутация, которую тест обязан ловить: toSeconds * 1000 — она даст
        // 1000 вместо 1001 и потеряет миллисекунды вовсе.
        val json = PhotonConfig(
            BridgeConfig(callTimeout = Duration.ofMillis(1001).plusNanos(999_999))
        ).toJson()
        assertEquals("""{"bridge":{"call_timeout_ms":1001}}""", json)
    }

    @Test
    fun `нулевая длительность сериализуется как ноль, а не исчезает`() {
        // Ноль ядро трактует как «по умолчанию», но это ЗАДАННОЕ значение:
        // исчезнуть оно не имеет права — исчезает только null.
        assertEquals(
            """{"bridge":{"connect_timeout_ms":0}}""",
            PhotonConfig(BridgeConfig(connectTimeout = Duration.ZERO)).toJson()
        )
    }

    @Test
    fun `отрицательную длительность SDK НЕ проверяет`() {
        // Non-goal спецификации: единственная валидация живёт в ParseConfig
        // (config.go:157-172) и отвечает PHOTON_ERR_INVALID_ARGUMENT. Без
        // этого теста дублирование валидации вернётся при первом же ревью
        // «а почему мы не проверяем ввод».
        assertEquals(
            """{"bridge":{"connect_timeout_ms":-1}}""",
            PhotonConfig(BridgeConfig(connectTimeout = Duration.ofMillis(-1))).toJson()
        )
    }

    // ── Экранирование: ради него и взята библиотека ──────────────────────

    @Test
    fun `строки экранируются корректно`() {
        val json = PhotonConfig(
            BridgeConfig(endpoint = """h"1\2""", sessionToken = "таб\tи\nперевод")
        ).toJson()
        assertEquals(
            """{"bridge":{"endpoint":"h\"1\\2","session_token":"таб\tи\nперевод"}}""",
            json
        )
    }

    // ── raw ──────────────────────────────────────────────────────────────

    @Test
    fun `raw уходит байт в байт и не разбирается`() {
        // Поле storage_path в схеме wireConfig отсутствует, но законно:
        // ядро игнорирует неизвестное намеренно (config.go:139-142).
        val src = """{ "storage_path" : "/tmp/photon" ,  "bridge":{"endpoint":"h:1"} }"""
        assertEquals(src, PhotonConfig.raw(src).toJson())
    }

    @Test
    fun `raw не проверяется на корректность`() {
        // SDK не валидирует: ответ на негодный config_json даёт ядро.
        assertEquals("не json", PhotonConfig.raw("не json").toJson())
    }

    @Test
    fun `raw и типизированные поля не смешиваются в одном экземпляре`() {
        // Структурная проверка, а не поведенческая: перегрузки, принимающей
        // и bridge, и raw, не существует. Мутация — добавить такую.
        //
        // Синтетические конструкторы исключаются: Kotlin порождает их для
        // значений по умолчанию и для доступа к приватному конструктору из
        // companion (проверено — среди них есть [String,
        // DefaultConstructorMarker]). Это артефакты компилятора, а не
        // объявленный API, и тест обязан смотреть на второе.
        val declaredCtors = PhotonConfig::class.java.constructors.filterNot { it.isSynthetic }

        assertFalse(
            declaredCtors.any { ctor ->
                val types = ctor.parameterTypes
                types.any { it == BridgeConfig::class.java } && types.any { it == String::class.java }
            },
            "появился конструктор, принимающий и bridge, и raw"
        )

        assertFalse(
            PhotonConfig.Companion::class.java.methods.filterNot { it.isSynthetic }.any { m ->
                val types = m.parameterTypes
                types.any { it == BridgeConfig::class.java } && types.any { it == String::class.java }
            },
            "появилась фабрика, принимающая и bridge, и raw"
        )

        assertTrue(
            declaredCtors.all { it.parameterTypes.none { t -> t == String::class.java } },
            "появился публичный конструктор от String — raw обязан быть фабрикой: " +
                declaredCtors.joinToString { c -> c.parameterTypes.joinToString(prefix = "(", postfix = ")") { it.simpleName } }
        )
    }
}
