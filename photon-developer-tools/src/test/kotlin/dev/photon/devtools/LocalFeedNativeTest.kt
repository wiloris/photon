package dev.photon.devtools

import dev.photon.core.Photon
import dev.photon.core.PhotonException
import dev.photon.core.PhotonNative
import dev.photon.sdk.BridgeConfig
import dev.photon.sdk.FeedBlock
import dev.photon.sdk.FeedGapAnchor
import dev.photon.sdk.FeedIngestResult
import dev.photon.sdk.FeedPosition
import dev.photon.sdk.FeedPutResult
import dev.photon.sdk.FeedRecord
import dev.photon.sdk.PhotonClient
import dev.photon.sdk.PhotonConfig
import dev.photon.sdk.PhotonResponse
import dev.photon.sdk.StorageConfig
import dev.photon.sdk.TlsConfig
import java.io.File
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.SocketTimeoutException
import java.nio.file.Files
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/*
 * Запись в локальную ленту через НАСТОЯЩИЙ нативный путь.
 *
 * Цепочка целиком, без единой подмены:
 *
 *   dev.photon.sdk.PhotonClient   (публичная поверхность SDK, ADR-066)
 *        → dev.photon.core.Photon / PhotonNative   (JVM-мост)
 *        → libphoton_jni.so                         (JNI)
 *        → libphoton_core.so                        (C ABI, ADR-026)
 *        → services/dispatcher                      (маршрутизация, ADR-064)
 *        → services/localfeed                       (инварианты, ADR-065)
 *        → adapters/bbolt                           (файл на диске)
 *        → конверт обратно                          (PhotonResponse / PhotonException)
 *
 * ЗАЧЕМ ОТДЕЛЬНО ОТ УЖЕ СУЩЕСТВУЮЩИХ ТЕСТОВ. Действия 801/802/803 закрыты
 * тестами на трёх уровнях, и ни один из них этой цепочки не проходит:
 * services/dispatcher проверяется in-process на Go, photon-bindings/c — через
 * экспортируемые функции того же процесса (localfeed_test.go, и только код
 * 800), photon-sdk — против FakeNativeSession, то есть вообще без нативного
 * кода. Все три зелены и при этом ничего не говорят о том, доезжает ли запись
 * до диска, когда её отправляет Kotlin.
 *
 * ПОЧЕМУ ЗДЕСЬ, А НЕ В photon-sdk. Настоящие .so собирает сборка Developer
 * Tools (`buildCoreLib` → `copyCoreLib` → `buildJniLib`), и второго такого
 * механизма заводить нельзя: он разошёлся бы с первым молча. Инструмент и так
 * ходит к ядру только через SDK (ADR-062 п. 7), поэтому тест пользуется
 * ПУБЛИЧНОЙ поверхностью SDK и ничем больше — `internal`-шов
 * `PhotonClient.createWith`, которым живут тесты самого SDK, из этого модуля
 * не виден по построению.
 *
 * ЭТО ТЕСТ, А НЕ ПРОГОН. Сети здесь нет вовсе: локальное действие адресата не
 * требует (ADR-064 п. 1.5), а единственный сокет в файле — измерительный, он
 * доказывает ОТСУТСТВИЕ сетевого хода.
 */
class LocalFeedNativeTest {

    // ── настоящий нативный путь ──────────────────────────────────────────────

    /**
     * Загрузка библиотек — ровно один раз на JVM.
     *
     * Путь приходит системным свойством от сборки (`photon.native.dir`), как
     * `photon.descriptor` и `photon.devtools.src` у соседних тестов. Умолчания
     * нет намеренно: отсутствие свойства означает, что тест запущен в обход
     * задачи, которая собирает .so, — и тогда он обязан ПАДАТЬ, а не тихо
     * пропускаться. Пропуск дал бы зелёный прогон без единой проверки, то есть
     * ровно тот тест, который хуже отсутствующего.
     */
    private object RealNative {

        private var loaded = false

        /**
         * Проверяет и загружает; повторный вызов библиотеку не грузит заново.
         *
         * Проверки живут ЗДЕСЬ, а не в инициализаторе объекта: отказ
         * инициализатора приходит `ExceptionInInitializerError`, у которого
         * собственного текста нет, и причина («.so не собрана») оказалась бы в
         * причине причины. Отказ обязан читаться первой строкой отчёта.
         */
        @Synchronized
        fun ensure(): File {
            val dir = File(
                requireNotNull(System.getProperty("photon.native.dir")) {
                    "photon.native.dir не задан: тест обязан идти задачей Gradle, собирающей libphoton_jni.so"
                }
            )
            val jni = File(dir, "libphoton_jni.so")
            val core = File(dir, "libphoton_core.so")
            require(jni.isFile) { "нет $jni — JNI-мост не собран, проверять нечего" }
            require(core.isFile) { "нет $core — C ABI не собран, проверять нечего" }
            if (!loaded) {
                Photon.loadFrom(jni.absolutePath)
                loaded = true
            }
            return jni
        }
    }

    private fun tempDb(): File {
        val dir = Files.createTempDirectory("photon-localfeed-native").toFile()
        dir.deleteOnExit()
        return File(dir, "feed.db")
    }

    private fun client(db: File?, bridge: BridgeConfig? = null): PhotonClient {
        RealNative.ensure()
        return PhotonClient.create(
            PhotonConfig(bridge = bridge, storage = db?.let { StorageConfig(path = it.absolutePath) })
        )
    }

    private fun body(text: String) = """{"message":"$text"}"""

    private fun page(c: PhotonClient, channel: String, after: FeedPosition? = null): String {
        val cursor = after?.let { ""","after_key":${it.orderKey},"after_id":"${it.id}"""" }.orEmpty()
        val r = c.sendAction(ACTION_PAGE, """{"channel_id":"$channel","limit":50$cursor}""")
        assertIs<PhotonResponse.Ok>(r, "страница локальной ленты обязана приходить ok:true")
        return r.raw
    }

    /**
     * Код страницы вписан здесь числом сознательно: SDK его НЕ типизирует
     * (ADR-066 п. 3.6), и цикл догрузки у вызывающего собирается именно так —
     * три типизированных вызова и один общий. Тест повторяет тот же способ,
     * каким этим будет пользоваться приложение.
     */
    private val ACTION_PAGE = 800

    /** Сетевое действие для контрольного измерения — ACTION_GET_ME. */
    private val ACTION_NETWORK = 200

    // ── 1. Доказательство, что путь настоящий ────────────────────────────────

    /**
     * Первая проверка отвечает на вопрос, от которого зависят все остальные:
     * исполняется ли вообще нативный код.
     *
     * Три независимых свидетельства, и ни одно не может выполниться на
     * подмене:
     *  - обе библиотеки ОТОБРАЖЕНЫ в адресное пространство процесса
     *    (`/proc/self/maps` — Linux, и это законно: область ADR-051 — Linux
     *    x86-64);
     *  - версию ABI отвечает `Photon_GetABIVersion` через JNI;
     *  - подстановка `FakeNativeSession` здесь невозможна структурно: шов
     *    `PhotonClient.createWith` объявлен `internal` в модуле photon-sdk и
     *    из этого модуля не виден — компилятор не даст.
     */
    @Test
    fun `процесс держит настоящие libphoton_jni и libphoton_core, ABI 1_0`() {
        RealNative.ensure()
        val maps = File("/proc/self/maps").readText()

        assertTrue(
            maps.contains("libphoton_jni.so"),
            "libphoton_jni.so не отображена в процесс — нативного моста нет"
        )
        assertTrue(
            maps.contains("libphoton_core.so"),
            "libphoton_core.so не отображена в процесс — C ABI не загружен"
        )
        assertEquals(1, Photon.abiMajor(), "ABI MAJOR обязан быть 1 (ADR-026)")
        assertEquals(0, Photon.abiMinor(), "ABI MINOR обязан быть 0: фаза его не меняла")
    }

    // ── 2. PUT (801) ─────────────────────────────────────────────────────────

    /**
     * Запись доезжает до ФАЙЛА, а не до памяти теста.
     *
     * Доказательства идут по возрастанию строгости: результат вызова → чтение
     * тем же инстансом → байты тела внутри файла bbolt → чтение ДРУГИМ
     * инстансом, созданным после закрытия первого. Последнее не может пройти
     * ни на какой подмене: данные обязаны пережить `Photon_Destroy` и
     * повторное открытие хранилища.
     */
    @Test
    fun `801 кладёт записи, и другой инстанс видит их после закрытия первого`() {
        val db = tempDb()
        val marker = "через настоящий SDK"

        client(db).use { c ->
            val put = c.localFeedPut(
                "ch",
                listOf(
                    FeedRecord(id = "a", orderKey = 100, version = 101, body = body(marker)),
                    FeedRecord(id = "b", orderKey = 200, version = 201, body = body("новое")),
                )
            )
            val stored = assertIs<FeedPutResult.Stored>(put, "Put обязан вернуть типизированный результат")
            assertEquals(2, stored.stored)

            val raw = page(c, "ch")
            assertTrue(raw.contains(""""id":"b""""), "свежая запись не пришла: $raw")
            assertTrue(raw.contains(""""id":"a""""), "старая запись не пришла: $raw")
            assertTrue(raw.contains(marker), "тело записи изменилось по дороге: $raw")

            // PUT о непрерывности не утверждает НИЧЕГО (ADR-065 п. 5.1):
            // сегментов нет, значит и пропуска нет.
            assertIs<FeedGapAnchor.None>(
                c.localFeedGapAnchor("ch"),
                "Put создал сегмент — этого он делать не имеет права"
            )
        }

        assertTrue(db.isFile && db.length() > 0, "файл хранилища не создан: $db")
        assertTrue(
            String(db.readBytes(), Charsets.UTF_8).contains(marker),
            "тела записи нет в файле bbolt — запись не дошла до диска"
        )

        client(db).use { second ->
            val raw = page(second, "ch")
            assertTrue(
                raw.contains(""""id":"a"""") && raw.contains(marker),
                "второй инстанс не увидел записи: $raw"
            )
        }
    }

    // ── 3. GAP_ANCHOR (803) и знаковый gapsClosed ────────────────────────────

    /**
     * `None` → `Found`, и отрицательный `gapsClosed` на настоящем пути.
     *
     * Сценарий воспроизводит первый шаг возвращения после offline: свежий
     * остров сверху, потом отдельный кусок снизу. Пропусков стало БОЛЬШЕ, и
     * `gapsClosed` обязан прийти отрицательным — ADR-065 п. 7.1 и ADR-066
     * п. 6.1. Нормализация в ноль здесь и ловится.
     *
     * Якорь берётся из НИЖНЕГО сегмента, а не из самой свежей записи: запрос
     * от верхней позиции вернул бы уже известное и оставил дыру навсегда.
     */
    @Test
    fun `803 отвечает None без пропуска и Found после разрыва, gapsClosed отрицательный`() {
        val db = tempDb()
        client(db).use { c ->
            assertIs<FeedGapAnchor.None>(
                c.localFeedGapAnchor("ch"),
                "у пустого канала пропуска быть не может"
            )

            val top = c.localFeedIngest(
                "ch",
                FeedBlock.Newest(
                    records = listOf(
                        FeedRecord("t400", 400, 401, body("верх-1")),
                        FeedRecord("t300", 300, 301, body("верх-2")),
                    ),
                    atEnd = false,
                )
            )
            val first = assertIs<FeedIngestResult.Applied>(top)
            assertEquals(2, first.stored)
            assertEquals(1, first.segments)
            assertEquals(0, first.gapsClosed, "первый блок пропусков не создаёт и не закрывает")
            assertEquals(0, first.stale)
            assertEquals(0, first.vanished)

            assertIs<FeedGapAnchor.None>(
                c.localFeedGapAnchor("ch"),
                "один сегмент — пропуска нет"
            )

            val below = c.localFeedIngest(
                "ch",
                FeedBlock.Before(
                    anchor = FeedPosition(orderKey = 100, id = "b100"),
                    records = listOf(FeedRecord("b100", 100, 101, body("низ"))),
                    atEnd = false,
                )
            )
            val second = assertIs<FeedIngestResult.Applied>(below)
            assertEquals(2, second.segments, "нижний кусок обязан лечь ОТДЕЛЬНЫМ сегментом")
            assertEquals(
                -1, second.gapsClosed,
                "появился НОВЫЙ пропуск: gapsClosed обязан быть отрицательным, а не нулём"
            )

            val anchor = assertIs<FeedGapAnchor.Found>(
                c.localFeedGapAnchor("ch"),
                "между двумя сегментами пропуск есть"
            )
            assertEquals(FeedPosition(100, "b100"), anchor.anchor)
        }
    }

    // ── 4. INGEST (802): согласование версий ─────────────────────────────────

    /**
     * `unchanged` и `stale` на настоящем хранилище.
     *
     * Повтор того же блока обязан дать `unchanged`, а блок с ВЕРСИЕЙ МЛАДШЕ —
     * `stale` и невозможность отката тела (ADR-053). Это единственное место
     * теста, где проверяется, что чтение существующей записи действительно
     * происходит: на пустом хранилище обе ветки выглядят одинаково.
     */
    @Test
    fun `802 отличает unchanged от stale, и откат тела не проходит`() {
        val db = tempDb()
        client(db).use { c ->
            val block = FeedBlock.Newest(
                records = listOf(FeedRecord("x", 500, 700, body("версия-700"))),
                atEnd = true,
            )
            assertIs<FeedIngestResult.Applied>(c.localFeedIngest("ch", block))

            val repeat = assertIs<FeedIngestResult.Applied>(c.localFeedIngest("ch", block))
            assertEquals(1, repeat.unchanged, "повтор того же блока обязан дать unchanged")
            assertEquals(0, repeat.stale)
            assertEquals(0, repeat.gapsClosed, "повтор блока пропусков не двигает")

            val older = assertIs<FeedIngestResult.Applied>(
                c.localFeedIngest(
                    "ch",
                    FeedBlock.Newest(
                        records = listOf(FeedRecord("x", 500, 600, body("версия-600"))),
                        atEnd = true,
                    )
                )
            )
            assertEquals(1, older.stale, "версия младше локальной обязана считаться stale")
            assertEquals(0, older.stored, "stale-запись не применяется, значит и не считается сохранённой")

            val raw = page(c, "ch")
            assertTrue(raw.contains("версия-700"), "тело откатилось к старой версии: $raw")
            assertTrue(!raw.contains("версия-600"), "устаревшее тело записано: $raw")
        }
    }

    // ── 5. Long выше 2^53 ────────────────────────────────────────────────────

    /**
     * 2^53+1 сквозь весь путь: Kotlin → JSON → JNI → Go → ключ bbolt → обратно.
     *
     * Значение выбрано так, что ЛЮБОЕ промежуточное `double` его испортит:
     * 9007199254740993 в double становится 9007199254740992. Миллисекунды
     * эпохи (~1.7·10¹²) такой мутации не ловят — это уже измерено на ядре,
     * поэтому здесь взято значение за пределом точности.
     *
     * Проверяются ОБА направления и оба типа: `order_key`/`version` в странице
     * и `order_key` якоря, который вычисляет ядро и разбирает SDK.
     */
    @Test
    fun `значение выше 2 в 53 доезжает до хранилища и обратно без искажения`() {
        val big = 9007199254740993L // 2^53 + 1
        val bigVersion = 9007199254740995L
        val above = 9007199254740997L

        val db = tempDb()
        client(db).use { c ->
            assertIs<FeedIngestResult.Applied>(
                c.localFeedIngest(
                    "big",
                    FeedBlock.Newest(
                        records = listOf(FeedRecord("top", above, 1, body("верх"))),
                        atEnd = false,
                    )
                )
            )
            assertIs<FeedIngestResult.Applied>(
                c.localFeedIngest(
                    "big",
                    FeedBlock.Before(
                        anchor = FeedPosition(orderKey = big, id = "big"),
                        records = listOf(FeedRecord("big", big, bigVersion, body("низ"))),
                        atEnd = false,
                    )
                )
            )

            /*
             * Страница обрывается на границе сегмента: над пропуском лежит
             * "top", под ним — "big", и это НЕ дефект, а то самое различие
             * «дальше ничего нет» против «дальше я не знаю», ради которого
             * заведён gap_after. Поэтому нижняя запись читается вторым
             * запросом — курсором, чей order_key сам выше 2^53.
             */
            val first = page(c, "big")
            assertTrue(first.contains("9007199254740997"), "order_key страницы потерял точность: $first")
            assertTrue(first.contains(""""gap_after":true"""), "пропуск не показан: $first")

            val second = page(c, "big", FeedPosition(above, "top"))
            assertTrue(second.contains("9007199254740993"), "order_key потерял точность: $second")
            assertTrue(second.contains("9007199254740995"), "version потеряла точность: $second")
            assertTrue(!second.contains("9007199254740992"), "значение проехало через double: $second")

            val anchor = assertIs<FeedGapAnchor.Found>(c.localFeedGapAnchor("big"))
            assertEquals(big, anchor.anchor.orderKey, "якорь вернулся искажённым")
            assertEquals("big", anchor.anchor.id)
        }
    }

    // ── 6. Отказ вызова приходит настоящим кодом ABI ─────────────────────────

    /**
     * Ядро остаётся единственным арбитром (ADR-066 §4.3).
     *
     * Два инварианта ADR-065, которые SDK выразить ПОЗВОЛЯЕТ: пустой `id`
     * (п. 3.6) и запись не с той стороны якоря (п. 3.9). Оба обязаны
     * отвергаться настоящим ABI одним кодом `PHOTON_ERR_INVALID_ARGUMENT`
     * (ADR-066 §8.1), с текстом в `nativeMessage` — и без частичного
     * применения: блок отвергается целиком (ADR-065 п. 3.10).
     *
     * Проверка кода здесь не формальность: in-process маппинг Go эти случаи
     * уже покрывает, а вот то, что код доезжает через JNI и становится
     * `PhotonException.code`, до сих пор не проверялось ни одним тестом.
     */
    @Test
    fun `ядро отвергает пустой id и запись не с той стороны якоря настоящим INVALID_ARGUMENT`() {
        val db = tempDb()
        client(db).use { c ->
            val empty = assertFailsWith<PhotonException> {
                c.localFeedPut("ch", listOf(FeedRecord(id = "", orderKey = 1, version = 1, body = "{}")))
            }
            assertEquals(PhotonNative.PHOTON_ERR_INVALID_ARGUMENT, empty.code)
            assertTrue(!empty.nativeMessage.isNullOrEmpty(), "текст причины обязан доезжать через ABI")

            val crossing = assertFailsWith<PhotonException> {
                c.localFeedIngest(
                    "ch",
                    FeedBlock.After(
                        anchor = FeedPosition(orderKey = 500, id = "anchor"),
                        records = listOf(FeedRecord("older", 100, 1, body("старее якоря"))),
                        atEnd = false,
                    )
                )
            }
            assertEquals(PhotonNative.PHOTON_ERR_INVALID_ARGUMENT, crossing.code)

            val raw = page(c, "ch")
            assertTrue(raw.contains(""""records":[]"""), "отвергнутый блок оставил записи: $raw")
            assertIs<FeedGapAnchor.None>(
                c.localFeedGapAnchor("ch"),
                "отвергнутый блок создал сегмент"
            )
        }
    }

    /**
     * Инстанс без секции `storage` — законное состояние, а не ошибка создания
     * (ADR-064 п. 1.5). Локальное действие на нём отвечает ОТКАЗОМ ВЫЗОВА с
     * кодом `PHOTON_ERR_NOT_CONNECTED`, и причина доступна текстом.
     */
    @Test
    fun `без секции storage запись отвечает NOT_CONNECTED, а не тишиной`() {
        client(db = null).use { c ->
            val e = assertFailsWith<PhotonException> {
                c.localFeedPut("ch", listOf(FeedRecord("a", 1, 1, "{}")))
            }
            assertEquals(PhotonNative.PHOTON_ERR_NOT_CONNECTED, e.code)
            assertTrue(
                e.nativeMessage.orEmpty().contains("storage"),
                "причина обязана называть storage, иначе отказ неотличим от сетевого: ${e.nativeMessage}"
            )
        }
    }

    // ── 7. Сетевого хода у локальной записи нет ──────────────────────────────

    /**
     * Измерение, а не утверждение.
     *
     * Урок Phase 13.5 записан прямо: проверка «отвергнуто без сети» на
     * инстансе, у которого сети нет вовсе, НЕ ИЗМЕРЯЕТ НИЧЕГО и проходит на
     * любой мутации. Поэтому здесь у инстанса Bridge ЕСТЬ: endpoint указывает
     * на сокет, поднятый этим тестом, и сокет считает пакеты.
     *
     * Утверждение: три действия записи не отправляют в него НИ ОДНОГО пакета.
     * Контроль тут же доказывает, что сокет вообще способен их увидеть —
     * сетевое действие 200 стучится в тот же порт. Без контроля «ноль
     * пакетов» означало бы только то, что измерять было нечем.
     *
     * QUIC — это UDP, поэтому попытка подключения видна первым же датаграммом.
     */
    @Test
    fun `801, 802 и 803 не открывают сетевого соединения`() {
        DatagramSocket(0, InetAddress.getLoopbackAddress()).use { probe ->
            val db = tempDb()
            val bridge = BridgeConfig(
                endpoint = "127.0.0.1:${probe.localPort}",
                connectTimeout = Duration.ofSeconds(1),
                callTimeout = Duration.ofSeconds(3),
                tls = TlsConfig(insecureSkipVerify = true),
            )

            client(db, bridge).use { c ->
                assertIs<FeedPutResult.Stored>(
                    c.localFeedPut("ch", listOf(FeedRecord("a", 10, 11, body("локально"))))
                )
                assertIs<FeedIngestResult.Applied>(
                    c.localFeedIngest(
                        "ch",
                        FeedBlock.Newest(
                            records = listOf(FeedRecord("a", 10, 11, body("локально"))),
                            atEnd = true,
                        )
                    )
                )
                assertIs<FeedGapAnchor.None>(c.localFeedGapAnchor("ch"))

                probe.soTimeout = 500
                val quiet = DatagramPacket(ByteArray(2048), 2048)
                assertFailsWith<SocketTimeoutException>(
                    "локальное действие ушло в сеть: сокет Bridge получил датаграмму"
                ) { probe.receive(quiet) }

                // Контроль: сетевое действие в этот же порт стучится.
                assertFailsWith<PhotonException> { c.sendAction(ACTION_NETWORK, "{}") }

                probe.soTimeout = 3000
                val seen = DatagramPacket(ByteArray(2048), 2048)
                probe.receive(seen) // не бросил — пакеты сюда доходят
                assertTrue(seen.length > 0, "контрольный пакет пуст: измерение недействительно")
            }
        }
    }
}
