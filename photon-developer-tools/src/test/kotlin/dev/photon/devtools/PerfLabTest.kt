package dev.photon.devtools

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/*
 * Тесты Performance: чистые функции, без сети и native.
 *
 * Проверяется главным образом честность выводимых величин — что производная
 * оценка не притворяется измерением и не уходит в минус.
 */
class PerfLabTest {

    @Test
    fun `доля сети считается разностью медиан`() {
        assertEquals(80.0, derivedNetworkShare(fullP50 = 90.0, localP50 = 10.0))
        assertEquals(0.5, derivedNetworkShare(1.0, 0.5))
    }

    @Test
    fun `отрицательной доли сети не бывает — это шум, а не отрицательная сеть`() {
        // Локальный путь «дороже» полного означает шум измерения. Показывать
        // минус значило бы утверждать невозможное.
        assertEquals(0.0, derivedNetworkShare(fullP50 = 5.0, localP50 = 9.0))
        assertEquals(0.0, derivedNetworkShare(0.0, 0.0))
    }

    @Test
    fun `масштабирование считается относительно одного потока`() {
        assertEquals(1.0, scaling(10.0, 10.0))
        assertEquals(4.0, scaling(10.0, 40.0))
        // Деления на ноль быть не должно: нулевая база даёт 0, а не бесконечность.
        assertEquals(0.0, scaling(0.0, 40.0))
    }

    @Test
    fun `снимок памяти различает кучу JVM и RSS процесса`() {
        val m = MemorySnapshot.take()
        assertTrue(m.jvmUsedBytes > 0, "куча JVM обязана быть измерима")
        // RSS читается из /proc; на не-Linux вернётся 0 — и это не провал,
        // а отсутствие источника. Проверяем лишь, что величины РАЗНЫЕ поля.
        assertTrue(m.rssKb >= 0)
        assertTrue(m.hwmKb >= m.rssKb || m.hwmKb == 0L,
            "пик не может быть меньше текущего RSS: hwm=${m.hwmKb} rss=${m.rssKb}")
    }

    @Test
    fun `зонды объявляют, включают ли они сеть`() {
        val local = Probe("x", "JNI + cgo", network = false, latency = Latencies(listOf(1.0)))
        val full = Probe("y", "всё", network = true, latency = Latencies(listOf(90.0)))
        assertTrue(!local.network && full.network)
        // Именно по этому признаку выбирается пара для вычитания: смешать их
        // значило бы вычесть сеть из сети.
        assertEquals(89.0, derivedNetworkShare(full.latency.p50, local.latency.p50))
    }

    @Test
    fun `перцентили зондов берутся из общей реализации Network Lab`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "PerfLab.kt").readText()
        assertTrue(src.contains("Latencies("),
            "PerfLab обязан использовать Latencies, а не заводить свой расчёт перцентилей")
        assertTrue(!src.contains("fun percentile("),
            "в PerfLab не должно быть второй реализации перцентилей")
    }

    @Test
    fun `отчёт не называет производную оценку измеренным RTT`() {
        val src = java.io.File(System.getProperty("photon.devtools.src"), "PerfLab.kt").readText()
        assertTrue(src.contains("ПРОИЗВОДНАЯ, не измерение"),
            "доля сети обязана быть подписана как производная")
        assertTrue(src.contains("не измеренный RTT") || src.contains("а не"),
            "в отчёте должно быть сказано, чем эта величина НЕ является")
    }
}
