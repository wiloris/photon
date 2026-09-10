package dev.photon.devtools

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/*
 * Тесты чистых функций Network Lab: сети и native здесь не нужно, а свойства
 * проверяются те, которые иначе проверить нечем.
 *
 * Главное из них — что вердикт сервера `ok:false` НЕ считается сетевым
 * сбоем. Без этого теста ошибка в классификации выглядела бы как сетевая
 * деградация, и диагностика уводила бы в другую сторону.
 */
class NetworkLabTest {

    @Test
    fun `перцентили считаются методом nearest-rank`() {
        val l = Latencies((1..100).map { it.toDouble() })
        assertEquals(100, l.count)
        assertEquals(1.0, l.min)
        assertEquals(100.0, l.max)
        assertEquals(50.0, l.p50)
        assertEquals(95.0, l.p95)
        assertEquals(99.0, l.p99)
    }

    @Test
    fun `перцентили не выходят за границы на малых выборках`() {
        val one = Latencies(listOf(7.0))
        assertEquals(7.0, one.p50)
        assertEquals(7.0, one.p99)
        assertEquals(7.0, one.max)

        val empty = Latencies(emptyList())
        assertEquals(0.0, empty.p95)
        assertEquals(0, empty.count)
    }

    @Test
    fun `порядок выборки не влияет на перцентили`() {
        val asc = Latencies(listOf(1.0, 2.0, 3.0, 4.0, 5.0))
        val desc = Latencies(listOf(5.0, 4.0, 3.0, 2.0, 1.0))
        assertEquals(asc.p50, desc.p50)
        assertEquals(asc.p95, desc.p95)
        assertEquals(asc.min, desc.min)
    }

    @Test
    fun `вердикт ok false не считается сетевым сбоем`() {
        assertEquals(Outcome.VERDICT_FALSE,
            classifyResponse("""{"ok":false,"error":"target_user_id is required"}"""))
        assertEquals(Outcome.OK, classifyResponse("""{"ok":true,"data":{"id":"abc"}}"""))
        assertEquals("target_user_id is required",
            verdictReason("""{"ok":false,"error":"target_user_id is required"}"""))
    }

    @Test
    fun `пробелы в конверте не меняют классификацию`() {
        assertEquals(Outcome.VERDICT_FALSE, classifyResponse("""{ "ok" : false , "error":"нет" }"""))
        assertEquals(Outcome.OK, classifyResponse("""{ "ok" : true }"""))
    }

    @Test
    fun `сводка разделяет три исхода и считает throughput`() {
        val r = ScenarioResult("t", "", 1000, listOf(
            CallResult(Outcome.OK, "{}", "", 10_000_000),
            CallResult(Outcome.OK, "{}", "", 20_000_000),
            CallResult(Outcome.VERDICT_FALSE, "{}", "", 30_000_000),
            CallResult(Outcome.CALL_ERROR, null, "", 40_000_000),
        ))
        assertEquals(4, r.total)
        assertEquals(2, r.ok)
        assertEquals(1, r.verdictFalse)
        assertEquals(1, r.callError)
        assertEquals(4.0, r.throughput)
        // Несостоявшийся вызов в задержку не входит: у него измеряется время
        // до отказа, а это другая величина.
        assertEquals(3, r.latency.count)
        assertEquals(30.0, r.latency.max)
    }

    @Test
    fun `деградация отказывается сравнивать короткую серию`() {
        assertTrue(degradation(listOf(1.0, 2.0, 3.0)).contains("короче"))
    }

    @Test
    fun `деградация видит рост задержки к концу серии`() {
        val samples = (1..50).map { 10.0 } + (1..50).map { 40.0 }
        val text = degradation(samples)
        assertTrue(text.contains("отношение 4.00"), "получено: $text")
    }
}
