package dev.photon.devtools

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/*
 * Тесты потока Logging: чистые функции, без сети и native.
 *
 * Проверяются свойства, которые иначе видны только глазами на живом прогоне:
 * порядок записей, вывод уровня из исхода, работа фильтров и то, что
 * корреляция не выдумывает связь там, где её нет.
 */
class LogViewTest {

    @Test
    fun `уровень выводится из исхода, а не назначается вручную`() {
        assertEquals(Level.INFO, levelOf(Outcome.OK))
        // Вердикт сервера — предупреждение: вызов СОСТОЯЛСЯ.
        assertEquals(Level.WARN, levelOf(Outcome.VERDICT_FALSE))
        // Несостоявшийся вызов — ошибка.
        assertEquals(Level.ERROR, levelOf(Outcome.CALL_ERROR))
    }

    @Test
    fun `порядок задаётся seq, а не отметкой времени`() {
        val s = LogStream()
        val a = s.add(Source.ACTION, Level.INFO, "one", "первая")
        val b = s.add(Source.EVENT, Level.INFO, "two", "вторая")
        val c = s.add(Source.ACTION, Level.INFO, "three", "третья")
        assertEquals(listOf(a.seq, b.seq, c.seq), s.all().map { it.seq })
        assertTrue(a.seq < b.seq && b.seq < c.seq)
        // Отметка времени двух соседних записей может совпасть до
        // миллисекунды — на порядок это влиять не должно.
        assertEquals(listOf("первая", "вторая", "третья"), s.all().map { it.text })
    }

    @Test
    fun `фильтры отбирают по источнику, уровню, компоненту и подстроке`() {
        val s = LogStream()
        s.add(Source.ACTION, Level.INFO, "ACTION_GET_ME", "состояние субъекта")
        s.add(Source.ACTION, Level.WARN, "ACTION_GET_OR_CREATE_DIRECT_CHANNEL", "target_user_id is required")
        s.add(Source.ACTION, Level.ERROR, "НЕИЗВЕСТНЫЙ код 999999", "unknown action code")
        s.add(Source.EVENT, Level.INFO, "CONNECTED", """{"type":"CONNECTED"}""")

        assertEquals(1, s.filter(source = Source.EVENT).size)
        assertEquals(3, s.filter(source = Source.ACTION).size)
        assertEquals(2, s.filter(minLevel = Level.WARN).size)
        assertEquals(1, s.filter(minLevel = Level.ERROR).size)
        assertEquals(1, s.filter(component = "connected").size, "фильтр по компоненту без учёта регистра")
        assertEquals(1, s.filter(contains = "target_user_id").size)
        assertEquals(0, s.filter(source = Source.EVENT, minLevel = Level.WARN).size)
    }

    @Test
    fun `корреляция собирает цепочку одного действия`() {
        val s = LogStream()
        s.add(Source.ACTION, Level.INFO, "ACTION_CREATE_POST", "создан", "act-1", key = "post-aaaaaaaaaaaaaaaaaaaa")
        s.add(Source.EVENT, Level.INFO, "MESSAGE", "пришло", key = "post-aaaaaaaaaaaaaaaaaaaa")
        s.add(Source.EVENT, Level.INFO, "MESSAGE", "чужое", key = "post-bbbbbbbbbbbbbbbbbbbb")
        val chain = s.correlated("act-1")
        assertEquals(2, chain.size)
        assertEquals(listOf(Source.ACTION, Source.EVENT), chain.map { it.source })
        assertEquals(1, s.chains().size, "чужое событие цепочкой не считается")
    }

    /**
     * Регрессия на дефект, найденный живым прогоном: push приходит в ту же
     * миллисекунду, что и ответ, и иногда РАНЬШЕ, чем ответ разобран. Связь,
     * которую ищут в момент прихода события, при таком порядке теряется.
     */
    @Test
    fun `событие, пришедшее раньше ответа, всё равно попадает в цепочку`() {
        val s = LogStream()
        s.add(Source.EVENT, Level.INFO, "MESSAGE", "push обогнал ответ", key = "post-cccccccccccccccccccc")
        s.add(Source.ACTION, Level.INFO, "ACTION_CREATE_POST", "ответ разобран позже", "act-2",
              key = "post-cccccccccccccccccccc")
        val chains = s.chains()
        assertEquals(1, chains.size)
        assertEquals(2, chains["act-2"]?.size)
    }

    @Test
    fun `ключ корреляции берётся из ответа и не выдумывается`() {
        assertEquals("jgdb3ut7stnefrae5mcyd13peo",
            correlationKeyOf("""{"ok":true,"data":{"id":"jgdb3ut7stnefrae5mcyd13peo"}}"""))
        // Сквозного идентификатора запроса протокол не передаёт: если в
        // ответе нечего связывать — связи нет, а не «какая-нибудь».
        assertNull(correlationKeyOf("""{"ok":true,"data":{}}"""))
        assertNull(correlationKeyOf(null))
        assertNull(correlationKeyOf("""{"ok":false,"error":"нет"}"""))
        // Короткие id постами не бывают: 20+ символов — форма Mattermost.
        assertNull(correlationKeyOf("""{"id":"abc"}"""))
    }

    @Test
    fun `в исходниках Logging нет собственной таблицы имён действий`() {
        val srcDir = System.getProperty("photon.devtools.src")
        val src = java.io.File(srcDir, "LogView.kt").readText()
        assertTrue(Regex("\\bACTION_[A-Z_]+\\b").findAll(src).none(),
            "имена действий вписаны в LogView — они обязаны браться из каталога")
        assertTrue(src.contains("catalog?.enum(\"Action\")"),
            "имя действия должно читаться из канонического каталога")
    }
}
