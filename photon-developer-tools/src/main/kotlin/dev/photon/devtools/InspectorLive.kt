package dev.photon.devtools

import dev.photon.sdk.PhotonClient

/*
 * Inspector Live — консольный режим (ADR-130 §2.7). Лента, снимок и панель
 * «Путь» — общая часть photon-devtools-core (LiveFeed.kt, ConnectionPath.kt);
 * здесь — только консольный цикл и имена действий из канонического каталога
 * `photon.proto` (тот же дескриптор, что у Protobuf Inspector), своей копии нет.
 */

/**
 * Консольный режим: опрос 820 с периодом [periodMs] (по умолчанию 250 мс,
 * ADR-130 §2.7) и снимок 819 раз в [snapshotEveryMs].
 */
fun runInspectorLive(client: PhotonClient, catalog: Inspector?, seconds: Int, periodMs: Long = 250, snapshotEveryMs: Long = 2000) {
    val names = catalog?.enum("Action")?.valueList?.associate { it.number to it.name } ?: emptyMap()
    val feed = LiveFeed(client.diagnosticsSource(), { names[it] }, catalogLoaded = catalog != null)
    val path = PathTracker()
    val source = client.diagnosticsSource()
    println("Inspector Live — события и снимки Photon Core (819/820)")
    println(if (catalog != null) "имена действий: ${catalog.sourceName}" else "каталог не загружен: действия — числами")
    println(LiveFeed.TIME_NOTE)
    val until = System.currentTimeMillis() + seconds * 1000L
    var nextSnapshot = 0L
    while (System.currentTimeMillis() < until) {
        val lines = feed.poll()
        lines.forEach { println(it.text()) }
        path.accept(lines)
        if (System.currentTimeMillis() >= nextSnapshot) {
            println("── снимок ".padEnd(64, '─'))
            renderSnapshot(source.snapshot()).forEach(::println)
            println("── путь ".padEnd(64, '─'))
            renderPath(path.view()).forEach(::println)
            nextSnapshot = System.currentTimeMillis() + snapshotEveryMs
        }
        Thread.sleep(periodMs)
    }
    println("── путь (итог) ".padEnd(64, '─'))
    renderPath(path.view()).forEach(::println)
    println("── итог: курсор ${feed.cursor}, перезапусков ${feed.restarts}, вытеснено ${feed.droppedTotal} ──")
}
