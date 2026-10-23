/*
 * Photon SDK — публичная поверхность для приложений.
 *
 * Спецификация: photon-bindings/docs/sdk-api.md (написана ДО этого кода).
 * Решение о существовании модуля и его размещении — ADR-062 п. 2 и §7.3
 * спецификации.
 *
 * Что этот модуль НЕ делает:
 *  - не собирает native (.so): их собирает потребитель, jar самодостаточным
 *    не является (§7.3). Здесь нет ни go, ни cc — сборка чисто JVM;
 *  - не публикуется: координаты Maven и публикация отложены ADR-051 и
 *    ADR-062 п. 8;
 *  - не знает о Developer Tools: зависимость направлена в обратную сторону.
 */

plugins {
    kotlin("jvm") version "2.0.21"
}

repositories { mavenCentral() }

dependencies {
    /*
     * JSON — §7.1 спецификации: ТОЛЬКО tree-API.
     *
     * Плагин компилятора сознательно НЕ подключается: `@Serializable` здесь
     * не используется, а без него плагин не нужен (измерено). Появление
     * плагина в этом файле означало бы, что кто-то ввёл автоматический
     * маппинг — то самое, что §7.1 запрещает без отдельного решения.
     */
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")

    testImplementation(kotlin("test"))
}

/*
 * Мост подключается ИСХОДНИКАМИ — той же конструкцией, какой сегодня
 * пользуется Developer Tools (его build.gradle.kts:72-78).
 *
 * Причина та же, по которой мост вообще один: опубликованного артефакта
 * Kotlin-обёртки нет (ADR-051), а вторая копия маршалинга запрещена
 * ADR-062 п. 2. После перевода инструмента на SDK эта конструкция остаётся
 * ровно в ОДНОМ месте — здесь.
 *
 * Тестовые точки входа моста исключаются по именам: в photon-bindings/jvm
 * production-код и тесты лежат в одних каталогах, у каждого теста свой
 * main(), и вместе они дали бы несколько кандидатов на запуск. Это записано
 * в §7.3 как работа, которую решение делает обязательной.
 */
val bridgeDir: File = File(rootDir.parentFile, "photon-bindings/jvm")

sourceSets {
    main {
        java.srcDir(File(bridgeDir, "java"))
        java.exclude("PhotonJniTest.java")
        kotlin.srcDir(File(bridgeDir, "kotlin"))
        kotlin.exclude("PhotonKotlinTest.kt", "PhotonEventsTest.kt")
    }
}

kotlin { jvmToolchain(21) }

tasks.test { useJUnitPlatform() }
