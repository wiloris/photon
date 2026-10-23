/*
 * photon-devtools-core — платформонезависимая часть Developer-Tools: лента
 * Inspector Live, снимок, CoreJournal, панель «Путь» (ADR-067, поправка
 * 2026-10-23). Чистый Kotlin поверх photon-sdk и ничего больше: потребители —
 * консольный инструмент (photon-developer-tools) и раздел «Для
 * разработчиков» ENGRAM. Подключение SDK — тем же механизмом, что у
 * инструмента (ADR-062 п. 7): includeBuild + подстановка.
 */

rootProject.name = "photon-devtools-core"

includeBuild("../photon-sdk") {
    dependencySubstitution {
        substitute(module("dev.photon:photon-sdk")).using(project(":"))
    }
}
