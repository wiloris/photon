// Photon SDK — публичный Kotlin/JVM API поверх C ABI (ADR-062).
//
// ОТДЕЛЬНЫЙ корень сборки, а не подпроект Developer Tools: инструмент —
// потребитель SDK (ADR-062 п. 7), и сборка, где потребитель является корнем
// для платформенного модуля, перевернула бы это отношение. Подключение —
// композитом (`includeBuild`), см. photon-bindings/docs/sdk-api.md §7.3.
rootProject.name = "photon-sdk"
