/*
 * photon-devtools-core — см. settings.gradle.kts.
 *
 * Toolchain и версия Kotlin — те же, что у photon-sdk: ENGRAM Android уже
 * принимает байт-код SDK этой сборки (D8), и библиотека не должна требовать
 * от него ничего сверх того. Зависимость одна — photon-sdk; никакого
 * Android, JNI, protobuf и сети: имена действий, сеть и экран — у потребителя.
 */
plugins {
    kotlin("jvm") version "2.0.21"
}

repositories { mavenCentral() }

dependencies {
    implementation("dev.photon:photon-sdk")

    testImplementation(kotlin("test"))
}

kotlin { jvmToolchain(21) }

tasks.test { useJUnitPlatform() }
