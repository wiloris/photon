import org.gradle.internal.os.OperatingSystem

/*
 * Photon Developer Tools — сборка первого Desktop-клиента.
 *
 * Область (ADR-051): Linux x86-64 и только он. Кроссплатформенная упаковка,
 * публикация артефактов и релизы здесь НЕ проектируются.
 *
 * Проект ходит тем же публичным путём, что и будущие приложения:
 *   Kotlin → JNI → C ABI → Photon Core
 * Прямых импортов внутренних пакетов ядра нет и быть не может: единственная
 * граница — photon_core.h (ROADMAP.md, Phase 16, «Public API constraint»).
 *
 * Go и C-компилятор — ПРЕДУСЛОВИЯ среды, а не то, что Gradle добывает сам.
 */

plugins {
    kotlin("jvm") version "2.0.21"
    application
}

repositories { mavenCentral() }

kotlin { jvmToolchain(21) }

val repoRoot: File = rootDir.parentFile
val bridgeDir: File = File(repoRoot, "photon-bindings/jvm")
val cAbiDir: File = File(repoRoot, "photon-bindings/c")

/*
 * Каталог, из которого JVM грузит нативные библиотеки. ОБЕ лежат рядом
 * намеренно: libphoton_jni.so слинкована с -rpath '$ORIGIN', поэтому
 * динамический загрузчик находит libphoton_core.so соседом, и ручной
 * LD_LIBRARY_PATH не нужен ни при запуске, ни в инструкции.
 */
val nativeDir: File = layout.buildDirectory.dir("native").get().asFile

/*
 * Исходники моста подключаются НАПРЯМУЮ из photon-bindings/jvm: публикуемого
 * артефакта Kotlin-обёртки пока нет, и заводить его ради первого экрана —
 * решать больше, чем требует Phase 16 (ADR-051).
 *
 * Тестовые точки входа моста исключены: у каждой свой main(), и вместе с
 * приложением они дали бы несколько кандидатов на запуск.
 */
sourceSets {
    main {
        java.srcDir(File(bridgeDir, "java"))
        java.exclude("PhotonJniTest.java")
        kotlin.srcDir(File(bridgeDir, "kotlin"))
        kotlin.exclude("PhotonKotlinTest.kt", "PhotonEventsTest.kt")
    }
}

application {
    mainClass.set("dev.photon.devtools.MainKt")
    applicationDefaultJvmArgs = listOf("-Djava.library.path=${nativeDir.absolutePath}")
}

/* ── native ─────────────────────────────────────────────────────────────── */

val checkPlatform by tasks.registering {
    doFirst {
        val os = OperatingSystem.current()
        val arch = System.getProperty("os.arch")
        require(os.isLinux && arch == "amd64") {
            "ADR-051: область — Linux x86-64. Обнаружено: $os / $arch. " +
            "Windows, macOS и другие архитектуры этим шагом не поддерживаются намеренно."
        }
    }
}

/*
 * libphoton_core.so собирается ТОЛЬКО в photon-bindings/c/build/.
 * -buildmode=c-shared кладёт сгенерированный заголовок рядом с выходным
 * файлом и по его имени; сборка в корень пакета молча затирает
 * photon_core.h — замороженный контракт ABI.
 */
val buildCoreLib by tasks.registering(Exec::class) {
    dependsOn(checkPlatform)
    workingDir = cAbiDir
    commandLine("go", "build", "-buildmode=c-shared", "-o", "build/libphoton_core.so", ".")
    inputs.dir(cAbiDir).withPropertyName("cAbiSources")
    outputs.file(File(cAbiDir, "build/libphoton_core.so"))
}

val copyCoreLib by tasks.registering(Copy::class) {
    dependsOn(buildCoreLib)
    from(File(cAbiDir, "build/libphoton_core.so"))
    into(nativeDir)
}

/*
 * JNI-мост. Заголовки JDK берутся из toolchain'а, а не из переменной среды:
 * иначе сборка зависела бы от того, что настроено у запускающего.
 */
val buildJniLib by tasks.registering(Exec::class) {
    dependsOn(copyCoreLib)
    val javaHome = javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(21)) }
        .get().metadata.installationPath.asFile
    doFirst { nativeDir.mkdirs() }
    commandLine(
        "cc", "-shared", "-fPIC", "-O2", "-Wall", "-Wextra",
        "-o", File(nativeDir, "libphoton_jni.so").absolutePath,
        File(bridgeDir, "jni/photon_jni.c").absolutePath,
        "-I${File(javaHome, "include").absolutePath}",
        "-I${File(javaHome, "include/linux").absolutePath}",
        "-I${cAbiDir.absolutePath}",
        "-L${nativeDir.absolutePath}", "-lphoton_core",
        // $ORIGIN — поиск libphoton_core.so рядом с самим мостом.
        "-Wl,-rpath,\$ORIGIN"
    )
    inputs.file(File(bridgeDir, "jni/photon_jni.c"))
    outputs.file(File(nativeDir, "libphoton_jni.so"))
}

tasks.register("native") { dependsOn(buildJniLib) }

tasks.named("run") { dependsOn(buildJniLib) }
tasks.named("classes") { dependsOn(buildJniLib) }
