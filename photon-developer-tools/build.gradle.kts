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

dependencies {
    /*
     * protobuf-java нужен ровно для одного: прочитать FileDescriptorSet,
     * который выдаёт protoc. Кодогенерация Java НЕ используется — 154 файла
     * и 3.2 МБ классов ради трёх enum'ов были бы платой ни за что.
     *
     * Версия привязана к версии protoc, которой сгенерирован photon.pb.go
     * (v3.21.12): дескриптор и Go-код обязаны происходить из одного
     * компилятора, иначе «канонический» каталог зависел бы от того, что
     * стоит на машине.
     */
    implementation("com.google.protobuf:protobuf-java:3.21.12")

    /*
     * Photon SDK — публичный путь к ядру (ADR-062 п. 7). Пришёл на смену
     * прямому подключению исходников моста из photon-bindings/jvm: инструмент
     * ходит тем же путём, что и будущие приложения, а не своей дорогой.
     *
     * Подстановка на композитную сборку объявлена в settings.gradle.kts.
     * Классы моста (`dev.photon.core.*`) приходят внутри артефакта SDK —
     * второй копии маршалинга не появляется (ADR-062 п. 2).
     */
    implementation("dev.photon:photon-sdk")

    testImplementation(kotlin("test"))
}

tasks.test { useJUnitPlatform() }

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
 * Канонический каталог протокола: FileDescriptorSet, который выдаёт сам
 * protoc. Единственный источник истины — photon-core/protobuf/photon.proto;
 * дескриптор из него ВЫВОДИТСЯ и потому в репозиторий не коммитится —
 * иначе появилась бы вторая вещь, способная отстать.
 */
val protoFile: File = File(repoRoot, "photon-core/protobuf/photon.proto")
val descriptorFile: File = layout.buildDirectory.file("descriptor/photon.desc").get().asFile

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

/*
 * protoc — предусловие среды, как go и cc (ADR-051). Проверяется ЯВНО:
 * иначе отсутствие компилятора вылезло бы невнятным стеком Exec-задачи, а
 * не сообщением о том, чего не хватает.
 */
val checkProtoc by tasks.registering {
    doFirst {
        val found = try {
            val p = ProcessBuilder("protoc", "--version").redirectErrorStream(true).start()
            p.waitFor() == 0
        } catch (e: Exception) {
            false
        }
        if (!found) {
            throw GradleException(
                "protoc не найден в PATH.\n" +
                "Protobuf Inspector читает FileDescriptorSet, который выдаёт protoc;\n" +
                "своего парсера .proto у инструмента нет и не будет.\n" +
                "protoc — предусловие среды, как go и cc (ADR-051).\n" +
                "Ubuntu/Debian: sudo apt install protobuf-compiler\n" +
                "Проверить: protoc --version   (ожидается 3.21.x)"
            )
        }
    }
}

/*
 * --include_source_info обязателен: без него в дескрипторе НЕТ комментариев
 * схемы, а групповые заголовки диапазонов (── PostResource (100-114) ──) —
 * единственное человекочитаемое описание, которое в photon.proto есть.
 */
val generateDescriptor by tasks.registering(Exec::class) {
    dependsOn(checkProtoc)
    doFirst { descriptorFile.parentFile.mkdirs() }
    workingDir = File(repoRoot, "photon-core/protobuf")
    commandLine(
        "protoc",
        "--descriptor_set_out=${descriptorFile.absolutePath}",
        "--include_source_info",
        "photon.proto"
    )
    inputs.file(protoFile)
    outputs.file(descriptorFile)
}

tasks.register("descriptor") { dependsOn(generateDescriptor) }

/*
 * Plugin Lab (ADR-057): собственный экземпляр runtime и его фикстура.
 *
 * Два отдельных бинаря, а не один: runtime — то, чем инструмент управляет,
 * фикстура — плагин, которым управляет уже runtime. Собираются из
 * photon-plugin-runtime, потому что там они и живут; Developer Tools их
 * только запускает.
 *
 * Это НЕ production daemon и не часть релиза инструмента — артефакты
 * лаборатории, лежащие рядом с нативными библиотеками.
 */
val runtimeModuleDir: File = File(repoRoot, "photon-plugin-runtime")

val buildLabRuntime by tasks.registering(Exec::class) {
    workingDir = runtimeModuleDir
    doFirst { nativeDir.mkdirs() }
    commandLine("go", "build", "-o",
        File(nativeDir, "photon-devtools-runtime").absolutePath, "./cmd/devtools-runtime")
    inputs.dir(File(runtimeModuleDir, "cmd/devtools-runtime"))
    outputs.file(File(nativeDir, "photon-devtools-runtime"))
}

/*
 * Закрытый набор фикстур (ADR-058 п. 6): hello и sdkscenario. Какой сценарий
 * запускать, решает каталог devtools-runtime; Gradle только собирает бинари и
 * передаёт пути — из управляющего канала путь не выбирается.
 */
val buildLabFixtureHello by tasks.registering(Exec::class) {
    workingDir = runtimeModuleDir
    doFirst { nativeDir.mkdirs() }
    commandLine("go", "build", "-o",
        File(nativeDir, "photon-lab-fixture-hello").absolutePath, "./tests/plugins/hello")
    inputs.dir(File(runtimeModuleDir, "tests/plugins/hello"))
    outputs.file(File(nativeDir, "photon-lab-fixture-hello"))
}

val buildLabFixtureSDKScenario by tasks.registering(Exec::class) {
    workingDir = runtimeModuleDir
    doFirst { nativeDir.mkdirs() }
    commandLine("go", "build", "-o",
        File(nativeDir, "photon-lab-fixture-sdkscenario").absolutePath, "./tests/plugins/sdkscenario")
    inputs.dir(File(runtimeModuleDir, "tests/plugins/sdkscenario"))
    outputs.file(File(nativeDir, "photon-lab-fixture-sdkscenario"))
}

tasks.register("pluginLabNative") { dependsOn(buildLabRuntime, buildLabFixtureHello, buildLabFixtureSDKScenario) }

tasks.register("native") { dependsOn(buildJniLib) }

tasks.named("run") { dependsOn(buildJniLib, generateDescriptor, buildLabRuntime, buildLabFixtureHello, buildLabFixtureSDKScenario) }
tasks.named("classes") { dependsOn(buildJniLib) }

// Путь к дескриптору не «где-то рядом», а передаётся явно: инструмент не
// должен угадывать расположение сгенерированного артефакта.
tasks.named<JavaExec>("run") {
    systemProperty("photon.descriptor", descriptorFile.absolutePath)
    // Пути артефактов лаборатории — тоже явно, по той же причине.
    systemProperty("photon.lab.runtime", File(nativeDir, "photon-devtools-runtime").absolutePath)
    systemProperty("photon.lab.fixture.hello", File(nativeDir, "photon-lab-fixture-hello").absolutePath)
    systemProperty("photon.lab.fixture.sdkscenario", File(nativeDir, "photon-lab-fixture-sdkscenario").absolutePath)
}
tasks.test {
    dependsOn(generateDescriptor)
    systemProperty("photon.descriptor", descriptorFile.absolutePath)
    systemProperty("photon.devtools.src", File(projectDir, "src/main/kotlin/dev/photon/devtools").absolutePath)
}
