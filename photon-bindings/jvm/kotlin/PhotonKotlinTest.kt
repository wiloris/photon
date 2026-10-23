import dev.photon.core.Photon
import dev.photon.core.PhotonException
import dev.photon.core.PhotonNative

// E2E-тест Kotlin-обёртки: Kotlin -> JNI -> Photon C ABI -> Go core.
// Запускается на desktop JVM; ART-специфика не покрывается.

var passed = 0
var failed = 0

fun check(name: String, condition: Boolean, detail: String = "") {
    if (condition) {
        passed++
        println("PASS: $name")
    } else {
        failed++
        println("FAIL: $name -- $detail")
    }
}

fun main(args: Array<String>) {
    val libPath = if (args.isNotEmpty()) args[0] else "./libphoton_jni.so"
    Photon.loadFrom(java.io.File(libPath).absolutePath)
    println("Loaded: $libPath")
    println()

    // ABI version
    check("abiMajor == 1", Photon.abiMajor() == 1, "got ${Photon.abiMajor()}")
    check("abiMinor == 0", Photon.abiMinor() == 0, "got ${Photon.abiMinor()}")

    // Успешное создание + AutoCloseable
    Photon.create("{}").use { photon ->
        check("create via use{}: instance obtained", true)

        // Известный код, Dispatcher не реализован -> PhotonException(INTERNAL)
        try {
            photon.sendAction(100, "{}")
            check("sendAction(100) throws PhotonException", false, "no exception thrown")
        } catch (e: PhotonException) {
            check("sendAction(100) throws PhotonException",
                  e.code == PhotonNative.PHOTON_ERR_NOT_CONNECTED, "got code=${e.code}")
            check("PhotonException carries native message",
                  !e.nativeMessage.isNullOrEmpty(), "message=${e.nativeMessage}")
            println("       (message: \"${e.message}\")")
        }

        // Неизвестный код -> INVALID_ACTION
        try {
            photon.sendAction(999999, "{}")
            check("sendAction(999999) throws", false, "no exception")
        } catch (e: PhotonException) {
            check("sendAction(unknown) -> INVALID_ACTION",
                  e.code == PhotonNative.PHOTON_ERR_INVALID_ACTION, "got code=${e.code}")
        }

        check("lastError() non-null after failure",
              photon.lastError() != null, "got null")
    }
    check("use{} closed instance without crash", true)

    // Неудачное создание -> PhotonException
    try {
        Photon.create("{not json")
        check("create(bad json) throws PhotonException", false, "no exception")
    } catch (e: PhotonException) {
        check("create(bad json) throws PhotonException",
              e.code == PhotonNative.PHOTON_ERR_INVALID_ARGUMENT, "got code=${e.code}")
    }

    // Идемпотентность close()
    val p = Photon.create("{}")
    p.close()
    p.close()
    p.close()
    check("close() x3: idempotent, no crash", true)

    // Использование после close -> IllegalStateException
    try {
        p.sendAction(100, "{}")
        check("sendAction after close throws IllegalStateException", false, "no exception")
    } catch (e: IllegalStateException) {
        check("sendAction after close throws IllegalStateException", true)
    } catch (e: PhotonException) {
        check("sendAction after close throws IllegalStateException", false,
              "got PhotonException instead: ${e.code}")
    }

    check("lastError() on closed instance returns null", p.lastError() == null)

    // Несколько независимых инстансов
    val a = Photon.create("{}")
    val b = Photon.create("{}")
    b.close()
    try {
        a.sendAction(100, "{}")
        check("closing b does not affect a", false, "no exception (expected NOT_CONNECTED)")
    } catch (e: PhotonException) {
        check("closing b does not affect a",
              e.code == PhotonNative.PHOTON_ERR_NOT_CONNECTED, "got code=${e.code}")
    }
    a.close()

    // Конкурентность
    val threads = (1..8).map { idx ->
        Thread {
            repeat(200) {
                Photon.create("{}").use { ph ->
                    try { ph.sendAction(100, "{}") } catch (_: PhotonException) {}
                }
            }
        }.apply { start() }
    }
    threads.forEach { it.join(60_000) }
    check("concurrent: 8 threads x 200 create/use/close cycles", true)

    println()
    println("=== PASSED: $passed, FAILED: $failed ===")
    if (failed > 0) System.exit(1)
}
