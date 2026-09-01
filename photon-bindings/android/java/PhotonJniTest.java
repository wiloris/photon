import dev.photon.core.PhotonNative;

/**
 * E2E-тест JNI-моста: Java -> JNI -> Photon C ABI -> Go core.
 *
 * Проверяет реальную работу через все слои, а не только компиляцию.
 * Запускается на desktop JVM (x86-64), а не на Android ART — JNI ABI
 * и контракт одинаковы, отличается только сборка под архитектуру.
 * ART-специфика (реальные фоновые потоки, GC-политика) этим тестом НЕ
 * покрывается — см. отчёт.
 */
public class PhotonJniTest {

    private static int passed = 0;
    private static int failed = 0;

    private static void check(String name, boolean condition, String detail) {
        if (condition) {
            passed++;
            System.out.println("PASS: " + name);
        } else {
            failed++;
            System.out.println("FAIL: " + name + " -- " + detail);
        }
    }

    public static void main(String[] args) {
        String libPath = args.length > 0 ? args[0] : "./libphoton_jni.so";
        System.load(new java.io.File(libPath).getAbsolutePath());
        System.out.println("Loaded: " + libPath);
        System.out.println();

        testAbiVersion();
        testCreateDestroy();
        testCreateFailures();
        testDestroyIdempotent();
        testSendAction();
        testGetLastError();
        testMultipleHandles();
        testCreateDestroyCreate();
        testConcurrentCalls();

        System.out.println();
        System.out.println("=== PASSED: " + passed + ", FAILED: " + failed + " ===");
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ── ABI version ──────────────────────────────────────────────────

    private static void testAbiVersion() {
        int v = PhotonNative.getAbiVersion();
        int major = v >>> 16;
        int minor = v & 0xFFFF;
        check("abiVersion: MAJOR == 1", major == 1, "got MAJOR=" + major);
        check("abiVersion: MINOR == 0", minor == 0, "got MINOR=" + minor);
    }

    // ── Жизненный цикл ───────────────────────────────────────────────

    private static void testCreateDestroy() {
        long h = PhotonNative.create("{\"storage_path\":\"/tmp/photon\"}");
        check("create: returns non-zero handle", h != 0, "got handle=" + h);
        check("create: result is PHOTON_OK",
              PhotonNative.lastCreateResult() == PhotonNative.PHOTON_OK,
              "got rc=" + PhotonNative.lastCreateResult());
        PhotonNative.destroy(h);
        check("destroy: did not crash", true, "");
    }

    private static void testCreateFailures() {
        long h1 = PhotonNative.create(null);
        check("create(null): returns 0", h1 == 0, "got handle=" + h1);
        check("create(null): INVALID_ARGUMENT",
              PhotonNative.lastCreateResult() == PhotonNative.PHOTON_ERR_INVALID_ARGUMENT,
              "got rc=" + PhotonNative.lastCreateResult());

        long h2 = PhotonNative.create("{not valid json");
        check("create(bad json): returns 0", h2 == 0, "got handle=" + h2);
        check("create(bad json): INVALID_ARGUMENT",
              PhotonNative.lastCreateResult() == PhotonNative.PHOTON_ERR_INVALID_ARGUMENT,
              "got rc=" + PhotonNative.lastCreateResult());

        // ADR-028: destroy(0) — безопасный no-op, безусловный вызов разрешён.
        PhotonNative.destroy(h2);
        check("destroy(0) after failed create: no crash", true, "");
    }

    private static void testDestroyIdempotent() {
        long h = PhotonNative.create("{}");
        check("create for idempotency test", h != 0, "got handle=" + h);
        PhotonNative.destroy(h);
        PhotonNative.destroy(h);   // именно этот сценарий поймал double free в C ABI
        PhotonNative.destroy(h);
        check("destroy x3 on same handle: no crash (double-free guard works)", true, "");

        PhotonNative.destroy(0);
        PhotonNative.destroy(0);
        check("destroy(0) x2: no crash", true, "");
    }

    private static void testCreateDestroyCreate() {
        long h1 = PhotonNative.create("{}");
        PhotonNative.destroy(h1);
        long h2 = PhotonNative.create("{}");
        check("create -> destroy -> create: second create succeeds",
              h2 != 0, "got handle=" + h2);
        check("create -> destroy -> create: handles differ",
              h1 != h2, "h1=" + h1 + " h2=" + h2);
        PhotonNative.destroy(h2);
    }

    // ── SendAction ───────────────────────────────────────────────────

    private static void testSendAction() {
        long h = PhotonNative.create("{}");

        // Неизвестный action_code -> PHOTON_ERR_INVALID_ACTION (ADR-026 п.5)
        PhotonNative.sendAction(h, 999999, "{}");
        check("sendAction(unknown code): INVALID_ACTION",
              PhotonNative.lastSendActionResult() == PhotonNative.PHOTON_ERR_INVALID_ACTION,
              "got rc=" + PhotonNative.lastSendActionResult());

        // action_code == 0 (ACTION_UNSPECIFIED) тоже отвергается
        PhotonNative.sendAction(h, 0, "{}");
        check("sendAction(code 0): INVALID_ACTION",
              PhotonNative.lastSendActionResult() == PhotonNative.PHOTON_ERR_INVALID_ACTION,
              "got rc=" + PhotonNative.lastSendActionResult());

        // Известный код (100 = ACTION_CREATE_POST) на инстансе, созданном из
        // "{}", то есть без секции bridge: адресата нет ->
        // PHOTON_ERR_NOT_CONNECTED, НЕ INVALID_ACTION.
        //
        // До Phase 13.5 здесь стояла PHOTON_ERR_INTERNAL — честная заглушка
        // "dispatcher не существует". Смена кода — смена контракта, а не
        // регресс (photon-core/docs/action-routing.md §10).
        PhotonNative.sendAction(h, 100, "{}");
        check("sendAction(known code 100): NOT_CONNECTED, not INVALID_ACTION",
              PhotonNative.lastSendActionResult() == PhotonNative.PHOTON_ERR_NOT_CONNECTED,
              "got rc=" + PhotonNative.lastSendActionResult());

        // Все известные коды не должны отвергаться как INVALID_ACTION
        int[] knownCodes = {100, 114, 200, 210, 300, 314, 400, 405, 500, 505, 600, 602};
        boolean allAccepted = true;
        int rejected = -1;
        for (int code : knownCodes) {
            PhotonNative.sendAction(h, code, "{}");
            if (PhotonNative.lastSendActionResult() == PhotonNative.PHOTON_ERR_INVALID_ACTION) {
                allAccepted = false;
                rejected = code;
                break;
            }
        }
        check("sendAction: all known action codes accepted",
              allAccepted, "code " + rejected + " rejected as INVALID_ACTION");

        // null payload -> INVALID_ARGUMENT
        PhotonNative.sendAction(h, 100, null);
        check("sendAction(null payload): INVALID_ARGUMENT",
              PhotonNative.lastSendActionResult() == PhotonNative.PHOTON_ERR_INVALID_ARGUMENT,
              "got rc=" + PhotonNative.lastSendActionResult());

        PhotonNative.destroy(h);

        // Вызов на уничтоженном handle -> INVALID_ARGUMENT, без падения
        PhotonNative.sendAction(h, 100, "{}");
        check("sendAction on destroyed handle: INVALID_ARGUMENT, no crash",
              PhotonNative.lastSendActionResult() == PhotonNative.PHOTON_ERR_INVALID_ARGUMENT,
              "got rc=" + PhotonNative.lastSendActionResult());

        // Вызов с handle == 0
        PhotonNative.sendAction(0, 100, "{}");
        check("sendAction(handle 0): INVALID_ARGUMENT, no crash",
              PhotonNative.lastSendActionResult() == PhotonNative.PHOTON_ERR_INVALID_ARGUMENT,
              "got rc=" + PhotonNative.lastSendActionResult());
    }

    // ── GetLastError ─────────────────────────────────────────────────

    private static void testGetLastError() {
        check("getLastError(0): returns null",
              PhotonNative.getLastError(0) == null, "expected null");

        long h = PhotonNative.create("{}");
        PhotonNative.sendAction(h, 999999, "{}");
        String err = PhotonNative.getLastError(h);
        check("getLastError after failure: non-null", err != null, "got null");
        check("getLastError after failure: non-empty",
              err != null && !err.isEmpty(), "got: " + err);
        System.out.println("       (message: \"" + err + "\")");
        PhotonNative.destroy(h);

        check("getLastError on destroyed handle: null, no crash",
              PhotonNative.getLastError(h) == null, "expected null");
    }

    // ── Несколько независимых handle ─────────────────────────────────

    private static void testMultipleHandles() {
        long h1 = PhotonNative.create("{}");
        long h2 = PhotonNative.create("{}");
        long h3 = PhotonNative.create("{}");
        check("multiple handles: all non-zero",
              h1 != 0 && h2 != 0 && h3 != 0,
              "h1=" + h1 + " h2=" + h2 + " h3=" + h3);
        check("multiple handles: all distinct",
              h1 != h2 && h2 != h3 && h1 != h3,
              "h1=" + h1 + " h2=" + h2 + " h3=" + h3);

        PhotonNative.destroy(h2);

        // h1 и h3 должны остаться валидными после уничтожения h2.
        // NOT_CONNECTED, а не INVALID_ARGUMENT: хендл жив, у инстанса просто
        // нет адресата (создан из "{}"). Именно эта разница и показывает, что
        // сосед не пострадал (action-routing.md §8).
        PhotonNative.sendAction(h1, 100, "{}");
        check("destroying h2 does not invalidate h1",
              PhotonNative.lastSendActionResult() == PhotonNative.PHOTON_ERR_NOT_CONNECTED,
              "got rc=" + PhotonNative.lastSendActionResult());
        PhotonNative.sendAction(h3, 100, "{}");
        check("destroying h2 does not invalidate h3",
              PhotonNative.lastSendActionResult() == PhotonNative.PHOTON_ERR_NOT_CONNECTED,
              "got rc=" + PhotonNative.lastSendActionResult());

        PhotonNative.destroy(h1);
        PhotonNative.destroy(h3);
    }

    // ── Конкурентные вызовы ──────────────────────────────────────────

    private static void testConcurrentCalls() {
        final int threads = 8;
        final int iterations = 500;
        final Thread[] workers = new Thread[threads];
        final boolean[] ok = new boolean[threads];

        for (int t = 0; t < threads; t++) {
            final int idx = t;
            workers[t] = new Thread(() -> {
                try {
                    for (int i = 0; i < iterations; i++) {
                        long h = PhotonNative.create("{}");
                        if (h == 0) return;
                        PhotonNative.sendAction(h, 100, "{}");
                        PhotonNative.getLastError(h);
                        PhotonNative.destroy(h);
                        PhotonNative.destroy(h); // повторный, должен быть no-op
                    }
                    ok[idx] = true;
                } catch (Throwable e) {
                    System.out.println("       thread " + idx + " threw: " + e);
                }
            });
            workers[t].start();
        }

        boolean allJoined = true;
        for (Thread w : workers) {
            try {
                w.join(60000);
            } catch (InterruptedException e) {
                allJoined = false;
            }
        }
        check("concurrent: all threads joined", allJoined, "join interrupted");

        boolean allOk = true;
        for (boolean b : ok) {
            if (!b) allOk = false;
        }
        check("concurrent: " + threads + " threads x " + iterations
              + " create/send/destroy cycles completed", allOk, "some thread failed");
    }
}
