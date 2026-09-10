/*
 * photon_jni.c — JNI-мост к Photon C ABI v1.
 *
 * Правило границы (ARCHITECTURE.md, раздел 7): этот файл обращается
 * ИСКЛЮЧИТЕЛЬНО к функциям Photon_* из photon_core.h. Никаких
 * обращений к Go-символам, внутренним структурам photon-core или
 * деталям реализации cgo — именно граница .so проверена тестами
 * bindings/c (32 теста, race, leak-check), и обход её обнулил бы
 * всю проверку.
 *
 * Ownership: мост НЕ владеет ресурсами Photon и ничего не освобождает
 * самостоятельно. Единственное, что он освобождает, — собственные
 * временные буферы, полученные от JNI (GetStringUTFChars) и от
 * Photon (Photon_FreeString), строго парно к их получению.
 *
 * Мост намеренно тонкий: никакой бизнес-логики, только преобразование
 * Java-типов в C и обратно.
 */

#include <jni.h>
#include <stdlib.h>
#include <string.h>

#include "photon_core.h"
#include "dev_photon_core_PhotonNative.h"

/*
 * Compile-time проверка: константы PhotonResult в Java-классе обязаны
 * совпадать с enum PhotonResult в photon_core.h. javac -h сгенерировал
 * из Java-полей макросы dev_photon_core_PhotonNative_PHOTON_*, поэтому
 * расхождение ловится компилятором здесь, а не в рантайме на устройстве.
 */
#define PHOTON_STATIC_ASSERT(cond, msg) \
    typedef char photon_static_assert_##msg[(cond) ? 1 : -1]

PHOTON_STATIC_ASSERT(PHOTON_OK == dev_photon_core_PhotonNative_PHOTON_OK, ok);
PHOTON_STATIC_ASSERT(PHOTON_ERR_TIMEOUT == dev_photon_core_PhotonNative_PHOTON_ERR_TIMEOUT, timeout);
PHOTON_STATIC_ASSERT(PHOTON_ERR_INVALID_ARGUMENT == dev_photon_core_PhotonNative_PHOTON_ERR_INVALID_ARGUMENT, invalid_argument);
PHOTON_STATIC_ASSERT(PHOTON_ERR_PERMISSION_DENIED == dev_photon_core_PhotonNative_PHOTON_ERR_PERMISSION_DENIED, permission_denied);
PHOTON_STATIC_ASSERT(PHOTON_ERR_NOT_FOUND == dev_photon_core_PhotonNative_PHOTON_ERR_NOT_FOUND, not_found);
PHOTON_STATIC_ASSERT(PHOTON_ERR_NOT_CONNECTED == dev_photon_core_PhotonNative_PHOTON_ERR_NOT_CONNECTED, not_connected);
PHOTON_STATIC_ASSERT(PHOTON_ERR_ALREADY_EXISTS == dev_photon_core_PhotonNative_PHOTON_ERR_ALREADY_EXISTS, already_exists);
PHOTON_STATIC_ASSERT(PHOTON_ERR_INTERNAL == dev_photon_core_PhotonNative_PHOTON_ERR_INTERNAL, internal);
PHOTON_STATIC_ASSERT(PHOTON_ERR_ABI_MISMATCH == dev_photon_core_PhotonNative_PHOTON_ERR_ABI_MISMATCH, abi_mismatch);
PHOTON_STATIC_ASSERT(PHOTON_ERR_INVALID_ACTION == dev_photon_core_PhotonNative_PHOTON_ERR_INVALID_ACTION, invalid_action);

/*
 * Коды результата последних вызовов — per-thread, потому что два потока
 * могут одновременно вызывать create/sendAction, и глобальная
 * переменная давала бы гонку с чужим результатом. __thread — расширение,
 * поддерживаемое gcc/clang (включая NDK-clang).
 */
static __thread PhotonResult g_last_create_result = PHOTON_OK;
static __thread PhotonResult g_last_send_result = PHOTON_OK;

/*
 * handle_from_jlong: jlong -> PhotonHandle*.
 *
 * Java видит handle как непрозрачный long и не знает, что внутри.
 * 0 означает "нет handle" — соответствует гарантии C ABI, что *handle
 * == NULL при любой ошибке Photon_Create (ADR-027/028). Передача 0
 * дальше в Photon_* безопасна: C ABI трактует NULL как no-op
 * (Photon_Destroy) или как ошибку (остальные).
 */
static PhotonHandle* handle_from_jlong(jlong h) {
    return (PhotonHandle*)(intptr_t)h;
}

static jlong jlong_from_handle(PhotonHandle* h) {
    return (jlong)(intptr_t)h;
}

/*
 * new_jstring_from_photon: PhotonString* -> jstring, с обязательным
 * освобождением через Photon_FreeString.
 *
 * Единственное место, где мост вызывает Photon_Free* — и делает это
 * строго парно к получению PhotonString* из Photon_SendAction. Сама
 * jstring — уже копия в JVM-heap, поэтому освобождать PhotonString
 * сразу безопасно.
 */
static jstring new_jstring_from_photon(JNIEnv* env, PhotonString* ps) {
    if (ps == NULL) {
        return NULL;
    }
    const char* data = Photon_StringData(ps);
    jstring result = (data != NULL) ? (*env)->NewStringUTF(env, data) : NULL;
    Photon_FreeString(ps);
    return result;
}

/* ── ABI versioning ─────────────────────────────────────────────────── */

JNIEXPORT jint JNICALL Java_dev_photon_core_PhotonNative_getAbiVersion
  (JNIEnv* env, jclass clazz) {
    (void)env; (void)clazz;
    return (jint)Photon_GetABIVersion();
}

/* ── Жизненный цикл ─────────────────────────────────────────────────── */

JNIEXPORT jlong JNICALL Java_dev_photon_core_PhotonNative_create
  (JNIEnv* env, jclass clazz, jstring configJson) {
    (void)clazz;

    if (configJson == NULL) {
        g_last_create_result = PHOTON_ERR_INVALID_ARGUMENT;
        return 0;
    }

    const char* cfg = (*env)->GetStringUTFChars(env, configJson, NULL);
    if (cfg == NULL) {
        /* OOM — JVM уже бросила исключение. */
        g_last_create_result = PHOTON_ERR_INTERNAL;
        return 0;
    }

    PhotonHandle* handle = NULL;
    PhotonResult rc = Photon_Create(cfg, &handle);
    (*env)->ReleaseStringUTFChars(env, configJson, cfg);

    g_last_create_result = rc;
    if (rc != PHOTON_OK) {
        /* C ABI гарантирует handle == NULL при ошибке (ADR-027/028);
           транслируем это в 0 для Java, не полагаясь на значение handle. */
        return 0;
    }
    return jlong_from_handle(handle);
}

JNIEXPORT jint JNICALL Java_dev_photon_core_PhotonNative_lastCreateResult
  (JNIEnv* env, jclass clazz) {
    (void)env; (void)clazz;
    return (jint)g_last_create_result;
}

JNIEXPORT void JNICALL Java_dev_photon_core_PhotonNative_destroy
  (JNIEnv* env, jclass clazz, jlong handle) {
    (void)env; (void)clazz;
    /* Photon_Destroy(NULL) — безопасный no-op (ADR-028), повторный
       вызов на уже уничтоженном handle — тоже. Мост не добавляет
       собственных проверок: это ответственность C ABI, и она покрыта
       его тестами. */
    Photon_Destroy(handle_from_jlong(handle));
}

/* ── Photon Protocol ────────────────────────────────────────────────── */

JNIEXPORT jstring JNICALL Java_dev_photon_core_PhotonNative_sendAction
  (JNIEnv* env, jclass clazz, jlong handle, jint actionCode, jstring payloadJson) {
    (void)clazz;

    const char* payload = NULL;
    if (payloadJson != NULL) {
        payload = (*env)->GetStringUTFChars(env, payloadJson, NULL);
        if (payload == NULL) {
            g_last_send_result = PHOTON_ERR_INTERNAL;
            return NULL;
        }
    }

    PhotonString* response = NULL;
    PhotonResult rc = Photon_SendAction(handle_from_jlong(handle),
                                        (int32_t)actionCode,
                                        payload,
                                        &response);

    if (payload != NULL) {
        (*env)->ReleaseStringUTFChars(env, payloadJson, payload);
    }

    g_last_send_result = rc;
    if (rc != PHOTON_OK) {
        /* C ABI: при любом результате кроме PHOTON_OK *response_out не
           устанавливается — освобождать нечего. */
        return NULL;
    }
    return new_jstring_from_photon(env, response);
}

JNIEXPORT jint JNICALL Java_dev_photon_core_PhotonNative_lastSendActionResult
  (JNIEnv* env, jclass clazz) {
    (void)env; (void)clazz;
    return (jint)g_last_send_result;
}

/* ── Диагностика ────────────────────────────────────────────────────── */

JNIEXPORT jstring JNICALL Java_dev_photon_core_PhotonNative_getLastError
  (JNIEnv* env, jclass clazz, jlong handle) {
    (void)clazz;

    /* Photon_GetLastError возвращает const char*, принадлежащий Photon
       и валидный до следующего Photon_* вызова на этом handle. НЕ
       освобождаем его — владелец Photon (photon_core.h). NewStringUTF
       копирует в JVM-heap, поэтому дальнейшая инвалидация безопасна. */
    const char* msg = Photon_GetLastError(handle_from_jlong(handle));
    if (msg == NULL) {
        return NULL;
    }
    return (*env)->NewStringUTF(env, msg);
}

/* ── События ────────────────────────────────────────────────────────────
 *
 * Контракт — photon-bindings/docs/jni-events.md. Кратко о том, что здесь
 * нормативно, а что деталь реализации.
 *
 * D1: доставка синхронная, НА ПОТОКЕ ДОСТАВКИ ядра. Мост не заводит ни
 * очереди, ни исполнителя, ни собственного потока. Поэтому порядок событий
 * и свойство «колбэки никогда не параллельны» переходят из ядра как есть
 * (event-delivery.md §9.1), а реентерабельный Photon_Destroy продолжает
 * опознаваться сравнением идентификатора OS-потока (§10.3): увод колбэка на
 * другой поток сломал бы это опознание.
 *
 * D2: AttachCurrentThread перед вызовом, если поток ещё не присоединён, и
 * DetachCurrentThread после — ТОЛЬКО если присоединил этот же вызов. Поток
 * доставки принадлежит пулу Go (насос снимает LockOSThread через defer, и
 * поток возвращается в пул, а не умирает), поэтому постоянное присоединение
 * накапливало бы attached-потоки по одному на каждый созданный инстанс.
 * AttachCurrentThreadAsDaemon здесь не используется.
 *
 * D4: исключение слушателя фиксируется и очищается, доставка продолжается,
 * слушатель НЕ отцепляется. Пробросить его в Go нельзя — вызывающий тут
 * C-функция, вызванная из горутины; оставить непогашенным нельзя тем более:
 * следующий JNI-вызов в этом потоке упал бы на pending exception.
 *
 * Мост не берёт СВОИХ мьютексов вокруг вызова слушателя. Ядро специально
 * отпускает c.mu до вызова (§9.1), и замок моста вернул бы ровно тот дефект:
 * рядовое «получил MESSAGE → отправил действие» упёрлось бы в него.
 */

static JavaVM* g_vm = NULL;

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved) {
    (void)reserved;
    g_vm = vm;
    return JNI_VERSION_1_6;
}

/*
 * Контекст доставки. Живёт как user_data и обязан оставаться валидным до
 * возврата из Photon_Destroy — прямое требование photon_core.h. Поэтому
 * освобождает его отдельный вызов releaseEventListener, а не destroy:
 * мост не ведёт реестра «handle → ctx», порядок обеспечивает вызывающий.
 */
typedef struct {
    jobject   listener;  /* GlobalRef, владение у моста */
    jmethodID on_event;  /* кэшируется при установке: поиск по классу на
                            каждом событии — работа в горячем пути */
} photon_listener_ctx;

static void photon_event_bridge(const char* event_json, void* user_data) {
    photon_listener_ctx* ctx = (photon_listener_ctx*)user_data;
    if (ctx == NULL || ctx->listener == NULL || g_vm == NULL) {
        return;
    }

    JNIEnv* env = NULL;
    int attached_here = 0;

    jint rc = (*g_vm)->GetEnv(g_vm, (void**)&env, JNI_VERSION_1_6);
    if (rc == JNI_EDETACHED) {
        if ((*g_vm)->AttachCurrentThread(g_vm, (void**)&env, NULL) != JNI_OK) {
            return; /* без env сделать нельзя ничего, включая логирование */
        }
        attached_here = 1;
    } else if (rc != JNI_OK || env == NULL) {
        return;
    }

    /* event_json невалиден сразу после возврата из колбэка (photon_core.h),
       поэтому копия в JVM-heap делается здесь, внутри вызова. */
    jstring js = (*env)->NewStringUTF(env, event_json != NULL ? event_json : "");
    if (js != NULL) {
        (*env)->CallVoidMethod(env, ctx->listener, ctx->on_event, js);
        if ((*env)->ExceptionCheck(env)) {
            /* D4: зафиксировать и погасить. Поток событий обязан продолжиться. */
            (*env)->ExceptionDescribe(env);
            (*env)->ExceptionClear(env);
        }
        /* Локальная ссылка освобождается явно: поток может быть присоединён
           надолго, и автоматическая очистка фрейма не наступит сама. */
        (*env)->DeleteLocalRef(env, js);
    } else if ((*env)->ExceptionCheck(env)) {
        (*env)->ExceptionClear(env);
    }

    if (attached_here) {
        (*g_vm)->DetachCurrentThread(g_vm);
    }
}

/*
 * setEventListener возвращает указатель на контекст как jlong. Вызывающий
 * обязан передать его в releaseEventListener ПОСЛЕ возврата из destroy.
 *
 * listener == NULL отцепляет адресата, подписку не останавливая (так
 * устроен C ABI), и возвращает 0.
 */
JNIEXPORT jlong JNICALL Java_dev_photon_core_PhotonNative_setEventListener
  (JNIEnv* env, jclass clazz, jlong handle, jobject listener) {
    (void)clazz;

    PhotonHandle* h = handle_from_jlong(handle);
    if (h == NULL) {
        return 0;
    }

    if (listener == NULL) {
        Photon_SetEventCallback(h, NULL, NULL);
        return 0;
    }

    jclass cls = (*env)->GetObjectClass(env, listener);
    jmethodID mid = (*env)->GetMethodID(env, cls, "onEvent", "(Ljava/lang/String;)V");
    (*env)->DeleteLocalRef(env, cls);
    if (mid == NULL) {
        /* GetMethodID оставил NoSuchMethodError — гасим и сообщаем нулём:
           бросать из native здесь значило бы завести семантику сверх ABI. */
        if ((*env)->ExceptionCheck(env)) {
            (*env)->ExceptionClear(env);
        }
        return 0;
    }

    photon_listener_ctx* ctx = (photon_listener_ctx*)malloc(sizeof(photon_listener_ctx));
    if (ctx == NULL) {
        return 0;
    }
    ctx->listener = (*env)->NewGlobalRef(env, listener);
    ctx->on_event = mid;
    if (ctx->listener == NULL) {
        free(ctx);
        return 0;
    }

    Photon_SetEventCallback(h, photon_event_bridge, ctx);
    return (jlong)(intptr_t)ctx;
}

/*
 * releaseEventListener освобождает GlobalRef и контекст.
 *
 * Безопасен ТОЛЬКО после возврата из Photon_Destroy: до этого момента
 * доставка может идти с этим самым user_data, и освобождение стало бы
 * use-after-free на стороне JVM (jni-events.md §3.3, §9.1).
 *
 * Устаревший контекст после замены слушателя освобождается тем же вызовом
 * и тогда же: Photon_SetEventCallback не даёт дождаться уже начатой
 * доставки со старым user_data, поэтому немедленное освобождение было бы
 * гонкой.
 */
JNIEXPORT void JNICALL Java_dev_photon_core_PhotonNative_releaseEventListener
  (JNIEnv* env, jclass clazz, jlong ctxPtr) {
    (void)clazz;

    photon_listener_ctx* ctx = (photon_listener_ctx*)(intptr_t)ctxPtr;
    if (ctx == NULL) {
        return;
    }
    if (ctx->listener != NULL) {
        (*env)->DeleteGlobalRef(env, ctx->listener);
        ctx->listener = NULL;
    }
    free(ctx);
}
