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
