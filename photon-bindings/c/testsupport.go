package main

/*
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "internal_cgo_types.h"

// ── Тестовые колбэки ─────────────────────────────────────────────────────
//
// Настоящие C-функции, а не Go-замыкания: реентерабельность проверяется
// только так. Колбэк, вызванный из насоса, обязан суметь позвать Photon_*
// обратно — а это возможно лишь через ту же границу, что у настоящего
// клиента.
//
// Экспортируемые функции объявлены вручную: cgo генерирует _cgo_export.h
// уже ПОСЛЕ компиляции преамбулы, поэтому включить его здесь нельзя.
// Сигнатуры обязаны совпадать с photon.go — расхождение поймает компоновщик.
extern void         Photon_Destroy(PhotonHandle* handle);
extern const char*  Photon_GetLastError(PhotonHandle* handle);
extern PhotonResult Photon_SendAction(PhotonHandle* handle, int32_t action_code,
                                      const char* payload_json, PhotonString** response_out);

// Состояние наблюдения. Одиночное: тесты, использующие колбэки, не
// параллелятся между собой (t.Parallel не вызывается).
static int         g_calls;
static char        g_last[4096];
static int         g_order[64];
static int         g_order_n;
static PhotonHandle* g_handle;
static int         g_reentrant_rc;
static int         g_destroy_returned;
static pthread_t   g_cb_thread;
static int         g_cb_thread_set;

static void photon_test_reset(void) {
    g_calls = 0; g_last[0] = 0; g_order_n = 0;
    g_handle = NULL; g_reentrant_rc = -1; g_destroy_returned = 0;
    g_cb_thread_set = 0;
}

static void photon_test_note(const char* json) {
    g_calls++;
    if (json) { strncpy(g_last, json, sizeof(g_last) - 1); g_last[sizeof(g_last) - 1] = 0; }
    if (!g_cb_thread_set) { g_cb_thread = pthread_self(); g_cb_thread_set = 1; }
}

// Простой наблюдатель: только считает и запоминает.
static void photon_cb_plain(const char* json, void* ud) {
    (void)ud;
    photon_test_note(json);
}

// Зовёт Photon_SendAction из колбэка — обязан не дедлокнуть.
static void photon_cb_send_action(const char* json, void* ud) {
    (void)ud;
    photon_test_note(json);
    g_reentrant_rc = (int)Photon_SendAction(g_handle, 201, "{}", NULL);
}

// Зовёт Photon_GetLastError из колбэка — обязан не дедлокнуть.
static void photon_cb_last_error(const char* json, void* ud) {
    (void)ud;
    photon_test_note(json);
    (void)Photon_GetLastError(g_handle);
    g_reentrant_rc = 0;
}

// Зовёт Photon_Destroy из колбэка — обязан вернуться немедленно, не повесив
// процесс. Флаг ставится ПОСЛЕ возврата: если бы Destroy ждал, сюда бы не
// дошли.
static void photon_cb_destroy(const char* json, void* ud) {
    (void)ud;
    photon_test_note(json);
    Photon_Destroy(g_handle);
    g_destroy_returned = 1;
}

// Доступ к наблюдению — только через функции: static-переменные преамбулы из
// Go не адресуются (каждый файл cgo — своя единица трансляции), а static-
// функции вызываются штатно.
static int  photon_test_calls(void)            { return g_calls; }
static const char* photon_test_last(void)      { return g_last; }
static int  photon_test_rc(void)               { return g_reentrant_rc; }
static int  photon_test_destroy_returned(void) { return g_destroy_returned; }
static void photon_test_set_handle(PhotonHandle* h) { g_handle = h; }

static PhotonEventCallback photon_cb_of(int kind) {
    switch (kind) {
        case 1:  return photon_cb_send_action;
        case 2:  return photon_cb_last_error;
        case 3:  return photon_cb_destroy;
        default: return photon_cb_plain;
    }
}
*/
import "C"

import (
	"unsafe"

	"github.com/wiloris/photon/photon-core/services/dispatcher"
)

// Файл существует из-за ограничения Go: cgo не поддерживается в
// _test.go файлах ("use of cgo in test ... not supported"). Поэтому
// весь код, которому нужен доступ к C-типам, живёт здесь, в обычном
// файле пакета, а тесты вызывают эти обёртки и работают с
// unsafe.Pointer/Go-типами, не упоминая C напрямую.
//
// Обёртки не экспортируются в C (нет //export) — они не часть ABI и
// не попадают в .so как публичные символы.

// tsCString — C.CString + функция освобождения (вызывающий обязан
// вызвать её, обычно через t.Cleanup).
func tsCString(s string) (unsafe.Pointer, func()) {
	p := unsafe.Pointer(C.CString(s))
	return p, func() { C.free(p) }
}

func tsGoString(p unsafe.Pointer) string {
	if p == nil {
		return ""
	}
	return C.GoString((*C.char)(p))
}

func tsGoBytes(p unsafe.Pointer, n int) []byte {
	if p == nil {
		return nil
	}
	return C.GoBytes(p, C.int(n))
}

func tsABIMajor() uint32 { return uint32(C.PHOTON_ABI_MAJOR) }
func tsABIMinor() uint32 { return uint32(C.PHOTON_ABI_MINOR) }

// Коды результата, продублированные как Go-константы для тестов.
var (
	tsOK                 = int(C.PHOTON_OK)
	tsErrInvalidArgument = int(C.PHOTON_ERR_INVALID_ARGUMENT)
	tsErrInternal        = int(C.PHOTON_ERR_INTERNAL)
	tsErrInvalidAction   = int(C.PHOTON_ERR_INVALID_ACTION)

	// Появились в Phase 13.5: до неё ни один путь их не возвращал.
	tsErrNotConnected     = int(C.PHOTON_ERR_NOT_CONNECTED)
	tsErrPermissionDenied = int(C.PHOTON_ERR_PERMISSION_DENIED)
	tsErrTimeout          = int(C.PHOTON_ERR_TIMEOUT)
)

// tsResultFor открывает resultFor тестам, не таща в них типы C.
func tsResultFor(class dispatcher.Class) int { return int(resultFor(class)) }

// ── Обёртки над экспортируемыми функциями, принимающие/возвращающие
//    Go-типы вместо C-типов ────────────────────────────────────────────

func tsCreate(configJSON unsafe.Pointer, passNilHandlePtr bool) (handle unsafe.Pointer, rc int) {
	if passNilHandlePtr {
		return nil, int(Photon_Create((*C.char)(configJSON), nil))
	}
	var h *C.PhotonHandle
	r := Photon_Create((*C.char)(configJSON), &h)
	return unsafe.Pointer(h), int(r)
}

func tsDestroy(handle unsafe.Pointer) {
	Photon_Destroy((*C.PhotonHandle)(handle))
}

func tsSendAction(handle unsafe.Pointer, action int32, payload unsafe.Pointer, wantRespOut bool) (rc int, respOut unsafe.Pointer) {
	var ro *C.PhotonString
	var roPtr **C.PhotonString
	if wantRespOut {
		roPtr = &ro
	}
	r := Photon_SendAction((*C.PhotonHandle)(handle), C.int32_t(action), (*C.char)(payload), roPtr)
	return int(r), unsafe.Pointer(ro)
}

func tsGetLastError(handle unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(Photon_GetLastError((*C.PhotonHandle)(handle)))
}

func tsSetEventCallback(handle unsafe.Pointer) {
	Photon_SetEventCallback((*C.PhotonHandle)(handle), nil, nil)
}

func tsGetABIVersion() uint32 { return uint32(Photon_GetABIVersion()) }

func tsNewString(s string) unsafe.Pointer { return unsafe.Pointer(newPhotonString(s)) }

func tsStringData(p unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(Photon_StringData((*C.PhotonString)(p)))
}

func tsStringLen(p unsafe.Pointer) int {
	return int(Photon_StringLen((*C.PhotonString)(p)))
}

func tsFreeString(p unsafe.Pointer) { Photon_FreeString((*C.PhotonString)(p)) }

func tsNewBytes(b []byte) unsafe.Pointer { return unsafe.Pointer(newPhotonBytes(b)) }

func tsBytesData(p unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(Photon_BytesData((*C.PhotonBytes)(p)))
}

func tsBytesLen(p unsafe.Pointer) int {
	return int(Photon_BytesLen((*C.PhotonBytes)(p)))
}

func tsFreeBytes(p unsafe.Pointer) { Photon_FreeBytes((*C.PhotonBytes)(p)) }

func tsCoreIsValid(handle unsafe.Pointer) bool {
	_, epoch, ok := enterCore((*C.PhotonHandle)(handle))
	handles.exit(epoch)
	return ok
}

// tsLookup — объект за handle, найденный парой вход/выход реестра, как в
// экспортируемой функции. Вызывать только внутри окна времени жизни handle:
// за окном найденный объект ничего не значит (ADR-059 п. 2, п. 5.1).
func tsLookup(handle unsafe.Pointer) (interface{}, bool) {
	v, epoch, ok := handles.enter(handle)
	handles.exit(epoch)
	return v, ok
}

// ── Обёртки для тестов доставки событий (Phase 15.5) ─────────────────────

// tsEventReset обнуляет наблюдение и запоминает handle, который тестовые
// колбэки будут дёргать реентерабельно.
func tsEventReset(handle unsafe.Pointer) {
	C.photon_test_reset()
	C.photon_test_set_handle((*C.PhotonHandle)(handle))
}

// tsSetEventCallbackKind ставит один из тестовых C-колбэков:
// 0 — наблюдатель, 1 — зовёт SendAction, 2 — GetLastError, 3 — Destroy.
func tsSetEventCallbackKind(handle unsafe.Pointer, kind int) {
	Photon_SetEventCallback((*C.PhotonHandle)(handle), C.photon_cb_of(C.int(kind)), nil)
}

// tsSetEventCallbackNil снимает адресата, не трогая подписку.
func tsSetEventCallbackNil(handle unsafe.Pointer) {
	Photon_SetEventCallback((*C.PhotonHandle)(handle), nil, nil)
}

func tsEventCalls() int       { return int(C.photon_test_calls()) }
func tsEventLast() string     { return C.GoString(C.photon_test_last()) }
func tsReentrantRC() int      { return int(C.photon_test_rc()) }
func tsDestroyReturned() bool { return C.photon_test_destroy_returned() != 0 }

// tsNewCoreWithFakeDispatcher собирает инстанс поверх подставленного
// транспорта: настоящей сети в юнит-тестах биндинга нет и быть не должно.
func tsNewCoreWithFakeDispatcher(cfg dispatcher.Config) (unsafe.Pointer, error) {
	disp, err := dispatcher.New(cfg)
	if err != nil {
		return nil, err
	}
	return registerCore(disp), nil
}
