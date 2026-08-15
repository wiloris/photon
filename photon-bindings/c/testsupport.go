package main

/*
#include <stdlib.h>
#include "internal_cgo_types.h"
*/
import "C"

import "unsafe"

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
)

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
	_, ok := coreFromHandle((*C.PhotonHandle)(handle))
	return ok
}
