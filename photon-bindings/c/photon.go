// Package main — реализация photon_core.h через cgo, -buildmode=c-shared.
//
// Opaque handles (PhotonHandle*, PhotonString*, PhotonBytes*) реализованы
// через runtime/cgo.Handle, но НЕ напрямую как unsafe.Pointer(handle) —
// документация runtime/cgo прямо предупреждает: "It is not safe to
// coerce a cgo.Handle (an integer) to a Go unsafe.Pointer". Вместо
// этого для каждого handle выделяется небольшой блок реальной C-памяти
// (C.malloc), хранящий значение cgo.Handle как uintptr_t; наружу
// отдаётся указатель на эту память — настоящий валидный указатель на
// C-кучу, а не Go-значение, притворяющееся указателем.
package main

/*
#include <stdlib.h>
#include "internal_cgo_types.h"
*/
import "C"

import (
	"encoding/json"
	"runtime/cgo"
	"sync"
	"unsafe"

	photonpb "github.com/wiloris/photon-core/protobuf"
)

// main требуется для -buildmode=c-shared, тело не используется.
func main() {}

// ── Opaque handle helpers ────────────────────────────────────────────────

// newOpaqueHandle оборачивает v в cgo.Handle и выделяет реальную
// C-память для хранения его значения.
func newOpaqueHandle(v interface{}) unsafe.Pointer {
	h := cgo.NewHandle(v)
	mem := C.malloc(C.size_t(unsafe.Sizeof(C.uintptr_t(0))))
	*(*C.uintptr_t)(mem) = C.uintptr_t(h)
	return mem
}

// safeHandleValue вызывает h.Value(), перехватывая панику cgo.Handle на
// невалидном/уже удалённом хендле — превращает её в (nil, false) вместо
// падения процесса через границу C.
func safeHandleValue(h cgo.Handle) (v interface{}, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			v, ok = nil, false
		}
	}()
	return h.Value(), true
}

// resolveOpaqueHandle читает значение cgo.Handle из C-памяти и резолвит
// его в исходное Go-значение.
func resolveOpaqueHandle(p unsafe.Pointer) (v interface{}, h cgo.Handle, ok bool) {
	if p == nil {
		return nil, 0, false
	}
	raw := *(*C.uintptr_t)(p)
	// 0 — sentinel "уже освобождён" (freeOpaqueHandle зануляет ячейку
	// перед C.free). Без этой проверки повторный Destroy/Free на том же
	// указателе прочитал бы старое значение Handle, которое cgo мог уже
	// переиспользовать под ЧУЖОЙ объект — и мы освободили бы чужую
	// память (double free). cgo.Handle никогда не выдаёт 0 (нумерация
	// начинается с 1), поэтому 0 безопасен как маркер.
	if raw == 0 {
		return nil, 0, false
	}
	h = cgo.Handle(uintptr(raw))
	v, ok = safeHandleValue(h)
	return v, h, ok
}

// ── core: состояние одного PhotonHandle ──────────────────────────────────

type core struct {
	mu        sync.Mutex
	lastError *C.char
	callback  C.PhotonEventCallback
	userData  unsafe.Pointer
	destroyed bool
}

func (c *core) setError(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastError != nil {
		C.free(unsafe.Pointer(c.lastError))
	}
	c.lastError = C.CString(msg)
}

// releaseOpaqueHandle зануляет ячейку C-памяти (делая повторные вызовы
// безопасными no-op), удаляет cgo.Handle и освобождает саму память.
// Порядок важен: зануление ДО C.free, иначе между ними остаётся окно,
// в котором другой поток прочитал бы валидное на вид значение.
func releaseOpaqueHandle(p unsafe.Pointer, h cgo.Handle, handleValid bool) {
	if p == nil {
		return
	}
	*(*C.uintptr_t)(p) = 0
	if handleValid {
		h.Delete()
	}
	C.free(p)
}

// coreFromHandle резолвит *C.PhotonHandle обратно в *core. ok=false
// значит handle невалиден (nil, уже уничтожен, или битый указатель).
func coreFromHandle(handle *C.PhotonHandle) (*core, bool) {
	v, _, ok := resolveOpaqueHandle(unsafe.Pointer(handle))
	if !ok {
		return nil, false
	}
	c, ok := v.(*core)
	if !ok {
		return nil, false
	}
	c.mu.Lock()
	destroyed := c.destroyed
	c.mu.Unlock()
	if destroyed {
		return nil, false
	}
	return c, true
}

// ── ABI Versioning ──────────────────────────────────────────────────────

//export Photon_GetABIVersion
func Photon_GetABIVersion() C.uint32_t {
	return (C.uint32_t(C.PHOTON_ABI_MAJOR) << 16) | C.uint32_t(C.PHOTON_ABI_MINOR)
}

// ── Жизненный цикл ──────────────────────────────────────────────────────

//export Photon_Create
func Photon_Create(configJSON *C.char, handle **C.PhotonHandle) C.PhotonResult {
	// ADR-028: первая строка тела функции, до разбора config_json и до
	// любого пути ошибки.
	if handle == nil {
		return C.PHOTON_ERR_INVALID_ARGUMENT
	}
	*handle = nil

	if configJSON == nil {
		return C.PHOTON_ERR_INVALID_ARGUMENT
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(C.GoString(configJSON)), &parsed); err != nil {
		return C.PHOTON_ERR_INVALID_ARGUMENT
	}

	*handle = (*C.PhotonHandle)(newOpaqueHandle(&core{}))
	return C.PHOTON_OK
}

//export Photon_Destroy
func Photon_Destroy(handle *C.PhotonHandle) {
	// ADR-028: Photon_Destroy(NULL) — безопасный no-op, как free(NULL).
	if handle == nil {
		return
	}

	v, h, ok := resolveOpaqueHandle(unsafe.Pointer(handle))
	if !ok {
		// Уже освобождён (ячейка занулена) или мусорный указатель —
		// ничего не делаем: освобождать эту память второй раз нельзя,
		// первый Destroy уже сделал C.free.
		return
	}
	c, ok := v.(*core)
	if !ok {
		releaseOpaqueHandle(unsafe.Pointer(handle), h, true)
		return
	}

	c.mu.Lock()
	if c.destroyed {
		c.mu.Unlock()
		return
	}
	c.destroyed = true
	if c.lastError != nil {
		C.free(unsafe.Pointer(c.lastError))
		c.lastError = nil
	}
	c.mu.Unlock()

	// Гарантия callback lifetime (photon_core.h, Photon_Destroy): после
	// возврата из этой функции callback не будет вызван снова. Сегодня
	// это тривиально — фоновых горутин, вызывающих callback, ещё не
	// существует (Dispatcher/Transport — Phase 6/13, не реализованы).
	// Когда они появятся, здесь обязано появиться реальное ожидание их
	// завершения (WaitGroup) — эта строка фиксирует место.

	releaseOpaqueHandle(unsafe.Pointer(handle), h, true)
}

// ── Event Callback ──────────────────────────────────────────────────────

//export Photon_SetEventCallback
func Photon_SetEventCallback(handle *C.PhotonHandle, callback C.PhotonEventCallback, userData unsafe.Pointer) {
	c, ok := coreFromHandle(handle)
	if !ok {
		return
	}
	c.mu.Lock()
	c.callback = callback
	c.userData = userData
	c.mu.Unlock()
}

// ── Диагностика ─────────────────────────────────────────────────────────

//export Photon_GetLastError
func Photon_GetLastError(handle *C.PhotonHandle) *C.char {
	c, ok := coreFromHandle(handle)
	if !ok {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastError
}

// ── Photon Protocol ─────────────────────────────────────────────────────

//export Photon_SendAction
func Photon_SendAction(handle *C.PhotonHandle, actionCode C.int32_t, payloadJSON *C.char, responseOut **C.PhotonString) C.PhotonResult {
	c, ok := coreFromHandle(handle)
	if !ok {
		return C.PHOTON_ERR_INVALID_ARGUMENT
	}

	// action_code валидируется против канонического источника —
	// photon-core/protobuf (ADR-025), не против локальной копии списка.
	if _, known := photonpb.Action_name[int32(actionCode)]; !known || actionCode == 0 {
		c.setError("unknown action_code")
		return C.PHOTON_ERR_INVALID_ACTION
	}

	if payloadJSON == nil {
		c.setError("payload_json is required")
		return C.PHOTON_ERR_INVALID_ARGUMENT
	}
	// payload_json — собственность вызывающего до и после вызова
	// (photon_core.h); GoString копирует байты в Go-память здесь,
	// указатель дальше не хранится.
	_ = C.GoString(payloadJSON)

	// Dispatcher (Phase 13) не реализован — нет service-уровня, который
	// мог бы выполнить это действие. Честный "не реализовано", а не
	// притворная успешность.
	c.setError("action not yet implemented: dispatcher (Phase 13) does not exist")
	if responseOut != nil {
		*responseOut = nil
	}
	return C.PHOTON_ERR_INTERNAL
}

// ── PhotonString ────────────────────────────────────────────────────────

type photonString struct {
	data       string
	cachedCStr *C.char
}

// newPhotonString создаёт PhotonString* handle. Не экспортирована в C —
// вызывается изнутри Go-кода, когда нужно вернуть строку через границу.
func newPhotonString(s string) *C.PhotonString {
	return (*C.PhotonString)(newOpaqueHandle(&photonString{data: s}))
}

//export Photon_StringData
func Photon_StringData(s *C.PhotonString) *C.char {
	v, _, ok := resolveOpaqueHandle(unsafe.Pointer(s))
	if !ok {
		return nil
	}
	ps, ok := v.(*photonString)
	if !ok {
		return nil
	}
	if ps.cachedCStr == nil {
		ps.cachedCStr = C.CString(ps.data)
	}
	return ps.cachedCStr
}

//export Photon_StringLen
func Photon_StringLen(s *C.PhotonString) C.size_t {
	v, _, ok := resolveOpaqueHandle(unsafe.Pointer(s))
	if !ok {
		return 0
	}
	ps, ok := v.(*photonString)
	if !ok {
		return 0
	}
	return C.size_t(len(ps.data))
}

//export Photon_FreeString
func Photon_FreeString(s *C.PhotonString) {
	if s == nil {
		return
	}
	v, h, ok := resolveOpaqueHandle(unsafe.Pointer(s))
	if !ok {
		return // уже освобождён
	}
	if ps, ok := v.(*photonString); ok && ps.cachedCStr != nil {
		C.free(unsafe.Pointer(ps.cachedCStr))
		ps.cachedCStr = nil
	}
	releaseOpaqueHandle(unsafe.Pointer(s), h, true)
}

// ── PhotonBytes ─────────────────────────────────────────────────────────

type photonBytes struct {
	data        []byte
	cachedCData unsafe.Pointer
}

func newPhotonBytes(b []byte) *C.PhotonBytes {
	return (*C.PhotonBytes)(newOpaqueHandle(&photonBytes{data: b}))
}

//export Photon_BytesData
func Photon_BytesData(b *C.PhotonBytes) *C.uint8_t {
	v, _, ok := resolveOpaqueHandle(unsafe.Pointer(b))
	if !ok {
		return nil
	}
	pb, ok := v.(*photonBytes)
	if !ok || len(pb.data) == 0 {
		return nil
	}
	if pb.cachedCData == nil {
		pb.cachedCData = C.CBytes(pb.data)
	}
	return (*C.uint8_t)(pb.cachedCData)
}

//export Photon_BytesLen
func Photon_BytesLen(b *C.PhotonBytes) C.size_t {
	v, _, ok := resolveOpaqueHandle(unsafe.Pointer(b))
	if !ok {
		return 0
	}
	pb, ok := v.(*photonBytes)
	if !ok {
		return 0
	}
	return C.size_t(len(pb.data))
}

//export Photon_FreeBytes
func Photon_FreeBytes(b *C.PhotonBytes) {
	if b == nil {
		return
	}
	v, h, ok := resolveOpaqueHandle(unsafe.Pointer(b))
	if !ok {
		return // уже освобождён
	}
	if pb, ok := v.(*photonBytes); ok && pb.cachedCData != nil {
		C.free(pb.cachedCData)
		pb.cachedCData = nil
	}
	releaseOpaqueHandle(unsafe.Pointer(b), h, true)
}
