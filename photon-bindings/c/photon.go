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
	"runtime/cgo"
	"sync"
	"unsafe"

	"github.com/wiloris/photon-core/services/dispatcher"
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

	// disp — маршрутизатор действий этого инстанса (Phase 13.5). Своё поле
	// на каждый хендл, не пакетная переменная: адресат — это сам
	// PhotonHandle, и два Photon_Create с разным config_json обязаны дать два
	// независимых адресата (action-routing.md §8).
	//
	// nil означает «инстанс уничтожается»: Photon_Destroy забирает
	// диспетчер под мьютексом и закрывает его уже снаружи.
	disp *dispatcher.Dispatcher
}

func (c *core) setError(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Вызов в полёте может дойти сюда уже после Photon_Destroy: тот
	// освободил lastError и больше сюда не вернётся, поэтому новая строка
	// осталась бы висеть навсегда. Не «защитное программирование» — прямое
	// следствие того, что с Phase 13.5 SendAction живёт на сети дольше, чем
	// длится вызов Destroy из другого потока.
	if c.destroyed {
		return
	}
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

	// Схема config_json — доменное знание, поэтому разбор живёт в Core, а не
	// здесь: иначе он был бы переписан заново в каждом биндинге (JNI,
	// Desktop, будущие Rust/Swift). См. action-routing.md §4.1.
	cfg, err := dispatcher.ParseConfig([]byte(C.GoString(configJSON)))
	if err != nil {
		return C.PHOTON_ERR_INVALID_ARGUMENT
	}

	// Сети не касается: dispatcher.New строит объекты, подключение
	// происходит по требованию при первом SendAction (D-1). Обещание
	// заголовка «ни одного сетевого пакета до возврата» держится здесь.
	disp, err := dispatcher.New(cfg)
	if err != nil {
		return C.PHOTON_ERR_INVALID_ARGUMENT
	}

	*handle = (*C.PhotonHandle)(newOpaqueHandle(&core{disp: disp}))
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
	disp := c.disp
	c.disp = nil
	if c.lastError != nil {
		C.free(unsafe.Pointer(c.lastError))
		c.lastError = nil
	}
	c.mu.Unlock()

	// Настоящее ожидание, а не пометка «здесь однажды появится WaitGroup»:
	// с Phase 13.5 существует фоновая работа, переживающая возврат из
	// вызова, — SendAction, висящий на сети, и идущее подключение.
	//
	// dispatcher.Close сначала ОТМЕНЯЕТ их и только потом ждёт. Ожидание без
	// отмены висело бы до call_timeout — ровно то зависание, которое Phase 9
	// нашла тестом отказа на host.Stop с неотвечающим плагином.
	//
	// Закрывается вне c.mu: Close ждёт возврата вызовов, а те могут
	// обратиться к setError и взяли бы тот же мьютекс.
	//
	// После возврата отсюда ни один вызов больше не пользуется этим core —
	// это и есть предусловие гарантии lifetime из photon_core.h. Колбэк
	// сегодня не вызывает никто (эмиттера событий нет — push за рамками
	// фазы), но механизм ожидания уже здесь, и эмиттеру останется встать в
	// ту же очередь.
	if disp != nil {
		disp.Close()
	}

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

	if payloadJSON == nil {
		c.setError("payload_json is required")
		return C.PHOTON_ERR_INVALID_ARGUMENT
	}
	// payload_json — собственность вызывающего до и после вызова
	// (photon_core.h); GoString копирует байты в Go-память здесь,
	// указатель дальше не хранится.
	payload := []byte(C.GoString(payloadJSON))

	c.mu.Lock()
	disp := c.disp
	c.mu.Unlock()
	if disp == nil {
		// Гонка с Photon_Destroy: хендл ещё не освобождён, но инстанс уже
		// уходит. Повторять вызов не на чем — это не «нет связи».
		c.setError("instance is being destroyed")
		return C.PHOTON_ERR_INVALID_ARGUMENT
	}

	// Валидности action_code здесь НЕТ намеренно. Она уехала в Core вместе с
	// импортом photonpb: bindings отвечает за гигиену ABI (хендл, nil,
	// владение памятью), а знание о том, какие action-коды существуют, —
	// доменное (action-routing.md §5.1). Свойство «неизвестный код
	// отвергается без обращения к сети» при этом сохранено: диспетчер
	// проверяет код первым делом, до состояния связи и до её наличия.
	resp, err := disp.SendAction(int32(actionCode), payload)
	if err != nil {
		c.setError(err.Error())
		return resultFor(dispatcher.Classify(err))
	}

	// *responseOut устанавливается ТОЛЬКО при PHOTON_OK — прямое требование
	// photon_core.h. Записать туда NULL на пути ошибки было бы нарушением
	// того же правила с практическим следствием: если вызывающий держит там
	// ещё не освобождённый PhotonString от прошлого вызова, он потеряет
	// указатель и вместе с ним память.
	if responseOut != nil {
		*responseOut = newPhotonString(string(resp))
	}
	return C.PHOTON_OK
}

// resultFor переводит класс исхода в код ABI.
//
// Таблица «ошибка → класс» живёт в Core (dispatcher.Classify) — там её можно
// проверить на полноту относительно исходников session/errors.go. Здесь
// остаётся то, что действительно принадлежит границе: соответствие класса
// значению enum PhotonResult.
//
// Enum не расширяется (D-3): ErrNotReady и ErrConnectionLost сходятся в
// PHOTON_ERR_NOT_CONNECTED, потому что оба ведут клиента к одному решению —
// повторить вызов, который и переподключится. Различие между ними
// сохраняется в тексте Photon_GetLastError, где оно диагностическое, а не
// управляющее.
func resultFor(class dispatcher.Class) C.PhotonResult {
	switch class {
	case dispatcher.ClassOK:
		return C.PHOTON_OK
	case dispatcher.ClassInvalidArgument:
		return C.PHOTON_ERR_INVALID_ARGUMENT
	case dispatcher.ClassInvalidAction:
		return C.PHOTON_ERR_INVALID_ACTION
	case dispatcher.ClassNotConnected:
		return C.PHOTON_ERR_NOT_CONNECTED
	case dispatcher.ClassPermissionDenied:
		return C.PHOTON_ERR_PERMISSION_DENIED
	case dispatcher.ClassTimeout:
		return C.PHOTON_ERR_TIMEOUT
	case dispatcher.ClassInternal:
		return C.PHOTON_ERR_INTERNAL
	}
	// Недостижимо при исчерпывающем switch выше; молчать нельзя — новый
	// класс без строки здесь обязан выглядеть как внутренняя ошибка, а не
	// как успех.
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
