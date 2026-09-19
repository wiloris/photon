// Package main — реализация photon_core.h через cgo, -buildmode=c-shared.
//
// Opaque handles (PhotonHandle*, PhotonString*, PhotonBytes*) — адреса
// небольших блоков реальной C-памяти (C.malloc): наружу уходит настоящий
// указатель на C-кучу, а не Go-значение, притворяющееся указателем
// (документация runtime/cgo прямо запрещает приводить cgo.Handle к
// unsafe.Pointer).
//
// Содержимое ячейки не читается никогда. Объект удерживает реестр живых
// handle (handle_registry.go) по адресу ячейки; каждая экспортируемая
// функция, принимающая handle, входит в эпоху реестра первым действием и
// выходит последним (ADR-059 п. 5). Прежняя схема хранила в ячейке
// cgo.Handle и метку 0 «уже освобождён» — метку можно было прочитать только
// из освобождённой памяти, что и нашёл ASan.
package main

/*
#include <stdlib.h>
#include <pthread.h>
#include "internal_cgo_types.h"

// photon_invoke_callback — трамплин: cgo не умеет вызывать C-указатель на
// функцию напрямую из Go.
//
// Вызывается БЕЗ удерживаемых мьютексов Go-стороны: c.mu берут четыре
// экспортируемые функции (Destroy, SetEventCallback, GetLastError,
// SendAction) и setError, поэтому вызов колбэка под ним вешал бы клиента на
// любом реентерабельном обращении к Photon — включая рядовое «получил
// MESSAGE → отправил действие» (event-delivery.md §10.5).
static void photon_invoke_callback(PhotonEventCallback cb, const char* json, void* ud) {
    cb(json, ud);
}
*/
import "C"

import (
	"errors"
	"sync"
	"unsafe"

	"github.com/wiloris/photon-core/services/dispatcher"
)

// main требуется для -buildmode=c-shared, тело не используется.
func main() {}

// ── Opaque handle helpers ────────────────────────────────────────────────

// cCells — аллокатор ячеек рабочего кода. Ячейка служит только источником
// уникального адреса; её содержимое не пишется и не читается.
type cCells struct{}

func (cCells) alloc() unsafe.Pointer {
	return C.malloc(C.size_t(unsafe.Sizeof(C.uintptr_t(0))))
}

func (cCells) free(p unsafe.Pointer) { C.free(p) }

// handles — единственный реестр всех непрозрачных handle библиотеки.
var handles = newHandleRegistry[unsafe.Pointer](cCells{})

// newOpaqueHandle выделяет ячейку и регистрирует за её адресом v.
func newOpaqueHandle(v interface{}) unsafe.Pointer {
	return handles.register(v)
}

// enterCore — вход экспортируемой функции, работающей с инстансом. Эпоху
// вызывающий отдаёт в handles.exit ПРИ ЛЮБОМ исходе, последним действием
// вызова (defer сразу после входа). ok=false — handle не живой, не *core или
// инстанс уже уничтожается.
func enterCore(handle *C.PhotonHandle) (c *core, epoch uint64, ok bool) {
	v, epoch, found := handles.enter(unsafe.Pointer(handle))
	if !found {
		return nil, epoch, false
	}
	c, ok = v.(*core)
	if !ok {
		return nil, epoch, false
	}
	c.mu.Lock()
	destroyed := c.destroyed
	c.mu.Unlock()
	if destroyed {
		return nil, epoch, false
	}
	return c, epoch, true
}

// ── core: состояние одного PhotonHandle ──────────────────────────────────

type core struct {
	mu        sync.Mutex
	lastError *C.char
	callback  C.PhotonEventCallback
	userData  unsafe.Pointer
	destroyed bool

	// ── Доставка событий (Phase 15.5, ADR-045) ───────────────────────────
	//
	// inCallback/cbThread — признак того, что прямо сейчас исполняется
	// колбэк, и OS-поток, на котором это происходит. Нужны ровно для одного:
	// отличить Photon_Destroy, вызванный ИЗ колбэка, от вызова из чужого
	// потока. «Горутина == поток» неверно, поэтому сравниваются именно
	// потоки, а насос привязан к своему через runtime.LockOSThread
	// (event-delivery.md §10.3).
	inCallback bool
	cbThread   C.pthread_t

	// deferred — уничтожение отложено до возврата из колбэка. Ставится
	// только реентерабельным Destroy.
	deferred bool

	// teardownDone закрывается ровно однажды — тем, кто фактически выполнил
	// уничтожение. Конкурентный Destroy из чужого потока ждёт его, иначе
	// вернул бы управление, пока колбэк ещё исполняется, нарушив гарантию
	// заголовка для себя (event-delivery.md §10.4).
	teardownDone chan struct{}

	// handleMem — собственный адрес handle. Нужен отложенному пути: снимает
	// handle с учёта тот же, кто выполняет уничтожение, — насос, которому
	// неоткуда взять указатель иначе. Забирается (обнуляется) ровно один раз
	// под mu тем, кто получил право на уничтожение.
	handleMem unsafe.Pointer

	// pendingDisp — диспетчер, снятый с c.disp вызовом Destroy и ещё не
	// закрытый. При отложенном пути его закрывает насос.
	pendingDisp *dispatcher.Dispatcher

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
	//
	// Файловой системы КАСАЕТСЯ, если задана секция storage: хранилище
	// инстанса открывается и мигрируется здесь (ADR-064 п. 1.3). Эти два
	// свойства разные, и смешивать их нельзя.
	disp, err := dispatcher.New(cfg)
	if err != nil {
		// Негодная конфигурация — ошибка вызывающего в параметре; годный
		// путь, который не открылся (права, блокировка, повреждение), —
		// внутренняя ошибка: проверять вызывающему нечего (ADR-064 п. 1.6).
		if errors.Is(err, dispatcher.ErrStorageUnavailable) {
			return C.PHOTON_ERR_INTERNAL
		}
		return C.PHOTON_ERR_INVALID_ARGUMENT
	}

	*handle = (*C.PhotonHandle)(registerCore(disp))
	return C.PHOTON_OK
}

// registerCore создаёт инстанс поверх disp и регистрирует его handle.
//
// Инстанс запоминает СВОЙ handle: при отложенном уничтожении (§10.2) снимает
// его с учёта не тот, кто вызвал Destroy, а насос, и ему неоткуда взять
// указатель иначе. Поле пишется после регистрации, но до того, как адрес
// уходит вызывающему, — законно обратиться к инстансу раньше некому.
func registerCore(disp *dispatcher.Dispatcher) unsafe.Pointer {
	c := &core{disp: disp, teardownDone: make(chan struct{})}
	mem := newOpaqueHandle(c)
	c.mu.Lock()
	c.handleMem = mem
	c.mu.Unlock()
	return mem
}

//export Photon_Destroy
func Photon_Destroy(handle *C.PhotonHandle) {
	// ADR-028: Photon_Destroy(NULL) — безопасный no-op, как free(NULL).
	if handle == nil {
		return
	}

	// Эпоха удерживается весь вызов, включая ожидание teardownDone и снятие с
	// учёта: пока вызов активен, ячейка не освобождается и её адрес не
	// достанется новому объекту (ADR-059 п. 5, И5).
	v, epoch, ok := handles.enter(unsafe.Pointer(handle))
	defer handles.exit(epoch)
	if !ok {
		// Промах внутри окна возможен только после снятия с учёта, а оно
		// стоит после close(teardownDone): уничтожение уже завершено. Вне
		// окна — UB (ADR-059 п. 2); ячейка при этом не читается.
		return
	}
	c, ok := v.(*core)
	if !ok {
		// Не инстанс — no-op, и handle остаётся действительным (ADR-061 п. 1).
		// Снимать чужой объект с учёта нельзя: строка или байты исчезли бы из
		// реестра, а их собственный Photon_Free* потом не нашёл бы их и не
		// освободил ни cachedCStr, ни саму запись.
		return
	}

	c.mu.Lock()

	// Реентерабельность определяется по OS-ПОТОКУ, а не по горутине: «горутина
	// == поток» неверно. Насос привязан к своему потоку через LockOSThread, а
	// обратный вызов C → Go идёт на том же потоке, поэтому Destroy, вызванный
	// из колбэка, видит тот же идентификатор. pthread_t непрозрачен по
	// стандарту — сравнение только через pthread_equal (event-delivery.md
	// §10.3).
	reentrant := c.inCallback && C.pthread_equal(C.pthread_self(), c.cbThread) != 0

	if c.destroyed {
		c.mu.Unlock()
		if reentrant {
			// Поток колбэка не блокируется никогда.
			return
		}
		// Уничтожение уже начал кто-то другой. Вернуться сейчас значило бы
		// отдать управление, пока колбэк ещё исполняется, нарушив гарантию
		// заголовка для СЕБЯ, — поэтому ждём его завершения
		// (event-delivery.md §10.4).
		<-c.teardownDone
		return
	}

	// Право на уничтожение получает тот, кто первым перевёл инстанс в
	// destroyed. Ровно один раз.
	c.destroyed = true
	disp := c.disp
	c.disp = nil
	if c.lastError != nil {
		C.free(unsafe.Pointer(c.lastError))
		c.lastError = nil
	}

	if reentrant {
		// ЯВНОЕ ИСКЛЮЧЕНИЕ из обычной семантики Destroy (event-delivery.md
		// §10.2). Замороженное «дождаться завершения уже начатых вызовов
		// колбэка» в случае САМОВЫЗОВА невыполнимо логически: колбэк не может
		// завершиться раньше, чем вернётся Destroy, который он сам и вызвал.
		//
		// Поэтому здесь НИЧЕГО не ждётся, ничего не закрывается и handle не
		// освобождается. К моменту возврата колбэк НЕ завершён — он и есть
		// вызывающий; инстанс НЕ уничтожен; handle ЕЩЁ выделен. Гарантировано
		// ровно одно: новых вызовов колбэка не будет.
		c.deferred = true
		c.pendingDisp = disp
		c.mu.Unlock()
		if disp != nil {
			// Разбудить насос: Close звать нельзя — он ждёт d.inflight, в
			// котором состоит сам насос, вызвавший этот колбэк.
			disp.Cancel()
		}
		return
	}

	mem := c.handleMem
	c.handleMem = nil
	c.mu.Unlock()

	// Настоящее ожидание, а не пометка «здесь однажды появится WaitGroup»:
	// фоновая работа переживает возврат из вызова — SendAction на сети, идущее
	// подключение и, с Phase 15.5, насос событий.
	//
	// dispatcher.Close сначала ОТМЕНЯЕТ их и только потом ждёт. Ожидание без
	// отмены висело бы до call_timeout — ровно то зависание, которое Phase 9
	// нашла тестом отказа на host.Stop с неотвечающим плагином.
	//
	// Единственный барьер времени жизни колбэка — d.inflight: Wait не
	// вернётся, пока насос не выполнил Done, а если в этот момент колбэк
	// исполнялся, Wait дождётся его возврата. Признак destroyed барьером НЕ
	// является и в этой роли не используется (event-delivery.md §10.1).
	c.teardown(disp, mem)
}

// ── Event Callback ──────────────────────────────────────────────────────

//export Photon_SetEventCallback
func Photon_SetEventCallback(handle *C.PhotonHandle, callback C.PhotonEventCallback, userData unsafe.Pointer) {
	c, epoch, ok := enterCore(handle)
	defer handles.exit(epoch)
	if !ok {
		return
	}
	c.mu.Lock()
	c.callback = callback
	c.userData = userData
	disp := c.disp
	c.mu.Unlock()

	// Подписку запускает ПЕРВЫЙ ненулевой колбэк, и это единственный вызов
	// ABI, выражающий намерение получать события (event-delivery.md §5.1).
	// Photon_Create для этого не годится — ему запрещено касаться сети;
	// первый SendAction тоже: клиент, который только слушает, не подписался
	// бы никогда.
	//
	// Замена колбэка подписку не трогает, NULL лишь отцепляет адресата:
	// тушить и поднимать подписку по установке и снятию — лишний путь с
	// состоянием гонки при нулевом выигрыше.
	//
	// StartEvents идемпотентен, поэтому проверять «уже запущен» здесь не
	// нужно; ErrNoEndpoint на инстансе без адресата — законный исход, а не
	// ошибка вызова.
	if callback != nil && disp != nil {
		_ = disp.StartEvents(c.deliver, c.finishDeferredTeardown)
	}
}

// deliver — единственная точка вызова пользовательского колбэка.
//
// Вызывается насосом СИНХРОННО и последовательно, поэтому порядок событий
// сохраняется, а колбэки никогда не параллельны (event-delivery.md §9.1).
//
// Мьютекс НЕ удерживается во время вызова. Под ним только читается адресат и
// публикуется поток доставки: c.mu берут Destroy, SetEventCallback,
// GetLastError и SendAction, и вызов колбэка под ним вешал бы клиента на любом
// реентерабельном обращении к Photon (§10.5).
func (c *core) deliver(eventJSON []byte) {
	c.mu.Lock()
	if c.destroyed || c.callback == nil {
		c.mu.Unlock()
		return
	}
	cb, ud := c.callback, c.userData
	// Поток публикуется ДО вызова: реентерабельный Destroy сравнивает с ним
	// свой pthread_self. Насос привязан к потоку через LockOSThread, поэтому
	// значение, взятое здесь, принадлежит потоку, который исполнит колбэк.
	c.cbThread = C.pthread_self()
	c.inCallback = true
	c.mu.Unlock()

	cs := C.CString(string(eventJSON))
	C.photon_invoke_callback(cb, cs, ud)
	// event_json невалиден сразу после возврата — прямое требование
	// photon_core.h; клиент, которому нужны данные, копирует их внутри.
	C.free(unsafe.Pointer(cs))

	c.mu.Lock()
	c.inCallback = false
	c.mu.Unlock()

	// Реентерабельный Destroy уже отменил контекст диспетчера и вернулся
	// внутрь колбэка. Насос обнаружит отмену на следующем шаге, выйдет и
	// вызовет finishDeferredTeardown — здесь делать нечего.
}

// finishDeferredTeardown вызывается насосом ПОСЛЕ его выхода из d.inflight
// (гарантия порядка — StartEvents). Только здесь безопасно звать Close: он
// ждёт d.inflight, и до снятия себя с учёта насос ждал бы сам себя
// (event-delivery.md §10.2).
func (c *core) finishDeferredTeardown() {
	c.mu.Lock()
	if !c.deferred {
		c.mu.Unlock()
		return
	}
	c.deferred = false
	disp := c.pendingDisp
	c.pendingDisp = nil
	mem := c.handleMem
	c.handleMem = nil
	c.mu.Unlock()

	c.teardown(disp, mem)
}

// teardown — фактическое уничтожение. Выполняется РОВНО ОДИН РАЗ: право на
// него получает тот, кто первым перевёл инстанс в destroyed
// (event-delivery.md §10.4).
//
// Порядок — ADR-059 п. 5: close(teardownDone) строго ДО снятия с учёта (И3).
// Иначе конкурентный Destroy, промахнувшийся после снятия, вернулся бы, пока
// уничтожение ещё идёт. Ячейку снятие не освобождает: её освобождает проход
// освобождения — сразу при снятии, если активных вызовов старых эпох нет
// (отложенный путь на насосе), иначе на выходе последнего из них.
func (c *core) teardown(disp *dispatcher.Dispatcher, mem unsafe.Pointer) {
	if disp != nil {
		disp.Close()
	}
	close(c.teardownDone)
	if mem != nil {
		handles.retire(mem)
	}
}

// ── Диагностика ─────────────────────────────────────────────────────────

//export Photon_GetLastError
func Photon_GetLastError(handle *C.PhotonHandle) *C.char {
	c, epoch, ok := enterCore(handle)
	defer handles.exit(epoch)
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
	c, epoch, ok := enterCore(handle)
	defer handles.exit(epoch)
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
	v, epoch, ok := handles.enter(unsafe.Pointer(s))
	defer handles.exit(epoch)
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
	v, epoch, ok := handles.enter(unsafe.Pointer(s))
	defer handles.exit(epoch)
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
	v, epoch, ok := handles.enter(unsafe.Pointer(s))
	defer handles.exit(epoch)
	if !ok {
		// Повторный Free — UB (photon_core.h, ADR-059 п. 2); промах лишь не
		// читает ячейку и ничего не обещает.
		return
	}
	// Проверка типа СТРОГО до retire (ADR-061 п. 2): иначе чужой объект — в
	// худшем случае живой инстанс — исчезал бы из реестра без уничтожения, с
	// работающим насосом и незакрытым teardownDone.
	ps, ok := v.(*photonString)
	if !ok {
		// Не строка — no-op; объект остаётся действительным и освобождается
		// своей функцией (ADR-061 п. 1). Его данные здесь не трогаются.
		return
	}
	if ps.cachedCStr != nil {
		C.free(unsafe.Pointer(ps.cachedCStr))
		ps.cachedCStr = nil
	}
	handles.retire(unsafe.Pointer(s))
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
	v, epoch, ok := handles.enter(unsafe.Pointer(b))
	defer handles.exit(epoch)
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
	v, epoch, ok := handles.enter(unsafe.Pointer(b))
	defer handles.exit(epoch)
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
	v, epoch, ok := handles.enter(unsafe.Pointer(b))
	defer handles.exit(epoch)
	if !ok {
		// Повторный Free — UB (photon_core.h, ADR-059 п. 2); промах лишь не
		// читает ячейку и ничего не обещает.
		return
	}
	// Проверка типа СТРОГО до retire — см. Photon_FreeString (ADR-061 п. 2).
	pb, ok := v.(*photonBytes)
	if !ok {
		// Не байты — no-op; объект остаётся действительным (ADR-061 п. 1).
		return
	}
	if pb.cachedCData != nil {
		C.free(pb.cachedCData)
		pb.cachedCData = nil
	}
	handles.retire(unsafe.Pointer(b))
}
