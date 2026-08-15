package main

import (
	"testing"
	"unsafe"
)

// Тесты не используют cgo напрямую (Go не поддерживает cgo в _test.go
// файлах) — все обращения к C-типам идут через обёртки ts* из
// testsupport.go.

// cstr — строка в C-памяти с автоосвобождением в конце теста, чтобы
// сами тесты не текли независимо от того, что проверяют.
func cstr(t *testing.T, s string) unsafe.Pointer {
	t.Helper()
	p, free := tsCString(s)
	t.Cleanup(free)
	return p
}

// mustCreate — успешное создание handle с автоуничтожением.
func mustCreate(t *testing.T, config string) unsafe.Pointer {
	t.Helper()
	h, rc := tsCreate(cstr(t, config), false)
	if rc != tsOK {
		t.Fatalf("Photon_Create(%q): rc = %d, want PHOTON_OK", config, rc)
	}
	if h == nil {
		t.Fatal("PHOTON_OK but handle is nil")
	}
	t.Cleanup(func() { tsDestroy(h) })
	return h
}

// ── Photon_Create / Photon_Destroy ──────────────────────────────────────

func TestCreate_Success(t *testing.T) {
	h, rc := tsCreate(cstr(t, `{"storage_path":"/tmp/photon"}`), false)
	if rc != tsOK {
		t.Fatalf("rc = %d, want PHOTON_OK", rc)
	}
	if h == nil {
		t.Fatal("PHOTON_OK but handle is nil")
	}
	tsDestroy(h)
}

func TestCreate_NilHandlePointer(t *testing.T) {
	if _, rc := tsCreate(cstr(t, `{}`), true); rc != tsErrInvalidArgument {
		t.Fatalf("rc = %d, want PHOTON_ERR_INVALID_ARGUMENT", rc)
	}
}

func TestCreate_NilConfig(t *testing.T) {
	h, rc := tsCreate(nil, false)
	if rc != tsErrInvalidArgument {
		t.Fatalf("rc = %d, want PHOTON_ERR_INVALID_ARGUMENT", rc)
	}
	if h != nil {
		t.Fatal("ADR-027/028: handle must be NULL after failed Create")
	}
}

func TestCreate_InvalidJSON(t *testing.T) {
	h, rc := tsCreate(cstr(t, `{not valid json`), false)
	if rc != tsErrInvalidArgument {
		t.Fatalf("rc = %d, want PHOTON_ERR_INVALID_ARGUMENT", rc)
	}
	if h != nil {
		t.Fatal("ADR-027/028: handle must be NULL after failed Create")
	}
	// ADR-028: раз handle гарантированно NULL, безусловный Destroy безопасен.
	tsDestroy(h)
}

func TestCreate_EmptyJSONObject(t *testing.T) {
	h, rc := tsCreate(cstr(t, `{}`), false)
	if rc != tsOK {
		t.Fatalf("rc = %d, want PHOTON_OK for `{}`", rc)
	}
	tsDestroy(h)
}

func TestDestroy_NilIsNoOp(t *testing.T) {
	tsDestroy(nil)
	tsDestroy(nil)
	tsDestroy(nil)
}

func TestDestroy_AfterFailedCreate_RepeatedIsSafe(t *testing.T) {
	h, rc := tsCreate(cstr(t, `not json at all`), false)
	if rc == tsOK {
		t.Fatal("expected Create to fail on invalid JSON")
	}
	if h != nil {
		t.Fatal("handle must be NULL after failed Create")
	}
	tsDestroy(h)
	tsDestroy(h)
}

func TestDestroy_IdempotentOnValidHandle(t *testing.T) {
	h, rc := tsCreate(cstr(t, `{}`), false)
	if rc != tsOK {
		t.Fatalf("Create: rc = %d", rc)
	}
	tsDestroy(h)
	tsDestroy(h) // повторный — не должен падать / double-free
	tsDestroy(h)
}

func TestCreate_MultipleIndependentHandles(t *testing.T) {
	cfg := cstr(t, `{}`)
	h1, rc1 := tsCreate(cfg, false)
	h2, rc2 := tsCreate(cfg, false)
	h3, rc3 := tsCreate(cfg, false)
	if rc1 != tsOK || rc2 != tsOK || rc3 != tsOK {
		t.Fatalf("Create rc: %d %d %d", rc1, rc2, rc3)
	}
	if h1 == h2 || h2 == h3 || h1 == h3 {
		t.Fatal("independent Create calls produced colliding handles")
	}

	tsDestroy(h2)
	if !tsCoreIsValid(h1) {
		t.Fatal("destroying h2 invalidated h1")
	}
	if !tsCoreIsValid(h3) {
		t.Fatal("destroying h2 invalidated h3")
	}
	tsDestroy(h1)
	tsDestroy(h3)
}

// ── Photon_GetLastError ──────────────────────────────────────────────────

func TestGetLastError_NilHandle(t *testing.T) {
	if tsGetLastError(nil) != nil {
		t.Fatal("GetLastError(nil) must return nil")
	}
}

func TestGetLastError_AfterSendActionFailure(t *testing.T) {
	h := mustCreate(t, `{}`)
	rc, _ := tsSendAction(h, 999999, cstr(t, `{}`), true)
	if rc != tsErrInvalidAction {
		t.Fatalf("rc = %d, want PHOTON_ERR_INVALID_ACTION", rc)
	}
	msg := tsGetLastError(h)
	if msg == nil || tsGoString(msg) == "" {
		t.Fatal("GetLastError must return a non-empty message after a failed SendAction")
	}
}

// ── Photon_SendAction: валидация action_code (ADR-026, п.5) ─────────────

func TestSendAction_UnknownActionCode(t *testing.T) {
	h := mustCreate(t, `{}`)
	rc, respOut := tsSendAction(h, 424242, cstr(t, `{}`), true)
	if rc != tsErrInvalidAction {
		t.Fatalf("rc = %d, want PHOTON_ERR_INVALID_ACTION", rc)
	}
	if respOut != nil {
		t.Fatal("response_out must stay nil on PHOTON_ERR_INVALID_ACTION")
	}
}

func TestSendAction_ZeroActionCodeIsInvalid(t *testing.T) {
	// 0 == ACTION_UNSPECIFIED — валидное значение enum, но не осмысленное
	// действие, поэтому тоже отклоняется.
	h := mustCreate(t, `{}`)
	rc, _ := tsSendAction(h, 0, cstr(t, `{}`), true)
	if rc != tsErrInvalidAction {
		t.Fatalf("rc = %d, want PHOTON_ERR_INVALID_ACTION for action_code=0", rc)
	}
}

func TestSendAction_KnownActionCode_NotYetImplemented(t *testing.T) {
	// 100 = ACTION_CREATE_POST — валидный код, но Dispatcher (Phase 13)
	// не реализован => честная PHOTON_ERR_INTERNAL, не INVALID_ACTION.
	h := mustCreate(t, `{}`)
	rc, _ := tsSendAction(h, 100, cstr(t, `{}`), true)
	if rc != tsErrInternal {
		t.Fatalf("rc = %d, want PHOTON_ERR_INTERNAL (known action, not implemented)", rc)
	}
}

func TestSendAction_AllKnownActionCodesAreAccepted(t *testing.T) {
	// Ни один код из enum Action (photon-core/protobuf) не должен
	// отвергаться как INVALID_ACTION — они все валидны, просто пока
	// не реализованы.
	h := mustCreate(t, `{}`)
	for _, code := range []int32{100, 114, 200, 210, 300, 314, 400, 405, 500, 505, 600, 602} {
		rc, _ := tsSendAction(h, code, cstr(t, `{}`), true)
		if rc == tsErrInvalidAction {
			t.Fatalf("action_code %d rejected as INVALID_ACTION, but it exists in enum Action", code)
		}
	}
}

func TestSendAction_NilHandle(t *testing.T) {
	rc, _ := tsSendAction(nil, 100, cstr(t, `{}`), true)
	if rc != tsErrInvalidArgument {
		t.Fatalf("rc = %d, want PHOTON_ERR_INVALID_ARGUMENT", rc)
	}
}

func TestSendAction_NilPayload(t *testing.T) {
	h := mustCreate(t, `{}`)
	rc, _ := tsSendAction(h, 100, nil, true)
	if rc != tsErrInvalidArgument {
		t.Fatalf("rc = %d, want PHOTON_ERR_INVALID_ARGUMENT for nil payload", rc)
	}
}

func TestSendAction_OnDestroyedHandle(t *testing.T) {
	h, rc := tsCreate(cstr(t, `{}`), false)
	if rc != tsOK {
		t.Fatalf("Create: rc = %d", rc)
	}
	tsDestroy(h)

	rc2, _ := tsSendAction(h, 100, cstr(t, `{}`), true)
	if rc2 != tsErrInvalidArgument {
		t.Fatalf("rc = %d, want PHOTON_ERR_INVALID_ARGUMENT on destroyed handle", rc2)
	}
}

func TestSendAction_NilResponseOutIsAllowed(t *testing.T) {
	// Вызывающий может не интересоваться ответом — передача NULL
	// вместо response_out не должна падать.
	h := mustCreate(t, `{}`)
	rc, _ := tsSendAction(h, 100, cstr(t, `{}`), false)
	if rc != tsErrInternal {
		t.Fatalf("rc = %d, want PHOTON_ERR_INTERNAL", rc)
	}
}

// ── PhotonString ownership ───────────────────────────────────────────────

func TestPhotonString_DataAndFree(t *testing.T) {
	ps := tsNewString("hello photon")
	if got := tsGoString(tsStringData(ps)); got != "hello photon" {
		t.Fatalf("StringData = %q", got)
	}
	if n := tsStringLen(ps); n != len("hello photon") {
		t.Fatalf("StringLen = %d, want %d", n, len("hello photon"))
	}
	tsFreeString(ps)
}

func TestPhotonString_DataIsStableAcrossCalls(t *testing.T) {
	// Повторные вызовы должны возвращать один и тот же указатель
	// (кэшируется), иначе вызывающий не знает, сколько раз освобождать.
	ps := tsNewString("stable")
	if tsStringData(ps) != tsStringData(ps) {
		t.Fatal("StringData returned different pointers on repeated calls")
	}
	tsFreeString(ps)
}

func TestPhotonString_EmptyString(t *testing.T) {
	ps := tsNewString("")
	if n := tsStringLen(ps); n != 0 {
		t.Fatalf("StringLen = %d, want 0", n)
	}
	if got := tsGoString(tsStringData(ps)); got != "" {
		t.Fatalf("StringData = %q, want empty", got)
	}
	tsFreeString(ps)
}

func TestPhotonString_FreeNilIsNoOp(t *testing.T) {
	tsFreeString(nil)
	tsFreeString(nil)
}

func TestPhotonString_DataAfterFreeReturnsNil(t *testing.T) {
	ps := tsNewString("gone")
	tsFreeString(ps)
	if tsStringData(ps) != nil {
		t.Fatal("StringData on a freed handle must return nil, not a dangling pointer")
	}
	if tsStringLen(ps) != 0 {
		t.Fatal("StringLen on a freed handle must return 0")
	}
}

// ── PhotonBytes ownership ────────────────────────────────────────────────

func TestPhotonBytes_DataAndFree(t *testing.T) {
	pb := tsNewBytes([]byte{1, 2, 3, 4, 5})
	if n := tsBytesLen(pb); n != 5 {
		t.Fatalf("BytesLen = %d, want 5", n)
	}
	dataPtr := tsBytesData(pb)
	if dataPtr == nil {
		t.Fatal("BytesData returned nil for non-empty data")
	}
	got := tsGoBytes(dataPtr, 5)
	for i, want := range []byte{1, 2, 3, 4, 5} {
		if got[i] != want {
			t.Fatalf("byte %d = %d, want %d", i, got[i], want)
		}
	}
	tsFreeBytes(pb)
}

func TestPhotonBytes_EmptyDataReturnsNil(t *testing.T) {
	pb := tsNewBytes([]byte{})
	if tsBytesData(pb) != nil {
		t.Fatal("BytesData must return nil for empty data")
	}
	if tsBytesLen(pb) != 0 {
		t.Fatal("BytesLen must return 0 for empty data")
	}
	tsFreeBytes(pb)
}

func TestPhotonBytes_FreeNilIsNoOp(t *testing.T) {
	tsFreeBytes(nil)
	tsFreeBytes(nil)
}

func TestPhotonBytes_DataAfterFreeReturnsNil(t *testing.T) {
	pb := tsNewBytes([]byte{9, 9, 9})
	tsFreeBytes(pb)
	if tsBytesData(pb) != nil {
		t.Fatal("BytesData on a freed handle must return nil")
	}
	if tsBytesLen(pb) != 0 {
		t.Fatal("BytesLen on a freed handle must return 0")
	}
}

// ── ABI Versioning ────────────────────────────────────────────────────────

func TestGetABIVersion_PackedCorrectly(t *testing.T) {
	v := tsGetABIVersion()
	if major := v >> 16; major != tsABIMajor() {
		t.Fatalf("MAJOR = %d, want %d", major, tsABIMajor())
	}
	if minor := v & 0xFFFF; minor != tsABIMinor() {
		t.Fatalf("MINOR = %d, want %d", minor, tsABIMinor())
	}
}

// ── Photon_SetEventCallback ────────────────────────────────────────────────

func TestSetEventCallback_NilHandleDoesNotPanic(t *testing.T) {
	tsSetEventCallback(nil)
}

func TestSetEventCallback_OnValidHandle(t *testing.T) {
	h := mustCreate(t, `{}`)
	tsSetEventCallback(h)
}

func TestSetEventCallback_OnDestroyedHandleDoesNotPanic(t *testing.T) {
	h, rc := tsCreate(cstr(t, `{}`), false)
	if rc != tsOK {
		t.Fatalf("Create: rc = %d", rc)
	}
	tsDestroy(h)
	tsSetEventCallback(h)
}
