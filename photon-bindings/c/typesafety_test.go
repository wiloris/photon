package main

import (
	"bytes"
	"testing"
	"unsafe"
)

// Типовая безопасность непрозрачных handle — контракт ADR-061.
//
// Разрушающая функция, получившая handle ЧУЖОГО типа, не делает ничего;
// объект остаётся зарегистрированным и освобождается своей функцией.
//
// Дефект, ради которого это записано: прежде `Photon_FreeString` снимал с
// учёта что угодно, включая handle инстанса. Инстанс исчезал из реестра
// ЖИВЫМ — `disp.Close()` не вызывался, насос событий продолжал работать,
// `teardownDone` не закрывался, а handle больше не резолвился, поэтому
// последующий `Photon_Destroy` промахивался. Утечка горутин и подписки,
// заметная только по поведению.
//
// Каждый тест проверяет не отсутствие паники, а ДВА шага: чужой объект
// пережил неправильный вызов и после него доступен своей правильной
// операции. Матрица полная — три разрушающие функции × два чужих типа.
//
// Что здесь НЕ проверяется и не закрепляется (ADR-061 п. 3): двойной
// `Free*` своего типа, вызов после закрытия окна времени жизни (ADR-059
// п. 2) и поведение устаревшего указателя на переиспользованном адресе.
// Это undefined behavior, и зелёный тест на него обещал бы то, чего
// контракт не даёт.

// liveInstance — инстанс без адреса Bridge: сети не касается, но живой,
// и его состояние наблюдаемо через SendAction.
func liveInstance(t *testing.T) unsafe.Pointer {
	t.Helper()
	h, rc := tsCreate(cstr(t, `{}`), false)
	if rc != tsOK || h == nil {
		t.Fatalf("Photon_Create: rc = %d, handle = %v", rc, h)
	}
	return h
}

// requireInstanceAlive доказывает, что инстанс уцелел: живой отвечает на
// SendAction кодом из ядра, а снятый с учёта не резолвится вовсе и даёт
// INVALID_ARGUMENT по промаху реестра.
func requireInstanceAlive(t *testing.T, h unsafe.Pointer) {
	t.Helper()
	rc, _ := tsSendAction(h, 200, cstr(t, `{}`), false)
	if rc != tsErrNotConnected {
		t.Fatalf("инстанс не пережил чужой вызов: SendAction rc = %d, ожидался NOT_CONNECTED (%d)",
			rc, tsErrNotConnected)
	}
}

func requireStringAlive(t *testing.T, ps unsafe.Pointer, want string) {
	t.Helper()
	if got := tsGoString(tsStringData(ps)); got != want {
		t.Fatalf("строка не пережила чужой вызов: StringData = %q, ожидалось %q", got, want)
	}
	if n := tsStringLen(ps); n != len(want) {
		t.Fatalf("StringLen = %d, ожидалось %d", n, len(want))
	}
}

func requireBytesAlive(t *testing.T, pb unsafe.Pointer, want []byte) {
	t.Helper()
	if n := tsBytesLen(pb); n != len(want) {
		t.Fatalf("байты не пережили чужой вызов: BytesLen = %d, ожидалось %d", n, len(want))
	}
	if got := tsGoBytes(tsBytesData(pb), len(want)); !bytes.Equal(got, want) {
		t.Fatalf("BytesData = %v, ожидалось %v", got, want)
	}
}

// ── 1. Photon_Destroy(PhotonString) ──────────────────────────────────────

func TestTypeSafety_DestroyOnStringIsNoOp(t *testing.T) {
	ps := tsNewString("строка переживает Destroy")

	tsDestroy(ps)

	requireStringAlive(t, ps, "строка переживает Destroy")
	tsFreeString(ps) // и освобождается своей функцией
}

// ── 2. Photon_Destroy(PhotonBytes) ───────────────────────────────────────

func TestTypeSafety_DestroyOnBytesIsNoOp(t *testing.T) {
	want := []byte{1, 2, 3, 4, 5}
	pb := tsNewBytes(want)

	tsDestroy(pb)

	requireBytesAlive(t, pb, want)
	tsFreeBytes(pb)
}

// ── 3. Photon_FreeString(handle инстанса) ────────────────────────────────
//
// Самый дорогой случай: до ADR-061 инстанс исчезал из реестра живым.

func TestTypeSafety_FreeStringOnInstanceIsNoOp(t *testing.T) {
	h := liveInstance(t)

	tsFreeString(h)

	requireInstanceAlive(t, h)
	tsDestroy(h) // настоящее уничтожение по-прежнему возможно
}

// ── 4. Photon_FreeString(PhotonBytes) ────────────────────────────────────

func TestTypeSafety_FreeStringOnBytesIsNoOp(t *testing.T) {
	want := []byte{9, 8, 7}
	pb := tsNewBytes(want)

	tsFreeString(pb)

	requireBytesAlive(t, pb, want)
	tsFreeBytes(pb)
}

// ── 5. Photon_FreeBytes(handle инстанса) ─────────────────────────────────

func TestTypeSafety_FreeBytesOnInstanceIsNoOp(t *testing.T) {
	h := liveInstance(t)

	tsFreeBytes(h)

	requireInstanceAlive(t, h)
	tsDestroy(h)
}

// ── 6. Photon_FreeBytes(PhotonString) ────────────────────────────────────

func TestTypeSafety_FreeBytesOnStringIsNoOp(t *testing.T) {
	ps := tsNewString("строка переживает FreeBytes")

	tsFreeBytes(ps)

	requireStringAlive(t, ps, "строка переживает FreeBytes")
	tsFreeString(ps)
}

// Чужой вызов не снимает объект с учёта — проверяется прямо по реестру, а
// не только по поведению: запись обязана остаться на месте.
func TestTypeSafety_ForeignCallDoesNotRetire(t *testing.T) {
	h := liveInstance(t)
	ps := tsNewString("s")
	pb := tsNewBytes([]byte{1})

	tsFreeString(h)
	tsFreeBytes(h)
	tsDestroy(ps)
	tsDestroy(pb)
	tsFreeBytes(ps)
	tsFreeString(pb)

	for name, p := range map[string]unsafe.Pointer{"инстанс": h, "строка": ps, "байты": pb} {
		if _, ok := tsLookup(p); !ok {
			t.Errorf("%s снят с учёта чужим вызовом", name)
		}
	}

	tsDestroy(h)
	tsFreeString(ps)
	tsFreeBytes(pb)
}
