package aesgcm

import (
	"bytes"
	"testing"
)

func TestHKDF_Deterministic(t *testing.T) {
	ikm := []byte("input key material")
	prk1 := hkdfExtract(nil, ikm)
	prk2 := hkdfExtract(nil, ikm)
	if !bytes.Equal(prk1, prk2) {
		t.Fatal("hkdfExtract is not deterministic for the same input")
	}

	out1, err := hkdfExpand(prk1, []byte("info-a"), 32)
	if err != nil {
		t.Fatalf("hkdfExpand: %v", err)
	}
	out2, err := hkdfExpand(prk1, []byte("info-a"), 32)
	if err != nil {
		t.Fatalf("hkdfExpand: %v", err)
	}
	if !bytes.Equal(out1, out2) {
		t.Fatal("hkdfExpand is not deterministic for the same input")
	}
}

func TestHKDF_DifferentInfoProducesDifferentOutput(t *testing.T) {
	prk := hkdfExtract(nil, []byte("ikm"))

	outA, err := hkdfExpand(prk, []byte("info-a"), 32)
	if err != nil {
		t.Fatalf("hkdfExpand (a): %v", err)
	}
	outB, err := hkdfExpand(prk, []byte("info-b"), 32)
	if err != nil {
		t.Fatalf("hkdfExpand (b): %v", err)
	}
	if bytes.Equal(outA, outB) {
		t.Fatal("different info produced identical output")
	}
}

func TestHKDF_ExpandBeyondSingleHashBlock(t *testing.T) {
	// sha256.Size = 32, запрашиваем больше одного блока — проверяем,
	// что многораундовый expand действительно работает, а не только
	// частный случай "ровно 32 байта".
	prk := hkdfExtract(nil, []byte("ikm"))
	out, err := hkdfExpand(prk, []byte("info"), 100)
	if err != nil {
		t.Fatalf("hkdfExpand: %v", err)
	}
	if len(out) != 100 {
		t.Fatalf("hkdfExpand returned %d bytes, want 100", len(out))
	}
}

func TestHKDF_RejectsExcessiveLength(t *testing.T) {
	prk := hkdfExtract(nil, []byte("ikm"))
	_, err := hkdfExpand(prk, []byte("info"), 255*32+1)
	if err == nil {
		t.Fatal("expected error for length exceeding 255*HashLen, got nil")
	}
}
