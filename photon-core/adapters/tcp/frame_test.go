package tcp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// ── Round-trip ──────────────────────────────────────────────────────

func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		f    frame
	}{
		{"OPEN bidi", frame{typ: frameOpen, streamID: 2}},
		{"OPEN uni", frame{typ: frameOpen, flags: flagUni, streamID: 4}},
		{"DATA", frame{typ: frameData, streamID: 2, payload: []byte("photon payload")}},
		{"DATA empty", frame{typ: frameData, streamID: 6}},
		{"FIN", frame{typ: frameFin, streamID: 2}},
		{"RESET", frame{typ: frameReset, streamID: 8}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := encodeFrame(&buf, tc.f); err != nil {
				t.Fatalf("encode: %v", err)
			}
			got, err := decodeFrame(&buf)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.typ != tc.f.typ || got.flags != tc.f.flags || got.streamID != tc.f.streamID {
				t.Fatalf("header mismatch: got %+v, want %+v", got, tc.f)
			}
			if !bytes.Equal(got.payload, tc.f.payload) {
				t.Fatalf("payload: got %q, want %q", got.payload, tc.f.payload)
			}
		})
	}
}

func TestEncodeRejectsOversizedPayload(t *testing.T) {
	var buf bytes.Buffer
	err := encodeFrame(&buf, frame{typ: frameData, streamID: 2, payload: make([]byte, maxFrameSize+1)})
	if !errors.Is(err, errFrameTooBig) {
		t.Fatalf("expected errFrameTooBig, got %v", err)
	}
}

// ── Malformed frames (каждая строка таблицы WIRE_FORMAT.md) ────────

// buildHeader собирает заголовок вручную — именно так его прислал бы
// злонамеренный peer, в обход encodeFrame.
func buildHeader(ver, typ, flags, rsvd byte, streamID, length uint32) []byte {
	h := make([]byte, headerSize)
	h[0], h[1], h[2], h[3] = ver, typ, flags, rsvd
	binary.BigEndian.PutUint32(h[4:8], streamID)
	binary.BigEndian.PutUint32(h[8:12], length)
	return h
}

func TestDecodeRejectsBadVersion(t *testing.T) {
	r := bytes.NewReader(buildHeader(99, byte(frameData), 0, 0, 2, 0))
	_, err := decodeFrame(r)
	if !errors.Is(err, errBadVersion) {
		t.Fatalf("expected errBadVersion, got %v", err)
	}
	if !isProtocolError(err) {
		t.Fatal("bad version must be a protocol error (connection must close)")
	}
}

func TestDecodeRejectsUnknownType(t *testing.T) {
	r := bytes.NewReader(buildHeader(frameVersion, 0x99, 0, 0, 2, 0))
	_, err := decodeFrame(r)
	if !errors.Is(err, errBadType) {
		t.Fatalf("expected errBadType, got %v", err)
	}
	if !isProtocolError(err) {
		t.Fatal("unknown type must be a protocol error")
	}
}

func TestDecodeRejectsNonZeroReserved(t *testing.T) {
	r := bytes.NewReader(buildHeader(frameVersion, byte(frameData), 0, 0xFF, 2, 0))
	_, err := decodeFrame(r)
	if !errors.Is(err, errBadReserved) {
		t.Fatalf("expected errBadReserved, got %v", err)
	}
}

func TestDecodeRejectsFlagsOnNonOpenFrame(t *testing.T) {
	r := bytes.NewReader(buildHeader(frameVersion, byte(frameData), flagUni, 0, 2, 0))
	_, err := decodeFrame(r)
	if !errors.Is(err, errBadFlags) {
		t.Fatalf("expected errBadFlags, got %v", err)
	}
}

func TestDecodeRejectsUnknownOpenFlags(t *testing.T) {
	r := bytes.NewReader(buildHeader(frameVersion, byte(frameOpen), 0x80, 0, 2, 0))
	_, err := decodeFrame(r)
	if !errors.Is(err, errBadFlags) {
		t.Fatalf("expected errBadFlags, got %v", err)
	}
}

func TestDecodeRejectsZeroStreamID(t *testing.T) {
	r := bytes.NewReader(buildHeader(frameVersion, byte(frameData), 0, 0, 0, 0))
	_, err := decodeFrame(r)
	if !errors.Is(err, errZeroStreamID) {
		t.Fatalf("expected errZeroStreamID, got %v", err)
	}
}

// countingReader считает, сколько байт реально прочитано — чтобы
// доказать, что декодер НЕ пытается читать (и аллоцировать) payload
// по вражескому length.
type countingReader struct {
	data []byte
	pos  int
	read int
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	r.read += n
	return n, nil
}

func TestDecodeRejectsOversizedLengthWithoutAllocating(t *testing.T) {
	// Ключевой тест безопасности: кадр объявляет length = 0xFFFFFFFF
	// (4 ГиБ). Декодер обязан отказать ДО выделения буфера — иначе это
	// тривиальный DoS на парсере недоверенного входа.
	hdr := buildHeader(frameVersion, byte(frameData), 0, 0, 2, 0xFFFFFFFF)
	r := &countingReader{data: hdr}

	_, err := decodeFrame(r)
	if !errors.Is(err, errFrameTooBig) {
		t.Fatalf("expected errFrameTooBig, got %v", err)
	}
	if r.read != headerSize {
		t.Fatalf("decoder read %d bytes, want exactly %d (header only) — it must not touch the payload",
			r.read, headerSize)
	}
}

func TestDecodeRejectsLengthJustOverLimit(t *testing.T) {
	hdr := buildHeader(frameVersion, byte(frameData), 0, 0, 2, maxFrameSize+1)
	_, err := decodeFrame(bytes.NewReader(hdr))
	if !errors.Is(err, errFrameTooBig) {
		t.Fatalf("expected errFrameTooBig, got %v", err)
	}
}

func TestDecodeAcceptsLengthAtLimit(t *testing.T) {
	payload := make([]byte, maxFrameSize)
	var buf bytes.Buffer
	if err := encodeFrame(&buf, frame{typ: frameData, streamID: 2, payload: payload}); err != nil {
		t.Fatalf("encode at limit: %v", err)
	}
	f, err := decodeFrame(&buf)
	if err != nil {
		t.Fatalf("decode at limit: %v", err)
	}
	if len(f.payload) != maxFrameSize {
		t.Fatalf("payload len = %d, want %d", len(f.payload), maxFrameSize)
	}
}

func TestDecodeTruncatedHeader(t *testing.T) {
	_, err := decodeFrame(bytes.NewReader([]byte{1, 2, 3}))
	if err == nil {
		t.Fatal("expected error on truncated header")
	}
	if isProtocolError(err) {
		t.Fatal("truncated header is an I/O error, not a protocol error")
	}
}

func TestDecodeTruncatedPayload(t *testing.T) {
	hdr := buildHeader(frameVersion, byte(frameData), 0, 0, 2, 100)
	// Заголовок обещает 100 байт, даём 10.
	data := append(hdr, make([]byte, 10)...)
	_, err := decodeFrame(bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected error on truncated payload")
	}
}

func TestDecodeEmptyInput(t *testing.T) {
	_, err := decodeFrame(bytes.NewReader(nil))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

// ── Fuzzing декодера ────────────────────────────────────────────────

func FuzzDecodeFrame(f *testing.F) {
	// Seed: валидные кадры + заведомо некорректные.
	var buf bytes.Buffer
	encodeFrame(&buf, frame{typ: frameOpen, streamID: 2})
	f.Add(buf.Bytes())

	buf.Reset()
	encodeFrame(&buf, frame{typ: frameData, streamID: 4, payload: []byte("hello")})
	f.Add(buf.Bytes())

	f.Add(buildHeader(frameVersion, byte(frameData), 0, 0, 2, 0xFFFFFFFF))
	f.Add(buildHeader(99, 0x99, 0xFF, 0xFF, 0, 0))
	f.Add([]byte{})
	f.Add([]byte{0x01})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Единственное требование: декодер не паникует и не зависает на
		// произвольном недоверенном входе. Ошибка — нормальный результат.
		f, err := decodeFrame(bytes.NewReader(data))
		if err == nil {
			// Если разобралось — payload не должен превышать лимит.
			if len(f.payload) > maxFrameSize {
				t.Fatalf("decoded payload %d exceeds maxFrameSize %d", len(f.payload), maxFrameSize)
			}
			if f.streamID == 0 {
				t.Fatal("decoded frame with stream_id 0")
			}
		}
	})
}
