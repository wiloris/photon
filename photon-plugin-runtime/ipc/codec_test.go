package ipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestCodec_RoundTrip(t *testing.T) {
	cases := []*Message{
		{Type: MsgHello, HelloProtocolVersion: 1},
		{Type: MsgHelloAck, HelloProtocolVersion: 1, HelloPluginID: "com.engram.ssh",
			HelloCapabilities: []string{"ssh.session", "ssh.exec"}},
		{Type: MsgRequest, RequestID: 42, Op: "storage.read", Payload: []byte("key")},
		{Type: MsgResponse, RequestID: 42, Payload: []byte("value")},
		{Type: MsgResponse, RequestID: 43, ErrorCode: ErrForbidden, ErrorMessage: "permission denied"},
		{Type: MsgCancel, RequestID: 42},
		{Type: MsgEvent, EventType: "chat.message", Payload: []byte(`{"id":1}`)},
		{Type: MsgStop},
		{Type: MsgStopAck},
	}

	for _, want := range cases {
		t.Run(want.Type.String(), func(t *testing.T) {
			var buf bytes.Buffer
			if err := Encode(&buf, want); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := Decode(&buf)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if got.Type != want.Type || got.RequestID != want.RequestID || got.Op != want.Op {
				t.Fatalf("header mismatch:\ngot  %+v\nwant %+v", got, want)
			}
			if !bytes.Equal(got.Payload, want.Payload) {
				t.Fatalf("payload: got %q want %q", got.Payload, want.Payload)
			}
			if got.ErrorCode != want.ErrorCode || got.ErrorMessage != want.ErrorMessage {
				t.Fatalf("error fields mismatch")
			}
			if got.HelloPluginID != want.HelloPluginID {
				t.Fatalf("plugin id: got %q want %q", got.HelloPluginID, want.HelloPluginID)
			}
			if len(got.HelloCapabilities) != len(want.HelloCapabilities) {
				t.Fatalf("capabilities: got %v want %v", got.HelloCapabilities, want.HelloCapabilities)
			}
			if got.EventType != want.EventType {
				t.Fatalf("event type mismatch")
			}
		})
	}
}

func TestCodec_UnknownMessageTypeRejected(t *testing.T) {
	// Тип 99 не существует — должно быть отвергнуто, а не тихо принято.
	body := []byte{99}
	body = binary.BigEndian.AppendUint64(body, 0)
	full := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(full, uint32(len(body)))
	full = append(full, body...)

	_, err := Decode(bytes.NewReader(full))
	if !errors.Is(err, ErrUnknownType) && !errors.Is(err, ErrMalformed) {
		t.Fatalf("expected ErrUnknownType/ErrMalformed, got %v", err)
	}
	if !IsProtocolError(err) {
		t.Fatal("unknown message type must be a protocol error")
	}
}

// allocCountingReader считает реально прочитанные байты — доказывает,
// что декодер не пытается прочитать (и аллоцировать) тело по вражеской
// длине.
type allocCountingReader struct {
	data []byte
	pos  int
	read int
}

func (r *allocCountingReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	r.read += n
	return n, nil
}

func TestCodec_OversizedLengthRejectedWithoutAllocating(t *testing.T) {
	// Ключевой тест безопасности: заголовок объявляет 4 ГиБ. Декодер
	// обязан отказать ДО make([]byte, length) — иначе один пакет из
	// недоверенного процесса кладёт Runtime.
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 0xFFFFFFFF)
	r := &allocCountingReader{data: hdr[:]}

	_, err := Decode(r)
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("expected ErrMessageTooLarge, got %v", err)
	}
	if r.read != 4 {
		t.Fatalf("decoder read %d bytes, want exactly 4 (header only)", r.read)
	}
}

func TestCodec_HugeCapabilityCountRejected(t *testing.T) {
	// Внутреннее поле count тоже недоверенное: без предела заголовок с
	// count=0xFFFFFFFF вызвал бы аллокацию среза на 4 млрд элементов.
	body := []byte{uint8(MsgHelloAck)}
	body = binary.BigEndian.AppendUint64(body, 0)
	for i := 0; i < 4; i++ { // Op, Payload, ErrorCode, ErrorMessage
		body = binary.BigEndian.AppendUint32(body, 0)
	}
	body = binary.BigEndian.AppendUint32(body, 1) // protocol version
	body = binary.BigEndian.AppendUint32(body, 0) // plugin id
	body = binary.BigEndian.AppendUint32(body, 0xFFFFFFFF)

	full := make([]byte, 4)
	binary.BigEndian.PutUint32(full, uint32(len(body)))
	full = append(full, body...)

	if _, err := Decode(bytes.NewReader(full)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("expected ErrMalformed for huge capability count, got %v", err)
	}
}

func TestCodec_TruncatedInput(t *testing.T) {
	for _, data := range [][]byte{
		{},
		{0, 0},
		{0, 0, 0, 10}, // заголовок обещает 10 байт, тела нет
	} {
		if _, err := Decode(bytes.NewReader(data)); err == nil {
			t.Fatalf("input %v: expected an error", data)
		}
	}
}

func TestCodec_EmptyMessageRejected(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 0)
	if _, err := Decode(bytes.NewReader(hdr[:])); !errors.Is(err, ErrMalformed) {
		t.Fatalf("expected ErrMalformed for zero-length message, got %v", err)
	}
}

func TestCodec_TrailingBytesRejected(t *testing.T) {
	// Лишние байты в конце — рассинхрон потока, а не безобидный мусор.
	var buf bytes.Buffer
	Encode(&buf, &Message{Type: MsgStop})
	data := buf.Bytes()
	// увеличиваем объявленную длину, дописывая мусор
	binary.BigEndian.PutUint32(data[:4], uint32(len(data)-4+3))
	data = append(data, 1, 2, 3)

	if _, err := Decode(bytes.NewReader(data)); !errors.Is(err, ErrMalformed) {
		t.Fatalf("expected ErrMalformed for trailing bytes, got %v", err)
	}
}

func TestCodec_EncodeRejectsOversizedPayload(t *testing.T) {
	err := Encode(io.Discard, &Message{Type: MsgRequest, Payload: make([]byte, maxMessageSize+1)})
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("expected ErrMessageTooLarge, got %v", err)
	}
}

func TestCodec_IOErrorIsNotProtocolError(t *testing.T) {
	// Разрыв канала означает завершение процесса, а не враждебные
	// данные — реакция должна отличаться.
	_, err := Decode(bytes.NewReader(nil))
	if IsProtocolError(err) {
		t.Fatal("EOF must not be classified as a protocol error")
	}
}

func FuzzDecode(f *testing.F) {
	var buf bytes.Buffer
	Encode(&buf, &Message{Type: MsgRequest, RequestID: 1, Op: "storage.read", Payload: []byte("x")})
	f.Add(buf.Bytes())
	f.Add([]byte{0, 0, 0, 1, 99})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := Decode(bytes.NewReader(data))
		if err != nil {
			return
		}
		if m == nil {
			t.Fatal("nil message with nil error")
		}
		if !m.Type.valid() {
			t.Fatalf("decoded invalid message type %d", uint8(m.Type))
		}
		if len(m.Payload) > maxMessageSize {
			t.Fatalf("payload %d exceeds max", len(m.Payload))
		}
	})
}
