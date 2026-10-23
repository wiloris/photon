package sdk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Wire-формат IPC со стороны плагина.
//
// Зеркалит формат runtime (photon-plugin-runtime/ipc/codec.go), но
// реализован независимо: SDK не импортирует runtime (ADR-031). Это
// осознанное дублирование — альтернативой был бы общий пакет, который
// SDK и runtime импортируют оба, а значит недоверенный процесс получил
// бы кусок кода доверенной стороны в свой линковочный граф.
//
// Цена дублирования: расхождение форматов не поймается компилятором.
// Поэтому существует тест против НАСТОЯЩЕГО ipc.Session (см.
// photon-plugin-runtime/tests/integration) — он ловит расхождение
// сразу, а не в проде.
//
// Формат кадра:
//
//	[4 байта: длина тела][тело]
//
// Тело:
//
//	type              1 байт
//	request_id        8 байт
//	op                len-prefixed string
//	payload           len-prefixed bytes
//	error_code        len-prefixed string
//	error_message     len-prefixed string
//	protocol_version  4 байта
//	plugin_id         len-prefixed string
//	capabilities      4 байта (количество) + N строк
//	event_type        len-prefixed string

type messageType uint8

const (
	msgHello    messageType = 1
	msgHelloAck messageType = 2
	msgRequest  messageType = 3
	msgResponse messageType = 4
	msgCancel   messageType = 5
	msgEvent    messageType = 6
	msgStop     messageType = 7
	msgStopAck  messageType = 8
)

func (t messageType) String() string {
	switch t {
	case msgHello:
		return "HELLO"
	case msgHelloAck:
		return "HELLO_ACK"
	case msgRequest:
		return "REQUEST"
	case msgResponse:
		return "RESPONSE"
	case msgCancel:
		return "CANCEL"
	case msgEvent:
		return "EVENT"
	case msgStop:
		return "STOP"
	case msgStopAck:
		return "STOP_ACK"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(t))
	}
}

func (t messageType) valid() bool { return t >= msgHello && t <= msgStopAck }

const (
	// maxMessageSize и maxStringLen обязаны совпадать со значениями
	// runtime: клиент, отправивший кадр больше предела, получил бы
	// разрыв сессии как нарушение протокола.
	maxMessageSize = 4 << 20
	maxStringLen   = 64 << 10

	maxCapabilities = 1024
)

var (
	errMessageTooLarge = errors.New("sdk: message exceeds maximum size")
	errMalformed       = errors.New("sdk: malformed message")
	errUnknownType     = errors.New("sdk: unknown message type")
)

type message struct {
	Type      messageType
	RequestID uint64
	Op        string
	Payload   []byte

	ErrorCode    string
	ErrorMessage string

	ProtocolVersion int
	PluginID        string
	Capabilities    []string

	EventType string
}

func encodeMessage(w io.Writer, m *message) error {
	if !m.Type.valid() {
		return fmt.Errorf("%w: %d", errUnknownType, uint8(m.Type))
	}

	buf := make([]byte, 0, 128+len(m.Payload))
	buf = append(buf, uint8(m.Type))
	buf = binary.BigEndian.AppendUint64(buf, m.RequestID)

	var err error
	if buf, err = appendString(buf, m.Op); err != nil {
		return err
	}
	if buf, err = appendBytes(buf, m.Payload); err != nil {
		return err
	}
	if buf, err = appendString(buf, m.ErrorCode); err != nil {
		return err
	}
	if buf, err = appendString(buf, m.ErrorMessage); err != nil {
		return err
	}

	buf = binary.BigEndian.AppendUint32(buf, uint32(m.ProtocolVersion))
	if buf, err = appendString(buf, m.PluginID); err != nil {
		return err
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(m.Capabilities)))
	for _, c := range m.Capabilities {
		if buf, err = appendString(buf, c); err != nil {
			return err
		}
	}
	if buf, err = appendString(buf, m.EventType); err != nil {
		return err
	}

	if len(buf) > maxMessageSize {
		return fmt.Errorf("%w: %d bytes", errMessageTooLarge, len(buf))
	}

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(buf)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func appendString(buf []byte, s string) ([]byte, error) {
	if len(s) > maxStringLen {
		return nil, fmt.Errorf("%w: string field of %d bytes", errMessageTooLarge, len(s))
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s)))
	return append(buf, s...), nil
}

func appendBytes(buf []byte, b []byte) ([]byte, error) {
	if len(b) > maxMessageSize {
		return nil, fmt.Errorf("%w: payload of %d bytes", errMessageTooLarge, len(b))
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(b)))
	return append(buf, b...), nil
}

// decodeMessage читает один кадр.
//
// Длина проверяется ДО аллокации — runtime доверенный, но канал может
// быть повреждён, и заголовок с длиной 4 ГиБ не должен вызывать
// попытку выделить столько памяти. Та же защита, что на стороне
// runtime.
func decodeMessage(r io.Reader) (*message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(hdr[:])
	if length > maxMessageSize {
		return nil, fmt.Errorf("%w: %d > %d", errMessageTooLarge, length, maxMessageSize)
	}
	if length == 0 {
		return nil, fmt.Errorf("%w: empty message", errMalformed)
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	d := &wireDecoder{buf: body}
	typ := messageType(d.u8())
	if !typ.valid() {
		return nil, fmt.Errorf("%w: %d", errUnknownType, uint8(typ))
	}

	m := &message{Type: typ}
	m.RequestID = d.u64()
	m.Op = d.str()
	m.Payload = d.bytes()
	m.ErrorCode = d.str()
	m.ErrorMessage = d.str()
	m.ProtocolVersion = int(d.u32())
	m.PluginID = d.str()

	capCount := d.u32()
	if capCount > maxCapabilities {
		return nil, fmt.Errorf("%w: too many capabilities (%d)", errMalformed, capCount)
	}
	if capCount > 0 {
		m.Capabilities = make([]string, 0, capCount)
		for i := uint32(0); i < capCount; i++ {
			m.Capabilities = append(m.Capabilities, d.str())
		}
	}
	m.EventType = d.str()

	if d.err != nil {
		return nil, fmt.Errorf("%w: %v", errMalformed, d.err)
	}
	if d.pos != len(d.buf) {
		return nil, fmt.Errorf("%w: %d trailing bytes", errMalformed, len(d.buf)-d.pos)
	}
	return m, nil
}

type wireDecoder struct {
	buf []byte
	pos int
	err error
}

func (d *wireDecoder) need(n int) bool {
	if d.err != nil {
		return false
	}
	if d.pos+n > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return false
	}
	return true
}

func (d *wireDecoder) u8() uint8 {
	if !d.need(1) {
		return 0
	}
	v := d.buf[d.pos]
	d.pos++
	return v
}

func (d *wireDecoder) u32() uint32 {
	if !d.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(d.buf[d.pos:])
	d.pos += 4
	return v
}

func (d *wireDecoder) u64() uint64 {
	if !d.need(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(d.buf[d.pos:])
	d.pos += 8
	return v
}

func (d *wireDecoder) bytes() []byte {
	n := d.u32()
	if d.err != nil {
		return nil
	}
	if int(n) > len(d.buf)-d.pos {
		d.err = io.ErrUnexpectedEOF
		return nil
	}
	out := make([]byte, n)
	copy(out, d.buf[d.pos:d.pos+int(n)])
	d.pos += int(n)
	return out
}

func (d *wireDecoder) str() string {
	b := d.bytes()
	if b == nil {
		return ""
	}
	return string(b)
}
