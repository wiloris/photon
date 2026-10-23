package ipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Wire-format: length-prefixed бинарные кадры.
//
// Выбор формата не влияет на семантику (docs/ipc.md, раздел 12):
// разделы 1-11 спецификации описаны в терминах сообщений и правил, а не
// сериализации. Проверочный критерий — смена формата не должна
// затрагивать security-логику; если затронет, значит семантика была
// описана в терминах формата.
//
// Здесь простой бинарный кодек, а не protobuf: он не тянет зависимость
// в SDK (который исполняется в недоверенном процессе плагина), а
// требования к формату минимальны — 8 типов сообщений с плоскими
// полями.

const (
	// maxMessageSize — предел размера одного сообщения.
	//
	// Проверяется ДО аллокации: сообщение приходит из недоверенного
	// процесса, и кадр с длиной 0xFFFFFFFF вызвал бы попытку выделить
	// 4 ГиБ. Тот же принцип, что в adapters/tcp и manifest.Parse.
	maxMessageSize = 4 << 20 // 4 MiB

	// maxStringLen — предел длины строковых полей внутри сообщения.
	maxStringLen = 64 << 10 // 64 KiB
)

var (
	ErrMessageTooLarge = errors.New("ipc: message exceeds maximum size")
	ErrMalformed       = errors.New("ipc: malformed message")
	ErrUnknownType     = errors.New("ipc: unknown message type")
)

// IsProtocolError сообщает, требует ли ошибка закрытия сессии.
//
// Ошибки ввода-вывода (EOF, разрыв канала) — не протокольные: они
// означают завершение процесса, а не некорректные данные. Разделение
// то же, что в adapters/tcp.
func IsProtocolError(err error) bool {
	return errors.Is(err, ErrMessageTooLarge) ||
		errors.Is(err, ErrMalformed) ||
		errors.Is(err, ErrUnknownType)
}

// Encode сериализует сообщение в w.
func Encode(w io.Writer, m *Message) error {
	body, err := encodeBody(m)
	if err != nil {
		return err
	}
	if len(body) > maxMessageSize {
		return fmt.Errorf("%w: %d bytes", ErrMessageTooLarge, len(body))
	}

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

func encodeBody(m *Message) ([]byte, error) {
	if !m.Type.valid() {
		return nil, fmt.Errorf("%w: %d", ErrUnknownType, uint8(m.Type))
	}

	buf := make([]byte, 0, 128+len(m.Payload))
	buf = append(buf, uint8(m.Type))
	buf = binary.BigEndian.AppendUint64(buf, m.RequestID)

	var err error
	if buf, err = appendString(buf, m.Op); err != nil {
		return nil, err
	}
	if buf, err = appendBytes(buf, m.Payload); err != nil {
		return nil, err
	}
	if buf, err = appendString(buf, string(m.ErrorCode)); err != nil {
		return nil, err
	}
	if buf, err = appendString(buf, m.ErrorMessage); err != nil {
		return nil, err
	}

	buf = binary.BigEndian.AppendUint32(buf, uint32(m.HelloProtocolVersion))
	if buf, err = appendString(buf, m.HelloPluginID); err != nil {
		return nil, err
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(m.HelloCapabilities)))
	for _, c := range m.HelloCapabilities {
		if buf, err = appendString(buf, c); err != nil {
			return nil, err
		}
	}
	if buf, err = appendString(buf, m.EventType); err != nil {
		return nil, err
	}
	return buf, nil
}

func appendString(buf []byte, s string) ([]byte, error) {
	if len(s) > maxStringLen {
		return nil, fmt.Errorf("%w: string field of %d bytes", ErrMessageTooLarge, len(s))
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s)))
	return append(buf, s...), nil
}

func appendBytes(buf []byte, b []byte) ([]byte, error) {
	if len(b) > maxMessageSize {
		return nil, fmt.Errorf("%w: payload of %d bytes", ErrMessageTooLarge, len(b))
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(b)))
	return append(buf, b...), nil
}

// Decode читает одно сообщение из r.
//
// Порядок проверок: сначала то, что не требует аллокации. Длина
// валидируется ДО выделения буфера — иначе вражеский заголовок
// становится DoS одним пакетом.
func Decode(r io.Reader) (*Message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(hdr[:])
	if length > maxMessageSize {
		// Критично: возвращаемся ДО make([]byte, length).
		return nil, fmt.Errorf("%w: %d > %d", ErrMessageTooLarge, length, maxMessageSize)
	}
	if length == 0 {
		return nil, fmt.Errorf("%w: empty message", ErrMalformed)
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return decodeBody(body)
}

func decodeBody(body []byte) (*Message, error) {
	d := &decoder{buf: body}

	typ := MessageType(d.u8())
	if !typ.valid() {
		return nil, fmt.Errorf("%w: %d", ErrUnknownType, uint8(typ))
	}

	m := &Message{Type: typ}
	m.RequestID = d.u64()
	m.Op = d.str()
	m.Payload = d.bytes()
	m.ErrorCode = ErrorCode(d.str())
	m.ErrorMessage = d.str()
	m.HelloProtocolVersion = int(d.u32())
	m.HelloPluginID = d.str()

	capCount := d.u32()
	// Предел на количество: без него заголовок с count=0xFFFFFFFF
	// вызвал бы аллокацию среза на 4 млрд элементов ещё до чтения
	// самих строк.
	if capCount > 1024 {
		return nil, fmt.Errorf("%w: too many capabilities (%d)", ErrMalformed, capCount)
	}
	if capCount > 0 {
		m.HelloCapabilities = make([]string, 0, capCount)
		for i := uint32(0); i < capCount; i++ {
			m.HelloCapabilities = append(m.HelloCapabilities, d.str())
		}
	}
	m.EventType = d.str()

	if d.err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, d.err)
	}
	if d.pos != len(d.buf) {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrMalformed, len(d.buf)-d.pos)
	}
	return m, nil
}

// decoder — последовательное чтение с накоплением ошибки, чтобы не
// проверять её после каждого поля. Любое чтение за границей буфера
// фиксирует ошибку и возвращает нулевое значение.
type decoder struct {
	buf []byte
	pos int
	err error
}

func (d *decoder) need(n int) bool {
	if d.err != nil {
		return false
	}
	if d.pos+n > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return false
	}
	return true
}

func (d *decoder) u8() uint8 {
	if !d.need(1) {
		return 0
	}
	v := d.buf[d.pos]
	d.pos++
	return v
}

func (d *decoder) u32() uint32 {
	if !d.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(d.buf[d.pos:])
	d.pos += 4
	return v
}

func (d *decoder) u64() uint64 {
	if !d.need(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(d.buf[d.pos:])
	d.pos += 8
	return v
}

func (d *decoder) bytes() []byte {
	n := d.u32()
	if d.err != nil {
		return nil
	}
	if int(n) > len(d.buf)-d.pos {
		// Заявленная длина больше остатка — не аллоцируем, фиксируем
		// ошибку.
		d.err = io.ErrUnexpectedEOF
		return nil
	}
	out := make([]byte, n)
	copy(out, d.buf[d.pos:d.pos+int(n)])
	d.pos += int(n)
	return out
}

func (d *decoder) str() string {
	b := d.bytes()
	if b == nil {
		return ""
	}
	return string(b)
}
