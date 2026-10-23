package tcp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Кадрирование для мультиплексора поверх TCP.
// Формат зафиксирован в WIRE_FORMAT.md ДО написания этого кода —
// выводился из требований ports.Connection, а не из возможностей QUIC.

const (
	frameVersion = 1
	headerSize   = 12
	maxFrameSize = 64 * 1024 // защита от аллокации по вражескому length
)

type frameType uint8

const (
	frameOpen  frameType = 0x01
	frameData  frameType = 0x02
	frameFin   frameType = 0x03
	frameReset frameType = 0x04
)

func (t frameType) String() string {
	switch t {
	case frameOpen:
		return "OPEN"
	case frameData:
		return "DATA"
	case frameFin:
		return "FIN"
	case frameReset:
		return "RESET"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02x)", uint8(t))
	}
}

// Флаги (только для OPEN).
const flagUni = 0x01

// Ошибки протокола. Каждая означает, что соединение должно быть закрыто:
// продолжать разбор потока, в котором уже обнаружен рассинхрон или
// намеренно некорректные данные, небезопасно.
var (
	errBadVersion   = errors.New("tcp: unsupported frame version")
	errBadType      = errors.New("tcp: unknown frame type")
	errBadReserved  = errors.New("tcp: reserved field must be zero")
	errBadFlags     = errors.New("tcp: flags must be zero for this frame type")
	errFrameTooBig  = errors.New("tcp: frame length exceeds maximum")
	errZeroStreamID = errors.New("tcp: stream_id 0 is not allowed")
)

type frame struct {
	typ      frameType
	flags    uint8
	streamID uint32
	payload  []byte
}

// encodeFrame сериализует кадр в w. Заголовок пишется одним Write вместе
// с payload там, где это возможно, чтобы не давать другим горутинам
// вклиниться между заголовком и телом (вызывающий всё равно обязан
// держать мьютекс сокета, но лишний шанс на ошибку не нужен).
func encodeFrame(w io.Writer, f frame) error {
	if len(f.payload) > maxFrameSize {
		return errFrameTooBig
	}

	buf := make([]byte, headerSize+len(f.payload))
	buf[0] = frameVersion
	buf[1] = uint8(f.typ)
	buf[2] = f.flags
	buf[3] = 0 // rsvd
	binary.BigEndian.PutUint32(buf[4:8], f.streamID)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(f.payload)))
	copy(buf[headerSize:], f.payload)

	_, err := w.Write(buf)
	return err
}

// decodeFrame читает один кадр из r.
//
// Проверки выполняются в порядке «сначала то, что не требует аллокации»:
// length валидируется ДО выделения буфера под payload, иначе кадр с
// length = 0xFFFFFFFF вызвал бы попытку аллокации 4 ГиБ — тривиальный DoS
// на парсере, разбирающем недоверенный вход.
func decodeFrame(r io.Reader) (frame, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return frame{}, err
	}

	if hdr[0] != frameVersion {
		return frame{}, fmt.Errorf("%w: got %d, want %d", errBadVersion, hdr[0], frameVersion)
	}

	typ := frameType(hdr[1])
	switch typ {
	case frameOpen, frameData, frameFin, frameReset:
	default:
		return frame{}, fmt.Errorf("%w: 0x%02x", errBadType, hdr[1])
	}

	flags := hdr[2]
	if typ != frameOpen && flags != 0 {
		return frame{}, fmt.Errorf("%w: type=%s flags=0x%02x", errBadFlags, typ, flags)
	}
	if typ == frameOpen && flags&^flagUni != 0 {
		return frame{}, fmt.Errorf("%w: unknown OPEN flags 0x%02x", errBadFlags, flags)
	}

	if hdr[3] != 0 {
		return frame{}, fmt.Errorf("%w: got 0x%02x", errBadReserved, hdr[3])
	}

	streamID := binary.BigEndian.Uint32(hdr[4:8])
	if streamID == 0 {
		return frame{}, errZeroStreamID
	}

	length := binary.BigEndian.Uint32(hdr[8:12])
	if length > maxFrameSize {
		// Критично: возвращаемся ДО выделения памяти.
		return frame{}, fmt.Errorf("%w: %d > %d", errFrameTooBig, length, maxFrameSize)
	}

	f := frame{typ: typ, flags: flags, streamID: streamID}
	if length > 0 {
		f.payload = make([]byte, length)
		if _, err := io.ReadFull(r, f.payload); err != nil {
			return frame{}, err
		}
	}
	return f, nil
}

// isProtocolError сообщает, требует ли ошибка закрытия соединения.
// Ошибки ввода-вывода (EOF, разрыв сокета) — не протокольные: они
// означают штатное или сетевое завершение, а не некорректные данные.
func isProtocolError(err error) bool {
	return errors.Is(err, errBadVersion) ||
		errors.Is(err, errBadType) ||
		errors.Is(err, errBadReserved) ||
		errors.Is(err, errBadFlags) ||
		errors.Is(err, errFrameTooBig) ||
		errors.Is(err, errZeroStreamID)
}
