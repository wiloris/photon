package pion

import (
	"errors"
	"fmt"
	"io"

	"github.com/pion/sctp"
	"github.com/wiloris/photon-core/ports"
)

// Признак направления — первый байт вновь открытого потока.
//
// SCTP-поток двунаправлен, как и WebRTC DataChannel: нативных
// однонаправленных потоков не даёт ни один рассмотренный кандидат
// (ADR-043 §3). Но ADR-016 делает OpenUniStream/AcceptUniStream обязательными
// для ВСЕХ адаптеров, поэтому направление выражается соглашением поверх — тем
// же приёмом, что во флаге кадра adapters/tcp (WIRE_FORMAT.md), только здесь
// один байт поверх готового мультиплексора.
const (
	dirBidi = byte(0x01)
	dirUni  = byte(0x02)
)

var (
	_ ports.Stream     = (*stream)(nil)
	_ ports.SendStream = (*sendStream)(nil)
	_ ports.RecvStream = (*recvStream)(nil)
)

// sctpReader приводит message-oriented чтение SCTP к БАЙТОВОМУ потоку, который
// обещает ports.Stream (transport.go: Stream — это io.Reader/io.Writer).
//
// Без него контракт порта нарушается, причём МОЛЧА и с подменой данных.
// Механизм (pion/sctp v1.8.37, reassembly_queue.go:299-311): если буфер короче
// пришедшего сообщения, очередь НИЧЕГО не копирует в буфер, оставляет сообщение
// в очереди и возвращает `n` = размер ВСЕГО сообщения вместе с
// io.ErrShortBuffer. Стандартный io.ReadFull, получив n >= запрошенного,
// СБРАСЫВАЕТ ошибку (io.ReadAtLeast: `if n >= min { err = nil }`) — и отдаёт
// вызывающему нетронутый, то есть нулевой, буфер без единого признака отказа.
//
// Найдено двухпроцессным acceptance ADR-075: заголовок кадра читался как
// 00 00 00 00, сообщение приходило пустым, ошибки не было. Существующие тесты
// этого не показывали, потому что все они читают буфером РОВНО в размер
// сообщения (`make([]byte, 4)` под "ping"), а при точном совпадении ветка
// короткого буфера не исполняется вовсе.
//
// Читатель обязан быть ОДИН на поток: он держит непрочитанный хвост сообщения.
type sctpReader struct {
	s   *sctp.Stream
	rem []byte // хвост последнего SCTP-сообщения, не поместившийся в буфер
}

func (r *sctpReader) Read(p []byte) (int, error) {
	if len(r.rem) > 0 {
		n := copy(p, r.rem)
		r.rem = r.rem[n:]
		return n, nil
	}
	if len(p) == 0 {
		return 0, nil
	}
	n, err := r.s.Read(p)
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, io.ErrShortBuffer) {
		return n, err
	}
	// n — размер целого сообщения, а НЕ число скопированных байт: в p не
	// записано ничего, и сообщение осталось в очереди. Перечитываем его
	// целиком, отдаём сколько просили, остальное держим до следующего Read.
	whole := make([]byte, n)
	got, err := r.s.Read(whole)
	if err != nil {
		return 0, err
	}
	copied := copy(p, whole[:got])
	r.rem = append(r.rem[:0], whole[copied:got]...)
	return copied, nil
}

// streamPair — поток вместе со своим единственным читателем и пределом
// исходящего SCTP-сообщения.
type streamPair struct {
	s   *sctp.Stream
	r   *sctpReader
	max uint32
}

func newStreamPair(s *sctp.Stream, maxMessage uint32) streamPair {
	return streamPair{s: s, r: &sctpReader{s: s}, max: maxMessage}
}

// write — вторая половина приведения SCTP к байтовому потоку, симметричная
// sctpReader.
//
// io.Writer предела длины не имеет: байтовый поток обязан принять любой срез.
// SCTP же ограничивает ОДНО сообщение — pion отвергает запись длиннее
// Association.MaxMessageSize() (по умолчанию 65536) ошибкой
// ErrOutboundPacketTooLarge. Найдено тем же двухпроцессным acceptance:
// кадр в 1 MiB не уходил вовсе, `Send` возвращал «outbound packet larger than
// maximum message size: 65536».
//
// Предел спрашивается у ассоциации в рантайме, а не зашивается константой:
// он согласуется сторонами и может отличаться от умолчания.
//
// Дробление безопасно ровно потому, что приём собран в байтовый поток:
// границы SCTP-сообщений на той стороне уже ничего не значат. До правки
// sctpReader это дробление молча резало бы сообщения на части.
func (p streamPair) write(b []byte) (int, error) {
	if p.max == 0 || len(b) <= int(p.max) {
		return p.s.Write(b)
	}
	written := 0
	for written < len(b) {
		end := written + int(p.max)
		if end > len(b) {
			end = len(b)
		}
		n, err := p.s.Write(b[written:end])
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// stream — двунаправленный поток.
type stream struct{ p streamPair }

func (s *stream) Read(b []byte) (int, error)  { return s.p.r.Read(b) }
func (s *stream) Write(b []byte) (int, error) { return s.p.write(b) }

// Close завершает отправку (half-close по контракту SendStream). У SCTP это
// выражается сбросом исходящего направления потока; приём продолжает работать
// до конца данных с той стороны.
func (s *stream) Close() error { return s.p.s.Close() }

// sendStream — пишущая половина однонаправленного потока.
type sendStream struct{ p streamPair }

func (s *sendStream) Write(b []byte) (int, error) { return s.p.write(b) }
func (s *sendStream) Close() error                { return s.p.s.Close() }

// recvStream — читающая половина. Метода записи нет по типу: нарушение
// направления не выражается в API вовсе, а не проверяется в рантайме.
type recvStream struct{ p streamPair }

func (s *recvStream) Read(b []byte) (int, error) { return s.p.r.Read(b) }

// writeDirection пишет признак направления первым байтом, до прикладных
// данных.
func writeDirection(s *sctp.Stream, dir byte) error {
	if _, err := s.Write([]byte{dir}); err != nil {
		return fmt.Errorf("pion: write stream direction: %w", err)
	}
	return nil
}

// readDirection читает признак направления. Неизвестное значение отвергает
// поток, но не рушит соединение (p2p-session.md §7).
//
// Читает через sctpReader того же потока, а не напрямую: иначе хвост
// сообщения, в котором признак пришёл бы не один, терялся бы безвозвратно.
func readDirection(r *sctpReader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, fmt.Errorf("pion: read stream direction: %w", err)
	}
	switch b[0] {
	case dirBidi, dirUni:
		return b[0], nil
	default:
		return 0, fmt.Errorf("pion: unknown stream direction 0x%02x", b[0])
	}
}
