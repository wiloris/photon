package pion

import (
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

// stream — двунаправленный поток.
type stream struct{ s *sctp.Stream }

func (s *stream) Read(p []byte) (int, error)  { return s.s.Read(p) }
func (s *stream) Write(p []byte) (int, error) { return s.s.Write(p) }

// Close завершает отправку (half-close по контракту SendStream). У SCTP это
// выражается сбросом исходящего направления потока; приём продолжает работать
// до конца данных с той стороны.
func (s *stream) Close() error { return s.s.Close() }

// sendStream — пишущая половина однонаправленного потока.
type sendStream struct{ s *sctp.Stream }

func (s *sendStream) Write(p []byte) (int, error) { return s.s.Write(p) }
func (s *sendStream) Close() error                { return s.s.Close() }

// recvStream — читающая половина. Метода записи нет по типу: нарушение
// направления не выражается в API вовсе, а не проверяется в рантайме.
type recvStream struct{ s *sctp.Stream }

func (s *recvStream) Read(p []byte) (int, error) { return s.s.Read(p) }

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
func readDirection(s *sctp.Stream) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(s, b[:]); err != nil {
		return 0, fmt.Errorf("pion: read stream direction: %w", err)
	}
	switch b[0] {
	case dirBidi, dirUni:
		return b[0], nil
	default:
		return 0, fmt.Errorf("pion: unknown stream direction 0x%02x", b[0])
	}
}
