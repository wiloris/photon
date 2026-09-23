package pion

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pion/sctp"
	"github.com/wiloris/photon-core/ports"
)

var _ ports.Session = (*session)(nil)

// session — установленное (или устанавливаемое) P2P-соединение.
//
// Возвращается из AcceptOffer ДО завершения согласования, поэтому все методы
// подчиняются блокирующей семантике ADR-044 §1: ждут established, терминальной
// ошибки или отмены переданного контекста. Смешанного контракта «где-то
// блокирует, где-то сразу ошибка» здесь нет — это и был дефект, найденный в
// комментарии порта до реализации.
type session struct {
	peerExpected ports.PublicKey

	ready  chan struct{} // закрывается при established
	failed chan struct{} // закрывается при терминальном исходе
	once   sync.Once
	err    atomic.Value // error, установлена до закрытия failed

	mu       sync.Mutex
	peer     ports.PublicKey
	assoc    *sctp.Association
	endpoint ports.Endpoint
	nextID   uint16
	closed   bool

	bidi chan streamPair
	uni  chan streamPair

	onClose func() // снимает сессию с учёта у адаптера
}

func newSession(expected ports.PublicKey, onClose func()) *session {
	return &session{
		peerExpected: expected,
		ready:        make(chan struct{}),
		failed:       make(chan struct{}),
		bidi:         make(chan streamPair, 8),
		uni:          make(chan streamPair, 8),
		onClose:      onClose,
		// Идентификаторы потоков: инициатор чётные, отвечающий нечётные —
		// иначе обе стороны заняли бы один и тот же id. Начальное значение
		// проставляется в establish().
	}
}

// establish переводит сессию в established. Вызывается один раз.
func (s *session) establish(peer ports.PublicKey, assoc *sctp.Association, ep ports.Endpoint, firstStreamID uint16) {
	s.mu.Lock()
	s.peer, s.assoc, s.endpoint, s.nextID = peer, assoc, ep, firstStreamID
	s.mu.Unlock()
	s.once.Do(func() { close(s.ready) })
	go s.acceptLoop()
}

// fail переводит сессию в терминальное состояние. Необратимо (ADR-044 §5):
// повторные вызовы методов возвращают ту же ошибку, а не согласовываются
// заново.
func (s *session) fail(err error) {
	s.once.Do(func() {
		if err == nil {
			err = ports.ErrSessionFailed
		}
		s.err.Store(err)
		close(s.failed)
	})
}

func (s *session) failure() error {
	if v := s.err.Load(); v != nil {
		return v.(error)
	}
	return ports.ErrSessionFailed
}

// await — единственная точка ожидания. Все методы проходят через неё, поэтому
// семантика одинакова у всех и не может разойтись.
func (s *session) await(ctx context.Context) error {
	select {
	case <-s.ready:
		return nil
	case <-s.failed:
		return s.failure()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Peer возвращает КРИПТОГРАФИЧЕСКИ ПОДТВЕРЖДЁННЫЙ ключ (ADR-041 §1).
//
// Контекста в сигнатуре порта нет, поэтому ожидание ограничено терминальным
// исходом — а он гарантирован обязательным потолком таймаута установления
// (ADR-044 §3). Возврат «ожидаемого» ключа до подтверждения запрещён.
func (s *session) Peer() ports.PublicKey {
	select {
	case <-s.ready:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.peer
	case <-s.failed:
		return nil
	}
}

func (s *session) RemoteEndpoint() ports.Endpoint {
	// Диагностика — не блокирует (p2p-session.md §6 п. 2).
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endpoint
}

func (s *session) OpenStream(ctx context.Context) (ports.Stream, error) {
	p, err := s.open(ctx, dirBidi)
	if err != nil {
		return nil, err
	}
	return &stream{p: p}, nil
}

func (s *session) OpenUniStream(ctx context.Context) (ports.SendStream, error) {
	p, err := s.open(ctx, dirUni)
	if err != nil {
		return nil, err
	}
	return &sendStream{p: p}, nil
}

func (s *session) open(ctx context.Context, dir byte) (streamPair, error) {
	if err := s.await(ctx); err != nil {
		return streamPair{}, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return streamPair{}, ports.ErrSessionFailed
	}
	id := s.nextID
	s.nextID += 2
	assoc := s.assoc
	s.mu.Unlock()

	st, err := assoc.OpenStream(id, sctp.PayloadTypeWebRTCBinary)
	if err != nil {
		return streamPair{}, fmt.Errorf("pion: open stream: %w", err)
	}
	if err := writeDirection(st, dir); err != nil {
		_ = st.Close()
		return streamPair{}, err
	}
	return newStreamPair(st, assoc.MaxMessageSize()), nil
}

func (s *session) AcceptStream(ctx context.Context) (ports.Stream, error) {
	p, err := s.accept(ctx, s.bidi)
	if err != nil {
		return nil, err
	}
	return &stream{p: p}, nil
}

func (s *session) AcceptUniStream(ctx context.Context) (ports.RecvStream, error) {
	p, err := s.accept(ctx, s.uni)
	if err != nil {
		return nil, err
	}
	return &recvStream{p: p}, nil
}

func (s *session) accept(ctx context.Context, q chan streamPair) (streamPair, error) {
	if err := s.await(ctx); err != nil {
		return streamPair{}, err
	}
	select {
	case p, ok := <-q:
		if !ok {
			return streamPair{}, ports.ErrSessionFailed
		}
		return p, nil
	case <-s.failed:
		return streamPair{}, s.failure()
	case <-ctx.Done():
		return streamPair{}, ctx.Err()
	}
}

// acceptLoop читает признак направления и раскладывает входящие потоки по
// двум очередям. Без него AcceptStream не смог бы отличить двунаправленный
// поток от однонаправленного: у SCTP это один и тот же примитив.
func (s *session) acceptLoop() {
	for {
		st, err := s.assoc.AcceptStream()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if !closed {
				s.fail(fmt.Errorf("pion: accept stream: %w", errors.Join(err, ports.ErrSessionFailed)))
			}
			return
		}
		pair := newStreamPair(st, s.assoc.MaxMessageSize())
		dir, derr := readDirection(pair.r)
		if derr != nil {
			// Неизвестное направление отвергает ПОТОК, не соединение.
			_ = st.Close()
			continue
		}
		q := s.bidi
		if dir == dirUni {
			q = s.uni
		}
		select {
		case q <- pair:
		case <-s.failed:
			_ = st.Close()
			return
		}
	}
}

// Close закрывает эту сессию и её потоки. На другие сессии и на экземпляр P2P
// не влияет (ADR-044 §8).
func (s *session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil // идемпотентно
	}
	s.closed = true
	assoc := s.assoc
	s.mu.Unlock()

	s.fail(ports.ErrSessionFailed)
	var err error
	if assoc != nil {
		err = assoc.Close()
	}
	if s.onClose != nil {
		s.onClose()
	}
	return err
}
