package p2phost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wiloris/photon-core/p2pmsg"
	"github.com/wiloris/photon-core/ports"
)

// Служебные потоки P2P-сессии (проект записи о служебных потоках к
// ADR-075 §2.3, не принят): несколько разговоров в одной сессии, каждый в
// своём bidi-потоке. Поток по умолчанию (run, 863, P2P_MESSAGE) не меняется
// ни на байт.
//
// Совместимость со старым узлом держится на порядке, а не на договорённости:
//
//   - поддержка объявляется uni-потоком (кадр "photon-streams/<N>"). Старый
//     узел uni-потоков не принимает; признак направления адаптера разводит
//     bidi и uni до прикладного кода, и в поток по умолчанию объявление не
//     попадает никогда;
//   - отвечающий объявляет только ПОСЛЕ того, как принял свой поток по
//     умолчанию; служебный поток открывается только ПОСЛЕ объявления
//     собеседника. Поэтому к старому узлу служебный bidi-поток не уходит
//     вовсе, а новый отвечающий не может принять служебный поток за поток по
//     умолчанию: к моменту, когда его открыли, поток по умолчанию уже занят.

const (
	// StreamsVersion — наибольшая версия конверта служебных потоков.
	StreamsVersion = 1
	// MaxStreams — предел служебных потоков сессии, в обе стороны вместе.
	MaxStreams = 16

	announcePrefix = "photon-streams/"
	openPrefix     = "open "
	replyOK        = "ok"
	refusedPrefix  = "refused "

	refuseInvalid = "invalid_service"
	refuseTooMany = "too_many_streams"
)

// Типы событий служебных потоков.
const (
	EventStreamsReady  = "P2P_STREAMS_READY"
	EventStreamOpened  = "P2P_STREAM_OPENED"
	EventStreamMessage = "P2P_STREAM_MESSAGE"
	EventStreamClosed  = "P2P_STREAM_CLOSED"
)

var (
	// ErrPeerUnsupported — собеседник не объявил служебные потоки за
	// AnnounceWait (старый узел либо объявление ещё не дошло).
	ErrPeerUnsupported = errors.New("p2phost: peer does not support service streams")
	// ErrStreamRefused — собеседник отказал в открытии потока.
	ErrStreamRefused = errors.New("p2phost: stream refused by peer")
	// ErrTooManyStreams — предел служебных потоков сессии.
	ErrTooManyStreams = errors.New("p2phost: too many streams")
	// ErrInvalidService — имя службы не проходит синтаксис.
	ErrInvalidService = errors.New("p2phost: invalid service name")
)

// serviceName — синтаксис имени службы. Смысл и версия функции — в самом
// имени (photon.call/1, app:<extension>/<name>); Core проверяет только форму.
var serviceName = regexp.MustCompile(`^[a-z][a-z0-9._:/-]{0,63}$`)

// ValidService сообщает, допустимо ли имя службы.
func ValidService(s string) bool { return serviceName.MatchString(s) }

type svcStream struct {
	id      string
	sess    *session
	service string
	conv    *p2pmsg.Conversation

	sendMu sync.Mutex

	// opened закрывается, когда открывающий получил ответ собеседника;
	// refusal — причина отказа (пусто — ok). Для принятого потока закрыт
	// сразу.
	opened  chan struct{}
	refusal string

	mu     sync.Mutex
	closed bool // снят с учёта; данные дочитываются и отбрасываются
	live   bool // открыт и сообщает события
}

// uniReader и uniWriter приводят половины uni-потока к ports.Stream, который
// принимает p2pmsg: кадрирование одно и то же в обоих направлениях.
type uniReader struct{ ports.RecvStream }

func (uniReader) Write([]byte) (int, error) { return 0, errors.New("p2phost: receive-only stream") }
func (uniReader) Close() error              { return nil }

type uniWriter struct{ ports.SendStream }

func (uniWriter) Read([]byte) (int, error) { return 0, io.EOF }

// startStreams запускается, когда поток по умолчанию готов: объявить себя и
// слушать объявления собеседника.
func (e *Executor) startStreams(s *session) {
	e.wg.Add(2)
	go e.announce(s)
	go e.acceptAnnouncements(s)
}

func (e *Executor) announce(s *session) {
	defer e.wg.Done()
	w, err := s.sess.OpenUniStream(s.ctx)
	if err != nil {
		return // собеседник без объявления от нас — просто старый протокол
	}
	conv := p2pmsg.New(uniWriter{w})
	_ = conv.Send([]byte(announcePrefix + strconv.Itoa(StreamsVersion)))
	_ = conv.Close()
}

// acceptAnnouncements принимает uni-потоки собеседника. Первый, чей кадр —
// объявление, открывает служебные потоки; прочие дочитываются и
// отбрасываются: непрочитанный поток занимал бы общий буфер ассоциации.
func (e *Executor) acceptAnnouncements(s *session) {
	defer e.wg.Done()
	for {
		r, err := s.sess.AcceptUniStream(s.ctx)
		if err != nil {
			return
		}
		conv := p2pmsg.New(uniReader{r})
		msg, err := conv.Recv()
		if err == nil {
			if v, ok := parseAnnouncement(msg); ok {
				s.peerOnce.Do(func() {
					s.peerVersion = min(v, StreamsVersion)
					close(s.peerStreams)
					e.pushIfAlive(s, Event{Type: EventStreamsReady, SessionID: s.id}, false)
					e.wg.Add(1)
					go e.acceptStreams(s)
				})
			}
		}
		e.wg.Add(1)
		go e.drain(conv)
	}
}

func parseAnnouncement(msg []byte) (int, bool) {
	v, ok := strings.CutPrefix(string(msg), announcePrefix)
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, false
	}
	return n, true
}

// drain дочитывает поток до конца, отбрасывая данные.
func (e *Executor) drain(conv *p2pmsg.Conversation) {
	defer e.wg.Done()
	for {
		if _, err := conv.Recv(); err != nil {
			return
		}
	}
}

// acceptStreams принимает служебные потоки собеседника. Слот берётся ДО
// AcceptStream: при исчерпании пределов новые потоки ждут в очереди
// адаптера, а уже открытые читаются как раньше.
func (e *Executor) acceptStreams(s *session) {
	defer e.wg.Done()
	slots := make(chan struct{}, 2*MaxStreams)
	for {
		select {
		case slots <- struct{}{}:
		case <-s.ctx.Done():
			return
		}
		st, err := s.sess.AcceptStream(s.ctx)
		if err != nil {
			return
		}
		e.wg.Add(1)
		go func() {
			defer func() { <-slots }()
			e.serveIncoming(s, st)
		}()
	}
}

func (e *Executor) serveIncoming(s *session, st ports.Stream) {
	defer e.wg.Done()
	conv := p2pmsg.New(st)
	first, err := conv.Recv()
	if err != nil {
		return
	}
	service, ok := strings.CutPrefix(string(first), openPrefix)
	if !ok || !ValidService(service) {
		e.refuse(conv, refuseInvalid)
		return
	}
	ss := &svcStream{sess: s, service: service, conv: conv, opened: make(chan struct{}), live: true}
	close(ss.opened)
	if err := e.register(ss); err != nil {
		reason := refuseTooMany
		if !errors.Is(err, ErrTooManyStreams) {
			reason = refuseInvalid // сессия закрывается — отказ любой причиной
		}
		e.refuse(conv, reason)
		return
	}
	if err := e.sendFrame(ss, []byte(replyOK)); err != nil {
		e.unregister(ss)
		return
	}
	e.pushStreamEvent(ss, Event{Type: EventStreamOpened, SessionID: s.id, StreamID: ss.id, Service: service}, false)
	e.read(ss)
}

func (e *Executor) refuse(conv *p2pmsg.Conversation, reason string) {
	_ = conv.Send([]byte(refusedPrefix + reason))
	_ = conv.Close()
	for {
		if _, err := conv.Recv(); err != nil {
			return
		}
	}
}

// register ставит поток на учёт сессии под пределом MaxStreams.
func (e *Executor) register(ss *svcStream) error {
	id, err := newHandle()
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, alive := e.sessions[ss.sess.id]; !alive {
		return ErrNotFound
	}
	if ss.sess.streams >= MaxStreams {
		return ErrTooManyStreams
	}
	ss.id = id
	ss.sess.streams++
	e.streams[id] = ss
	return nil
}

// unregister снимает поток с учёта; true — снял именно этот вызов.
func (e *Executor) unregister(ss *svcStream) bool {
	ss.mu.Lock()
	if ss.closed {
		ss.mu.Unlock()
		return false
	}
	ss.closed = true
	ss.live = false
	ss.mu.Unlock()
	e.mu.Lock()
	if _, ok := e.streams[ss.id]; ok {
		delete(e.streams, ss.id)
		ss.sess.streams--
	}
	e.mu.Unlock()
	return true
}

// closeStream снимает поток с учёта, завершает свою запись и сообщает
// причину — ровно один раз.
func (e *Executor) closeStream(ss *svcStream, reason string) {
	ss.mu.Lock()
	notify := ss.live && !ss.closed
	ss.mu.Unlock()
	if !e.unregister(ss) {
		return
	}
	ss.sendMu.Lock()
	_ = ss.conv.Close()
	ss.sendMu.Unlock()
	if notify {
		e.pushStreamEvent(ss, Event{Type: EventStreamClosed, SessionID: ss.sess.id, StreamID: ss.id, Reason: reason}, false)
	}
}

// pushStreamEvent (pushIfAlive) кладёт событие, только пока сессия на учёте:
// проверка под тем же замком, под которым finish снимает сессию, поэтому
// после P2P_CLOSED событий о её потоках не бывает по построению. bounded —
// событие подчиняется пределу очереди (сообщения); false — не поместилось.
func (e *Executor) pushStreamEvent(ss *svcStream, ev Event, bounded bool) bool {
	return e.pushIfAlive(ss.sess, ev, bounded)
}

func (e *Executor) pushIfAlive(s *session, ev Event, bounded bool) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, alive := e.sessions[s.id]; !alive {
		return true // сессия закрыта: дальше — только её P2P_CLOSED
	}
	ev.Owner = s.owner
	return e.deliverLocked(ev, bounded)
}

// read — приём сообщений потока до его конца. Снятый с учёта поток
// дочитывается и отбрасывается (общий буфер ассоциации, см. drain).
func (e *Executor) read(ss *svcStream) {
	for {
		msg, err := ss.conv.Recv()
		if err != nil {
			if e.sessionGone(ss.sess) {
				e.unregister(ss) // закрытие сессии — без событий о потоках
				return
			}
			reason := ReasonFailed
			if errors.Is(err, io.EOF) {
				reason = ReasonRemote
			}
			e.closeStream(ss, reason)
			return
		}
		ss.mu.Lock()
		live := ss.live
		ss.mu.Unlock()
		if !live {
			continue
		}
		if !e.pushStreamMessage(ss, msg) {
			e.closeStream(ss, ReasonSlowConsumer)
		}
	}
}

func (e *Executor) sessionGone(s *session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (e *Executor) pushStreamMessage(ss *svcStream, msg []byte) bool {
	return e.pushStreamEvent(ss, Event{Type: EventStreamMessage, SessionID: ss.sess.id, StreamID: ss.id, Payload: msg}, true)
}

func (e *Executor) sendFrame(ss *svcStream, payload []byte) error {
	ss.sendMu.Lock()
	defer ss.sendMu.Unlock()
	return ss.conv.Send(payload)
}

// OpenStream открывает служебный поток службы service в установленной
// сессии. Ждёт объявления собеседника не дольше AnnounceWait и ответа на
// открытие не дольше StreamOpenTimeout.
func (e *Executor) OpenStream(ctx context.Context, sessionID, service string) (string, error) {
	if !ValidService(service) {
		return "", fmt.Errorf("%w: %q", ErrInvalidService, service)
	}
	// Служебные потоки — у хоста приложения (владелец по умолчанию); у
	// потребителя с NoStreams их нет вовсе.
	if e.cfg.NoStreams {
		return "", ErrPeerUnsupported
	}
	s, ok := e.lookup("", sessionID)
	if !ok {
		return "", ErrNotFound
	}
	select {
	case <-s.ready:
	default:
		return "", ErrNotReady
	}
	if s.conv == nil {
		return "", ErrNotReady
	}
	wait := time.NewTimer(e.cfg.AnnounceWait)
	defer wait.Stop()
	select {
	case <-s.peerStreams:
	case <-wait.C:
		return "", ErrPeerUnsupported
	case <-s.ctx.Done():
		return "", ErrNotFound
	case <-ctx.Done():
		return "", ctx.Err()
	}

	octx, cancel := context.WithTimeout(ctx, e.cfg.StreamOpenTimeout)
	defer cancel()
	ss := &svcStream{sess: s, service: service, opened: make(chan struct{})}
	if err := e.register(ss); err != nil {
		return "", err
	}
	st, err := s.sess.OpenStream(octx)
	if err != nil {
		e.unregister(ss)
		return "", fmt.Errorf("%w: %v", ports.ErrSessionFailed, err)
	}
	ss.conv = p2pmsg.New(st)
	if err := e.sendFrame(ss, []byte(openPrefix+service)); err != nil {
		e.unregister(ss)
		return "", fmt.Errorf("%w: %v", ports.ErrSessionFailed, err)
	}
	e.wg.Add(1)
	go e.awaitReply(ss)

	select {
	case <-ss.opened:
	case <-octx.Done():
		e.closeStream(ss, ReasonLocal) // не открыт: событий о нём не было и не будет
		return "", fmt.Errorf("%w: no reply to open", ports.ErrSessionFailed)
	}
	if ss.refusal != "" {
		return "", fmt.Errorf("%w: %s", ErrStreamRefused, ss.refusal)
	}
	return ss.id, nil
}

// awaitReply читает ответ на открытие, затем сообщения потока.
func (e *Executor) awaitReply(ss *svcStream) {
	defer e.wg.Done()
	first, err := ss.conv.Recv()
	if err != nil {
		ss.refusal = "closed"
		close(ss.opened)
		e.unregister(ss)
		return
	}
	reply := string(first)
	if reply != replyOK {
		if r, ok := strings.CutPrefix(reply, refusedPrefix); ok {
			ss.refusal = r
		} else {
			ss.refusal = "protocol"
		}
		close(ss.opened)
		e.closeStream(ss, ReasonRemote)
		for {
			if _, err := ss.conv.Recv(); err != nil {
				return
			}
		}
	}
	ss.mu.Lock()
	if !ss.closed {
		ss.live = true
	}
	ss.mu.Unlock()
	close(ss.opened)
	e.read(ss)
}

func (e *Executor) lookupStream(id string) (*svcStream, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ss, ok := e.streams[id]
	if !ok || ss.sess.owner != "" {
		return nil, false
	}
	ss.mu.Lock()
	live := ss.live
	ss.mu.Unlock()
	return ss, live
}

// StreamSend отправляет сообщение по открытому служебному потоку.
func (e *Executor) StreamSend(id string, payload []byte) error {
	if len(payload) > MaxPayload {
		return ErrPayloadTooBig
	}
	ss, ok := e.lookupStream(id)
	if !ok {
		return ErrNotFound
	}
	if err := e.sendFrame(ss, payload); err != nil {
		e.closeStream(ss, ReasonFailed)
		return fmt.Errorf("%w: %v", ports.ErrSessionFailed, err)
	}
	return nil
}

// StreamClose закрывает свой служебный поток; сессия не затрагивается.
func (e *Executor) StreamClose(id string) error {
	ss, ok := e.lookupStream(id)
	if !ok {
		return ErrNotFound
	}
	e.closeStream(ss, ReasonLocal)
	return nil
}

// dropStreamsLocked снимает с учёта потоки закрывающейся сессии — без
// событий: P2P_CLOSED означает и их конец. Вызывается под e.mu.
func (e *Executor) dropStreamsLocked(s *session) {
	for id, ss := range e.streams {
		if ss.sess != s {
			continue
		}
		ss.mu.Lock()
		ss.closed = true
		ss.live = false
		ss.mu.Unlock()
		delete(e.streams, id)
	}
	s.streams = 0
}

// StreamCount — число служебных потоков на учёте (для проверок утечек).
func (e *Executor) StreamCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.streams)
}
