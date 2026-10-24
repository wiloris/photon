// Package p2pmsg — кадрирование сообщений поверх потока ports.Stream.
//
// Спецификация: photon-core/docs/p2p-messaging.md. Решение: ADR-075.
//
// Слой даёт ровно одно: ГРАНИЦУ сообщения. Смысл нагрузки принадлежит
// приложению (ADR-003, ADR-030): Photon её не разбирает, не валидирует и не
// знает её кодировку. Ни message_id, ни ACK, ни повторов, ни очереди — они
// живут ВНУТРИ payload у того, кому понадобятся, и Photon от их появления не
// меняется (ADR-075 §2.7, §2.14).
//
// Зависимость ровно одна — ports.Stream. Ни pion, ни quic, ни tcp в графе
// пакета нет: тот же слой работает поверх любого адаптера. ports/*.go этим
// пакетом не меняется (ADR-020).
package p2pmsg

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/wiloris/photon/photon-core/ports"
)

// MaxMessageSize — предел ОДНОГО кадра, а не механизм передачи файлов
// (ADR-075 §2.14). Attachments в этот контракт не входят.
const MaxMessageSize = 1 << 20 // 1 MiB

// ErrMessageTooLarge — единственная экспортируемая ошибка пакета.
//
// Экспортируется потому, что это единственное условие, которое вызывающий
// может осмысленно РАЗЛИЧИТЬ и ПРЕДОТВРАТИТЬ. Остальные исходы — обёртки над
// ошибками потока плюс io.EOF/io.ErrUnexpectedEOF, различаемые errors.Is;
// заводить под них свои значения значило бы расширять поверхность без
// потребителя (p2p-messaging.md §7).
var ErrMessageTooLarge = errors.New("p2pmsg: message exceeds MaxMessageSize")

// Conversation — разговор на одном двунаправленном потоке.
//
// Это ИМЯ РАЗГОВОРА НА ПОТОКЕ, а не сущность мессенджера (ADR-075 §2.12): у
// него нет ни участников, ни темы, ни истории, ни идентификатора. Доменную
// модель строит ENGRAM, и в photon-core она не попадает.
//
// Потоком владеет Conversation; СЕССИЕЙ — вызывающий (ADR-044 §8). Close()
// здесь закрывает поток и никогда не трогает Session.
//
// Параллельные Send между собой и параллельные Recv между собой не
// поддерживаются — как и у ports.Stream под ним. Send параллельно с Recv
// допустим: направления независимы.
type Conversation struct {
	s ports.Stream

	mu sync.Mutex
	// term — терминальный отказ разговора: касается и Send, и Recv.
	// Необратим (p2p-messaging.md §4.3, то же правило, что ADR-044 §5).
	term error
	// recvEnd — ЧИСТОЕ завершение приёма (io.EOF). Касается только Recv:
	// собеседник закончил отправку, но наша отправка по-прежнему возможна —
	// это half-close, а не отказ (§6). Смешать его с term значило бы
	// запретить ответ на последнее сообщение.
	recvEnd error
	closed  bool
}

// New оборачивает поток. Ввода-вывода не делает и отказать не может.
func New(s ports.Stream) *Conversation {
	return &Conversation{s: s}
}

// Send отправляет одно сообщение.
//
// Возврат без ошибки означает, что кадр ПРИНЯТ ТРАНСПОРТОМ, и НЕ означает,
// что приложение на той стороне его прочитало (ADR-075 §2.9). Прикладного
// подтверждения в этом контракте нет.
func (c *Conversation) Send(payload []byte) error {
	// Предел проверяется ДО записи и терминальным разговор НЕ делает: провод
	// не тронут ни одним байтом, поток исправен, следующий Send годного
	// размера обязан пройти (p2p-messaging.md §4.1). Это не симметрично
	// нарушению на проводе — и именно поэтому записано отдельно.
	if len(payload) > MaxMessageSize {
		return fmt.Errorf("p2pmsg: send %d bytes, limit %d: %w", len(payload), MaxMessageSize, ErrMessageTooLarge)
	}
	if err := c.terminal(); err != nil {
		return err
	}

	// Заголовок и нагрузка идут ОДНОЙ записью. Раздельная оставила бы поток с
	// заголовком без нагрузки, если вторая запись не удалась, — то есть ровно
	// в том состоянии, которое §4.2 объявляет у получателя неисправимым.
	// Цена — копия нагрузки на отправку; измеряется, а не угадывается.
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)

	if _, err := c.s.Write(frame); err != nil {
		return c.failTerminal(fmt.Errorf("p2pmsg: write frame: %w", err))
	}
	return nil
}

// Recv принимает одно сообщение.
//
// Исходы:
//   - (payload, nil) — сообщение; срез всегда свежий (§7);
//   - (nil, io.EOF) — собеседник ЧИСТО завершил отправку на границе кадра;
//     отправлять по-прежнему можно;
//   - (nil, err) — терминальный отказ разговора, необратимый.
func (c *Conversation) Recv() ([]byte, error) {
	if err := c.recvState(); err != nil {
		return nil, err
	}

	var hdr [4]byte
	if _, err := io.ReadFull(c.s, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			// Ноль байт РОВНО на границе кадра. io.ReadFull отдаёт io.EOF
			// только при нуле прочитанных; 1..3 байта дают
			// io.ErrUnexpectedEOF и попадают в ветку ниже.
			return nil, c.endRecv(io.EOF)
		}
		return nil, c.failTerminal(fmt.Errorf("p2pmsg: read length: %w", err))
	}

	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxMessageSize {
		// Заголовку верить нельзя — значит неизвестно, где начинается
		// следующий кадр. Пропустить этот и читать дальше означало бы выдавать
		// мусор за сообщения, причём молча. Отсюда терминальность (§4.2).
		return nil, c.failTerminal(fmt.Errorf("p2pmsg: declared length %d, limit %d: %w", n, MaxMessageSize, ErrMessageTooLarge))
	}
	if n == 0 {
		// Пустое сообщение валидно и концом потока НЕ является (§3).
		// Возвращается ненулевой пустой срез: тогда msg != nil отличает
		// пустое сообщение от отказа без обращения к ошибке.
		return []byte{}, nil
	}

	payload := make([]byte, n)
	if _, err := io.ReadFull(c.s, payload); err != nil {
		if errors.Is(err, io.EOF) {
			// Обрыв ПОСЛЕ заголовка чистым завершением не является, сколько бы
			// байт ни дошло. io.ReadFull отдаёт io.EOF при нуле прочитанных, и
			// оставить его как есть значило бы выдать обрыв посреди кадра за
			// корректный конец потока — то самое различие, ради которого §6 и
			// написан.
			err = io.ErrUnexpectedEOF
		}
		return nil, c.failTerminal(fmt.Errorf("p2pmsg: read payload of %d bytes: %w", n, err))
	}
	return payload, nil
}

// Close завершает ОТПРАВКУ (half-close, p2p-session.md §7): приём продолжает
// работать до конца данных с той стороны.
//
// Сессию не закрывает — ею владеет вызывающий (ADR-044 §8). Поскольку контракт
// даёт один разговор на сессию, закрыть сессию после терминального отказа —
// обязанность вызывающего, и слой не делает этого за него.
//
// Идемпотентен: повторный вызов — успешный no-op. Без этого привычная пара
// `defer c.Close()` рядом с явным Close давала бы ошибку на ровном месте.
func (c *Conversation) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	return c.s.Close()
}

// terminal — текущий терминальный отказ разговора, если он наступил.
func (c *Conversation) terminal() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.term
}

// recvState — то, что должен вернуть Recv, не читая поток: терминальный отказ
// либо уже наступивший чистый конец приёма.
func (c *Conversation) recvState() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.term != nil {
		return c.term
	}
	return c.recvEnd
}

// failTerminal фиксирует терминальный отказ и возвращает его.
// ПЕРВАЯ ошибка побеждает: исход необратим, и подмена его более поздней
// ошибкой скрыла бы настоящую причину за следствием.
func (c *Conversation) failTerminal(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.term == nil {
		c.term = err
	}
	return c.term
}

// endRecv фиксирует чистый конец приёма.
func (c *Conversation) endRecv(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.recvEnd == nil {
		c.recvEnd = err
	}
	return c.recvEnd
}
