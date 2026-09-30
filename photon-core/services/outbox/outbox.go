// Package outbox — долговечная очередь исходящих операций (ADR-093).
//
// Операция — непрозрачная для Core единица «сделать на сервере ровно один
// раз»: тело — JSON вызывающего, очередь — его же непрозрачная строка.
// Core не знает ни сообщений, ни Mattermost, ни HTTP (ADR-046 вариант C,
// ADR-011): сеть, разбор ответа сервера и выбор момента повтора — у
// вызывающего (ADR-093 §2.4, ADR-066 §9).
//
// ЭТО НЕ ПОРТ. Внутренняя механика в services/ по правилу ADR-046 §6.1:
// ports.Storage не меняется, используется ForEachPrefixFrom (ADR-047) как
// упорядоченный обход.
//
// Каждый переход состояния — одна транзакция ports.Storage.Update: убитый
// процесс оставляет операцию в одном из состояний, никогда «между»
// (ADR-093 §3.1).
package outbox

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/wiloris/photon-core/ports"
)

// Namespace очереди в хранилище инстанса (ADR-093 §2.8). Создаётся
// миграцией композиционного корня: порт namespace неявно не заводит (ADR-010).
const Namespace = "photon.outbox"

// State — состояние операции (ADR-093 §2.2).
type State string

const (
	// Pending — ждёт отправки.
	Pending State = "pending"
	// Sending — выдана вызывающему под аренду.
	Sending State = "sending"
	// Sent — сервер подтвердил ЭТОТ op_id.
	Sent State = "sent"
	// Failed — окончательный отказ, ждёт действия пользователя.
	Failed State = "failed"
)

var (
	// ErrInvalidQueue — пустая очередь или очередь с байтом 0x00: он —
	// разделитель ключа, и очередь с ним пересеклась бы с чужой.
	ErrInvalidQueue = errors.New("outbox: queue must be non-empty and must not contain 0x00")
	// ErrInvalidBody — тело не JSON. Тело непрозрачно, но хранится как JSON
	// (ADR-034): иначе его нельзя вернуть в конверте ответа сырыми байтами.
	ErrInvalidBody = errors.New("outbox: body must be valid JSON")
	// ErrInvalidLease — аренда без срока.
	ErrInvalidLease = errors.New("outbox: lease duration must be positive")
	// ErrUnknownOp — op_id не существует.
	ErrUnknownOp = errors.New("outbox: unknown op_id")
	// ErrQueueEmpty — в очереди нет незавершённой операции.
	ErrQueueEmpty = errors.New("outbox: no pending operation in queue")
	// ErrHeadLeased — голова очереди уже в аренде и срок не истёк: следующую
	// выдавать нельзя, иначе нарушится FIFO (ADR-093 §2.3, инвариант §3.3).
	ErrHeadLeased = errors.New("outbox: head of queue is leased")
	// ErrStateConflict — переход из текущего состояния запрещён.
	ErrStateConflict = errors.New("outbox: transition not allowed from current state")
)

// Op — операция в том виде, в каком её видит вызывающий.
//
// State — ДЕЙСТВУЮЩЕЕ состояние: операция в Sending с истёкшей арендой
// показывается как Pending — так переживается убитый процесс, без фонового
// потока (ADR-093 §2.2). На диске она перепишется при следующем переходе.
type Op struct {
	OpID       string          `json:"op_id"`
	Queue      string          `json:"queue"`
	Seq        uint64          `json:"seq"`
	Body       json.RawMessage `json:"body"`
	State      State           `json:"state"`
	Attempts   int             `json:"attempts"`
	LeaseUntil int64           `json:"lease_until,omitempty"` // Unix ms
	RetryAt    int64           `json:"retry_at,omitempty"`    // Unix ms, сообщает вызывающий
	LastError  string          `json:"last_error,omitempty"`
	CreatedAt  int64           `json:"created_at"` // Unix ms, локальное время; не для порядка
	UpdatedAt  int64           `json:"updated_at"`
}

// Store — очередь поверх ports.Storage.
type Store struct {
	st  ports.Storage
	now func() time.Time
}

// Option настраивает Store.
type Option func(*Store)

// WithClock подставляет часы: тестам аренды незачем ждать настоящее время.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// New — очередь поверх хранилища, в котором namespace уже создан миграцией.
func New(st ports.Storage, opts ...Option) *Store {
	s := &Store{st: st, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	return s
}

// ── ключи ──────────────────────────────────────────────────────────────
//
// Три пространства внутри namespace, различимые первым байтом:
//
//	'q' 0x00 <queue> 0x00 <seq BE>  → операция (порядок обхода = FIFO)
//	'i' 0x00 <op_id>                → ключ операции (поиск по op_id)
//	'n' 0x00 <queue>                → последний выданный seq очереди
//
// Big-endian seq: байтовый порядок совпадает с числовым.

func queuePrefix(queue string) []byte {
	b := make([]byte, 0, len(queue)+3)
	b = append(b, 'q', 0x00)
	b = append(b, queue...)
	return append(b, 0x00)
}

func opKey(queue string, seq uint64) []byte {
	b := queuePrefix(queue)
	var s [8]byte
	binary.BigEndian.PutUint64(s[:], seq)
	return append(b, s[:]...)
}

func idxKey(opID string) []byte { return append([]byte{'i', 0x00}, opID...) }

func seqKey(queue string) []byte { return append([]byte{'n', 0x00}, queue...) }

func validQueue(q string) error {
	if q == "" || strings.IndexByte(q, 0x00) >= 0 {
		return ErrInvalidQueue
	}
	return nil
}

// newOpID — UUID v4 из crypto/rand (ADR-093 §2.1: ключ создаёт Core).
func newOpID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("outbox: op_id: %w", err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func ms(t time.Time) int64 { return t.UnixMilli() }

// effective — действующее состояние на момент now.
func effective(op Op, now int64) Op {
	if op.State == Sending && now >= op.LeaseUntil {
		op.State = Pending
		op.LeaseUntil = 0
	}
	return op
}

func decodeOp(v []byte) (Op, error) {
	var op Op
	if err := json.Unmarshal(v, &op); err != nil {
		return Op{}, fmt.Errorf("outbox: decode op: %w", err)
	}
	return op, nil
}

func putOp(tx ports.WriteTx, op Op) error {
	v, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("outbox: encode op: %w", err)
	}
	return tx.Put(Namespace, opKey(op.Queue, op.Seq), v)
}

// loadByID — операция и её ключ по op_id.
func loadByID(tx ports.ReadTx, opID string) (Op, error) {
	k, err := tx.Get(Namespace, idxKey(opID))
	if errors.Is(err, ports.ErrNotFound) {
		return Op{}, fmt.Errorf("%w: %s", ErrUnknownOp, opID)
	}
	if err != nil {
		return Op{}, err
	}
	v, err := tx.Get(Namespace, k)
	if err != nil {
		// Индекс есть, операции нет — нарушение собственного инварианта, не
		// «неизвестный op_id»: молча отвечать ErrUnknownOp значило бы
		// спрятать повреждение.
		return Op{}, fmt.Errorf("outbox: index points to missing op %s: %w", opID, err)
	}
	return decodeOp(v)
}

// ── постановка ─────────────────────────────────────────────────────────

// Enqueue ставит операцию в хвост очереди. op_id создаётся здесь и
// записывается той же транзакцией, что сама операция (ADR-093 §2.1).
func (s *Store) Enqueue(ctx context.Context, queue string, body json.RawMessage) (Op, error) {
	if err := validQueue(queue); err != nil {
		return Op{}, err
	}
	if !json.Valid(body) {
		return Op{}, ErrInvalidBody
	}
	var out Op
	err := s.st.Update(ctx, func(tx ports.WriteTx) error {
		var last uint64
		v, err := tx.Get(Namespace, seqKey(queue))
		switch {
		case err == nil:
			last = binary.BigEndian.Uint64(v)
		case !errors.Is(err, ports.ErrNotFound):
			return err
		}
		opID, err := s.uniqueOpID(tx)
		if err != nil {
			return err
		}
		now := ms(s.now())
		op := Op{
			OpID: opID, Queue: queue, Seq: last + 1,
			Body: append(json.RawMessage(nil), body...), State: Pending,
			CreatedAt: now, UpdatedAt: now,
		}
		var seq [8]byte
		binary.BigEndian.PutUint64(seq[:], op.Seq)
		if err := tx.Put(Namespace, seqKey(queue), seq[:]); err != nil {
			return err
		}
		if err := tx.Put(Namespace, idxKey(opID), opKey(queue, op.Seq)); err != nil {
			return err
		}
		if err := putOp(tx, op); err != nil {
			return err
		}
		out = op
		return nil
	})
	return out, err
}

// uniqueOpID — 122 случайных бита не столкнутся на практике, но проверка
// стоит одного Get и делает инвариант «op_id уникален» структурным.
func (s *Store) uniqueOpID(tx ports.ReadTx) (string, error) {
	for i := 0; i < 4; i++ {
		id, err := newOpID()
		if err != nil {
			return "", err
		}
		if _, err := tx.Get(Namespace, idxKey(id)); errors.Is(err, ports.ErrNotFound) {
			return id, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", errors.New("outbox: op_id collision four times in a row")
}

// ── аренда ─────────────────────────────────────────────────────────────

// Lease выдаёт голову очереди — первую операцию в Pending или Sending
// (Sent и Failed пропускаются: окончательный отказ очередь не блокирует,
// ADR-093 §2.3). Голова в действующей аренде → ErrHeadLeased с самой
// операцией. Истёкшая аренда восстанавливается здесь же: операция
// выдаётся снова с тем же op_id и увеличенным attempts (§3.2, §3.5).
func (s *Store) Lease(ctx context.Context, queue string, leaseFor time.Duration) (Op, error) {
	if err := validQueue(queue); err != nil {
		return Op{}, err
	}
	if leaseFor <= 0 {
		return Op{}, ErrInvalidLease
	}
	var out Op
	err := s.st.Update(ctx, func(tx ports.WriteTx) error {
		now := ms(s.now())
		head, found, err := s.head(tx, queue, now)
		if err != nil {
			return err
		}
		if !found {
			return ErrQueueEmpty
		}
		if head.State == Sending {
			out = head
			return ErrHeadLeased
		}
		head.State = Sending
		head.Attempts++
		head.LeaseUntil = now + leaseFor.Milliseconds()
		head.RetryAt = 0
		head.UpdatedAt = now
		if err := putOp(tx, head); err != nil {
			return err
		}
		out = head
		return nil
	})
	if errors.Is(err, ErrHeadLeased) {
		return out, err
	}
	if err != nil {
		return Op{}, err
	}
	return out, nil
}

// head — первая незавершённая операция очереди в действующем состоянии.
func (s *Store) head(tx ports.ReadTx, queue string, now int64) (Op, bool, error) {
	var (
		head  Op
		found bool
	)
	stop := errors.New("stop")
	err := tx.ForEachPrefixFrom(Namespace, queuePrefix(queue), nil, func(_, v []byte) error {
		op, err := decodeOp(v)
		if err != nil {
			return err
		}
		op = effective(op, now)
		if op.State == Pending || op.State == Sending {
			head, found = op, true
			return stop
		}
		return nil
	})
	if err != nil && !errors.Is(err, stop) {
		return Op{}, false, err
	}
	return head, found, nil
}

// ── исходы ─────────────────────────────────────────────────────────────

// transition — общий каркас перехода по op_id в одной транзакции.
// apply получает действующее состояние и возвращает (новое, изменилось ли).
func (s *Store) transition(ctx context.Context, opID string, apply func(op Op, now int64) (Op, bool, error)) (Op, bool, error) {
	var (
		out     Op
		changed bool
	)
	err := s.st.Update(ctx, func(tx ports.WriteTx) error {
		now := ms(s.now())
		op, err := loadByID(tx, opID)
		if err != nil {
			return err
		}
		next, ch, err := apply(effective(op, now), now)
		if err != nil {
			out = effective(op, now)
			return err
		}
		if ch {
			next.UpdatedAt = now
			if err := putOp(tx, next); err != nil {
				return err
			}
		}
		out, changed = next, ch
		return nil
	})
	return out, changed, err
}

// Complete — сервер подтвердил ЭТОТ op_id ответом на отправку. Повторное
// подтверждение — не ошибка и ничего не меняет (changed = false).
// Подтверждение после Failed принимается: утверждение сервера об успехе
// сильнее нашей прежней классификации.
func (s *Store) Complete(ctx context.Context, opID string) (Op, bool, error) {
	return s.transition(ctx, opID, func(op Op, _ int64) (Op, bool, error) {
		if op.State == Sent {
			return op, false, nil
		}
		op.State, op.LeaseUntil, op.RetryAt = Sent, 0, 0
		return op, true, nil
	})
}

// FailTransient — временный отказ (сеть, 5xx, «в процессе»): операция
// возвращается в Pending и остаётся головой — очередь ждёт её, иначе
// порядок нарушится при восстановлении связи (ADR-093 §2.3). retryAt —
// когда вызывающий собирается повторить; Core его хранит, но не исполняет.
func (s *Store) FailTransient(ctx context.Context, opID, reason string, retryAt time.Time) (Op, bool, error) {
	return s.transition(ctx, opID, func(op Op, _ int64) (Op, bool, error) {
		if op.State != Pending && op.State != Sending {
			return op, false, fmt.Errorf("%w: %s is %s", ErrStateConflict, opID, op.State)
		}
		op.State, op.LeaseUntil, op.LastError = Pending, 0, reason
		op.RetryAt = 0
		if !retryAt.IsZero() {
			op.RetryAt = ms(retryAt)
		}
		return op, true, nil
	})
}

// FailPermanent — окончательный отказ сервера: Failed. Голова переходит к
// следующей операции. Повторный вызов на Failed ничего не меняет.
func (s *Store) FailPermanent(ctx context.Context, opID, reason string) (Op, bool, error) {
	return s.transition(ctx, opID, func(op Op, _ int64) (Op, bool, error) {
		switch op.State {
		case Failed:
			return op, false, nil
		case Sent:
			return op, false, fmt.Errorf("%w: %s is already sent", ErrStateConflict, opID)
		}
		op.State, op.LeaseUntil, op.RetryAt, op.LastError = Failed, 0, 0, reason
		return op, true, nil
	})
}

// Retry — пользователь повторяет окончательно отказанную операцию: Failed →
// Pending на прежнем месте очереди (seq не меняется), с тем же op_id.
func (s *Store) Retry(ctx context.Context, opID string) (Op, bool, error) {
	return s.transition(ctx, opID, func(op Op, _ int64) (Op, bool, error) {
		switch op.State {
		case Pending:
			return op, false, nil
		case Failed:
			op.State, op.RetryAt = Pending, 0
			return op, true, nil
		}
		return op, false, fmt.Errorf("%w: %s is %s", ErrStateConflict, opID, op.State)
	})
}

// Cancel удаляет операцию, о которой сервер заведомо не знает: Failed либо
// Pending, ни разу не выданную (attempts = 0). Выданная хотя бы раз могла
// дойти до сервера — её отмена спрятала бы уже созданный объект.
func (s *Store) Cancel(ctx context.Context, opID string) error {
	return s.st.Update(ctx, func(tx ports.WriteTx) error {
		op, err := loadByID(tx, opID)
		if err != nil {
			return err
		}
		op = effective(op, ms(s.now()))
		ok := op.State == Failed || (op.State == Pending && op.Attempts == 0)
		if !ok {
			return fmt.Errorf("%w: %s is %s after %d attempts", ErrStateConflict, opID, op.State, op.Attempts)
		}
		if err := tx.Delete(Namespace, opKey(op.Queue, op.Seq)); err != nil {
			return err
		}
		return tx.Delete(Namespace, idxKey(opID))
	})
}

// ── подтверждение синхронизацией ───────────────────────────────────────

// ConfirmResult — итог подтверждения списком op_id.
type ConfirmResult struct {
	Confirmed   int `json:"confirmed"`    // перешли в Sent
	AlreadySent int `json:"already_sent"` // уже были Sent
	Unknown     int `json:"unknown"`      // такого op_id здесь нет (например, операция другого устройства)
}

// ConfirmTx переводит в Sent операции, которые сервер вернул синхронизацией
// (ADR-093 §2.5). Работает ВНУТРИ чужой транзакции: так подтверждение
// ложится атомарно с блоком ленты (localfeed.IngestWith, поле Also).
// Какие op_id подтверждены, сообщает вызывающий — тело Core не разбирает.
func (s *Store) ConfirmTx(tx ports.WriteTx, opIDs []string) (ConfirmResult, error) {
	var res ConfirmResult
	now := ms(s.now())
	for _, id := range opIDs {
		op, err := loadByID(tx, id)
		if errors.Is(err, ErrUnknownOp) {
			res.Unknown++
			continue
		}
		if err != nil {
			return ConfirmResult{}, err
		}
		if op.State == Sent {
			res.AlreadySent++
			continue
		}
		op.State, op.LeaseUntil, op.RetryAt, op.UpdatedAt = Sent, 0, 0, now
		if err := putOp(tx, op); err != nil {
			return ConfirmResult{}, err
		}
		res.Confirmed++
	}
	return res, nil
}

// Confirm — ConfirmTx в собственной транзакции.
func (s *Store) Confirm(ctx context.Context, opIDs []string) (ConfirmResult, error) {
	var res ConfirmResult
	err := s.st.Update(ctx, func(tx ports.WriteTx) error {
		var err error
		res, err = s.ConfirmTx(tx, opIDs)
		return err
	})
	return res, err
}

// ── чтение и уборка ────────────────────────────────────────────────────

// Get — операция по op_id в действующем состоянии.
func (s *Store) Get(ctx context.Context, opID string) (Op, error) {
	var out Op
	err := s.st.View(ctx, func(tx ports.ReadTx) error {
		op, err := loadByID(tx, opID)
		if err != nil {
			return err
		}
		out = effective(op, ms(s.now()))
		return nil
	})
	return out, err
}

// List — все операции очереди в порядке постановки, в действующем состоянии.
func (s *Store) List(ctx context.Context, queue string) ([]Op, error) {
	if err := validQueue(queue); err != nil {
		return nil, err
	}
	var out []Op
	err := s.st.View(ctx, func(tx ports.ReadTx) error {
		now := ms(s.now())
		return tx.ForEachPrefixFrom(Namespace, queuePrefix(queue), nil, func(_, v []byte) error {
			op, err := decodeOp(v)
			if err != nil {
				return err
			}
			out = append(out, effective(op, now))
			return nil
		})
	})
	return out, err
}

// PurgeSent удаляет Sent-операции очереди, подтверждённые раньше before
// (ADR-093 §2.6). Failed не удаляется никогда.
func (s *Store) PurgeSent(ctx context.Context, queue string, before time.Time) (int, error) {
	if err := validQueue(queue); err != nil {
		return 0, err
	}
	n := 0
	err := s.st.Update(ctx, func(tx ports.WriteTx) error {
		var doomed []Op
		err := tx.ForEachPrefixFrom(Namespace, queuePrefix(queue), nil, func(_, v []byte) error {
			op, err := decodeOp(v)
			if err != nil {
				return err
			}
			if op.State == Sent && op.UpdatedAt < ms(before) {
				doomed = append(doomed, op)
			}
			return nil
		})
		if err != nil {
			return err
		}
		// Удаление после обхода: менять bucket под курсором обхода нельзя.
		for _, op := range doomed {
			if err := tx.Delete(Namespace, opKey(op.Queue, op.Seq)); err != nil {
				return err
			}
			if err := tx.Delete(Namespace, idxKey(op.OpID)); err != nil {
				return err
			}
		}
		n = len(doomed)
		return nil
	})
	return n, err
}
