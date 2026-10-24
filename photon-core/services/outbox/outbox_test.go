package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	bboltadapter "github.com/wiloris/photon/photon-core/adapters/bbolt"
	"github.com/wiloris/photon/photon-core/ports"
	"github.com/wiloris/photon/photon-core/services/localfeed"
)

// Инварианты ADR-093 §3 на настоящем bbolt. Часы подставные: аренда
// проверяется сдвигом времени, а не ожиданием.

// migrations — обе namespace хранилища инстанса: очередь и лента в одном
// файле, иначе подтверждение не было бы атомарно с блоком (ADR-093 §2.5).
func migrations() []bboltadapter.Migration {
	return []bboltadapter.Migration{
		{Version: 1, Up: func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(localfeed.Namespace))
			return err
		}},
		{Version: 2, Up: func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(Namespace))
			return err
		}},
	}
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.UnixMilli(1_700_000_000_000)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func openStorage(t *testing.T, path string) ports.Storage {
	t.Helper()
	st, err := bboltadapter.New(path, migrations())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.Migrate(context.Background(), 2); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return st
}

type rig struct {
	path string
	st   ports.Storage
	clk  *clock
	q    *Store
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{path: filepath.Join(t.TempDir(), "core.db"), clk: newClock()}
	r.st = openStorage(t, r.path)
	r.q = New(r.st, WithClock(r.clk.now))
	t.Cleanup(func() { r.st.Close(context.Background()) })
	return r
}

// reopen закрывает файл и открывает заново — перезапуск процесса без
// потери часов теста.
func (r *rig) reopen(t *testing.T) {
	t.Helper()
	if err := r.st.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	r.st = openStorage(t, r.path)
	r.q = New(r.st, WithClock(r.clk.now))
}

var ctx = context.Background()

func body(s string) json.RawMessage { return json.RawMessage(fmt.Sprintf(`{"t":%q}`, s)) }

func mustEnqueue(t *testing.T, q *Store, queue, text string) Op {
	t.Helper()
	op, err := q.Enqueue(ctx, queue, body(text))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return op
}

func mustLease(t *testing.T, q *Store, queue string) Op {
	t.Helper()
	op, err := q.Lease(ctx, queue, time.Minute)
	if err != nil {
		t.Fatalf("Lease(%s): %v", queue, err)
	}
	return op
}

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// ── постановка и аренда ────────────────────────────────────────────────

func TestEnqueueIsPending(t *testing.T) {
	r := newRig(t)
	a := mustEnqueue(t, r.q, "q", "a")
	b := mustEnqueue(t, r.q, "q", "b")

	if !uuidV4.MatchString(a.OpID) || a.OpID == b.OpID {
		t.Fatalf("op_id не UUID v4 или повторился: %q %q", a.OpID, b.OpID)
	}
	if a.State != Pending || a.Attempts != 0 || a.Seq != 1 || b.Seq != 2 {
		t.Fatalf("после постановки: %+v %+v", a, b)
	}
	got, err := r.q.Get(ctx, a.OpID)
	if err != nil || got.State != Pending || string(got.Body) != `{"t":"a"}` {
		t.Fatalf("Get: %+v %v", got, err)
	}
}

func TestEnqueueRejectsBadInput(t *testing.T) {
	r := newRig(t)
	if _, err := r.q.Enqueue(ctx, "", body("x")); !errors.Is(err, ErrInvalidQueue) {
		t.Fatalf("пустая очередь: %v", err)
	}
	if _, err := r.q.Enqueue(ctx, "a\x00b", body("x")); !errors.Is(err, ErrInvalidQueue) {
		t.Fatalf("очередь с 0x00: %v", err)
	}
	if _, err := r.q.Enqueue(ctx, "q", json.RawMessage(`{не json`)); !errors.Is(err, ErrInvalidBody) {
		t.Fatalf("тело не JSON: %v", err)
	}
	if _, err := r.q.Lease(ctx, "q", 0); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("аренда без срока: %v", err)
	}
}

func TestLeaseMarksSending(t *testing.T) {
	r := newRig(t)
	a := mustEnqueue(t, r.q, "q", "a")
	l := mustLease(t, r.q, "q")
	if l.OpID != a.OpID || l.State != Sending || l.Attempts != 1 {
		t.Fatalf("аренда: %+v", l)
	}
	if want := r.clk.now().Add(time.Minute).UnixMilli(); l.LeaseUntil != want {
		t.Fatalf("lease_until = %d, ожидалось %d", l.LeaseUntil, want)
	}
	got, _ := r.q.Get(ctx, a.OpID)
	if got.State != Sending {
		t.Fatalf("после аренды хранится %s", got.State)
	}
}

func TestLeaseExpirationReturnsToPending(t *testing.T) {
	r := newRig(t)
	a := mustEnqueue(t, r.q, "q", "a")
	mustLease(t, r.q, "q")

	r.clk.advance(time.Minute - time.Millisecond)
	if got, _ := r.q.Get(ctx, a.OpID); got.State != Sending {
		t.Fatalf("до истечения: %s", got.State)
	}
	r.clk.advance(time.Millisecond)
	if got, _ := r.q.Get(ctx, a.OpID); got.State != Pending {
		t.Fatalf("после истечения действующее состояние %s, ожидался pending", got.State)
	}
	again := mustLease(t, r.q, "q")
	if again.OpID != a.OpID || again.Attempts != 2 {
		t.Fatalf("восстановленная аренда: %+v — op_id обязан сохраниться, attempts вырасти", again)
	}
}

// ── порядок ────────────────────────────────────────────────────────────

func TestFIFO(t *testing.T) {
	r := newRig(t)
	ops := []Op{mustEnqueue(t, r.q, "q", "1"), mustEnqueue(t, r.q, "q", "2"), mustEnqueue(t, r.q, "q", "3")}

	for i, want := range ops {
		got := mustLease(t, r.q, "q")
		if got.OpID != want.OpID {
			t.Fatalf("шаг %d: выдана %s, ожидалась %s", i, got.OpID, want.OpID)
		}
		// Пока голова в аренде, следующую выдать нельзя.
		busy, err := r.q.Lease(ctx, "q", time.Minute)
		if !errors.Is(err, ErrHeadLeased) || busy.OpID != want.OpID {
			t.Fatalf("шаг %d: вторая аренда при занятой голове: %+v %v", i, busy, err)
		}
		if _, _, err := r.q.Complete(ctx, got.OpID); err != nil {
			t.Fatalf("Complete: %v", err)
		}
	}
	if _, err := r.q.Lease(ctx, "q", time.Minute); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("после всех: %v", err)
	}
}

func TestAtMostOneSendingPerQueue(t *testing.T) {
	r := newRig(t)
	for i := 0; i < 5; i++ {
		mustEnqueue(t, r.q, "q", fmt.Sprint(i))
	}
	mustLease(t, r.q, "q")
	for i := 0; i < 3; i++ {
		_, _ = r.q.Lease(ctx, "q", time.Minute)
	}
	ops, _ := r.q.List(ctx, "q")
	n := 0
	for _, op := range ops {
		if op.State == Sending {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("в sending %d операций, допустима одна", n)
	}
}

func TestFailedDoesNotBlockQueue(t *testing.T) {
	r := newRig(t)
	a := mustEnqueue(t, r.q, "q", "a")
	b := mustEnqueue(t, r.q, "q", "b")
	mustLease(t, r.q, "q")
	if _, _, err := r.q.FailPermanent(ctx, a.OpID, "message_too_long"); err != nil {
		t.Fatalf("FailPermanent: %v", err)
	}
	next := mustLease(t, r.q, "q")
	if next.OpID != b.OpID {
		t.Fatalf("после окончательного отказа выдана %s, ожидалась следующая %s", next.OpID, b.OpID)
	}
	got, _ := r.q.Get(ctx, a.OpID)
	if got.State != Failed || got.LastError != "message_too_long" {
		t.Fatalf("отказанная: %+v", got)
	}
}

func TestTransientFailureBlocksHead(t *testing.T) {
	r := newRig(t)
	a := mustEnqueue(t, r.q, "q", "a")
	mustEnqueue(t, r.q, "q", "b")
	mustLease(t, r.q, "q")
	retryAt := r.clk.now().Add(30 * time.Second)
	got, _, err := r.q.FailTransient(ctx, a.OpID, "network", retryAt)
	if err != nil || got.State != Pending || got.RetryAt != retryAt.UnixMilli() || got.LastError != "network" {
		t.Fatalf("FailTransient: %+v %v", got, err)
	}
	again := mustLease(t, r.q, "q")
	if again.OpID != a.OpID || again.Attempts != 2 {
		t.Fatalf("после временного отказа выдана %+v — голова обязана остаться той же", again)
	}
}

// Повтор окончательно отказанной — на прежнем месте, с прежним op_id.
func TestRetryKeepsIdentityAndPosition(t *testing.T) {
	r := newRig(t)
	a := mustEnqueue(t, r.q, "q", "a")
	b := mustEnqueue(t, r.q, "q", "b")
	mustLease(t, r.q, "q")
	r.q.FailPermanent(ctx, a.OpID, "x")
	if _, _, err := r.q.Retry(ctx, a.OpID); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	head := mustLease(t, r.q, "q")
	if head.OpID != a.OpID || head.Seq != a.Seq {
		t.Fatalf("после повтора голова %+v, ожидалась %s (b = %s)", head, a.OpID, b.OpID)
	}
	if _, _, err := r.q.Retry(ctx, a.OpID); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("Retry операции в аренде: %v", err)
	}
}

// ── исходы ─────────────────────────────────────────────────────────────

func TestDuplicateCompletion(t *testing.T) {
	r := newRig(t)
	a := mustEnqueue(t, r.q, "q", "a")
	mustLease(t, r.q, "q")
	first, changed, err := r.q.Complete(ctx, a.OpID)
	if err != nil || !changed || first.State != Sent {
		t.Fatalf("первое подтверждение: %+v %v %v", first, changed, err)
	}
	second, changed, err := r.q.Complete(ctx, a.OpID)
	if err != nil || changed || second.State != Sent || second.UpdatedAt != first.UpdatedAt {
		t.Fatalf("повторное подтверждение обязано быть no-op: %+v changed=%v err=%v", second, changed, err)
	}
	// Отменить доставленную нельзя — это ошибка, вызывающий обязан узнать.
	if err := r.q.Cancel(ctx, a.OpID); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("Cancel после sent: %v", err)
	}
}

// sent поглощает поздние исходы (docs/local-actions.md): ответ на отправку
// потерян, синхронизация подтвердила тот же op_id, а вызывающий следом
// сообщает отказ или повтор. Ни ошибки, ни изменения.
func TestSentAbsorbsLateOutcomes(t *testing.T) {
	r := newRig(t)
	a := mustEnqueue(t, r.q, "q", "a")
	b := mustEnqueue(t, r.q, "q", "b")
	mustLease(t, r.q, "q")
	// Синхронизация подтвердила a, пока ответ на отправку был в пути.
	if res, err := r.q.Confirm(ctx, []string{a.OpID}); err != nil || res.Confirmed != 1 {
		t.Fatalf("Confirm: %+v %v", res, err)
	}
	sent, _ := r.q.Get(ctx, a.OpID)

	late := []struct {
		name string
		call func() (Op, bool, error)
	}{
		{"FailTransient", func() (Op, bool, error) {
			return r.q.FailTransient(ctx, a.OpID, "timeout", r.clk.now().Add(time.Second))
		}},
		{"FailPermanent", func() (Op, bool, error) { return r.q.FailPermanent(ctx, a.OpID, "forbidden") }},
		{"Retry", func() (Op, bool, error) { return r.q.Retry(ctx, a.OpID) }},
		{"Complete", func() (Op, bool, error) { return r.q.Complete(ctx, a.OpID) }},
	}
	for _, c := range late {
		op, changed, err := c.call()
		if err != nil || changed || op.State != Sent {
			t.Fatalf("%s на sent: state=%s changed=%v err=%v — ожидался no-op без ошибки", c.name, op.State, changed, err)
		}
	}
	after, _ := r.q.Get(ctx, a.OpID)
	if after.State != Sent || after.UpdatedAt != sent.UpdatedAt || after.LastError != sent.LastError ||
		after.Attempts != sent.Attempts || after.RetryAt != sent.RetryAt {
		t.Fatalf("поздние исходы изменили операцию: %+v → %+v", sent, after)
	}
	// Голова ушла дальше: следующая операция выдаётся.
	if got := mustLease(t, r.q, "q"); got.OpID != b.OpID {
		t.Fatalf("после sent выдана %s, ожидалась %s", got.OpID, b.OpID)
	}
}

func TestCompletionOfUnknownOpID(t *testing.T) {
	r := newRig(t)
	mustEnqueue(t, r.q, "q", "a")
	for _, id := range []string{"00000000-0000-4000-8000-000000000000", ""} {
		if _, _, err := r.q.Complete(ctx, id); !errors.Is(err, ErrUnknownOp) {
			t.Fatalf("Complete(%q): %v", id, err)
		}
	}
	// Чужое подтверждение не трогает существующие операции.
	ops, _ := r.q.List(ctx, "q")
	if ops[0].State != Pending {
		t.Fatalf("после чужого подтверждения: %s", ops[0].State)
	}
}

// Подтверждение пришло после того, как аренда истекла (ответ опоздал), —
// принимается: сервер подтвердил именно этот op_id.
func TestLateCompletionAfterLeaseExpiry(t *testing.T) {
	r := newRig(t)
	a := mustEnqueue(t, r.q, "q", "a")
	mustLease(t, r.q, "q")
	r.clk.advance(2 * time.Minute)
	if got, _, err := r.q.Complete(ctx, a.OpID); err != nil || got.State != Sent {
		t.Fatalf("позднее подтверждение: %+v %v", got, err)
	}
}

func TestCancelOnlyWhatServerCannotHave(t *testing.T) {
	r := newRig(t)
	never := mustEnqueue(t, r.q, "q", "never")
	if err := r.q.Cancel(ctx, never.OpID); err != nil {
		t.Fatalf("отмена ни разу не выданной: %v", err)
	}
	if _, err := r.q.Get(ctx, never.OpID); !errors.Is(err, ErrUnknownOp) {
		t.Fatalf("после отмены: %v", err)
	}

	tried := mustEnqueue(t, r.q, "q", "tried")
	mustLease(t, r.q, "q")
	if err := r.q.Cancel(ctx, tried.OpID); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("отмена в аренде: %v", err)
	}
	r.clk.advance(2 * time.Minute) // аренда истекла, но попытка была
	if err := r.q.Cancel(ctx, tried.OpID); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("отмена выданной хотя бы раз: %v — сервер мог её получить", err)
	}
	r.q.FailPermanent(ctx, tried.OpID, "x")
	if err := r.q.Cancel(ctx, tried.OpID); err != nil {
		t.Fatalf("отмена отказанной: %v", err)
	}
}

// ── перезапуск ─────────────────────────────────────────────────────────

func TestRestartAfterPending(t *testing.T) {
	r := newRig(t)
	a := mustEnqueue(t, r.q, "q", "a")
	b := mustEnqueue(t, r.q, "q", "b")
	r.reopen(t)

	ops, err := r.q.List(ctx, "q")
	if err != nil || len(ops) != 2 || ops[0].OpID != a.OpID || ops[1].OpID != b.OpID || ops[0].State != Pending {
		t.Fatalf("после перезапуска: %+v %v", ops, err)
	}
	if got := mustLease(t, r.q, "q"); got.OpID != a.OpID {
		t.Fatalf("после перезапуска выдана %s, ожидалась голова %s", got.OpID, a.OpID)
	}
	c := mustEnqueue(t, r.q, "q", "c")
	if c.Seq != 3 {
		t.Fatalf("seq после перезапуска = %d, ожидалось 3", c.Seq)
	}
}

func TestRestartAfterSending(t *testing.T) {
	r := newRig(t)
	a := mustEnqueue(t, r.q, "q", "a")
	mustLease(t, r.q, "q")
	r.reopen(t)

	if _, err := r.q.Lease(ctx, "q", time.Minute); !errors.Is(err, ErrHeadLeased) {
		t.Fatalf("действующая аренда после перезапуска: %v", err)
	}
	r.clk.advance(time.Minute)
	again := mustLease(t, r.q, "q")
	if again.OpID != a.OpID || again.Attempts != 2 {
		t.Fatalf("восстановление после перезапуска: %+v", again)
	}
}

// Настоящий крах: дочерний процесс ставит и арендует операции и убивает
// себя SIGKILL, не закрывая файл. Родитель открывает файл и видит каждую
// операцию в одном из состояний §2.2 с неизменным op_id — ни потерь, ни
// дублей.
func TestCrashDuringSendingRecovers(t *testing.T) {
	if os.Getenv("OUTBOX_CRASH_CHILD") != "" {
		return // в дочернем процессе — только crashChild
	}
	path := filepath.Join(t.TempDir(), "core.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.v")
	cmd.Env = append(os.Environ(), "OUTBOX_CRASH_CHILD="+path)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("дочерний процесс обязан погибнуть от SIGKILL: err=%v\n%s", err, out)
	}
	ids := regexp.MustCompile(`CRASH-IDS (\S+) (\S+)`).FindStringSubmatch(string(out))
	if ids == nil {
		t.Fatalf("дочерний процесс не сообщил op_id:\n%s", out)
	}

	st := openStorage(t, path)
	defer st.Close(ctx)
	clk := newClock()
	q := New(st, WithClock(clk.now))

	ops, err := q.List(ctx, "q")
	if err != nil || len(ops) != 2 {
		t.Fatalf("после краха: %+v %v", ops, err)
	}
	if ops[0].OpID != ids[1] || ops[1].OpID != ids[2] {
		t.Fatalf("op_id изменились после краха: %s %s против %s %s", ops[0].OpID, ops[1].OpID, ids[1], ids[2])
	}
	if ops[0].State != Sending || ops[0].Attempts != 1 || ops[1].State != Pending {
		t.Fatalf("состояния после краха: %+v", ops)
	}
	clk.advance(time.Minute)
	again, err := q.Lease(ctx, "q", time.Minute)
	if err != nil || again.OpID != ids[1] || again.Attempts != 2 {
		t.Fatalf("восстановление после краха: %+v %v", again, err)
	}
}

// TestCrashChild — тело дочернего процесса. Вне него ничего не делает.
func TestCrashChild(t *testing.T) {
	path := os.Getenv("OUTBOX_CRASH_CHILD")
	if path == "" {
		t.Skip("только как дочерний процесс TestCrashDuringSendingRecovers")
	}
	st, err := bboltadapter.New(path, migrations())
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(3)
	}
	if err := st.Migrate(ctx, 2); err != nil {
		fmt.Println("migrate:", err)
		os.Exit(3)
	}
	q := New(st, WithClock(newClock().now))
	a, err1 := q.Enqueue(ctx, "q", body("a"))
	b, err2 := q.Enqueue(ctx, "q", body("b"))
	_, err3 := q.Lease(ctx, "q", time.Minute)
	if err := errors.Join(err1, err2, err3); err != nil {
		fmt.Println("scenario:", err)
		os.Exit(3)
	}
	fmt.Printf("CRASH-IDS %s %s\n", a.OpID, b.OpID)
	os.Stdout.Sync()
	syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}

// ── подтверждение синхронизацией: одна транзакция с лентой ─────────────

func TestIngestAndConfirmInOneTransaction(t *testing.T) {
	r := newRig(t)
	feed := localfeed.New(r.st, nil)
	a := mustEnqueue(t, r.q, "conv", "a")
	b := mustEnqueue(t, r.q, "conv", "b")
	mustLease(t, r.q, "conv")

	blk := localfeed.Block{Dir: localfeed.DirNewest, AtEnd: true, Records: []localfeed.Record{
		{ID: "srv1", OrderKey: 100, Version: 100, Body: json.RawMessage(`{"op_id":"x"}`)},
	}}

	// Отказ внутри транзакции откатывает и блок, и подтверждение.
	boom := errors.New("boom")
	_, err := feed.IngestWith(ctx, "conv", blk, localfeed.IngestOptions{
		Cursor: []byte("c1"),
		Also: func(tx ports.WriteTx) error {
			if _, err := r.q.ConfirmTx(tx, []string{a.OpID}); err != nil {
				return err
			}
			return boom
		},
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if got, _ := r.q.Get(ctx, a.OpID); got.State != Sending {
		t.Fatalf("подтверждение пережило откат: %s", got.State)
	}
	if n, _ := feed.Count(ctx, "conv"); n != 0 {
		t.Fatalf("блок пережил откат: записей %d", n)
	}
	if _, found, _ := feed.Cursor(ctx, "conv"); found {
		t.Fatal("курсор пережил откат")
	}

	// Успех: запись, курсор и подтверждение — вместе.
	var res ConfirmResult
	_, err = feed.IngestWith(ctx, "conv", blk, localfeed.IngestOptions{
		Cursor: []byte("c1"),
		Also: func(tx ports.WriteTx) error {
			var err error
			res, err = r.q.ConfirmTx(tx, []string{a.OpID, "unknown-op", b.OpID})
			return err
		},
	})
	if err != nil {
		t.Fatalf("IngestWith: %v", err)
	}
	if res != (ConfirmResult{Confirmed: 2, Unknown: 1}) {
		t.Fatalf("итог подтверждения: %+v", res)
	}
	for _, id := range []string{a.OpID, b.OpID} {
		if got, _ := r.q.Get(ctx, id); got.State != Sent {
			t.Fatalf("%s не подтверждена: %s", id, got.State)
		}
	}
	if n, _ := feed.Count(ctx, "conv"); n != 1 {
		t.Fatalf("записей %d", n)
	}
	if cur, _, _ := feed.Cursor(ctx, "conv"); string(cur) != "c1" {
		t.Fatalf("курсор %q", cur)
	}

	// Повтор того же блока с тем же подтверждением безвреден.
	res2, err := r.q.Confirm(ctx, []string{a.OpID})
	if err != nil || res2 != (ConfirmResult{AlreadySent: 1}) {
		t.Fatalf("повторное подтверждение: %+v %v", res2, err)
	}
}

// ── изоляция очередей ──────────────────────────────────────────────────

func TestQueuesDoNotMixState(t *testing.T) {
	r := newRig(t)
	a1 := mustEnqueue(t, r.q, "alice", "1")
	b1 := mustEnqueue(t, r.q, "bob", "1")
	a2 := mustEnqueue(t, r.q, "alice", "2")
	// Имя-префикс: «al» не должна видеть операций «alice».
	mustEnqueue(t, r.q, "al", "x")

	if a1.Seq != 1 || a2.Seq != 2 || b1.Seq != 1 {
		t.Fatalf("seq общие между очередями: %d %d %d", a1.Seq, a2.Seq, b1.Seq)
	}
	// Занятая голова одной очереди не блокирует другую.
	mustLease(t, r.q, "alice")
	if got := mustLease(t, r.q, "bob"); got.OpID != b1.OpID {
		t.Fatalf("bob: выдана %s", got.OpID)
	}
	if _, err := r.q.Lease(ctx, "alice", time.Minute); !errors.Is(err, ErrHeadLeased) {
		t.Fatalf("alice: %v", err)
	}
	for q, want := range map[string]int{"alice": 2, "bob": 1, "al": 1} {
		ops, _ := r.q.List(ctx, q)
		if len(ops) != want {
			t.Fatalf("очередь %s: %d операций, ожидалось %d", q, len(ops), want)
		}
		for _, op := range ops {
			if op.Queue != q {
				t.Fatalf("в очереди %s операция очереди %s", q, op.Queue)
			}
		}
	}
}

// ── уборка ─────────────────────────────────────────────────────────────

func TestPurgeSentKeepsFailedAndPending(t *testing.T) {
	r := newRig(t)
	sent := mustEnqueue(t, r.q, "q", "sent")
	failed := mustEnqueue(t, r.q, "q", "failed")
	pending := mustEnqueue(t, r.q, "q", "pending")
	mustLease(t, r.q, "q")
	r.q.Complete(ctx, sent.OpID)
	mustLease(t, r.q, "q")
	r.q.FailPermanent(ctx, failed.OpID, "x")

	r.clk.advance(time.Hour)
	n, err := r.q.PurgeSent(ctx, "q", r.clk.now())
	if err != nil || n != 1 {
		t.Fatalf("PurgeSent: %d %v", n, err)
	}
	if _, err := r.q.Get(ctx, sent.OpID); !errors.Is(err, ErrUnknownOp) {
		t.Fatalf("отправленная осталась: %v", err)
	}
	for _, id := range []string{failed.OpID, pending.OpID} {
		if _, err := r.q.Get(ctx, id); err != nil {
			t.Fatalf("уборка задела %s: %v", id, err)
		}
	}
}

// ── граница домена ─────────────────────────────────────────────────────

// Core не знает домена (ADR-093 §3.6, ADR-011): ни одного доменного имени
// в КОДЕ пакета — идентификаторах и строковых литералах. Комментарии не
// проверяются: в них граница как раз и описывается словами.
func TestNoDomainNamesInPackage(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "outbox.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	banned := regexp.MustCompile(`(?i)mattermost|\bposts?\b|\bmessages?\b|\bhttps?\b|photon_?id|\btext\b|\bchannels?\b|\bconversations?\b|\bpeer\b`)
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.Ident:
			if banned.MatchString(x.Name) {
				t.Errorf("%s: доменный идентификатор %q", fset.Position(x.Pos()), x.Name)
			}
		case *ast.BasicLit:
			if x.Kind == token.STRING && banned.MatchString(x.Value) {
				t.Errorf("%s: доменная строка %s", fset.Position(x.Pos()), x.Value)
			}
		}
		return true
	})
}
