package bbolt

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/wiloris/photon-core/ports"
)

/*
 * Аварийные испытания bbolt — ADR-130 §5.3.
 *
 * Дочерний процесс (этот же тестовый бинарник, режим по BBOLT_CRASH_MODE)
 * пишет ПРОНУМЕРОВАННЫЕ транзакции: в каждой — crashKeys ключей с номером и
 * ключ контрольной суммы их содержимого. После каждого успешного возврата
 * Update он сообщает номер родителю («OK n») ДО начала следующей; в режиме
 * commit — ещё и «FN n» в самом конце fn, то есть перед фиксацией. Родитель
 * убивает его SIGKILL в случайный момент (seed записывается) либо сразу по
 * «FN n» — строго во время фиксации.
 *
 * После каждого убийства файл открывается заново и сверяется с
 * ПОДТВЕРЖДЁННЫМИ транзакциями, а не только с фактом открытия:
 *   - tx.Check() без ошибок;
 *   - каждая подтверждённая транзакция присутствует полностью и с верным
 *     содержимым и контрольной суммой;
 *   - транзакция, шедшая в момент SIGKILL, — целиком либо не присутствует
 *     вовсе;
 *   - данных сверх «подтверждённые + не более одной в полёте» нет.
 */

const (
	crashNS   = "crash"
	crashKeys = 8
)

var crashMigrations = []Migration{{Version: 1, Up: func(tx *bolt.Tx) error {
	_, err := tx.CreateBucketIfNotExists([]byte(crashNS))
	return err
}}}

func crashValue(n uint64, i int) []byte {
	return []byte(fmt.Sprintf("tx=%d key=%d payload=%s", n, i, strings.Repeat("x", 64+int(n%7)*32)))
}

func crashKey(n uint64, i int) []byte {
	k := make([]byte, 12)
	binary.BigEndian.PutUint64(k, n)
	binary.BigEndian.PutUint32(k[8:], uint32(i))
	return k
}

// sumKey — ключ контрольной суммы транзакции n (после ключей данных).
func sumKey(n uint64) []byte { return crashKey(n, 1<<31) }

func crashSum(n uint64) []byte {
	h := sha256.New()
	for i := 0; i < crashKeys; i++ {
		h.Write(crashValue(n, i))
	}
	return []byte(hex.EncodeToString(h.Sum(nil)))
}

// TestCrashBboltChild — тело дочернего процесса. Вне него ничего не делает.
func TestCrashBboltChild(t *testing.T) {
	path, mode := os.Getenv("BBOLT_CRASH_PATH"), os.Getenv("BBOLT_CRASH_MODE")
	if path == "" {
		t.Skip("только как дочерний процесс аварийных испытаний")
	}
	out := bufio.NewWriter(os.Stdout)
	say := func(format string, a ...any) {
		fmt.Fprintf(out, format+"\n", a...)
		out.Flush()
	}
	if mode == "migrate" {
		crashMigrateChild(path, say)
		return
	}
	st, err := New(path, crashMigrations)
	if err != nil {
		say("ERR open %v", err)
		os.Exit(3)
	}
	ctx := context.Background()
	if err := st.Migrate(ctx, 1); err != nil {
		say("ERR migrate %v", err)
		os.Exit(3)
	}
	start, _ := strconv.ParseUint(os.Getenv("BBOLT_CRASH_START"), 10, 64)
	say("READY")
	for n := start; ; n++ {
		err := st.Update(ctx, func(tx ports.WriteTx) error {
			for i := 0; i < crashKeys; i++ {
				if err := tx.Put(crashNS, crashKey(n, i), crashValue(n, i)); err != nil {
					return err
				}
			}
			if err := tx.Put(crashNS, sumKey(n), crashSum(n)); err != nil {
				return err
			}
			if mode == "commit" {
				say("FN %d", n) // fn вернётся сейчас — дальше фиксация
			}
			return nil
		})
		if err != nil {
			say("ERR update %v", err)
			os.Exit(3)
		}
		say("OK %d", n)
	}
}

type crashRun struct {
	confirmed uint64 // последний подтверждённый номер; start-1, если ни одного
	inFlight  bool   // была ли начата транзакция после последнего подтверждения
}

// runCrashChild запускает дочерний процесс и убивает его по правилу kill.
// kill получает очередную строку дочернего процесса и решает, убить ли сейчас.
func runCrashChild(t *testing.T, path, mode string, start uint64, kill func(line string) bool, deadline time.Duration) crashRun {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashBboltChild$")
	cmd.Env = append(os.Environ(), "BBOLT_CRASH_PATH="+path, "BBOLT_CRASH_MODE="+mode,
		"BBOLT_CRASH_START="+strconv.FormatUint(start, 10))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	run := crashRun{confirmed: start - 1}
	killed := false
	timer := time.AfterFunc(deadline, func() { cmd.Process.Signal(syscall.SIGKILL) })
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "ERR") {
			t.Fatalf("дочерний процесс: %s", line)
		}
		if strings.HasPrefix(line, "OK ") {
			n, _ := strconv.ParseUint(strings.TrimPrefix(line, "OK "), 10, 64)
			run.confirmed = n
		}
		if !killed && kill(line) {
			cmd.Process.Signal(syscall.SIGKILL)
			killed = true
		}
	}
	timer.Stop()
	io.Copy(io.Discard, stdout)
	err = cmd.Wait()
	ws, _ := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if err == nil || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("дочерний процесс обязан погибнуть от SIGKILL: %v", err)
	}
	run.inFlight = true // после последнего OK дочерний процесс мог начать следующую
	return run
}

// verifyCrashState — сверка файла с подтверждёнными транзакциями.
// Возвращает, присутствует ли транзакция, шедшая в момент убийства.
func verifyCrashState(t *testing.T, path string, run crashRun, start uint64) (inFlightPresent bool) {
	t.Helper()
	st, err := New(path, crashMigrations)
	if err != nil {
		t.Fatalf("повторное открытие после SIGKILL: %v", err)
	}
	defer st.Close(context.Background())
	err = st.db.View(func(tx *bolt.Tx) error {
		for e := range tx.Check() {
			t.Errorf("tx.Check: %v", e)
		}
		b := tx.Bucket([]byte(crashNS))
		if b == nil {
			if run.confirmed >= start {
				t.Fatal("bucket исчез при подтверждённых транзакциях")
			}
			return nil
		}
		present := func(n uint64) (all, none bool) {
			got := 0
			for i := 0; i < crashKeys; i++ {
				v := b.Get(crashKey(n, i))
				if v != nil {
					got++
					if string(v) != string(crashValue(n, i)) {
						t.Fatalf("tx %d ключ %d: содержимое испорчено", n, i)
					}
				}
			}
			s := b.Get(sumKey(n))
			if s != nil {
				got++
				if string(s) != string(crashSum(n)) {
					t.Fatalf("tx %d: контрольная сумма не сходится", n)
				}
			}
			return got == crashKeys+1, got == 0
		}
		for n := uint64(1); n <= run.confirmed; n++ {
			if all, _ := present(n); !all {
				t.Fatalf("подтверждённая tx %d присутствует не полностью", n)
			}
		}
		next := run.confirmed + 1
		all, none := present(next)
		if !all && !none {
			t.Fatalf("tx %d в полёте присутствует ЧАСТИЧНО", next)
		}
		inFlightPresent = all
		// Сверх «подтверждённые + одна в полёте» — ничего.
		if all2, none2 := present(next + 1); !none2 || all2 {
			t.Fatalf("есть данные tx %d — дальше одной в полёте", next+1)
		}
		total := 0
		b.ForEach(func(k, v []byte) error { total++; return nil })
		want := int(run.confirmed) * (crashKeys + 1)
		if inFlightPresent {
			want += crashKeys + 1
		}
		if total != want {
			t.Fatalf("ключей %d, ожидалось %d", total, want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return inFlightPresent
}

func crashSeed(t *testing.T) int64 {
	seed := time.Now().UnixNano()
	if s := os.Getenv("BBOLT_CRASH_SEED"); s != "" {
		seed, _ = strconv.ParseInt(s, 10, 64)
	}
	t.Logf("seed = %d (воспроизведение: BBOLT_CRASH_SEED=%d)", seed, seed)
	return seed
}

// ≥ 200 убийств в случайные моменты: после случайного числа подтверждений и
// случайной паузы — то есть посреди fn, фиксации или между транзакциями.
// Один файл на всю серию: каждое следующее убийство — поверх восстановленного.
func TestCrashBboltRandomKills(t *testing.T) {
	if os.Getenv("BBOLT_CRASH_PATH") != "" || testing.Short() {
		t.Skip("дочерний процесс или -short")
	}
	rng := rand.New(rand.NewSource(crashSeed(t)))
	path := filepath.Join(t.TempDir(), "crash.db")
	const kills = 200
	start := uint64(1)
	present, absent := 0, 0
	for k := 0; k < kills; k++ {
		afterOK := rng.Intn(6)
		delay := time.Duration(rng.Intn(3000)) * time.Microsecond
		seen := 0
		run := runCrashChild(t, path, "random", start, func(line string) bool {
			if line == "READY" && afterOK == 0 || strings.HasPrefix(line, "OK ") && func() bool { seen++; return seen >= afterOK }() {
				time.Sleep(delay)
				return true
			}
			return false
		}, 30*time.Second)
		if verifyCrashState(t, path, run, 1) {
			present++
			start = run.confirmed + 2
		} else {
			absent++
			start = run.confirmed + 1
		}
		if t.Failed() {
			t.Fatalf("убийство %d из %d", k+1, kills)
		}
	}
	t.Logf("убийств %d; транзакция в полёте после восстановления: целиком %d, отсутствует %d; подтверждено всего %d",
		kills, present, absent, start-1)
}

// Убийство строго во время фиксации: по сигналу «FN n» из конца fn, то есть
// между возвратом fn и возвратом Update.
func TestCrashBboltDuringCommit(t *testing.T) {
	if os.Getenv("BBOLT_CRASH_PATH") != "" || testing.Short() {
		t.Skip("дочерний процесс или -short")
	}
	rng := rand.New(rand.NewSource(crashSeed(t)))
	path := filepath.Join(t.TempDir(), "commit.db")
	const kills = 60
	start := uint64(1)
	present, absent := 0, 0
	for k := 0; k < kills; k++ {
		target := rng.Intn(4) // какую по счёту фиксацию убивать
		fns := 0
		run := runCrashChild(t, path, "commit", start, func(line string) bool {
			if strings.HasPrefix(line, "FN ") {
				fns++
				return fns > target
			}
			return false
		}, 30*time.Second)
		if verifyCrashState(t, path, run, 1) {
			present++
			start = run.confirmed + 2
		} else {
			absent++
			start = run.confirmed + 1
		}
		if t.Failed() {
			t.Fatalf("убийство %d из %d", k+1, kills)
		}
	}
	t.Logf("убийств во время фиксации %d: транзакция целиком %d, отсутствует %d", kills, present, absent)
}

// ── SIGKILL посреди миграции ────────────────────────────────────────────

const (
	migNS   = "mig"
	migKeys = 3000
)

func migValue(i int) []byte {
	return []byte(fmt.Sprintf("migrated-%06d-%s", i, strings.Repeat("m", 48)))
}

// migrations2 — v1 (bucket данных) и v2 (bucket миграции с migKeys ключами).
// progress, если задан, вызывается каждые 500 ключей внутри Up — ДО фиксации.
func migrations2(progress func(i int)) []Migration {
	return []Migration{
		crashMigrations[0],
		{Version: 2, Up: func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(migNS))
			if err != nil {
				return err
			}
			for i := 0; i < migKeys; i++ {
				if err := b.Put([]byte(fmt.Sprintf("k%06d", i)), migValue(i)); err != nil {
					return err
				}
				if progress != nil && i%500 == 0 {
					progress(i)
				}
			}
			return nil
		}},
	}
}

func crashMigrateChild(path string, say func(string, ...any)) {
	ctx := context.Background()
	st, err := New(path, migrations2(func(i int) { say("MIG %d", i) }))
	if err != nil {
		say("ERR open %v", err)
		os.Exit(3)
	}
	if err := st.Migrate(ctx, 1); err != nil {
		say("ERR migrate1 %v", err)
		os.Exit(3)
	}
	if err := st.Update(ctx, func(tx ports.WriteTx) error {
		return tx.Put(crashNS, []byte("base"), []byte("до миграции"))
	}); err != nil {
		say("ERR base %v", err)
		os.Exit(3)
	}
	say("BASE")
	if err := st.Migrate(ctx, 2); err != nil {
		say("ERR migrate2 %v", err)
		os.Exit(3)
	}
	say("MIGRATED")
	select {} // ждать убийства
}

// schemaAndMig — версия схемы и содержимое bucket миграции.
func schemaAndMig(t *testing.T, s *Storage) (version uint64, mig map[string]string, base string) {
	t.Helper()
	mig = map[string]string{}
	s.db.View(func(tx *bolt.Tx) error {
		for e := range tx.Check() {
			t.Errorf("tx.Check: %v", e)
		}
		if m := tx.Bucket(metaBucket); m != nil {
			if v := m.Get(versionKey); v != nil {
				version = binary.BigEndian.Uint64(v)
			}
		}
		if b := tx.Bucket([]byte(crashNS)); b != nil {
			base = string(b.Get([]byte("base")))
		}
		if b := tx.Bucket([]byte(migNS)); b != nil {
			b.ForEach(func(k, v []byte) error { mig[string(k)] = string(v); return nil })
		}
		return nil
	})
	return
}

// SIGKILL посреди миграции: версия схемы прежняя либо новая, промежуточной
// нет; содержимое соответствует версии (миграция применена целиком или не
// применена); повторная миграция проходит и даёт то же содержимое, что
// миграция без сбоя.
func TestCrashBboltDuringMigration(t *testing.T) {
	if os.Getenv("BBOLT_CRASH_PATH") != "" || testing.Short() {
		t.Skip("дочерний процесс или -short")
	}
	ctx := context.Background()
	// Эталон — миграция без сбоя.
	refPath := filepath.Join(t.TempDir(), "ref.db")
	ref, err := New(refPath, migrations2(nil))
	if err != nil {
		t.Fatal(err)
	}
	ref.Migrate(ctx, 2)
	_, want, _ := schemaAndMig(t, ref)
	ref.Close(ctx)
	if len(want) != migKeys {
		t.Fatalf("эталон: %d ключей", len(want))
	}

	rng := rand.New(rand.NewSource(crashSeed(t)))
	old, migrated := 0, 0
	for k := 0; k < 30; k++ {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("mig-%d.db", k))
		// На каком отчёте о прогрессе убить; migKeys — уже после MIGRATED,
		// чтобы проверялись оба исхода, а не только «не применена».
		target := rng.Intn(migKeys/500+1) * 500
		runCrashChild(t, path, "migrate", 1, func(line string) bool {
			if strings.HasPrefix(line, "MIG ") {
				i, _ := strconv.Atoi(strings.TrimPrefix(line, "MIG "))
				return i >= target
			}
			return line == "MIGRATED"
		}, 30*time.Second)

		st, err := New(path, migrations2(nil))
		if err != nil {
			t.Fatalf("открытие после SIGKILL: %v", err)
		}
		v, mig, base := schemaAndMig(t, st)
		if base != "до миграции" {
			t.Fatalf("данные до миграции потеряны: %q", base)
		}
		switch {
		case v == 1 && len(mig) == 0:
			old++
		case v == 2 && len(mig) == migKeys:
			migrated++
		default:
			t.Fatalf("промежуточное состояние: версия %d, ключей миграции %d", v, len(mig))
		}
		if err := st.Migrate(ctx, 2); err != nil {
			t.Fatalf("повторная миграция: %v", err)
		}
		v, mig, _ = schemaAndMig(t, st)
		st.Close(ctx)
		if v != 2 || len(mig) != len(want) {
			t.Fatalf("после повторной миграции: версия %d, ключей %d", v, len(mig))
		}
		for key, val := range want {
			if mig[key] != val {
				t.Fatalf("после повторной миграции ключ %s отличается от эталона", key)
			}
		}
	}
	t.Logf("убийств посреди миграции 30: схема осталась прежней %d, миграция применена целиком %d", old, migrated)
}

// Close при активных транзакциях — без зависания, ≤ 15 с: Close ждёт
// идущие View/Update и возвращается после них.
func TestCloseWithActiveTransactions(t *testing.T) {
	s := newTestStorage(t, crashMigrations)
	ctx := context.Background()
	inView, inUpdate := make(chan struct{}), make(chan struct{})
	done := make(chan error, 2)
	go func() {
		done <- s.View(ctx, func(ports.ReadTx) error { close(inView); time.Sleep(700 * time.Millisecond); return nil })
	}()
	<-inView
	go func() {
		done <- s.Update(ctx, func(ports.WriteTx) error { close(inUpdate); time.Sleep(700 * time.Millisecond); return nil })
	}()
	<-inUpdate
	t0 := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- s.Close(ctx) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Close при активных транзакциях не вернулся за 15 с")
	}
	t.Logf("Close вернулся через %v", time.Since(t0).Round(time.Millisecond))
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("транзакция, шедшая во время Close: %v", err)
		}
	}
}
