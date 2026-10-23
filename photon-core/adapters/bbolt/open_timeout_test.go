package bbolt

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Регрессия: занятый файл не подвешивает New навсегда (ADR-064 п. 1.6).
//
// До исправления New открывал bbolt с nil-опциями (Timeout = 0), и второй
// New того же файла ждал бесконечно. Тест не может зависнуть сам: второй
// New идёт в горутине, а ожидание ограничено сторожем с запасом над
// таймаутом открытия.

func withOpenTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := openTimeout
	openTimeout = d
	t.Cleanup(func() { openTimeout = prev })
}

func TestNewLockedFileFailsWithinTimeout(t *testing.T) {
	withOpenTimeout(t, 200*time.Millisecond)
	path := filepath.Join(t.TempDir(), "locked.db")

	first, err := New(path, nil)
	if err != nil {
		t.Fatalf("первое открытие: %v", err)
	}
	defer first.Close(context.Background())

	type result struct {
		st  *Storage
		err error
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		st, err := New(path, nil)
		done <- result{st, err}
	}()

	var r result
	select {
	case r = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("второй New не вернулся за 10 с: открытие занятого файла снова ждёт бесконечно")
	}
	elapsed := time.Since(start)

	if r.err == nil {
		r.st.Close(context.Background())
		t.Fatal("второй New того же файла обязан вернуть ошибку, пока первый держит файл")
	}
	if !errors.Is(r.err, bolt.ErrTimeout) {
		t.Fatalf("ошибка = %v, ожидалась обёртка bolt.ErrTimeout", r.err)
	}
	// bbolt сдаётся на шаг повтора flock (50 мс) раньше номинала, поэтому
	// нижняя граница — половина таймаута: важно, что ждал, а не отказал сразу.
	if elapsed < 100*time.Millisecond {
		t.Fatalf("отказ за %v — без ожидания: держателя не ждали вовсе", elapsed)
	}

	// Первый инстанс продолжает работать: запись и чтение.
	ctx := context.Background()
	if err := first.Migrate(ctx, 0); err != nil {
		t.Fatalf("первый инстанс после отказа второго: Migrate: %v", err)
	}
	if err := first.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("ns"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte("v"))
	}); err != nil {
		t.Fatalf("первый инстанс после отказа второго: запись: %v", err)
	}
	var got []byte
	if err := first.db.View(func(tx *bolt.Tx) error {
		got = append([]byte(nil), tx.Bucket([]byte("ns")).Get([]byte("k"))...)
		return nil
	}); err != nil || string(got) != "v" {
		t.Fatalf("первый инстанс после отказа второго: чтение %q, %v", got, err)
	}
}

// Держатель отпустил файл раньше таймаута — New дожидается и открывает:
// таймаут ограничивает ожидание, а не отменяет его.
func TestNewWaitsForShortHolder(t *testing.T) {
	withOpenTimeout(t, 3*time.Second)
	path := filepath.Join(t.TempDir(), "short.db")

	first, err := New(path, nil)
	if err != nil {
		t.Fatalf("первое открытие: %v", err)
	}
	time.AfterFunc(150*time.Millisecond, func() { first.Close(context.Background()) })

	second, err := New(path, nil)
	if err != nil {
		t.Fatalf("второй New после освобождения файла: %v", err)
	}
	second.Close(context.Background())
}

// Действующее значение — объявленная константа: переопределение живёт только
// в тестах пакета.
func TestOpenTimeoutDefault(t *testing.T) {
	if openTimeout != OpenTimeout || OpenTimeout <= 0 {
		t.Fatalf("openTimeout = %v, OpenTimeout = %v", openTimeout, OpenTimeout)
	}
}
