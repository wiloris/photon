package dispatcher

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

/*
 * Ошибки ввода-вывода хранилища — ADR-130 §5.3: классифицированная ошибка
 * хранилища (ErrStorageUnavailable), без паники; сетевые действия инстанса
 * продолжают работать.
 */

// Каталог только для чтения: файл хранилища не создаётся — Photon_Create
// отказывает классифицированной ошибкой (ADR-064 п. 1.6), без паники.
func TestStorageReadOnlyDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root пишет в каталог только для чтения")
	}
	dir := filepath.Join(t.TempDir(), "ro")
	if err := os.Mkdir(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) })
	_, err := New(Config{StoragePath: filepath.Join(dir, "core.db")})
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("err = %v, want ErrStorageUnavailable", err)
	}
}

// Нехватка места во время работы: дочерний процесс ограничивает размер
// файла (RLIMIT_FSIZE — без root и без отдельной файловой системы), и запись
// в ленту упирается в предел. Родитель проверяет отчёт дочернего процесса.
func TestStorageNoSpaceKeepsNetworkWorking(t *testing.T) {
	if os.Getenv("DISPATCHER_NOSPACE_CHILD") != "" {
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestStorageNoSpaceChild$", "-test.v")
	cmd.Env = append(os.Environ(), "DISPATCHER_NOSPACE_CHILD="+filepath.Join(t.TempDir(), "core.db"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("дочерний процесс: %v\n%s", err, out)
	}
	for _, want := range []string{
		"NOSPACE write-failed class=internal storage-unavailable=true",
		"NOSPACE network-ok",
		"NOSPACE local-read-ok",
		"NOSPACE diag update_errors>0",
	} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("нет «%s» в отчёте:\n%s", want, out)
		}
	}
}

func TestStorageNoSpaceChild(t *testing.T) {
	path := os.Getenv("DISPATCHER_NOSPACE_CHILD")
	if path == "" {
		t.Skip("только как дочерний процесс TestStorageNoSpaceKeepsNetworkWorking")
	}
	signal.Ignore(syscall.SIGXFSZ) // вместо сигнала — EFBIG из write/ftruncate
	br := newFakeBridge()
	d := newDispatcher(t, br, func(c *Config) { c.StoragePath = path })
	if _, err := d.SendAction(800, []byte(`{"channel_id":"c","limit":10}`)); err != nil {
		t.Fatalf("800 до предела: %v", err)
	}
	fi, _ := os.Stat(path)
	lim := uint64(fi.Size()) + 64<<10
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: lim, Max: lim}); err != nil {
		t.Fatalf("setrlimit: %v", err)
	}
	body := strings.Repeat("ж", 4000)
	var failed error
	for i := 0; i < 400 && failed == nil; i++ {
		payload := fmt.Sprintf(`{"channel_id":"c","records":[{"id":"r%d","order_key":%d,"version":1,"body":{"t":"%s"}}]}`, i, i, body)
		_, failed = d.SendAction(801, []byte(payload))
	}
	if failed == nil {
		t.Fatal("предел размера файла не достигнут за 400 записей")
	}
	fmt.Printf("NOSPACE write-failed class=%s storage-unavailable=%v (%v)\n",
		Classify(failed), errors.Is(failed, ErrStorageUnavailable), failed)

	if _, err := d.SendAction(100, []byte(`{}`)); err != nil {
		t.Fatalf("сетевое действие после отказа хранилища: %v", err)
	}
	fmt.Println("NOSPACE network-ok")
	if _, err := d.SendAction(800, []byte(`{"channel_id":"c","limit":10}`)); err != nil {
		t.Fatalf("чтение ленты после отказа записи: %v", err)
	}
	fmt.Println("NOSPACE local-read-ok")
	if s := d.Diag(); s != nil && s.Snapshot().Storage.UpdateErrors > 0 {
		fmt.Println("NOSPACE diag update_errors>0")
	} else if s == nil {
		fmt.Println("NOSPACE diag update_errors>0 (диагностика выключена — не проверялось)")
	}
}
