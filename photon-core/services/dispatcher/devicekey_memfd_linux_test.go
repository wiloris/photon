//go:build linux

package dispatcher

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"testing"

	"golang.org/x/sys/unix"
)

// Ключ устройства без открытого файла на диске — по существующему контракту
// device_key_path (ADR-116 §3.3).
//
// Хост, который хранит ключ зашифрованным (ENGRAM Android: AES-ключ Keystore),
// расшифровывает его в анонимный файл в памяти своего же процесса (memfd,
// права 0600) и передаёт путь /proc/self/fd/N. Core читает ключ один раз при
// создании инстанса, после чего хост закрывает дескриптор. Контракт и проверки
// readDeviceKey не меняются: тот же base64, те же права не шире 0600.
func memfdKey(t *testing.T, content []byte, mode uint32) string {
	t.Helper()
	fd, err := unix.MemfdCreate("photon-device-key", unix.MFD_CLOEXEC)
	if err != nil {
		t.Skipf("memfd недоступен: %v", err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	if _, err := unix.Write(fd, content); err != nil {
		t.Fatal(err)
	}
	if err := unix.Fchmod(fd, mode); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("/proc/self/fd/%d", fd)
}

func TestDeviceKeyFromMemfdPath(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	enc := []byte(base64.StdEncoding.EncodeToString(priv))

	got, err := readDeviceKey(memfdKey(t, enc, 0o600))
	if err != nil {
		t.Fatalf("memfd 0600: %v", err)
	}
	if string(got) != string(priv) {
		t.Fatal("ключ из memfd не совпал")
	}
	// Тот же запрет, что для обычного файла: memfd, доступный группе или
	// остальным, — отказ.
	if _, err := readDeviceKey(memfdKey(t, enc, 0o644)); err == nil {
		t.Fatal("memfd 0644 принят")
	}

	// Исполнитель строится из такого пути — инстанс получает P2P.
	exec, err := newP2PExecutor(P2PConfig{DeviceKeyPath: memfdKey(t, enc, 0o600)})
	if err != nil {
		t.Fatalf("newP2PExecutor из memfd: %v", err)
	}
	exec.Shutdown()
}

// Анонимный канал вместо memfd. На Android приложению запрещено менять права
// memfd (SELinux: avc denied { setattr } … memfd:…), а создаётся он с 0777 —
// readDeviceKey отверг бы его. Inode канала создаётся ядром с правами 0600, и
// менять их не нужно: хост пишет ключ, закрывает конец записи, Core читает
// /proc/self/fd/N до EOF один раз при создании инстанса. Найдено запуском на
// AVD API 35.
func pipeKey(t *testing.T, content []byte) string {
	t.Helper()
	var p [2]int
	if err := unix.Pipe2(p[:], unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(p[0]) })
	if _, err := unix.Write(p[1], content); err != nil {
		t.Fatal(err)
	}
	_ = unix.Close(p[1])
	return fmt.Sprintf("/proc/self/fd/%d", p[0])
}

func TestDeviceKeyFromPipePath(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	enc := []byte(base64.StdEncoding.EncodeToString(priv))
	exec, err := newP2PExecutor(P2PConfig{DeviceKeyPath: pipeKey(t, enc)})
	if err != nil {
		t.Fatalf("newP2PExecutor из канала: %v", err)
	}
	exec.Shutdown()
	got, err := readDeviceKey(pipeKey(t, enc))
	if err != nil || string(got) != string(priv) {
		t.Fatalf("ключ из канала: %v", err)
	}
}
