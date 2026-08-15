package host

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/wiloris/photon-plugin-runtime/sandbox"
)

// execSpawner запускает плагин как реальный процесс, общаясь с ним
// через stdin/stdout.
//
// Выбор stdin/stdout, а не unix-сокета: работает одинаково на Linux,
// macOS и Windows, не требует файловой системы для сокета и не
// оставляет следов при падении процесса. Для Android потребуется своя
// реализация Spawner — интерфейс существует именно для этого.
type execSpawner struct{}

// pipeConn склеивает stdin и stdout процесса в один
// io.ReadWriteCloser: пишем в stdin плагина, читаем из его stdout.
type pipeConn struct {
	stdin  io.WriteCloser
	stdout io.ReadCloser

	closeOnce sync.Once
}

func (c *pipeConn) Read(p []byte) (int, error)  { return c.stdout.Read(p) }
func (c *pipeConn) Write(p []byte) (int, error) { return c.stdin.Write(p) }

func (c *pipeConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		// Сначала stdin: закрытие даёт плагину EOF и шанс завершиться
		// самому, до того как мы закроем чтение.
		if e := c.stdin.Close(); e != nil {
			err = e
		}
		if e := c.stdout.Close(); e != nil && err == nil {
			err = e
		}
	})
	return err
}

func (execSpawner) Spawn(ctx context.Context, entrypoint string, iso Isolation, spec sandbox.Spec) (io.ReadWriteCloser, func() error, func(), sandbox.Applied, error) {
	cmd := exec.Command(entrypoint)

	// Изоляция применяется ДО старта процесса: после запуска настроить
	// её уже нельзя.
	var applied sandbox.Applied
	if iso != nil {
		var err error
		applied, err = iso.Prepare(cmd, spec)
		if err != nil {
			return nil, nil, nil, applied, fmt.Errorf("apply isolation: %w", err)
		}
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, applied, fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stdin.Close()
		return nil, nil, nil, applied, fmt.Errorf("stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		stdin.Close()
		stdout.Close()
		return nil, nil, nil, applied, fmt.Errorf("start: %w", err)
	}

	conn := &pipeConn{stdin: stdin, stdout: stdout}

	var waitOnce sync.Once
	var waitErr error
	wait := func() error {
		waitOnce.Do(func() { waitErr = cmd.Wait() })
		return waitErr
	}

	kill := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}

	return conn, wait, kill, applied, nil
}
