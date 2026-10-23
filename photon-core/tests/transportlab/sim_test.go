package transportlab

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// bridgeSim — процесс bridgesim (quic-go v0.59, как у Bridge), управляемый
// строками stdin; «ACTION n» из его stdout вызывает onAction.
type bridgeSim struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	addr     string
	onAction atomic.Value // func(int)
	wg       sync.WaitGroup
}

var (
	simOnce sync.Once
	simBin  string
	simErr  error
)

// buildSim собирает bridgesim один раз на прогон — своим toolchain
// (GOTOOLCHAIN=auto подставляет go1.24.7 по директиве toolchain).
func buildSim(t *testing.T) string {
	simOnce.Do(func() {
		dir, err := os.MkdirTemp("", "bridgesim-")
		if err != nil {
			simErr = err
			return
		}
		simBin = filepath.Join(dir, "bridgesim")
		cmd := exec.Command("go", "build", "-o", simBin, ".")
		cmd.Dir = "bridgesim"
		cmd.Env = append(os.Environ(), "GOTOOLCHAIN=auto", "GOFLAGS=")
		if out, err := cmd.CombinedOutput(); err != nil {
			simErr = fmt.Errorf("сборка bridgesim: %v\n%s", err, out)
		}
	})
	if simErr != nil {
		t.Fatal(simErr)
	}
	return simBin
}

func startSim(t *testing.T) *bridgeSim { return startSimAt(t, "") }

// startSimAt — bridgesim на заданном адресе ("" — свободный порт).
func startSimAt(t *testing.T, addr string) *bridgeSim {
	var args []string
	if addr != "" {
		args = append(args, addr)
	}
	cmd := exec.Command(buildSim(t), args...)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s := &bridgeSim{cmd: cmd, stdin: stdin}
	sc := bufio.NewScanner(stdout)
	if !sc.Scan() || !strings.HasPrefix(sc.Text(), "LISTEN ") {
		t.Fatalf("bridgesim не сообщил адрес: %q", sc.Text())
	}
	s.addr = strings.TrimPrefix(sc.Text(), "LISTEN ")
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for sc.Scan() {
			var n int
			if _, err := fmt.Sscanf(sc.Text(), "ACTION %d", &n); err == nil {
				if f, ok := s.onAction.Load().(func(int)); ok && f != nil {
					f(n)
				}
			}
		}
	}()
	return s
}

func (s *bridgeSim) mode(m string) { fmt.Fprintln(s.stdin, m) }

func (s *bridgeSim) Close() {
	s.stdin.Close() // bridgesim выходит по закрытию stdin
	done := make(chan struct{})
	go func() { s.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		s.cmd.Process.Kill()
		<-done
	}
	s.wg.Wait()
}

// kill — SIGKILL: процесс исчезает без CONNECTION_CLOSE, как процесс
// плагина, завершённый Mattermost (S3-a, локальный аналог).
func (s *bridgeSim) kill() {
	s.cmd.Process.Kill()
	s.cmd.Wait()
	s.stdin.Close()
	s.wg.Wait()
}
