//go:build !unix

package host

import "os/exec"

// killProcess: вне Unix (Windows) process group через Setpgid не
// применяется — sandbox/linux.go специфичен для Linux, и на этой
// платформе группу создавать было не из чего. Убиваем сам процесс, как
// и раньше.
func killProcess(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
