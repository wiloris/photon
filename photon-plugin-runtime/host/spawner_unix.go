//go:build unix

package host

import (
	"os/exec"
	"syscall"
)

// killProcess завершает процесс плагина.
//
// Если при запуске была создана отдельная process group (Setpgid,
// sandbox/linux.go), SIGKILL уходит ВСЕЙ группе, а не только самому
// процессу: иначе дочерние процессы, которых плагин успел породить,
// пережили бы его собственную смерть как сироты под исходным init —
// P7 (завершаемость, docs/sandbox.md) было бы формально применено, но
// фактически не выполнено. См. docs/test-laboratory.md, "stress" /
// fork-bomb — единственная проверка этого свойства на настоящих
// процессах.
//
// Group-kill адресуется отрицательным PID (см. kill(2)): Setpgid делает
// сам процесс лидером своей группы, то есть pgid равен его собственному
// PID, и -pid убивает группу целиком, включая лидера.
func killProcess(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	if cmd.SysProcAttr != nil && cmd.SysProcAttr.Setpgid {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err == nil {
			return
		}
		// Группа не адресуется (уже не существует, права и т.п.) —
		// гарантируем хотя бы то, что было ДО этого изменения: сам
		// процесс должен умереть.
	}
	_ = cmd.Process.Kill()
}
