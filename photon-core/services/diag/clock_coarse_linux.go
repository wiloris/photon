//go:build linux

package diag

import (
	"syscall"
	"unsafe"
)

// clockMonotonicCoarse — CLOCK_MONOTONIC_COARSE (linux/time.h). Та же шкала,
// что у CLOCK_MONOTONIC (и у runtime.nanotime), но значение — время
// последнего тика ядра: чтение не обращается к аппаратному таймеру.
const clockMonotonicCoarse = 6

const coarseAvailable = true

// coarseMono — грубые монотонные часы, нс. Системный вызов, а не vDSO: Go
// не даёт vDSO-вызова для COARSE. RawSyscall — вызов не блокирует и не
// нуждается в уведомлении планировщика.
func coarseMono() int64 {
	var ts syscall.Timespec
	syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, clockMonotonicCoarse, uintptr(unsafe.Pointer(&ts)), 0)
	return ts.Nano()
}

// coarseResolution — разрешение грубых часов, нс (тик ядра: 1 мс при HZ=1000,
// 4 мс при HZ=250).
func coarseResolution() int64 {
	var ts syscall.Timespec
	if _, _, e := syscall.RawSyscall(syscall.SYS_CLOCK_GETRES, clockMonotonicCoarse, uintptr(unsafe.Pointer(&ts)), 0); e != 0 {
		return 0
	}
	return ts.Nano()
}
