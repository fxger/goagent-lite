//go:build windows

package benchmark_test

import (
	"syscall"
	"unsafe"
)

var (
	modkernel32         = syscall.NewLazyDLL("kernel32.dll")
	procGetProcessTimes = modkernel32.NewProc("GetProcessTimes")
)

type filetime struct {
	LowDateTime  uint32
	HighDateTime uint32
}

func ftSeconds(ft filetime) float64 {
	ticks := uint64(ft.HighDateTime)<<32 | uint64(ft.LowDateTime)
	return float64(ticks) / 1e7 // 100ns ticks
}

// processCPUSeconds returns user+kernel CPU time of this process. The Go
// runtime's /cpu/classes metrics either include idle time (total) or omit
// kernel time, so the OS counter is the accurate source for a load report.
func processCPUSeconds() float64 {
	var creation, exit, kernel, user filetime
	handle, _ := syscall.GetCurrentProcess()
	r, _, _ := procGetProcessTimes.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(&creation)),
		uintptr(unsafe.Pointer(&exit)),
		uintptr(unsafe.Pointer(&kernel)),
		uintptr(unsafe.Pointer(&user)),
	)
	if r == 0 {
		return 0
	}
	return ftSeconds(kernel) + ftSeconds(user)
}
