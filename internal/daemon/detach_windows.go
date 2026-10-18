//go:build windows

package daemon

import (
	"errors"
	"os"
	"syscall"
)

const (
	detachedProcess       = 0x00000008
	createNewProcessGroup = 0x00000200
)

func detachAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{CreationFlags: detachedProcess | createNewProcessGroup}
}

func processAlive(pid int) bool {
	h, err := syscall.OpenProcess(syscall.PROCESS_QUERY_INFORMATION, false, uint32(pid))
	if err != nil {
		return false
	}
	syscall.CloseHandle(h)
	return true
}

func terminateProcess(pid int) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return p.Kill()
}

func killProcess(pid int) error { return terminateProcess(pid) }

var errNoSignals = errors.New("process signals are not supported by the windows daemon")

func killGroup(pgid int, name string) error { return errNoSignals }

func killProc(pid int, name string) error { return errNoSignals }

func signalNumber(name string) (int, error) { return 0, errNoSignals }

func ignoreHangup() {}

func interruptSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
