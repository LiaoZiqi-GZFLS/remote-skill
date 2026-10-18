//go:build !windows

package daemon

import (
	"os"
	"os/signal"
	"strings"
	"syscall"

	"remote-skill/internal/proto"
)

// detachAttr makes a child process leave the current session so that it
// survives the ssh connection that spawned it.
func detachAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}

func processAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

func terminateProcess(pid int) error {
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return err
	}
	return nil
}

func killProcess(pid int) error {
	return syscall.Kill(pid, syscall.SIGKILL)
}

// killGroup signals an entire process group (negative pid).
func killGroup(pgid int, name string) error {
	sig, err := signalNumber(name)
	if err != nil {
		return err
	}
	return syscall.Kill(-pgid, sig)
}

// killProc signals a single process.
func killProc(pid int, name string) error {
	sig, err := signalNumber(name)
	if err != nil {
		return err
	}
	return syscall.Kill(pid, sig)
}

func signalNumber(name string) (syscall.Signal, error) {
	switch strings.ToUpper(strings.TrimPrefix(name, "SIG")) {
	case "INT", "INTERRUPT":
		return syscall.SIGINT, nil
	case "TERM":
		return syscall.SIGTERM, nil
	case "KILL":
		return syscall.SIGKILL, nil
	case "HUP":
		return syscall.SIGHUP, nil
	case "QUIT":
		return syscall.SIGQUIT, nil
	case "WINCH":
		return syscall.SIGWINCH, nil
	case "STOP":
		return syscall.SIGSTOP, nil
	case "CONT":
		return syscall.SIGCONT, nil
	default:
		return 0, proto.Errorf(proto.CodeBadRequest, "unsupported signal %q", name)
	}
}

// ignoreHangup shields a detached daemon from terminal/ssh session teardown.
func ignoreHangup() {
	signal.Ignore(syscall.SIGHUP, syscall.SIGPIPE)
}

// interruptSignals are forwarded to a clean shutdown handler.
func interruptSignals() []os.Signal {
	return []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}
}
