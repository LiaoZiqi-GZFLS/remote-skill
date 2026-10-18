//go:build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func signalNotifyImpl(ch chan os.Signal) {
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
}

func detachAttr() *syscall.SysProcAttr { return &syscall.SysProcAttr{Setsid: true} }

func processAlive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func terminateProcess(pid int) error { return syscall.Kill(pid, syscall.SIGTERM) }

func killProcess(pid int) error { return syscall.Kill(pid, syscall.SIGKILL) }

func ignoreHangup() { signal.Ignore(syscall.SIGHUP, syscall.SIGPIPE) }
