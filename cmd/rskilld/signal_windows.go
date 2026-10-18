//go:build windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

const (
	detachedProcess       = 0x00000008
	createNewProcessGroup = 0x00000200
)

func signalNotifyImpl(ch chan os.Signal) {
	signal.Notify(ch, os.Interrupt)
}

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

func ignoreHangup() {}
