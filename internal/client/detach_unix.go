//go:build !windows

package client

import "syscall"

func brokerSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}
