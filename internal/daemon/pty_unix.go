//go:build linux || darwin

package daemon

import (
	"syscall"
	"unsafe"
)

// tcGetPgrp reports the foreground process group of the terminal behind fd.
// It is used to deliver signals only to the job the user is currently running,
// exactly like a real terminal would.
func tcGetPgrp(fd int) (int, error) {
	var pgrp int32
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uintptr(syscall.TIOCGPGRP), uintptr(unsafe.Pointer(&pgrp)))
	if errno != 0 {
		return 0, errno
	}
	return int(pgrp), nil
}
