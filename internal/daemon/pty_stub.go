//go:build !(linux || darwin)

package daemon

import "errors"

func tcGetPgrp(fd int) (int, error) {
	return 0, errors.New("tcgetpgrp: unsupported platform")
}
