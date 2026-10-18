package daemon

import (
	"context"
	"os/exec"
	"strings"
	"sync"
	"time"
)

var (
	kernelOnce sync.Once
	kernelRel  string
)

// kernelRelease reports the host kernel version, best effort and cached.
func kernelRelease() string {
	kernelOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "uname", "-r").Output()
		if err == nil {
			kernelRel = strings.TrimSpace(string(out))
		}
	})
	return kernelRel
}

func (c *conn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
