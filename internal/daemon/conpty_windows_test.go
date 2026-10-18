//go:build windows

package daemon

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestWindowsConPTY proves the Windows tty path produces output natively.
func TestWindowsConPTY(t *testing.T) {
	cp, script, err := startWindowsTTY("Write-Output conpty-integration-ok", "", 40, 100)
	if err != nil {
		t.Fatalf("startWindowsTTY: %v", err)
	}
	defer cp.Close()
	defer func() { _ = os.Remove(script) }()
	var out strings.Builder
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			n, rerr := cp.Read(buf)
			if n > 0 {
				out.Write(buf[:n])
			}
			if rerr != nil {
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	code, werr := cp.Wait(ctx)
	if werr != nil {
		t.Fatalf("wait: %v", werr)
	}
	_ = code
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
	if !strings.Contains(out.String(), "conpty-integration-ok") {
		t.Fatalf("output did not contain the marker: %q", out.String())
	}
}
