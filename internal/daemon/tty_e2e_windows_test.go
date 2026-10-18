//go:build windows

package daemon

import (
	"encoding/json"
	"log"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"remote-skill/internal/proto"
)

// TestWindowsTTYExecEndToEnd drives the real daemon over a unix socket and
// runs a command through exec.tty on ConPTY (output, exit code, done frame).
func TestWindowsTTYExecEndToEnd(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "d.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(Options{SocketPath: socket, Logger: log.New(testWriter{t}, "", 0)})
	go func() { _ = srv.Serve(ln) }()
	defer srv.Shutdown(true)

	nc, err := net.DialTimeout("unix", socket, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	pc := proto.NewConn(nc, nc)
	hello, _ := proto.NewRequest("h", "", map[string]any{"api": proto.APIVersion})
	hello.Type = proto.TypeHello
	if err := pc.Write(hello); err != nil {
		t.Fatal(err)
	}
	if _, err := pc.Read(); err != nil {
		t.Fatal(err)
	}

	req, err := proto.NewRequest("t1", proto.MethodExecTTY, map[string]any{
		"cmd":        "Write-Output tty-e2e-ok; exit 7",
		"rows":       40,
		"cols":       100,
		"timeout_ms": 20000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.Write(req); err != nil {
		t.Fatal(err)
	}

	var out strings.Builder
	var lastRes *proto.Envelope
	_ = nc.SetReadDeadline(time.Now().Add(30 * time.Second))
	for {
		env, rerr := pc.Read()
		if rerr != nil {
			t.Fatalf("read: %v (output so far %q)", rerr, out.String())
		}
		switch env.Type {
		case proto.TypeEvent:
			if env.Chan == proto.ChOut {
				out.Write(env.Data)
			}
			if env.Chan == proto.ChDone {
				if lastRes == nil {
					t.Fatal("done before final result")
				}
				var res ExecResult
				if derr := lastRes.DecodeResult(&res); derr != nil {
					t.Fatal(derr)
				}
				if !strings.Contains(out.String(), "tty-e2e-ok") {
					t.Fatalf("output missing marker: %q", out.String())
				}
				if res.RC != 7 {
					raw, _ := json.Marshal(res)
					t.Fatalf("rc=%d want 7 (%s)", res.RC, raw)
				}
				return
			}
		case proto.TypeRes:
			if env.ID == "t1" {
				lastRes = env
			}
		}
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}
