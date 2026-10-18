//go:build windows

package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/UserExistsError/conpty"

	"remote-skill/internal/proto"
)

// writeTTYScript stores a command so the shell never has to parse quotes.
func writeTTYScript(command string) (string, error) {
	path := filepath.Join(os.TempDir(), "rsk-tty-"+newRequestID()+".ps1")
	body := "\ufeff" + command + "\r\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

// startWindowsTTY launches a command on a Windows pseudo console.
func startWindowsTTY(command, cwd string, rows, cols uint16) (*conpty.ConPty, string, error) {
	script, err := writeTTYScript(command)
	if err != nil {
		return nil, "", err
	}
	shell := detectShell()
	line := fmt.Sprintf(`"%s" -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%s"`, shell, script)
	opts := []conpty.ConPtyOption{conpty.ConPtyDimensions(int(cols), int(rows))}
	if st, serr := os.Stat(cwd); serr == nil && st.IsDir() {
		opts = append(opts, conpty.ConPtyWorkDir(cwd))
	}
	opts = append(opts, conpty.ConPtyEnv(os.Environ()))
	cp, err := conpty.Start(line, opts...)
	if err != nil {
		_ = os.Remove(script)
		return nil, "", err
	}
	return cp, script, nil
}

// handleExecTTYWindows runs a single command on a ConPTY.
func (c *conn) handleExecTTYWindows(req *request, env *proto.Envelope, p TTYParams) {
	cwd := p.Cwd
	if cwd == "" {
		if info, ok := c.srv.sessionInfo(p.Session); ok {
			cwd = info.Cwd
		}
	}
	rows, cols := p.Rows, p.Cols
	if rows == 0 {
		rows = 40
	}
	if cols == 0 {
		cols = 120
	}
	start := time.Now()
	cp, script, err := startWindowsTTY(p.Cmd, cwd, rows, cols)
	if err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeIO, "start conpty: %v", err))
		return
	}
	defer os.Remove(script)
	defer cp.Close()
	c.respond(env.ID, map[string]any{"pid": cp.Pid(), "cwd": cwd})
	sink := &connSink{c: c, id: env.ID}
	defer sink.detach()

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		buf := make([]byte, 32<<10)
		for {
			n, rerr := cp.Read(buf)
			if n > 0 {
				sink.publish(Event{Chan: proto.ChOut, Data: append([]byte(nil), buf[:n]...)})
			}
			if rerr != nil {
				return
			}
		}
	}()
	type waitResult struct {
		code uint32
		err  error
	}
	waitDone := make(chan waitResult, 1)
	go func() {
		code, werr := cp.Wait(context.Background())
		waitDone <- waitResult{code, werr}
	}()

	var timeoutCh <-chan time.Time
	if p.TimeoutMS > 0 {
		timer := time.NewTimer(time.Duration(p.TimeoutMS) * time.Millisecond)
		defer timer.Stop()
		timeoutCh = timer.C
	}
	timedOut := false
	finish := func(rc int) {
		_ = cp.Close()
		c.respond(env.ID, &ExecResult{
			ID: env.ID, Session: p.Session, Cmd: p.Cmd, RC: rc, TimedOut: timedOut,
			DurationMS: time.Since(start).Milliseconds(), Cwd: cwd, StartedAt: start,
		})
		sink.publish(Event{Chan: proto.ChDone})
	}
	for {
		select {
		case <-c.closed:
			_ = cp.Close()
			return
		case <-req.closed:
			_ = cp.Close()
			return
		case <-timeoutCh:
			timedOut = true
			_, _ = cp.Write([]byte{3})
			time.AfterFunc(3*time.Second, func() { killProcessTree(cp.Pid()) })
			timeoutCh = nil
		case in := <-req.in:
			switch in.Chan {
			case proto.ChIn:
				if _, werr := cp.Write(in.Data); werr != nil {
					return
				}
			case proto.ChSignal:
				sig, _ := in.Extra["sig"].(string)
				if sig == "INT" || sig == "" {
					_, _ = cp.Write([]byte{3})
				} else {
					killProcessTree(cp.Pid())
				}
			case proto.ChMeta:
				r, cc := extraUint16(in.Extra, "rows"), extraUint16(in.Extra, "cols")
				if r > 0 && cc > 0 {
					_ = cp.Resize(int(cc), int(r))
				}
			case proto.ChEof:
				_, _ = cp.Write([]byte{0x1a}) // Ctrl-Z ends a console input line
			}
		case wr := <-waitDone:
			rc := int(wr.code)
			if wr.err != nil {
				rc = -1
			}
			select {
			case <-readDone:
			case <-time.After(500 * time.Millisecond):
			}
			finish(rc)
			return
		}
	}
}
