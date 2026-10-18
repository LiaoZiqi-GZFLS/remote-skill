//go:build windows

package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"remote-skill/internal/proto"
)

// nativePTY is false on Windows: sessions are virtual (one PowerShell child per
// command) because there is no portable ConPTY support in this codebase.
const nativePTY = false

// startNative prepares a virtual session: no shell process is kept alive, the
// keeper only tracks state on disk.
func (s *Session) startNative() error {
	if err := os.MkdirAll(filepath.Dir(s.winEnvFile()), 0o700); err != nil {
		return err
	}
	shell := detectShell()
	s.mu.Lock()
	s.winShell = shell
	s.mu.Unlock()
	if env := readEnvFile(s.winEnvFile()); env != nil {
		s.mu.Lock()
		s.winEnv = env
		s.mu.Unlock()
	}
	s.initComplete.Store(true)
	close(s.initDone)
	s.logf("virtual windows session ready (shell %s, cwd %s)", shell, s.cwd)
	return nil
}

// detectShell prefers PowerShell 7 when it is installed.
func detectShell() string {
	for _, name := range []string{"pwsh.exe", "powershell.exe"} {
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
	}
	return "powershell.exe"
}

func (s *Session) winStateDir() string {
	if s.home == "" {
		return ""
	}
	return filepath.Join(s.home, ".rsk", "state")
}

func (s *Session) winEnvFile() string {
	return filepath.Join(s.winStateDir(), sanitizeName(s.name)+".wenv.json")
}

func (s *Session) winCwdFile() string {
	return filepath.Join(s.winStateDir(), sanitizeName(s.name)+".cwd")
}

// runNative executes one command in a fresh PowerShell process, restoring and
// then persisting the session's working directory and environment.
func (s *Session) runNative(ctx context.Context, req ExecRequest, sink Sink) (*ExecResult, error) {
	if strings.TrimSpace(req.Cmd) == "" {
		return nil, proto.Errorf(proto.CodeBadRequest, "command is empty")
	}
	if sink == nil {
		sink = NopSink{}
	}
	if err := s.WaitReady(ctx); err != nil {
		return nil, err
	}
	if err := s.acquire(ctx, req.WaitMS); err != nil {
		return nil, err
	}
	defer s.release()

	id := newRequestID()
	st := &execState{id: id, cmd: req.Cmd, started: time.Now()}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrSessionClosed
	}
	s.execs[id] = st
	s.cur = st
	cwd := s.cwd
	if req.Cwd != "" {
		cwd = req.Cwd
	}
	if cwd == "" {
		cwd = s.home
	}
	shell := s.winShell
	envSnapshot := make(map[string]string, len(s.winEnv))
	for k, v := range s.winEnv {
		envSnapshot[k] = v
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.execs, id)
		if s.cur == st {
			s.cur = nil
		}
		s.mu.Unlock()
	}()

	sub, err := s.subscribe(0)
	if err != nil {
		return nil, err
	}
	defer s.unsubscribe(sub)
	delivered := make(chan struct{})
	go func() {
		defer close(delivered)
		for {
			select {
			case ev := <-sub.ch:
				sink.Event(ev)
			case <-sub.dead:
				for {
					select {
					case ev := <-sub.ch:
						sink.Event(ev)
					default:
						return
					}
				}
			}
		}
	}()
	sink.Event(Event{Session: s.name, Chan: proto.ChMeta, ExecID: id, Data: []byte("exec_start")})

	script := s.buildNativeScript(req, cwd, envSnapshot)
	args := []string{"-NoLogo", "-NonInteractive", "-Command", script}
	if s.opts.NoProfile {
		args = append([]string{"-NoProfile"}, args...)
	}
	cmd := exec.Command(shell, args...)
	cmd.Dir = dirIfExists(cwd)
	cmd.Env = nativeEnv(envSnapshot, s.opts.Env, req.Env, s.name)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, proto.Errorf(proto.CodeIO, "stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, proto.Errorf(proto.CodeIO, "stderr pipe: %v", err)
	}
	s.logf("exec %s start (timeout=%dms): %s", id, req.TimeoutMS, truncate(req.Cmd, 200))
	if err := cmd.Start(); err != nil {
		return nil, proto.Errorf(proto.CodeIO, "start powershell: %v", err)
	}
	s.mu.Lock()
	s.cmd = cmd
	s.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.pumpNative(stdout) }()
	go func() { defer wg.Done(); s.pumpNative(stderr) }()

	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()

	timedOut, interrupted := false, false
	var waitErr error
	killTree := func() {
		if cmd.Process != nil {
			killProcessTree(cmd.Process.Pid)
		}
	}
	if req.TimeoutMS > 0 {
		timer := time.NewTimer(time.Duration(req.TimeoutMS) * time.Millisecond)
		select {
		case waitErr = <-waitDone:
			timer.Stop()
		case <-timer.C:
			timedOut = true
			killTree()
			waitErr = <-waitDone
		case <-ctx.Done():
			interrupted = true
			killTree()
			waitErr = <-waitDone
		}
	} else {
		select {
		case waitErr = <-waitDone:
		case <-ctx.Done():
			interrupted = true
			killTree()
			waitErr = <-waitDone
		}
	}
	wg.Wait()
	s.waitQuiet(60*time.Millisecond, 750*time.Millisecond)
	s.mu.Lock()
	s.cmd = nil
	s.mu.Unlock()

	rc := 0
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			rc = exitErr.ExitCode()
		} else {
			rc = -1
		}
	}
	endCwd, endEnv := s.readNativeState()
	s.mu.Lock()
	if endCwd != "" {
		s.cwd = endCwd
		cwd = endCwd
	}
	if endEnv != nil {
		s.winEnv = endEnv
	}
	s.cmdCount++
	s.lastActive = time.Now()
	s.mu.Unlock()

	res := &ExecResult{
		ID:          id,
		Session:     s.name,
		Cmd:         req.Cmd,
		RC:          rc,
		TimedOut:    timedOut,
		Interrupted: interrupted,
		DurationMS:  time.Since(st.started).Milliseconds(),
		Cwd:         cwd,
		StartedAt:   st.started,
	}
	s.mu.Lock()
	s.history = append(s.history, *res)
	if len(s.history) > 100 {
		s.history = s.history[len(s.history)-100:]
	}
	s.mu.Unlock()

	s.unsubscribe(sub)
	<-delivered
	s.maybeSaveState()
	sink.Event(Event{Session: s.name, Chan: proto.ChDone, ExecID: id, Data: []byte("exec_done")})
	s.logf("exec %s finished rc=%d cwd=%s", id, rc, cwd)
	return res, nil
}

// pumpNative streams one pipe as session output.
func (s *Session) pumpNative(r interface{ Read([]byte) (int, error) }) {
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			s.publish(chunk)
		}
		if err != nil {
			return
		}
	}
}

// buildNativeScript wraps a command with state restore and snapshot.
func (s *Session) buildNativeScript(req ExecRequest, cwd string, env map[string]string) string {
	var b strings.Builder
	b.WriteString("$ErrorActionPreference = 'Continue'\n")
	if cwd != "" {
		fmt.Fprintf(&b, "Set-Location -LiteralPath %s -ErrorAction SilentlyContinue\n", psQuote(cwd))
	}
	write := func(k, v string) {
		fmt.Fprintf(&b, "[Environment]::SetEnvironmentVariable(%s, %s)\n", psQuote(k), psQuote(v))
	}
	for k, v := range env {
		write(k, v)
	}
	for k, v := range s.opts.Env {
		write(k, v)
	}
	for k, v := range req.Env {
		write(k, v)
	}
	b.WriteString(req.Cmd)
	b.WriteString("\n")
	b.WriteString("$__rsk_ok = $?\n")
	b.WriteString("$__rsk_rc = $LASTEXITCODE\n")
	b.WriteString("if ($null -eq $__rsk_rc) { if ($__rsk_ok) { $__rsk_rc = 0 } else { $__rsk_rc = 1 } }\n")
	fmt.Fprintf(&b, "[IO.File]::WriteAllText(%s, (Get-Location).Path)\n", psQuote(s.winCwdFile()))
	fmt.Fprintf(&b, "try { (Get-ChildItem env: | ForEach-Object { @{n=$_.Name; v=$_.Value} } | ConvertTo-Json -Compress) | Set-Content -LiteralPath %s -Encoding UTF8 } catch {}\n", psQuote(s.winEnvFile()))
	b.WriteString("exit $__rsk_rc\n")
	return b.String()
}

// readNativeState reads the cwd and environment the last command left behind.
func (s *Session) readNativeState() (string, map[string]string) {
	var cwd string
	if data, err := os.ReadFile(s.winCwdFile()); err == nil {
		cwd = strings.TrimSpace(strings.TrimPrefix(string(data), "\ufeff"))
	}
	return cwd, readEnvFile(s.winEnvFile())
}

// readEnvFile parses the environment snapshot written by a command.
func readEnvFile(path string) map[string]string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var list []struct {
		N string `json:"n"`
		V string `json:"v"`
	}
	if json.Unmarshal(data, &list) != nil {
		return nil
	}
	env := make(map[string]string, len(list))
	for _, kv := range list {
		if kv.N != "" {
			env[kv.N] = kv.V
		}
	}
	return env
}

// signalNative stops the running command; the virtual session survives.
func (s *Session) signalNative(string) error {
	s.mu.Lock()
	cmd := s.cmd
	s.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return proto.Errorf(proto.CodeConflict, "no foreground command to signal")
	}
	killProcessTree(cmd.Process.Pid)
	return nil
}

func (s *Session) closeNative() error {
	if s.isClosed() {
		return nil
	}
	_ = s.signalNative("KILL")
	s.markClosed(nil)
	return nil
}

// attachSupported is false: there is no pty to stream interactively.
func (s *Session) attachSupported() bool { return false }

// psQuote renders a PowerShell single quoted string.
func psQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func dirIfExists(dir string) string {
	if st, err := os.Stat(dir); err == nil && st.IsDir() {
		return dir
	}
	return ""
}

// nativeEnv builds the child environment from the process, the session and the
// per-command overrides.
func nativeEnv(base, session, req map[string]string, name string) []string {
	seen := map[string]bool{}
	env := make([]string, 0, len(os.Environ())+len(base)+4)
	add := func(k, v string) {
		key := strings.ToUpper(k)
		if seen[key] {
			return
		}
		seen[key] = true
		env = append(env, k+"="+v)
	}
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i > 0 {
			add(kv[:i], kv[i+1:])
		}
	}
	for k, v := range base {
		add(k, v)
	}
	for k, v := range session {
		add(k, v)
	}
	for k, v := range req {
		add(k, v)
	}
	add("RSK", "1")
	add("RSK_SESSION", name)
	return env
}

// killProcessTree terminates a process and everything it started.
func killProcessTree(pid int) {
	if pid <= 0 {
		return
	}
	kill := exec.Command("taskkill", "/PID", fmt.Sprint(pid), "/T", "/F")
	kill.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	_ = kill.Run()
}
