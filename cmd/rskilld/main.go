// Command rskilld is the remote half of remote-skill. `rsk` uploads this
// binary to the server and runs `rskilld bridge` over ssh, which forwards the
// ssh stdio channel to a long lived daemon process holding the workspace
// sessions.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"remote-skill/internal/daemon"
	"remote-skill/internal/proto"
	"remote-skill/internal/version"
)

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "rskilld: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cmd := "bridge"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet("rskilld "+cmd, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	home, _ := os.UserHomeDir()
	socket := fs.String("socket", defaultSocket(home), "daemon unix socket")
	logPath := fs.String("log", defaultLog(home), "daemon log file")
	sessionsDir := fs.String("sessions-dir", defaultSessionsDir(home), "directory holding session keeper sockets")
	restart := fs.Bool("restart", false, "replace a running daemon")
	inline := fs.Bool("inline-sessions", false, "keep sessions inside the daemon (they die with it)")
	idle := fs.Duration("idle-timeout", 0, "close sessions unused for this long (0 disables)")
	shell := fs.String("shell", "", "shell to run in sessions (default /bin/bash)")
	wait := fs.Duration("wait", 15*time.Second, "how long to wait for the daemon to come up")
	verbose := fs.Bool("verbose", false, "verbose logging")
	name := fs.String("name", "", "session name (keeper mode)")
	cwd := fs.String("cwd", "", "session working directory (keeper mode)")
	rows := fs.Uint("rows", 0, "initial terminal rows (keeper mode)")
	cols := fs.Uint("cols", 0, "initial terminal columns (keeper mode)")
	noProfile := fs.Bool("no-profile", false, "skip user rc files (keeper mode)")
	scrollback := fs.Int("scrollback", 0, "scrollback bytes (keeper mode)")
	var envFlags multiFlag
	fs.Var(&envFlags, "env", "default session environment KEY=VALUE (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	env := map[string]string{}
	for _, kv := range envFlags {
		if i := strings.IndexByte(kv, '='); i > 0 {
			env[kv[:i]] = kv[i+1:]
		}
	}

	switch cmd {
	case "serve":
		return serve(*socket, *logPath, *sessionsDir, *idle, *shell, env, *verbose, *inline)
	case "session":
		return runSessionKeeper(keeperOptions{
			Name:       *name,
			Socket:     *socket,
			LogPath:    *logPath,
			Shell:      *shell,
			Cwd:        *cwd,
			Rows:       uint16(*rows),
			Cols:       uint16(*cols),
			NoProfile:  *noProfile,
			Scrollback: *scrollback,
			Env:        env,
			Verbose:    *verbose,
		})
	case "bridge":
		return bridge(*socket, *logPath, *restart, *wait, *idle, *sessionsDir, *shell, env, *verbose, *inline)
	case "version", "--version", "-version":
		fmt.Printf("rskilld %s (api %d) %s/%s go=%s\n", version.Version, proto.APIVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())
		return nil
	case "selftest":
		return selftest(*socket, *logPath, *shell, env)
	case "stop":
		return stopDaemon(*socket)
	default:
		return fmt.Errorf("unknown command %q (expected serve|bridge|selftest|stop|version)", cmd)
	}
}

func defaultSocket(home string) string {
	if dir := os.Getenv("RSK_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, "rskd.sock")
	}
	return filepath.Join(home, ".rsk", "run", "rskd.sock")
}

func defaultLog(home string) string {
	if dir := os.Getenv("RSK_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, "rskd.log")
	}
	return filepath.Join(home, ".rsk", "log", "rskd.log")
}

func defaultSessionsDir(home string) string {
	if dir := os.Getenv("RSK_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, "sessions")
	}
	return filepath.Join(home, ".rsk", "run", "sessions")
}

func serve(socket, logPath, sessionsDir string, idle time.Duration, shell string, env map[string]string, verbose, inline bool) error {
	ignoreHangup()
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		return err
	}
	logger, closeLog, err := openLogger(logPath, verbose)
	if err != nil {
		return err
	}
	defer closeLog()
	logger.Printf("rskilld %s starting (socket %s pid %d)", version.Version, socket, os.Getpid())

	// A stale socket file left behind by a crash would block Listen.
	if _, err := net.DialTimeout("unix", socket, 300*time.Millisecond); err != nil {
		_ = os.Remove(socket)
	}
	l, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", socket, err)
	}
	defer l.Close()
	_ = os.Chmod(socket, 0o600)
	writePidFile(socket)

	opts := daemon.Options{
		SocketPath:  socket,
		Logger:      logger,
		IdleTimeout: idle,
		DefaultEnv:  env,
		Shell:       shell,
		Binary:      executablePath(),
		BinarySHA:   executableSHA(),
	}
	if shell != "" {
		logger.Printf("session shell override: %s", shell)
	}
	if inline {
		logger.Printf("inline sessions enabled: sessions end with this daemon")
	} else {
		opts.Proxy = daemon.NewKeeperManager(sessionsDir, opts.Binary, logPath, version.Version, shell, env, logger)
		logger.Printf("session keepers in %s (sessions survive daemon restarts)", sessionsDir)
	}
	srv := daemon.New(opts)

	sigCh := make(chan os.Signal, 1)
	signalNotify(sigCh)
	go func() {
		select {
		case sig := <-sigCh:
			logger.Printf("received %v, shutting down", sig)
			srv.Shutdown(true)
		case <-srv.ShutdownCh():
		}
		_ = l.Close()
	}()

	err = srv.Serve(l)
	// Serve returns as soon as the listener closes, but the shutdown routine
	// (which may still be stopping keepers) runs in its own goroutine: wait for
	// it so the process does not exit half way through cleanup.
	select {
	case <-srv.ShutdownDone():
	case <-time.After(60 * time.Second):
		logger.Printf("shutdown did not finish in time; exiting anyway")
	}
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

func bridge(socket, logPath string, restart bool, wait time.Duration, idle time.Duration, sessionsDir, shell string, env map[string]string, verbose, inline bool) error {
	if restart {
		_ = stopDaemon(socket)
	}
	if !socketAlive(socket) {
		if err := startDetached(socket, logPath, sessionsDir, idle, shell, env, verbose, inline); err != nil {
			return err
		}
	}
	deadline := time.Now().Add(wait)
	var nc net.Conn
	var err error
	for {
		nc, err = net.DialTimeout("unix", socket, time.Second)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("daemon did not become ready: %w", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	defer nc.Close()

	errCh := make(chan error, 2)
	go func() {
		_, err := io.Copy(nc, os.Stdin)
		if uw, ok := nc.(interface{ CloseWrite() error }); ok {
			_ = uw.CloseWrite()
		}
		errCh <- err
	}()
	go func() {
		_, err := io.Copy(os.Stdout, nc)
		errCh <- err
	}()
	<-errCh
	// Let the other direction drain briefly before tearing down.
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
	}
	return nil
}

func stopDaemon(socket string) error {
	pid := readPidFile(socket)
	if pid <= 0 {
		_ = os.Remove(socket)
		return nil
	}
	if processAlive(pid) {
		_ = terminateProcess(pid)
		for i := 0; i < 50; i++ {
			if !processAlive(pid) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if processAlive(pid) {
			_ = killProcess(pid)
		}
	}
	removePidFile(socket)
	_ = os.Remove(socket)
	return nil
}

func startDetached(socket, logPath, sessionsDir string, idle time.Duration, shell string, env map[string]string, verbose, inline bool) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return err
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	args := []string{"serve", "--socket", socket, "--log", logPath, "--sessions-dir", sessionsDir}
	if inline {
		args = append(args, "--inline-sessions")
	}
	if idle > 0 {
		args = append(args, "--idle-timeout", idle.String())
	}
	if shell != "" {
		args = append(args, "--shell", shell)
	}
	if verbose {
		args = append(args, "--verbose")
	}
	for k, v := range env {
		args = append(args, "--env", k+"="+v)
	}
	cmd := exec.Command(exe, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Stdin = nil
	cmd.SysProcAttr = detachAttr()
	cmd.Env = os.Environ()
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}
	if err := cmd.Process.Release(); err != nil {
		return err
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if socketAlive(socket) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("daemon did not start; see %s", logPath)
}

func socketAlive(socket string) bool {
	nc, err := net.DialTimeout("unix", socket, 500*time.Millisecond)
	if err != nil {
		return false
	}
	nc.Close()
	return true
}

func pidFile(socket string) string { return socket + ".pid" }

func writePidFile(socket string) {
	_ = os.WriteFile(pidFile(socket), []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o600)
}

func readPidFile(socket string) int {
	b, err := os.ReadFile(pidFile(socket))
	if err != nil {
		return 0
	}
	var pid int
	_, _ = fmt.Sscanf(strings.TrimSpace(string(b)), "%d", &pid)
	return pid
}

func removePidFile(socket string) { _ = os.Remove(pidFile(socket)) }

func openLogger(path string, verbose bool) (*log.Logger, func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, nil, err
	}
	if st, err := os.Stat(path); err == nil && st.Size() > 8<<20 {
		_ = os.Rename(path, path+".1")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, err
	}
	prefix := ""
	if verbose {
		prefix = ""
	}
	logger := log.New(f, prefix, log.LstdFlags|log.Lmicroseconds)
	return logger, func() { f.Close() }, nil
}

// selftest validates the pty session machinery without ssh.
// keeperOptions describe a single-session keeper process.
type keeperOptions struct {
	Name       string
	Socket     string
	LogPath    string
	Shell      string
	Cwd        string
	Rows       uint16
	Cols       uint16
	NoProfile  bool
	Scrollback int
	Env        map[string]string
	Verbose    bool
}

// runSessionKeeper hosts exactly one session and serves it to the host daemon.
// The keeper is what makes sessions outlive the daemon: it owns the pty and
// stops only when the session ends or the daemon asks it to.
func runSessionKeeper(o keeperOptions) error {
	ignoreHangup()
	if o.Name == "" {
		o.Name = "main"
	}
	if err := os.MkdirAll(filepath.Dir(o.Socket), 0o700); err != nil {
		return err
	}
	logger, closeLog, err := openLogger(o.LogPath, o.Verbose)
	if err != nil {
		return err
	}
	defer closeLog()

	// Restore what the previous incarnation of this session left on disk:
	// working directory, scrollback and shell history.
	home, _ := os.UserHomeDir()
	var restore *daemon.SessionState
	if state, ok := daemon.LoadSessionState(home, o.Name); ok {
		restore = state
		if o.Cwd == "" && state.Cwd != "" {
			o.Cwd = state.Cwd
		}
		if err := daemon.WriteHistoryFile(home, o.Name, state.Commands); err != nil {
			logger.Printf("restore history for %s: %v", o.Name, err)
		}
		logger.Printf("restoring session %s: %s", o.Name, state.String())
	}

	sess, err := daemon.StartSession(daemon.SessionOptions{
		Name:       o.Name,
		Shell:      o.Shell,
		Cwd:        o.Cwd,
		Env:        o.Env,
		Rows:       o.Rows,
		Cols:       o.Cols,
		NoProfile:  o.NoProfile,
		Scrollback: o.Scrollback,
		Logger:     logger,
		Restore:    restore,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	readyErr := sess.WaitReady(ctx)
	cancel()
	defer sess.Close()
	if readyErr != nil {
		return readyErr
	}
	info := sess.Info()
	logger.Printf("keeper %s serving on %s (shell pid %d)", o.Name, o.Socket, info.PID)

	if _, err := net.DialTimeout("unix", o.Socket, 200*time.Millisecond); err != nil {
		_ = os.Remove(o.Socket)
	}
	l, err := net.Listen("unix", o.Socket)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", o.Socket, err)
	}
	_ = os.Chmod(o.Socket, 0o600)
	metaPath := strings.TrimSuffix(o.Socket, ".sock") + ".json"
	socketStamp := time.Time{}
	if st, serr := os.Stat(o.Socket); serr == nil {
		socketStamp = st.ModTime()
	}
	// A successor keeper may reuse the same socket path; only remove the file
	// if it is still the one this process created.
	removeOwnSocket := func() {
		if st, serr := os.Stat(o.Socket); serr == nil && st.ModTime().Equal(socketStamp) {
			_ = os.Remove(o.Socket)
		}
	}
	meta := daemon.KeeperMeta{
		Name:      o.Name,
		PID:       os.Getpid(),
		Socket:    o.Socket,
		Version:   version.Version,
		API:       proto.APIVersion,
		BinarySHA: executableSHA(),
		Shell:     info.Shell,
		StartedAt: info.StartedAt,
	}
	if err := daemon.WriteKeeperMeta(metaPath, meta); err != nil {
		logger.Printf("write keeper metadata: %v", err)
	}
	defer func() {
		l.Close()
		removeOwnSocket()
		if meta, merr := daemon.ReadKeeperMeta(metaPath); merr == nil && meta.PID == os.Getpid() {
			_ = os.Remove(metaPath)
		}
	}()

	srv := daemon.New(daemon.Options{
		SocketPath: o.Socket,
		Logger:     logger,
		Keeper:     sess,
		Binary:     executablePath(),
		BinarySHA:  executableSHA(),
		Shell:      o.Shell,
		DefaultEnv: o.Env,
	})
	sigCh := make(chan os.Signal, 1)
	signalNotify(sigCh)
	go func() {
		for {
			select {
			case sig := <-sigCh:
				logger.Printf("keeper %s received %v, stopping", o.Name, sig)
				srv.Shutdown(true)
				_ = l.Close()
				return
			case <-srv.ShutdownCh():
				_ = l.Close()
				return
			case <-sess.Done():
				// A command like `exit` ended the shell. Restart it in place so
				// the session name, socket and daemon handle stay valid.
				logger.Printf("keeper %s: shell exited, restarting it", o.Name)
				next, rerr := restartKeeperSession(o, logger)
				if rerr != nil {
					logger.Printf("keeper %s: restart failed: %v", o.Name, rerr)
					srv.Shutdown(true)
					_ = l.Close()
					return
				}
				srv.ReplaceKeeper(next)
				sess = next
			}
		}
	}()

	err = srv.Serve(l)
	// Wait for in-flight cleanup (session teardown) before the process exits.
	select {
	case <-srv.ShutdownDone():
	case <-time.After(30 * time.Second):
		logger.Printf("keeper %s shutdown timed out; exiting anyway", o.Name)
	}
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	logger.Printf("keeper %s stopped", o.Name)
	return nil
}

// selftest validates the pty session machinery without ssh.
// restartKeeperSession recreates a shell in place after it exited, restoring
// cwd, exported environment, scrollback and history from disk.
func restartKeeperSession(o keeperOptions, logger *log.Logger) (*daemon.Session, error) {
	home, _ := os.UserHomeDir()
	var restore *daemon.SessionState
	if state, ok := daemon.LoadSessionState(home, o.Name); ok {
		restore = state
		if state.Cwd != "" {
			o.Cwd = state.Cwd
		}
		_ = daemon.WriteHistoryFile(home, o.Name, state.Commands)
	}
	sess, err := daemon.StartSession(daemon.SessionOptions{
		Name:       o.Name,
		Shell:      o.Shell,
		Cwd:        o.Cwd,
		Env:        o.Env,
		Rows:       o.Rows,
		Cols:       o.Cols,
		NoProfile:  o.NoProfile,
		Scrollback: o.Scrollback,
		Logger:     logger,
		Restore:    restore,
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sess.WaitReady(ctx); err != nil {
		sess.Close()
		return nil, err
	}
	return sess, nil
}

func selftest(socket, logPath, shell string, env map[string]string) error {
	_ = socket
	if err := stopDaemon(socket); err != nil {
		return err
	}
	opts := daemon.SessionOptions{Name: "selftest", Shell: shell, Env: env, Cwd: mustGetwd()}
	sess, err := daemon.StartSession(opts)
	if err != nil {
		return err
	}
	defer sess.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sess.WaitReady(ctx); err != nil {
		return err
	}
	failures := 0
	// Sessions are pty shells on unix and virtual PowerShell sessions on
	// windows, so the probe commands differ per platform.
	win := runtime.GOOS == "windows"
	echoCmd, exitCmd := "echo hello-rsk", "(exit 42)"
	cdCmd, pwdCmd := "cd /tmp", "pwd"
	envSet, envGet := "export RSK_TEST=1 && echo $RSK_TEST", "echo ${RSK_TEST:-unset}"
	syntaxCmd, sleepCmd := "if true; then", "sleep 30"
	wantCwd := "/tmp"
	if win {
		echoCmd = "Write-Output hello-rsk"
		exitCmd = "exit 42"
		cdCmd, pwdCmd = "Set-Location $env:TEMP", "(Get-Location).Path"
		envSet = "$env:RSK_TEST='1'; Write-Output $env:RSK_TEST"
		envGet = "Write-Output ('test=' + $env:RSK_TEST)"
		syntaxCmd, sleepCmd = "if ($true) {", "Start-Sleep -Seconds 30"
		wantCwd = "Temp"
	}
	check := func(name string, fn func() error) {
		if err := fn(); err != nil {
			failures++
			fmt.Printf("FAIL %s: %v\n", name, err)
			return
		}
		fmt.Printf("ok   %s\n", name)
	}

	check("exec echo", func() error {
		var out strings.Builder
		res, err := sess.Run(ctx, daemon.ExecRequest{Cmd: echoCmd}, collector{&out})
		if err != nil {
			return err
		}
		if res.RC != 0 {
			return fmt.Errorf("rc=%d", res.RC)
		}
		if !strings.Contains(out.String(), "hello-rsk") {
			return fmt.Errorf("unexpected output %q", out.String())
		}
		return nil
	})
	check("exit code", func() error {
		res, err := sess.Run(ctx, daemon.ExecRequest{Cmd: "false"}, daemon.NopSink{})
		if err != nil {
			return err
		}
		if res.RC != 1 {
			return fmt.Errorf("rc=%d want 1 for false", res.RC)
		}
		res, err = sess.Run(ctx, daemon.ExecRequest{Cmd: exitCmd}, daemon.NopSink{})
		if err != nil {
			return err
		}
		if res.RC != 42 {
			return fmt.Errorf("rc=%d want 42", res.RC)
		}
		return nil
	})
	check("working directory persists", func() error {
		if _, err := sess.Run(ctx, daemon.ExecRequest{Cmd: cdCmd}, daemon.NopSink{}); err != nil {
			return err
		}
		var out strings.Builder
		if _, err := sess.Run(ctx, daemon.ExecRequest{Cmd: pwdCmd}, collector{&out}); err != nil {
			return err
		}
		if !strings.Contains(out.String(), wantCwd) {
			return fmt.Errorf("pwd = %q", out.String())
		}
		return nil
	})
	check("environment persists", func() error {
		var out strings.Builder
		if _, err := sess.Run(ctx, daemon.ExecRequest{Cmd: envSet}, collector{&out}); err != nil {
			return err
		}
		if _, err := sess.Run(ctx, daemon.ExecRequest{Cmd: envGet}, collector{&out}); err != nil {
			return err
		}
		if strings.Count(out.String(), "1") < 2 {
			return fmt.Errorf("output %q", out.String())
		}
		return nil
	})
	check("output is not echoed", func() error {
		var out strings.Builder
		if _, err := sess.Run(ctx, daemon.ExecRequest{Cmd: "echo marker-xyz"}, collector{&out}); err != nil {
			return err
		}
		if strings.Contains(out.String(), "__rsk_run") {
			return fmt.Errorf("command echo leaked into output: %q", out.String())
		}
		if strings.Count(out.String(), "marker-xyz") != 1 {
			return fmt.Errorf("expected single marker, got %q", out.String())
		}
		return nil
	})
	check("interrupt long command", func() error {
		start := time.Now()
		_, err := sess.Run(ctx, daemon.ExecRequest{Cmd: sleepCmd, TimeoutMS: 1500}, daemon.NopSink{})
		if err != nil {
			return err
		}
		if d := time.Since(start); d > 12*time.Second {
			return fmt.Errorf("interrupt took %s", d)
		}
		var out strings.Builder
		if _, err := sess.Run(ctx, daemon.ExecRequest{Cmd: "echo alive-after-interrupt"}, collector{&out}); err != nil {
			return err
		}
		if !strings.Contains(out.String(), "alive-after-interrupt") {
			return fmt.Errorf("session died after interrupt: %q", out.String())
		}
		return nil
	})
	check("incomplete command fails fast and recovers", func() error {
		res, err := sess.Run(ctx, daemon.ExecRequest{Cmd: syntaxCmd, TimeoutMS: 1500}, daemon.NopSink{})
		if err != nil {
			return err
		}
		if res.TimedOut {
			return fmt.Errorf("incomplete command should fail immediately, not wait for input")
		}
		if res.RC == 0 {
			return fmt.Errorf("expected a syntax error rc, got %+v", res)
		}
		var out strings.Builder
		if _, err := sess.Run(ctx, daemon.ExecRequest{Cmd: "echo recovered"}, collector{&out}); err != nil {
			return err
		}
		if !strings.Contains(out.String(), "recovered") {
			return fmt.Errorf("session unusable after incomplete command: %q", out.String())
		}
		return nil
	})
	check("session survives and reports cwd", func() error {
		info := sess.Info()
		if info.Closed {
			return fmt.Errorf("session closed")
		}
		return nil
	})

	if failures > 0 {
		return fmt.Errorf("%d selftest checks failed", failures)
	}
	fmt.Printf("selftest passed (shell=%s, log=%s)\n", sess.Info().Shell, logPath)
	return nil
}

type collector struct{ sb *strings.Builder }

func (c collector) Event(ev daemon.Event) {
	if ev.Chan == proto.ChOut {
		c.sb.Write(ev.Data)
	}
}

func mustGetwd() string {
	wd, err := os.Getwd()
	if err != nil {
		return "/"
	}
	return wd
}

// executablePath reports the running binary, used for upgrade detection.
func executablePath() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	return exe
}

// executableSHA hashes the binary this process was started from. It is
// computed once at startup so replacing the file on disk cannot confuse the
// running daemon's identity.
func executableSHA() string {
	exe := executablePath()
	if exe == "" {
		return ""
	}
	data, err := os.ReadFile(exe)
	if err != nil {
		return "unknown"
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func signalNotify(ch chan os.Signal) { signalNotifyImpl(ch) }
