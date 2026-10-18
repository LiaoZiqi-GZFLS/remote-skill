package daemon

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creack/pty"

	"remote-skill/internal/proto"
)

// Status channel: the shell writes "<exec-id>\t<rc>\t<cwd>\n" to this file
// descriptor of the daemon-owned pipe. It is independent from the terminal
// stream, so command output can never be confused with control data.
const (
	statusFD  = 3
	initID    = "__rsk_init"
	shellExit = "__rsk_shell_exit"
)

// Errors returned by the session layer.
var (
	ErrSessionBusy   = proto.Errorf(proto.CodeBusy, "session is busy running another command")
	ErrSessionClosed = proto.Errorf(proto.CodeClosed, "session is closed")
)

// SessionOptions describes how a persistent shell session is created.
type SessionOptions struct {
	Name       string            `json:"name"`
	Shell      string            `json:"shell,omitempty"`
	Cwd        string            `json:"cwd,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	Rows       uint16            `json:"rows,omitempty"`
	Cols       uint16            `json:"cols,omitempty"`
	NoProfile  bool              `json:"no_profile,omitempty"`
	Scrollback int               `json:"scrollback,omitempty"`
	// Logger is provided by the server and never sent over the wire.
	Logger *log.Logger `json:"-"`
	// Restore, when set, seeds a fresh shell with the state of a previous
	// incarnation of the same session (cwd, scrollback, history).
	Restore *SessionState `json:"-"`
}

func (o SessionOptions) applyDefaults() SessionOptions {
	if o.Name == "" {
		o.Name = "main"
	}
	if o.Shell == "" {
		o.Shell = "/bin/bash"
	}
	if o.Rows == 0 {
		o.Rows = 50
	}
	if o.Cols == 0 {
		o.Cols = 200
	}
	if o.Scrollback <= 0 {
		o.Scrollback = 512 << 10
	}
	return o
}

// Event is a chunk of session output or a session level notification.
type Event struct {
	Session string
	Chan    string
	Data    []byte
	Seq     int64
	ExecID  string
	// Extra carries channel specific metadata (for example the connection id
	// of a multiplexed reverse tunnel).
	Extra map[string]any
}

// Sink consumes events produced by a session operation.
type Sink interface {
	Event(ev Event)
}

// NopSink drops every event.
type NopSink struct{}

// Event implements Sink.
func (NopSink) Event(Event) {}

// ExecRequest is a single command to run inside a session.
type ExecRequest struct {
	Session   string            `json:"session,omitempty"`
	Cmd       string            `json:"cmd"`
	TimeoutMS int64             `json:"timeout_ms,omitempty"`
	WaitMS    int64             `json:"wait_ms,omitempty"`
	Cwd       string            `json:"cwd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
}

// ExecResult is the outcome of a command.
type ExecResult struct {
	ID          string    `json:"id"`
	Session     string    `json:"session"`
	Cmd         string    `json:"cmd"`
	RC          int       `json:"rc"`
	TimedOut    bool      `json:"timed_out,omitempty"`
	Interrupted bool      `json:"interrupted,omitempty"`
	Unfinished  bool      `json:"unfinished,omitempty"`
	ShellExited bool      `json:"shell_exited,omitempty"`
	DurationMS  int64     `json:"duration_ms"`
	Bytes       int64     `json:"bytes"`
	Cwd         string    `json:"cwd"`
	StartedAt   time.Time `json:"started_at"`
}

type execState struct {
	id       string
	cmd      string
	started  time.Time
	startSeq int64
	done     chan struct{}
	rc       int
	timedOut bool
	killed   bool
	cwd      string
	once     sync.Once
}

func (e *execState) finish(rc int, cwd string) {
	e.once.Do(func() {
		e.rc = rc
		e.cwd = cwd
		close(e.done)
	})
}

type subscriber struct {
	id    int64
	ch    chan Event
	dead  chan struct{}
	once  sync.Once
	drops atomic.Int64
}

func (s *subscriber) close() {
	s.once.Do(func() { close(s.dead) })
}

// Session is a long lived shell attached to a pty. It survives client
// disconnects: output is buffered and commands keep running.
type Session struct {
	name string
	opts SessionOptions

	startedAt time.Time
	doneCh    chan struct{}

	mu       sync.Mutex
	cmd      *exec.Cmd
	ptmx     *os.File
	statusR  *os.File
	statusW  *os.File
	closed   bool
	exitErr  error
	initDone chan struct{}
	initErr  error
	lastOut  atomic.Int64
	waitDone chan struct{}
	waitErr  error
	// initComplete gates output publishing: bytes produced while the shell is
	// still echoing our init script, or printing the login prompt, must never
	// reach clients or the scrollback.
	initComplete atomic.Bool

	ring    *Ring
	seq     int64
	subs    map[int64]*subscriber
	nextSub int64

	token      chan struct{}
	execs      map[string]*execState
	cur        *execState
	history    []ExecResult
	lastActive time.Time
	cmdCount   int
	cwd        string
	logger     *log.Logger
	home       string
	dirty      atomic.Bool
	restoredAt *time.Time
	lastSave   atomic.Int64
	// Windows virtual sessions keep no shell process; these track the state
	// that stands in for one.
	winShell string
	winEnv   map[string]string
}

func (s *Session) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf("session %s: "+format, append([]any{s.name}, args...)...)
	}
}

// Start launches a new session.
func StartSession(opts SessionOptions) (*Session, error) {
	opts = opts.applyDefaults()
	s := &Session{
		name:       opts.Name,
		opts:       opts,
		startedAt:  time.Now(),
		doneCh:     make(chan struct{}),
		initDone:   make(chan struct{}),
		waitDone:   make(chan struct{}),
		ring:       NewRing(opts.Scrollback),
		subs:       map[int64]*subscriber{},
		token:      make(chan struct{}, 1),
		execs:      map[string]*execState{},
		cwd:        opts.Cwd,
		lastActive: time.Now(),
		logger:     opts.Logger,
		home:       homeDir(),
	}
	if opts.Restore != nil {
		if s.cwd == "" && opts.Restore.Cwd != "" {
			s.cwd = opts.Restore.Cwd
		}
		if !opts.Restore.SavedAt.IsZero() {
			at := opts.Restore.SavedAt
			s.restoredAt = &at
		}
	}
	if err := s.start(); err != nil {
		return nil, err
	}
	if opts.Restore != nil && len(opts.Restore.Scrollback) > 0 {
		s.seedScrollback(opts.Restore.Scrollback, opts.Restore.SavedAt)
	}
	go s.stateSaver()
	return s, nil
}

func homeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return home
}

func (s *Session) start() error {
	if !nativePTY {
		return s.startNative()
	}
	statusR, statusW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create status pipe: %w", err)
	}
	s.statusR, s.statusW = statusR, statusW

	shell := s.opts.Shell
	flags := "-l -i"
	if s.opts.NoProfile {
		flags = "-i --norc"
	}
	// The outer sh disables terminal echo before the interactive shell starts
	// so the daemon never reads back the commands it writes.
	launch := fmt.Sprintf("stty raw -echo 2>/dev/null; exec %s %s", shell, flags)
	cmd := exec.Command("/bin/sh", "-c", launch)
	cmd.Env = s.buildEnv()
	if s.opts.Cwd != "" {
		if st, err := os.Stat(s.opts.Cwd); err == nil && st.IsDir() {
			cmd.Dir = s.opts.Cwd
		}
	}
	cmd.ExtraFiles = []*os.File{statusW} // becomes fd 3 in the shell

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: s.opts.Rows, Cols: s.opts.Cols})
	if err != nil {
		statusR.Close()
		statusW.Close()
		return fmt.Errorf("start shell: %w", err)
	}
	statusW.Close() // keep only the shell's copy

	s.mu.Lock()
	s.cmd = cmd
	s.ptmx = ptmx
	s.mu.Unlock()

	go s.readLoop(ptmx)
	go s.statusLoop(bufio.NewReaderSize(statusR, 64<<10))
	go s.waitLoop()

	if err := s.writeInit(); err != nil {
		s.Close()
		return err
	}
	return nil
}

func (s *Session) buildEnv() []string {
	env := os.Environ()
	preset := map[string]string{
		"TERM":                "xterm-256color",
		"PS1":                 "",
		"PAGER":               "cat",
		"GIT_PAGER":           "cat",
		"SYSTEMD_PAGER":       "cat",
		"GIT_TERMINAL_PROMPT": "0",
		"LESS":                "FRX",
		"RSK":                 "1",
		"RSK_SESSION":         s.name,
	}
	seen := map[string]bool{}
	for _, kv := range env {
		if i := strings.IndexByte(kv, '='); i > 0 {
			seen[kv[:i]] = true
		}
	}
	for k, v := range preset {
		if !seen[k] {
			env = append(env, k+"="+v)
		}
	}
	for k, v := range s.opts.Env {
		env = append(env, k+"="+v)
	}
	env = append(env, "RSK_SESSION="+s.name)
	// Set HISTFILE from the environment so bash loads the persisted history at
	// startup (setting it later, from the init script, would be too late).
	if s.home != "" {
		hist := filepath.Join(s.home, ".rsk", "history", sanitizeName(s.name))
		env = append(env, "HISTFILE="+hist)
	}
	return env
}

// writeInit seeds the shell with the helper functions the daemon relies on.
func (s *Session) writeInit() error {
	name := sanitizeName(s.name)
	lines := []string{
		// HISTCONTROL=ignorespace lets every line we write start with a space,
		// so the init preamble and the command wrappers never end up in the
		// user's shell history (commands typed in `rsk sh` still are).
		"HISTCONTROL=ignorespace; export HISTCONTROL",
		"stty raw -echo 2>/dev/null",
		"PS1='' PS2='' PS3='' PS4=''; PROMPT_COMMAND=''",
		"{ command -v bind >/dev/null 2>&1 && bind 'set bell-style none' 'set enable-bracketed-paste off' >/dev/null 2>&1; } || true",
		"mkdir -p \"$HOME/.rsk/history\" \"$HOME/.rsk/state\" 2>/dev/null",
		"HISTFILE=\"$HOME/.rsk/history/" + name + "\" 2>/dev/null || true",
		"[ -f \"$HOME/.rsk/state/" + name + ".env\" ] && . \"$HOME/.rsk/state/" + name + ".env\" >/dev/null 2>&1 || true",
		"__rsk_emit() { printf '%s\\t%d\\t%s\\n' \"$1\" \"$2\" \"$PWD\" >&" + strconv.Itoa(statusFD) + "; }",
		`__rsk_run() { local __i="$1" __c="$2" __d="$3" __r=0; if [ -n "$__d" ]; then cd "$__d" 2>/dev/null || { __rsk_emit "$__i" 127; return; }; fi; eval "$__c"; __r=$?; export -p >"$HOME/.rsk/state/` + name + `.env" 2>/dev/null; __rsk_emit "$__i" "$__r"; }`,
		"__rsk_emit " + initID + " 0",
		"",
	}
	script := " " + strings.Join(lines, "\n ")
	_, err := s.ptmx.WriteString(script)
	if err != nil {
		return fmt.Errorf("write session init: %w", err)
	}
	return nil
}

func (s *Session) readLoop(ptmx *os.File) {
	buf := make([]byte, 32<<10)
	for {
		n, err := ptmx.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			s.publish(chunk)
		}
		if err != nil {
			s.markClosed(fmt.Errorf("terminal closed: %w", err))
			return
		}
	}
}

func (s *Session) statusLoop(r *bufio.Reader) {
	for {
		line, err := r.ReadString('\n')
		if line != "" {
			s.handleStatus(strings.TrimRight(line, "\r\n"))
		}
		if err != nil {
			return
		}
	}
}

func (s *Session) handleStatus(line string) {
	parts := strings.SplitN(line, "\t", 3)
	if len(parts) < 2 {
		return
	}
	id := parts[0]
	rc, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return
	}
	cwd := ""
	if len(parts) > 2 {
		cwd = parts[2]
	}
	s.mu.Lock()
	if cwd != "" {
		s.cwd = cwd
	}
	st := s.execs[id]
	if st != nil {
		delete(s.execs, id)
	}
	s.mu.Unlock()

	switch id {
	case initID:
		s.initComplete.Store(true)
		s.logf("shell ready (cwd %s)", cwd)
		close(s.initDone)
	case shellExit:
	default:
		if st != nil {
			s.logf("exec %s finished rc=%d cwd=%s", id, rc, cwd)
			st.finish(rc, cwd)
		} else {
			s.logf("status for unknown exec %s rc=%d", id, rc)
		}
	}
}

func (s *Session) waitLoop() {
	err := s.cmd.Wait()
	s.mu.Lock()
	s.waitErr = err
	s.mu.Unlock()
	close(s.waitDone)
	s.markClosed(err)
}

func (s *Session) markClosed(err error) {
	// The pty usually reports EOF before wait(2) delivers the exit status.
	// Prefer the real status so a command that exits the shell reports its
	// own exit code.
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		select {
		case <-s.waitDone:
			s.mu.Lock()
			if s.waitErr != nil {
				err = s.waitErr
			}
			s.mu.Unlock()
		case <-time.After(2 * time.Second):
		}
	}
	s.logf("shell exit status: %v", err)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.logf("shell exited: %v", err)
	s.closed = true
	s.exitErr = err
	ptmx := s.ptmx
	statusR := s.statusR
	pending := make([]*execState, 0, len(s.execs))
	for _, st := range s.execs {
		pending = append(pending, st)
	}
	s.execs = map[string]*execState{}
	s.mu.Unlock()

	if ptmx != nil {
		ptmx.Close()
	}
	if statusR != nil {
		statusR.Close()
	}
	// A command that exits the shell still has a meaningful status: the
	// shell's own exit code.
	rc := -1
	var statusErr *exec.ExitError
	if errors.As(err, &statusErr) {
		rc = statusErr.ExitCode()
	}
	for _, st := range pending {
		st.finish(rc, "")
	}
	close(s.doneCh)
	s.publishEvent(Event{Session: s.name, Chan: proto.ChMeta, Data: []byte("session_exit")})
	// Last chance to persist what happened in this incarnation.
	s.saveState()
}

// stateSaver periodically persists session state so a machine crash (or a
// SIGKILL) still leaves something to restore.
func (s *Session) stateSaver() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.doneCh:
			return
		case <-ticker.C:
			if s.dirty.Swap(false) {
				s.lastSave.Store(time.Now().UnixNano())
				s.saveState()
			}
		}
	}
}

// maybeSaveState persists state after a command, throttled so a busy session
// does not write the scrollback on every single command.
func (s *Session) maybeSaveState() {
	now := time.Now()
	if now.Sub(time.Unix(0, s.lastSave.Load())) < 2*time.Second {
		s.dirty.Store(true)
		return
	}
	s.lastSave.Store(now.UnixNano())
	s.dirty.Store(false)
	s.saveState()
}

// saveState writes cwd, recent commands and scrollback to disk.
func (s *Session) saveState() {
	if s.home == "" {
		return
	}
	s.mu.Lock()
	st := SessionState{
		Name:       s.name,
		Shell:      s.opts.Shell,
		Cwd:        s.cwd,
		StartedAt:  s.startedAt,
		LastActive: s.lastActive,
	}
	if n := len(s.history); n > 0 {
		from := 0
		if n > maxStateCommands {
			from = n - maxStateCommands
		}
		st.Commands = append(st.Commands, s.history[from:]...)
	}
	s.mu.Unlock()
	st.Scrollback = s.ring.Tail(maxStateScrollback)
	if err := SaveSessionState(s.home, st); err != nil {
		s.logf("save session state: %v", err)
	}
}

// seedScrollback prepends restored output to the live scrollback with a marker
// so readers can tell where the old incarnation ended.
func (s *Session) seedScrollback(data []byte, savedAt time.Time) {
	marker := "\n--- rsk: session restored"
	if !savedAt.IsZero() {
		marker += " from " + savedAt.Format("2006-01-02 15:04:05")
	}
	if s.cwd != "" {
		marker += ", cwd " + s.cwd
	}
	marker += " ---\n"
	s.ring.Write(data)
	s.ring.Write([]byte(marker))
}

// publish fans a chunk of terminal output out to the ring buffer and all
// live subscribers.
func (s *Session) publish(chunk []byte) {
	s.lastOut.Store(time.Now().UnixNano())
	if !s.initComplete.Load() {
		return
	}
	s.dirty.Store(true)
	s.ring.Write(chunk)
	s.mu.Lock()
	s.seq += int64(len(chunk))
	seq := s.seq
	execID := ""
	if s.cur != nil {
		execID = s.cur.id
	}
	subs := make([]*subscriber, 0, len(s.subs))
	for _, sub := range s.subs {
		subs = append(subs, sub)
	}
	s.mu.Unlock()

	ev := Event{Session: s.name, Chan: proto.ChOut, Data: chunk, Seq: seq, ExecID: execID}
	for _, sub := range subs {
		sub.deliver(ev)
	}
}

func (s *Session) publishEvent(ev Event) {
	s.mu.Lock()
	subs := make([]*subscriber, 0, len(s.subs))
	for _, sub := range s.subs {
		subs = append(subs, sub)
	}
	s.mu.Unlock()
	for _, sub := range subs {
		sub.deliver(ev)
	}
}

// deliver pushes an event to a subscriber, applying coarse back pressure so a
// stalled client cannot corrupt the terminal stream.
func (sub *subscriber) deliver(ev Event) {
	select {
	case <-sub.dead:
		return
	default:
	}
	select {
	case sub.ch <- ev:
		return
	default:
	}
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case sub.ch <- ev:
	case <-sub.dead:
	case <-timer.C:
		sub.drops.Add(1)
		sub.close()
	}
}

// subscribe registers a live output subscription. When replay > 0 the tail of
// the scrollback is delivered first.
func (s *Session) subscribe(replay int) (*subscriber, error) {
	sub := &subscriber{ch: make(chan Event, 1024), dead: make(chan struct{})}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrSessionClosed
	}
	s.nextSub++
	sub.id = s.nextSub
	s.subs[sub.id] = sub
	s.mu.Unlock()
	if replay > 0 {
		if tail := s.ring.Tail(replay); len(tail) > 0 {
			sub.ch <- Event{Session: s.name, Chan: proto.ChOut, Data: tail}
		}
	}
	return sub, nil
}

func (s *Session) unsubscribe(sub *subscriber) {
	s.mu.Lock()
	delete(s.subs, sub.id)
	s.mu.Unlock()
	sub.close()
}

// Name returns the session name.
func (s *Session) Name() string { return s.name }

// SessionInfo is a JSON friendly session description.
type SessionInfo struct {
	Name        string      `json:"name"`
	Shell       string      `json:"shell"`
	PID         int         `json:"pid"`
	Cwd         string      `json:"cwd"`
	Busy        bool        `json:"busy"`
	Closed      bool        `json:"closed"`
	StartedAt   time.Time   `json:"started_at"`
	LastActive  time.Time   `json:"last_active"`
	Commands    int         `json:"commands"`
	Scrollback  int         `json:"scrollback_bytes"`
	Subscribers int         `json:"subscribers"`
	Last        *ExecResult `json:"last_exec,omitempty"`
	RestoredAt  *time.Time  `json:"restored_at,omitempty"`
}

// Info describes the session.
func (s *Session) Info() SessionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	pid := 0
	if s.cmd != nil && s.cmd.Process != nil {
		pid = s.cmd.Process.Pid
	}
	var last *ExecResult
	if n := len(s.history); n > 0 {
		res := s.history[n-1]
		last = &res
	}
	shell := s.opts.Shell
	if !nativePTY && s.winShell != "" {
		shell = s.winShell
	}
	return SessionInfo{
		Name:        s.name,
		Shell:       shell,
		PID:         pid,
		Cwd:         s.cwd,
		Busy:        s.cur != nil,
		Closed:      s.closed,
		StartedAt:   s.startedAt,
		LastActive:  s.lastActive,
		Commands:    s.cmdCount,
		Scrollback:  s.ring.Len(),
		Subscribers: len(s.subs),
		Last:        last,
		RestoredAt:  s.restoredAt,
	}
}

// RecentCommands returns the tail of executed commands.
func (s *Session) RecentCommands(n int) []ExecResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n <= 0 || n > len(s.history) {
		n = len(s.history)
	}
	out := make([]ExecResult, n)
	copy(out, s.history[len(s.history)-n:])
	return out
}

// Scrollback returns the last n bytes of terminal output.
func (s *Session) Scrollback(n int) []byte { return s.ring.Tail(n) }

// StatePath is the persisted environment snapshot of this session.
func (s *Session) StatePath() string {
	home := s.home
	if home == "" {
		home = homeDir()
	}
	if home == "" {
		return ""
	}
	return filepath.Join(home, ".rsk", "state", sanitizeName(s.name)+".env")
}

// RemoveState drops the persisted state so an explicitly stopped session does
// not come back with the old variables or scrollback.
func (s *Session) RemoveState() {
	home := s.home
	if home == "" {
		home = homeDir()
	}
	if home == "" {
		return
	}
	RemoveSessionState(home, s.name)
}

// Terminate stops the shell and forgets the session's persisted state. It is
// what an explicit "stop this session" request means.
func (s *Session) Terminate() error {
	err := s.Close()
	s.RemoveState()
	return err
}

// Write sends raw bytes to the session terminal (used by interactive attach).
func (s *Session) Write(p []byte) error {
	s.mu.Lock()
	ptmx := s.ptmx
	closed := s.closed
	s.mu.Unlock()
	if closed || ptmx == nil {
		return ErrSessionClosed
	}
	s.lastActive = time.Now()
	_, err := ptmx.Write(p)
	return err
}

// Resize changes the pty window size and notifies the foreground job.
func (s *Session) Resize(rows, cols uint16) error {
	if !nativePTY {
		return nil
	}
	s.mu.Lock()
	ptmx := s.ptmx
	closed := s.closed
	s.mu.Unlock()
	if closed || ptmx == nil {
		return ErrSessionClosed
	}
	if rows == 0 || cols == 0 {
		return nil
	}
	if err := pty.Setsize(ptmx, &pty.Winsize{Rows: rows, Cols: cols}); err != nil {
		return err
	}
	s.SignalNoErr("WINCH")
	return nil
}

// Signal delivers a signal to the foreground process group of the terminal.
func (s *Session) Signal(name string) error {
	if !nativePTY {
		return s.signalNative(name)
	}
	upper := strings.ToUpper(strings.TrimPrefix(name, "SIG"))
	if _, err := signalNumber(upper); err != nil {
		return err
	}
	fg, ok := s.foregroundPgid()
	shell := s.shellPid()
	if shell == 0 {
		return ErrSessionClosed
	}
	if ok && fg > 0 && fg != shell {
		if err := killGroup(fg, upper); err != nil {
			if err2 := killProc(fg, upper); err2 != nil {
				return fmt.Errorf("signal %s: %v", upper, err)
			}
		}
		return nil
	}
	if ok && fg == shell {
		// The shell sits at a prompt (or a continuation prompt): a control
		// byte is the only thing readline understands.
		if b, has := controlByte(upper); has {
			return s.Write([]byte{b})
		}
		return proto.Errorf(proto.CodeConflict, "no foreground job to signal %s", upper)
	}
	// Without a usable pty foreground group fall back to the shell's group.
	// An interactive shell ignores SIGINT while it waits for a child.
	switch upper {
	case "INT", "QUIT", "HUP", "WINCH":
		if err := killGroup(shell, upper); err != nil {
			if err2 := killProc(shell, upper); err2 != nil {
				return fmt.Errorf("signal %s: %v", upper, err)
			}
		}
		return nil
	}
	return proto.Errorf(proto.CodeInternal, "cannot determine foreground process group")
}

func controlByte(name string) (byte, bool) {
	switch name {
	case "INT":
		return 0x03, true
	case "QUIT":
		return 0x1c, true
	case "TSTP", "SUSP":
		return 0x1a, true
	case "EOF":
		return 0x04, true
	}
	return 0, false
}

// SignalNoErr is Signal without error handling, for housekeeping signals.
func (s *Session) SignalNoErr(name string) { _ = s.Signal(name) }

// Close terminates the session shell.
func (s *Session) Close() error {
	if !nativePTY {
		return s.closeNative()
	}
	s.mu.Lock()
	cmd := s.cmd
	closed := s.closed
	ptmx := s.ptmx
	s.mu.Unlock()
	if closed {
		return nil
	}
	if ptmx != nil {
		// A polite hangup lets the shell run its own cleanup.
		_, _ = ptmx.WriteString("exit\n")
	}
	if cmd != nil && cmd.Process != nil {
		go func(pid int) {
			for i := 0; i < 30; i++ {
				time.Sleep(100 * time.Millisecond)
				if !processAlive(pid) {
					return
				}
			}
			_ = killGroup(pid, "KILL")
			_ = killProc(pid, "KILL")
		}(cmd.Process.Pid)
	}
	return nil
}

// Done is closed when the shell exits.
func (s *Session) Done() <-chan struct{} { return s.doneCh }

// isClosed reports whether the session shell has exited.
func (s *Session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// WaitReady blocks until the session init script has completed.
func (s *Session) WaitReady(ctx context.Context) error {
	select {
	case <-s.initDone:
		s.mu.Lock()
		err := s.initErr
		closed := s.closed
		exitErr := s.exitErr
		s.mu.Unlock()
		if err != nil {
			return err
		}
		if closed && exitErr != nil {
			return proto.Errorf(proto.CodeClosed, "session shell exited during startup: %v", exitErr).
				WithHint("the remote login shell may abort in non-interactive mode; retry with --no-profile")
		}
		return nil
	case <-s.doneCh:
		s.mu.Lock()
		exitErr := s.exitErr
		s.mu.Unlock()
		return proto.Errorf(proto.CodeClosed, "session shell exited during startup: %v", exitErr).
			WithHint("the remote login shell may abort in non-interactive mode; retry with --no-profile")
	case <-ctx.Done():
		return proto.Errorf(proto.CodeTimeout, "timed out waiting for session %q to become ready", s.name)
	}
}

func (s *Session) acquire(ctx context.Context, waitMS int64) error {
	deadline := time.Now().Add(time.Duration(waitMS) * time.Millisecond)
	for {
		select {
		case s.token <- struct{}{}:
			return nil
		default:
		}
		if waitMS <= 0 || time.Now().After(deadline) {
			return ErrSessionBusy
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Session) release() {
	select {
	case <-s.token:
	default:
	}
}

// Run executes a command inside the persistent shell and streams its output.
func (s *Session) Run(ctx context.Context, req ExecRequest, sink Sink) (*ExecResult, error) {
	if !nativePTY {
		return s.runNative(ctx, req, sink)
	}
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
	st.done = make(chan struct{})

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrSessionClosed
	}
	st.startSeq = s.seq
	s.execs[id] = st
	s.cur = st
	s.mu.Unlock()

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

	sink.Event(Event{
		Session: s.name,
		Chan:    proto.ChMeta,
		ExecID:  id,
		Data:    []byte("exec_start"),
	})

	script := s.buildScript(st, req)
	s.logf("exec %s start (timeout=%dms wait=%dms): %s", id, req.TimeoutMS, req.WaitMS, truncate(req.Cmd, 200))
	if _, err := s.ptmx.WriteString(script); err != nil {
		s.mu.Lock()
		delete(s.execs, id)
		s.cur = nil
		s.mu.Unlock()
		return nil, proto.Errorf(proto.CodeIO, "write to session terminal: %v", err)
	}

	timedOut, interrupted := false, false
	if req.TimeoutMS > 0 {
		timer := time.NewTimer(time.Duration(req.TimeoutMS) * time.Millisecond)
		select {
		case <-st.done:
			timer.Stop()
		case <-timer.C:
			timedOut = true
			s.interrupt()
			s.waitForDone(st, 5*time.Second)
		case <-ctx.Done():
			interrupted = true
			s.interrupt()
			s.waitForDone(st, 5*time.Second)
		}
	} else {
		select {
		case <-st.done:
		case <-ctx.Done():
			interrupted = true
			s.interrupt()
			s.waitForDone(st, 5*time.Second)
		}
	}

	// Give the pty a moment to flush trailing output before we report done.
	s.waitQuiet(60*time.Millisecond, 750*time.Millisecond)

	s.mu.Lock()
	delete(s.execs, id)
	if s.cur == st {
		s.cur = nil
	}
	endSeq := s.seq
	cwd := s.cwd
	s.mu.Unlock()

	s.unsubscribe(sub)
	<-delivered

	unfinished := false
	select {
	case <-st.done:
	default:
		unfinished = true
		s.logf("exec %s never reported completion; reporting rc=-1", id)
	}

	res := &ExecResult{
		ID:          id,
		Session:     s.name,
		Cmd:         req.Cmd,
		RC:          st.rc,
		TimedOut:    timedOut,
		Interrupted: interrupted,
		DurationMS:  time.Since(st.started).Milliseconds(),
		Bytes:       endSeq - st.startSeq,
		Cwd:         cwd,
		StartedAt:   st.started,
	}
	if unfinished {
		res.RC = -1
		res.Unfinished = true
	}
	if s.isClosed() {
		res.ShellExited = true
	}
	if st.cwd != "" {
		res.Cwd = st.cwd
	}

	s.mu.Lock()
	s.cmdCount++
	s.lastActive = time.Now()
	s.history = append(s.history, *res)
	if len(s.history) > 100 {
		s.history = s.history[len(s.history)-100:]
	}
	s.mu.Unlock()

	// Persist before telling the caller we are done, so the state on disk is at
	// least as recent as the completion they just saw.
	s.maybeSaveState()
	sink.Event(Event{Session: s.name, Chan: proto.ChDone, ExecID: id, Data: []byte("exec_done")})
	return res, nil
}

func (s *Session) waitQuiet(quiet, max time.Duration) {
	deadline := time.Now().Add(max)
	for {
		last := time.Unix(0, s.lastOut.Load())
		if time.Since(last) >= quiet || time.Now().After(deadline) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForDone waits for the command to finish, escalating to TERM and then
// KILL only when a foreground job (not the shell itself) is still running.
//
// An interactive bash discards the rest of the current input line when a
// foreground job dies from SIGINT, which means the status line the wrapper
// would normally write never arrives. Once the job is gone we therefore
// synthesise the completion ourselves so the session stays usable.
func (s *Session) waitForDone(st *execState, grace time.Duration) {
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		select {
		case <-st.done:
			return
		case <-time.After(100 * time.Millisecond):
		}
		fg, ok := s.foregroundPgid()
		if ok && fg == s.shellPid() {
			// The job is gone: give the shell a moment to report the status
			// line, otherwise complete it on its behalf.
			select {
			case <-st.done:
				return
			case <-time.After(250 * time.Millisecond):
			}
			s.synthesize(st, 130, "interrupted")
			return
		}
	}
	fg, ok := s.foregroundPgid()
	s.logf("exec %s unfinished after interrupt (fg=%d ok=%v shell=%d)", st.id, fg, ok, s.shellPid())
	if !ok || fg <= 0 || fg == s.shellPid() {
		// The job is already gone but the shell swallowed the status line.
		s.synthesize(st, 130, "interrupted")
		return
	}
	_ = killGroup(fg, "TERM")
	s.logf("exec %s escalating to TERM on pgid %d", st.id, fg)
	select {
	case <-st.done:
		return
	case <-time.After(2 * time.Second):
	}
	if again, ok := s.foregroundPgid(); ok && again == fg {
		_ = killGroup(fg, "KILL")
		s.logf("exec %s escalating to KILL on pgid %d", st.id, fg)
		select {
		case <-st.done:
			return
		case <-time.After(2 * time.Second):
		}
		if again, ok := s.foregroundPgid(); ok && again == fg {
			s.logf("exec %s still holds the terminal after KILL", st.id)
			return
		}
	}
	s.synthesize(st, 137, "killed")
}

// synthesize completes an exec whose status line never made it back.
func (s *Session) synthesize(st *execState, rc int, reason string) {
	s.mu.Lock()
	cwd := s.cwd
	if s.cur == st {
		s.cur = nil
	}
	delete(s.execs, st.id)
	ptmx := s.ptmx
	s.mu.Unlock()
	s.logf("exec %s completed locally rc=%d (%s)", st.id, rc, reason)
	st.finish(rc, cwd)
	if ptmx != nil {
		// Leave the shell on a fresh line so the next command is not eaten.
		_, _ = ptmx.Write([]byte("\n"))
	}
}

// interrupt asks the running job to stop, and also aborts a half typed
// command line so the session never stays stuck at a continuation prompt.
func (s *Session) interrupt() {
	fg, ok := s.foregroundPgid()
	shell := s.shellPid()
	if ok && fg > 0 && fg != shell {
		killed := killGroup(fg, "INT") == nil
		s.logf("interrupt: fg=%d ok=%v shell=%d int_sent=%v", fg, ok, shell, killed)
		return
	}
	// No foreground job: the shell is sitting at a prompt (possibly a
	// continuation prompt), where a control byte aborts the pending line.
	s.logf("interrupt: fg=%d ok=%v shell=%d at_prompt", fg, ok, shell)
	s.mu.Lock()
	ptmx := s.ptmx
	s.mu.Unlock()
	if ptmx != nil {
		_, _ = ptmx.Write([]byte{0x03})
	}
}

// foregroundPgid reports the terminal's foreground process group.
func (s *Session) foregroundPgid() (int, bool) {
	s.mu.Lock()
	ptmx := s.ptmx
	closed := s.closed
	s.mu.Unlock()
	if closed || ptmx == nil {
		return 0, false
	}
	pgid, err := tcGetPgrp(int(ptmx.Fd()))
	if err != nil || pgid <= 0 {
		return 0, false
	}
	return pgid, true
}

// shellPid is the pid of the session shell (also its process group).
func (s *Session) shellPid() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd != nil && s.cmd.Process != nil {
		return s.cmd.Process.Pid
	}
	return 0
}

func (s *Session) buildScript(st *execState, req ExecRequest) string {
	var cmd strings.Builder
	for k, v := range req.Env {
		cmd.WriteString("export " + k + "=" + shQuote(v) + "; ")
	}
	cmd.WriteString(req.Cmd)
	// A leading space keeps the wrapper call itself out of the shell history.
	return fmt.Sprintf(" __rsk_run %s %s %s\n", shQuote(st.id), shQuote(cmd.String()), shQuote(req.Cwd))
}

// Attach streams the scrollback and live output; input written through the
// returned writer goes to the terminal.
func (s *Session) Attach(replay int) (*Attach, error) {
	if !nativePTY {
		return nil, proto.Errorf(proto.CodeBadRequest,
			"interactive attach needs a pty, which windows remotes do not provide; use `rsk exec` or `rsk logs`")
	}
	sub, err := s.subscribe(replay)
	if err != nil {
		return nil, err
	}
	return &Attach{s: s, sub: sub}, nil
}

// Attach is an interactive attachment to a session.
type Attach struct {
	s    *Session
	sub  *subscriber
	once sync.Once
}

// Events yields session output.
func (a *Attach) Events() <-chan Event { return a.sub.ch }

// Write forwards user input to the terminal.
func (a *Attach) Write(p []byte) (int, error) { return len(p), a.s.Write(p) }

// Resize updates the pty size.
func (a *Attach) Resize(rows, cols uint16) error { return a.s.Resize(rows, cols) }

// Signal forwards a signal to the foreground job.
func (a *Attach) Signal(name string) error { return a.s.Signal(name) }

// Close detaches from the session without killing it.
func (a *Attach) Close() error {
	a.once.Do(func() { a.s.unsubscribe(a.sub) })
	return nil
}

func shQuote(s string) string {
	if s == "" {
		return "''"
	}
	// Leave a leading ~ unquoted so the shell expands it.
	if s == "~" {
		return "~"
	}
	if strings.HasPrefix(s, "~/") {
		return "~" + shQuote(s[1:])
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// sanitizeName keeps session names usable as file names.
func sanitizeName(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			out = append(out, r)
		case r == '-' || r == '_' || r == '.' || r == '@':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		return "main"
	}
	return string(out)
}

// truncate shortens a command for logging.
func truncate(s string, n int) string {
	s = strings.ReplaceAll(strings.ReplaceAll(s, "\n", " ; "), "\t", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
