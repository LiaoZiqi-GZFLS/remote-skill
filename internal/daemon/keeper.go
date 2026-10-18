package daemon

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"remote-skill/internal/proto"
)

// KeeperMeta is written next to a keeper socket for diagnostics and cleanup.
type KeeperMeta struct {
	Name      string    `json:"name"`
	PID       int       `json:"pid"`
	Socket    string    `json:"socket"`
	Version   string    `json:"version"`
	API       int       `json:"api"`
	BinarySHA string    `json:"binary_sha"`
	Shell     string    `json:"shell,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

// WriteKeeperMeta persists keeper metadata.
func WriteKeeperMeta(path string, meta KeeperMeta) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// ReadKeeperMeta loads keeper metadata.
func ReadKeeperMeta(path string) (KeeperMeta, error) {
	var meta KeeperMeta
	data, err := os.ReadFile(path)
	if err != nil {
		return meta, err
	}
	err = json.Unmarshal(data, &meta)
	return meta, err
}

// KeeperManager owns the session keeper processes of one host daemon. Keepers
// outlive the daemon: that is what makes sessions persistent across daemon
// restarts, upgrades and crashes.
type KeeperManager struct {
	dir     string
	binary  string
	logPath string
	version string
	shell   string
	env     map[string]string
	logger  *log.Logger

	mu      sync.Mutex
	handles map[string]*keeperHandle
}

// NewKeeperManager prepares the manager. It adopts keepers that are already
// running so a restarted daemon picks the sessions back up.
func NewKeeperManager(dir, binary, logPath, version, shell string, env map[string]string, logger *log.Logger) *KeeperManager {
	m := &KeeperManager{
		dir:     dir,
		binary:  binary,
		logPath: logPath,
		version: version,
		shell:   shell,
		env:     env,
		logger:  logger,
		handles: map[string]*keeperHandle{},
	}
	go m.scan()
	return m
}

func (m *KeeperManager) logf(format string, args ...any) {
	if m.logger != nil {
		m.logger.Printf("keeper: "+format, args...)
	}
}

// Dir reports the directory holding keeper sockets.
func (m *KeeperManager) Dir() string { return m.dir }

func (m *KeeperManager) socketPath(name string) string {
	return filepath.Join(m.dir, sanitizeName(name)+".sock")
}

// MetaPath is the metadata file of a session keeper.
func (m *KeeperManager) MetaPath(name string) string {
	return filepath.Join(m.dir, sanitizeName(name)+".json")
}

func (m *KeeperManager) forget(name string, h *keeperHandle) {
	m.mu.Lock()
	if cur, ok := m.handles[name]; ok && cur == h {
		delete(m.handles, name)
	}
	m.mu.Unlock()
}

// ensure returns a live keeper for name, adopting or spawning as needed.
func (m *KeeperManager) ensure(name string, opts *SessionOptions, create bool) (*keeperHandle, error) {
	if name == "" {
		name = "main"
	}
	m.mu.Lock()
	h := m.handles[name]
	m.mu.Unlock()
	if h != nil && !h.isClosed() {
		return h, nil
	}
	if adopted, err := m.adopt(name); err == nil {
		return adopted, nil
	}
	if !create {
		return nil, proto.Errorf(proto.CodeNotFound, "session %q does not exist", name).
			WithHint("start it with `rsk up <host> --session " + name + "`")
	}
	return m.spawn(name, opts)
}

// adopt reconnects to an already running keeper.
func (m *KeeperManager) adopt(name string) (*keeperHandle, error) {
	socket := m.socketPath(name)
	nc, err := net.DialTimeout("unix", socket, time.Second)
	if err != nil {
		return nil, err
	}
	h, err := m.register(name, socket, nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	m.logf("adopted session %s from %s", name, socket)
	return h, nil
}

// register performs the handshake on an established keeper connection.
func (m *KeeperManager) register(name, socket string, nc net.Conn) (*keeperHandle, error) {
	pc := proto.NewConn(nc, nc)
	helloEnv, err := proto.NewRequest("hello", "", map[string]any{
		"version": m.version,
		"api":     proto.APIVersion,
		"client":  "rskd",
	})
	if err != nil {
		return nil, err
	}
	helloEnv.Type = proto.TypeHello
	if err := pc.Write(helloEnv); err != nil {
		return nil, err
	}
	welcome, err := pc.Read()
	if err != nil {
		return nil, err
	}
	if welcome.Err != nil {
		return nil, welcome.Err
	}
	var info HostInfo
	if err := welcome.DecodeResult(&info); err != nil {
		return nil, err
	}
	sess := SessionInfo{Name: name}
	if len(info.Sessions) > 0 {
		sess = info.Sessions[0]
	}
	h := &keeperHandle{
		mgr:    m,
		name:   name,
		socket: socket,
		pid:    info.PID,
		conn:   pc,
		routes: map[string]*proxyRoute{},
		info:   sess,
		infoAt: time.Now(),
		done:   make(chan struct{}),
	}
	m.mu.Lock()
	old := m.handles[name]
	m.handles[name] = h
	m.mu.Unlock()
	if old != nil && old != h {
		old.close()
	}
	go h.readLoop()
	return h, nil
}

// spawn starts a new keeper process and connects to it.
func (m *KeeperManager) spawn(name string, opts *SessionOptions) (*keeperHandle, error) {
	if m.binary == "" {
		return nil, proto.Errorf(proto.CodeInternal, "daemon does not know its own executable path")
	}
	if err := os.MkdirAll(m.dir, 0o700); err != nil {
		return nil, err
	}
	socket := m.socketPath(name)
	args := []string{"session", "--name", name, "--socket", socket, "--log", m.logPath}
	shell := m.shell
	if opts != nil {
		if opts.Shell != "" {
			shell = opts.Shell
		}
		if opts.Cwd != "" {
			args = append(args, "--cwd", opts.Cwd)
		}
		if opts.Rows > 0 && opts.Cols > 0 {
			args = append(args, "--rows", fmt.Sprint(opts.Rows), "--cols", fmt.Sprint(opts.Cols))
		}
		if opts.NoProfile {
			args = append(args, "--no-profile")
		}
		if opts.Scrollback > 0 {
			args = append(args, "--scrollback", fmt.Sprint(opts.Scrollback))
		}
	}
	if shell != "" {
		args = append(args, "--shell", shell)
	}
	for k, v := range m.env {
		args = append(args, "--env", k+"="+v)
	}
	if opts != nil {
		for k, v := range opts.Env {
			args = append(args, "--env", k+"="+v)
		}
	}
	logFile, err := os.OpenFile(m.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	defer logFile.Close()
	cmd := exec.Command(m.binary, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = detachAttr()
	if err := cmd.Start(); err != nil {
		return nil, proto.Errorf(proto.CodeInternal, "start session keeper: %v", err)
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	m.logf("started session keeper %s (pid %d)", name, pid)

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		h, err := m.adopt(name)
		if err == nil {
			h.mu.Lock()
			if h.pid == 0 {
				h.pid = pid
			}
			h.mu.Unlock()
			return h, nil
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	return nil, proto.Errorf(proto.CodeInternal, "session %q did not start: %v", name, lastErr).
		WithHint("check " + m.logPath)
}

// info returns cached session state, refreshing it when stale.
func (m *KeeperManager) info(name string, refresh bool) (SessionInfo, bool) {
	if name == "" {
		name = "main"
	}
	m.mu.Lock()
	h := m.handles[name]
	m.mu.Unlock()
	if h == nil {
		adopted, err := m.adopt(name)
		if err != nil {
			return SessionInfo{}, false
		}
		h = adopted
	}
	h.mu.Lock()
	stale := time.Since(h.infoAt) > time.Second
	info := h.info
	h.mu.Unlock()
	if refresh || stale {
		if fresh, err := h.refresh(); err == nil {
			info = fresh
		}
	}
	return info, true
}

// list reports every session, adopting keepers that were started elsewhere.
func (m *KeeperManager) list() []SessionInfo {
	m.scan()
	m.mu.Lock()
	handles := make([]*keeperHandle, 0, len(m.handles))
	for _, h := range m.handles {
		handles = append(handles, h)
	}
	m.mu.Unlock()
	out := make([]SessionInfo, 0, len(handles))
	for _, h := range handles {
		if info, ok := m.info(h.name, false); ok {
			out = append(out, info)
		}
	}
	return out
}

// scan adopts keepers whose sockets exist but are unknown to the daemon.
func (m *KeeperManager) scan() {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sock") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".sock")
		m.mu.Lock()
		_, known := m.handles[name]
		m.mu.Unlock()
		if known {
			continue
		}
		if _, ok := m.info(name, true); !ok {
			m.cleanupStale(name)
		}
	}
}

// cleanupStale removes socket and metadata of a keeper that is not running.
func (m *KeeperManager) cleanupStale(name string) {
	meta, err := ReadKeeperMeta(m.MetaPath(name))
	if err == nil && meta.PID > 0 && processAlive(meta.PID) {
		return
	}
	_ = os.Remove(m.socketPath(name))
	_ = os.Remove(m.MetaPath(name))
}

// routeSessionRequest serves a session method by proxying it to the keeper.
func (m *KeeperManager) routeSessionRequest(c *conn, env *proto.Envelope) {
	name := sessionNameFrom(env)
	if name == "" {
		name = "main"
	}
	switch env.Method {
	case proto.MethodSessionOpen:
		var opts SessionOptions
		if err := env.DecodeParams(&opts); err != nil {
			c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
			return
		}
		if _, err := m.ensure(name, &opts, true); err != nil {
			c.fail(env.ID, asProto(err))
			return
		}
		info, _ := m.info(name, true)
		c.respond(env.ID, info)
	case proto.MethodExec, proto.MethodSessionAttach:
		h, err := m.ensure(name, nil, true)
		if err != nil {
			c.fail(env.ID, asProto(err))
			return
		}
		h.route(c, env, env.Method == proto.MethodSessionAttach)
	default:
		h, err := m.ensure(name, nil, false)
		if err != nil {
			c.fail(env.ID, asProto(err))
			return
		}
		h.route(c, env, false)
	}
}

// kill stops one keeper and waits for it to exit.
func (m *KeeperManager) kill(name string, force bool) error {
	m.logf("stopping session %s (force=%v)", name, force)
	defer m.logf("session %s stopped", name)
	m.removeSessionState(name)
	h, err := m.ensure(name, nil, false)
	if err != nil {
		m.cleanupStale(name)
		return err
	}
	meta, _ := ReadKeeperMeta(m.MetaPath(name))
	// Ask the keeper to stop its session, then fall back to signals.
	_, _ = h.call(proto.MethodSessionClose, map[string]any{"session": name}, 5*time.Second)
	h.close()
	pid := meta.PID
	if pid == 0 {
		h.mu.Lock()
		pid = h.pid
		h.mu.Unlock()
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(m.socketPath(name)); os.IsNotExist(err) {
			return nil
		}
		if pid > 0 && !processAlive(pid) {
			m.cleanupStale(name)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pid > 0 && processAlive(pid) {
		m.logf("keeper %s did not stop, terminating pid %d", name, pid)
		_ = terminateProcess(pid)
		time.Sleep(500 * time.Millisecond)
		if processAlive(pid) {
			_ = killProcess(pid)
		}
	}
	m.cleanupStale(name)
	return nil
}

// removeSessionState forgets the environment snapshot of a stopped session.
func (m *KeeperManager) removeSessionState(name string) {
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	_ = os.Remove(filepath.Join(home, ".rsk", "state", sanitizeName(name)+".env"))
}

// killAll stops every known keeper.
func (m *KeeperManager) killAll(force bool) {
	m.scan()
	m.mu.Lock()
	names := make([]string, 0, len(m.handles))
	for name := range m.handles {
		names = append(names, name)
	}
	m.mu.Unlock()
	m.logf("stopping %d session keeper(s)", len(names))
	for _, name := range names {
		if err := m.kill(name, force); err != nil {
			m.logf("kill session %s: %v", name, err)
		}
	}
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sock") {
			m.cleanupStale(strings.TrimSuffix(e.Name(), ".sock"))
		}
	}
}

// reapIdle closes keepers that have been unused for longer than timeout.
func (m *KeeperManager) reapIdle(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	cutoff := time.Now().Add(-timeout)
	m.mu.Lock()
	names := make([]string, 0, len(m.handles))
	for name := range m.handles {
		names = append(names, name)
	}
	m.mu.Unlock()
	for _, name := range names {
		info, ok := m.info(name, false)
		if !ok || info.Busy || info.Subscribers > 0 {
			continue
		}
		if !info.LastActive.IsZero() && info.LastActive.After(cutoff) {
			continue
		}
		m.logf("reaping idle session %s", name)
		_ = m.kill(name, true)
	}
}
