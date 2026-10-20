package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creack/pty"

	"remote-skill/internal/proto"
	"remote-skill/internal/transfer"
	"remote-skill/internal/version"
)

// HostInfo describes the daemon and its host.
type HostInfo struct {
	Version   string        `json:"version"`
	API       int           `json:"api"`
	OS        string        `json:"os"`
	Arch      string        `json:"arch"`
	User      string        `json:"user"`
	Home      string        `json:"home"`
	Hostname  string        `json:"hostname"`
	PID       int           `json:"pid"`
	StartedAt time.Time     `json:"started_at"`
	UptimeSec int64         `json:"uptime_sec"`
	Socket    string        `json:"socket"`
	Shell     string        `json:"shell"`
	GoVersion string        `json:"go_version"`
	Kernel    string        `json:"kernel,omitempty"`
	Binary    string        `json:"binary,omitempty"`
	BinarySHA string        `json:"binary_sha,omitempty"`
	Sessions  []SessionInfo `json:"sessions"`
}

// Options configures a Server.
type Options struct {
	SocketPath  string
	Logger      *log.Logger
	IdleTimeout time.Duration
	DefaultEnv  map[string]string
	Shell       string
	// Keeper holds the single session this process serves; when set the
	// process is a per-session keeper rather than the host daemon.
	Keeper *Session
	// Proxy routes session methods to keeper processes. When set (and Keeper
	// is nil) the process is the host daemon: it owns no sessions itself.
	Proxy *KeeperManager
	// Binary and BinarySHA describe the running daemon executable, letting a
	// client detect that it is talking to an outdated build.
	Binary    string
	BinarySHA string
}

// Server hosts persistent sessions on a remote machine.
type Server struct {
	opts      Options
	log       *log.Logger
	startedAt time.Time
	home      string
	user      string
	hostname  string

	mu       sync.Mutex
	sessions map[string]*Session
	conns    map[*conn]struct{}
	createMu sync.Mutex

	shutdownOnce sync.Once
	shutdownCh   chan struct{}
	shutdownDone chan struct{}
	closed       atomic.Bool
}

// New creates a server.
func New(opts Options) *Server {
	if opts.Logger == nil {
		opts.Logger = log.New(io.Discard, "", 0)
	}
	home, _ := os.UserHomeDir()
	user := os.Getenv("USER")
	if user == "" {
		user = os.Getenv("USERNAME")
	}
	host, _ := os.Hostname()
	s := &Server{
		opts:         opts,
		log:          opts.Logger,
		startedAt:    time.Now(),
		home:         home,
		user:         user,
		hostname:     host,
		sessions:     map[string]*Session{},
		conns:        map[*conn]struct{}{},
		shutdownCh:   make(chan struct{}),
		shutdownDone: make(chan struct{}),
	}
	if opts.Keeper != nil {
		s.sessions[opts.Keeper.Name()] = opts.Keeper
	}
	return s
}

// SocketPath returns the unix socket the daemon serves on.
func (s *Server) SocketPath() string { return s.opts.SocketPath }

// StartedAt reports daemon start time.
func (s *Server) StartedAt() time.Time { return s.startedAt }

// Info returns host details and session summaries.
func (s *Server) Info() HostInfo {
	s.mu.Lock()
	var sessions []SessionInfo
	if s.opts.Proxy != nil {
		sessions = s.opts.Proxy.list()
	} else {
		sessions = make([]SessionInfo, 0, len(s.sessions))
		for _, sess := range s.sessions {
			sessions = append(sessions, sess.Info())
		}
	}
	s.mu.Unlock()
	sortSessions(sessions)
	return HostInfo{
		Version:   version.Version,
		API:       proto.APIVersion,
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		User:      s.user,
		Home:      s.home,
		Hostname:  s.hostname,
		PID:       os.Getpid(),
		StartedAt: s.startedAt,
		UptimeSec: int64(time.Since(s.startedAt).Seconds()),
		Socket:    s.opts.SocketPath,
		Shell:     os.Getenv("SHELL"),
		GoVersion: runtime.Version(),
		Kernel:    kernelRelease(),
		Binary:    s.opts.Binary,
		BinarySHA: s.opts.BinarySHA,
		Sessions:  sessions,
	}
}

func sortSessions(list []SessionInfo) {
	for i := 1; i < len(list); i++ {
		for j := i; j > 0 && list[j-1].Name > list[j].Name; j-- {
			list[j-1], list[j] = list[j], list[j-1]
		}
	}
}

// Serve accepts connections until the listener is closed or Shutdown runs.
func (s *Server) Serve(l net.Listener) error {
	go s.janitor()
	for {
		nc, err := l.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			return err
		}
		c := &conn{
			srv:    s,
			nc:     nc,
			pc:     proto.NewConn(nc, nc),
			active: map[string]*request{},
			closed: make(chan struct{}),
		}
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		go c.serve()
	}
}

func (s *Server) janitor() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.shutdownCh:
			return
		case <-ticker.C:
			if s.opts.IdleTimeout <= 0 {
				continue
			}
			if s.opts.Proxy != nil {
				s.opts.Proxy.reapIdle(s.opts.IdleTimeout)
				continue
			}
			cutoff := time.Now().Add(-s.opts.IdleTimeout)
			var dead []*Session
			s.mu.Lock()
			for name, sess := range s.sessions {
				info := sess.Info()
				if !info.Busy && info.Subscribers == 0 && info.LastActive.Before(cutoff) {
					dead = append(dead, sess)
					delete(s.sessions, name)
				}
			}
			s.mu.Unlock()
			for _, sess := range dead {
				s.log.Printf("reaping idle session %s", sess.Name())
				sess.Close()
			}
		}
	}
}

// Shutdown stops the daemon, optionally keeping the caller's sessions. When
// force is false running commands are allowed to finish.
func (s *Server) Shutdown(force bool) {
	s.shutdownWith(force, false)
}

// ShutdownAll stops the daemon and, when killSessions is set, every keeper.
func (s *Server) ShutdownAll(force, killSessions bool) {
	s.shutdownWith(force, killSessions)
}

func (s *Server) shutdownWith(force, killSessions bool) {
	s.shutdownOnce.Do(func() {
		s.closed.Store(true)
		close(s.shutdownCh)
		s.log.Printf("daemon shutting down (kill_sessions=%v, force=%v)", killSessions, force)
		if killSessions && s.opts.Proxy != nil {
			s.opts.Proxy.killAll(force)
		}
		s.mu.Lock()
		conns := make([]*conn, 0, len(s.conns))
		for c := range s.conns {
			conns = append(conns, c)
		}
		sessions := make([]*Session, 0, len(s.sessions))
		for _, sess := range s.sessions {
			sessions = append(sessions, sess)
		}
		s.sessions = map[string]*Session{}
		s.mu.Unlock()
		for _, sess := range sessions {
			if !force {
				deadline := time.Now().Add(30 * time.Second)
				for time.Now().Before(deadline) && sess.Info().Busy {
					time.Sleep(100 * time.Millisecond)
				}
			}
			sess.Close()
		}
		for _, c := range conns {
			c.close()
		}
		close(s.shutdownDone)
	})
}

// ShutdownCh is closed when the daemon begins shutting down.
func (s *Server) ShutdownCh() <-chan struct{} { return s.shutdownCh }

// ShutdownDone is closed when the shutdown work (including stopping session
// keepers) has finished. Callers must wait for it before exiting, otherwise an
// in-flight cleanup is killed halfway.
func (s *Server) ShutdownDone() <-chan struct{} { return s.shutdownDone }

// session returns (creating when requested) the named session.
func (s *Server) session(name string, create bool, opts *SessionOptions) (*Session, error) {
	if name == "" {
		name = "main"
	}
	if keeper := s.opts.Keeper; keeper != nil {
		if name != keeper.Name() {
			return nil, proto.Errorf(proto.CodeNotFound, "session %q does not exist", name)
		}
		if sess, ok := s.sessions[keeper.Name()]; ok {
			if !sess.Info().Closed {
				return sess, nil
			}
			return nil, ErrSessionClosed
		}
		return keeper, nil
	}
	s.mu.Lock()
	sess, ok := s.sessions[name]
	if ok && !sess.Info().Closed {
		s.mu.Unlock()
		return sess, nil
	}
	s.mu.Unlock()
	if !create {
		return nil, proto.Errorf(proto.CodeNotFound, "session %q does not exist", name).
			WithHint("create it with `rsk up <host> --session " + name + "`")
	}

	s.createMu.Lock()
	defer s.createMu.Unlock()
	s.mu.Lock()
	var previous *Session
	if sess, ok := s.sessions[name]; ok && !sess.Info().Closed {
		s.mu.Unlock()
		return sess, nil
	} else if ok {
		previous = sess
	}
	s.mu.Unlock()

	o := SessionOptions{Name: name}
	if opts != nil {
		o = *opts
		o.Name = name
	}
	if o.Cwd == "" && previous != nil {
		// Restore the working directory of a shell that exited so a crash
		// inside a command does not silently move the workspace.
		if cwd := previous.Info().Cwd; cwd != "" {
			o.Cwd = cwd
		}
	}
	if o.Env == nil {
		o.Env = map[string]string{}
	}
	for k, v := range s.opts.DefaultEnv {
		if _, ok := o.Env[k]; !ok {
			o.Env[k] = v
		}
	}
	if o.Cwd == "" {
		o.Cwd = s.home
	}
	if o.Shell == "" {
		o.Shell = s.opts.Shell
	}
	o.Logger = s.log
	sess, err := StartSession(o)
	if err != nil {
		return nil, proto.Errorf(proto.CodeInternal, "start session %q: %v", name, err)
	}
	s.mu.Lock()
	s.sessions[name] = sess
	s.mu.Unlock()
	s.log.Printf("session %s started (pid %d)", name, sess.Info().PID)
	if previous != nil {
		s.log.Printf("session %s was recreated after its shell exited", name)
	}
	return sess, nil
}

func (s *Server) dropSession(name string) {
	s.mu.Lock()
	delete(s.sessions, name)
	s.mu.Unlock()
}

// ReplaceKeeper swaps the session served by a keeper (used when its shell
// exits and is restarted in place).
func (s *Server) ReplaceKeeper(sess *Session) {
	s.mu.Lock()
	s.sessions[sess.Name()] = sess
	s.mu.Unlock()
}

// sessionInfo reports cached session state without owning the session.
func (s *Server) sessionInfo(name string) (SessionInfo, bool) {
	if s.opts.Proxy != nil {
		return s.opts.Proxy.info(name, false)
	}
	sess, err := s.session(name, false, nil)
	if err != nil {
		return SessionInfo{}, false
	}
	return sess.Info(), true
}

// conn is one client connection (one ssh invocation).
type conn struct {
	srv *Server
	nc  net.Conn
	pc  *proto.Conn

	mu     sync.Mutex
	active map[string]*request
	greet  bool
	// proxyRoutes maps client request ids to routes served by a keeper.
	proxyRoutes map[string]*proxyRoute

	closeOnce sync.Once
	closed    chan struct{}
}

type request struct {
	id     string
	method string
	in     chan *proto.Envelope
	closed chan struct{}
	once   sync.Once
}

func (r *request) deliver(env *proto.Envelope) {
	select {
	case r.in <- env:
	case <-r.closed:
	case <-time.After(2 * time.Minute):
	}
}

func (r *request) close() {
	r.once.Do(func() { close(r.closed) })
}

func (c *conn) serve() {
	defer func() {
		c.close()
		c.srv.mu.Lock()
		delete(c.srv.conns, c)
		c.srv.mu.Unlock()
	}()
	for {
		env, err := c.pc.Read()
		if err != nil {
			return
		}
		switch env.Type {
		case proto.TypeHello:
			if err := c.handleHello(env); err != nil {
				return
			}
		case proto.TypeReq:
			if !c.greet {
				_ = c.pc.Write(proto.NewError(env.ID, proto.Errorf(proto.CodeBadRequest, "say hello first")))
				return
			}
			c.handleRequest(env)
		case proto.TypeEvent:
			if r := c.proxyRouteFor(env.ID); r != nil {
				r.forwardToKeeper(env)
				continue
			}
			c.route(env)
		case proto.TypeBye:
			return
		default:
			// ignore unknown frames for forward compatibility
		}
	}
}

func (c *conn) close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.nc.Close()
		c.mu.Lock()
		reqs := make([]*request, 0, len(c.active))
		for _, r := range c.active {
			reqs = append(reqs, r)
		}
		c.mu.Unlock()
		for _, r := range reqs {
			r.close()
		}
		for _, r := range c.clearProxyRoutes() {
			r.detachClient()
		}
	})
}

func (c *conn) handleHello(env *proto.Envelope) error {
	var hello struct {
		Version string `json:"version"`
		API     int    `json:"api"`
		Client  string `json:"client"`
		PID     int    `json:"pid"`
	}
	if err := env.DecodeParams(&hello); err != nil {
		_ = c.pc.Write(proto.NewError(env.ID, proto.Errorf(proto.CodeBadRequest, "bad hello: %v", err)))
		return err
	}
	if hello.API != proto.APIVersion {
		e := proto.Errorf(proto.CodeVersion, "protocol mismatch: client api %d, daemon api %d", hello.API, proto.APIVersion)
		e.Hint = "re-run `rsk up --restart` to replace the remote daemon"
		_ = c.pc.Write(&proto.Envelope{Type: proto.TypeBye, ID: env.ID, Err: e})
		return errors.New("protocol mismatch")
	}
	c.mu.Lock()
	c.greet = true
	c.mu.Unlock()
	info := c.srv.Info()
	res, err := proto.NewResult(env.ID, info)
	if err != nil {
		return err
	}
	res.Type = proto.TypeWelcome
	return c.pc.Write(res)
}

func (c *conn) register(req *request) {
	c.mu.Lock()
	c.active[req.id] = req
	c.mu.Unlock()
}

func (c *conn) unregister(req *request) {
	c.mu.Lock()
	delete(c.active, req.id)
	c.mu.Unlock()
}

// setProxyRoute records that a client request is being served by a keeper.
func (c *conn) setProxyRoute(id string, r *proxyRoute) {
	c.mu.Lock()
	if c.proxyRoutes == nil {
		c.proxyRoutes = map[string]*proxyRoute{}
	}
	c.proxyRoutes[id] = r
	c.mu.Unlock()
}

// proxyRouteFor looks up the keeper route for a client request id.
func (c *conn) proxyRouteFor(id string) *proxyRoute {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.proxyRoutes[id]
}

// clearProxyRoute forgets a finished proxied request.
func (c *conn) clearProxyRoute(id string) {
	c.mu.Lock()
	delete(c.proxyRoutes, id)
	c.mu.Unlock()
}

// clearProxyRoutes detaches every keeper route of this connection.
func (c *conn) clearProxyRoutes() []*proxyRoute {
	c.mu.Lock()
	routes := make([]*proxyRoute, 0, len(c.proxyRoutes))
	for _, r := range c.proxyRoutes {
		routes = append(routes, r)
	}
	c.proxyRoutes = nil
	c.mu.Unlock()
	return routes
}

func (c *conn) route(env *proto.Envelope) {
	c.mu.Lock()
	req := c.active[env.ID]
	c.mu.Unlock()
	if req == nil {
		return
	}
	req.deliver(env)
}

func (c *conn) respond(id string, result any) {
	res, err := proto.NewResult(id, result)
	if err != nil {
		c.fail(id, proto.Errorf(proto.CodeInternal, "encode result: %v", err))
		return
	}
	_ = c.pc.Write(res)
}

// respondRaw forwards an already encoded result (used by the keeper proxy).
func (c *conn) respondRaw(id string, result json.RawMessage) {
	_ = c.pc.Write(&proto.Envelope{Type: proto.TypeRes, ID: id, Result: result})
}

// proxyRequest hands a session scoped method to the keeper manager.
func (s *Server) proxyRequest(c *conn, req *request, env *proto.Envelope) {
	_ = req
	s.opts.Proxy.routeSessionRequest(c, env)
}

func (c *conn) fail(id string, err *proto.Error) {
	_ = c.pc.Write(proto.NewError(id, err))
}

func (c *conn) event(id, channel string, data []byte, extra map[string]any) {
	env := proto.NewEvent(id, channel, data)
	env.Extra = extra
	_ = c.pc.Write(env)
}

func (c *conn) handleRequest(env *proto.Envelope) {
	req := &request{
		id:     env.ID,
		method: env.Method,
		in:     make(chan *proto.Envelope, 64),
		closed: make(chan struct{}),
	}
	c.register(req)
	go func() {
		defer func() {
			c.unregister(req)
			req.close()
		}()
		if c.srv.opts.Proxy != nil && isSessionMethod(env.Method) {
			c.srv.proxyRequest(c, req, env)
			return
		}
		switch env.Method {
		case proto.MethodInfo:
			c.respond(env.ID, c.srv.Info())
		case proto.MethodPing:
			c.respond(env.ID, map[string]any{"pong": time.Now().UnixMilli()})
		case proto.MethodSessionOpen:
			c.handleSessionOpen(env)
		case proto.MethodSessionInfo:
			c.handleSessionInfo(env)
		case proto.MethodSessionList:
			c.respond(env.ID, c.srv.Info())
		case proto.MethodSessionClose:
			c.handleSessionClose(env)
		case proto.MethodExec:
			c.handleExec(req, env)
		case proto.MethodExecTTY:
			c.handleExecTTY(req, env)
		case proto.MethodSessionAttach:
			c.handleAttach(req, env)
		case proto.MethodSessionResize:
			c.handleResize(env)
		case proto.MethodSessionSignal:
			c.handleSignal(env)
		case proto.MethodSessionScrollback:
			c.handleScrollback(env)
		case proto.MethodFilePut:
			c.handleFilePut(req, env)
		case proto.MethodFileGet:
			c.handleFileGet(req, env)
		case proto.MethodFileRead:
			c.handleFileRead(env)
		case proto.MethodFileWrite:
			c.handleFileWrite(req, env)
		case proto.MethodFilePatch:
			c.handleFilePatch(req, env)
		case proto.MethodFileList:
			c.handleFileList(env)
		case proto.MethodFileManifest:
			c.handleFileManifest(env)
		case proto.MethodFileRemove:
			c.handleFileRemove(env)
		case proto.MethodTunnelOpen:
			c.handleTunnel(req, env)
		case proto.MethodTunnelListen:
			c.handleTunnelListen(req, env)
		case proto.MethodShutdown:
			var p struct {
				Force        bool   `json:"force"`
				KillSessions bool   `json:"kill_sessions"`
				Sessions     string `json:"sessions"`
			}
			_ = env.DecodeParams(&p)
			kill := p.KillSessions || p.Sessions == "kill"
			c.respond(env.ID, map[string]any{"stopping": true})
			go func() {
				time.Sleep(100 * time.Millisecond)
				c.srv.ShutdownAll(p.Force, kill)
			}()
		default:
			c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "unknown method %q", env.Method))
		}
	}()
}

func (c *conn) handleSessionOpen(env *proto.Envelope) {
	var p SessionOptions
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	sess, err := c.srv.session(p.Name, true, &p)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	if err := waitReady(sess, 60*time.Second); err != nil {
		c.srv.dropSession(sess.Name())
		c.fail(env.ID, asProto(err))
		return
	}
	c.respond(env.ID, sess.Info())
}

func (c *conn) handleSessionInfo(env *proto.Envelope) {
	if c.srv.opts.Proxy != nil {
		info, ok := c.srv.opts.Proxy.info(sessionNameFrom(env), true)
		if !ok {
			c.fail(env.ID, proto.Errorf(proto.CodeNotFound, "session %q does not exist", sessionNameFrom(env)))
			return
		}
		c.respond(env.ID, info)
		return
	}
	sess, err := c.srv.session(sessionNameFrom(env), false, nil)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	c.respond(env.ID, sess.Info())
}

func (c *conn) handleSessionClose(env *proto.Envelope) {
	var p struct {
		Session string `json:"session"`
	}
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	if p.Session == "" {
		p.Session = "main"
	}
	sess, err := c.srv.session(p.Session, false, nil)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	c.srv.dropSession(sess.Name())
	_ = sess.Terminate()
	c.respond(env.ID, map[string]any{"session": sess.Name(), "closed": true})
}

func (c *conn) handleExec(req *request, env *proto.Envelope) {
	sess, err := c.srv.session(sessionNameFrom(env), true, nil)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	serveExec(c, sess, req, env)
}

// handleExecTTY runs one command on a dedicated pty (ssh -t semantics), which
// lets interactive tools like sudo or top work. It does not touch the
// persistent session's shell, but starts in its working directory.
// TTYParams describe a one-shot pty command.
type TTYParams struct {
	Session   string `json:"session"`
	Cmd       string `json:"cmd"`
	Cwd       string `json:"cwd"`
	TimeoutMS int64  `json:"timeout_ms"`
	Rows      uint16 `json:"rows"`
	Cols      uint16 `json:"cols"`
}

func (c *conn) handleExecTTY(req *request, env *proto.Envelope) {
	var p TTYParams
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	if !nativePTY {
		c.handleExecTTYWindows(req, env, p)
		return
	}
	if strings.TrimSpace(p.Cmd) == "" {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "command is empty"))
		return
	}
	cwd := p.Cwd
	if cwd == "" {
		if info, ok := c.srv.sessionInfo(p.Session); ok {
			cwd = info.Cwd
		}
	}
	if cwd == "" {
		cwd = os.Getenv("HOME")
	}
	if p.Rows == 0 {
		p.Rows = 40
	}
	if p.Cols == 0 {
		p.Cols = 120
	}
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/bash"
	}
	start := time.Now()
	cmd := exec.Command(shell, "-l", "-c", p.Cmd)
	cmd.Dir = dirOrEmpty(cwd)
	cmd.Env = append(os.Environ(), "TERM=xterm-256color", "RSK=1")
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: p.Rows, Cols: p.Cols})
	if err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeIO, "start pty: %v", err))
		return
	}
	defer ptmx.Close()
	pid := cmd.Process.Pid
	c.respond(env.ID, map[string]any{"pid": pid, "cwd": cwd, "started_at": start})
	sink := &connSink{c: c, id: env.ID}
	defer sink.detach()

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		buf := make([]byte, 32<<10)
		for {
			n, rerr := ptmx.Read(buf)
			if n > 0 {
				sink.publish(Event{Chan: proto.ChOut, Data: append([]byte(nil), buf[:n]...)})
			}
			if rerr != nil {
				return
			}
		}
	}()

	var timeoutCh <-chan time.Time
	if p.TimeoutMS > 0 {
		timer := time.NewTimer(time.Duration(p.TimeoutMS) * time.Millisecond)
		defer timer.Stop()
		timeoutCh = timer.C
	}
	timedOut := false
	for {
		select {
		case <-c.closed:
			_ = killGroup(pid, "TERM")
			return
		case <-req.closed:
			_ = killGroup(pid, "TERM")
			return
		case <-timeoutCh:
			timedOut = true
			_ = killGroup(pid, "INT")
			time.AfterFunc(3*time.Second, func() { _ = killGroup(pid, "KILL") })
			timeoutCh = nil
		case in := <-req.in:
			switch in.Chan {
			case proto.ChIn:
				if _, werr := ptmx.Write(in.Data); werr != nil {
					return
				}
			case proto.ChSignal:
				sig, _ := in.Extra["sig"].(string)
				if sig == "" {
					sig = "INT"
				}
				_ = killGroup(pid, sig)
			case proto.ChMeta:
				rows, cols := extraUint16(in.Extra, "rows"), extraUint16(in.Extra, "cols")
				if rows > 0 && cols > 0 {
					_ = pty.Setsize(ptmx, &pty.Winsize{Rows: rows, Cols: cols})
					_ = killGroup(pid, "WINCH")
				}
			case proto.ChEof:
				_, _ = ptmx.Write([]byte{0x04})
			}
		case <-readDone:
			waitErr := cmd.Wait()
			rc := 0
			var exitErr *exec.ExitError
			if errors.As(waitErr, &exitErr) {
				rc = exitErr.ExitCode()
			} else if waitErr != nil {
				rc = -1
			}
			c.respond(env.ID, &ExecResult{
				ID:         env.ID,
				Session:    p.Session,
				Cmd:        p.Cmd,
				RC:         rc,
				TimedOut:   timedOut,
				DurationMS: time.Since(start).Milliseconds(),
				Cwd:        cwd,
				StartedAt:  start,
			})
			// The final result must precede the terminal done event, otherwise
			// the client stops at the empty done frame and reports rc 0.
			sink.publish(Event{Chan: proto.ChDone})
			return
		}
	}
}

// dirOrEmpty returns dir when it exists, otherwise "".
func dirOrEmpty(dir string) string {
	if st, err := os.Stat(dir); err == nil && st.IsDir() {
		return dir
	}
	return ""
}

func (c *conn) handleAttach(req *request, env *proto.Envelope) {
	sess, err := c.srv.session(sessionNameFrom(env), true, nil)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	serveAttach(c, sess, req, env)
}

func (c *conn) handleResize(env *proto.Envelope) {
	sess, err := c.srv.session(sessionNameFrom(env), false, nil)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	serveResize(c, sess, env)
}

func (c *conn) handleSignal(env *proto.Envelope) {
	sess, err := c.srv.session(sessionNameFrom(env), false, nil)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	serveSignal(c, sess, env)
}

func (c *conn) handleScrollback(env *proto.Envelope) {
	sess, err := c.srv.session(sessionNameFrom(env), false, nil)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	serveScrollback(c, sess, env)
}

// FileEntry describes one directory entry.
type FileEntry struct {
	Name       string    `json:"name"`
	Path       string    `json:"path"`
	Dir        bool      `json:"dir"`
	Symlink    bool      `json:"symlink"`
	Size       int64     `json:"size"`
	Mode       uint32    `json:"mode"`
	ModeString string    `json:"mode_string"`
	Mtime      time.Time `json:"mtime"`
}

func (c *conn) handleFileList(env *proto.Envelope) {
	var p struct {
		Session string `json:"session"`
		Path    string `json:"path"`
	}
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	dir, err := c.srv.resolvePath(p.Session, p.Path)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	out := make([]FileEntry, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		kind := info.Mode()
		out = append(out, FileEntry{
			Name:       e.Name(),
			Path:       filepath.Join(dir, e.Name()),
			Dir:        info.IsDir(),
			Symlink:    kind&os.ModeSymlink != 0,
			Size:       info.Size(),
			Mode:       uint32(kind.Perm()),
			ModeString: kind.String(),
			Mtime:      info.ModTime(),
		})
	}
	c.respond(env.ID, map[string]any{"path": dir, "entries": out, "count": len(out)})
}

// ManifestEntry describes one path for incremental sync.
type ManifestEntry struct {
	Path  string    `json:"path"`
	Size  int64     `json:"size"`
	Mtime time.Time `json:"mtime"`
	Dir   bool      `json:"dir"`
}

func (c *conn) handleFileManifest(env *proto.Envelope) {
	var p struct {
		Session string `json:"session"`
		Path    string `json:"path"`
	}
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	root, err := c.srv.resolvePath(p.Session, p.Path)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	c.srv.log.Printf("manifest %s", root)
	entries := make([]ManifestEntry, 0, 256)
	count := 0
	walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// unreadable entries are skipped rather than failing the sync
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if path == root {
			return nil
		}
		if count >= 200000 {
			return filepath.SkipAll
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return nil
		}
		entries = append(entries, ManifestEntry{
			Path:  filepath.ToSlash(rel),
			Size:  info.Size(),
			Mtime: info.ModTime(),
			Dir:   d.IsDir(),
		})
		count++
		return nil
	})
	if walkErr != nil {
		c.fail(env.ID, asProto(walkErr))
		return
	}
	c.respond(env.ID, map[string]any{"root": root, "entries": entries, "count": len(entries)})
}

func (c *conn) handleFileRemove(env *proto.Envelope) {
	var p struct {
		Session string   `json:"session"`
		Root    string   `json:"root"`
		Paths   []string `json:"paths"`
	}
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	root, err := c.srv.resolvePath(p.Session, p.Root)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	removed := 0
	for _, rel := range p.Paths {
		target, terr := transfer.SafeJoin(root, rel)
		if terr != nil {
			continue
		}
		if err := os.Remove(target); err == nil {
			removed++
		}
	}
	c.respond(env.ID, map[string]any{"removed": removed, "requested": len(p.Paths)})
}

func (c *conn) handleFilePut(req *request, env *proto.Envelope) {
	var p struct {
		Session   string `json:"session"`
		Path      string `json:"path"`
		Recursive bool   `json:"recursive"`
		Overwrite bool   `json:"overwrite"`
		Touch     bool   `json:"touch"`
	}
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	destDir, rename, err := c.srv.resolveDest(p.Session, p.Path)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	c.srv.log.Printf("file.put %s -> %s (rename %q)", p.Path, destDir, rename)

	pr, pw := io.Pipe()
	type extractOut struct {
		res *transfer.Result
		err error
	}
	outCh := make(chan extractOut, 1)
	go func() {
		res, err := transfer.Extract(pr, destDir, transfer.ExtractOptions{
			Overwrite: p.Overwrite,
			Touch:     p.Touch,
			TopRename: rename,
		})
		// Unblock the writer: if extraction failed early, later chunks would
		// otherwise block forever on a pipe with no reader.
		_ = pr.CloseWithError(err)
		c.srv.log.Printf("file.put extract done: %v %v", res, err)
		outCh <- extractOut{res, err}
	}()

	var writeErr error
loop:
	for {
		select {
		case <-c.closed:
			_ = pr.CloseWithError(io.ErrClosedPipe)
			return
		case in := <-req.in:
			switch in.Chan {
			case proto.ChIn:
				if _, err := pw.Write(in.Data); err != nil {
					writeErr = err
					break loop
				}
			case proto.ChEof:
				c.srv.log.Printf("file.put eof received")
				break loop
			case proto.ChError:
				writeErr = fmt.Errorf("client aborted upload")
				break loop
			}
		}
	}
	if writeErr != nil {
		_ = pr.CloseWithError(writeErr)
	} else {
		_ = pw.Close()
	}
	out := <-outCh
	if out.err != nil {
		c.event(env.ID, proto.ChError, nil, map[string]any{"error": asProto(out.err).Error()})
		c.fail(env.ID, asProto(out.err))
		return
	}
	c.respond(env.ID, map[string]any{"result": out.res, "dest_dir": destDir, "rename": rename})
}

func (c *conn) handleFileGet(req *request, env *proto.Envelope) {
	var p struct {
		Session   string `json:"session"`
		Path      string `json:"path"`
		Recursive bool   `json:"recursive"`
	}
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	src, err := c.srv.resolvePath(p.Session, p.Path)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	st, err := os.Lstat(src)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	if st.IsDir() && !p.Recursive {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "%s is a directory", p.Path).
			WithHint("pass --recursive to download a directory"))
		return
	}
	c.respond(env.ID, map[string]any{
		"path":  src,
		"name":  filepath.Base(src),
		"dir":   st.IsDir(),
		"size":  st.Size(),
		"mode":  uint32(st.Mode().Perm()),
		"mtime": st.ModTime(),
		"ready": true,
	})

	pr, pw := io.Pipe()
	go func() {
		_, err := transfer.Archive(pw, []string{src}, transfer.ArchiveOptions{Recursive: true})
		_ = pw.CloseWithError(err)
	}()
	buf := make([]byte, 256<<10)
	sink := &connSink{c: c, id: env.ID}
	defer sink.detach()
	for {
		if sink.dead.Load() || c.isClosed() {
			_ = pr.CloseWithError(io.ErrClosedPipe)
			return
		}
		n, err := pr.Read(buf)
		if n > 0 {
			sink.publish(Event{Chan: proto.ChOut, Data: append([]byte(nil), buf[:n]...)})
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				c.doneEvent(env.ID, map[string]any{"path": src})
			} else {
				c.event(env.ID, proto.ChError, nil, map[string]any{"error": err.Error()})
				c.fail(env.ID, proto.Errorf(proto.CodeIO, "%v", err))
			}
			return
		}
	}
}

func (c *conn) handleTunnel(req *request, env *proto.Envelope) {
	var p struct {
		Target    string `json:"target"`
		TimeoutMS int64  `json:"timeout_ms"`
	}
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	timeout := time.Duration(p.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	nc, err := net.DialTimeout("tcp", p.Target, timeout)
	if err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeIO, "dial %s: %v", p.Target, err))
		return
	}
	defer nc.Close()
	sink := &connSink{c: c, id: env.ID}
	defer sink.detach()
	c.respond(env.ID, map[string]any{
		"target": p.Target,
		"local":  nc.LocalAddr().String(),
		"remote": nc.RemoteAddr().String(),
	})
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		buf := make([]byte, 32<<10)
		for {
			n, err := nc.Read(buf)
			if n > 0 {
				sink.publish(Event{Chan: proto.ChOut, Data: append([]byte(nil), buf[:n]...)})
			}
			if err != nil {
				sink.publish(Event{Chan: proto.ChEof})
				return
			}
		}
	}()
	for {
		select {
		case <-c.closed:
			return
		case <-pumpDone:
			return
		case in := <-req.in:
			switch in.Chan {
			case proto.ChIn:
				if _, err := nc.Write(in.Data); err != nil {
					return
				}
			case proto.ChEof:
				if cw, ok := nc.(interface{ CloseWrite() error }); ok {
					_ = cw.CloseWrite()
				}
				return
			}
		}
	}
}

// handleTunnelListen serves a reverse forward: it listens on the server and
// streams every accepted connection back to the client, which dials the real
// service on its own side (the ssh -R shape).
func (c *conn) handleTunnelListen(req *request, env *proto.Envelope) {
	var p struct {
		Bind string `json:"bind"`
		Port int    `json:"port"`
	}
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	if p.Port <= 0 || p.Port > 65535 {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "invalid port %d", p.Port))
		return
	}
	if p.Bind == "" {
		p.Bind = "127.0.0.1"
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(p.Bind, fmt.Sprint(p.Port)))
	if err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeIO, "listen on %s:%d: %v", p.Bind, p.Port, err))
		return
	}
	defer ln.Close()

	var mu sync.Mutex
	active := map[string]net.Conn{}
	closeAll := func() {
		mu.Lock()
		for id, conn := range active {
			_ = conn.Close()
			delete(active, id)
		}
		mu.Unlock()
	}
	defer closeAll()

	sink := &connSink{c: c, id: env.ID}
	defer sink.detach()
	c.respond(env.ID, map[string]any{
		"listening": ln.Addr().String(),
		"bind":      p.Bind,
		"port":      p.Port,
	})

	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			rc, err := ln.Accept()
			if err != nil {
				return
			}
			id := newRequestID()
			mu.Lock()
			active[id] = rc
			mu.Unlock()
			sink.publish(Event{Chan: proto.ChOpen, Extra: map[string]any{
				"conn": id,
				"peer": rc.RemoteAddr().String(),
			}})
			go func(id string, rc net.Conn) {
				buf := make([]byte, 32<<10)
				for {
					n, err := rc.Read(buf)
					if n > 0 {
						sink.publish(Event{Chan: proto.ChOut, Data: append([]byte(nil), buf[:n]...), Extra: map[string]any{"conn": id}})
					}
					if err != nil {
						break
					}
				}
				mu.Lock()
				_, ok := active[id]
				delete(active, id)
				mu.Unlock()
				_ = rc.Close()
				if ok {
					sink.publish(Event{Chan: proto.ChClose, Extra: map[string]any{"conn": id}})
				}
			}(id, rc)
		}
	}()

	for {
		select {
		case <-c.closed:
			return
		case <-req.closed:
			return
		case <-accepted:
			return
		case in := <-req.in:
			id, _ := in.Extra["conn"].(string)
			mu.Lock()
			rc := active[id]
			mu.Unlock()
			switch in.Chan {
			case proto.ChIn:
				if rc != nil {
					if _, err := rc.Write(in.Data); err != nil {
						return
					}
				}
			case proto.ChEof, proto.ChClose:
				if rc != nil {
					mu.Lock()
					delete(active, id)
					mu.Unlock()
					_ = rc.Close()
				}
			}
		}
	}
}

// resolvePath expands a user supplied path relative to a session working
// directory (or the daemon user's home directory).
func (s *Server) resolvePath(session, p string) (string, error) {
	if p == "" {
		return "", proto.Errorf(proto.CodeBadRequest, "empty path")
	}
	base := s.home
	if session != "" {
		if info, ok := s.sessionInfo(session); ok {
			if cwd := info.Cwd; cwd != "" {
				base = cwd
			}
		}
	}
	switch {
	case p == "~":
		p = s.home
	case strings.HasPrefix(p, "~/"):
		p = filepath.Join(s.home, p[2:])
	}
	if !filepath.IsAbs(p) {
		p = filepath.Join(base, p)
	}
	return filepath.Clean(p), nil
}

// resolveDest turns a destination path into (directory, rename) for uploads.
func (s *Server) resolveDest(session, p string) (string, string, error) {
	expanded, err := s.resolvePath(session, p)
	if err != nil {
		return "", "", err
	}
	if st, err := os.Stat(expanded); err == nil && st.IsDir() {
		return expanded, "", nil
	}
	if strings.HasSuffix(p, "/") {
		if err := os.MkdirAll(expanded, 0o755); err != nil {
			return "", "", proto.Errorf(proto.CodeIO, "create %s: %v", expanded, err)
		}
		return expanded, "", nil
	}
	if err := os.MkdirAll(filepath.Dir(expanded), 0o755); err != nil {
		return "", "", proto.Errorf(proto.CodeIO, "create %s: %v", filepath.Dir(expanded), err)
	}
	return filepath.Dir(expanded), filepath.Base(expanded), nil
}

func (c *conn) doneEvent(id string, result map[string]any) {
	env := &proto.Envelope{Type: proto.TypeEvent, ID: id, Chan: proto.ChDone}
	if result != nil {
		raw, err := json.Marshal(result)
		if err == nil {
			env.Result = raw
		}
	}
	_ = c.pc.Write(env)
}

// connSink streams session events to a client connection. If the client goes
// away the sink turns into a black hole: the command keeps running so its
// result can be picked up later from the session scrollback.
type connSink struct {
	c    *conn
	id   string
	dead atomic.Bool
}

func (s *connSink) Event(ev Event) { s.publish(ev) }

func (s *connSink) publish(ev Event) {
	if s.dead.Load() {
		return
	}
	env := proto.NewEvent(s.id, ev.Chan, ev.Data)
	var extra map[string]any
	if len(ev.Extra) > 0 {
		extra = make(map[string]any, len(ev.Extra)+3)
		for k, v := range ev.Extra {
			extra[k] = v
		}
	}
	if ev.ExecID != "" || ev.Session != "" || ev.Seq != 0 {
		if extra == nil {
			extra = map[string]any{}
		}
		extra["session"] = ev.Session
		extra["seq"] = ev.Seq
		if ev.ExecID != "" {
			extra["exec"] = ev.ExecID
		}
	}
	env.Extra = extra
	if err := s.c.pc.Write(env); err != nil {
		s.dead.Store(true)
	}
}

// detach marks the sink dead so a long running command stops streaming to a
// client that is no longer interested.
func (s *connSink) detach() { s.dead.Store(true) }

func asProto(err error) *proto.Error {
	var pe *proto.Error
	if errors.As(err, &pe) {
		return pe
	}
	if os.IsNotExist(err) {
		return proto.Errorf(proto.CodeNotFound, "%v", err)
	}
	return proto.Errorf(proto.CodeInternal, "%v", err)
}

func extraUint16(extra map[string]any, key string) uint16 {
	if extra == nil {
		return 0
	}
	switch v := extra[key].(type) {
	case float64:
		return uint16(v)
	case int:
		return uint16(v)
	case uint16:
		return v
	}
	return 0
}
