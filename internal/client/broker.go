package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"remote-skill/internal/daemon"
	"remote-skill/internal/proto"
)

// A broker is a local process that keeps one SSH connection to a host open and
// multiplexes short lived `rsk` invocations over it. Without it every command
// pays a full ssh/plink login (and a password login may even need prompting).

// BrokerSocketPath is the local unix socket of a host's broker.
func BrokerSocketPath(h *Host) string {
	sum := sha256.Sum256([]byte(h.Spec() + "|" + strconv.Itoa(h.Port) + "|" + h.Identity))
	return filepath.Join(RuntimeDir(), "brokers", hex.EncodeToString(sum[:8])+".sock")
}

// BrokerLogPath is where the broker writes its diagnostics.
func BrokerLogPath(h *Host) string {
	return strings.TrimSuffix(BrokerSocketPath(h), ".sock") + ".log"
}

func brokerPIDPath(h *Host) string { return BrokerSocketPath(h) + ".pid" }

// brokerActive reports whether a broker is accepting connections.
func brokerActive(h *Host) bool {
	nc, err := net.DialTimeout("unix", BrokerSocketPath(h), 300*time.Millisecond)
	if err != nil {
		return false
	}
	nc.Close()
	return true
}

// StopBroker terminates the local broker of a host, if any.
func StopBroker(h *Host) {
	path := BrokerSocketPath(h)
	if nc, err := net.DialTimeout("unix", path, 300*time.Millisecond); err == nil {
		pc := proto.NewConn(nc, nc)
		if hello, herr := proto.NewRequest("h", "", map[string]any{"api": proto.APIVersion, "version": "stop"}); herr == nil {
			hello.Type = proto.TypeHello
			_ = pc.Write(hello)
			_, _ = pc.Read()
			if req, rerr := proto.NewRequest("1", "broker.stop", nil); rerr == nil {
				_ = pc.Write(req)
			}
		}
		time.Sleep(150 * time.Millisecond)
		nc.Close()
	}
	for i := 0; i < 20; i++ {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = os.Remove(path)
	_ = os.Remove(brokerPIDPath(h))
}

// startBroker launches the broker process detached from this invocation.
func startBroker(h *Host, impl string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	dir := filepath.Dir(BrokerSocketPath(h))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	logFile, err := os.OpenFile(BrokerLogPath(h), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	args := []string{"__broker", "--host", h.Name, "--socket", BrokerSocketPath(h)}
	if impl != "" {
		args = append(args, "--ssh-impl", impl)
	}
	cmd := exec.Command(exe, args...)
	cmd.Env = os.Environ()
	if pw := os.Getenv("RSK_PASSWORD"); pw != "" {
		cmd.Env = append(cmd.Env, "RSK_PASSWORD="+pw)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = brokerSysProcAttr()
	if err := cmd.Start(); err != nil {
		return err
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	_ = os.WriteFile(brokerPIDPath(h), []byte(strconv.Itoa(pid)), 0o600)
	return nil
}

// dialBroker connects to (starting if needed) the broker of this host.
func (c *Client) dialBroker(ctx context.Context) (*proto.Conn, net.Conn, bool) {
	if !c.UseBroker && os.Getenv("RSK_BROKER") != "1" {
		return nil, nil, false
	}
	if c.NoBroker || c.forceDirect || os.Getenv("RSK_NO_BROKER") != "" {
		return nil, nil, false
	}
	path := BrokerSocketPath(c.Host)
	if nc, err := net.DialTimeout("unix", path, 400*time.Millisecond); err == nil {
		return proto.NewConn(nc, nc), nc, true
	}
	impl := c.SSH.Impl
	if impl == "" && HasCredential(c.Host.Name) {
		impl = "plink"
	}
	if err := startBroker(c.Host, impl); err != nil {
		c.logf("broker unavailable (%v); using a direct ssh connection", err)
		return nil, nil, false
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, nil, false
		case <-time.After(100 * time.Millisecond):
		}
		if nc, err := net.DialTimeout("unix", path, 500*time.Millisecond); err == nil {
			return proto.NewConn(nc, nc), nc, true
		}
	}
	c.logf("broker did not become ready; using a direct ssh connection")
	return nil, nil, false
}

// brokerPing verifies that the broker's remote daemon is still reachable
// before a short lived client commits to using it.
func brokerPing(conn *proto.Conn, nc net.Conn, timeout time.Duration) error {
	_ = nc.SetDeadline(time.Now().Add(timeout))
	defer nc.SetDeadline(time.Time{})
	req, err := proto.NewRequest("__ping", proto.MethodPing, nil)
	if err != nil {
		return err
	}
	if err := conn.Write(req); err != nil {
		return err
	}
	res, err := conn.Read()
	if err != nil {
		return err
	}
	if res.Type != proto.TypeRes || res.ID != "__ping" || res.Err != nil {
		return fmt.Errorf("broker ping failed")
	}
	return nil
}

// ServeBroker is the broker process: establish the one SSH connection and
// proxy every local client over it.
func (c *Client) ServeBroker(ctx context.Context, socket string) error {
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		return err
	}
	if !brokerActive(c.Host) {
		_ = os.Remove(socket)
	}
	ln, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer func() {
		ln.Close()
		_ = os.Remove(socket)
	}()

	c.forceDirect = true
	c.noReadLoop = true
	// The ctx must live as long as the connection: exec.CommandContext kills
	// the ssh child when it is cancelled.
	err = c.Connect(ctx, ConnectOptions{Quiet: true, Force: false})
	if err != nil {
		return fmt.Errorf("broker connect: %w", err)
	}
	remote := c.conn
	if remote == nil {
		return fmt.Errorf("broker has no remote connection")
	}
	info := c.Info
	if info == nil {
		info = &daemon.HostInfo{}
	}
	b := &broker{
		remote:   remote,
		info:     info,
		sessions: map[*brokerSession]struct{}{},
		dead:     make(chan struct{}),
		ln:       ln,
	}
	go b.readRemote()

	idle := time.NewTicker(30 * time.Second)
	defer idle.Stop()
	var lastActive atomic.Int64
	lastActive.Store(time.Now().UnixNano())
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-b.dead:
			return nil
		case <-idle.C:
			b.mu.Lock()
			n := len(b.sessions)
			b.mu.Unlock()
			if n == 0 && time.Since(time.Unix(0, lastActive.Load())) > 10*time.Minute {
				return nil
			}
		default:
		}
		if dl, ok := ln.(interface{ SetDeadline(time.Time) error }); ok {
			_ = dl.SetDeadline(time.Now().Add(2 * time.Second))
		}
		nc, aerr := ln.Accept()
		if aerr != nil {
			if errors.Is(aerr, net.ErrClosed) {
				return nil
			}
			if ne, ok := aerr.(net.Error); ok && ne.Timeout() {
				continue
			}
			return aerr
		}
		lastActive.Store(time.Now().UnixNano())
		s := b.newSession(nc)
		go func() { _ = s.serve(); lastActive.Store(time.Now().UnixNano()) }()
	}
}

// broker relays frames between local clients and the single remote connection.
type broker struct {
	remote   *proto.Conn
	info     *daemon.HostInfo
	ln       net.Listener
	mu       sync.Mutex
	sessions map[*brokerSession]struct{}
	seq      atomic.Int64
	dead     chan struct{}
	deadOnce sync.Once
}

type brokerSession struct {
	b      *broker
	nc     net.Conn
	pc     *proto.Conn
	mu     sync.Mutex
	routes map[string]string // internal id -> local id
	local  map[string]string // local id -> internal id
	closed chan struct{}
	once   sync.Once
}

func (b *broker) newSession(nc net.Conn) *brokerSession {
	s := &brokerSession{
		b:      b,
		nc:     nc,
		pc:     proto.NewConn(nc, nc),
		routes: map[string]string{},
		local:  map[string]string{},
		closed: make(chan struct{}),
	}
	b.mu.Lock()
	b.sessions[s] = struct{}{}
	b.mu.Unlock()
	return s
}

func (b *broker) closeAll() {
	b.deadOnce.Do(func() {
		// Stop accepting before tearing sessions down: a client that connects
		// in that window would otherwise be handed a dead remote.
		if b.ln != nil {
			_ = b.ln.Close()
		}
		close(b.dead)
		b.mu.Lock()
		sessions := make([]*brokerSession, 0, len(b.sessions))
		for s := range b.sessions {
			sessions = append(sessions, s)
		}
		b.mu.Unlock()
		for _, s := range sessions {
			s.close()
		}
	})
}

// readRemote dispatches remote frames to the owning local client.
func (b *broker) readRemote() {
	for {
		env, err := b.remote.Read()
		if err != nil {
			b.closeAll()
			return
		}
		b.mu.Lock()
		var target *brokerSession
		var localID string
		for s := range b.sessions {
			s.mu.Lock()
			if lid, ok := s.routes[env.ID]; ok {
				target, localID = s, lid
				s.mu.Unlock()
				break
			}
			s.mu.Unlock()
		}
		b.mu.Unlock()
		if target == nil {
			continue
		}
		out := *env
		out.ID = localID
		_ = target.pc.Write(&out)
	}
}

func (s *brokerSession) serve() error {
	defer s.close()
	// Handshake: the remote connection was already greeted by the broker.
	env, err := s.pc.Read()
	if err != nil || env.Type != proto.TypeHello {
		return errors.New("expected hello")
	}
	welcome, err := proto.NewResult(env.ID, s.b.info)
	if err != nil {
		return err
	}
	welcome.Type = proto.TypeWelcome
	if err := s.pc.Write(welcome); err != nil {
		return err
	}
	for {
		env, err := s.pc.Read()
		if err != nil {
			return err
		}
		switch env.Type {
		case proto.TypeReq:
			if env.Method == "broker.stop" {
				if res, rerr := proto.NewResult(env.ID, map[string]any{"stopping": true}); rerr == nil {
					_ = s.pc.Write(res)
				}
				s.b.closeAll()
				return nil
			}
			internal := fmt.Sprintf("b%d", s.b.seq.Add(1))
			s.mu.Lock()
			s.routes[internal] = env.ID
			s.local[env.ID] = internal
			s.mu.Unlock()
			out := *env
			out.ID = internal
			if err := s.b.remote.Write(&out); err != nil {
				return err
			}
		case proto.TypeEvent:
			s.mu.Lock()
			internal, ok := s.local[env.ID]
			s.mu.Unlock()
			if !ok {
				continue
			}
			out := *env
			out.ID = internal
			if err := s.b.remote.Write(&out); err != nil {
				return err
			}
		case proto.TypeBye:
			return nil
		}
	}
}

// close drops the local connection and tells the daemon to end any streams it
// still has open for it (attachments, tunnels, uploads).
func (s *brokerSession) close() {
	s.once.Do(func() {
		close(s.closed)
		_ = s.nc.Close()
		s.mu.Lock()
		internals := make([]string, 0, len(s.routes))
		for internal := range s.routes {
			internals = append(internals, internal)
		}
		s.routes = map[string]string{}
		s.local = map[string]string{}
		s.mu.Unlock()
		for _, internal := range internals {
			_ = s.b.remote.Write(proto.NewEvent(internal, proto.ChEof, nil))
		}
		s.b.mu.Lock()
		delete(s.b.sessions, s)
		s.b.mu.Unlock()
	})
}
