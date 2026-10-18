package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"remote-skill/internal/assets"
	"remote-skill/internal/daemon"
	"remote-skill/internal/proto"
	"remote-skill/internal/version"
)

// ConnectOptions controls how a connection is established.
type ConnectOptions struct {
	Restart   bool
	Force     bool
	Session   string
	Shell     string
	Env       map[string]string
	NoProfile bool
	// Quiet suppresses bootstrap progress messages.
	Quiet bool
}

// EventHandler receives streaming events for a request.
type EventHandler func(*proto.Envelope) error

// errDaemonStale means the remote daemon is running a different build.
var errDaemonStale = errors.New("remote daemon is running a different build")

type reqEntry struct {
	open    bool
	handler EventHandler
	ackCh   chan *proto.Envelope
	finCh   chan *proto.Envelope
	ackOnce sync.Once
	finOnce sync.Once
}

func (e *reqEntry) deliverAck(env *proto.Envelope) {
	e.ackOnce.Do(func() {
		e.ackCh <- env
		close(e.ackCh)
	})
}

func (e *reqEntry) finish(env *proto.Envelope) {
	e.finOnce.Do(func() {
		e.finCh <- env
		close(e.finCh)
	})
}

// Client is a connection to a remote daemon.
type Client struct {
	Host    *Host
	Cfg     *Config
	SSH     *SSH
	Info    *daemon.HostInfo
	Verbose bool
	Logw    io.Writer
	// Per-invocation overrides; they never touch the stored host config.
	SessionOverride string
	ShellOverride   string
	EnvOverride     map[string]string
	InlineOverride  bool
	// NoBroker disables the local connection multiplexer for this run.
	NoBroker bool
	// UseBroker enables the (experimental) local connection multiplexer.
	UseBroker bool
	// forceDirect / noReadLoop are set by the broker process itself.
	forceDirect bool
	noReadLoop  bool

	conn *proto.Conn
	proc *exec.Cmd

	mu      sync.Mutex
	reqs    map[string]*reqEntry
	closed  bool
	closeEr error
	done    chan struct{}
	seq     atomic.Int64
}

// NewClient prepares a client for a host.
func NewClient(h *Host, cfg *Config) *Client {
	c := &Client{
		Host: h,
		Cfg:  cfg,
		reqs: map[string]*reqEntry{},
		done: make(chan struct{}),
		Logw: os.Stderr,
	}
	c.SSH = NewSSH(h)
	return c
}

func (c *Client) logf(format string, args ...any) {
	if c.Verbose && c.Logw != nil {
		fmt.Fprintf(c.Logw, "[rsk] "+format+"\n", args...)
	}
}

// Connect bootstraps (if needed), starts the remote daemon and completes the
// protocol handshake.
func (c *Client) Connect(ctx context.Context, opts ConnectOptions) error {
	if err := c.Bootstrap(ctx, opts); err != nil {
		return err
	}
	err := c.dial(ctx, opts)
	switch {
	case errors.Is(err, errDaemonStale):
		if !opts.Quiet {
			fmt.Fprintf(c.Logw, "restarting the daemon on %s (client and server builds differ)\n", c.Host.Name)
		}
		c.cleanup()
		// Do not let the local broker replay the old daemon: stop it and
		// restart the daemon over a direct connection for this run.
		StopBroker(c.Host)
		c.forceDirect = true
		opts.Restart = true
		if err = c.dial(ctx, opts); err != nil {
			return err
		}
	case err != nil:
		// The cached view of the remote was wrong (binary removed, daemon
		// killed, host rebuilt): refresh and try once more.
		c.logf("connect failed (%v), re-bootstrapping", err)
		c.cleanup()
		opts.Force = true
		if err := c.Bootstrap(ctx, opts); err != nil {
			return err
		}
		err = c.dial(ctx, opts)
		if err != nil {
			return err
		}
	}
	if opts.Session != "" || opts.Shell != "" || opts.Env != nil || opts.NoProfile {
		if _, err := c.OpenSession(ctx, daemon.SessionOptions{
			Name:      opts.Session,
			Shell:     opts.Shell,
			Env:       opts.Env,
			NoProfile: opts.NoProfile,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) dial(ctx context.Context, opts ConnectOptions) error {
	if conn, nc, ok := c.dialBroker(ctx); ok {
		c.conn = conn
		herr := c.handshake(ctx, opts.Restart)
		if herr == nil {
			herr = brokerPing(conn, nc, 3*time.Second)
		}
		if herr != nil {
			nc.Close()
			// The broker may be shutting down (e.g. right after `rsk down`).
			// Drop it and use a direct connection for this run.
			c.logf("broker unhealthy (%v); falling back to a direct connection", herr)
			StopBroker(c.Host)
			c.conn = nil
			c.forceDirect = true
		} else {
			go c.readLoop()
			return nil
		}
	}
	flags := []string{
		c.Host.RemoteBin, "bridge",
		"--socket", c.Host.SocketPath(),
		"--log", c.Host.LogPath(),
	}
	if c.Host.RemoteOS == "windows" {
		// The uploaded launcher already knows the socket and log paths, and
		// cmd/PowerShell both execute a single quoted path.
		launcher := strings.TrimSuffix(c.Host.RemoteBin, filepath.Ext(c.Host.RemoteBin))
		launcher = filepath.Join(filepath.Dir(launcher), "bridge.cmd")
		flags = []string{`"` + launcher + `"`}
	}
	if c.Host.Inline {
		flags = append(flags, "--inline-sessions")
	}
	if c.InlineOverride {
		flags = append(flags, "--inline-sessions")
	}
	if opts.Restart {
		flags = append(flags, "--restart")
	}
	shell := c.ShellOverride
	if shell == "" {
		shell = c.Host.Shell
	}
	if shell != "" {
		flags = append(flags, "--shell", shell)
	}
	for k, v := range c.Host.Env {
		flags = append(flags, "--env", k+"="+v)
	}
	for k, v := range c.EnvOverride {
		flags = append(flags, "--env", k+"="+v)
	}
	remote := QuoteJoin(flags...)

	proc, stdin, stdout, err := c.SSH.Start(ctx, remote)
	if err != nil {
		return fmt.Errorf("start ssh: %w", err)
	}
	c.proc = proc
	c.conn = proto.NewConn(stdout, stdin)

	// A wedged daemon must not hang the client forever.
	hsDone := make(chan error, 1)
	go func() { hsDone <- c.handshake(ctx, opts.Restart) }()
	select {
	case herr := <-hsDone:
		if herr != nil {
			return herr
		}
	case <-time.After(20 * time.Second):
		return fmt.Errorf("remote daemon did not answer the handshake (hint: `rsk up %s --restart`)", c.Host.Name)
	}
	if !c.noReadLoop {
		go c.readLoop()
	}
	return nil
}

// handshake greets the daemon (or the local broker) and caches host info.
func (c *Client) handshake(ctx context.Context, allowStale bool) error {
	hello := map[string]any{
		"version": version.Version,
		"api":     proto.APIVersion,
		"client":  "rsk",
		"pid":     os.Getpid(),
	}
	env, err := proto.NewRequest("hello", "", hello)
	if err != nil {
		return err
	}
	env.Type = proto.TypeHello
	if err := c.conn.Write(env); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}
	welcome, err := c.conn.Read()
	if err != nil {
		return fmt.Errorf("remote daemon handshake failed: %w (hint: run `rsk doctor %s`)", err, c.Host.Name)
	}
	switch welcome.Type {
	case proto.TypeWelcome:
		var info daemon.HostInfo
		if err := welcome.DecodeResult(&info); err != nil {
			return fmt.Errorf("decode daemon info: %w", err)
		}
		c.Info = &info
		// An empty BinarySHA means a daemon started before upgrade detection
		// existed, so it is outdated by definition.
		if want := c.assetSHA(); want != "" && info.BinarySHA != want && !allowStale {
			return fmt.Errorf("%w (daemon %s, client %s)", errDaemonStale, shortSHA(info.BinarySHA), shortSHA(want))
		}
	case proto.TypeBye, proto.TypeRes:
		if welcome.Err != nil {
			if welcome.Err.Code == proto.CodeVersion {
				return fmt.Errorf("%s (run `rsk up %s --restart` to upgrade the remote daemon)", welcome.Err.Error(), c.Host.Name)
			}
			return fmt.Errorf("daemon refused connection: %s", welcome.Err.Error())
		}
		return errors.New("daemon refused connection")
	default:
		return fmt.Errorf("unexpected handshake reply %q", welcome.Type)
	}
	return nil
}

// assetSHA is the digest of the daemon build this client would upload.
func (c *Client) assetSHA() string {
	if c.Host.RemoteOS == "" || c.Host.RemoteArch == "" {
		return ""
	}
	data, _, ok := assets.Binary(c.Host.RemoteOS, c.Host.RemoteArch)
	if !ok {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func shortSHA(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

func (c *Client) readLoop() {
	for {
		env, err := c.conn.Read()
		if err != nil {
			c.failAll(fmt.Errorf("connection closed: %w", err))
			return
		}
		switch env.Type {
		case proto.TypeRes:
			c.mu.Lock()
			e := c.reqs[env.ID]
			c.mu.Unlock()
			if e == nil {
				continue
			}
			if e.open {
				if env.Err != nil {
					e.deliverAck(env)
					e.finish(env)
					c.remove(env.ID)
					continue
				}
				select {
				case <-e.ackCh:
					// Already acknowledged: this is a late final result.
					e.finish(env)
					c.remove(env.ID)
				default:
					e.deliverAck(env)
				}
				continue
			}
			e.finish(env)
			c.remove(env.ID)
		case proto.TypeEvent:
			c.mu.Lock()
			e := c.reqs[env.ID]
			c.mu.Unlock()
			if e == nil {
				continue
			}
			if e.handler != nil {
				if err := e.handler(env); err != nil {
					e.finish(&proto.Envelope{Type: proto.TypeRes, ID: env.ID, Err: proto.Errorf(proto.CodeIO, "%v", err)})
					c.remove(env.ID)
					continue
				}
			}
			// ChDone terminates streamed requests (ack + events); plain
			// requests finish on the response envelope that follows.
			if env.Chan == proto.ChDone && e.open {
				e.finish(env)
				c.remove(env.ID)
			}
		case proto.TypeBye:
			e := proto.Errorf(proto.CodeClosed, "daemon closed the connection")
			if env.Err != nil {
				e = env.Err
			}
			c.failAll(e)
			return
		}
	}
}

func (c *Client) failAll(err error) {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		c.closeEr = err
		close(c.done)
	}
	reqs := make([]*reqEntry, 0, len(c.reqs))
	for _, e := range c.reqs {
		reqs = append(reqs, e)
	}
	c.reqs = map[string]*reqEntry{}
	c.mu.Unlock()
	for _, e := range reqs {
		env := &proto.Envelope{Type: proto.TypeRes, Err: proto.Errorf(proto.CodeClosed, "%v", err)}
		e.deliverAck(env)
		e.finish(env)
	}
}

func (c *Client) remove(id string) {
	c.mu.Lock()
	delete(c.reqs, id)
	c.mu.Unlock()
}

// Close shuts the connection down. The remote daemon and its sessions keep
// running.
func (c *Client) Close() error {
	c.mu.Lock()
	conn := c.conn
	proc := c.proc
	closed := c.closed
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Write(&proto.Envelope{Type: proto.TypeBye})
	}
	if !closed {
		c.failAll(proto.Errorf(proto.CodeClosed, "client closed"))
	}
	if proc != nil && proc.Process != nil {
		done := make(chan struct{})
		go func() {
			_ = proc.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(1500 * time.Millisecond):
			_ = proc.Process.Kill()
			<-done
		}
	}
	return nil
}

func (c *Client) cleanup() {
	c.mu.Lock()
	proc := c.proc
	c.mu.Unlock()
	if proc != nil && proc.Process != nil {
		_ = proc.Process.Kill()
		_, _ = proc.Process.Wait()
	}
}

// Done is closed when the connection is gone.
func (c *Client) Done() <-chan struct{} { return c.done }

// Err reports why the connection ended.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeEr
}

func (c *Client) newEntry(open bool, h EventHandler) (*reqEntry, string) {
	id := fmt.Sprintf("r%d", c.seq.Add(1))
	e := &reqEntry{
		open:    open,
		handler: h,
		ackCh:   make(chan *proto.Envelope, 1),
		finCh:   make(chan *proto.Envelope, 4),
	}
	c.mu.Lock()
	c.reqs[id] = e
	c.mu.Unlock()
	return e, id
}

func (c *Client) waitDone(e *reqEntry) error {
	select {
	case <-e.finCh:
		return nil
	case <-c.done:
		return c.Err()
	}
}

// Call performs a request/response exchange without streaming.
func (c *Client) Call(ctx context.Context, method string, params any, result any) error {
	// Never let a wedged daemon hang a short request forever.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	env, err := c.StreamWait(ctx, method, params, nil)
	if err != nil {
		return err
	}
	if result != nil && len(env.Result) > 0 {
		return env.DecodeResult(result)
	}
	return nil
}

// StreamWait sends a request, forwards events to h and returns the final
// response envelope.
func (c *Client) StreamWait(ctx context.Context, method string, params any, h EventHandler) (*proto.Envelope, error) {
	e, id := c.newEntry(false, h)
	defer c.remove(id)
	req, err := proto.NewRequest(id, method, params)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Write(req); err != nil {
		return nil, err
	}
	select {
	case env := <-e.finCh:
		if env.Err != nil {
			return env, env.Err
		}
		return env, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		if err := c.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("connection closed")
	}
}

// Stream is an open, streamed request (attach, download, tunnel).
type Stream struct {
	c   *Client
	e   *reqEntry
	id  string
	ack *proto.Envelope
}

// Open sends a request and waits for the acknowledgement.
func (c *Client) Open(ctx context.Context, method string, params any, h EventHandler) (*Stream, error) {
	e, id := c.newEntry(true, h)
	req, err := proto.NewRequest(id, method, params)
	if err != nil {
		c.remove(id)
		return nil, err
	}
	if err := c.conn.Write(req); err != nil {
		c.remove(id)
		return nil, err
	}
	select {
	case env := <-e.ackCh:
		if env.Err != nil {
			c.remove(id)
			return nil, env.Err
		}
		return &Stream{c: c, e: e, id: id, ack: env}, nil
	case <-ctx.Done():
		c.remove(id)
		return nil, ctx.Err()
	case <-c.done:
		c.remove(id)
		if err := c.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("connection closed")
	}
}

// ID is the request identifier.
func (s *Stream) ID() string { return s.id }

// Send writes a data frame to the daemon.
func (s *Stream) Send(channel string, data []byte) error {
	return s.SendExtra(channel, data, nil)
}

// SendExtra writes a frame with extra metadata.
func (s *Stream) SendExtra(channel string, data []byte, extra map[string]any) error {
	env := proto.NewEvent(s.id, channel, data)
	env.Extra = extra
	return s.c.conn.Write(env)
}

// Wait blocks until the stream finished (done event, error or disconnect).
func (s *Stream) Wait(ctx context.Context) (*proto.Envelope, error) {
	select {
	case env := <-s.e.finCh:
		s.c.remove(s.id)
		if env.Err != nil {
			return env, env.Err
		}
		return env, nil
	case <-ctx.Done():
		s.c.remove(s.id)
		return nil, ctx.Err()
	case <-s.c.done:
		s.c.remove(s.id)
		if err := s.c.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("connection closed")
	}
}

// Close abandons the stream, telling the daemon to stop sending.
func (s *Stream) Close() {
	_ = s.Send(proto.ChEof, nil)
	s.c.remove(s.id)
}

// Info returns the daemon host details learned during the handshake.
func (c *Client) HostInfo() *daemon.HostInfo { return c.Info }

// SessionName resolves the session to use.
func (c *Client) SessionName(override string) string {
	if override != "" {
		return override
	}
	if c.SessionOverride != "" {
		return c.SessionOverride
	}
	return c.Host.SessionName()
}
