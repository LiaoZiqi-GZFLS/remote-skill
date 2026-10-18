package daemon

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"remote-skill/internal/proto"
)

// isSessionMethod reports whether a method is scoped to a session and must be
// served by that session's keeper process instead of the host daemon.
func isSessionMethod(method string) bool {
	switch method {
	case proto.MethodExec,
		proto.MethodSessionOpen,
		proto.MethodSessionClose,
		proto.MethodSessionAttach,
		proto.MethodSessionResize,
		proto.MethodSessionSignal,
		proto.MethodSessionScrollback:
		return true
	}
	return false
}

// keeperHandle is the daemon's connection to one session keeper.
type keeperHandle struct {
	mgr    *KeeperManager
	name   string
	socket string
	pid    int

	mu     sync.Mutex
	conn   *proto.Conn
	routes map[string]*proxyRoute
	info   SessionInfo
	infoAt time.Time
	closed bool

	done     chan struct{}
	closeOne sync.Once
	seq      atomic.Int64
}

func (h *keeperHandle) write(env *proto.Envelope) error {
	h.mu.Lock()
	conn := h.conn
	closed := h.closed
	h.mu.Unlock()
	if closed || conn == nil {
		return fmt.Errorf("session %q is not connected", h.name)
	}
	return conn.Write(env)
}

func (h *keeperHandle) nextRequestID() string {
	return fmt.Sprintf("k%d", h.seq.Add(1))
}

func (h *keeperHandle) addRoute(r *proxyRoute) {
	h.mu.Lock()
	h.routes[r.keeperID] = r
	h.mu.Unlock()
}

func (h *keeperHandle) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// call performs a request/response exchange with the keeper, bypassing the
// client routing table. Used for state refreshes.
func (h *keeperHandle) call(method string, params any, timeout time.Duration) (*proto.Envelope, error) {
	id := h.nextRequestID()
	env, err := proto.NewRequest(id, method, params)
	if err != nil {
		return nil, err
	}
	reply := make(chan *proto.Envelope, 1)
	r := &proxyRoute{handle: h, keeperID: id, method: method, internal: reply}
	h.addRoute(r)
	if err := h.write(env); err != nil {
		r.complete()
		return nil, err
	}
	select {
	case resp := <-reply:
		return resp, nil
	case <-time.After(timeout):
		r.complete()
		return nil, proto.Errorf(proto.CodeTimeout, "session %q did not answer %s", h.name, method)
	case <-h.done:
		return nil, proto.Errorf(proto.CodeClosed, "session %q stopped", h.name)
	}
}

// refresh asks the keeper for current session state and updates the cache.
func (h *keeperHandle) refresh() (SessionInfo, error) {
	resp, err := h.call(proto.MethodSessionInfo, map[string]any{}, 3*time.Second)
	if err != nil {
		return h.infoCached(), err
	}
	if resp.Err != nil {
		return h.infoCached(), resp.Err
	}
	var info SessionInfo
	if err := resp.DecodeResult(&info); err != nil {
		return h.infoCached(), err
	}
	h.cacheInfo(info)
	return info, nil
}

func (h *keeperHandle) cacheInfo(info SessionInfo) {
	h.mu.Lock()
	h.info = info
	h.infoAt = time.Now()
	h.mu.Unlock()
}

// noteExec folds an exec result into the cached session state.
func (h *keeperHandle) noteExec(raw json.RawMessage) {
	var res ExecResult
	if len(raw) == 0 || json.Unmarshal(raw, &res) != nil {
		return
	}
	h.mu.Lock()
	if res.Cwd != "" {
		h.info.Cwd = res.Cwd
	}
	h.info.Busy = false
	h.info.Commands++
	h.info.LastActive = time.Now()
	h.infoAt = time.Now()
	last := res
	h.info.Last = &last
	h.mu.Unlock()
}

// noteScrollback folds a scrollback reply into the cached session state.
func (h *keeperHandle) noteScrollback(raw json.RawMessage) {
	var p struct {
		Session   string      `json:"session"`
		Cwd       string      `json:"cwd"`
		Commands  int         `json:"commands"`
		Busy      bool        `json:"busy"`
		StartedAt time.Time   `json:"started_at"`
		Last      *ExecResult `json:"last_exec"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &p) != nil {
		return
	}
	h.mu.Lock()
	if p.Session != "" {
		h.info.Name = p.Session
	}
	if p.Cwd != "" {
		h.info.Cwd = p.Cwd
	}
	h.info.Commands = p.Commands
	h.info.Busy = p.Busy
	if !p.StartedAt.IsZero() {
		h.info.StartedAt = p.StartedAt
	}
	if p.Last != nil {
		h.info.Last = p.Last
	}
	h.infoAt = time.Now()
	h.mu.Unlock()
}

// infoCached returns the last known session state.
func (h *keeperHandle) infoCached() SessionInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.info
}

// route registers a proxied client request and forwards it to the keeper.
func (h *keeperHandle) route(c *conn, env *proto.Envelope, stream bool) {
	r := &proxyRoute{
		handle:   h,
		client:   c,
		clientID: env.ID,
		keeperID: h.nextRequestID(),
		method:   env.Method,
		stream:   stream,
	}
	h.addRoute(r)
	c.setProxyRoute(env.ID, r)

	out := *env
	out.ID = r.keeperID
	if err := h.write(&out); err != nil {
		r.complete()
		c.fail(env.ID, proto.Errorf(proto.CodeClosed, "session %q is not reachable: %v", h.name, err))
	}
}

// readLoop relays keeper frames back to the clients that own the routes.
func (h *keeperHandle) readLoop() {
	for {
		env, err := h.readConn()
		if err != nil {
			h.fail(fmt.Errorf("session %q stopped: %v", h.name, err))
			return
		}
		h.mu.Lock()
		r := h.routes[env.ID]
		h.mu.Unlock()
		if r == nil {
			continue
		}
		r.dispatch(env)
	}
}

func (h *keeperHandle) readConn() (*proto.Envelope, error) {
	h.mu.Lock()
	conn := h.conn
	h.mu.Unlock()
	if conn == nil {
		return nil, fmt.Errorf("keeper connection closed")
	}
	return conn.Read()
}

// fail reports a keeper failure to every waiting client.
func (h *keeperHandle) fail(err error) {
	h.closeOne.Do(func() {
		h.mu.Lock()
		h.closed = true
		routes := make([]*proxyRoute, 0, len(h.routes))
		for _, r := range h.routes {
			routes = append(routes, r)
		}
		h.routes = map[string]*proxyRoute{}
		conn := h.conn
		h.conn = nil
		h.mu.Unlock()
		close(h.done)
		if conn != nil {
			_ = conn.Write(&proto.Envelope{Type: proto.TypeBye})
		}
		for _, r := range routes {
			r.failClient(proto.Errorf(proto.CodeClosed, "%v", err))
		}
		h.mgr.forget(h.name, h)
	})
}

// close detaches from the keeper. The keeper process keeps running.
func (h *keeperHandle) close() { h.fail(fmt.Errorf("detached from session %q", h.name)) }

// proxyRoute links one client request to one keeper request.
type proxyRoute struct {
	handle   *keeperHandle
	client   *conn
	clientID string
	keeperID string
	method   string
	stream   bool
	internal chan *proto.Envelope
	once     sync.Once
}

// forwardToKeeper relays a client frame (stdin, resize, signal, eof).
func (r *proxyRoute) forwardToKeeper(env *proto.Envelope) {
	out := *env
	out.ID = r.keeperID
	if err := r.handle.write(&out); err != nil {
		r.failClient(proto.Errorf(proto.CodeClosed, "session %q is not reachable: %v", r.handle.name, err))
	}
}

// dispatch handles one frame coming back from the keeper.
func (r *proxyRoute) dispatch(env *proto.Envelope) {
	if r.internal != nil {
		select {
		case r.internal <- env:
		default:
		}
		if env.Type == proto.TypeRes {
			r.complete()
		}
		return
	}
	switch env.Type {
	case proto.TypeRes:
		if env.Err != nil {
			r.client.fail(r.clientID, env.Err)
			r.complete()
			return
		}
		switch r.method {
		case proto.MethodExec:
			r.handle.noteExec(env.Result)
		case proto.MethodSessionScrollback:
			r.handle.noteScrollback(env.Result)
		}
		r.client.respondRaw(r.clientID, env.Result)
		if !r.stream {
			r.complete()
		}
	case proto.TypeEvent:
		r.client.event(r.clientID, env.Chan, env.Data, env.Extra)
		// Only streamed requests end on a done event; a plain request such as
		// exec still has its final response frame on the way.
		if env.Chan == proto.ChDone && r.stream {
			r.complete()
		}
	}
}

// failClient reports a failure for a proxied request.
func (r *proxyRoute) failClient(err *proto.Error) {
	if r.internal != nil {
		select {
		case r.internal <- &proto.Envelope{Type: proto.TypeRes, Err: err}:
		default:
		}
		r.complete()
		return
	}
	r.client.fail(r.clientID, err)
	r.complete()
}

// detachClient is called when the client connection goes away: streamed
// requests are cancelled, plain requests are forgotten (the command keeps
// running inside the keeper).
func (r *proxyRoute) detachClient() {
	if r.stream {
		_ = r.handle.write(proto.NewEvent(r.keeperID, proto.ChEof, nil))
	}
	r.complete()
}

func (r *proxyRoute) complete() {
	r.once.Do(func() {
		r.handle.mu.Lock()
		delete(r.handle.routes, r.keeperID)
		r.handle.mu.Unlock()
		if r.client != nil {
			r.client.clearProxyRoute(r.clientID)
		}
	})
}
