package daemon

import (
	"context"
	"time"

	"remote-skill/internal/proto"
)

// responder is the subset of a client connection the session handlers need.
// Both the daemon (which proxies to keepers) and a keeper process (which owns
// the session) implement it, so the handlers live in exactly one place.
type responder interface {
	respond(id string, result any)
	fail(id string, err *proto.Error)
	event(id, channel string, data []byte, extra map[string]any)
	isClosed() bool
	closedCh() <-chan struct{}
}

// sessionNameFrom peeks at the session name without consuming the request.
func sessionNameFrom(env *proto.Envelope) string {
	var p struct {
		Session string `json:"session"`
		Name    string `json:"name"`
	}
	_ = env.DecodeParams(&p)
	if p.Session != "" {
		return p.Session
	}
	return p.Name
}

// serveExec runs a command in a session and streams its output.
func serveExec(rsp responder, sess *Session, req *request, env *proto.Envelope) {
	var p ExecRequest
	if err := env.DecodeParams(&p); err != nil {
		rsp.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	sink := &responderSink{rsp: rsp, id: env.ID}
	defer sink.detach()
	res, err := sess.Run(context.Background(), p, sink)
	if err != nil {
		rsp.fail(env.ID, asProto(err))
		return
	}
	rsp.respond(env.ID, res)
}

// serveAttach streams a session to an interactive client and forwards input.
func serveAttach(rsp responder, sess *Session, req *request, env *proto.Envelope) {
	var p struct {
		Session     string `json:"session"`
		ReplayBytes int    `json:"replay_bytes"`
		Rows        uint16 `json:"rows"`
		Cols        uint16 `json:"cols"`
	}
	if err := env.DecodeParams(&p); err != nil {
		rsp.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	if p.ReplayBytes == 0 {
		p.ReplayBytes = 256 << 10
	}
	att, err := sess.Attach(p.ReplayBytes)
	if err != nil {
		rsp.fail(env.ID, asProto(err))
		return
	}
	defer att.Close()
	if p.Rows > 0 {
		_ = att.Resize(p.Rows, p.Cols)
	}
	rsp.respond(env.ID, map[string]any{"session": sess.Info(), "attached": true})

	sink := &responderSink{rsp: rsp, id: env.ID}
	defer sink.detach()
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		for ev := range att.Events() {
			sink.publish(ev)
		}
	}()
	defer func() {
		// An explicit terminal frame lets a proxying daemon release the route
		// instead of waiting for the connection to close.
		rsp.event(env.ID, proto.ChDone, nil, nil)
	}()
	for {
		select {
		case <-rsp.closedCh():
			return
		case <-sess.Done():
			return
		case <-pumpDone:
			return
		case in := <-req.in:
			switch in.Chan {
			case proto.ChIn:
				if _, err := att.Write(in.Data); err != nil {
					return
				}
			case proto.ChSignal:
				sig, _ := in.Extra["sig"].(string)
				if sig == "" {
					sig = "INT"
				}
				_ = att.Signal(sig)
			case proto.ChMeta:
				rows, cols := extraUint16(in.Extra, "rows"), extraUint16(in.Extra, "cols")
				if rows > 0 && cols > 0 {
					_ = att.Resize(rows, cols)
				}
			case proto.ChEof:
				return
			}
		}
	}
}

// serveResize changes the pty size of a session.
func serveResize(rsp responder, sess *Session, env *proto.Envelope) {
	var p struct {
		Session string `json:"session"`
		Rows    uint16 `json:"rows"`
		Cols    uint16 `json:"cols"`
	}
	if err := env.DecodeParams(&p); err != nil {
		rsp.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	if err := sess.Resize(p.Rows, p.Cols); err != nil {
		rsp.fail(env.ID, asProto(err))
		return
	}
	rsp.respond(env.ID, map[string]any{"resized": true, "rows": p.Rows, "cols": p.Cols})
}

// serveSignal delivers a signal to the foreground job of a session.
func serveSignal(rsp responder, sess *Session, env *proto.Envelope) {
	var p struct {
		Session string `json:"session"`
		Sig     string `json:"sig"`
	}
	if err := env.DecodeParams(&p); err != nil {
		rsp.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	if err := sess.Signal(p.Sig); err != nil {
		rsp.fail(env.ID, asProto(err))
		return
	}
	rsp.respond(env.ID, map[string]any{"signalled": p.Sig})
}

// serveScrollback returns recent terminal output plus session state.
func serveScrollback(rsp responder, sess *Session, env *proto.Envelope) {
	var p struct {
		Session string `json:"session"`
		Tail    int    `json:"tail"`
	}
	if err := env.DecodeParams(&p); err != nil {
		rsp.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	data := sess.Scrollback(p.Tail)
	info := sess.Info()
	rsp.respond(env.ID, map[string]any{
		"session":    info.Name,
		"cwd":        info.Cwd,
		"data":       data,
		"bytes":      len(data),
		"commands":   info.Commands,
		"last_exec":  info.Last,
		"busy":       info.Busy,
		"started_at": info.StartedAt,
	})
}

// responderSink adapts a responder into a session Sink.
type responderSink struct {
	rsp  responder
	id   string
	dead bool
}

func (s *responderSink) Event(ev Event) { s.publish(ev) }

func (s *responderSink) publish(ev Event) {
	if s.dead {
		return
	}
	extra := map[string]any(nil)
	if ev.ExecID != "" || ev.Session != "" || ev.Seq != 0 {
		extra = map[string]any{"session": ev.Session, "seq": ev.Seq}
		if ev.ExecID != "" {
			extra["exec"] = ev.ExecID
		}
	}
	s.rsp.event(s.id, ev.Chan, ev.Data, extra)
}

// detach stops streaming to a client that is no longer interested; the
// command keeps running inside the session.
func (s *responderSink) detach() { s.dead = true }

// closedCh reports when the peer is gone, so handlers can select on it.
func (c *conn) closedCh() <-chan struct{} { return c.closed }

// waitReady bounds session startup for a client request.
func waitReady(sess *Session, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return sess.WaitReady(ctx)
}
