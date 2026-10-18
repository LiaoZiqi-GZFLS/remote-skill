package client

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"remote-skill/internal/proto"
)

// ForwardSpec describes one port forward.
type ForwardSpec struct {
	// BindHost/BindPort are the listening side (local for -L, remote for -R).
	BindHost   string
	BindPort   int
	TargetHost string
	TargetPort int
	Reverse    bool
}

// ListenAddress renders the address the forward listens on.
func (f ForwardSpec) ListenAddress() string {
	return net.JoinHostPort(f.BindHost, strconv.Itoa(f.BindPort))
}

// Target renders host:port of the service the tunnel connects to.
func (f ForwardSpec) Target() string {
	return net.JoinHostPort(f.TargetHost, strconv.Itoa(f.TargetPort))
}

// ParseForward understands the usual ssh -L shorthands:
//
//	8080                      local 8080 -> remote 8080
//	8080:3000                 local 8080 -> remote localhost:3000
//	8080:db.internal:5432     explicit remote host
//	0.0.0.0:8080:localhost:3000  explicit local bind address
func ParseForward(spec, bind string) (ForwardSpec, error) {
	parts := strings.Split(strings.TrimSpace(spec), ":")
	var localHost, localPort, remoteHost, remotePort string
	switch len(parts) {
	case 1:
		localPort, remoteHost, remotePort = parts[0], "localhost", parts[0]
	case 2:
		localPort, remoteHost, remotePort = parts[0], "localhost", parts[1]
	case 3:
		localPort, remoteHost, remotePort = parts[0], parts[1], parts[2]
	case 4:
		localHost, localPort, remoteHost, remotePort = parts[0], parts[1], parts[2], parts[3]
	default:
		return ForwardSpec{}, fmt.Errorf("cannot parse forward spec %q", spec)
	}
	if localHost == "" {
		localHost = bind
	}
	if remoteHost == "" {
		remoteHost = "localhost"
	}
	lp, err := strconv.Atoi(localPort)
	if err != nil || lp <= 0 || lp > 65535 {
		return ForwardSpec{}, fmt.Errorf("invalid local port %q", localPort)
	}
	rp, err := strconv.Atoi(remotePort)
	if err != nil || rp <= 0 || rp > 65535 {
		return ForwardSpec{}, fmt.Errorf("invalid remote port %q", remotePort)
	}
	return ForwardSpec{
		BindHost:   localHost,
		BindPort:   lp,
		TargetHost: remoteHost,
		TargetPort: rp,
	}, nil
}

// ParseReverse parses the ssh -R shorthands (remote listens, local serves):
//
//	8080                        remote 8080 -> local 8080
//	8080:3000                   remote 8080 -> local localhost:3000
//	8080:host.internal:3000     remote 8080 -> local host.internal:3000
//	0.0.0.0:8080:localhost:3000 remote 0.0.0.0:8080 -> local localhost:3000
func ParseReverse(spec, bind string) (ForwardSpec, error) {
	f, err := ParseForward(spec, bind)
	if err != nil {
		return ForwardSpec{}, err
	}
	f.Reverse = true
	return f, nil
}

// Forward listens locally and tunnels every connection to the server side.
func Forward(ctx context.Context, c *Client, spec ForwardSpec) error {
	ln, err := net.Listen("tcp", spec.ListenAddress())
	if err != nil {
		return err
	}
	defer ln.Close()
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go forwardConn(ctx, c, conn, spec)
	}
}

func forwardConn(ctx context.Context, c *Client, conn net.Conn, spec ForwardSpec) {
	defer conn.Close()
	stream, err := c.Open(ctx, "tunnel.open", map[string]any{
		"target":     spec.Target(),
		"timeout_ms": 15000,
	}, func(ev *proto.Envelope) error {
		switch ev.Chan {
		case proto.ChOut:
			_, err := conn.Write(ev.Data)
			return err
		case proto.ChEof:
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.CloseWrite()
			}
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(c.Logw, "rsk: forward %s failed: %v\n", spec.Target(), err)
		return
	}
	defer stream.Close()
	buf := make([]byte, 32<<10)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			if werr := stream.Send(proto.ChIn, buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				// The local side closed; tell the tunnel to finish.
			}
			_ = stream.Send(proto.ChEof, nil)
			break
		}
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = stream.Wait(waitCtx)
}

// ReverseForward listens on the remote host and tunnels each accepted
// connection back here, where it is dialled on the local network. This is the
// ssh -R direction: useful for webhooks, local services behind NAT, or letting
// a remote host reach a database or API that only exists on this machine.
func ReverseForward(ctx context.Context, c *Client, spec ForwardSpec) error {
	events := make(chan *proto.Envelope, 128)
	stream, err := c.Open(ctx, proto.MethodTunnelListen, map[string]any{
		"bind": spec.BindHost,
		"port": spec.BindPort,
	}, func(ev *proto.Envelope) error {
		select {
		case events <- ev:
		case <-ctx.Done():
		}
		return nil
	})
	if err != nil {
		return err
	}
	defer stream.Close()

	var ack struct {
		Listening string `json:"listening"`
	}
	if err := decodeAck(stream, &ack); err == nil && ack.Listening != "" {
		fmt.Fprintf(c.Logw, "rsk: remote %s forwards to %s\n", ack.Listening, spec.Target())
	}

	conns := map[string]net.Conn{}
	var mu sync.Mutex
	defer func() {
		mu.Lock()
		for id, conn := range conns {
			_ = conn.Close()
			delete(conns, id)
		}
		mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.Done():
			return c.Err()
		case ev := <-events:
			id, _ := ev.Extra["conn"].(string)
			switch ev.Chan {
			case proto.ChOpen:
				conn, err := net.DialTimeout("tcp", spec.Target(), 10*time.Second)
				if err != nil {
					fmt.Fprintf(c.Logw, "rsk: remote connection %s could not reach %s: %v\n", id, spec.Target(), err)
					_ = stream.SendExtra(proto.ChClose, nil, map[string]any{"conn": id})
					continue
				}
				mu.Lock()
				conns[id] = conn
				mu.Unlock()
				go pumpLocalToRemote(stream, id, conn, &mu, conns)
			case proto.ChOut:
				mu.Lock()
				conn := conns[id]
				mu.Unlock()
				if conn != nil {
					if _, err := conn.Write(ev.Data); err != nil {
						_ = stream.SendExtra(proto.ChClose, nil, map[string]any{"conn": id})
					}
				}
			case proto.ChClose, proto.ChEof:
				mu.Lock()
				conn := conns[id]
				delete(conns, id)
				mu.Unlock()
				if conn != nil {
					_ = conn.Close()
				}
			case proto.ChDone:
				return nil
			}
		}
	}
}

// pumpLocalToRemote forwards one local connection into the tunnel.
func pumpLocalToRemote(stream *Stream, id string, conn net.Conn, mu *sync.Mutex, conns map[string]net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			if werr := stream.SendExtra(proto.ChIn, buf[:n], map[string]any{"conn": id}); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = stream.SendExtra(proto.ChEof, nil, map[string]any{"conn": id})
	mu.Lock()
	_, still := conns[id]
	delete(conns, id)
	mu.Unlock()
	_ = conn.Close()
	if still {
		// nothing else to do: the daemon closes its side on eof
	}
}
