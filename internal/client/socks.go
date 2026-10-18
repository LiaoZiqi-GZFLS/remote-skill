package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"remote-skill/internal/proto"
)

// SOCKS runs a local SOCKS5 proxy (no auth, CONNECT only) whose connections
// are dialled from the server through the existing tunnel stream. It is the
// `ssh -D` shape: browsers and tools can treat the remote network as local.
func SOCKS(ctx context.Context, c *Client, listen string) error {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	defer ln.Close()
	fmt.Fprintf(c.Logw, "rsk: socks5 proxy on %s (via %s)\n", ln.Addr(), c.Host.Name)
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
		go socksConn(ctx, c, conn)
	}
}

// socksConn performs the SOCKS5 handshake for one local connection.
func socksConn(ctx context.Context, c *Client, conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	var hdr [2]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil || hdr[0] != 5 {
		return
	}
	methods := make([]byte, hdr[1])
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}
	if _, err := conn.Write([]byte{5, 0}); err != nil { // no authentication
		return
	}

	var req [4]byte
	if _, err := io.ReadFull(conn, req[:]); err != nil || req[0] != 5 {
		return
	}
	if req[1] != 1 { // CONNECT only
		socksReply(conn, 7)
		return
	}
	var host string
	switch req[3] {
	case 1:
		var ip [4]byte
		if _, err := io.ReadFull(conn, ip[:]); err != nil {
			return
		}
		host = net.IP(ip[:]).String()
	case 3:
		var n [1]byte
		if _, err := io.ReadFull(conn, n[:]); err != nil {
			return
		}
		name := make([]byte, n[0])
		if _, err := io.ReadFull(conn, name); err != nil {
			return
		}
		host = string(name)
	case 4:
		var ip [16]byte
		if _, err := io.ReadFull(conn, ip[:]); err != nil {
			return
		}
		host = net.IP(ip[:]).String()
	default:
		socksReply(conn, 8)
		return
	}
	var portBuf [2]byte
	if _, err := io.ReadFull(conn, portBuf[:]); err != nil {
		return
	}
	target := net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(portBuf[:]))))

	_ = conn.SetDeadline(time.Time{})
	stream, err := c.Open(ctx, proto.MethodTunnelOpen, map[string]any{
		"target":     target,
		"timeout_ms": 15000,
	}, func(ev *proto.Envelope) error {
		if ev.Chan == proto.ChOut {
			_, werr := conn.Write(ev.Data)
			return werr
		}
		return nil
	})
	if err != nil {
		socksReply(conn, 5) // connection refused
		return
	}
	defer stream.Close()
	socksReply(conn, 0)

	go func() {
		buf := make([]byte, 32<<10)
		for {
			n, rerr := conn.Read(buf)
			if n > 0 {
				if werr := stream.Send(proto.ChIn, buf[:n]); werr != nil {
					return
				}
			}
			if rerr != nil {
				_ = stream.Send(proto.ChEof, nil)
				return
			}
		}
	}()
	_, _ = stream.Wait(ctx)
}

func socksReply(conn net.Conn, code byte) {
	_, _ = conn.Write([]byte{5, code, 0, 1, 0, 0, 0, 0, 0, 0})
}
