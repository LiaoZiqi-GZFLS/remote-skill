package client

import (
	"context"
	"fmt"
	"os"

	"golang.org/x/term"

	"remote-skill/internal/daemon"
	"remote-skill/internal/proto"
)

// TTYOptions describe a one-shot command that should run on a pty.
type TTYOptions struct {
	Session string
	Cmd     string
	Cwd     string
	Timeout int
}

// RunTTY executes a command on a dedicated remote pty (ssh -t semantics).
// Output is streamed to out; when stdin is a terminal it is forwarded live and
// window resizes are propagated. Returns the command's exit code.
func RunTTY(ctx context.Context, c *Client, opts TTYOptions, out *os.File) (int, error) {
	rows, cols := terminalSize()
	stream, err := c.Open(ctx, proto.MethodExecTTY, map[string]any{
		"session":    c.SessionName(opts.Session),
		"cmd":        opts.Cmd,
		"cwd":        opts.Cwd,
		"timeout_ms": opts.Timeout,
		"rows":       rows,
		"cols":       cols,
	}, func(ev *proto.Envelope) error {
		if ev.Chan == proto.ChOut {
			_, werr := out.Write(ev.Data)
			return werr
		}
		return nil
	})
	if err != nil {
		return -1, err
	}
	defer stream.Close()

	interactive := TTY(os.Stdin)
	if interactive {
		fd := int(os.Stdin.Fd())
		if state, serr := term.MakeRaw(fd); serr == nil {
			defer term.Restore(fd, state)
			go watchResize(ctx, stream)
			go func() {
				buf := make([]byte, 4096)
				for {
					n, rerr := os.Stdin.Read(buf)
					if n > 0 {
						if serr := sendTTYInput(stream, buf[:n]); serr != nil {
							return
						}
					}
					if rerr != nil {
						return
					}
				}
			}()
		}
	}
	env, err := stream.Wait(ctx)
	if err != nil {
		return -1, err
	}
	var res daemon.ExecResult
	if len(env.Result) > 0 {
		if derr := env.DecodeResult(&res); derr != nil {
			return -1, derr
		}
	}
	if res.TimedOut {
		fmt.Fprintf(os.Stderr, "\r\nrsk: command timed out\r\n")
	}
	return res.RC, nil
}

// sendTTYInput forwards bytes, turning Ctrl-C into a remote signal.
func sendTTYInput(stream *Stream, chunk []byte) error {
	start := 0
	flush := func(end int) error {
		if end <= start {
			return nil
		}
		err := stream.Send(proto.ChIn, chunk[start:end])
		start = end
		return err
	}
	for i, b := range chunk {
		switch b {
		case 0x03:
			if err := flush(i); err != nil {
				return err
			}
			_ = stream.SendExtra(proto.ChSignal, nil, map[string]any{"sig": "INT"})
			start = i + 1
		case 0x1c:
			if err := flush(i); err != nil {
				return err
			}
			_ = stream.SendExtra(proto.ChSignal, nil, map[string]any{"sig": "QUIT"})
			start = i + 1
		}
	}
	return flush(len(chunk))
}
