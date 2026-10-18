package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/term"

	"remote-skill/internal/proto"
)

// AttachOptions configures an interactive session attachment.
type AttachOptions struct {
	Session string
	Replay  int
	NoInput bool
}

// Control bytes handled locally while attached.
const (
	keyCtrlC  = 0x03
	keyCtrlQ  = 0x1c
	keyDetach = 0x1d // Ctrl-]
)

// Attach streams a session to the local terminal and forwards input back.
func Attach(ctx context.Context, c *Client, opts AttachOptions) error {
	rows, cols := terminalSize()
	interactive := !opts.NoInput && TTY(os.Stdin)
	stream, err := c.Open(ctx, proto.MethodSessionAttach, map[string]any{
		"session":      opts.Session,
		"replay_bytes": opts.Replay,
		"rows":         rows,
		"cols":         cols,
	}, func(ev *proto.Envelope) error {
		switch ev.Chan {
		case proto.ChOut:
			_, err := os.Stdout.Write(ev.Data)
			return err
		case proto.ChError:
			if msg, _ := ev.Extra["error"].(string); msg != "" {
				fmt.Fprintf(os.Stderr, "rsk: %s\n", msg)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	defer stream.Close()

	if !interactive {
		_, err := stream.Wait(ctx)
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	fmt.Fprintf(os.Stderr, "rsk: attached to %s (Ctrl-] to detach)\n", c.Host.Name)
	fd := int(os.Stdin.Fd())
	state, err := term.MakeRaw(fd)
	if err != nil {
		return fmt.Errorf("switch terminal to raw mode: %w", err)
	}
	defer term.Restore(fd, state)

	go watchResize(ctx, stream)
	done := make(chan error, 1)
	go func() {
		_, err := stream.Wait(ctx)
		done <- err
	}()

	input := make(chan []byte, 32)
	go readInput(input)
	for {
		select {
		case <-done:
			fmt.Fprintf(os.Stderr, "\r\nrsk: session ended\r\n")
			return nil
		case <-ctx.Done():
			return nil
		case chunk := <-input:
			if len(chunk) == 0 {
				return nil
			}
			if chunk[0] == keyDetach {
				fmt.Fprintf(os.Stderr, "\r\nrsk: detached\r\n")
				return nil
			}
			// Batch ordinary bytes into a single frame; control bytes are
			// translated into signals.
			var pending []byte
			flush := func() error {
				if len(pending) == 0 {
					return nil
				}
				err := stream.Send(proto.ChIn, pending)
				pending = nil
				return err
			}
			for _, b := range chunk {
				switch b {
				case keyCtrlC:
					if err := flush(); err != nil {
						return err
					}
					_ = stream.SendExtra(proto.ChSignal, nil, map[string]any{"sig": "INT"})
				case keyCtrlQ:
					if err := flush(); err != nil {
						return err
					}
					_ = stream.SendExtra(proto.ChSignal, nil, map[string]any{"sig": "QUIT"})
				default:
					pending = append(pending, b)
					if len(pending) >= 1024 {
						if err := flush(); err != nil {
							return err
						}
					}
				}
			}
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

func readInput(out chan<- []byte) {
	buf := make([]byte, 4096)
	for {
		n, err := os.Stdin.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			out <- chunk
		}
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "rsk: stdin: %v\n", err)
			}
			close(out)
			return
		}
	}
}

func watchResize(ctx context.Context, stream *Stream) {
	lastRows, lastCols := terminalSize()
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rows, cols := terminalSize()
			if rows != lastRows || cols != lastCols {
				lastRows, lastCols = rows, cols
				_ = stream.SendExtra(proto.ChMeta, nil, map[string]any{"rows": rows, "cols": cols})
			}
		}
	}
}

func terminalSize() (uint16, uint16) {
	w, h, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || w <= 0 || h <= 0 {
		return 50, 200
	}
	return uint16(h), uint16(w)
}
