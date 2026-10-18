package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"remote-skill/internal/daemon"
	"remote-skill/internal/proto"
	"remote-skill/internal/transfer"
)

// OpenSession creates (or returns) a persistent session.
func (c *Client) OpenSession(ctx context.Context, opts daemon.SessionOptions) (*daemon.SessionInfo, error) {
	var info daemon.SessionInfo
	if err := c.Call(ctx, proto.MethodSessionOpen, opts, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// Exec runs a command in a persistent session, streaming output to out.
func (c *Client) Exec(ctx context.Context, req daemon.ExecRequest, out io.Writer) (*daemon.ExecResult, error) {
	var execID string
	env, err := c.StreamWait(ctx, proto.MethodExec, req, func(ev *proto.Envelope) error {
		switch ev.Chan {
		case proto.ChMeta:
			if id, _ := ev.Extra["exec"].(string); id != "" {
				execID = id
			}
		case proto.ChOut:
			id, _ := ev.Extra["exec"].(string)
			if execID == "" || id == "" || id == execID {
				if out != nil {
					if _, err := out.Write(ev.Data); err != nil {
						return err
					}
				}
			}
		case proto.ChError:
			if msg, _ := ev.Extra["error"].(string); msg != "" {
				fmt.Fprintf(c.Logw, "rsk: %s\n", msg)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var res daemon.ExecResult
	if len(env.Result) > 0 {
		if err := env.DecodeResult(&res); err != nil {
			return nil, err
		}
	}
	return &res, nil
}

// SetCwd points the session at a directory, returning the resolved path.
func (c *Client) SetCwd(ctx context.Context, session, dir string) (string, error) {
	var out strings.Builder
	res, err := c.Exec(ctx, daemon.ExecRequest{
		Session: session,
		Cmd:     "cd " + Quote(dir) + " && pwd",
	}, &out)
	if err != nil {
		return "", err
	}
	if res.RC != 0 {
		return "", fmt.Errorf("cannot use %s as the workspace: the remote shell reported exit status %d (does the directory exist?)", dir, res.RC)
	}
	return strings.TrimSpace(out.String()), nil
}

// PutStream uploads using a tar producer, streaming as it goes.
func (c *Client) PutStream(ctx context.Context, remote string, recursive, overwrite bool, produce func(io.Writer) error) (*transfer.Result, error) {
	params := map[string]any{
		"session":   c.SessionName(""),
		"path":      remote,
		"recursive": recursive,
		"overwrite": overwrite,
		"touch":     true,
	}
	e, id := c.newEntry(false, nil)
	defer c.remove(id)
	req, err := proto.NewRequest(id, proto.MethodFilePut, params)
	if err != nil {
		return nil, err
	}
	if err := c.conn.Write(req); err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	produceErr := make(chan error, 1)
	go func() {
		err := produce(pw)
		_ = pw.CloseWithError(err)
		produceErr <- err
	}()
	buf := make([]byte, 256<<10)
	sendErr := error(nil)
	for {
		select {
		case fin := <-e.finCh:
			if fin.Err != nil {
				return nil, fin.Err
			}
			var res struct {
				Result *transfer.Result `json:"result"`
			}
			if len(fin.Result) > 0 {
				_ = fin.DecodeResult(&res)
			}
			return res.Result, nil
		default:
		}
		n, err := pr.Read(buf)
		if n > 0 {
			if werr := c.conn.Write(proto.NewEvent(id, proto.ChIn, buf[:n])); werr != nil {
				sendErr = werr
				break
			}
		}
		if err != nil {
			break
		}
	}
	if sendErr != nil {
		return nil, sendErr
	}
	if err := <-produceErr; err != nil {
		_ = c.conn.Write(proto.NewEvent(id, proto.ChError, nil))
		return nil, err
	}
	if err := c.conn.Write(proto.NewEvent(id, proto.ChEof, nil)); err != nil {
		return nil, err
	}
	select {
	case fin := <-e.finCh:
		if fin.Err != nil {
			return nil, fin.Err
		}
		var res struct {
			Result *transfer.Result `json:"result"`
		}
		if len(fin.Result) > 0 {
			_ = fin.DecodeResult(&res)
		}
		return res.Result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.Err()
	}
}

// PutPath uploads a local file or directory.
func (c *Client) PutPath(ctx context.Context, local, remote string, recursive, overwrite bool) (*transfer.Result, error) {
	st, err := os.Lstat(local)
	if err != nil {
		return nil, err
	}
	if st.IsDir() && !recursive {
		return nil, fmt.Errorf("%s is a directory (use --recursive)", local)
	}
	if remote == "" {
		remote = "."
	}
	abs, err := filepath.Abs(local)
	if err != nil {
		return nil, err
	}
	return c.PutStream(ctx, remote, recursive, overwrite, func(w io.Writer) error {
		_, err := transfer.Archive(w, []string{abs}, transfer.ArchiveOptions{
			Recursive: recursive,
			Root:      filepath.Dir(abs),
		})
		return err
	})
}

// GetInfo describes a remote path before downloading.
type GetInfo struct {
	Path  string    `json:"path"`
	Name  string    `json:"name"`
	Dir   bool      `json:"dir"`
	Size  int64     `json:"size"`
	Mode  uint32    `json:"mode"`
	Mtime time.Time `json:"mtime"`
}

// Get downloads a remote file or directory into local.
func (c *Client) Get(ctx context.Context, remote, local string, recursive, overwrite bool) (*GetInfo, *transfer.Result, error) {
	var info GetInfo
	var result *transfer.Result
	var buf []byte
	stream, err := c.Open(ctx, proto.MethodFileGet, map[string]any{
		"session":   c.SessionName(""),
		"path":      remote,
		"recursive": recursive,
	}, func(ev *proto.Envelope) error {
		switch ev.Chan {
		case proto.ChOut:
			buf = append(buf, ev.Data...)
		case proto.ChError:
			if msg, _ := ev.Extra["error"].(string); msg != "" {
				return fmt.Errorf("%s", msg)
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if err := decodeAck(stream, &info); err != nil {
		return nil, nil, err
	}
	if _, err := stream.Wait(ctx); err != nil {
		return nil, nil, err
	}
	destDir, rename := localDestination(local, info.Name, info.Dir)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return nil, nil, err
	}
	result, err = transfer.Extract(bytes.NewReader(buf), destDir, transfer.ExtractOptions{
		Overwrite: overwrite,
		TopRename: rename,
		Touch:     true,
	})
	if err != nil {
		return &info, result, err
	}
	return &info, result, nil
}

// localDestination mirrors the remote naming rule for downloads.
func localDestination(local, remoteName string, isDir bool) (dir, rename string) {
	if local == "" {
		local = "."
	}
	if st, err := os.Stat(local); err == nil && st.IsDir() {
		return local, ""
	}
	if strings.HasSuffix(local, "/") || strings.HasSuffix(local, string(os.PathSeparator)) {
		return strings.TrimRight(local, "/\\"), ""
	}
	return filepath.Dir(local), filepath.Base(local)
}

func decodeAck(stream *Stream, out any) error {
	env := stream.ack
	if env == nil {
		return fmt.Errorf("missing remote acknowledgement")
	}
	if env.Err != nil {
		return env.Err
	}
	if out != nil && len(env.Result) > 0 {
		return env.DecodeResult(out)
	}
	return nil
}

// List describes a remote directory.
func (c *Client) List(ctx context.Context, remote string) ([]daemon.FileEntry, string, error) {
	var res struct {
		Path    string             `json:"path"`
		Entries []daemon.FileEntry `json:"entries"`
	}
	if err := c.Call(ctx, proto.MethodFileList, map[string]any{
		"session": c.SessionName(""),
		"path":    remote,
	}, &res); err != nil {
		return nil, "", err
	}
	sort.Slice(res.Entries, func(i, j int) bool {
		if res.Entries[i].Dir != res.Entries[j].Dir {
			return res.Entries[i].Dir
		}
		return res.Entries[i].Name < res.Entries[j].Name
	})
	return res.Entries, res.Path, nil
}

// ScrollbackResult is the tail of a session's terminal output.
type ScrollbackResult struct {
	Session   string             `json:"session"`
	Cwd       string             `json:"cwd"`
	Data      []byte             `json:"data"`
	Bytes     int                `json:"bytes"`
	Commands  int                `json:"commands"`
	Busy      bool               `json:"busy"`
	LastExec  *daemon.ExecResult `json:"last_exec"`
	StartedAt time.Time          `json:"started_at"`
}

// Scrollback reads recent terminal output.
func (c *Client) Scrollback(ctx context.Context, session string, tail int) (*ScrollbackResult, error) {
	var res ScrollbackResult
	if err := c.Call(ctx, proto.MethodSessionScrollback, map[string]any{
		"session": c.SessionName(session),
		"tail":    tail,
	}, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Status returns daemon information.
func (c *Client) Status(ctx context.Context) (*daemon.HostInfo, error) {
	var info daemon.HostInfo
	if err := c.Call(ctx, proto.MethodInfo, nil, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// CloseSession terminates a session.
func (c *Client) CloseSession(ctx context.Context, session string) error {
	return c.Call(ctx, proto.MethodSessionClose, map[string]any{"session": c.SessionName(session)}, nil)
}

// Shutdown stops the remote daemon. Sessions keep running unless killSessions
// is set, which is what makes them survive daemon restarts.
func (c *Client) Shutdown(ctx context.Context, force, killSessions bool) error {
	return c.Call(ctx, proto.MethodShutdown, map[string]any{
		"force":         force,
		"kill_sessions": killSessions,
	}, nil)
}

// SessionInfo returns the current state of one session.
func (c *Client) SessionInfo(ctx context.Context, session string) (*daemon.SessionInfo, error) {
	var info daemon.SessionInfo
	if err := c.Call(ctx, proto.MethodSessionInfo, map[string]any{
		"session": c.SessionName(session),
	}, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// Resize changes a session terminal size.
func (c *Client) Resize(ctx context.Context, session string, rows, cols uint16) error {
	return c.Call(ctx, proto.MethodSessionResize, map[string]any{
		"session": c.SessionName(session),
		"rows":    rows,
		"cols":    cols,
	}, nil)
}
