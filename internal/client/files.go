package client

import (
	"context"
	"errors"
	"io"

	"remote-skill/internal/daemon"
	"remote-skill/internal/proto"
)

// ReadFile reads a bounded text window from a file relative to the session cwd.
func (c *Client) ReadFile(ctx context.Context, path string, opts daemon.FileReadOptions) (*daemon.FileReadResult, error) {
	var res daemon.FileReadResult
	if err := c.Call(ctx, proto.MethodFileRead, map[string]any{
		"session":       c.SessionName(""),
		"path":          path,
		"offset":        opts.Offset,
		"limit":         opts.Limit,
		"max_bytes":     opts.MaxBytes,
		"metadata_only": opts.MetadataOnly,
	}, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// WriteFile streams stdin to a remote file and commits it atomically.
func (c *Client) WriteFile(ctx context.Context, path string, overwrite, createParents bool, content io.Reader) (*daemon.FileWriteResult, error) {
	var res daemon.FileWriteResult
	err := c.sendInput(ctx, proto.MethodFileWrite, map[string]any{
		"session":        c.SessionName(""),
		"path":           path,
		"overwrite":      overwrite,
		"create_parents": createParents,
	}, content, &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// PatchFiles streams a JSON patch document and applies it atomically.
func (c *Client) PatchFiles(ctx context.Context, content io.Reader) (*daemon.PatchResult, error) {
	var res daemon.PatchResult
	err := c.sendInput(ctx, proto.MethodFilePatch, map[string]any{
		"session": c.SessionName(""),
	}, content, &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *Client) sendInput(ctx context.Context, method string, params any, content io.Reader, result any) error {
	e, id := c.newEntry(false, nil)
	defer c.remove(id)
	req, err := proto.NewRequest(id, method, params)
	if err != nil {
		return err
	}
	if err := c.conn.Write(req); err != nil {
		return err
	}
	buf := make([]byte, 256<<10)
	for {
		if err := ctx.Err(); err != nil {
			_ = c.conn.Write(proto.NewEvent(id, proto.ChError, nil))
			return err
		}
		n, rerr := content.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			if err := c.conn.Write(proto.NewEvent(id, proto.ChIn, chunk)); err != nil {
				return err
			}
		}
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			_ = c.conn.Write(proto.NewEvent(id, proto.ChError, nil))
			return rerr
		}
	}
	if err := c.conn.Write(proto.NewEvent(id, proto.ChEof, nil)); err != nil {
		return err
	}
	select {
	case env := <-e.finCh:
		if env.Err != nil {
			return env.Err
		}
		if result != nil && len(env.Result) > 0 {
			return env.DecodeResult(result)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		if err := c.Err(); err != nil {
			return err
		}
		return errors.New("connection closed")
	}
}
