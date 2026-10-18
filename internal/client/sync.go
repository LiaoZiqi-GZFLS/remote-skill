package client

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"remote-skill/internal/daemon"
	"remote-skill/internal/proto"
	"remote-skill/internal/transfer"
)

// SyncOptions controls an incremental directory sync.
type SyncOptions struct {
	Exclude []string
	Delete  bool
	DryRun  bool
}

// SyncResult summarises what a sync did (or would do, with DryRun).
type SyncResult struct {
	Added     int      `json:"added"`
	Changed   int      `json:"changed"`
	Deleted   int      `json:"deleted"`
	Unchanged int      `json:"unchanged"`
	Bytes     int64    `json:"bytes"`
	Files     []string `json:"files"`
	Removed   []string `json:"removed"`
}

type remoteEntry struct {
	Path  string    `json:"path"`
	Size  int64     `json:"size"`
	Mtime time.Time `json:"mtime"`
	Dir   bool      `json:"dir"`
}

// Sync makes the remote directory mirror the local one: files are compared by
// size and modification time, only differences are transferred, and --delete
// removes remote files that no longer exist locally.
func (c *Client) Sync(ctx context.Context, local, remote string, opts SyncOptions) (*SyncResult, error) {
	abs, err := filepath.Abs(local)
	if err != nil {
		return nil, err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", local)
	}
	entries, _, err := c.manifest(ctx, remote)
	if err != nil {
		// create the remote root and retry once
		var ignore strings.Builder
		if _, mkErr := c.Exec(ctx, daemon.ExecRequest{
			Session:   c.SessionName(""),
			Cmd:       "mkdir -p " + Quote(remote),
			TimeoutMS: 30000,
		}, &ignore); mkErr != nil {
			return nil, err
		}
		entries, _, err = c.manifest(ctx, remote)
		if err != nil {
			return nil, err
		}
	}
	remoteByPath := make(map[string]remoteEntry, len(entries))
	for _, e := range entries {
		remoteByPath[e.Path] = e
	}

	res := &SyncResult{}
	seen := map[string]bool{}
	var changedPaths []string
	err = filepath.WalkDir(abs, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return nil
		}
		rel, rerr := filepath.Rel(abs, p)
		if rerr != nil || rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if excluded(rel, opts.Exclude) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		seen[rel] = true
		r, ok := remoteByPath[rel]
		switch {
		case !ok || r.Dir:
			res.Added++
			res.Bytes += info.Size()
			changedPaths = append(changedPaths, p)
			res.Files = append(res.Files, rel)
		case r.Size != info.Size() || info.ModTime().After(r.Mtime.Add(time.Second)):
			res.Changed++
			res.Bytes += info.Size()
			changedPaths = append(changedPaths, p)
			res.Files = append(res.Files, rel)
		default:
			res.Unchanged++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if opts.Delete {
		for rel, e := range remoteByPath {
			if e.Dir || seen[rel] || excluded(rel, opts.Exclude) {
				continue
			}
			res.Removed = append(res.Removed, rel)
		}
		res.Deleted = len(res.Removed)
	}
	if opts.DryRun {
		return res, nil
	}

	if len(changedPaths) > 0 {
		if _, err := c.PutStream(ctx, remote, true, true, func(w io.Writer) error {
			_, aerr := transfer.Archive(w, changedPaths, transfer.ArchiveOptions{
				Recursive: true,
				Root:      abs,
			})
			return aerr
		}); err != nil {
			return res, fmt.Errorf("upload changed files: %w", err)
		}
	}
	if len(res.Removed) > 0 {
		if err := c.Call(ctx, proto.MethodFileRemove, map[string]any{
			"session": c.SessionName(""),
			"root":    remote,
			"paths":   res.Removed,
		}, nil); err != nil {
			return res, fmt.Errorf("delete remote files: %w", err)
		}
	}
	return res, nil
}

func excluded(rel string, patterns []string) bool {
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		p = strings.TrimSuffix(p, "/")
		if ok, _ := path.Match(p, rel); ok {
			return true
		}
		if ok, _ := path.Match(p, path.Base(rel)); ok {
			return true
		}
		if rel == p || strings.HasPrefix(rel, p+"/") {
			return true
		}
	}
	return false
}

func (c *Client) manifest(ctx context.Context, remote string) ([]remoteEntry, string, error) {
	var res struct {
		Root    string        `json:"root"`
		Entries []remoteEntry `json:"entries"`
	}
	if err := c.Call(ctx, proto.MethodFileManifest, map[string]any{
		"session": c.SessionName(""),
		"path":    remote,
	}, &res); err != nil {
		return nil, "", err
	}
	return res.Entries, res.Root, nil
}
