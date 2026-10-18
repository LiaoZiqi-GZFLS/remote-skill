// Package transfer implements directory and file transfer over the rsk
// protocol. Both directions use a tar stream so permissions, symlinks and
// empty directories survive, and a single code path handles files and trees.
package transfer

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Result summarises a completed transfer.
type Result struct {
	Bytes    int64    `json:"bytes"`
	Files    int      `json:"files"`
	Dirs     int      `json:"dirs"`
	Symlinks int      `json:"symlinks"`
	Skipped  []string `json:"skipped,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// ArchiveOptions configures Archive.
type ArchiveOptions struct {
	// Recursive allows directories to be archived.
	Recursive bool
	// Root is the path prefix entries are made relative to. When empty the
	// base name of each source path is used, which matches "cp -r src dst/".
	Root string
}

// Archive writes a tar stream containing paths to w.
func Archive(w io.Writer, paths []string, opts ArchiveOptions) (*Result, error) {
	tw := tar.NewWriter(w)
	res := &Result{}
	for _, p := range paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			return res, err
		}
		info, err := os.Lstat(abs)
		if err != nil {
			return res, err
		}
		base := filepath.Dir(abs)
		if opts.Root != "" {
			base = opts.Root
		}
		if info.IsDir() && !opts.Recursive {
			return res, fmt.Errorf("%s is a directory (use recursive mode)", p)
		}
		if err := archivePath(tw, abs, base, opts, res); err != nil {
			return res, err
		}
	}
	if err := tw.Close(); err != nil {
		return res, err
	}
	return res, nil
}

func archivePath(tw *tar.Writer, abs, base string, opts ArchiveOptions, res *Result) error {
	return filepath.WalkDir(abs, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p != abs && d.IsDir() && !opts.Recursive {
			return filepath.SkipDir
		}
		rel, err := filepath.Rel(base, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case info.Mode()&os.ModeSocket != 0:
			res.Skipped = append(res.Skipped, name+" (socket)")
			return nil
		case info.Mode()&os.ModeNamedPipe != 0:
			res.Skipped = append(res.Skipped, name+" (fifo)")
			return nil
		case info.Mode()&os.ModeDevice != 0:
			res.Skipped = append(res.Skipped, name+" (device)")
			return nil
		}

		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			link, err = os.Readlink(p)
			if err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = name
		hdr.Uname, hdr.Gname = "", ""
		if info.IsDir() {
			hdr.Name = strings.TrimSuffix(name, "/") + "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		switch {
		case info.IsDir():
			res.Dirs++
		case info.Mode()&os.ModeSymlink != 0:
			res.Symlinks++
		case info.Mode().IsRegular():
			f, err := os.Open(p)
			if err != nil {
				return err
			}
			n, err := io.Copy(tw, f)
			f.Close()
			if err != nil {
				return err
			}
			res.Bytes += n
			res.Files++
		}
		return nil
	})
}

// ExtractOptions configures Extract.
type ExtractOptions struct {
	// Overwrite replaces existing files. Existing directories are merged.
	Overwrite bool
	// StripComponents removes leading path elements, like tar --strip-components.
	StripComponents int
	// TopRename replaces the first path element of every entry, used when the
	// destination is a new file or directory name.
	TopRename string
	// Touch restores modification times.
	Touch bool
}

// Extract unpacks a tar stream into root.
func Extract(r io.Reader, root string, opts ExtractOptions) (*Result, error) {
	tr := tar.NewReader(r)
	res := &Result{}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return res, err
	}
	hardlinks := map[string]string{}
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return res, err
		}
		name := stripComponents(hdr.Name, opts.StripComponents)
		if name == "" {
			continue
		}
		name = renameTop(name, opts.TopRename)
		if name == "" {
			continue
		}
		target, err := SafeJoin(root, name)
		if err != nil {
			return res, err
		}
		mode := fs.FileMode(hdr.Mode) & 0o7777
		if mode == 0 {
			mode = 0o644
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, mode|0o700); err != nil {
				return res, err
			}
			if err := os.Chmod(target, mode|0o700); err != nil {
				res.Warnings = append(res.Warnings, fmt.Sprintf("chmod %s: %v", name, err))
			}
			res.Dirs++
		case tar.TypeSymlink:
			if err := checkLinkTarget(root, target, hdr.Linkname); err != nil {
				res.Warnings = append(res.Warnings, fmt.Sprintf("skip symlink %s: %v", name, err))
				continue
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return res, err
			}
			_ = os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				// Windows and some filesystems cannot create symlinks; keep
				// the target as plain text so nothing is silently lost.
				msg := fmt.Sprintf("symlink %s -> %s: %v", name, hdr.Linkname, err)
				if werr := os.WriteFile(target, []byte(hdr.Linkname), 0o644); werr != nil {
					res.Warnings = append(res.Warnings, msg)
					continue
				}
				res.Warnings = append(res.Warnings, msg+" (stored as text file)")
			} else {
				res.Symlinks++
			}
		case tar.TypeLink:
			src, err := SafeJoin(root, renameTop(stripComponents(hdr.Linkname, opts.StripComponents), opts.TopRename))
			if err != nil {
				res.Warnings = append(res.Warnings, fmt.Sprintf("skip hardlink %s: %v", name, err))
				continue
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return res, err
			}
			_ = os.Remove(target)
			if err := os.Link(src, target); err != nil {
				data, rerr := os.ReadFile(src)
				if rerr != nil {
					res.Warnings = append(res.Warnings, fmt.Sprintf("hardlink %s: %v", name, err))
					continue
				}
				if werr := os.WriteFile(target, data, mode); werr != nil {
					return res, werr
				}
				res.Warnings = append(res.Warnings, fmt.Sprintf("hardlink %s copied instead of linked", name))
			}
			hardlinks[hdr.Linkname] = target
			res.Files++
		case tar.TypeReg, tar.TypeRegA:
			if !opts.Overwrite {
				if _, err := os.Lstat(target); err == nil {
					res.Skipped = append(res.Skipped, name+" (exists)")
					continue
				}
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return res, err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
			if err != nil {
				return res, err
			}
			n, err := io.Copy(f, tr)
			closeErr := f.Close()
			if err != nil {
				return res, err
			}
			if closeErr != nil {
				return res, closeErr
			}
			if runtime.GOOS != "windows" {
				if err := os.Chmod(target, mode); err != nil {
					res.Warnings = append(res.Warnings, fmt.Sprintf("chmod %s: %v", name, err))
				}
			}
			res.Bytes += n
			res.Files++
		default:
			res.Skipped = append(res.Skipped, name+" (unsupported type)")
			continue
		}
		if opts.Touch && !hdr.ModTime.IsZero() {
			_ = os.Chtimes(target, time.Now(), hdr.ModTime)
		}
	}
	return res, nil
}

// SafeJoin resolves a tar entry name inside root, refusing escapes.
func SafeJoin(root, name string) (string, error) {
	slash := filepath.ToSlash(name)
	if slash == "" {
		return root, nil
	}
	// Leading slashes are stripped (like tar) but any attempt to climb out of
	// the destination is an error rather than a silent relocation.
	clean := path.Clean(strings.TrimLeft(slash, "/"))
	if clean == "." || clean == "" {
		return root, nil
	}
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("unsafe path %q", name)
	}
	target := filepath.Join(root, filepath.FromSlash(clean))
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe path %q", name)
	}
	return target, nil
}

func checkLinkTarget(root, target, link string) error {
	if filepath.IsAbs(link) {
		return fmt.Errorf("absolute symlink target %q", link)
	}
	resolved := filepath.Join(filepath.Dir(target), filepath.FromSlash(link))
	rel, err := filepath.Rel(root, resolved)
	if err != nil {
		return err
	}
	if strings.HasPrefix(rel, "..") {
		return fmt.Errorf("symlink target escapes destination")
	}
	return nil
}

func stripComponents(name string, n int) string {
	if n <= 0 {
		return name
	}
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	if len(parts) <= n {
		return ""
	}
	return strings.Join(parts[n:], "/")
}

// renameTop swaps the leading path element of name.
func renameTop(name, newTop string) string {
	if newTop == "" {
		return name
	}
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	if len(parts) == 0 {
		return newTop
	}
	parts[0] = newTop
	return strings.Join(parts, "/")
}
