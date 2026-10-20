package daemon

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"remote-skill/internal/proto"
)

const (
	DefaultReadLineLimit = 2000
	MaxReadLineLimit     = 1_000_000
	DefaultReadMaxBytes  = 1 << 20
	MaxReadMaxBytes      = 8 << 20
	MaxPatchBytes        = 8 << 20
	MaxPatchFileBytes    = 16 << 20
)

// FileReadOptions controls a bounded, text-only read of one remote file.
type FileReadOptions struct {
	Offset       int   `json:"offset"`
	Limit        int   `json:"limit"`
	MaxBytes     int64 `json:"max_bytes"`
	MetadataOnly bool  `json:"metadata_only"`
}

// FileReadResult is returned by file.read.
type FileReadResult struct {
	Path         string    `json:"path"`
	Name         string    `json:"name"`
	Size         int64     `json:"size"`
	Mode         uint32    `json:"mode"`
	ModeString   string    `json:"mode_string"`
	Mtime        time.Time `json:"mtime"`
	Symlink      bool      `json:"symlink"`
	Binary       bool      `json:"binary"`
	MetadataOnly bool      `json:"metadata_only"`
	Offset       int       `json:"offset"`
	Limit        int       `json:"limit"`
	Lines        int       `json:"lines"`
	NextOffset   int       `json:"next_offset"`
	Bytes        int       `json:"bytes"`
	Truncated    bool      `json:"truncated"`
	EOF          bool      `json:"eof"`
	Content      string    `json:"content,omitempty"`
}

// ReadFilePath reads a line window from a regular text file. Paths are
// resolved by the caller; this helper enforces binary, size and line bounds.
func ReadFilePath(path string, opts FileReadOptions) (*FileReadResult, error) {
	if opts.Offset == 0 {
		opts.Offset = 1
	}
	if opts.Offset < 1 {
		return nil, proto.Errorf(proto.CodeBadRequest, "--offset must be at least 1")
	}
	if opts.Limit < 0 {
		return nil, proto.Errorf(proto.CodeBadRequest, "--limit cannot be negative")
	}
	if opts.Limit > MaxReadLineLimit {
		return nil, proto.Errorf(proto.CodeBadRequest, "--limit exceeds the maximum of %d lines", MaxReadLineLimit)
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = DefaultReadMaxBytes
	}
	if opts.MaxBytes > MaxReadMaxBytes {
		return nil, proto.Errorf(proto.CodeBadRequest, "--max-bytes must not exceed %d", MaxReadMaxBytes)
	}

	linkInfo, err := os.Lstat(path)
	if err != nil {
		return nil, asFileError(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, asFileError(err)
	}
	if info.IsDir() {
		return nil, proto.Errorf(proto.CodeBadRequest, "%s is a directory", path).
			WithHint("use `rsk ls` to list a directory")
	}
	if !info.Mode().IsRegular() {
		return nil, proto.Errorf(proto.CodeBadRequest, "%s is not a regular file", path)
	}

	res := &FileReadResult{
		Path:         path,
		Name:         filepath.Base(path),
		Size:         info.Size(),
		Mode:         uint32(info.Mode().Perm()),
		ModeString:   info.Mode().String(),
		Mtime:        info.ModTime(),
		Symlink:      linkInfo.Mode()&os.ModeSymlink != 0,
		MetadataOnly: opts.MetadataOnly,
		Offset:       opts.Offset,
		Limit:        opts.Limit,
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, asFileError(err)
	}
	defer f.Close()

	binary, err := fileLooksBinary(f, info.Size())
	if err != nil {
		return nil, asFileError(err)
	}
	res.Binary = binary
	if opts.MetadataOnly {
		res.NextOffset = res.Offset
		return res, nil
	}
	if binary {
		return nil, proto.Errorf(proto.CodeBadRequest, "%s appears to be a binary file", path).
			WithHint("use `rsk get` for binary files")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, asFileError(err)
	}

	reader := bufio.NewReaderSize(f, 64<<10)
	line := 1
	for line < opts.Offset {
		_, rerr := discardLine(reader)
		if errors.Is(rerr, io.EOF) {
			res.EOF = true
			res.NextOffset = opts.Offset
			return res, nil
		}
		if rerr != nil {
			return nil, asFileError(rerr)
		}
		line++
	}

	var content bytes.Buffer
	for {
		data, rerr := readLine(reader, opts.MaxBytes)
		if errors.Is(rerr, errLineTooLong) {
			res.Truncated = true
			break
		}
		if rerr != nil && !errors.Is(rerr, io.EOF) {
			return nil, asFileError(rerr)
		}
		if len(data) > 0 {
			if opts.Limit > 0 && res.Lines >= opts.Limit {
				res.Truncated = true
				break
			}
			if int64(content.Len()+len(data)) > opts.MaxBytes {
				res.Truncated = true
				break
			}
			if !utf8.Valid(data) {
				return nil, proto.Errorf(proto.CodeBadRequest, "%s is not valid UTF-8", path).
					WithHint("use `rsk get` for binary files")
			}
			_, _ = content.Write(data)
			res.Lines++
		}
		if errors.Is(rerr, io.EOF) {
			res.EOF = true
			break
		}
		if opts.Limit > 0 && res.Lines >= opts.Limit || int64(content.Len()) >= opts.MaxBytes {
			_, peekErr := reader.Peek(1)
			if peekErr == nil {
				res.Truncated = true
			} else if errors.Is(peekErr, io.EOF) {
				res.EOF = true
			}
			break
		}
	}
	res.Content = content.String()
	res.Bytes = content.Len()
	res.NextOffset = opts.Offset + res.Lines
	return res, nil
}

var errLineTooLong = errors.New("line exceeds read limit")

func readLine(r *bufio.Reader, max int64) ([]byte, error) {
	line := make([]byte, 0, 256)
	for {
		frag, err := r.ReadSlice('\n')
		if int64(len(line)+len(frag)) > max {
			return nil, errLineTooLong
		}
		line = append(line, frag...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, err
	}
}

func discardLine(r *bufio.Reader) ([]byte, error) {
	var first []byte
	for {
		frag, err := r.ReadSlice('\n')
		if first == nil && len(frag) > 0 {
			first = append([]byte(nil), frag[:1]...)
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return first, err
	}
}

func fileLooksBinary(f *os.File, size int64) (bool, error) {
	if size == 0 {
		return false, nil
	}
	n := int64(8 << 10)
	if size < n {
		n = size
	}
	buf := make([]byte, n)
	read, err := f.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	buf = buf[:read]
	if bytes.IndexByte(buf, 0) >= 0 {
		return true, nil
	}
	if utf8.Valid(buf) {
		return false, nil
	}
	if int64(read) == size {
		return true, nil
	}
	// A short sample can end in the middle of a UTF-8 rune.
	for trim := 1; trim <= utf8.UTFMax-1 && trim < len(buf); trim++ {
		if utf8.Valid(buf[:len(buf)-trim]) {
			return false, nil
		}
	}
	return true, nil
}

// FileWriteResult is returned by file.write.
type FileWriteResult struct {
	Path        string `json:"path"`
	Bytes       int64  `json:"bytes"`
	Created     bool   `json:"created"`
	Overwritten bool   `json:"overwritten"`
	Mode        uint32 `json:"mode"`
}

type atomicWrite struct {
	path        string
	tmpPath     string
	file        *os.File
	mode        fs.FileMode
	existed     bool
	overwrite   bool
	bytes       int64
	createdDirs []string
	done        bool
}

func beginAtomicWrite(path string, overwrite, createParents bool) (*atomicWrite, error) {
	if strings.TrimSpace(path) == "" {
		return nil, proto.Errorf(proto.CodeBadRequest, "empty path")
	}
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	existed := err == nil
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, proto.Errorf(proto.CodeConflict, "%s is a symlink", path).
				WithHint("edit the symlink target explicitly")
		}
		if info.IsDir() {
			return nil, proto.Errorf(proto.CodeConflict, "%s is a directory", path)
		}
		if !info.Mode().IsRegular() {
			return nil, proto.Errorf(proto.CodeConflict, "%s is not a regular file", path)
		}
		if !overwrite {
			return nil, proto.Errorf(proto.CodeConflict, "%s already exists", path).
				WithHint("pass --overwrite to replace it")
		}
	case !os.IsNotExist(err):
		return nil, asFileError(err)
	}

	var createdDirs []string
	parent := filepath.Dir(path)
	if _, err := os.Stat(parent); err != nil {
		if !os.IsNotExist(err) {
			return nil, asFileError(err)
		}
		if !createParents {
			return nil, proto.Errorf(proto.CodeNotFound, "parent directory %s does not exist", parent).
				WithHint("pass --create-parents to create it")
		}
		createdDirs, err = ensureDir(parent)
		if err != nil {
			return nil, asFileError(err)
		}
	}

	mode := fs.FileMode(0o644)
	if existed {
		mode = info.Mode().Perm()
	}
	tmp, err := os.CreateTemp(parent, ".rsk-write-*")
	if err != nil {
		removeEmptyDirs(createdDirs)
		return nil, asFileError(err)
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		removeEmptyDirs(createdDirs)
		return nil, asFileError(err)
	}
	return &atomicWrite{
		path:        path,
		tmpPath:     tmp.Name(),
		file:        tmp,
		mode:        mode,
		existed:     existed,
		overwrite:   overwrite,
		createdDirs: createdDirs,
	}, nil
}

func (w *atomicWrite) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	w.bytes += int64(n)
	return n, err
}

func (w *atomicWrite) Commit() (*FileWriteResult, error) {
	if w.done {
		return nil, proto.Errorf(proto.CodeInternal, "write already finalized")
	}
	if info, err := os.Lstat(w.path); err == nil {
		if !w.overwrite {
			w.Abort()
			return nil, proto.Errorf(proto.CodeConflict, "%s already exists", w.path).
				WithHint("pass --overwrite to replace it")
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			w.Abort()
			return nil, proto.Errorf(proto.CodeConflict, "%s changed type before commit", w.path)
		}
	} else if !os.IsNotExist(err) {
		w.Abort()
		return nil, asFileError(err)
	}
	if err := w.file.Sync(); err != nil {
		w.Abort()
		return nil, asFileError(err)
	}
	if err := w.file.Close(); err != nil {
		w.Abort()
		return nil, asFileError(err)
	}
	if err := os.Rename(w.tmpPath, w.path); err != nil {
		w.Abort()
		return nil, asFileError(err)
	}
	w.tmpPath = ""
	w.done = true
	return &FileWriteResult{
		Path:        w.path,
		Bytes:       w.bytes,
		Created:     !w.existed,
		Overwritten: w.existed,
		Mode:        uint32(w.mode.Perm()),
	}, nil
}

func (w *atomicWrite) Abort() {
	if w == nil || w.done {
		return
	}
	w.done = true
	if w.file != nil {
		_ = w.file.Close()
	}
	if w.tmpPath != "" {
		_ = os.Remove(w.tmpPath)
	}
	removeEmptyDirs(w.createdDirs)
}

// PatchOperation is one operation in the JSON document accepted by file.patch.
type PatchOperation struct {
	Op            string `json:"op"`
	Path          string `json:"path"`
	Old           string `json:"old,omitempty"`
	New           string `json:"new,omitempty"`
	Count         *int   `json:"count,omitempty"`
	Content       string `json:"content,omitempty"`
	Mode          string `json:"mode,omitempty"`
	CreateParents bool   `json:"create_parents,omitempty"`
}

// ResolvedPatchOperation has an absolute path resolved against a session cwd.
type ResolvedPatchOperation struct {
	Op            string
	Path          string
	Old           string
	New           string
	Count         *int
	Content       string
	Mode          string
	CreateParents bool
}

// PatchChange describes the final state of one path changed by a patch.
type PatchChange struct {
	Path         string `json:"path"`
	Action       string `json:"action"`
	Replacements int    `json:"replacements,omitempty"`
	Bytes        int64  `json:"bytes"`
	Mode         uint32 `json:"mode"`
}

// PatchResult is returned by file.patch.
type PatchResult struct {
	Changes []PatchChange `json:"changes"`
}

// PatchDocument is the object form accepted by ParsePatchDocument.
type PatchDocument struct {
	Operations []PatchOperation `json:"operations"`
}

// ParsePatchDocument accepts either {"operations":[...]} or a top-level array.
func ParsePatchDocument(data []byte) ([]PatchOperation, error) {
	if len(data) == 0 {
		return nil, proto.Errorf(proto.CodeBadRequest, "empty patch document")
	}
	if len(data) > MaxPatchBytes {
		return nil, proto.Errorf(proto.CodeBadRequest, "patch document exceeds %d bytes", MaxPatchBytes)
	}
	trimmed := bytes.TrimSpace(data)
	var ops []PatchOperation
	if len(trimmed) > 0 && trimmed[0] == '[' {
		if err := decodeStrictJSON(trimmed, &ops); err != nil {
			return nil, proto.Errorf(proto.CodeBadRequest, "invalid patch JSON: %v", err)
		}
	} else {
		var doc PatchDocument
		if err := decodeStrictJSON(trimmed, &doc); err != nil {
			return nil, proto.Errorf(proto.CodeBadRequest, "invalid patch JSON: %v", err)
		}
		ops = doc.Operations
	}
	if len(ops) == 0 {
		return nil, proto.Errorf(proto.CodeBadRequest, "patch contains no operations")
	}
	return ops, nil
}

func decodeStrictJSON(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

type patchFileState struct {
	path          string
	originalExist bool
	exists        bool
	originalInfo  fs.FileInfo
	original      []byte
	content       []byte
	mode          fs.FileMode
	createParents bool
	replacements  int
	touched       bool
}

type stagedPatchFile struct {
	path string
	tmp  string
	size int64
	mode fs.FileMode
}

type patchBackup struct {
	path   string
	backup string
}

// ApplyFilePatch validates all operations before touching the target files,
// then stages the final contents and commits them with per-file backups.
func ApplyFilePatch(ops []ResolvedPatchOperation) (*PatchResult, error) {
	if len(ops) == 0 {
		return nil, proto.Errorf(proto.CodeBadRequest, "patch contains no operations")
	}
	states := map[string]*patchFileState{}
	order := make([]string, 0, len(ops))
	getState := func(path string) (*patchFileState, error) {
		path = filepath.Clean(path)
		if st, ok := states[path]; ok {
			return st, nil
		}
		st := &patchFileState{path: path, mode: 0o644}
		info, err := os.Lstat(path)
		switch {
		case err == nil:
			if info.Mode()&os.ModeSymlink != 0 {
				return nil, proto.Errorf(proto.CodeConflict, "%s is a symlink", path).
					WithHint("edit the symlink target explicitly")
			}
			if info.IsDir() || !info.Mode().IsRegular() {
				return nil, proto.Errorf(proto.CodeConflict, "%s is not a regular file", path)
			}
			st.originalExist = true
			st.exists = true
			st.originalInfo = info
			st.mode = info.Mode().Perm()
		case !os.IsNotExist(err):
			return nil, asFileError(err)
		}
		states[path] = st
		order = append(order, path)
		return st, nil
	}
	loadText := func(st *patchFileState) error {
		if st.original != nil || !st.originalExist {
			return nil
		}
		data, err := os.ReadFile(st.path)
		if err != nil {
			return asFileError(err)
		}
		if len(data) > MaxPatchFileBytes {
			return proto.Errorf(proto.CodeConflict, "%s exceeds the %d-byte patch limit", st.path, MaxPatchFileBytes)
		}
		if !utf8.Valid(data) {
			return proto.Errorf(proto.CodeConflict, "%s is not valid UTF-8", st.path).
				WithHint("use write/put for binary files")
		}
		st.original = data
		st.content = append([]byte(nil), data...)
		return nil
	}

	changedPaths := make([]string, 0, len(ops))
	changedSet := map[string]bool{}
	createdDirs := []string{}
	keepCreatedDirs := false
	defer func() {
		if !keepCreatedDirs {
			removeEmptyDirs(createdDirs)
		}
	}()
	for _, op := range ops {
		if strings.TrimSpace(op.Path) == "" {
			return nil, proto.Errorf(proto.CodeBadRequest, "patch operation has an empty path")
		}
		st, err := getState(op.Path)
		if err != nil {
			return nil, err
		}
		switch op.Op {
		case "replace":
			if op.Old == "" {
				return nil, proto.Errorf(proto.CodeBadRequest, "replace operation for %s has an empty old string", st.path)
			}
			if !st.exists {
				return nil, proto.Errorf(proto.CodeConflict, "cannot replace missing file %s", st.path)
			}
			if err := loadText(st); err != nil {
				return nil, err
			}
			expected := 1
			if op.Count != nil {
				expected = *op.Count
			}
			if expected < 1 {
				return nil, proto.Errorf(proto.CodeBadRequest, "replacement count for %s must be at least 1", st.path)
			}
			actual := strings.Count(string(st.content), op.Old)
			if actual != expected {
				return nil, proto.Errorf(proto.CodeConflict,
					"replacement for %s matched %d time(s), expected %d", st.path, actual, expected)
			}
			st.content = []byte(strings.Replace(string(st.content), op.Old, op.New, expected))
			st.replacements += expected
		case "add":
			if st.exists {
				return nil, proto.Errorf(proto.CodeConflict, "cannot add existing file %s", st.path)
			}
			mode, merr := parsePatchMode(op.Mode, 0o644)
			if merr != nil {
				return nil, proto.Errorf(proto.CodeBadRequest, "%s: %v", st.path, merr)
			}
			st.exists = true
			st.content = []byte(op.Content)
			st.mode = mode
			st.createParents = op.CreateParents
		case "delete":
			if !st.exists {
				return nil, proto.Errorf(proto.CodeConflict, "cannot delete missing file %s", st.path)
			}
			st.exists = false
		default:
			return nil, proto.Errorf(proto.CodeBadRequest, "unknown patch op %q", op.Op)
		}
		st.touched = true
		if !changedSet[st.path] {
			changedSet[st.path] = true
			changedPaths = append(changedPaths, st.path)
		}
	}

	effectivePaths := make([]string, 0, len(changedPaths))
	for _, path := range changedPaths {
		st := states[path]
		if !st.originalExist && !st.exists {
			continue
		}
		if st.originalExist && st.exists && st.original != nil && bytes.Equal(st.original, st.content) {
			continue
		}
		effectivePaths = append(effectivePaths, path)
	}
	if len(effectivePaths) == 0 {
		return nil, proto.Errorf(proto.CodeConflict, "patch made no file changes")
	}

	// Refuse to commit if any file changed since it was loaded.
	for _, path := range order {
		st := states[path]
		if !st.touched {
			continue
		}
		if err := verifyPatchState(st); err != nil {
			return nil, err
		}
		if st.exists && st.createParents {
			created, err := ensureDir(filepath.Dir(st.path))
			if err != nil {
				return nil, asFileError(err)
			}
			createdDirs = append(createdDirs, created...)
		}
	}

	staged := map[string]stagedPatchFile{}
	for _, path := range effectivePaths {
		st := states[path]
		if !st.exists {
			continue
		}
		file, err := stagePatchFile(st.path, st.content, st.mode)
		if err != nil {
			abortStagedPatch(staged)
			return nil, err
		}
		staged[path] = file
	}

	backups := make([]patchBackup, 0, len(effectivePaths))
	installed := make([]string, 0, len(effectivePaths))
	rollback := func() {
		for i := len(installed) - 1; i >= 0; i-- {
			_ = os.Remove(installed[i])
		}
		for i := len(backups) - 1; i >= 0; i-- {
			_ = os.Rename(backups[i].backup, backups[i].path)
		}
		abortStagedPatch(staged)
	}

	for _, path := range effectivePaths {
		st := states[path]
		if st.originalExist {
			backup, err := makePatchBackup(path)
			if err != nil {
				rollback()
				return nil, err
			}
			backups = append(backups, backup)
		}
		if st.exists {
			if err := os.Rename(staged[path].tmp, path); err != nil {
				rollback()
				return nil, asFileError(err)
			}
			delete(staged, path)
			installed = append(installed, path)
		}
	}
	for _, backup := range backups {
		_ = os.Remove(backup.backup)
	}

	res := &PatchResult{Changes: make([]PatchChange, 0, len(effectivePaths))}
	for _, path := range effectivePaths {
		st := states[path]
		action := "replaced"
		switch {
		case !st.originalExist && st.exists:
			action = "created"
		case st.originalExist && !st.exists:
			action = "deleted"
		}
		res.Changes = append(res.Changes, PatchChange{
			Path:         path,
			Action:       action,
			Replacements: st.replacements,
			Bytes:        int64(len(st.content)),
			Mode:         uint32(st.mode.Perm()),
		})
	}
	keepCreatedDirs = true
	return res, nil
}

func verifyPatchState(st *patchFileState) error {
	info, err := os.Lstat(st.path)
	if !st.originalExist {
		if os.IsNotExist(err) {
			return nil
		}
		if err == nil {
			return proto.Errorf(proto.CodeConflict, "%s appeared while the patch was being prepared", st.path)
		}
		return asFileError(err)
	}
	if err != nil {
		return proto.Errorf(proto.CodeConflict, "%s changed while the patch was being prepared", st.path)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return proto.Errorf(proto.CodeConflict, "%s changed type while the patch was being prepared", st.path)
	}
	if st.originalInfo != nil &&
		(info.Size() != st.originalInfo.Size() || !info.ModTime().Equal(st.originalInfo.ModTime())) {
		return proto.Errorf(proto.CodeConflict, "%s changed while the patch was being prepared", st.path)
	}
	return nil
}

func stagePatchFile(path string, content []byte, mode fs.FileMode) (stagedPatchFile, error) {
	parent := filepath.Dir(path)
	tmp, err := os.CreateTemp(parent, ".rsk-patch-*")
	if err != nil {
		return stagedPatchFile{}, asFileError(err)
	}
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}
	if err := tmp.Chmod(mode); err != nil {
		cleanup()
		return stagedPatchFile{}, asFileError(err)
	}
	if _, err := tmp.Write(content); err != nil {
		cleanup()
		return stagedPatchFile{}, asFileError(err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return stagedPatchFile{}, asFileError(err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return stagedPatchFile{}, asFileError(err)
	}
	return stagedPatchFile{path: path, tmp: tmp.Name(), size: int64(len(content)), mode: mode}, nil
}

func abortStagedPatch(files map[string]stagedPatchFile) {
	for _, file := range files {
		if file.tmp != "" {
			_ = os.Remove(file.tmp)
		}
	}
}

func makePatchBackup(path string) (patchBackup, error) {
	parent := filepath.Dir(path)
	tmp, err := os.CreateTemp(parent, ".rsk-backup-*")
	if err != nil {
		return patchBackup{}, asFileError(err)
	}
	backup := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(backup)
		return patchBackup{}, asFileError(err)
	}
	if err := os.Rename(path, backup); err != nil {
		_ = os.Remove(backup)
		return patchBackup{}, asFileError(err)
	}
	return patchBackup{path: path, backup: backup}, nil
}

func parsePatchMode(value string, fallback fs.FileMode) (fs.FileMode, error) {
	if value == "" {
		return fallback, nil
	}
	raw := strings.TrimPrefix(value, "0o")
	if raw == "" {
		return 0, errors.New("empty mode")
	}
	n, err := strconv.ParseUint(raw, 8, 32)
	if err != nil || n > 0o777 {
		return 0, fmt.Errorf("mode %q must be an octal value from 0000 to 0777", value)
	}
	return fs.FileMode(n), nil
}

func ensureDir(path string) ([]string, error) {
	path = filepath.Clean(path)
	var missing []string
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return nil, fmt.Errorf("%s is not a directory", current)
			}
			break
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			return nil, err
		}
	}
	created := make([]string, 0, len(missing))
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], 0o755); err != nil {
			if os.IsExist(err) {
				continue
			}
			removeEmptyDirs(created)
			return nil, err
		} else {
			created = append(created, missing[i])
		}
	}
	return created, nil
}

func removeEmptyDirs(paths []string) {
	for i := len(paths) - 1; i >= 0; i-- {
		_ = os.Remove(paths[i])
	}
}

func asFileError(err error) *proto.Error {
	var pe *proto.Error
	if errors.As(err, &pe) {
		return pe
	}
	switch {
	case os.IsNotExist(err):
		return proto.Errorf(proto.CodeNotFound, "%v", err)
	case os.IsPermission(err):
		return proto.Errorf(proto.CodeConflict, "%v", err)
	default:
		return proto.Errorf(proto.CodeIO, "%v", err)
	}
}
