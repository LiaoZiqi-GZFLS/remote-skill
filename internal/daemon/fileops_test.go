package daemon

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"remote-skill/internal/proto"
)

func TestReadFilePathWindowAndTruncation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.txt")
	content := "one\ntwo\nthree\nfour"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := ReadFilePath(path, FileReadOptions{Offset: 2, Limit: 2, MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if res.Content != "two\nthree\n" {
		t.Fatalf("content = %q", res.Content)
	}
	if res.Lines != 2 || res.NextOffset != 4 || !res.Truncated || res.EOF {
		t.Fatalf("unexpected window metadata: %+v", res)
	}

	res, err = ReadFilePath(path, FileReadOptions{Offset: 4, Limit: 2, MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if res.Content != "four" || res.Lines != 1 || !res.EOF {
		t.Fatalf("unexpected tail result: %+v", res)
	}

	res, err = ReadFilePath(path, FileReadOptions{Offset: 20, Limit: 2, MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if res.Content != "" || !res.EOF {
		t.Fatalf("expected EOF past end: %+v", res)
	}

	res, err = ReadFilePath(path, FileReadOptions{Offset: 1, Limit: 0, MaxBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	if res.Content != "one\n" || !res.Truncated || res.NextOffset != 2 {
		t.Fatalf("unexpected byte-limited result: %+v", res)
	}
}

func TestReadFilePathBinaryAndMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binary.dat")
	if err := os.WriteFile(path, []byte{0x00, 0x01, 0x02, 0xff}, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFilePath(path, FileReadOptions{}); err == nil {
		t.Fatal("expected binary read to fail")
	} else {
		var pe *proto.Error
		if !errors.As(err, &pe) || pe.Code != proto.CodeBadRequest {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	res, err := ReadFilePath(path, FileReadOptions{MetadataOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Binary || !res.MetadataOnly || res.Content != "" {
		t.Fatalf("unexpected metadata result: %+v", res)
	}
}

func TestAtomicWriteCreateOverwriteAndAbort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "file.txt")
	writer, err := beginAtomicWrite(path, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(writer, "first"); err != nil {
		t.Fatal(err)
	}
	res, err := writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created || res.Overwritten || res.Bytes != 5 {
		t.Fatalf("unexpected create result: %+v", res)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "first" {
		t.Fatalf("read after create = %q, %v", data, err)
	}

	if _, err := beginAtomicWrite(path, false, false); err == nil {
		t.Fatal("expected overwrite refusal")
	}

	writer, err = beginAtomicWrite(path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(writer, "second"); err != nil {
		t.Fatal(err)
	}
	res, err = writer.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if res.Created || !res.Overwritten || res.Bytes != 6 {
		t.Fatalf("unexpected overwrite result: %+v", res)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "second" {
		t.Fatalf("read after overwrite = %q, %v", data, err)
	}

	abortPath := filepath.Join(dir, "aborted", "file.txt")
	writer, err = beginAtomicWrite(abortPath, false, true)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(writer, "partial")
	writer.Abort()
	if _, err := os.Stat(abortPath); !os.IsNotExist(err) {
		t.Fatalf("aborted file should not exist: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(abortPath)); !os.IsNotExist(err) {
		t.Fatalf("created parent should be cleaned after abort: %v", err)
	}
}

func TestAtomicWritePreservesMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not preserve POSIX permission bits")
	}
	path := filepath.Join(t.TempDir(), "script.sh")
	if err := os.WriteFile(path, []byte("old\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	writer, err := beginAtomicWrite(path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(writer, "new\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("mode = %o", info.Mode().Perm())
	}
}

func TestApplyFilePatchReplacementAddDelete(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.txt")
	b := filepath.Join(dir, "b.txt")
	c := filepath.Join(dir, "nested", "c.txt")
	if err := os.WriteFile(a, []byte("alpha beta alpha\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("remove me\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	two := 2
	res, err := ApplyFilePatch([]ResolvedPatchOperation{
		{Op: "replace", Path: a, Old: "alpha", New: "gamma", Count: &two},
		{Op: "add", Path: c, Content: "new file\n", Mode: "0600", CreateParents: true},
		{Op: "delete", Path: b},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Changes) != 3 {
		t.Fatalf("changes = %+v", res.Changes)
	}
	if data, err := os.ReadFile(a); err != nil || string(data) != "gamma beta gamma\n" {
		t.Fatalf("a = %q, %v", data, err)
	}
	if data, err := os.ReadFile(c); err != nil || string(data) != "new file\n" {
		t.Fatalf("c = %q, %v", data, err)
	}
	if _, err := os.Stat(b); !os.IsNotExist(err) {
		t.Fatalf("b should be deleted: %v", err)
	}
	if runtime.GOOS != "windows" {
		if info, err := os.Stat(c); err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("c mode = %v, %v", info, err)
		}
	}
}

func TestApplyFilePatchMismatchIsAllOrNothing(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.txt")
	c := filepath.Join(dir, "new.txt")
	if err := os.WriteFile(a, []byte("only once\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	two := 2
	_, err := ApplyFilePatch([]ResolvedPatchOperation{
		{Op: "add", Path: c, Content: "must not appear\n"},
		{Op: "replace", Path: a, Old: "once", New: "twice", Count: &two},
	})
	if err == nil || !strings.Contains(err.Error(), "matched 1 time") {
		t.Fatalf("unexpected error: %v", err)
	}
	if data, readErr := os.ReadFile(a); readErr != nil || string(data) != "only once\n" {
		t.Fatalf("original changed: %q, %v", data, readErr)
	}
	if _, statErr := os.Stat(c); !os.IsNotExist(statErr) {
		t.Fatalf("new file should not exist: %v", statErr)
	}
}

func TestApplyFilePatchRejectsNetNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "temporary.txt")
	_, err := ApplyFilePatch([]ResolvedPatchOperation{
		{Op: "add", Path: path, Content: "temporary\n"},
		{Op: "delete", Path: path},
	})
	if err == nil || !strings.Contains(err.Error(), "made no file changes") {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("no-op patch created a file: %v", statErr)
	}
}

func TestParsePatchDocument(t *testing.T) {
	ops, err := ParsePatchDocument([]byte(`{"operations":[{"op":"delete","path":"a"}]}`))
	if err != nil || len(ops) != 1 || ops[0].Op != "delete" {
		t.Fatalf("object parse = %+v, %v", ops, err)
	}
	ops, err = ParsePatchDocument([]byte(`[{"op":"add","path":"b","content":""}]`))
	if err != nil || len(ops) != 1 || ops[0].Path != "b" {
		t.Fatalf("array parse = %+v, %v", ops, err)
	}
	if _, err := ParsePatchDocument([]byte(`[{"op":"add","path":"c","unknown":true}]`)); err == nil {
		t.Fatal("expected unknown field to be rejected")
	}
	if _, err := ParsePatchDocument([]byte(`[]`)); err == nil {
		t.Fatal("expected empty patch to be rejected")
	}
}
