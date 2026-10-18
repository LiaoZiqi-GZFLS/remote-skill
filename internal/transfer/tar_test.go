package transfer

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestArchiveExtractRoundTrip(t *testing.T) {
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "nested", "deep"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "top.txt"), []byte("top"), 0o644); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(src, "run.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho hi\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	blob := bytes.Repeat([]byte{0x00, 0xff, 0x10}, 100000)
	if err := os.WriteFile(filepath.Join(src, "nested", "deep", "blob.bin"), blob, 0o600); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Symlink("top.txt", filepath.Join(src, "link")); err != nil {
			t.Logf("symlink not supported: %v", err)
		}
	}

	var buf bytes.Buffer
	archived, err := Archive(&buf, []string{src}, ArchiveOptions{Recursive: true, Root: filepath.Dir(src)})
	if err != nil {
		t.Fatal(err)
	}
	if archived.Files != 3 || archived.Dirs < 3 {
		t.Fatalf("archive stats: %+v", archived)
	}

	dst := t.TempDir()
	extracted, err := Extract(bytes.NewReader(buf.Bytes()), dst, ExtractOptions{Overwrite: true, Touch: true})
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Base(src)
	got, err := os.ReadFile(filepath.Join(dst, base, "nested", "deep", "blob.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, blob) {
		t.Fatal("binary content differs after round trip")
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(filepath.Join(dst, base, "run.sh"))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o111 == 0 {
			t.Fatalf("exec bit lost: %v", info.Mode())
		}
	}
	if extracted.Files != 3 {
		t.Fatalf("extract stats: %+v", extracted)
	}
}

func TestExtractRejectsTraversal(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	body := []byte("evil")
	if err := tw.WriteHeader(&tar.Header{Name: "../../evil.txt", Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "link", Linkname: "/etc/passwd", Mode: 0o777, Typeflag: tar.TypeSymlink}); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	res, err := Extract(bytes.NewReader(buf.Bytes()), root, ExtractOptions{Overwrite: true})
	if err == nil && len(res.Warnings) == 0 {
		t.Fatalf("expected traversal to be rejected, got %+v", res)
	}
	if _, statErr := os.Stat(filepath.Join(filepath.Dir(root), "evil.txt")); statErr == nil {
		t.Fatal("file escaped the destination directory")
	}
}

func TestStripAndRename(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	body := []byte("x")
	_ = tw.WriteHeader(&tar.Header{Name: "pkg/a.txt", Mode: 0o644, Size: 1, Typeflag: tar.TypeReg})
	_, _ = tw.Write(body)
	_ = tw.Close()

	dst := t.TempDir()
	if _, err := Extract(bytes.NewReader(buf.Bytes()), dst, ExtractOptions{
		Overwrite:       true,
		StripComponents: 1,
		TopRename:       "renamed",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "renamed")); err != nil {
		t.Fatalf("renamed file missing: %v", err)
	}
}

func TestExtractDoesNotOverwriteByDefault(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	_ = tw.WriteHeader(&tar.Header{Name: "a.txt", Mode: 0o644, Size: 3, Typeflag: tar.TypeReg})
	_, _ = tw.Write([]byte("new"))
	_ = tw.Close()

	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(dst, "a.txt"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := Extract(bytes.NewReader(buf.Bytes()), dst, ExtractOptions{Overwrite: false})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(dst, "a.txt"))
	if string(got) != "old" {
		t.Fatalf("file was overwritten: %q", got)
	}
	if len(res.Skipped) != 1 || !strings.Contains(res.Skipped[0], "exists") {
		t.Fatalf("skipped list: %+v", res.Skipped)
	}
}

func TestSafeJoin(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	cases := map[string]bool{
		"a/b.txt":     true,
		"./a.txt":     true,
		"/abs.txt":    true,
		"../escape":   false,
		"a/../../esc": false,
		"":            true,
	}
	for name, ok := range cases {
		_, err := SafeJoin(root, name)
		if ok && err != nil {
			t.Errorf("SafeJoin(%q) unexpected error: %v", name, err)
		}
		if !ok && err == nil {
			t.Errorf("SafeJoin(%q) should have failed", name)
		}
	}
}
