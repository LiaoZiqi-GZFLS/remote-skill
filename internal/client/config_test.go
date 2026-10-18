package client

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestParseTarget(t *testing.T) {
	cases := []struct {
		spec string
		user string
		host string
		port int
	}{
		{"host", "", "host", 0},
		{"user@host", "user", "host", 0},
		{"host:2222", "", "host", 2222},
		{"user@host:2222", "user", "host", 2222},
		{"[::1]:2222", "", "::1", 2222},
		{"alias", "", "alias", 0},
	}
	for _, c := range cases {
		user, host, port := ParseTarget(c.spec)
		if user != c.user || host != c.host || port != c.port {
			t.Errorf("ParseTarget(%q) = %q,%q,%d want %q,%q,%d", c.spec, user, host, port, c.user, c.host, c.port)
		}
	}
}

func TestParseForward(t *testing.T) {
	cases := []struct {
		spec   string
		listen string
		target string
	}{
		{"8080", "127.0.0.1:8080", "localhost:8080"},
		{"8080:3000", "127.0.0.1:8080", "localhost:3000"},
		{"8080:db:5432", "127.0.0.1:8080", "db:5432"},
		{"0.0.0.0:8080:localhost:3000", "0.0.0.0:8080", "localhost:3000"},
	}
	for _, c := range cases {
		f, err := ParseForward(c.spec, "127.0.0.1")
		if err != nil {
			t.Fatalf("ParseForward(%q): %v", c.spec, err)
		}
		if f.ListenAddress() != c.listen || f.Target() != c.target {
			t.Errorf("ParseForward(%q) = %s -> %s want %s -> %s", c.spec, f.ListenAddress(), f.Target(), c.listen, c.target)
		}
	}
	if _, err := ParseForward("notaport", "127.0.0.1"); err == nil {
		t.Error("expected an error for a non numeric port")
	}
	rev, err := ParseReverse("9000:127.0.0.1:3000", "127.0.0.1")
	if err != nil {
		t.Fatalf("ParseReverse: %v", err)
	}
	if !rev.Reverse || rev.ListenAddress() != "127.0.0.1:9000" || rev.Target() != "127.0.0.1:3000" {
		t.Fatalf("unexpected reverse spec: %+v", rev)
	}
}

func TestConfigRoundTrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("RSK_CONFIG", filepath.Join(dir, "config.json"))
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	host := &Host{Target: "example.com", User: "me", Port: 2222, RemoteHome: "/home/me"}
	cfg.Put(host)
	if err := cfg.Save(); err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	got, ok := reloaded.Get("me@example.com")
	if !ok {
		t.Fatalf("host missing after reload: %v", reloaded.Names())
	}
	if got.Port != 2222 || got.SocketPath() != "/home/me/.rsk/run/rskd.sock" {
		t.Fatalf("unexpected host: %+v", got)
	}
	if info, err := os.Stat(reloaded.path); err != nil {
		t.Fatal(err)
	} else if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("config file is too permissive: %v", info.Mode())
	}
}

func TestQuote(t *testing.T) {
	if got := Quote("simple"); got != "simple" {
		t.Errorf("Quote(simple) = %q", got)
	}
	if got := Quote("it's here"); got != `'it'\''s here'` {
		t.Errorf("Quote(quoted) = %q", got)
	}
	if got := QuoteJoin("a", "b c"); got != "a 'b c'" {
		t.Errorf("QuoteJoin = %q", got)
	}
	// a leading ~ must stay unquoted so the remote shell expands it
	if got := Quote("~/work dir"); got != `~'/work dir'` {
		t.Errorf("Quote(tilde path) = %q", got)
	}
	if got := Quote("~"); got != "~" {
		t.Errorf("Quote(~) = %q", got)
	}
	if got := Quote("/tmp/a b"); got != `'/tmp/a b'` {
		t.Errorf("Quote(absolute path with space) = %q", got)
	}
}

func TestJoinRemote(t *testing.T) {
	if got := joinRemote("linux", "/home/me", ".rsk", "bin", "0.1.0", "rskilld_linux_amd64"); got != "/home/me/.rsk/bin/0.1.0/rskilld_linux_amd64" {
		t.Errorf("linux join = %q", got)
	}
	got := joinRemote("windows", `C:\Users\me`, ".rsk", "bin", "0.1.0", "rskilld_windows_amd64.exe")
	if got != `C:\Users\me\.rsk\bin\0.1.0\rskilld_windows_amd64.exe` {
		t.Errorf("windows join = %q", got)
	}
}

func TestParseWindowsProbe(t *testing.T) {
	out := "RSK_WINPROBE\r\nMicrosoft Windows [Version 10.0.22631.4460]\r\nC:\\Users\\tester\r\ntester\r\nAMD64\r\n"
	info, err := parseWindowsProbe(out)
	if err != nil {
		t.Fatalf("parseWindowsProbe: %v", err)
	}
	if info.OS != "windows" || info.Arch != "amd64" || info.Home != `C:\Users\tester` || info.User != "tester" {
		t.Fatalf("unexpected info: %+v", info)
	}
	if _, err := parseWindowsProbe("no marker here"); err == nil {
		t.Error("expected an error without the probe marker")
	}
}

func TestWindowsLauncher(t *testing.T) {
	script := windowsLauncher("rskilld_windows_amd64.exe")
	for _, want := range []string{"@echo off", `%~dp0rskilld_windows_amd64.exe`, `--socket "%USERPROFILE%\.rsk\run\rskd.sock"`, "%*", "\r\n"} {
		if !strings.Contains(script, want) {
			t.Errorf("launcher is missing %q:\n%s", want, script)
		}
	}
}
