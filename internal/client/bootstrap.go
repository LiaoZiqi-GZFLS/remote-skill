package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"remote-skill/internal/assets"
	"remote-skill/internal/version"
)

// remoteInfo is what we learn about the server before uploading anything.
type remoteInfo struct {
	OS        string
	Arch      string
	Home      string
	User      string
	Shell     string
	HasSha256 bool
	Note      string
}

const probeScript = `printf 'RSK_PROBE\n'; uname -sm; printf '%s\n' "$HOME"; id -un; printf '%s\n' "${SHELL:-/bin/bash}"; command -v sha256sum || command -v shasum || true`

// windowsProbe works through cmd.exe, which every Windows sshd can run
// regardless of whether the login shell is cmd or PowerShell.
const windowsProbe = `cmd /c "echo RSK_WINPROBE& ver& echo %USERPROFILE%& echo %USERNAME%& echo %PROCESSOR_ARCHITECTURE%"`

func probeRemote(ctx context.Context, ssh *SSH) (*remoteInfo, error) {
	out, err := ssh.Output(ctx, probeScript)
	if err != nil {
		// A Windows server cannot run the unix probe: fall back to cmd.
		if win, werr := probeWindows(ctx, ssh); werr == nil {
			return win, nil
		}
		return nil, fmt.Errorf("ssh probe failed: %w", err)
	}
	if info, perr := parseProbe(out); perr == nil {
		return info, nil
	}
	if win, werr := probeWindows(ctx, ssh); werr == nil {
		return win, nil
	}
	return nil, fmt.Errorf("unexpected probe output: %q", out)
}

// probeWindows detects a Windows remote through cmd.exe.
func probeWindows(ctx context.Context, ssh *SSH) (*remoteInfo, error) {
	out, err := ssh.Output(ctx, windowsProbe)
	if err != nil {
		return nil, err
	}
	return parseWindowsProbe(out)
}

// parseWindowsProbe turns `cmd /c` probe output into remote info.
func parseWindowsProbe(out string) (*remoteInfo, error) {
	lines := strings.Split(strings.ReplaceAll(out, "\r\n", "\n"), "\n")
	start := -1
	for i, l := range lines {
		if strings.Contains(l, "RSK_WINPROBE") {
			start = i
			break
		}
	}
	if start < 0 {
		return nil, fmt.Errorf("not a windows host")
	}
	var version, home, user, arch string
	for _, l := range lines[start:] {
		l = strings.TrimSpace(l)
		switch {
		case strings.HasPrefix(l, "Microsoft Windows"):
			version = l
		case strings.HasPrefix(l, "RSK_WINPROBE"):
			// the probe marker line itself
		case strings.ContainsAny(l, `:\`) && home == "":
			home = l
		case arch == "" && (l == "AMD64" || l == "ARM64" || l == "x86" || l == "X86"):
			arch = l
		case user == "" && l != "":
			user = l
		}
	}
	if home == "" {
		return nil, fmt.Errorf("could not determine the windows profile directory: %q", out)
	}
	return &remoteInfo{
		OS:    "windows",
		Arch:  normalizeArch(arch),
		Home:  home,
		User:  user,
		Shell: "powershell.exe",
		Note:  version,
	}, nil
}

func parseProbe(out string) (*remoteInfo, error) {
	lines := strings.Split(strings.ReplaceAll(out, "\r\n", "\n"), "\n")
	start := -1
	for i, l := range lines {
		if strings.TrimSpace(l) == "RSK_PROBE" {
			start = i + 1
			break
		}
	}
	if start < 0 || len(lines) < start+3 {
		return nil, fmt.Errorf("unexpected probe output: %q", out)
	}
	uname := strings.Fields(lines[start])
	if len(uname) < 2 {
		return nil, fmt.Errorf("unexpected uname output: %q", lines[start])
	}
	info := &remoteInfo{
		OS:    normalizeOS(uname[0]),
		Arch:  normalizeArch(uname[1]),
		Home:  strings.TrimSpace(lines[start+1]),
		User:  strings.TrimSpace(lines[start+2]),
		Shell: "/bin/bash",
	}
	if len(lines) > start+3 {
		if s := strings.TrimSpace(lines[start+3]); strings.HasPrefix(s, "/") {
			info.Shell = s
		}
	}
	if len(lines) > start+4 && strings.TrimSpace(lines[start+4]) != "" {
		info.HasSha256 = true
	}
	if info.Home == "" {
		return nil, fmt.Errorf("could not determine remote home directory")
	}
	if info.OS != "linux" && info.OS != "darwin" && info.OS != "freebsd" {
		return nil, fmt.Errorf("unsupported remote OS %q (rsk targets linux, darwin and freebsd)", info.OS)
	}
	return info, nil
}

func normalizeOS(s string) string {
	switch strings.ToLower(s) {
	case "linux":
		return "linux"
	case "darwin":
		return "darwin"
	case "freebsd":
		return "freebsd"
	default:
		return strings.ToLower(s)
	}
}

func normalizeArch(s string) string {
	switch strings.ToLower(s) {
	case "x86_64", "amd64":
		return "amd64"
	case "aarch64", "arm64":
		return "arm64"
	case "armv7l", "armv6l", "arm":
		return "arm"
	case "i386", "i686", "x86":
		return "386"
	case "ppc64le":
		return "ppc64le"
	case "s390x":
		return "s390x"
	case "riscv64":
		return "riscv64"
	default:
		return strings.ToLower(s)
	}
}

// identityFresh reports whether the cached bootstrap data can be reused.
func (c *Client) identityFresh() bool {
	h := c.Host
	if h.RemoteBin == "" || h.Version != version.Version || h.RemoteOS == "" || h.RemoteArch == "" {
		return false
	}
	if time.Since(h.LastSeen) >= 12*time.Hour {
		return false
	}
	// Re-upload when the local build changed, so `go build` + `rsk exec`
	// always ships the daemon the client was compiled with.
	data, _, ok := assets.Binary(h.RemoteOS, h.RemoteArch)
	if !ok {
		return false
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]) == h.RemoteSHA
}

// Bootstrap makes sure a runnable rskilld binary exists on the server.
func (c *Client) Bootstrap(ctx context.Context, opts ConnectOptions) error {
	if !opts.Force && c.identityFresh() {
		return nil
	}
	info, err := probeRemote(ctx, c.SSH)
	if err != nil {
		return err
	}
	c.Host.RemoteOS = info.OS
	c.Host.RemoteArch = info.Arch
	c.Host.RemoteHome = info.Home
	c.Host.RemoteUser = info.User
	c.Host.RemoteShell = info.Shell
	c.Host.HasSha256 = info.HasSha256
	if c.Host.Shell == "" {
		c.Host.Shell = info.Shell
	}

	data, name, err := resolveBinary(info)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	remotePath := joinRemote(info.OS, info.Home, ".rsk", "bin", version.Version, name)
	prevBin, prevSHA := c.Host.RemoteBin, c.Host.RemoteSHA
	c.Host.RemoteBin = remotePath
	c.Host.RemoteSHA = hex.EncodeToString(sum[:])
	c.Host.Version = version.Version

	ok, err := c.remoteBinaryCurrent(ctx, info, remotePath, c.Host.RemoteSHA)
	if err != nil {
		return err
	}
	if !ok {
		if !opts.Quiet {
			fmt.Fprintf(c.Logw, "uploading rskilld %s (%s) to %s:%s\n",
				version.Version, humanBytes(int64(len(data))), c.Host.Name, remotePath)
		}
		if err := c.uploadBinary(ctx, info, data, remotePath); err != nil {
			c.Host.RemoteBin, c.Host.RemoteSHA = prevBin, prevSHA
			return err
		}
	}
	c.Host.LastSeen = time.Now()
	if c.Cfg != nil {
		c.Cfg.Put(c.Host)
		_ = c.Cfg.Save()
	}
	return nil
}

// joinRemote joins remote path elements with the separator the remote OS uses.
func joinRemote(goos, base string, parts ...string) string {
	sep := "/"
	if goos == "windows" {
		sep = `\`
	}
	out := strings.TrimRight(base, "/\\")
	for _, p := range parts {
		out += sep + strings.Trim(p, "/\\")
	}
	return out
}

func (c *Client) remoteBinaryCurrent(ctx context.Context, info *remoteInfo, remotePath, wantSHA string) (bool, error) {
	if info.OS == "windows" {
		out, err := c.SSH.Output(ctx, fmt.Sprintf(`cmd /c "if exist %q (echo yes) else (echo no)"`, remotePath))
		if err != nil {
			return false, nil
		}
		return strings.Contains(out, "yes"), nil
	}
	q := Quote(remotePath)
	if info.HasSha256 {
		script := fmt.Sprintf("if [ -x %s ]; then sha256sum %s 2>/dev/null || shasum -a 256 %s; fi", q, q, q)
		out, err := c.SSH.Output(ctx, script)
		if err != nil {
			return false, nil
		}
		fields := strings.Fields(out)
		if len(fields) > 0 && strings.EqualFold(fields[0], wantSHA) {
			return true, nil
		}
		return false, nil
	}
	out, err := c.SSH.Output(ctx, fmt.Sprintf("test -x %s && echo yes || echo no", q))
	if err != nil {
		return false, nil
	}
	return strings.Contains(out, "yes"), nil
}

func (c *Client) uploadBinary(ctx context.Context, info *remoteInfo, data []byte, remotePath string) error {
	if info.OS == "windows" {
		return c.uploadBinaryWindows(ctx, info, data, remotePath)
	}
	dir := path.Dir(remotePath)
	tmp := remotePath + ".tmp"
	script := fmt.Sprintf("mkdir -p %s && cat > %s && chmod 0755 %s && mv -f %s %s",
		Quote(dir), Quote(tmp), Quote(tmp), Quote(tmp), Quote(remotePath))
	var errBuf bytes.Buffer
	if err := c.SSH.Run(ctx, script, bytes.NewReader(data), io.Discard, &errBuf); err != nil {
		msg := strings.TrimSpace(errBuf.String())
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("upload failed: %s", msg)
	}
	return nil
}

// uploadBinaryWindows ships the daemon with scp (Windows has no usable
// "cat > file" through the default shell) and drops a small launcher next to
// it so the bridge can be started without any shell quoting games.
func (c *Client) uploadBinaryWindows(ctx context.Context, info *remoteInfo, data []byte, remotePath string) error {
	dir := filepath.Dir(strings.ReplaceAll(remotePath, `\`, "/"))
	if _, err := c.SSH.Output(ctx, fmt.Sprintf(`cmd /c "if not exist %q mkdir %q"`, dir, dir)); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp("", "rskilld-*.exe")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	tmp.Close()

	remoteFile := strings.ReplaceAll(remotePath, `\`, "/")
	scp, err := c.SSH.SCPCommand(ctx, tmp.Name(), remoteFile)
	if err != nil {
		return err
	}
	var errBuf bytes.Buffer
	scp.Stderr = &errBuf
	if err := scp.Run(); err != nil {
		return fmt.Errorf("scp %s: %v: %s", remoteFile, err, strings.TrimSpace(errBuf.String()))
	}

	// Launcher: %~dp0 keeps it independent of the profile path.
	launcher := windowsLauncher(filepath.Base(remotePath))
	scriptPath := filepath.Join(dir, "bridge.cmd")
	tmpScript, err := os.CreateTemp("", "rsk-bridge-*.cmd")
	if err != nil {
		return err
	}
	defer os.Remove(tmpScript.Name())
	if _, err := tmpScript.WriteString(launcher); err != nil {
		tmpScript.Close()
		return err
	}
	tmpScript.Close()
	scp, err = c.SSH.SCPCommand(ctx, tmpScript.Name(), strings.ReplaceAll(scriptPath, `\`, "/"))
	if err != nil {
		return err
	}
	errBuf.Reset()
	scp.Stderr = &errBuf
	if err := scp.Run(); err != nil {
		return fmt.Errorf("scp launcher: %v: %s", err, strings.TrimSpace(errBuf.String()))
	}
	return nil
}

// windowsLauncher renders bridge.cmd. %~dp0 keeps it independent of where the
// profile lives, and %* forwards extra flags such as --restart.
func windowsLauncher(exeName string) string {
	return "@echo off\r\n" +
		"setlocal\r\n" +
		"\"" + `%~dp0` + exeName + `" bridge --socket "%USERPROFILE%\.rsk\run\rskd.sock" --log "%USERPROFILE%\.rsk\log\rskd.log" %*` + "\r\n"
}

// resolveBinary picks the daemon build for the remote platform.
func resolveBinary(info *remoteInfo) ([]byte, string, error) {
	if data, name, ok := assets.Binary(info.OS, info.Arch); ok {
		return data, name, nil
	}
	data, err := buildLocal(info.OS, info.Arch)
	if err != nil {
		return nil, "", fmt.Errorf("no embedded rskilld for %s/%s (%v); run `go run ./tools/pack` to add one",
			info.OS, info.Arch, err)
	}
	return data, fmt.Sprintf("rskilld_%s_%s", info.OS, info.Arch), nil
}

// buildLocal cross compiles rskilld with a local Go toolchain.
func buildLocal(goos, goarch string) ([]byte, error) {
	root, err := moduleRoot()
	if err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp("", "rskilld-*")
	if err != nil {
		return nil, err
	}
	out := tmp.Name()
	tmp.Close()
	defer os.Remove(out)

	cmd := exec.Command("go", "build", "-trimpath", "-o", out, "./cmd/rskilld")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("go build: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return os.ReadFile(out)
}

func moduleRoot() (string, error) {
	dirs := []string{}
	if wd, err := os.Getwd(); err == nil {
		dirs = append(dirs, wd)
	}
	if exe, err := os.Executable(); err == nil {
		dirs = append(dirs, filepath.Dir(exe))
	}
	for _, start := range dirs {
		dir := start
		for i := 0; i < 8; i++ {
			if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
				if _, err := os.Stat(filepath.Join(dir, "cmd", "rskilld")); err == nil {
					return dir, nil
				}
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	return "", fmt.Errorf("project source tree not found")
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
