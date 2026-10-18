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
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// SSH runs commands on a remote host through the system ssh client, so
// ~/.ssh/config, ProxyJump, agents, certificates and 2FA all keep working.
type SSH struct {
	Host     *Host
	Verbose  bool
	Logw     io.Writer
	BatchAll bool
	// Impl selects the ssh client: "ssh" (OpenSSH, default) or "plink"
	// (PuTTY, needed for password authentication on Windows).
	Impl string
	// Password is handed to plink. It is never logged.
	Password string
	// AllowPrompts lets plink ask for the host key (used by `rsk login`).
	AllowPrompts bool
	// HostKey pins a SHA256 fingerprint for plink (TOFU without prompts).
	HostKey string

	noMux   bool
	muxInit bool
}

// NewSSH builds a transport for a host.
func NewSSH(h *Host) *SSH {
	return &SSH{Host: h, Logw: os.Stderr}
}

// ControlPath is the multiplexing socket for this host.
func (s *SSH) ControlPath() string {
	sum := sha256.Sum256([]byte(s.Host.Spec() + "|" + fmt.Sprint(s.Host.Port) + "|" + s.Host.Identity))
	name := hex.EncodeToString(sum[:8]) + ".sock"
	return filepath.Join(RuntimeDir(), "cm", name)
}

func (s *SSH) multiplex() bool {
	if s.noMux {
		return false
	}
	if s.Host.ControlMux != nil {
		return *s.Host.ControlMux
	}
	// Win32-OpenSSH ships without connection multiplexing support and fails
	// with "getsockname failed" when ControlMaster is requested.
	if runtime.GOOS == "windows" {
		return false
	}
	return true
}

// DisableMux remembers that this host cannot multiplex connections.
func (s *SSH) DisableMux() {
	s.noMux = true
	off := false
	s.Host.ControlMux = &off
}

func (s *SSH) args(remoteCmd string, allocTTY bool) []string {
	args := []string{}
	if !allocTTY {
		args = append(args, "-T")
	}
	if s.BatchAll {
		args = append(args, "-o", "BatchMode=yes")
	}
	args = append(args,
		"-o", "ConnectTimeout=15",
		"-o", "ServerAliveInterval=15",
		"-o", "ServerAliveCountMax=4",
	)
	if s.multiplex() {
		cp := s.ControlPath()
		if err := os.MkdirAll(filepath.Dir(cp), 0o700); err == nil {
			args = append(args, "-o", "ControlMaster=auto", "-o", "ControlPath="+cp, "-o", "ControlPersist=300")
		}
	}
	if s.Host.Port > 0 {
		args = append(args, "-p", fmt.Sprint(s.Host.Port))
	}
	if s.Host.Identity != "" {
		args = append(args, "-i", s.Host.Identity)
	}
	args = append(args, s.Host.SSHArgs...)
	args = append(args, s.Host.Spec())
	if remoteCmd != "" {
		args = append(args, remoteCmd)
	}
	return args
}

func (s *SSH) logf(format string, args ...any) {
	if s.Verbose && s.Logw != nil {
		fmt.Fprintf(s.Logw, "[rsk] "+format+"\n", args...)
	}
}

// resolveImpl reports which ssh client this transport uses.
func (s *SSH) resolveImpl() string {
	if s.Impl != "" {
		return s.Impl
	}
	return "ssh"
}

// plinkPath locates PuTTY's plink on this machine.
func (s *SSH) plinkPath() (string, error) {
	if path, err := exec.LookPath("plink.exe"); err == nil {
		return path, nil
	}
	if path, err := exec.LookPath("plink"); err == nil {
		return path, nil
	}
	for _, candidate := range []string{
		`C:\Program Files\PuTTY\plink.exe`,
		`C:\Program Files (x86)\PuTTY\plink.exe`,
	} {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("plink.exe not found: install PuTTY or use key authentication")
}

// command builds the client process for one remote command.
func (s *SSH) command(ctx context.Context, remote string) (*exec.Cmd, error) {
	if s.resolveImpl() == "plink" {
		plink, err := s.plinkPath()
		if err != nil {
			return nil, err
		}
		args := []string{"-ssh"}
		if !s.AllowPrompts {
			args = append(args, "-batch")
		}
		if s.Host.Port > 0 {
			args = append(args, "-P", fmt.Sprint(s.Host.Port))
		}
		if s.Host.User != "" {
			args = append(args, "-l", s.Host.User)
		}
		if s.Password != "" {
			args = append(args, "-pw", s.Password)
		}
		if s.Host.Identity != "" {
			args = append(args, "-i", s.Host.Identity)
		}
		if s.HostKey != "" {
			args = append(args, "-hostkey", s.HostKey)
		}
		args = append(args, s.Host.Target)
		if remote != "" {
			args = append(args, remote)
		}
		return exec.CommandContext(ctx, plink, args...), nil
	}
	return exec.CommandContext(ctx, "ssh", s.args(remote, false)...), nil
}

// RunInteractive runs a remote command attached to the local terminal, so
// plink can ask the user to confirm a new host key.
func (s *SSH) RunInteractive(ctx context.Context, remote string) error {
	cmd, err := s.command(ctx, remote)
	if err != nil {
		return err
	}
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// ProbeHostKey asks the server for its host key by making one connection
// without a pin and parsing the fingerprint plink prints for a new key.
func (s *SSH) ProbeHostKey(ctx context.Context, timeout time.Duration) (string, error) {
	saved := s.HostKey
	s.HostKey = ""
	defer func() { s.HostKey = saved }()
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var buf bytes.Buffer
	_ = s.Run(probeCtx, "echo rsk-hostkey-probe", nil, io.Discard, &buf)
	out := buf.String()
	re := regexp.MustCompile(`SHA256:[A-Za-z0-9+/=]+`)
	if m := re.FindString(out); m != "" {
		return m, nil
	}
	return "", fmt.Errorf("could not read the host key fingerprint from %s", strings.TrimSpace(out))
}

// looksLikeMuxFailure reports whether ssh rejected multiplexing options.
func looksLikeMuxFailure(stderr string) bool {
	l := strings.ToLower(stderr)
	for _, needle := range []string{
		"controlpath", "control path", "controlmaster", "bad configuration option",
		"unsupported option", "not supported", "getsockname", "not a socket",
	} {
		if strings.Contains(l, needle) {
			return true
		}
	}
	return false
}

// Run executes a remote command to completion.
func (s *SSH) Run(ctx context.Context, remoteCmd string, stdin io.Reader, stdout, stderr io.Writer) error {
	if stderr == nil {
		stderr = io.Discard
	}
	var buf bytes.Buffer
	tee := io.MultiWriter(stderr, &buf)
	cmd, err := s.command(ctx, remoteCmd)
	if err != nil {
		return err
	}
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = tee
	s.logf("running ssh %s", strings.Join(s.args(remoteCmd, false), " "))
	err = cmd.Run()
	if err != nil && s.resolveImpl() == "ssh" && s.multiplex() && looksLikeMuxFailure(buf.String()) {
		s.DisableMux()
		s.logf("control multiplexing unsupported, retrying without it")
		buf.Reset()
		cmd = exec.CommandContext(ctx, "ssh", s.args(remoteCmd, false)...)
		cmd.Stdin = stdin
		cmd.Stdout = stdout
		cmd.Stderr = io.MultiWriter(stderr, &buf)
		return cmd.Run()
	}
	return err
}

// Output runs a remote command and returns its stdout, failing on non-zero
// exit with the stderr text in the error.
func (s *SSH) Output(ctx context.Context, remoteCmd string) (string, error) {
	var out, errBuf bytes.Buffer
	err := s.Run(ctx, remoteCmd, nil, &out, &errBuf)
	if err != nil {
		msg := strings.TrimSpace(errBuf.String())
		if msg == "" {
			msg = err.Error()
		}
		return out.String(), fmt.Errorf("%s", msg)
	}
	return out.String(), nil
}

// Start launches a long lived ssh session, typically the daemon bridge.
func (s *SSH) Start(ctx context.Context, remoteCmd string) (*exec.Cmd, io.WriteCloser, io.ReadCloser, error) {
	cmd, err := s.command(ctx, remoteCmd)
	if err != nil {
		return nil, nil, nil, err
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, nil, err
	}
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	s.logf("starting ssh %s", strings.Join(s.args(remoteCmd, false), " "))
	if err := cmd.Start(); err != nil {
		return nil, nil, nil, err
	}
	return cmd, stdin, stdout, nil
}

// Quote wraps s for the remote shell.
func Quote(s string) string {
	if s == "" {
		return "''"
	}
	// Keep a leading ~ outside the quotes so the remote shell expands it.
	if s == "~" {
		return "~"
	}
	if strings.HasPrefix(s, "~/") {
		return "~" + Quote(s[1:])
	}
	if !strings.ContainsAny(s, " \t\n\"'\\$`&|;<>()*?[]{}~#!") {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// QuoteJoin quotes and joins arguments into one remote command line.
func QuoteJoin(parts ...string) string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, Quote(p))
	}
	return strings.Join(out, " ")
}

// SCPArgs builds an scp command line mirroring this transport's ssh options.
// Used for platforms where streaming a binary over stdin is not available.
func (s *SSH) SCPArgs(local, remote string) []string {
	args := []string{"-q"}
	if s.BatchAll {
		args = append(args, "-o", "BatchMode=yes")
	}
	args = append(args, "-o", "ConnectTimeout=15")
	if s.Host.Port > 0 {
		args = append(args, "-P", fmt.Sprint(s.Host.Port))
	}
	if s.Host.Identity != "" {
		args = append(args, "-i", s.Host.Identity)
	}
	args = append(args, s.Host.SSHArgs...)
	args = append(args, local, s.Host.Spec()+":"+remote)
	return args
}

// SCPCommand builds a file copy command matching this transport: pscp for
// plink (password auth), scp for OpenSSH.
func (s *SSH) SCPCommand(ctx context.Context, local, remote string) (*exec.Cmd, error) {
	if s.resolveImpl() == "plink" {
		pscp, err := exec.LookPath("pscp.exe")
		if err != nil {
			for _, candidate := range []string{
				`C:\Program Files\PuTTY\pscp.exe`,
				`C:\Program Files (x86)\PuTTY\pscp.exe`,
			} {
				if _, statErr := os.Stat(candidate); statErr == nil {
					pscp = candidate
					err = nil
					break
				}
			}
		}
		if err != nil {
			return nil, fmt.Errorf("pscp.exe not found: install PuTTY for password authenticated uploads")
		}
		args := []string{"-q", "-batch"}
		if s.Host.Port > 0 {
			args = append(args, "-P", fmt.Sprint(s.Host.Port))
		}
		if s.Host.User != "" {
			args = append(args, "-l", s.Host.User)
		}
		if s.Password != "" {
			args = append(args, "-pw", s.Password)
		}
		args = append(args, local, s.Host.Target+":"+remote)
		return exec.CommandContext(ctx, pscp, args...), nil
	}
	return exec.CommandContext(ctx, "scp", s.SCPArgs(local, remote)...), nil
}

// TTY reports whether f refers to an interactive terminal.
func TTY(f *os.File) bool {
	if f == nil {
		return false
	}
	st, err := f.Stat()
	if err != nil {
		return false
	}
	return st.Mode()&os.ModeCharDevice != 0
}
