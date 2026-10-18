// Command rsk drives persistent remote workspaces over ssh, in the spirit of
// VS Code Remote-SSH but made for agents and scripts: bootstrap a host, keep a
// terminal session alive on it, then run commands, move files and forward
// ports without paying for a new shell every time.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/term"

	"remote-skill/internal/client"
	"remote-skill/internal/daemon"
	"remote-skill/internal/proto"
	"remote-skill/internal/version"
)

const usage = `rsk - persistent remote workspaces over ssh

usage:
  rsk up <host> [flags]                  bootstrap a host and open a session
  rsk exec <host> [flags] [--] <cmd...>  run a command inside the session
  rsk sh <host> [flags]                  attach the session to this terminal
  rsk put <host> <local> [remote]        upload a file or directory
  rsk get <host> <remote> [local]        download a file or directory
  rsk ls <host> [path]                   list a remote directory
  rsk ports <host> [--json]              list listening TCP ports on the server
  rsk sync <host> <local> <remote>       incremental upload (--exclude, --delete, --dry-run)
  rsk ps <host>                          list background jobs (--detach)
  rsk kill <host> <job-id>               stop a background job
  rsk logs <host> [flags]                show recent session output
  rsk status <host> [flags]              daemon and session status
  rsk forward <host> <spec>              forward a local port to the server
  rsk down <host> [flags]                stop the remote daemon
  rsk doctor <host>                      diagnose connectivity and versions
  rsk login <host> [--password-stdin]    store a password (DPAPI) for password-only hosts
  rsk logout <host>                      forget the stored password
  rsk hosts                              list configured hosts
  rsk version                            print the client version

common flags:
  --session NAME    session to use (default "main")
  --user NAME       ssh user
  --port N          ssh port
  --identity FILE   ssh private key
  --ssh-arg ARG     extra ssh option, repeatable
  --verbose         trace ssh and protocol activity
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	var err error
	switch cmd {
	case "up", "connect":
		err = cmdUp(args)
	case "exec", "run":
		err = cmdExec(args)
	case "sh", "attach":
		err = cmdSh(args)
	case "put", "upload":
		err = cmdPut(args)
	case "get", "download":
		err = cmdGet(args)
	case "ls", "list":
		err = cmdLs(args)
	case "ports":
		err = cmdPorts(args)
	case "sync":
		err = cmdSync(args)
	case "ps", "jobs":
		err = cmdPs(args)
	case "kill":
		err = cmdKill(args)
	case "logs", "log":
		err = cmdLogs(args)
	case "status":
		err = cmdStatus(args)
	case "forward", "fwd":
		err = cmdForward(args)
	case "down", "stop":
		err = cmdDown(args)
	case "doctor":
		err = cmdDoctor(args)
	case "login", "auth":
		err = cmdLogin(args)
	case "logout":
		err = cmdLogout(args)
	case "__broker":
		err = cmdBroker(args)
	case "hosts":
		err = cmdHosts(args)
	case "version", "--version", "-v":
		fmt.Printf("rsk %s (api %d)\n", version.Version, proto.APIVersion)
		return
	case "help", "--help", "-h":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "rsk: unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
	if err != nil {
		if code, ok := err.(exitCode); ok {
			os.Exit(int(code))
		}
		fmt.Fprintf(os.Stderr, "rsk: %v\n", err)
		os.Exit(1)
	}
}

// exitCode lets commands report a specific process exit status.
type exitCode int

func (e exitCode) Error() string { return fmt.Sprintf("exit status %d", int(e)) }

// multiFlag collects repeatable flag values.
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

// hostFlags are the connection flags shared by every command.
type hostFlags struct {
	user      string
	port      int
	identity  string
	sshArgs   multiFlag
	session   string
	shell     string
	env       multiFlag
	noProfile bool
	verbose   bool
	quiet     bool
	inline    bool
	sshImpl   string
	askPass   bool
	noBroker  bool
	useBroker bool
}

func (hf *hostFlags) bind(fs *flag.FlagSet) {
	fs.StringVar(&hf.user, "user", "", "ssh user")
	fs.StringVar(&hf.user, "u", "", "ssh user")
	fs.IntVar(&hf.port, "port", 0, "ssh port")
	fs.IntVar(&hf.port, "p", 0, "ssh port")
	fs.StringVar(&hf.identity, "identity", "", "ssh private key")
	fs.StringVar(&hf.identity, "i", "", "ssh private key")
	fs.Var(&hf.sshArgs, "ssh-arg", "extra ssh option (repeatable)")
	fs.StringVar(&hf.session, "session", "", "session name")
	fs.StringVar(&hf.shell, "shell", "", "shell for new sessions")
	fs.Var(&hf.env, "env", "session environment KEY=VALUE (repeatable)")
	fs.BoolVar(&hf.noProfile, "no-profile", false, "start sessions without user rc files")
	fs.BoolVar(&hf.verbose, "verbose", false, "verbose diagnostics")
	fs.BoolVar(&hf.verbose, "v", false, "verbose diagnostics")
	fs.BoolVar(&hf.quiet, "quiet", false, "suppress progress output")
	fs.BoolVar(&hf.quiet, "q", false, "suppress progress output")
	fs.BoolVar(&hf.inline, "inline", false, "keep sessions inside the daemon (no persistence)")
	fs.StringVar(&hf.sshImpl, "ssh-impl", "", "ssh client: ssh (default) or plink (password auth)")
	fs.BoolVar(&hf.askPass, "ask-pass", false, "let ssh prompt on this terminal (passphrase / password)")
	fs.BoolVar(&hf.noBroker, "no-broker", false, "open a dedicated ssh connection instead of reusing the local broker")
	fs.BoolVar(&hf.useBroker, "broker", false, "reuse one local ssh connection for many commands (experimental)")
}

func (hf *hostFlags) apply(h *client.Host) {
	if hf.user != "" {
		h.User = hf.user
	}
	if hf.port != 0 {
		h.Port = hf.port
	}
	if hf.identity != "" {
		h.Identity = hf.identity
	}
	if len(hf.sshArgs) > 0 {
		h.SSHArgs = append(h.SSHArgs, hf.sshArgs...)
	}
}

// persistTo records session shaping flags as host defaults. Only `up` calls
// it, so a one-off `--session` or `--inline` never rewrites the host config.
func (hf *hostFlags) persistTo(h *client.Host) {
	if hf.session != "" {
		h.Session = hf.session
	}
	if hf.shell != "" {
		h.Shell = hf.shell
	}
	if hf.noProfile {
		h.NoProfile = true
	}
	// `up` also clears sticky switches the user did not ask for this time.
	h.Inline = hf.inline
	if !hf.noProfile {
		h.NoProfile = false
	}
	if len(hf.env) > 0 {
		if h.Env == nil {
			h.Env = map[string]string{}
		}
		for _, kv := range hf.env {
			if i := strings.IndexByte(kv, '='); i > 0 {
				h.Env[kv[:i]] = kv[i+1:]
			}
		}
	}
}

func (hf *hostFlags) connectOptions() client.ConnectOptions {
	return client.ConnectOptions{
		Session:   hf.session,
		Shell:     hf.shell,
		Env:       envMap(hf.env),
		NoProfile: hf.noProfile,
		Quiet:     hf.quiet,
	}
}

func envMap(list []string) map[string]string {
	if len(list) == 0 {
		return nil
	}
	out := map[string]string{}
	for _, kv := range list {
		if i := strings.IndexByte(kv, '='); i > 0 {
			out[kv[:i]] = kv[i+1:]
		}
	}
	return out
}

// cmdFlags wraps a FlagSet with positional/flag splitting so flags may appear
// anywhere on the command line.
type cmdFlags struct {
	fs     *flag.FlagSet
	values map[string]bool
	names  map[string]bool
}

func newCmdFlags(name string) *cmdFlags {
	return &cmdFlags{
		fs:     flag.NewFlagSet(name, flag.ContinueOnError),
		values: map[string]bool{},
		names:  map[string]bool{},
	}
}

// complete records which flags take values once they are registered.
func (cf *cmdFlags) complete() {
	cf.fs.VisitAll(func(f *flag.Flag) {
		cf.names[f.Name] = true
		if _, ok := f.Value.(interface{ IsBoolFlag() bool }); !ok {
			cf.values[f.Name] = true
		}
	})
}

// split separates args into positional arguments and known flags.
func (cf *cmdFlags) split(args []string) (pos []string, flags []string, err error) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			pos = append(pos, args[i+1:]...)
			return pos, flags, nil
		}
		if len(a) > 1 && strings.HasPrefix(a, "-") && a != "-" {
			name := strings.TrimLeft(a, "-")
			base := name
			if j := strings.IndexByte(base, '='); j >= 0 {
				base = base[:j]
			}
			if !cf.names[base] {
				// Not one of ours: part of the user's command.
				pos = append(pos, args[i:]...)
				return pos, flags, nil
			}
			flags = append(flags, a)
			if !strings.Contains(name, "=") && cf.values[base] {
				if i+1 >= len(args) {
					return nil, nil, fmt.Errorf("flag %s needs a value", a)
				}
				i++
				flags = append(flags, args[i])
			}
			continue
		}
		pos = append(pos, a)
	}
	return pos, flags, nil
}

func (cf *cmdFlags) parse(args []string) ([]string, error) {
	cf.complete()
	pos, flags, err := cf.split(args)
	if err != nil {
		return nil, err
	}
	if err := cf.fs.Parse(flags); err != nil {
		return nil, err
	}
	return pos, nil
}

// resolveHost finds the configured host or builds an ad-hoc one from a target.
func resolveHost(cfg *client.Config, spec string, hf *hostFlags) (*client.Host, error) {
	if spec == "" {
		return nil, fmt.Errorf("missing host (see `rsk help`)")
	}
	if h, ok := cfg.Get(spec); ok {
		clone := *h
		hf.apply(&clone)
		return &clone, nil
	}
	user, host, port := client.ParseTarget(spec)
	h := &client.Host{Name: spec, Target: host, User: user, Port: port}
	hf.apply(h)
	return h, nil
}

func newClient(ctx context.Context, cfg *client.Config, hf *hostFlags, spec string, opts client.ConnectOptions) (*client.Client, error) {
	host, err := resolveHost(cfg, spec, hf)
	if err != nil {
		return nil, err
	}
	c := client.NewClient(host, cfg)
	c.Verbose = hf.verbose
	c.SSH.Verbose = hf.verbose
	// Never let ssh sit on a password prompt nobody can answer: agents run
	// without a terminal, and a hidden prompt would look like a hang.
	c.SSH.BatchAll = !hf.askPass
	c.NoBroker = hf.noBroker
	c.UseBroker = hf.useBroker
	applyTransport(c, hf.sshImpl)
	c.SessionOverride = hf.session
	c.ShellOverride = hf.shell
	c.EnvOverride = envMap(hf.env)
	c.InlineOverride = hf.inline
	if !opts.Quiet {
		opts.Quiet = hf.quiet
	}
	if err := c.Connect(ctx, opts); err != nil {
		return nil, authHint(err, c.Host)
	}
	return c, nil
}

func withSignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt)
}

// ---------------------------------------------------------------- commands

func cmdUp(args []string) error {
	cf := newCmdFlags("up")
	var hf hostFlags
	hf.bind(cf.fs)
	var (
		workspace = cf.fs.String("workspace", "", "directory to make the session working directory")
		alias     = cf.fs.String("name", "", "name to store the host under")
		restart   = cf.fs.Bool("restart", false, "restart the remote daemon")
		force     = cf.fs.Bool("force", false, "re-upload the client binary")
	)
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: rsk up <host> [flags]")
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	host, err := resolveHost(cfg, pos[0], &hf)
	if err != nil {
		return err
	}
	if *alias != "" {
		host.Name = *alias
	}
	if *workspace != "" {
		host.Workspace = *workspace
	}
	hf.persistTo(host)
	opts := hf.connectOptions()
	opts.Restart = *restart
	opts.Force = *force
	c := client.NewClient(host, cfg)
	c.Verbose = hf.verbose
	c.SSH.Verbose = hf.verbose
	c.SSH.BatchAll = !hf.askPass
	c.NoBroker = hf.noBroker
	c.UseBroker = hf.useBroker
	applyTransport(c, hf.sshImpl)
	c.SessionOverride = hf.session
	c.ShellOverride = hf.shell
	c.EnvOverride = envMap(hf.env)
	c.InlineOverride = hf.inline
	if err := c.Connect(ctx, opts); err != nil {
		return authHint(err, c.Host)
	}
	defer c.Close()
	// Open the session explicitly so `up` does what it says, and so a
	// workspace is applied when the session is first created.
	sessionName := host.SessionName()
	if _, err := c.OpenSession(ctx, daemon.SessionOptions{
		Name:      sessionName,
		Shell:     host.Shell,
		Cwd:       host.Workspace,
		Env:       host.Env,
		NoProfile: host.NoProfile,
	}); err != nil {
		return err
	}
	if host.Workspace != "" {
		cwd, err := c.SetCwd(ctx, sessionName, host.Workspace)
		if err != nil {
			return fmt.Errorf("workspace %s: %w", host.Workspace, err)
		}
		host.Workspace = cwd
	}
	cfg.Put(host)
	if err := cfg.Save(); err != nil {
		return err
	}
	info := c.HostInfo()
	cwd := ""
	if sess, err := c.Scrollback(ctx, sessionName, 0); err == nil {
		cwd = sess.Cwd
	}
	fmt.Printf("host      %s (%s)\n", host.Name, host.Spec())
	fmt.Printf("server    %s/%s  user=%s\n", info.OS, info.Arch, info.User)
	fmt.Printf("daemon    rskilld %s  api %d  pid %d\n", info.Version, info.API, info.PID)
	fmt.Printf("session   %s  cwd=%s\n", sessionName, cwd)
	fmt.Printf("binary    %s\n", host.RemoteBin)
	if host.Inline {
		fmt.Printf("lifetime  inline sessions end when the daemon stops\n")
	} else {
		fmt.Printf("lifetime  persistent; `rsk down %s` keeps sessions running\n", host.Name)
	}
	return nil
}

func cmdExec(args []string) error {
	cf := newCmdFlags("exec")
	var hf hostFlags
	hf.bind(cf.fs)
	var (
		timeout = cf.fs.Duration("timeout", 120*time.Second, "kill the command after this long (0 disables)")
		wait    = cf.fs.Duration("wait", 30*time.Second, "wait this long for a busy session")
		cwd     = cf.fs.String("cwd", "", "working directory for this command")
		asJSON  = cf.fs.Bool("json", false, "print a JSON summary")
		detach  = cf.fs.Bool("detach", false, "run in the background and return immediately")
		stdin   = cf.fs.Bool("stdin", false, "feed local stdin to the command")
	)
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) < 2 {
		return fmt.Errorf("usage: rsk exec <host> [flags] -- <cmd...>")
	}
	host, command := pos[0], strings.Join(pos[1:], " ")
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, host, hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()

	session := c.SessionName(hf.session)
	if *detach {
		return runDetached(ctx, c, session, command, *cwd)
	}
	if *stdin {
		command = withStdinHeredoc(command)
	}
	req := daemon.ExecRequest{
		Session:   session,
		Cmd:       command,
		TimeoutMS: timeout.Milliseconds(),
		WaitMS:    wait.Milliseconds(),
		Cwd:       *cwd,
		Env:       envMap(hf.env),
	}
	var out io.Writer = os.Stdout
	var captured *strings.Builder
	if *asJSON {
		captured = &strings.Builder{}
		out = captured
	}
	res, err := c.Exec(ctx, req, out)
	if err != nil {
		if ctx.Err() != nil {
			return exitCode(130)
		}
		return err
	}
	if *asJSON {
		payload := map[string]any{
			"host":         c.Host.Name,
			"session":      res.Session,
			"exec_id":      res.ID,
			"rc":           res.RC,
			"duration_ms":  res.DurationMS,
			"timed_out":    res.TimedOut,
			"shell_exited": res.ShellExited,
			"cwd":          res.Cwd,
			"stdout":       captured.String(),
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(payload); err != nil {
			return err
		}
	} else if res.TimedOut {
		fmt.Fprintf(os.Stderr, "rsk: command timed out after %s (session %s, cwd %s)\n",
			(time.Duration(res.DurationMS) * time.Millisecond).Round(time.Millisecond), res.Session, res.Cwd)
		fmt.Fprintf(os.Stderr, "rsk: the session survived; use --timeout 0, --detach or a longer --timeout for long jobs\n")
	} else if res.ShellExited {
		fmt.Fprintf(os.Stderr, "rsk: the session shell exited (rc=%d); the next command starts a fresh shell\n", res.RC)
	}
	return exitCode(res.RC)
}

// withStdinHeredoc wraps a command so it reads local stdin. The daemon runs
// commands through a pty, so piped input has to travel inside the command.
func withStdinHeredoc(command string) string {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return command
	}
	return fmt.Sprintf("%s <<'RSK_STDIN_EOF'\n%s\nRSK_STDIN_EOF", command, string(data))
}

func runDetached(ctx context.Context, c *client.Client, session, command, cwd string) error {
	if c.Host.RemoteOS == "windows" {
		return fmt.Errorf("--detach is not supported on windows remotes yet")
	}
	id := jobID()
	inner := command
	if cwd != "" {
		inner = "cd " + client.Quote(cwd) + " && " + inner
	}
	script := fmt.Sprintf(
		"mkdir -p ~/.rsk/jobs; nohup bash -lc %s >~/.rsk/jobs/%s.log 2>&1 </dev/null & __rsk_job=$!; "+
			"printf 'id=%%s\\npid=%%s\\nlog=%%s\\nstarted=%%s\\n' %s \"$__rsk_job\" %s \"$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)\" >~/.rsk/jobs/%s.meta; "+
			"echo RSK_PID=$__rsk_job; echo RSK_LOG=~/.rsk/jobs/%s.log",
		client.Quote(inner), id, client.Quote(id), client.Quote("~/.rsk/jobs/"+id+".log"), id, id)
	var out strings.Builder
	res, err := c.Exec(ctx, daemon.ExecRequest{Session: session, Cmd: script, TimeoutMS: 20000}, &out)
	if err != nil {
		return err
	}
	if res.RC != 0 {
		return fmt.Errorf("failed to start background job (rc=%d)", res.RC)
	}
	pid, logPath := "", ""
	for _, line := range strings.Split(out.String(), "\n") {
		switch {
		case strings.HasPrefix(line, "RSK_PID="):
			pid = strings.TrimPrefix(line, "RSK_PID=")
		case strings.HasPrefix(line, "RSK_LOG="):
			logPath = strings.TrimPrefix(line, "RSK_LOG=")
		}
	}
	fmt.Printf("job       %s\n", id)
	fmt.Printf("pid       %s\n", strings.TrimSpace(pid))
	fmt.Printf("log       %s\n", strings.TrimSpace(logPath))
	fmt.Printf("follow    rsk exec %s --session %s -- tail -f %s\n", c.Host.Name, session, strings.TrimSpace(logPath))
	return nil
}

// cmdPs lists background jobs started with --detach.
func cmdPs(args []string) error {
	cf := newCmdFlags("ps")
	var hf hostFlags
	hf.bind(cf.fs)
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: rsk ps <host>")
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	if c.Host.RemoteOS == "windows" {
		return fmt.Errorf("rsk ps is not supported on windows remotes yet")
	}
	script := `for f in "$HOME"/.rsk/jobs/*.meta; do [ -f "$f" ] || continue; . "$f"; if kill -0 "$pid" 2>/dev/null; then s=running; else s=exited; fi; printf '%s\t%s\t%s\t%s\t%s\n' "$id" "$pid" "$s" "$started" "$log"; done`
	var out strings.Builder
	res, err := c.Exec(ctx, daemon.ExecRequest{Session: c.SessionName(""), Cmd: script, TimeoutMS: 30000}, &out)
	if err != nil {
		return err
	}
	if res.RC != 0 {
		return fmt.Errorf("listing jobs failed (rc=%d)", res.RC)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) == 1 && lines[0] == "" {
		fmt.Println("no background jobs")
		return nil
	}
	fmt.Printf("%-28s %-8s %-8s %-22s %s\n", "ID", "PID", "STATE", "STARTED", "LOG")
	for _, line := range lines {
		f := strings.Split(line, "\t")
		if len(f) < 5 {
			continue
		}
		fmt.Printf("%-28s %-8s %-8s %-22s %s\n", f[0], f[1], f[2], f[3], f[4])
	}
	return nil
}

// cmdKill stops a background job and its children.
func cmdKill(args []string) error {
	cf := newCmdFlags("kill")
	var hf hostFlags
	hf.bind(cf.fs)
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("usage: rsk kill <host> <job-id>")
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	script := fmt.Sprintf(`f="$HOME/.rsk/jobs/%s.meta"; if [ ! -f "$f" ]; then echo "no such job"; exit 3; fi; . "$f"; kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true; sleep 1; if kill -0 "$pid" 2>/dev/null; then kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true; fi; echo "stopped $id"`,
		strings.TrimSpace(pos[1]))
	var out strings.Builder
	res, err := c.Exec(ctx, daemon.ExecRequest{Session: c.SessionName(""), Cmd: script, TimeoutMS: 30000}, &out)
	if err != nil {
		return err
	}
	fmt.Print(out.String())
	if res.RC != 0 {
		return fmt.Errorf("kill failed (rc=%d)", res.RC)
	}
	return nil
}

func jobID() string {
	var b [3]byte
	_, _ = rand.Read(b[:])
	return time.Now().Format("20060102-150405") + "-" + hex.EncodeToString(b[:])
}

func cmdSh(args []string) error {
	cf := newCmdFlags("sh")
	var hf hostFlags
	hf.bind(cf.fs)
	replay := cf.fs.Int("replay", 64<<10, "bytes of scrollback to replay first")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: rsk sh <host> [flags]")
	}
	if !client.TTY(os.Stdin) {
		return fmt.Errorf("rsk sh needs an interactive terminal; use `rsk exec` for scripts")
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	return client.Attach(ctx, c, client.AttachOptions{
		Session: c.SessionName(hf.session),
		Replay:  *replay,
	})
}

func cmdPut(args []string) error {
	cf := newCmdFlags("put")
	var hf hostFlags
	hf.bind(cf.fs)
	recursive := cf.fs.Bool("recursive", false, "upload directories")
	cf.fs.BoolVar(recursive, "r", false, "upload directories")
	overwrite := cf.fs.Bool("overwrite", true, "replace existing files")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) < 2 || len(pos) > 3 {
		return fmt.Errorf("usage: rsk put <host> <local> [remote] [--recursive]")
	}
	local := pos[1]
	remote := "."
	if len(pos) == 3 {
		remote = pos[2]
	}
	st, err := os.Lstat(local)
	if err != nil {
		return err
	}
	if st.IsDir() && !*recursive {
		return fmt.Errorf("%s is a directory (add --recursive)", local)
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	res, err := c.PutPath(ctx, local, remote, *recursive, *overwrite)
	if err != nil {
		return err
	}
	fmt.Printf("uploaded %d file(s), %d dir(s), %s to %s:%s\n",
		res.Files, res.Dirs, humanBytes(res.Bytes), c.Host.Name, remote)
	for _, w := range res.Warnings {
		fmt.Fprintf(os.Stderr, "rsk: warning: %s\n", w)
	}
	return nil
}

func cmdGet(args []string) error {
	cf := newCmdFlags("get")
	var hf hostFlags
	hf.bind(cf.fs)
	recursive := cf.fs.Bool("recursive", false, "download directories")
	cf.fs.BoolVar(recursive, "r", false, "download directories")
	overwrite := cf.fs.Bool("overwrite", true, "replace existing files")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) < 2 || len(pos) > 3 {
		return fmt.Errorf("usage: rsk get <host> <remote> [local] [--recursive]")
	}
	remote := pos[1]
	local := "."
	if len(pos) == 3 {
		local = pos[2]
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	info, res, err := c.Get(ctx, remote, local, *recursive, *overwrite)
	if err != nil {
		return err
	}
	if res != nil {
		fmt.Printf("downloaded %d file(s), %d dir(s), %s from %s:%s\n",
			res.Files, res.Dirs, humanBytes(res.Bytes), c.Host.Name, info.Path)
	}
	if res != nil {
		for _, w := range res.Warnings {
			fmt.Fprintf(os.Stderr, "rsk: warning: %s\n", w)
		}
	}
	return nil
}

func cmdLs(args []string) error {
	cf := newCmdFlags("ls")
	var hf hostFlags
	hf.bind(cf.fs)
	long := cf.fs.Bool("long", false, "long listing")
	cf.fs.BoolVar(long, "l", false, "long listing")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) < 1 || len(pos) > 2 {
		return fmt.Errorf("usage: rsk ls <host> [path]")
	}
	remote := "."
	if len(pos) == 2 {
		remote = pos[1]
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	entries, dir, err := c.List(ctx, remote)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", dir)
	for _, e := range entries {
		name := e.Name
		if e.Dir {
			name += "/"
		}
		if *long {
			fmt.Printf("%10d  %s  %s\n", e.Size, e.Mtime.Format("2006-01-02 15:04"), name)
		} else {
			fmt.Println(name)
		}
	}
	return nil
}

// cmdPorts lists listening ports so a forward can be picked from real data.
func cmdPorts(args []string) error {
	cf := newCmdFlags("ports")
	var hf hostFlags
	hf.bind(cf.fs)
	asJSON := cf.fs.Bool("json", false, "machine readable output")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: rsk ports <host> [--json]")
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	ports, err := c.Ports(ctx)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(ports)
	}
	if len(ports) == 0 {
		fmt.Println("no listening TCP ports")
		return nil
	}
	fmt.Printf("%-22s %-6s %s\n", "ADDRESS", "PORT", "FORWARD")
	for _, p := range ports {
		hint := ""
		if p.Local {
			hint = fmt.Sprintf("rsk forward %s %d:localhost:%d", c.Host.Name, p.Port, p.Port)
		}
		fmt.Printf("%-22s %-6d %s\n", p.Address, p.Port, hint)
	}
	return nil
}

// cmdSync mirrors a local directory to the server, transferring only changes.
func cmdSync(args []string) error {
	cf := newCmdFlags("sync")
	var hf hostFlags
	hf.bind(cf.fs)
	var exclude multiFlag
	cf.fs.Var(&exclude, "exclude", "path or glob to skip (repeatable)")
	del := cf.fs.Bool("delete", false, "delete remote files that no longer exist locally")
	dry := cf.fs.Bool("dry-run", false, "report changes without transferring")
	asJSON := cf.fs.Bool("json", false, "machine readable output")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 3 {
		return fmt.Errorf("usage: rsk sync <host> <local> <remote> [--exclude GLOB] [--delete] [--dry-run]")
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	res, err := c.Sync(ctx, pos[1], pos[2], client.SyncOptions{
		Exclude: exclude,
		Delete:  *del,
		DryRun:  *dry,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	verb := "synced"
	if *dry {
		verb = "would sync"
	}
	fmt.Printf("%s: +%d added, ~%d changed, -%d deleted, =%d unchanged (%s)\n",
		verb, res.Added, res.Changed, res.Deleted, res.Unchanged, humanBytes(res.Bytes))
	for _, f := range res.Files {
		fmt.Printf("  upload  %s\n", f)
	}
	for _, f := range res.Removed {
		fmt.Printf("  delete  %s\n", f)
	}
	return nil
}

func cmdLogs(args []string) error {
	cf := newCmdFlags("logs")
	var hf hostFlags
	hf.bind(cf.fs)
	tail := cf.fs.Int("tail", 64<<10, "bytes of output to show")
	follow := cf.fs.Bool("follow", false, "keep streaming new output")
	cf.fs.BoolVar(follow, "f", false, "keep streaming new output")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: rsk logs <host> [--tail N] [--follow]")
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	session := c.SessionName(hf.session)
	if *follow {
		return client.Attach(ctx, c, client.AttachOptions{Session: session, Replay: *tail, NoInput: true})
	}
	res, err := c.Scrollback(ctx, session, *tail)
	if err != nil {
		return err
	}
	os.Stdout.Write(res.Data)
	if res.Busy {
		fmt.Fprintln(os.Stderr, "rsk: session is currently running a command")
	}
	return nil
}

func cmdStatus(args []string) error {
	cf := newCmdFlags("status")
	var hf hostFlags
	hf.bind(cf.fs)
	asJSON := cf.fs.Bool("json", false, "machine readable output")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: rsk status <host> [--json]")
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	info, err := c.Status(ctx)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(info)
	}
	fmt.Printf("host      %s (%s)\n", c.Host.Name, c.Host.Spec())
	fmt.Printf("server    %s/%s user=%s pid=%d\n", info.OS, info.Arch, info.User, info.PID)
	fmt.Printf("daemon    rskilld %s api=%d uptime=%s\n", info.Version, info.API, (time.Duration(info.UptimeSec) * time.Second).Round(time.Second))
	fmt.Printf("socket    %s\n", info.Socket)
	if c.Host.Inline {
		fmt.Printf("sessions  inline (die with the daemon)\n")
	} else {
		fmt.Printf("sessions  persistent (survive daemon restarts)\n")
	}
	if len(info.Sessions) == 0 {
		fmt.Printf("          none running\n")
		return nil
	}
	fmt.Printf("          %d running\n", len(info.Sessions))
	for _, s := range info.Sessions {
		state := "idle"
		if s.Busy {
			state = "running"
		}
		if s.Closed {
			state = "closed"
		}
		last := ""
		if s.Last != nil {
			last = fmt.Sprintf("last rc=%d %.2fs", s.Last.RC, float64(s.Last.DurationMS)/1000)
		}
		restored := ""
		if s.RestoredAt != nil {
			restored = "restored=" + s.RestoredAt.Format("2006-01-02 15:04")
		}
		fmt.Printf("  %-12s %-8s pid=%-6d cwd=%s %s %s\n", s.Name, state, s.PID, s.Cwd, last, restored)
	}
	return nil
}

func cmdForward(args []string) error {
	cf := newCmdFlags("forward")
	var hf hostFlags
	hf.bind(cf.fs)
	bind := cf.fs.String("bind", "127.0.0.1", "local address to listen on")
	remote := cf.fs.Bool("remote", false, "reverse forward: the server listens, the local side serves (ssh -R)")
	cf.fs.BoolVar(remote, "R", false, "reverse forward: the server listens, the local side serves (ssh -R)")
	detach := cf.fs.Bool("detach", false, "run the forward in the background")
	socks := cf.fs.Bool("socks", false, "run a local SOCKS5 proxy through the server (ssh -D)")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) < 1 || len(pos) > 2 {
		return fmt.Errorf("usage: rsk forward <host> <[bind:]port[:host:port]> [--remote] | rsk forward <host> [addr] --socks")
	}
	var spec client.ForwardSpec
	socksAddr := ""
	if *socks {
		socksAddr = net.JoinHostPort(*bind, "1080")
		if len(pos) == 2 {
			if _, portErr := strconv.Atoi(pos[1]); portErr == nil {
				socksAddr = net.JoinHostPort(*bind, pos[1])
			} else {
				socksAddr = pos[1]
			}
		}
	} else {
		if len(pos) != 2 {
			return fmt.Errorf("usage: rsk forward <host> <[bind:]port[:host:port]> [--remote]")
		}
		parse := client.ParseForward
		if *remote {
			parse = client.ParseReverse
		}
		spec, err = parse(pos[1], *bind)
		if err != nil {
			return err
		}
	}
	if *detach && os.Getenv("RSK_FORWARD_CHILD") == "" {
		return spawnDetachedForward()
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	// Forwarding survives daemon restarts and dropped connections: reconnect
	// and re-open the listener until the user stops us.
	for {
		c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			fmt.Fprintf(os.Stderr, "rsk: connect failed: %v (retrying)\n", err)
			if !sleepCtx(ctx, 3*time.Second) {
				return nil
			}
			continue
		}
		if *socks {
			err = client.SOCKS(ctx, c, socksAddr)
		} else if spec.Reverse {
			fmt.Printf("forwarding %s:%d on %s -> local %s (ssh -R)\n", spec.BindHost, spec.BindPort, c.Host.Name, spec.Target())
			err = client.ReverseForward(ctx, c, spec)
		} else {
			fmt.Printf("forwarding %s -> %s (via %s)\n", spec.ListenAddress(), spec.Target(), c.Host.Name)
			err = client.Forward(ctx, c, spec)
		}
		c.Close()
		if ctx.Err() != nil {
			return nil
		}
		fmt.Fprintf(os.Stderr, "rsk: forward dropped: %v (reconnecting)\n", err)
		if !sleepCtx(ctx, 2*time.Second) {
			return nil
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// spawnDetachedForward re-runs the same command in the background with a log.
func spawnDetachedForward() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	dir := client.RuntimeDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	id := jobID()
	logPath := filepath.Join(dir, "forward-"+id+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd := exec.Command(exe, os.Args[1:]...)
	cmd.Env = append(os.Environ(), "RSK_FORWARD_CHILD=1")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = detachSysProcAttr()
	if err := cmd.Start(); err != nil {
		return err
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	fmt.Printf("forward   running in background\npid       %d\nlog       %s\n", pid, logPath)
	return nil
}

func cmdDown(args []string) error {
	cf := newCmdFlags("down")
	var hf hostFlags
	hf.bind(cf.fs)
	force := cf.fs.Bool("force", false, "kill running commands immediately")
	purge := cf.fs.Bool("purge", false, "also delete uploaded binaries and state")
	killSessions := cf.fs.Bool("kill-sessions", false, "also stop the session keepers")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: rsk down <host> [--session NAME | --kill-sessions | --purge]")
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	// Do not open a session here: `down --session X` must not create X first.
	c, err := newClient(ctx, cfg, &hf, pos[0], client.ConnectOptions{Quiet: hf.quiet})
	if err != nil {
		return err
	}
	name := c.Host.Name
	if hf.session != "" {
		if err := c.CloseSession(ctx, hf.session); err != nil {
			return err
		}
		c.Close()
		fmt.Printf("stopped session %s on %s\n", hf.session, name)
		return nil
	}
	kill := *killSessions || *purge
	if err := c.Shutdown(ctx, *force, kill); err != nil {
		return err
	}
	waitForDaemonGone(ctx, c)
	client.StopBroker(c.Host)
	if kill {
		// The daemon acknowledges before the keepers are gone: wait until the
		// session sockets really disappear so the report is truthful.
		waitForSessionsGone(ctx, c)
	}
	c.Close()
	fmt.Printf("stopped rskilld on %s\n", name)
	if kill {
		fmt.Printf("stopped all sessions\n")
	} else if c.Host.Inline {
		fmt.Printf("sessions ended with the daemon (inline mode)\n")
	} else {
		fmt.Printf("sessions keep running; `rsk status %s` lists them, `--kill-sessions` stops them\n", name)
	}
	if *purge {
		script := purgeScript()
		if _, err := c.SSH.Output(ctx, script); err != nil {
			fmt.Fprintf(os.Stderr, "rsk: purge failed: %v\n", err)
		} else {
			fmt.Printf("removed ~/.rsk on %s\n", name)
		}
		if cfg.Delete(name) {
			_ = cfg.Save()
		}
	}
	return nil
}

// waitForDaemonGone blocks until the daemon removes its socket, so the next
// command cannot be served by a daemon that is halfway through shutting down.
func waitForDaemonGone(ctx context.Context, c *client.Client) {
	socket := c.Host.SocketPath()
	script := fmt.Sprintf("test -S %s && echo alive || echo gone", client.Quote(socket))
	if c.Host.RemoteOS == "windows" {
		script = fmt.Sprintf(`cmd /c "if exist %q (echo alive) else (echo gone)"`, socket)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return
		}
		out, err := c.SSH.Output(ctx, script)
		if err == nil && strings.Contains(out, "gone") {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitForSessionsGone polls the remote session socket directory.
func waitForSessionsGone(ctx context.Context, c *client.Client) {
	script := `ls "$HOME/.rsk/run/sessions"/*.sock 2>/dev/null | wc -l`
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		out, err := c.SSH.Output(ctx, script)
		if err == nil && strings.TrimSpace(out) == "0" {
			return
		}
		select {
		case <-time.After(300 * time.Millisecond):
		case <-ctx.Done():
			return
		}
	}
}

// purgeScript stops leftover keepers and deletes the tool's remote state.
func purgeScript() string {
	return `set -e
[ -n "$HOME" ] || exit 1
d="$HOME/.rsk"
if [ -d "$d/run/sessions" ]; then
  for f in "$d"/run/sessions/*.json; do
    [ -f "$f" ] || continue
    pid=$(sed -n 's/.*"pid": *\([0-9]*\).*/\1/p' "$f" | head -1)
    if [ -n "$pid" ]; then kill -TERM "$pid" 2>/dev/null || true; fi
  done
fi
sleep 0.3
# the bracket keeps pkill from matching this script's own command line
pkill -f 'rskilld sessio[n] --name' 2>/dev/null || true
rm -rf "$d"`
}

func cmdDoctor(args []string) error {
	cf := newCmdFlags("doctor")
	var hf hostFlags
	hf.bind(cf.fs)
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: rsk doctor <host>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	host, err := resolveHost(cfg, pos[0], &hf)
	if err != nil {
		return err
	}
	fmt.Printf("host      %s -> %s\n", host.Name, host.Spec())
	ssh := client.NewSSH(host)
	ssh.Verbose = hf.verbose
	ssh.BatchAll = !hf.askPass
	start := time.Now()
	out, err := ssh.Output(ctx, "uname -sm; id -un; printf '%s\\n' \"$HOME\"")
	if err != nil {
		fmt.Printf("ssh       FAILED (%v)\n", err)
		fmt.Printf("hint      try `ssh %s` interactively once to accept the host key\n", host.Spec())
		return err
	}
	fmt.Printf("ssh       ok (%.0f ms)\n", float64(time.Since(start).Milliseconds()))
	fmt.Printf("remote    %s\n", strings.ReplaceAll(strings.TrimSpace(out), "\n", " "))

	verbose := hf.verbose
	hf.verbose = verbose
	opts := hf.connectOptions()
	opts.Force = true
	opts.Quiet = true
	c := client.NewClient(host, cfg)
	c.Verbose = verbose
	c.SSH.Verbose = verbose
	c.NoBroker = hf.noBroker
	c.UseBroker = hf.useBroker
	applyTransport(c, hf.sshImpl)
	c.SessionOverride = hf.session
	c.ShellOverride = hf.shell
	c.EnvOverride = envMap(hf.env)
	c.InlineOverride = hf.inline
	start = time.Now()
	if err := c.Connect(ctx, opts); err != nil {
		fmt.Printf("bootstrap FAILED (%v)\n", err)
		return err
	}
	defer c.Close()
	info := c.HostInfo()
	fmt.Printf("bootstrap ok (%.0f ms)\n", float64(time.Since(start).Milliseconds()))
	fmt.Printf("binary    %s\n", host.RemoteBin)
	fmt.Printf("daemon    rskilld %s api=%d os=%s/%s socket=%s\n", info.Version, info.API, info.OS, info.Arch, info.Socket)
	var sb strings.Builder
	res, err := c.Exec(ctx, daemon.ExecRequest{Session: c.SessionName(hf.session), Cmd: "echo rsk-doctor-ok && pwd"}, &sb)
	if err != nil {
		fmt.Printf("exec      FAILED (%v)\n", err)
		return err
	}
	fmt.Printf("exec      rc=%d cwd=%s output=%q\n", res.RC, res.Cwd, strings.TrimSpace(sb.String()))
	if !strings.Contains(sb.String(), "rsk-doctor-ok") {
		return fmt.Errorf("session did not echo the probe output")
	}
	cfg.Put(host)
	_ = cfg.Save()
	fmt.Printf("result    all checks passed\n")
	return nil
}

func cmdHosts(_ []string) error {
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	names := cfg.Names()
	if len(names) == 0 {
		fmt.Println("no hosts configured; run `rsk up <user@host>`")
		return nil
	}
	for _, name := range names {
		h, _ := cfg.Get(name)
		fmt.Printf("%-20s %s\n", name, h.Describe())
	}
	return nil
}

// applyTransport picks the ssh client: stored password (or RSK_PASSWORD)
// selects plink, otherwise the system OpenSSH keeps its keys/config/agent.
func applyTransport(c *client.Client, impl string) {
	if impl == "" {
		if !client.HasCredential(c.Host.Name) && os.Getenv("RSK_PASSWORD") == "" {
			return
		}
		impl = "plink"
	}
	c.SSH.Impl = impl
	if impl != "plink" {
		return
	}
	c.SSH.HostKey = c.Host.HostKey
	if pw, ok, err := client.LoadCredential(c.Host.Name); err == nil && ok {
		c.SSH.Password = pw
		return
	}
	c.SSH.Password = os.Getenv("RSK_PASSWORD")
}

// authHint makes the password-only case obvious instead of a bare ssh error.
func authHint(err error, host *client.Host) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "permission denied") ||
		strings.Contains(msg, "no more authentication methods") ||
		strings.Contains(msg, "host key verification failed") ||
		strings.Contains(msg, "no supported authentication") ||
		strings.Contains(msg, "access denied") {
		return fmt.Errorf("%w\nhint: for password-only hosts run `rsk login %s` (stores the password and pins the host key); add --ask-pass only when you are at the terminal", err, host.Name)
	}
	return err
}

// cmdLogin stores a DPAPI-encrypted password and verifies it with plink.
func cmdLogin(args []string) error {
	cf := newCmdFlags("login")
	var hf hostFlags
	hf.bind(cf.fs)
	fromStdin := cf.fs.Bool("password-stdin", false, "read the password from stdin instead of prompting")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: rsk login <host> [--password-stdin]")
	}
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	host, err := resolveHost(cfg, pos[0], &hf)
	if err != nil {
		return err
	}
	var pw string
	if *fromStdin {
		b, rerr := io.ReadAll(io.LimitReader(os.Stdin, 4096))
		if rerr != nil {
			return rerr
		}
		pw = strings.TrimRight(string(b), "\r\n")
	} else {
		if !client.TTY(os.Stdin) {
			return fmt.Errorf("no interactive terminal; pipe the password with --password-stdin")
		}
		fmt.Fprintf(os.Stderr, "password for %s: ", host.Spec())
		b, rerr := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		if rerr != nil {
			return rerr
		}
		pw = string(b)
	}
	if pw == "" {
		return fmt.Errorf("empty password")
	}
	if err := client.SaveCredential(host.Name, pw); err != nil {
		return err
	}

	ssh := client.NewSSH(host)
	ssh.Impl = "plink"
	ssh.Password = pw
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	fp := ""
	if fp, ferr := client.HostKeyFingerprint(host.Target, host.Port); ferr == nil {
		host.HostKey = fp
	} else if probed, perr := ssh.ProbeHostKey(ctx, 20*time.Second); perr == nil {
		fp = probed
		host.HostKey = fp
	}
	if fp != "" {
		fmt.Fprintf(os.Stderr, "host key %s\n", fp)
	}
	ssh.HostKey = host.HostKey
	out, err := ssh.Output(ctx, "echo rsk-auth-ok")
	if err != nil || !strings.Contains(out, "rsk-auth-ok") {
		_ = client.DeleteCredential(host.Name)
		if err != nil {
			return fmt.Errorf("password login failed: %w", err)
		}
		return fmt.Errorf("password login failed: unexpected output %q", out)
	}
	cfg.Put(host)
	if err := cfg.Save(); err != nil {
		return err
	}
	fmt.Printf("saved password for %s (DPAPI encrypted); plink is used automatically\n", host.Name)
	return nil
}

// cmdLogout forgets the stored password of a host.
func cmdLogout(args []string) error {
	cf := newCmdFlags("logout")
	var hf hostFlags
	hf.bind(cf.fs)
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: rsk logout <host>")
	}
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	host, err := resolveHost(cfg, pos[0], &hf)
	if err != nil {
		return err
	}
	if !client.HasCredential(host.Name) {
		fmt.Printf("no stored password for %s\n", host.Name)
		return nil
	}
	if err := client.DeleteCredential(host.Name); err != nil {
		return err
	}
	client.StopBroker(host)
	fmt.Printf("removed stored password for %s\n", host.Name)
	return nil
}

// cmdBroker runs the local connection multiplexer (hidden subcommand).
func cmdBroker(args []string) error {
	cf := newCmdFlags("__broker")
	var hf hostFlags
	hf.bind(cf.fs)
	socket := cf.fs.String("socket", "", "broker socket")
	hostName := cf.fs.String("host", "", "configured host name")
	if _, err := cf.parse(args); err != nil {
		return err
	}
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	host, ok := cfg.Get(*hostName)
	if !ok {
		return fmt.Errorf("unknown host %q", *hostName)
	}
	c := client.NewClient(host, cfg)
	c.SSH.Impl = hf.sshImpl
	if c.SSH.Impl == "" && client.HasCredential(host.Name) {
		c.SSH.Impl = "plink"
	}
	if pw, ok, _ := client.LoadCredential(host.Name); ok {
		c.SSH.Password = pw
	} else {
		c.SSH.Password = os.Getenv("RSK_PASSWORD")
	}
	c.SSH.HostKey = host.HostKey
	c.SSH.BatchAll = true
	c.Verbose = hf.verbose
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return c.ServeBroker(ctx, *socket)
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
