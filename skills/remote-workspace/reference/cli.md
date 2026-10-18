# rsk command reference

## Common flags

Accepted by every host command:

```
--session NAME     session to use (default "main")
--user NAME        ssh user
--port N           ssh port
--identity FILE    ssh private key
--ssh-arg ARG      extra ssh option, repeatable
--env K=V          session environment, repeatable
--shell PATH       shell for new sessions (default /bin/bash)
--no-profile       start sessions without user rc files
--verbose, -v      trace ssh and protocol activity
--quiet, -q        suppress progress output
```

## rsk up <host>

Bootstraps the host (detects the platform, uploads `rskilld`, starts the
daemon) and opens a session. Re-running is cheap: nothing is uploaded when the
cached build still matches.

```
--workspace DIR    cd into DIR and remember it for the session
--name ALIAS       store the host under ALIAS
--restart          restart the remote daemon (kills its sessions)
--force            re-upload the daemon binary
```

## rsk exec <host> [flags] -- <cmd...>

Runs a shell command inside the session. Exit code of `rsk` is the remote exit
code. `--` is optional but recommended so command flags are not parsed locally.

```
--timeout DUR      interrupt after DUR (default 120s, 0 disables)
--wait DUR         wait up to DUR for a busy session (default 30s, 0 fails fast)
--cwd DIR          cd for this command (persists in the session)
--env K=V          export for this command (persists in the session)
--json             print {rc, session, exec_id, cwd, duration_ms, timed_out, stdout}
--detach           start in the background, print job id, pid and log path
--stdin            feed local stdin to the command through a heredoc
--tty              run the command on a dedicated remote pty (sudo, top, editors;
                   unix ptys, and Windows via ConPTY)
```

Timeout semantics: SIGINT goes to the foreground job only. If the job refuses
to die, the daemon escalates to SIGTERM then SIGKILL, and reports
`unfinished: true` if the process still holds the terminal. The shell itself is
never killed by a timeout.

## rsk sh <host>

Attaches the session to your terminal (needs a TTY). `Ctrl-]` detaches,
`Ctrl-C` interrupts the foreground job, window resizes are forwarded.

```
--replay BYTES     scrollback to replay first (default 64KiB)
```

## rsk put / rsk get

```
rsk put <host> <local> [remote] [--recursive] [--overwrite=false]
rsk get <host> <remote> [local] [--recursive] [--overwrite=false]
```

Naming rules, identical in both directions:

* target is an existing directory (or ends with a separator): the source is
  created inside it under its own base name
* target does not exist: the target path becomes the copy, including for
  directories

Remote paths are resolved relative to the session working directory; `~` and
absolute paths work too. Transfers stream a tar archive: permissions,
symlinks, empty directories and mtimes survive.

## rsk sync

```
rsk sync <host> <local-dir> <remote-dir> [--exclude GLOB]... [--delete] [--dry-run] [--json]
```

Incremental one-way sync: the server returns a manifest (size + mtime), only
changed files are packed into a single streamed tar, and `--delete` removes
remote files that no longer exist locally. Repeating a sync is a no-op;
`--dry-run` reports what would change without touching the server.

## rsk logs <host>

```
--tail BYTES       scrollback bytes to print (default 64KiB)
--follow, -f       keep streaming (Ctrl-] to stop)
```

## rsk status <host>

Prints the daemon summary, then one line per session: name, state
(`idle`/`running`/`closed`), pid, cwd and the last command's exit code.
`--json` returns the raw daemon info.

## rsk forward <host> <spec>

```
rsk forward devbox 8080                     # local 8080 -> remote 8080
rsk forward devbox 8080:3000                # local 8080 -> remote localhost:3000
rsk forward devbox 8080:db.internal:5432    # explicit remote host
rsk forward devbox 0.0.0.0:8080:localhost:3000 --bind 127.0.0.1
rsk forward devbox 8080:localhost:3000 --detach     # run in the background
rsk forward devbox --socks 1080                     # local SOCKS5 proxy (ssh -D)
rsk forward devbox 8080:localhost:3000 --remote     # ssh -R: the server listens
```

Each local connection opens one tunnel through the daemon; the remote side
dials the target from the server's network position.

With `--remote` the direction flips: the daemon listens on `<bind>:<port>` on
the server (default bind 127.0.0.1) and tunnels every accepted connection back
here, where the target is dialled on the local network. Useful for webhooks or
to let a remote host reach a service that only exists locally.

Forwards reconnect automatically after a dropped connection or a daemon
restart. `--detach` prints a pid and a log file.

## rsk ports / rsk ps / rsk kill

```
rsk ports <host> [--json]      # listening ports with a ready-to-copy forward command
rsk ps <host>                  # jobs started with `exec --detach`
rsk kill <host> <job-id>       # terminate a job and its children
```

`rsk get` streams the archive straight into the extractor, so memory use does
not grow with the download size.

## rsk fanout

```
rsk fanout --hosts dev1,dev2 -- <cmd>     # or --all
rsk fanout --all --concurrency 8 --timeout 60s --json -- <cmd>
```

Runs one command on several configured hosts in parallel, prints a `== host
[ok|failed] rc=... ==` block per host, and exits 1 if any host failed. Each
host uses its own default session and stored authentication.

## Connection reuse (experimental)

`--broker` (or `RSK_BROKER=1`) keeps one local SSH connection per host and
multiplexes short-lived invocations over it: measured round trips drop from
~1.9s to ~0.13s. It is opt-in because rapid daemon/session restarts can still
race the multiplexer. Without the flag every invocation opens its own
connection.

## rsk down <host>

Stops the daemon. Sessions are keepers with their own processes, so they keep
running (and so do their background jobs) until you ask for them to stop.

```
--session NAME    stop just that session, leave the daemon running
--kill-sessions   stop every session as well
--force           kill running commands immediately (default waits up to 30s)
--purge           stop sessions and delete ~/.rsk (bin, run, state) and the host entry
```

`--kill-sessions` also forgets the environment snapshot of those sessions, so a
later command starts clean. A session that died by accident (daemon crash,
keeper killed) keeps its snapshot and is recreated with the same variables.

## rsk up <host> --inline

Keeps sessions inside the daemon again (they end when it stops). Useful on hosts
where detached processes are restricted; `rsk up <host>` switches back to
per-session keepers.

## rsk hosts / rsk doctor / rsk version

* `rsk hosts` lists known hosts with platform, daemon version and last seen.
* `rsk doctor <host>` runs ssh, upload, daemon and exec checks with timings.
* `rsk version` prints client version and protocol API version.

## rsk login / rsk logout

```
rsk login <host> [--password-stdin]   # store a DPAPI-encrypted password, verify it, pin the host key
rsk logout <host>                     # forget it
```

Password-only hosts (no keys, no sudo) work through PuTTY's `plink`/`pscp`
(must be installed): `rsk` selects plink automatically once a password is
stored. `--password-stdin` reads the password from stdin for scripted setup;
otherwise it is prompted with echo off. `--ask-pass` lets the system ssh prompt
on a terminal instead (never used automatically, so agents never hang on a
hidden prompt). `--ssh-impl ssh|plink` forces a transport.
