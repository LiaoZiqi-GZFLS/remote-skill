---
name: remote-workspace
description: Run commands, build, test, deploy, transfer files and open ports on a remote machine over SSH in a persistent workspace, like VS Code Remote-SSH. Use whenever the user asks for work on a remote server, dev box, GPU box, staging host, or says "over ssh", "on the server", "remotely", "deploy to", "run that on <host>".
---

# Remote workspace (rsk)

`rsk` keeps a real login shell alive on a remote host and lets you drive it
from here. State persists between calls: `cd`, `export`, shell functions,
background jobs and TTY-ish tools all keep working, exactly as if you were
typing in a terminal on that machine.

## Mental model

- One **host** = one ssh destination plus its uploaded daemon (`rskd`).
- One **session** = one persistent shell on that host, owned by its own keeper
  process. Default name `main`.
- Every `rsk` invocation opens a short ssh connection to the daemon, which
  routes session work to that session's keeper.
- Sessions outlive almost everything: dropped ssh connections, a restarted or
  even `kill -9`ed daemon, client upgrades, and `rsk down`. A running command
  keeps running and its output stays in the scrollback.
- Commands run inside a pty, so output is merged and terminal-like: no
  separation between stdout and stderr, ANSI colours may appear.

## First steps

Check that the tool is available:

```bash
rsk version || echo "install remote-skill first (see the repo README)"
```

Connect a host (uploads the daemon the first time, then caches it):

```bash
rsk up devbox --workspace ~/project            # host from ~/.ssh/config
rsk up 10.0.0.5 --user ubuntu --identity ~/.ssh/id_ed25519 --name gpu
rsk doctor devbox                              # diagnose when something fails
```

After the first `up`, use the short name everywhere:

```bash
rsk exec devbox -- npm test
rsk exec devbox -- "cd services/api && go build ./..."
```

## Core commands

| Task | Command |
|---|---|
| Run a command in the session | `rsk exec <host> [flags] -- <cmd>` |
| Run in the background, keep a log | `rsk exec <host> --detach -- <cmd>` |
| Read recent terminal output | `rsk logs <host> --tail 20000` |
| Follow output live | `rsk logs <host> --follow` |
| Upload | `rsk put <host> <local> [remote] [--recursive]` |
| Download | `rsk get <host> <remote> [local] [--recursive]` |
| List a directory | `rsk ls <host> [path] [-l]` |
| Find listening ports | `rsk ports <host>` |
| List / stop background jobs | `rsk ps <host>`, `rsk kill <host> <job-id>` |
| Sessions, daemon, uptime | `rsk status <host> [--json]` |
| Expose a remote port locally | `rsk forward <host> 8080:localhost:3000` |
| Let the server reach a local service | `rsk forward <host> 8080:localhost:3000 --remote` |
| Parallel work in another shell | `rsk exec <host> --session build -- <cmd>` |
| Stop the daemon, keep the sessions | `rsk down <host>` |
| Stop a session too | `rsk down <host> --kill-sessions` (or `--session NAME`) |
| Wipe everything remote | `rsk down <host> --purge` |

## Rules that keep you out of trouble

1. **Always bound long commands.** `rsk exec` defaults to `--timeout 120s`.
   A timed out command is interrupted on the server (SIGINT, then TERM/KILL)
   and the result reports `timed_out: true`; the session survives.
   Use `--timeout 0` only when you really want to wait forever.
2. **Servers and watchers go in the background.** Use `--detach`, then read the
   printed log path with `rsk logs` or `rsk exec <host> -- tail -n 50 <log>`.
   A foreground server blocks the session for everything else.
3. **One session runs one command at a time.** If a command is still running,
   new ones wait up to `--wait 30s` and then fail with `busy`. Use
   `rsk exec --session other` for genuinely parallel work.
4. **Use `--json` when you need to parse.** It returns
   `{rc, session, exec_id, cwd, duration_ms, timed_out, stdout}` and still
   exits with the remote exit code.
5. **Quote carefully.** `rsk exec devbox -- "cd app && make"` sends the string
   to the remote shell; avoid double-expanding local `$VAR` unless intended.
6. **Check before assuming.** `rsk status <host> --json` shows sessions, cwd,
   busy state and the last command's exit code.
7. **Transfers keep permissions.** `rsk put --recursive` preserves the
   executable bit and symlinks; `--overwrite=false` skips existing files.
8. **Interrupting is safe.** Ctrl-C (or a timeout) interrupts the foreground
   job only; the session and its environment stay alive.
9. **Do not restart sessions to fix a daemon.** If the daemon dies (crash,
   upgrade, `rsk down`), just run the next command: the same shell, cwd and
   running jobs are still there. Only `--kill-sessions`/`--purge` end sessions,
   and a session that was stopped on purpose comes back without its old
   variables.
10. **Session flags are per call.** `--session`, `--env`, `--shell`, `--inline`
    affect only the current invocation; `rsk up` is what stores them as the
    host's defaults.
11. **Windows remotes work as virtual sessions**: `exec`, transfers, logs and
    forwarding work; `rsk sh` does not (no pty), and each command costs a
    PowerShell start, so pass `--no-profile` when speed matters.
12. **Password-only hosts**: never ask the user to paste a password into chat.
    Run `rsk login <host>` yourself if a terminal is available, or tell the
    user to run it once. After that `rsk` uses plink automatically; without a
    stored password it fails fast with a hint instead of prompting.

## Recipes

Build and test a checkout that lives on the server:

```bash
rsk exec devbox -- "cd ~/project && git pull --ff-only && make test"
```

Upload local edits, build, fetch the artifact back:

```bash
rsk put devbox ./src /home/me/project/src --recursive
rsk exec devbox -- "cd /home/me/project && make release"
rsk get devbox /home/me/project/dist/app ./dist/app
```

Run a dev server and reach it from here:

```bash
rsk exec devbox --detach -- "cd ~/project && npm run dev -- --host 127.0.0.1 --port 3000"
rsk forward devbox 3000:127.0.0.1:3000      # leave running in its own terminal
```

Investigate a stuck process:

```bash
rsk exec devbox -- "ps -ef | grep -c node"
rsk exec devbox -- "tail -n 100 /var/log/app.log"
rsk logs devbox --tail 8000                 # what the session itself printed
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Host key verification failed` | run `ssh <user@host>` once here to accept the key |
| `session is busy` | something is still running: `rsk status <host>`, `rsk logs <host>`, or use another session |
| `timed_out: true` | the command exceeded `--timeout`; raise it or use `--detach` |
| `unfinished: true` | the command ignored SIGINT/TERM/KILL; inspect with `rsk exec <host> -- ps -ef` |
| `daemon is running a different build` | automatic: the client restarts it, or run `rsk up <host> --restart` |
| output looks truncated | increase `--tail`, or read files directly with `rsk exec <host> -- cat <file>` |
| ssh works but `rsk` fails | `rsk doctor <host>` prints each stage (ssh, upload, daemon, session, exec) |

`rsk up --restart` restarts the daemon only; sessions keep running. To return to
the old "sessions die with the daemon" behaviour use `rsk up <host> --inline`
(and `rsk up <host>` again to switch back).

## Reference

Full flag list: `reference/cli.md` next to this file.
