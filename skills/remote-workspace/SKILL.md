---
name: remote-workspace
description: Run commands, build, test, deploy, transfer or sync files, and open ports on a remote machine over SSH in a persistent workspace, like VS Code Remote-SSH. Use whenever the user asks for work on a remote server, dev box, GPU box, staging host, or says "over ssh", "on the server", "remotely", "deploy to", or "run that on a host".
---

# Remote workspace (rsk)

`rsk` keeps a real login shell alive on a remote host and lets you drive it
from here. State persists between calls: `cd`, `export`, shell functions,
background jobs and TTY-ish tools keep working, as if you were typing in a
terminal on that machine.

## Install

The CLI and this skill live in
<https://github.com/LiaoZiqi-GZFLS/remote-skill>.

```bash
# Windows (PowerShell): builds, installs rsk on PATH and this skill
pwsh install.ps1 -InstallSkill

# Linux / macOS
./install.sh --install-skill
```

Verify with `rsk version` (expect `rsk <version> (api 1)`). The client embeds
the remote daemon for linux/amd64, linux/arm64, darwin and windows, so target
servers do not need a Go toolchain.

Server requirements: OpenSSH plus `bash` on Linux/macOS/FreeBSD, or OpenSSH
Server on Windows. **No sudo is required** — everything lives under `~/.rsk`.
On a Windows client, password-only hosts additionally need PuTTY
(`plink.exe`/`pscp.exe`); saved passwords are supported on Windows only.

## Mental model

- One **host** = one ssh destination plus its uploaded daemon (`rskd`).
- One **session** = one persistent shell on that host, owned by its own keeper
  process. Default name `main`.
- Each `rsk` call opens a short ssh connection to the daemon, which routes
  session work to that session's keeper. `--broker` (experimental, opt-in)
  reuses one local ssh connection and is much faster for many short calls.
- In the default keeper mode, sessions outlive almost everything: dropped ssh
  connections, a restarted or even `kill -9`ed daemon, client upgrades, and
  `rsk down`. A running command keeps running and its output stays in the
  scrollback.
- After a **machine reboot** the process is gone, but the next command restores
  the working directory, exported environment, shell history and recent
  scrollback from `~/.rsk/state/`.
- Commands run inside a pty, so output is merged and terminal-like: no
  separation between stdout and stderr, ANSI colours may appear.

## First steps

```bash
rsk version                       # is the CLI installed?
rsk up devbox --workspace ~/project            # host from ~/.ssh/config
rsk up 10.0.0.5 --user ubuntu --identity ~/.ssh/id_ed25519 --name gpu
rsk doctor devbox                              # diagnose when something fails
```

After the first `up`, use the short name everywhere:

```bash
rsk exec devbox -- npm test
rsk exec devbox -- "cd services/api && go build ./..."
```

Password-only host (no keys, no sudo, no `authorized_keys`):

```bash
rsk login devbox            # asks once, stores a DPAPI-encrypted password
rsk login devbox --password-stdin < secret.txt   # scripted setup
```

## Core commands

| Task | Command |
|---|---|
| Run a command in the session | `rsk exec <host> [flags] -- <cmd>` |
| Interactive single command (sudo/top) | `rsk exec <host> --tty -- <cmd>` |
| Attach to the session in a terminal | `rsk sh <host>` (unix remotes) |
| Read a bounded text window | `rsk read <host> <path> [--offset N] [--limit N] [--json]` |
| Atomically write from stdin | `rsk write <host> <path> --stdin [--overwrite]` |
| Apply exact replacements/add/delete | `rsk patch <host> --stdin --json` |
| Run in the background, keep a log | `rsk exec <host> --detach -- <cmd>` |
| Read recent terminal output | `rsk logs <host> --tail 20000` |
| Follow output live | `rsk logs <host> --follow` |
| Upload | `rsk put <host> <local> [remote] [--recursive]` |
| Download | `rsk get <host> <remote> [local] [--recursive]` |
| Incremental one-way sync | `rsk sync <host> <local> <remote> [--exclude PAT] [--delete] [--dry-run]` |
| List a directory | `rsk ls <host> [path] [-l] [--json]` |
| Find listening ports | `rsk ports <host>` |
| List / stop background jobs | `rsk ps <host>`, `rsk kill <host> <job-id>` |
| Run on many hosts | `rsk fanout --hosts a,b -- <cmd>` (or `--all`) |
| Sessions, daemon, uptime | `rsk status <host> [--json]` |
| Expose a remote port locally | `rsk forward <host> 8080:localhost:3000 [--detach]` |
| Let the server reach a local service | `rsk forward <host> 8080:localhost:3000 --remote` |
| SOCKS5 proxy through the server | `rsk forward <host> --socks 1080` |
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
   printed log with `rsk exec <host> -- tail -n 50 <log>`. `rsk logs` reads the
   session terminal, not the detached job's separate log file. A foreground
   server blocks the session for everything else.
3. **One session runs one command at a time.** If a command is still running,
   new ones wait up to `--wait 30s` and then fail with `busy`. Use
   `rsk exec --session other` for genuinely parallel work.
4. **Use `--json` when you need to parse.** It returns
   `{rc, session, exec_id, cwd, duration_ms, timed_out, stdout}` and still
   exits with the remote exit code.
5. **Use file primitives for text.** Read with `rsk read`, not `exec -- cat`;
   reads are directly bounded and do not enter the session scrollback. Use
   `rsk get` for binary files.
6. **Edit exactly.** `rsk write --stdin` is for complete content and refuses to
   overwrite unless `--overwrite` is passed. Use `rsk patch --stdin` for exact
   replace/add/delete operations; every match is validated before any file is
   committed.
7. **Quote carefully.** `rsk exec devbox -- "cd app && make"` sends the string
   to the remote shell; avoid double-expanding local `$VAR` unless intended.
8. **Never put secrets in commands.** Command text, output and environment are
   persisted in the session state (`~/.rsk/state`, shell history, scrollback).
   Prefer an uploaded file or an interactive credential prompt; `--env` values
   are exported into the session and can also be snapshotted.
9. **Prefer `sync` for trees, `put` for one-off files.** `sync` compares
   size+mtime on the server, uploads only changes, and is idempotent;
   `--delete` mirrors deletions, `--dry-run` previews.
10. **Interrupting is safe.** Ctrl-C (or a timeout) interrupts the foreground
   job only; the session and its environment stay alive. Use `--tty` for tools
   that need a real terminal (sudo password prompts, top, editors).
11. **Do not restart sessions to fix a daemon.** If the daemon dies (crash,
   upgrade, `rsk down`), just run the next command: the same shell, cwd and
   running jobs are still there. Only explicit `--session NAME`,
   `--kill-sessions` or `--purge` ends sessions, and those forget the saved
   environment too.
12. **Check before assuming.** `rsk status <host> --json` shows sessions, cwd,
    busy state and the last command's exit code.
13. **Know which flags stick.** `--session` selects the session for this call;
    `--cwd` and `--env` change that persistent shell and therefore carry into
    later commands. `--shell` and `--no-profile` shape new sessions, while
    `--inline` takes effect when a daemon starts. Only `rsk up` stores them as
    host defaults.
14. **Password-only hosts**: never ask the user to paste a password into chat.
    Run `rsk login <host>` yourself if a terminal is available, or tell the user
    to run it once. After that plink is used automatically; without a stored
    password the client fails fast with a hint instead of prompting.
15. **Windows remotes**: `exec`, file primitives, transfers, logs, sync and
    forwarding work; each plain command starts one PowerShell process (pass
    `--no-profile` for speed). `rsk sh`, `exec --detach`, `ps` and `kill` are
    not supported yet, but `rsk exec --tty` works through ConPTY.

## Recipes

Build and test a checkout that lives on the server:

```bash
rsk exec devbox -- "cd ~/project && git pull --ff-only && make test"
```

Inspect and patch a remote source file without transferring the whole file:

```bash
rsk read devbox src/config.go --offset 80 --limit 60 --json
cat <<'JSON' | rsk patch devbox --stdin
{"operations":[{"op":"replace","path":"src/config.go","old":"oldValue","new":"newValue","count":1}]}
JSON
```

Sync local edits, build, fetch the artifact back:

```bash
rsk sync devbox ./project /home/me/project --exclude .git --exclude node_modules
rsk exec devbox -- "cd /home/me/project && make release"
rsk get devbox /home/me/project/dist/app ./dist/app
```

Run a dev server and reach it from here:

```bash
rsk exec devbox --detach -- "cd ~/project && npm run dev -- --host 127.0.0.1 --port 3000"
rsk forward devbox 3000:127.0.0.1:3000 --detach   # or run it in its own terminal
rsk ports devbox                                   # what else is listening?
```

Browse a private network through the server:

```bash
rsk forward devbox --socks 1080 --detach
# then point a browser/CLI at socks5://127.0.0.1:1080
```

Run something that needs a terminal:

```bash
rsk exec devbox --tty -- sudo apt-get update
```

Fan out and collect results:

```bash
rsk fanout --all --json -- "git -C ~/project rev-parse --short HEAD"
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
| `Permission denied (publickey,password)` | `rsk login <host>` (password-only hosts) |
| `session is busy` | something is still running: `rsk status <host>`, `rsk logs <host>`, or use another session |
| `timed_out: true` | the command exceeded `--timeout`; raise it or use `--detach` |
| `unfinished: true` | the command ignored SIGINT/TERM/KILL; inspect from another session with `rsk exec <host> --session diag -- ps -ef` |
| `read` reports `truncated: true` | continue from `next_offset`, raise `--limit`/`--max-bytes`, or download with `get` |
| `write` refuses to replace | pass `--overwrite` only after confirming the target |
| `patch` matched the wrong number of times | make `old` more specific; validation leaves files unchanged |
| `daemon is running a different build` | automatic: the client restarts it, or run `rsk up <host> --restart` |
| output looks truncated | increase `--tail`, or read files directly with `rsk exec <host> -- cat <file>` |
| sync keeps re-uploading | compare clocks/mtimes, or use `rsk put` for that tree |
| ssh works but `rsk` fails | `rsk doctor <host>` prints each stage (ssh, upload, daemon, session, exec) |
| Windows `rsk sh` unsupported | use `rsk exec --tty` for interactive commands on Windows |

In the default keeper mode, `rsk up <host> --restart` restarts the daemon while
sessions keep running. To opt into inline sessions (they die with the daemon),
use `rsk up <host> --inline`; switch back and restart with
`rsk up <host> --restart`.

## Reference

- Full flag list: `reference/cli.md` next to this file.
- Source and releases: <https://github.com/LiaoZiqi-GZFLS/remote-skill>.
