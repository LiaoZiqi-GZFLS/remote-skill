# End to end test for rsk against a real ssh server.
#
#   pwsh test/e2e.ps1 -Target 172.26.215.11 -User lazybones -Key ~/.ssh/id_test
#
# The target must accept key based ssh and must be a Linux or macOS host with
# bash. The script never touches the daemon of another user: it stops and
# removes its own state at the end unless -Keep is passed.
param(
    [Parameter(Mandatory = $true)][string]$Target,
    [string]$User = $env:USERNAME,
    [int]$Port = 22,
    [string]$Key = "$env:USERPROFILE\.ssh\rsk_e2e",
    [string]$Rsk = "",
    [switch]$Keep,
    [switch]$Trace
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
if (-not $Rsk) { $Rsk = Join-Path $repo "dist\rsk.exe" }
if (-not (Test-Path $Rsk)) { throw "rsk binary not found: $Rsk (build with: go build -o dist/rsk.exe ./cmd/rsk)" }

$tmp = Join-Path $repo ".tmp\e2e"
Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force $tmp | Out-Null
$knownHosts = Join-Path $repo ".tmp\known_hosts"
$sshArgs = @(
    "--user", $User, "--port", "$Port", "--identity", $Key,
    "--ssh-arg", "-o", "--ssh-arg", "IdentitiesOnly=yes",
    "--ssh-arg", "-o", "--ssh-arg", "StrictHostKeyChecking=accept-new",
    "--ssh-arg", "-o", "--ssh-arg", "UserKnownHostsFile=$knownHosts"
)
if ($Trace) { $sshArgs += @("--verbose") }

$script:passed = 0
$script:failed = 0

function Invoke-Rsk {
    param([string[]]$Arguments, [switch]$AllowFailure)
    $all = $Arguments
    $output = & $Rsk @all 2>&1 | Out-String
    $code = $LASTEXITCODE
    if (-not $AllowFailure -and $code -ne 0) {
        throw "rsk $($Arguments -join ' ') failed (exit $code):`n$output"
    }
    return @{ Output = $output; Code = $code }
}

function Check {
    param([string]$Name, [scriptblock]$Body)
    try {
        & $Body
        Write-Host ("  ok   " + $Name) -ForegroundColor Green
        $script:passed++
    } catch {
        Write-Host ("  FAIL " + $Name + ": " + $_.Exception.Message) -ForegroundColor Red
        $script:failed++
    }
}

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
}

Write-Host "== e2e against ${User}@${Target}:${Port} ==" -ForegroundColor Cyan

# Sessions outlive daemons and upgrades, so start from a clean slate.
Invoke-Rsk (@("down", $Target, "--kill-sessions", "--force") + $sshArgs) -AllowFailure | Out-Null

# ---------------------------------------------------------------- bootstrap

Check "doctor bootstraps and runs a command" {
    $r = Invoke-Rsk (@("doctor", $Target) + $sshArgs)
    Assert-True ($r.Output -match "all checks passed") "doctor reported: $($r.Output)"
}

Check "up opens a session in the workspace" {
    $work = (Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "mktemp -d"))).Output.Trim()
    $script:workspace = $work
    $r = Invoke-Rsk (@("up", $Target, "--workspace", $work) + $sshArgs)
    Assert-True ($r.Output -match "session\s+main") "up output: $($r.Output)"
    $pwd = (Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "pwd"))).Output.Trim()
    Assert-True ($pwd -eq $work) "session cwd is $pwd, want $work"
}

Check "exec reports rc, session, cwd and duration" {
    $r = Invoke-Rsk (@("exec", $Target, "--json") + $sshArgs + @("--", "echo json-probe"))
    $json = $r.Output | ConvertFrom-Json
    Assert-True ($json.rc -eq 0) "rc=$($json.rc)"
    Assert-True ($json.session -eq "main") "session=$($json.session)"
    Assert-True ($json.cwd -ne "") "cwd empty"
    Assert-True ($json.duration_ms -ge 0) "duration missing"
    Assert-True ($json.stdout -match "json-probe") "stdout=$($json.stdout)"
}

Check "exit codes propagate" {
    $r = Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "false")) -AllowFailure
    Assert-True ($r.Code -eq 1) "false gave exit $($r.Code)"
    $r = Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "(exit 42)")) -AllowFailure
    Assert-True ($r.Code -eq 42) "subshell exit gave $($r.Code)"
    # exiting the shell itself still reports the shell's exit status and the
    # session is recreated for the next command
    $r = Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "exit 7")) -AllowFailure
    Assert-True ($r.Code -eq 7) "shell exit gave $($r.Code)"
    $r = Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "echo shell-recovered"))
    Assert-True ($r.Output -match "shell-recovered") "session did not recover: $($r.Output)"
}

Check "state persists across separate client invocations" {
    Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "cd /tmp && export RSK_E2E=persisted")) | Out-Null
    $r = Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", 'pwd; echo $RSK_E2E'))
    Assert-True ($r.Output -match "persisted") "env did not persist: $($r.Output)"
    Assert-True ($r.Output -match "(?m)^/tmp\s*$") "cwd did not persist: $($r.Output)"
}

Check "timeout interrupts and the session survives" {
    $start = Get-Date
    $r = Invoke-Rsk (@("exec", $Target, "--timeout", "2s", "--json") + $sshArgs + @("--", "sleep 30; echo SHOULD-NOT-RUN")) -AllowFailure
    Assert-True ($r.Code -ne 0) "sleep 30 should time out"
    $json = $r.Output | ConvertFrom-Json
    Assert-True ($json.timed_out) "timed_out flag missing: $($r.Output)"
    Assert-True ($json.stdout -notmatch "SHOULD-NOT-RUN") "command ran to completion: $($r.Output)"
    $elapsed = (Get-Date) - $start
    Assert-True ($elapsed.TotalSeconds -lt 15) "interrupt took $($elapsed.TotalSeconds)s"
    $r = Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "echo alive"))
    Assert-True ($r.Output -match "alive") "session died after timeout: $($r.Output)"
}

Check "syntax errors do not wedge the session" {
    Invoke-Rsk (@("exec", $Target, "--timeout", "8s") + $sshArgs + @("--", "if true; then")) -AllowFailure | Out-Null
    $r = Invoke-Rsk (@("exec", $Target, "--timeout", "15s") + $sshArgs + @("--", "echo after-syntax-error"))
    Assert-True ($r.Output -match "after-syntax-error") "session wedged: $($r.Output)"
}

Check "a second session is independent" {
    Invoke-Rsk (@("exec", $Target, "--session", "side") + $sshArgs + @("--", "cd / && echo side-ok")) | Out-Null
    $r = Invoke-Rsk (@("status", $Target, "--json") + $sshArgs)
    $json = $r.Output | ConvertFrom-Json
    $names = ($json.sessions | ForEach-Object { $_.name }) -join ","
    Assert-True ($names -match "main" -and $names -match "side") "sessions: $names"
    Invoke-Rsk (@("exec", $Target, "--session", "side") + $sshArgs + @("--", "pwd")) | Out-Null
}

# ---------------------------------------------------------------- transfers

Check "recursive upload and download round trip" {
    $src = Join-Path $tmp "src"
    New-Item -ItemType Directory -Force (Join-Path $src "nested") | Out-Null
    "hello rsk" | Set-Content (Join-Path $src "a.txt") -NoNewline
    ("x" * 300000) | Set-Content (Join-Path $src "nested\big.txt") -NoNewline
    "#!/bin/sh`necho script-ok" | Set-Content (Join-Path $src "run.sh") -NoNewline
    $remote = "$($script:workspace)/uploaded"
    $r = Invoke-Rsk (@("put", $Target, $src, $remote, "--recursive") + $sshArgs)
    Assert-True ($r.Output -match "uploaded") "put output: $($r.Output)"
    $r = Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "sh $remote/run.sh; wc -c < $remote/nested/big.txt"))
    Assert-True ($r.Output -match "script-ok") "exec bit not preserved: $($r.Output)"
    Assert-True ($r.Output -match "300000") "big file truncated: $($r.Output)"
    # downloading onto a new path copies the directory under that name
    $dest = Join-Path $tmp "download"
    Invoke-Rsk (@("get", $Target, $remote, $dest, "--recursive") + $sshArgs) | Out-Null
    $downloaded = $dest
    $a = (Get-FileHash (Join-Path $src "a.txt") -Algorithm SHA256).Hash
    $b = (Get-FileHash (Join-Path $downloaded "a.txt") -Algorithm SHA256).Hash
    Assert-True ($a -eq $b) "a.txt differs"
    $a = (Get-FileHash (Join-Path $src "nested\big.txt") -Algorithm SHA256).Hash
    $b = (Get-FileHash (Join-Path $downloaded "nested\big.txt") -Algorithm SHA256).Hash
    Assert-True ($a -eq $b) "big.txt differs"

    # downloading into an existing directory keeps the remote base name
    $into = Join-Path $tmp "into"
    New-Item -ItemType Directory -Force $into | Out-Null
    Invoke-Rsk (@("get", $Target, $remote, $into, "--recursive") + $sshArgs) | Out-Null
    $b = (Get-FileHash (Join-Path $into "uploaded\a.txt") -Algorithm SHA256).Hash
    $a = (Get-FileHash (Join-Path $src "a.txt") -Algorithm SHA256).Hash
    Assert-True ($a -eq $b) "nested download differs"
}

Check "single file put to an explicit path" {
    $f = Join-Path $tmp "one.txt"
    "single file body" | Set-Content $f -NoNewline
    $remoteFile = "$($script:workspace)/renamed.txt"
    Invoke-Rsk (@("put", $Target, $f, $remoteFile) + $sshArgs) | Out-Null
    $r = Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "cat $remoteFile"))
    Assert-True ($r.Output -match "single file body") "content: $($r.Output)"
}

Check "ls lists the remote directory" {
    $r = Invoke-Rsk (@("ls", $Target, $script:workspace, "--long") + $sshArgs)
    Assert-True ($r.Output -match "uploaded/") "ls output: $($r.Output)"
}

# ---------------------------------------------------------------- background

Check "detached jobs keep running and log to a file" {
    $r = Invoke-Rsk (@("exec", $Target, "--detach") + $sshArgs + @("--", "sleep 1; echo detached-finished"))
    Assert-True ($r.Output -match "RSK_LOG|log\s+") "detach output: $($r.Output)"
    $log = [regex]::Match($r.Output, "log\s+(\S+)").Groups[1].Value
    Assert-True ($log -ne "") "no log path in: $($r.Output)"
    Start-Sleep -Seconds 3
    $r = Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "cat $log"))
    Assert-True ($r.Output -match "detached-finished") "job log: $($r.Output)"
}

Check "logs shows recent session output" {
    Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "echo scrollback-marker")) | Out-Null
    $r = Invoke-Rsk (@("logs", $Target, "--tail", "20000") + $sshArgs)
    Assert-True ($r.Output -match "scrollback-marker") "logs output: $($r.Output)"
}

Check "relative workspace paths resolve on the remote" {
    # (a literal ~ cannot travel through a PowerShell argument list: it is
    # expanded locally before rsk sees it)
    $rel = "rsk-relative-ws"
    Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "mkdir -p ./$rel")) | Out-Null
    $r = Invoke-Rsk (@("up", $Target, "--workspace", $rel) + $sshArgs)
    Assert-True ($r.Output -match "cwd=\S*$rel") "workspace output: $($r.Output)"
}

# ---------------------------------------------------------------- forwarding

Check "port forwarding reaches a remote service" {
    $servePort = 18080
    Invoke-Rsk (@("exec", $Target, "--detach") + $sshArgs + @("--", "cd $($script:workspace) && python3 -m http.server $servePort --bind 127.0.0.1")) | Out-Null
    Start-Sleep -Seconds 2
    $job = Start-Process -FilePath $Rsk -ArgumentList (@("forward", $Target, "18081:127.0.0.1:$servePort") + $sshArgs) -PassThru -WindowStyle Hidden
    try {
        Start-Sleep -Seconds 3
        $ok = $false
        for ($i = 0; $i -lt 15; $i++) {
            try {
                $resp = Invoke-WebRequest -Uri "http://127.0.0.1:18081/" -UseBasicParsing -TimeoutSec 5
                if ($resp.StatusCode -eq 200 -and $resp.Content -match "Directory listing") { $ok = $true; break }
            } catch { Start-Sleep -Seconds 1 }
        }
        Assert-True $ok "forwarded request did not succeed"
    } finally {
        Stop-Process -Id $job.Id -Force -ErrorAction SilentlyContinue
    }
}

Check "reverse forward reaches a local service" {
    $localPort = 19091
    $remotePort = 18091
    $srv = Start-Process -FilePath "pwsh" -ArgumentList @(
        "-NoProfile", "-File", (Join-Path $repo "test\localsrv.ps1"),
        "-Port", "$localPort", "-Requests", "3"
    ) -PassThru -WindowStyle Hidden
    Start-Sleep -Seconds 2
    $fwd = Start-Process -FilePath $Rsk -ArgumentList (@(
        "forward", $Target, "${remotePort}:127.0.0.1:${localPort}", "--remote"
    ) + $sshArgs) -PassThru -WindowStyle Hidden
    try {
        Start-Sleep -Seconds 5
        $r = Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "curl -s --max-time 10 http://127.0.0.1:$remotePort/"))
        Assert-True ($r.Output -match "local-service-ok") "reverse forward output: $($r.Output)"
    } finally {
        Stop-Process -Id $fwd.Id -Force -ErrorAction SilentlyContinue
        Stop-Process -Id $srv.Id -Force -ErrorAction SilentlyContinue
    }
}

# ---------------------------------------------------------------- teardown

Check "session state survives a daemon restart" {
    # a dedicated session keeps `up`'s workspace handling out of the way
    Invoke-Rsk (@("exec", $Target, "--session", "persist") + $sshArgs + @("--", "export RSK_PERSIST=kept && cd /var && echo set")) | Out-Null
    Invoke-Rsk (@("up", $Target, "--restart") + $sshArgs) | Out-Null
    $r = Invoke-Rsk (@("exec", $Target, "--session", "persist") + $sshArgs + @("--", 'pwd; echo $RSK_PERSIST'))
    Assert-True ($r.Output -match "kept") "env lost after daemon restart: $($r.Output)"
    Assert-True ($r.Output -match "(?m)^/var\s*$") "cwd lost after daemon restart: $($r.Output)"
}

Check "down keeps sessions running" {
    $before = (Invoke-Rsk (@("exec", $Target, "--session", "persist") + $sshArgs + @("--", 'echo $$'))).Output.Trim()
    $r = Invoke-Rsk (@("down", $Target) + $sshArgs)
    Assert-True ($r.Output -match "keep running") "down output: $($r.Output)"
    $after = (Invoke-Rsk (@("exec", $Target, "--session", "persist") + $sshArgs + @("--", 'echo $$; echo ${RSK_PERSIST:-lost}'))).Output
    Assert-True ($after -match "kept") "session was lost by `rsk down`: $($after)"
    Assert-True ($after -match [regex]::Escape($before)) "shell process changed: $before -> $after"
}

Check "a running command survives a daemon crash" {
    $job = Start-Job -ScriptBlock {
        param($rsk, $target, $args_, $exe)
        & $rsk exec $target @args_ --timeout 90s -- 'sleep 12; echo SURVIVED-DAEMON-CRASH' 2>&1
    } -ArgumentList $Rsk, $Target, $sshArgs, $null
    Start-Sleep -Seconds 3
    Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", 'kill -9 $(cat ~/.rsk/run/rskd.sock.pid)')) -AllowFailure | Out-Null
    Start-Sleep -Seconds 14
    Receive-Job $job -ErrorAction SilentlyContinue | Out-Null
    Remove-Job $job -Force -ErrorAction SilentlyContinue
    $r = Invoke-Rsk (@("logs", $Target, "--tail", "8000") + $sshArgs)
    Assert-True ($r.Output -match "SURVIVED-DAEMON-CRASH") "command did not survive the crash: $($r.Output)"
    $r = Invoke-Rsk (@("exec", $Target) + $sshArgs + @("--", "echo survived-session-check"))
    Assert-True ($r.Output -match "survived-session-check") "session died with the daemon"
}

Check "down --kill-sessions stops the sessions" {
    $before = (Invoke-Rsk (@("exec", $Target, "--session", "persist") + $sshArgs + @("--", 'echo $$'))).Output.Trim()
    $r = Invoke-Rsk (@("down", $Target, "--kill-sessions", "--force") + $sshArgs)
    Assert-True ($r.Output -match "stopped all sessions") "down output: $($r.Output)"
    $after = (Invoke-Rsk (@("exec", $Target, "--session", "persist") + $sshArgs + @("--", 'echo $$; echo ${RSK_PERSIST:-fresh}'))).Output
    Assert-True ($after -match "fresh") "session env survived --kill-sessions: $($after)"
    Assert-True ($after -notmatch [regex]::Escape($before)) "shell process survived --kill-sessions: $before -> $after"
}

Check "session state is restored after a machine restart" {
    # leave state behind, then kill the daemon and the keeper alike (a reboot
    # loses both processes; only files on disk survive)
    Invoke-Rsk (@("exec", $Target, "--session", "reboot") + $sshArgs + @(
        "--", "cd /var && export RSK_REBOOT=yes && echo marker-before-reboot"
    )) | Out-Null
    Invoke-Rsk (@("exec", $Target, "--session", "reboot") + $sshArgs + @(
        "--", "pkill -9 -f 'rskilld.*session --name reboot'; kill -9 `$(cat ~/.rsk/run/rskd.sock.pid)"
    )) -AllowFailure | Out-Null
    Start-Sleep -Seconds 2
    $r = Invoke-Rsk (@("exec", $Target, "--session", "reboot") + $sshArgs + @(
        "--", 'pwd; echo "env=${RSK_REBOOT:-lost}"; history | grep -c marker-before-reboot'
    ))
    Assert-True ($r.Output -match "(?m)^/var\s*$") "cwd was not restored: $($r.Output)"
    Assert-True ($r.Output -match "env=yes") "environment was not restored: $($r.Output)"
    Assert-True ($r.Output -match "(?m)^[1-9]\d*\s*$") "shell history was not restored: $($r.Output)"
    $r = Invoke-Rsk (@("logs", $Target, "--session", "reboot", "--tail", "20000") + $sshArgs)
    Assert-True ($r.Output -match "marker-before-reboot") "scrollback was not restored: $($r.Output)"
    Assert-True ($r.Output -match "session restored") "restore marker missing: $($r.Output)"
    Invoke-Rsk (@("down", $Target, "--session", "reboot") + $sshArgs) -AllowFailure | Out-Null
}

Check "down stops the daemon" {
    $r = Invoke-Rsk (@("down", $Target, "--force") + $sshArgs)
    Assert-True ($r.Output -match "stopped") "down output: $($r.Output)"
}

if (-not $Keep) {
    Invoke-Rsk (@("down", $Target, "--purge") + $sshArgs) -AllowFailure | Out-Null
}

Write-Host ""
Write-Host "passed: $script:passed  failed: $script:failed" -ForegroundColor $(if ($script:failed -eq 0) { "Green" } else { "Red" })
if ($script:failed -gt 0) { exit 1 }
