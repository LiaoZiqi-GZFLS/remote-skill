# Install rsk on this machine and optionally the remote-workspace Agent Skill.
#
#   pwsh install.ps1                       # build + install binary
#   pwsh install.ps1 -InstallSkill         # also copy the skill into Codex
#   pwsh install.ps1 -BinDir D:\bin -SkillDir C:\path\to\skills
param(
    [string]$BinDir = "",
    [switch]$InstallSkill,
    [string]$SkillDir = "",
    [switch]$SkipBuild,
    [switch]$NoPathUpdate
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
if (-not $BinDir) { $BinDir = Join-Path $env:LOCALAPPDATA "rsk\bin" }

$binary = Join-Path $root "dist\rsk.exe"
if (-not $SkipBuild) {
    & (Join-Path $root "build.ps1") -Platforms "linux/amd64,linux/arm64"
    if ($LASTEXITCODE -ne 0) { throw "build failed" }
}
if (-not (Test-Path $binary)) { throw "missing $binary (run build.ps1 first)" }

New-Item -ItemType Directory -Force $BinDir | Out-Null
Copy-Item $binary (Join-Path $BinDir "rsk.exe") -Force
Write-Host "installed $(Join-Path $BinDir 'rsk.exe')" -ForegroundColor Green

if (-not $NoPathUpdate) {
    $userPath = [Environment]::GetEnvironmentVariable("PATH", "User")
    if ($userPath -notlike "*$BinDir*") {
        [Environment]::SetEnvironmentVariable("PATH", "$userPath;$BinDir", "User")
        Write-Host "added $BinDir to your user PATH (open a new terminal to pick it up)"
    } else {
        Write-Host "$BinDir already on PATH"
    }
}

if ($InstallSkill) {
    if (-not $SkillDir) {
        $codexHome = if ($env:CODEX_HOME) { $env:CODEX_HOME } else { Join-Path $env:USERPROFILE ".codex" }
        $SkillDir = Join-Path $codexHome "skills"
    }
    $dest = Join-Path $SkillDir "remote-workspace"
    New-Item -ItemType Directory -Force $dest | Out-Null
    Copy-Item (Join-Path $root "skills\remote-workspace\*") $dest -Recurse -Force
    Write-Host "installed skill -> $dest" -ForegroundColor Green
}

Write-Host ""
Write-Host "next:" -ForegroundColor Cyan
Write-Host "  rsk doctor <host>          # check a server"
Write-Host "  rsk up <host> --workspace ~/project"
