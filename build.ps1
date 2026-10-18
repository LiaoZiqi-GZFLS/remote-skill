# Build rsk (local CLI, with the remote daemon embedded) plus a local rskilld
# for debugging.
#
#   pwsh build.ps1                 # pack every platform, build host client
#   pwsh build.ps1 -Platforms linux/amd64,linux/arm64
#   pwsh build.ps1 -SkipPack       # reuse the binaries already embedded
param(
    [string]$Platforms = "",
    [switch]$SkipPack,
    [string]$Version = "",
    [switch]$All
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
Set-Location $root

function Find-Go {
    $cmd = Get-Command go -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    $cache = Join-Path $env:USERPROFILE ".cache\codex-tools\go"
    if (Test-Path $cache) {
        foreach ($pattern in @("go\bin\go.exe", "*\go\bin\go.exe", "*\bin\go.exe")) {
            $found = Get-ChildItem -Path (Join-Path $cache $pattern) -ErrorAction SilentlyContinue |
                Select-Object -First 1
            if ($found) { return $found.FullName }
        }
    }
    throw "go toolchain not found. Install Go or unpack it under $cache"
}

$go = Find-Go
if (-not $Version) {
    $line = Select-String -Path (Join-Path $root "internal\version\version.go") -Pattern '^var Version = "(.*)"' |
        Select-Object -First 1
    $Version = $line.Matches[0].Groups[1].Value
}
Write-Host "version $Version  toolchain $go" -ForegroundColor Cyan

if (-not $SkipPack) {
    $packArgs = @("run", "./tools/pack")
    if ($Platforms) { $packArgs += @("-platforms", $Platforms) }
    elseif (-not $All) { $packArgs += @("-platforms", "linux/amd64,linux/arm64,darwin/arm64,darwin/amd64,windows/amd64") }
    & $go @packArgs
    if ($LASTEXITCODE -ne 0) { throw "pack failed" }
}

New-Item -ItemType Directory -Force (Join-Path $root "dist") | Out-Null
$ld = "-X remote-skill/internal/version.Version=$Version"

& $go build -trimpath -ldflags $ld -o (Join-Path $root "dist\rsk.exe") ./cmd/rsk
if ($LASTEXITCODE -ne 0) { throw "building rsk failed" }
& $go build -trimpath -ldflags $ld -o (Join-Path $root "dist\rskilld.exe") ./cmd/rskilld
if ($LASTEXITCODE -ne 0) { throw "building rskilld failed" }

$targets = @(
    @{ OS = "linux"; Arch = "amd64"; Ext = "" },
    @{ OS = "darwin"; Arch = "arm64"; Ext = "" }
)
foreach ($t in $targets) {
    $env:GOOS = $t.OS; $env:GOARCH = $t.Arch; $env:CGO_ENABLED = "0"
    $out = Join-Path $root ("dist\rsk_{0}_{1}{2}" -f $t.OS, $t.Arch, $t.Ext)
    & $go build -trimpath -ldflags $ld -o $out ./cmd/rsk
    if ($LASTEXITCODE -ne 0) { throw "cross build $($t.OS)/$($t.Arch) failed" }
}
Remove-Item Env:GOOS, Env:GOARCH, Env:CGO_ENABLED -ErrorAction SilentlyContinue

Get-ChildItem (Join-Path $root "dist") | Sort-Object Name |
    Format-Table Name, @{ Label = "MiB"; Expression = { [math]::Round($_.Length / 1MB, 2) } }
Write-Host "done. try: dist\rsk.exe version" -ForegroundColor Green
