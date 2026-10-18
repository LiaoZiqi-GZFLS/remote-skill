#!/usr/bin/env bash
# Build rsk with the remote daemon embedded. Usage:
#   ./build.sh [platforms]
set -euo pipefail
cd "$(dirname "$0")"

platforms="${1:-linux/amd64,linux/arm64,darwin/arm64,darwin/amd64}"
version="$(sed -n 's/^var Version = "\(.*\)"/\1/p' internal/version/version.go | head -1)"
ldflags="-X remote-skill/internal/version.Version=${version}"

echo "version ${version}"
go run ./tools/pack -platforms "${platforms}"

mkdir -p dist
go build -trimpath -ldflags "${ldflags}" -o dist/rsk ./cmd/rsk
go build -trimpath -ldflags "${ldflags}" -o dist/rskilld ./cmd/rskilld
ls -l dist
echo "done. try: dist/rsk version"
