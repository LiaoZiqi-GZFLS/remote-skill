#!/usr/bin/env bash
# Install rsk and optionally the remote-workspace skill.
#   ./install.sh [--install-skill] [--bin-dir DIR] [--skip-build]
set -euo pipefail
cd "$(dirname "$0")"

bin_dir="${HOME}/.local/bin"
install_skill=0
skip_build=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-skill) install_skill=1; shift ;;
    --bin-dir) bin_dir="$2"; shift 2 ;;
    --skip-build) skip_build=1; shift ;;
    *) echo "unknown flag $1" >&2; exit 2 ;;
  esac
done

if [[ ${skip_build} -eq 0 ]]; then
  ./build.sh
fi

mkdir -p "${bin_dir}"
install -m 0755 dist/rsk "${bin_dir}/rsk"
echo "installed ${bin_dir}/rsk"

case ":${PATH}:" in
  *":${bin_dir}:"*) ;;
  *) echo "note: add ${bin_dir} to PATH" ;;
esac

if [[ ${install_skill} -eq 1 ]]; then
  codex_home="${CODEX_HOME:-${HOME}/.codex}"
  dest="${codex_home}/skills/remote-workspace"
  mkdir -p "${dest}"
  cp -R skills/remote-workspace/. "${dest}/"
  echo "installed skill -> ${dest}"
fi

echo
echo "next:"
echo "  rsk doctor <host>"
echo "  rsk up <host> --workspace ~/project"
