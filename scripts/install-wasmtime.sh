#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# The exercised CI host is Linux x86_64. Other hosts can install this release
# themselves and set WASMTIME for smoke-wasi-cli.mjs.
if [[ "$(uname -s)-$(uname -m)" != "Linux-x86_64" ]]; then
  echo 'Install Wasmtime 50.0.1 for your host and set WASMTIME to its executable.' >&2
  exit 1
fi
mkdir -p target/tools
wasmtime_download_dir=$(mktemp -d target/tools/wasmtime-download.XXXXXX)
trap 'rm -rf "$wasmtime_download_dir"' EXIT
curl -fsSL --retry 3 https://github.com/bytecodealliance/wasmtime/releases/download/v50.0.1/wasmtime-v50.0.1-x86_64-linux.tar.xz -o "$wasmtime_download_dir/archive.tar.xz"
echo "685466455dd42a14774a86c36c8f2e45cbb9bd6d92a850139315df60a75498a5  $wasmtime_download_dir/archive.tar.xz" | sha256sum -c -
mkdir -p target/tools/wasmtime
tar -xJf "$wasmtime_download_dir/archive.tar.xz" --strip-components=1 -C target/tools/wasmtime
target/tools/wasmtime/wasmtime --version
