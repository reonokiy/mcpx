#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mcpx_profile=debug
mcpx_flags=()
if [[ "${1:-}" == "--release" && "$#" == 1 ]]; then
  mcpx_profile=release
  mcpx_flags=(--release)
elif [[ "$#" != 0 ]]; then
  echo "Usage: bash scripts/build-wasm.sh [--release]" >&2
  exit 2
fi
cargo build -p mcpx-web --target wasm32-unknown-unknown --locked "${mcpx_flags[@]}"
cargo build -p mcpx-component --target wasm32-wasip2 --locked "${mcpx_flags[@]}"
target/tools/bin/wasm-bindgen "target/wasm32-unknown-unknown/$mcpx_profile/mcpx_web.wasm" --target web --out-dir dist/web
node scripts/transpile-component.mjs "$mcpx_profile"
