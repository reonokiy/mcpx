#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mcpx_profile=debug
mcpx_flags=()
mcpx_web_flags=()
mcpx_bindgen_flags=()
if [[ "${1:-}" == "--release" && "$#" == 1 ]]; then
  mcpx_profile=release
  mcpx_flags=(--release)
  # Keep size settings local to these Wasm builds, leaving native releases alone.
  mcpx_flags+=(--config 'profile.release.opt-level="s"'
    --config 'profile.release.lto=true'
    --config 'profile.release.codegen-units=1'
    --config 'profile.release.strip="debuginfo"')
  mcpx_web_flags=(--config 'profile.release.opt-level="z"')
  mcpx_bindgen_flags=(--remove-name-section --remove-producers-section)
elif [[ "$#" != 0 ]]; then
  echo "Usage: bash scripts/build-wasm.sh [--release]" >&2
  exit 2
fi
cargo build -p mcpx-web --target wasm32-unknown-unknown --locked "${mcpx_flags[@]}" "${mcpx_web_flags[@]}"
cargo build -p mcpx-component --target wasm32-wasip2 --locked "${mcpx_flags[@]}"
target/tools/bin/wasm-bindgen "target/wasm32-unknown-unknown/$mcpx_profile/mcpx_web.wasm" --target web --out-dir dist/web "${mcpx_bindgen_flags[@]}"
if [[ "$mcpx_profile" == release ]]; then
  node scripts/optimize-web-wasm.mjs
fi
node scripts/transpile-component.mjs "$mcpx_profile"
