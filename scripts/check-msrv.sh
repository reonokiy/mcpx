#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Keep the declared package floor and its independently installed compiler aligned.
# The developer's default toolchain and rust-toolchain.toml are not changed.
mcpx_msrv=1.88.0
cargo +"$mcpx_msrv" metadata --no-deps --format-version 1 --locked | python3 -c '
import json, sys
metadata = json.load(sys.stdin)
members = set(metadata["workspace_members"])
packages = [p for p in metadata["packages"] if p["id"] in members]
if not packages or any(p["rust_version"] != "1.88" for p in packages):
    sys.exit("MSRV gate and workspace rust-version disagree")
'

cargo +"$mcpx_msrv" check --workspace --all-targets --locked
cargo +"$mcpx_msrv" test --workspace --locked
cargo +"$mcpx_msrv" build -p mcpx-cli --locked
cargo +"$mcpx_msrv" build -p mcpx-native --examples --locked
cargo +"$mcpx_msrv" check -p mcpx --no-default-features --locked
cargo +"$mcpx_msrv" check -p mcpx --no-default-features --features command,config --target wasm32-unknown-unknown --locked
cargo +"$mcpx_msrv" build -p mcpx-web --target wasm32-unknown-unknown --locked
cargo +"$mcpx_msrv" build -p mcpx-component --target wasm32-wasip2 --locked
cargo +"$mcpx_msrv" build -p mcpx-cli --target wasm32-wasip2 --locked
cargo +"$mcpx_msrv" build -p mcpx-wasi --examples --target wasm32-wasip2 --locked
cargo +"$mcpx_msrv" check -p mcpx --no-default-features --features command,config,wasi --target wasm32-wasip2 --locked
