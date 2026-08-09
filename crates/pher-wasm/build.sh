#!/usr/bin/env bash
# Build the wasm package with JS bindings (target: nodejs; use --target web for browsers).
set -euo pipefail
cd "$(dirname "$0")"
npx --yes wasm-pack build --target "${1:-nodejs}" --release
echo "built pkg/ — test with: node test.mjs"
