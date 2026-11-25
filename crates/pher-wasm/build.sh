#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

bindgen_version="$(cargo pkgid --locked -p wasm-bindgen | sed 's/.*[@#]//')"
crate_version="$(cargo pkgid --locked -p pher-wasm | sed 's/.*[@#]//')"
target_dir="$(cargo metadata --format-version 1 --no-deps --locked | node -e 'console.log(JSON.parse(require("fs").readFileSync(0, "utf8")).target_directory)')"

installed="$(wasm-bindgen --version 2>/dev/null | awk '{print $2}' || true)"
if [ "$installed" != "$bindgen_version" ]; then
  echo "need wasm-bindgen-cli $bindgen_version (found: ${installed:-none}); run: cargo install wasm-bindgen-cli --version $bindgen_version --locked" >&2
  exit 1
fi

binaryen_version=132.0.0

cargo build --locked --profile wasm-release -p pher-wasm --target wasm32-unknown-unknown
rm -rf pkg
wasm-bindgen --target web --omit-default-module-path --out-dir pkg \
  "$target_dir/wasm32-unknown-unknown/wasm-release/pher_wasm.wasm"
npx --yes --package "binaryen@$binaryen_version" -- wasm-opt -Oz \
  --enable-bulk-memory --enable-nontrapping-float-to-int --enable-sign-ext \
  --enable-mutable-globals --enable-reference-types --enable-multivalue \
  pkg/pher_wasm_bg.wasm -o pkg/pher_wasm_bg.wasm
node bytes-glue.mjs pkg "$crate_version" "$bindgen_version"
echo "built pkg/ (@pheromone/wasm $crate_version, wasm-bindgen $bindgen_version); test with: node test.mjs"
