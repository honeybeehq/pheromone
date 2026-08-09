#!/usr/bin/env bash
# Build the napi cdylib and place it next to index.js as pher-node.node.
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release -p pher-node
case "$(uname -s)" in
  Darwin) LIB=libpher_node.dylib ;;
  Linux) LIB=libpher_node.so ;;
  *) echo "unsupported platform" >&2; exit 1 ;;
esac
cp "../../target/release/$LIB" pher-node.node
echo "built pher-node.node"
