#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

readonly WASM_PACK_VERSION="0.15.0"

if ! command -v wasm-pack >/dev/null 2>&1; then
    echo "wasm-pack ${WASM_PACK_VERSION} is required; install it with: cargo install wasm-pack --version ${WASM_PACK_VERSION} --locked" >&2
    exit 1
fi

if [[ "$(wasm-pack --version)" != "wasm-pack ${WASM_PACK_VERSION}" ]]; then
    echo "wasm-pack ${WASM_PACK_VERSION} is required" >&2
    exit 1
fi

if ! command -v node >/dev/null 2>&1; then
    echo "Node 22 is required" >&2
    exit 1
fi

if [[ "$(node --version)" != v22.* ]]; then
    echo "Node 22 is required; found $(node --version)" >&2
    exit 1
fi

if ! rustup target list --installed | grep --quiet '^wasm32-unknown-unknown$'; then
    echo "the wasm32-unknown-unknown Rust target is required; install it with: rustup target add wasm32-unknown-unknown" >&2
    exit 1
fi

output_directory="$(mktemp -d)"
trap 'rm -rf -- "$output_directory"' EXIT

wasm-pack build oximeter/oxql-wasm \
    --target web \
    --release \
    --out-dir "$output_directory" \
    --out-name oxql

ls -lh "$output_directory/oxql.js" "$output_directory/oxql_bg.wasm"
wc -c "$output_directory/oxql.js" "$output_directory/oxql_bg.wasm"
OXQL_WASM_OUTPUT="$output_directory" \
    node --test oximeter/oxql-wasm/tests/js/*.test.mjs
