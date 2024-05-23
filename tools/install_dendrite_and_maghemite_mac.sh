#! /usr/bin/env bash

# Updated from https://gist.github.com/david-crespo/d8aa7ea1afb877ff585e6ad90fc5bc2c.
# Relies on the `brew install` of `yq` from the install_builder_prerequisites.sh script.

set -euo pipefail

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."

DENDRITE_BIN_DIR="$(pwd)/out/dendrite-stub/root/opt/oxide/dendrite/bin"
MGD_BIN_DIR="$(pwd)/out/mgd/root/opt/oxide/mgd/bin"

DENDRITE_COMMIT="$(yq '.package.dendrite-stub.source.commit' package-manifest.toml)"
MGD_COMMIT="$(yq '.package.mgd.source.commit' package-manifest.toml)"

DENDRITE_REPO="../dendrite"
MGD_REPO="../maghemite"

if [ ! -d "$DENDRITE_REPO" ]; then
    mkdir -p "$DENDRITE_REPO"
    git clone git@github.com:oxidecomputer/dendrite.git $DENDRITE_REPO
fi

cd "$DENDRITE_REPO"
git fetch --all && git checkout "$DENDRITE_COMMIT"
cargo build -p dpd --release --features=tofino_stub
cargo build -p swadm --release
cp target/release/dpd "$DENDRITE_BIN_DIR/dpd"
cp target/release/swadm "$DENDRITE_BIN_DIR/swadm"

if [ ! -d "$MGD_REPO" ]; then
    mkdir -p "$MGD_REPO"
    git clone  git@github.com:oxidecomputer/maghemite.git "$MGD_REPO"
fi

cd "$MGD_REPO"
git fetch --all && git checkout "$MGD_COMMIT"
cargo build --bin mgd --no-default-features
cp target/debug/mgd "$MGD_BIN_DIR/mgd"

echo "now add the bin dirs to your path"
echo ""
echo "  export PATH=\"$DENDRITE_BIN_DIR:\$PATH\""
echo "  export PATH=\"$MGD_BIN_DIR:\$PATH\""
