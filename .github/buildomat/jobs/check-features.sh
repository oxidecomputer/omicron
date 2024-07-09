#!/bin/bash
#:
#: name = "check-features (helios)"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = true
#: output_rules = []

# Run cargo check on illumos with feature-specifics like `no-default-features`
# or `feature-powerset`.

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

# NOTE: This version should be in sync with the recommended version in
# ./dev-tools/xtask/src/check-features.rs.
CARGO_HACK_VERSION='0.6.28'

#
# Set up our PATH for use with this workspace.
#
source ./env.sh

banner prerequisites
ptime -m bash ./tools/install_builder_prerequisites.sh -y

banner check
export CARGO_INCREMENTAL=0
ptime -m cargo check --workspace --bins --tests --no-default-features
RUSTDOCFLAGS="--document-private-items -D warnings" ptime -m cargo doc --workspace --no-deps --no-default-features

#
# Check the feature set with the `cargo xtask check-features` command.
#
banner hack
ptime -m timeout 2h cargo xtask check-features --version "$CARGO_HACK_VERSION" --exclude-features image-trampoline,image-standard
