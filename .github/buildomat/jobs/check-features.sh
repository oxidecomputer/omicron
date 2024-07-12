#!/bin/bash
#:
#: name = "check-features (helios)"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = true
#: output_rules = []

# Run the check-features `xtask` on illumos, testing compilation of feature combinations.

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

#
# Check the feature set with the `cargo xtask check-features` command.
#
banner hack-check
export CARGO_INCREMENTAL=0
ptime -m timeout 2h cargo xtask check-features --ci --install-version "$CARGO_HACK_VERSION"
RUSTDOCFLAGS="--document-private-items -D warnings" ptime -m cargo doc --workspace --no-deps --no-default-features
