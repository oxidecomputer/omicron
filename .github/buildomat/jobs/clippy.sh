#!/bin/bash
#:
#: name = "clippy (helios)"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = true
#: output_rules = []

# Run clippy on illumos (not just other systems) because a bunch of our code
# (that we want to check) is conditionally-compiled on illumos only.
#
# Note that `cargo clippy` includes `cargo check, so this ends up checking all
# of our code.

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

#
# Set up our PATH for use with this workspace.
#
source ./env.sh

banner prerequisites
ptime -m bash ./tools/install_builder_prerequisites.sh -y

banner clippy
export CARGO_INCREMENTAL=0
ptime -m cargo xtask clippy
RUSTDOCFLAGS="-Dwarnings" ptime -m cargo doc --workspace
