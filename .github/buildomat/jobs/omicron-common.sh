#!/bin/bash
#:
#: name = "omicron-common (helios)"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = true
#: output_rules = []

# Verify that omicron-common builds successfully when used as a dependency
# in an external project. It must not leak anything that requires an external
# dependency (apart from OpenSSL/pkg-config).

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

cd /tmp
cargo new --lib test-project
cd test-project
cargo add omicron-common --path /work/oxidecomputer/omicron/common
cp /work/oxidecomputer/omicron/Cargo.lock Cargo.lock
cargo check
cargo build --release
