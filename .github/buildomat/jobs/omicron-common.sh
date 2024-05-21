#!/bin/bash
#:
#: name = "omicron-common (helios)"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = "1.77.2"
#: output_rules = []
#: skip_clone = true

# Verify that omicron-common builds successfully when used as a dependency
# in an external project. It must not leak anything that requires an external
# dependency (apart from OpenSSL/pkg-config).

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

cargo new --lib test-project
cd test-project
cargo add omicron-common \
    --git https://github.com/oxidecomputer/omicron.git \
    --rev "$GITHUB_SHA"
cargo check
cargo build --release
