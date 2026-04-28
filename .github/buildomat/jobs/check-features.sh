#!/bin/bash
#:
#: name = "check-features (helios)"
#: variety = "basic"
#: target = "helios-2.0-20260324"
#: rust_toolchain = true
#: output_rules = [
#:  "/out/*",
#:  "%/var/tmp/ci-resource-usage.csv",
#: ]

# Run the check-features `xtask` on illumos, testing compilation of feature combinations.

set -o errexit
set -o pipefail
set -o xtrace

# shellcheck source=/dev/null
source .github/buildomat/ci-env.sh

cargo --version
rustc --version

cargo build --release -p ci-resource-usage
bmat process start ci-resource-usage target/release/ci-resource-usage /var/tmp/ci-resource-usage.csv

#
# Set up our PATH for use with this workspace.
#
source ./env.sh
export PATH="$PATH:$PWD/out/cargo-hack"

banner prerequisites
ptime -m bash ./tools/install_builder_prerequisites.sh -y

#
# Check feature combinations with the `cargo xtask check-features` command.
#
banner hack-check
export CARGO_INCREMENTAL=0
ptime -m timeout 2h cargo xtask check-features --ci
