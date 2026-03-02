#!/bin/bash
#:
#: name = "omicron-common (helios)"
#: variety = "basic"
#: target = "helios-2.0-20260324"
#: rust_toolchain = true
#: output_rules = [
#:  "%/var/tmp/ci-resource-usage.csv",
#: ]

# Verify that omicron-common builds successfully when used as a dependency
# in an external project. It must not leak anything that requires an external
# dependency (apart from OpenSSL/pkg-config).

set -o errexit
set -o pipefail
set -o xtrace

# shellcheck source=/dev/null
source .github/buildomat/ci-env.sh

cargo --version
rustc --version

cargo build --release -p ci-resource-usage
bmat process start ci-resource-usage target/release/ci-resource-usage /var/tmp/ci-resource-usage.csv

cd /tmp
cargo new --lib test-project
cd test-project
cargo add omicron-common --path /work/oxidecomputer/omicron/common
# Bootstrap `test-project`'s dependencies from the checked-in Cargo.lock.
# This means that `test-project` builds with the same commits as the main repo
# for any dependencies referenced as `{ git = "...", ref = "<branch>" }`. If we
# do not prepopulate `Cargo.lock` like this, an update in a dependency might get
# picked up here and be incompatible with `omicron-common`, causing it to fail
# to build (see Omicron issue #6691).
#
# The extra dependencies in `omicron` will get pruned by Cargo when it
# recalculates dependencies, but any dependencies that match will stay at the
# commit/version/etc already indicated in the lockfile.
cp /work/oxidecomputer/omicron/Cargo.lock Cargo.lock
cargo check
cargo build --release
