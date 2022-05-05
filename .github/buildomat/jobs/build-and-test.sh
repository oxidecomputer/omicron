#!/bin/bash
#:
#: name = "helios / build-and-test"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "nightly-2022-04-27"
#: output_rules = []
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

#
# Set up a custom temporary directory within whatever one we were given so that
# we can check later whether we left detritus around.
#
TEST_TMPDIR="${TMPDIR:-/var/tmp}/omicron_tmp"
echo "tests will store output in $TEST_TMPDIR"
mkdir $TEST_TMPDIR

#
# Put "./cockroachdb/bin" and "./clickhouse" on the PATH for the test
# suite.
#
export PATH="$PATH:$PWD/out/cockroachdb/bin:$PWD/out/clickhouse"

banner prerequisites
ptime -m bash ./tools/install_prerequisites.sh -y

#
# We build with:
#
# - RUSTFLAGS="-D warnings" RUSTDOCFLAGS="-D warnings": disallow warnings
#   in CI builds.  This can result in breakage when the toolchain is
#   updated, but that should only happen with a change to the repo, which
#   gives us an opportunity to find and fix any newly-introduced warnings.
#
# - `--locked`: do not update Cargo.lock when building.  Checking in
#   Cargo.lock ensures that everyone is using the same dependencies and
#   also gives us a record of which dependencies were used for each CI
#   run.  Building with `--locked` ensures that the checked-in Cargo.lock
#   is up to date.
#
banner build
export RUSTFLAGS="-D warnings"
export RUSTDOCFLAGS="-D warnings"
export TMPDIR=$TEST_TMPDIR
ptime -m cargo +'nightly-2022-04-27' build --locked --all-targets --verbose

#
# Check that building individual packages as when deploying Omicron succeeds
#
banner deploy-check
ptime -m cargo run --bin omicron-package -- check

#
# NOTE: We're using using the same RUSTFLAGS and RUSTDOCFLAGS as above to avoid
# having to rebuild here.
#
banner test
ptime -m cargo +'nightly-2022-04-27' test --workspace --locked --verbose

#
# Make sure that we have left nothing around in $TEST_TMPDIR.  The easiest way
# to check is to try to remove it with `rmdir`.
#
unset TMPDIR
echo "files in $TEST_TMPDIR (none expected on success):"
find $TEST_TMPDIR -ls
rmdir $TEST_TMPDIR
