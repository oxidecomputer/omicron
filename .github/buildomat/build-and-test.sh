#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

#
# Set up our PATH for the test suite.
#

# shellcheck source=/dev/null
source ./env.sh
# shellcheck source=/dev/null
source .github/buildomat/ci-env.sh

target_os=$1

# NOTE: This version should be in sync with the recommended version in
# .config/nextest.toml. (Maybe build an automated way to pull the recommended
# version in the future.)
NEXTEST_VERSION='0.9.98'

cargo --version
rustc --version
curl -sSfL --retry 10 https://get.nexte.st/"$NEXTEST_VERSION"/"$1" | gunzip | tar -xvf - -C ~/.cargo/bin

#
# Set up a custom temporary directory within whatever one we were given so that
# we can check later whether we left detritus around.
#
TEST_TMPDIR='/var/tmp/omicron_tmp'
echo "tests will store ephemeral output in $TEST_TMPDIR" >&2
mkdir "$TEST_TMPDIR"

OUTPUT_DIR='/work'
echo "tests will store non-ephemeral output in $OUTPUT_DIR" >&2
mkdir -p "$OUTPUT_DIR"



banner prerequisites
ptime -m bash ./tools/install_builder_prerequisites.sh -y

#
# Write a machine-readable file with information about our build environment for
# later analysis of test results.
#
jq --null-input > "$OUTPUT_DIR/environment.json" \
    --arg bmat_factory_name "$(bmat factory name)" \
    --arg bmat_factory_private "$(bmat factory private)" \
    '{
        buildomat: {
            factory: {
                name: $bmat_factory_name,
                private: $bmat_factory_private,
            },
        },
    }'

# Do some test runs of the `ls-apis` command.
#
# This may require cloning some dependent private repos.  We do this before the
# main battery of tests because the GitHub tokens required for this only last
# for an hour so we want to get this done early.
#
# (TODO: This makes the build timings we record inaccurate.)
banner ls-apis
(
    source ./tools/include/force-git-over-https.sh;
    ptime -m cargo xtask ls-apis apis &&
        ptime -m cargo xtask ls-apis deployment-units &&
        ptime -m cargo xtask ls-apis servers &&
        ptime -m cargo xtask ls-apis check && \
	ptime -m cargo xtask ls-apis adoc > /dev/null
)

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
# We don't use `--workspace` here because we're not prepared to run tests
# from end-to-end-tests.
#
banner build
export RUSTFLAGS="-D warnings"
export RUSTDOCFLAGS="--document-private-items -D warnings"
# When running on illumos we need to pass an additional runpath that is
# usually configured via ".cargo/config" but the `RUSTFLAGS` env variable
# takes precedence. This path contains oxide specific libraries such as
# libipcc.
if [[ $target_os == "illumos" ]]; then
    RUSTFLAGS="$RUSTFLAGS -C link-arg=-R/usr/platform/oxide/lib/amd64"
fi
export TMPDIR="$TEST_TMPDIR"
export RUST_BACKTRACE=1
# We're building once, so there's no need to incur the overhead of an incremental build.
export CARGO_INCREMENTAL=0
# This allows us to build with unstable options, which gives us access to some
# timing information.
#
# If we remove "--timings=json" below, this would no longer be needed.
export RUSTC_BOOTSTRAP=1

# Build all the packages and tests, and keep track of how long each took to build.
# We report build progress to stderr, and the "--timings=json" output goes to stdout.
#
# The build graph ends up building several bin/test targets that depend on
# omicron-nexus at the same time, which uses significant memory to compile on
# illumos. To mitigate this we build everything except omicron-nexus's bin/test
# targets first, then finish the build after.
ptime -m cargo build -Z unstable-options --timings=json \
    --workspace --exclude=omicron-nexus --tests --locked --verbose \
    1>> "$OUTPUT_DIR/crate-build-timings.json"
ptime -m cargo build -Z unstable-options --timings=json \
    --workspace --tests --locked --verbose \
    1>> "$OUTPUT_DIR/crate-build-timings.json"

#
# We apply our own timeout to ensure that we get a normal failure on timeout
# rather than a buildomat timeout.  See oxidecomputer/buildomat#8.
#
banner test
ptime -m timeout 2h cargo nextest run --profile ci --locked --verbose

#
# https://github.com/nextest-rs/nextest/issues/16
#
banner doctest
ptime -m timeout 1h cargo test --doc --locked --verbose --no-fail-fast

# Build the live-tests.  This is only supported on illumos.
# We also can't actually run them here.  See the README for more details.
if [[ $target_os == "illumos" ]]; then
    banner "live-test"
    ptime -m cargo xtask live-tests
fi

# We expect the seed CRDB to be placed here, so we explicitly remove it so the
# rmdir check below doesn't get triggered. Nextest doesn't have support for
# teardown scripts so this is the best we've got.
rm -rf "$TEST_TMPDIR/crdb-base"*

#
# Make sure that we have left nothing around in $TEST_TMPDIR.  The easiest way
# to check is to try to remove it with `rmdir`.
#
unset TMPDIR
echo "files in $TEST_TMPDIR (none expected on success):" >&2
find "$TEST_TMPDIR" -ls
rmdir "$TEST_TMPDIR"
