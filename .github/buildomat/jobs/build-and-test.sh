#!/bin/bash
#:
#: name = "helios / build-and-test"
#: variety = "basic"
#: target = "helios"
#: rust_toolchain = "nightly-2022-04-27"
#: output_rules = []
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

banner clickhouse
ptime -m ./tools/ci_download_clickhouse

banner cockroach
ptime -m bash ./tools/ci_download_cockroachdb

#
# Put "./cockroachdb/bin" and "./clickhouse" on the PATH for the test
# suite.
#
export PATH="$PATH:$PWD/out/cockroachdb/bin:$PWD/out/clickhouse"

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
