#!/bin/bash
#:
#: name = "check-omicron-deployment (helios)"
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
# Put "./cockroachdb/bin" and "./clickhouse" on the PATH for the test
# suite.
#
export PATH="$PATH:$PWD/out/cockroachdb/bin:$PWD/out/clickhouse"

banner prerequisites
ptime -m bash ./tools/install_builder_prerequisites.sh -y

#
# Check that building individual packages as when deploying Omicron succeeds
#
banner deploy-check
ptime -m cargo run --bin omicron-package -- check
