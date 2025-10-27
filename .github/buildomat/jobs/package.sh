#!/bin/bash
#:
#: name = "helios / package"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = true
#: output_rules = [
#:	"=/work/package.tar.gz",
#: ]
#:

set -o errexit
set -o pipefail
set -o xtrace

# ARTEMIS TESTING
pfexec zfs set compression=lz4 rpool

# shellcheck source=/dev/null
source .github/buildomat/ci-env.sh

cargo --version
rustc --version

WORK=/work
pfexec mkdir -p $WORK && pfexec chown $USER $WORK

ptime -m ./tools/install_builder_prerequisites.sh -yp
ptime -m cargo xtask download softnpu

# Build the test target
export CARGO_INCREMENTAL=0
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t test target create -p dev
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t test package
mapfile -t packages \
  < <(cargo run --locked --release --bin omicron-package -- -t test list-outputs)

# Build the xtask binary used by the deploy job
ptime -m cargo build --locked --release -p xtask

# Build the end-to-end tests
# Reduce debuginfo just to line tables.
export CARGO_PROFILE_DEV_DEBUG=line-tables-only
export CARGO_PROFILE_TEST_DEBUG=line-tables-only
ptime -m cargo build --locked -p end-to-end-tests --tests --bin bootstrap \
  --message-format json-render-diagnostics >/tmp/output.end-to-end.json
mkdir tests
/opt/ooce/bin/jq -r 'select(.profile.test) | .executable' /tmp/output.end-to-end.json \
  | xargs -I {} -t cp {} tests/

# Assemble these outputs and some utilities into a tarball that can be used by
# deployment phases of buildomat.

files=(
	.github/buildomat/ci-env.sh
	out/target/test
	out/npuzone/*
	package-manifest.toml
	smf/sled-agent/non-gimlet/config.toml
	target/release/omicron-package
	target/release/xtask
	target/debug/bootstrap
	tests/*
)
ptime -m tar cvzf $WORK/package.tar.gz "${files[@]}" "${packages[@]}"
