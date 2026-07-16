#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#:
#: name = "helios / package"
#: variety = "basic"
#: target = "helios-3.0-16c64gb"
#: rust_toolchain = true
#: output_rules = [
#:	"=/work/package.tar.gz",
#: ]
#:

set -o errexit
set -o pipefail
set -o xtrace

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

# Exercise the same stamped-zone path used by releng while giving the deploy
# job a distinctive value to verify through Nexus. `install` reads packages
# from their unversioned output paths, so replace the Nexus archive with the
# stamped output before assembling package.tar.gz.
DEPLOY_SYSTEM_VERSION="0.0.0-deploy-test"
ptime -m cargo run --locked --release --bin omicron-package -- \
  --target test stamp nexus "$DEPLOY_SYSTEM_VERSION"
cp out/versioned/nexus.tar.gz out/nexus.tar.gz
printf '%s\n' "$DEPLOY_SYSTEM_VERSION" >out/deploy-system-version

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
	out/deploy-system-version
	out/target/test
	out/npuzone/*
	package-manifest.toml
	smf/sled-agent/non-gimlet/config.toml
	target/release/omicron-package
	target/release/xtask
	target/debug/bootstrap
	tests/*
	tools/opte_version
	tools/opte_version_override
)
ptime -m tar cvzf $WORK/package.tar.gz "${files[@]}" "${packages[@]}"
