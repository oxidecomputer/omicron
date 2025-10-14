#!/bin/bash
#:
#: name = "a4x2-prepare"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = true
#: output_rules = [
#:  "=/out/a4x2-package.tar.gz"
#:  "=/out/xtask"
#: ]
#: access_repos = [
#:	"oxidecomputer/testbed",
#: ]
#: enable = true

# shellcheck source=/dev/null
source ./env.sh
# shellcheck source=/dev/null
source .github/buildomat/ci-env.sh

set -o errexit
set -o pipefail
set -o xtrace

pfexec mkdir -p /out
pfexec chown "$UID" /out


# NOTE: This version should be in sync with the recommended version in
# .config/nextest.toml. (Maybe build an automated way to pull the recommended
# version in the future.)
NEXTEST_VERSION='0.9.98'

cargo --version
rustc --version
curl -sSfL --retry 10 https://get.nexte.st/"$NEXTEST_VERSION"/illumos | gunzip | tar -xvf - -C ~/.cargo/bin

#
# Prep to build omicron
#

banner "prerequisites"
./tools/install_builder_prerequisites.sh -y


#
# Build the commtest program and place in the output
#
# banner "commtest"
# cargo build -p end-to-end-tests --bin commtest --bin dhcp-server --release
# cp target/release/commtest /out/
# cp target/release/dhcp-server /out/

#
# Clone the testbed repo
#
# banner "testbed"
# cd /work/oxidecomputer
# rm -rf testbed
# git clone https://github.com/oxidecomputer/testbed
# cd testbed/a4x2

#
# Build the a4x2 cargo bay using the omicron sources in this branch, fetch the
# softnpu artifacts into the cargo bay, zip up the cargo bay and place it in the
# output.
#
# OMICRON=/work/oxidecomputer/omicron ./config/build-packages.sh

# Create an omicron archive that captures common assets

# pushd cargo-bay
# mkdir -p omicron-common/omicron/
# cp -r g0/omicron/out omicron-common/omicron/
# # sled agent, gateway and switch archives are sled-specific
# rm omicron-common/omicron/out/omicron-sled-agent.tar 
# rm omicron-common/omicron/out/omicron-gateway*
# rm omicron-common/omicron/out/switch-softnpu.tar.gz
# popd

# Remove everything in $sled/omicron/out except sled-agent, mgs (gateway), and
# switch tar archives, these common elements are in the omicron-common archive
# for sled in g0 g1 g2 g3; do
#     find cargo-bay/$sled/omicron/out/ -maxdepth 1 -mindepth 1 \
#         | grep -v sled-agent \
#         | grep -v omicron-gateway \
#         | grep -v switch-softnpu \
#         | xargs -l rm -rf
# done

# Put the softnpu artifacts in place.
# XXX I think this is no longer necessary?
# ./config/fetch-softnpu-artifacts.sh

# Archive everything up and place it in the output
# for x in ce cr1 cr2 g0 g1 g2 g3 tools omicron-common; do
#     tar -czf cargo-bay-$x.tgz cargo-bay/$x
#     mv cargo-bay-$x.tgz /out/
# done



cargo xtask a4x2 package \
    --testbed-commit 5dcfa349c7dd0953caeb23165db79f38929a0dca

# bundle of everything we need to run a4x2 tests
mv target/a4x2-package.tar.gz /out/

# xtask has the code to execute the tests
mv target/debug/xtask /out/
