#!/bin/bash
#:
#: name = "build-live-tests"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = true
#: output_rules = []

# Builds the "live-tests" nextest archive

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

#
# Set up our PATH for use with this workspace.
#
source ./env.sh

banner prerequisites
ptime -m bash ./tools/install_builder_prerequisites.sh -y

banner live-tests
ptime -m cargo xtask live-tests
