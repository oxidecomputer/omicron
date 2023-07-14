#!/bin/bash
#:
#: name = "clippy (helios)"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = "1.70.0"
#: output_rules = []

# Run clippy on illumos (not just other systems) because a bunch of our code
# (that we want to check) is conditionally-compiled on illumos only.

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

banner clippy
# See the corresponding GitHub Actions job for more about these arguments.
ptime -m cargo clippy --all-targets -- --deny warnings --allow clippy::style
