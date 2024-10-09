#!/bin/bash
#:
#: name = "a4x2-prepare"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = true
#: output_rules = [
#:	"=/work/a4x2-package/a4x2-package-out.tgz",
#: ]
#: access_repos = [
#:	"oxidecomputer/testbed",
#: ]
#: enable = true

source ./env.sh

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

cargo xtask a4x2-package --ci --live-tests --end-to-end-tests