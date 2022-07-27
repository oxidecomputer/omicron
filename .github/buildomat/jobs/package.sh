#!/bin/bash
#:
#: name = "helios / package"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "nightly-2022-04-27"
#: output_rules = [
#:	"=/work/package.tar.gz",
#: ]
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

sed -i -e 's^pfexec ./tools/install_opte.sh^true^' ./tools/install_prerequisites.sh
ptime -m ./tools/install_prerequisites.sh -yp

ptime -m cargo run --locked --release --bin omicron-package -- package

# TODO: write tests and add the resulting test bin here
files=(
	out/*.tar{,.gz}
	package-manifest.toml
	smf/sled-agent/config.toml
	target/release/omicron-package
	tools/{create,destroy}_virtual_hardware.sh
)
ptime -m tar cvzf /work/package.tar.gz "${files[@]}"
