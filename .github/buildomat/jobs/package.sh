#!/bin/bash
#:
#: name = "helios / package"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "nightly-2023-01-19"
#: output_rules = [
#:	"=/work/package.tar.gz",
#:	"=/work/zones/*.tar.gz",
#: ]
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

ptime -m ./tools/install_builder_prerequisites.sh -yp
ptime -m ./tools/create_self_signed_cert.sh -yp

ptime -m cargo run --locked --release --bin omicron-package -- package

files=(
	out/*.tar
	package-manifest.toml
	smf/sled-agent/config.toml
	target/release/omicron-package
	tools/create_virtual_hardware.sh
)
ptime -m tar cvzf /work/package.tar.gz "${files[@]}"
mkdir -p /work/zones
mv out/*.tar.gz /work/zones/
