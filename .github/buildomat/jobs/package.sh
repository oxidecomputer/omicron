#!/bin/bash
#:
#: name = "helios / package"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "nightly-2022-09-27"
#: output_rules = [
#:	"=/work/package.tar.gz",
#: ]
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

sed -e '/\[gateway\]/,/\[request\]/ s/^.*address =.*$/address = "192.168.1.199"/' \
	-e 's/^mac =.*$/mac = "18:c0:4d:0d:9f:b2"/' \
	-i smf/sled-agent/config-rss.toml

ptime -m ./tools/install_builder_prerequisites.sh -yp
ptime -m ./tools/create_self_signed_cert.sh -yp

ptime -m cargo run --locked --release --bin omicron-package -- package

files=(
	out/*.tar{,.gz}
	package-manifest.toml
	smf/sled-agent/config.toml
	target/release/omicron-package
	tools/create_virtual_hardware.sh
)
ptime -m tar cvzf /work/package.tar.gz "${files[@]}"
