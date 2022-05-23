#!/bin/bash
#:
#: name = "helios / package"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "nightly-2022-04-27"
#: output_rules = [
#:	"/work/package.tar.gz",
#: ]
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

ptime -m ./tools/ci_download_clickhouse
ptime -m ./tools/ci_download_cockroachdb
ptime -m ./tools/ci_download_console

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
