#!/bin/bash
#:
#: name = "helios / package"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "1.68.2"
#: output_rules = [
#:	"=/work/package.tar.gz",
#:	"=/work/global-zone-packages.tar.gz",
#:	"=/work/trampoline-global-zone-packages.tar.gz",
#:	"=/work/zones/*.tar.gz",
#: ]
#:
#: [[publish]]
#: series = "image"
#: name = "global-zone-packages"
#: from_output = "/work/global-zone-packages.tar.gz"
#:
#: [[publish]]
#: series = "image"
#: name = "trampoline-global-zone-packages"
#: from_output = "/work/trampoline-global-zone-packages.tar.gz"

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

ptime -m ./tools/install_builder_prerequisites.sh -yp
ptime -m ./tools/ci_download_softnpu_machinery

# Build the test target
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t test target create -i standard -m non-gimlet -s softnpu
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t test package

tarball_src_dir="$(pwd)/out"

# Assemble some utilities into a tarball that can be used by deployment
# phases of buildomat.

files=(
	out/*.tar
	out/target/test
	out/softnpu/*
	package-manifest.toml
	smf/sled-agent/non-gimlet/config.toml
	target/release/omicron-package
	tools/create_virtual_hardware.sh
	tools/scrimlet/*
)

pfexec mkdir -p /work && pfexec chown $USER /work
ptime -m tar cvzf /work/package.tar.gz "${files[@]}"

# Build necessary for the global zone
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t host target create -i standard -m gimlet -s asic
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t host package

# Create global zone package @ /work/global-zone-packages.tar.gz
ptime -m ./tools/build-global-zone-packages.sh $tarball_src_dir /work

# Non-Global Zones

# Assemble Zone Images into their respective output locations.
mkdir -p /work/zones
zones=(
	out/clickhouse.tar.gz
	out/cockroachdb.tar.gz
	out/crucible-pantry.tar.gz
	out/crucible.tar.gz
	out/external-dns.tar.gz
	out/internal-dns.tar.gz
	out/omicron-nexus.tar.gz
	out/oximeter-collector.tar.gz
	out/propolis-server.tar.gz
	out/switch-asic.tar.gz
	out/switch-softnpu.tar.gz
)
cp "${zones[@]}" /work/zones/

#
# Global Zone files for Trampoline image
#

# Build necessary for the trampoline image
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t recovery target create -i trampoline
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t recovery package

# Create trampoline global zone package @ /work/trampoline-global-zone-packages.tar.gz
ptime -m ./tools/build-trampoline-global-zone-packages.sh $tarball_src_dir /work

