#!/bin/bash
#:
#: name = "helios / package"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "1.66.1"
#: output_rules = [
#:	"=/work/global-zone-packages.tar.gz",
#:	"=/work/zones/*.tar.gz",
#: ]
#:
#: [[publish]]
#: series = "image"
#: name = "global-zone-packages"
#: from_output = "/out/global-zone-packages.tar.gz"

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

# Build
ptime -m ./tools/install_builder_prerequisites.sh -yp
ptime -m cargo run --locked --release --bin omicron-package -- package

# Assemble global zone files in a temporary directory.
tmp=$(mktemp -d)
mkdir -p "${tmp}/sled-agent"
tar -xvzf out/omicron-sled-agent.tar -C "${tmp}/sled-agent"
mkdir -p "${tmp}/maghemite"
tar -xvzf out/maghemite.tar -C "${tmp}/maghemite"

# Load those global zone files into a tarball that's ready to be exported.
mkdir -p /work
ptime -m tar cvzf /work/global-zone-packages.tar.gz -C "${tmp}" .

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
)
mv "${zones[@]}" /work/zones/
