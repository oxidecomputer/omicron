#!/bin/bash
#:
#: name = "helios / package"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = "1.78.0"
#: output_rules = [
#:	"=/work/version.txt",
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

WORK=/work
pfexec mkdir -p $WORK && pfexec chown $USER $WORK

#
# Generate the version for control plane artifacts here. We use `0.git` as the
# prerelease field because it comes before `alpha`.
#
# In this job, we stamp the version into packages installed in the host and
# trampoline global zone images.
#
COMMIT=$(git rev-parse HEAD)
VERSION="8.0.0-0.ci+git${COMMIT:0:11}"
echo "$VERSION" >/work/version.txt

ptime -m ./tools/install_builder_prerequisites.sh -yp
ptime -m ./tools/ci_download_softnpu_machinery

# Build the test target
export CARGO_INCREMENTAL=0
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t test target create -i standard -m non-gimlet -s softnpu -r single-sled
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t test package

# Build the xtask binary used by the deploy job
ptime -m cargo build --locked --release -p xtask

# Assemble some utilities into a tarball that can be used by deployment
# phases of buildomat.

files=(
	out/*.tar
	out/target/test
	out/npuzone/*
	package-manifest.toml
	smf/sled-agent/non-gimlet/config.toml
	target/release/omicron-package
	target/release/xtask
)

ptime -m tar cvzf $WORK/package.tar.gz "${files[@]}"

tarball_src_dir="$(pwd)/out/versioned"
stamp_packages() {
	for package in "$@"; do
		cargo run --locked --release --bin omicron-package -- stamp "$package" "$VERSION"
	done
}

# Keep the single-sled Nexus zone around for the deploy job. (The global zone
# build below overwrites the file.)
mv out/nexus.tar.gz out/nexus-single-sled.tar.gz

# Build necessary for the global zone
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t host target create -i standard -m gimlet -s asic -r multi-sled
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t host package
stamp_packages omicron-sled-agent mg-ddm-gz propolis-server overlay oxlog pumpkind-gz

# Create global zone package @ $WORK/global-zone-packages.tar.gz
ptime -m ./tools/build-global-zone-packages.sh "$tarball_src_dir" $WORK

# Non-Global Zones

# Assemble Zone Images into their respective output locations.
#
# Zones that are included into another are intentionally omitted from this list
# (e.g., the switch zone tarballs contain several other zone tarballs: dendrite,
# mg-ddm, etc.).
#
# Note that when building for a real gimlet, `propolis-server` and `switch-*`
# should be included in the OS ramdisk.
mkdir -p $WORK/zones
zones=(
  out/clickhouse.tar.gz
  out/clickhouse_keeper.tar.gz
  out/cockroachdb.tar.gz
  out/crucible-pantry-zone.tar.gz
  out/crucible-zone.tar.gz
  out/external-dns.tar.gz
  out/internal-dns.tar.gz
  out/nexus.tar.gz
  out/nexus-single-sled.tar.gz
  out/oximeter.tar.gz
  out/propolis-server.tar.gz
  out/switch-*.tar.gz
  out/ntp.tar.gz
  out/omicron-gateway-softnpu.tar.gz
  out/omicron-gateway-asic.tar.gz
  out/overlay.tar.gz
  out/probe.tar.gz
)
cp "${zones[@]}" $WORK/zones/

#
# Global Zone files for Trampoline image
#

# Build necessary for the trampoline image
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t recovery target create -i trampoline
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t recovery package
stamp_packages installinator mg-ddm-gz

# Create trampoline global zone package @ $WORK/trampoline-global-zone-packages.tar.gz
ptime -m ./tools/build-trampoline-global-zone-packages.sh "$tarball_src_dir" $WORK
