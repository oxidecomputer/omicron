#!/bin/bash
#:
#: name = "helios / package"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = "1.72.1"
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

#
# Generate the version for control plane artifacts here. We use `0.git` as the
# prerelease field because it comes before `alpha`.
#
# In this job, we stamp the version into packages installed in the host and
# trampoline global zone images.
#
COMMIT=$(git rev-parse HEAD)
VERSION="7.0.0-0.ci+git${COMMIT:0:11}"
echo "$VERSION" >/work/version.txt

ptime -m ./tools/install_builder_prerequisites.sh -yp
ptime -m ./tools/ci_download_softnpu_machinery

# Build the test target
export CARGO_INCREMENTAL=0
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t test target create -i standard -m non-gimlet -s softnpu -r single-sled
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t test package

# Assemble some utilities into a tarball that can be used by deployment
# phases of buildomat.

files=(
	out/*.tar
	out/target/test
	out/npuzone/*
	package-manifest.toml
	smf/sled-agent/non-gimlet/config.toml
	target/release/omicron-package
	tools/create_virtual_hardware.sh
    tools/virtual_hardware.sh
	tools/scrimlet/*
)

pfexec mkdir -p /work && pfexec chown $USER /work
ptime -m tar cvzf /work/package.tar.gz "${files[@]}"

tarball_src_dir="$(pwd)/out/versioned"
stamp_packages() {
	for package in "$@"; do
		# TODO: remove once https://github.com/oxidecomputer/omicron-package/pull/54 lands
		if [[ $package == mg-ddm-gz ]]; then
			echo "0.0.0" > VERSION
			tar rvf "out/$package.tar" VERSION
			rm VERSION
		fi

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
stamp_packages omicron-sled-agent mg-ddm-gz propolis-server overlay oxlog

# Create global zone package @ /work/global-zone-packages.tar.gz
ptime -m ./tools/build-global-zone-packages.sh "$tarball_src_dir" /work

# Non-Global Zones

# Assemble Zone Images into their respective output locations.
#
# Zones that are included into another are intentionally omitted from this list
# (e.g., the switch zone tarballs contain several other zone tarballs: dendrite,
# mg-ddm, etc.).
#
# Note that when building for a real gimlet, `propolis-server` and `switch-*`
# should be included in the OS ramdisk.
mkdir -p /work/zones
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
cp "${zones[@]}" /work/zones/

#
# Global Zone files for Trampoline image
#

# Build necessary for the trampoline image
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t recovery target create -i trampoline
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t recovery package
stamp_packages installinator mg-ddm-gz

# Create trampoline global zone package @ /work/trampoline-global-zone-packages.tar.gz
ptime -m ./tools/build-trampoline-global-zone-packages.sh "$tarball_src_dir" /work
