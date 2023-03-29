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

# Assemble global zone files in a temporary directory.
if ! tmp_gz=$(mktemp -d); then
  exit 1
fi
trap 'cd /; rm -rf "$tmp_gz"' EXIT

# Header file, identifying this is intended to be layered in the global zone.
# Within the ramdisk, this means that all files under "root/foo" should appear
# in the global zone as "/foo".
echo '{"v":"1","t":"layer"}' > "$tmp_gz/oxide.json"

# Extract the sled-agent tarball for re-packaging into the layered GZ archive.
pkg_dir="$tmp_gz/root/opt/oxide/sled-agent"
mkdir -p "$pkg_dir"
cd "$pkg_dir"
tar -xvfz "$tarball_src_dir/omicron-sled-agent.tar"
# Ensure that the manifest for the sled agent exists in a location where it may
# be automatically initialized.
mkdir -p "$tmp_gz/root/lib/svc/manifest/site/"
mv pkg/manifest.xml "$tmp_gz/root/lib/svc/manifest/site/sled-agent.xml"
cd -
# Extract the mg-ddm tarball for re-packaging into the layered GZ archive.
pkg_dir="$tmp_gz/root/opt/oxide/mg-ddm"
mkdir -p "$pkg_dir"
cd "$pkg_dir"
tar -xvfz "$tarball_src_dir/maghemite.tar"
cd -

cd "$tmp_gz" && tar cvfz /work/global-zone-packages.tar.gz oxide.json root
cd -

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
# Global Zone files for for Trampoline image
#

# Build necessary for the trampoline image
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t recovery target create -i trampoline
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t recovery package

if ! tmp_trampoline=$(mktemp -d); then
  exit 1
fi
trap 'cd /; rm -rf "$tmp_trampoline"' EXIT

echo '{"v":"1","t":"layer"}' > "$tmp_trampoline/oxide.json"

# Extract the installinator tarball for re-packaging into the layered GZ archive.
pkg_dir="$tmp_trampoline/root/opt/oxide/installinator"
mkdir -p "$pkg_dir"
cd "$pkg_dir"
tar -xvfz "$tarball_src_dir/installinator.tar"
# Ensure that the manifest for the installinator exists in a location where it may
# be automatically initialized.
mkdir -p "$tmp_trampoline/root/lib/svc/manifest/site/"
mv pkg/manifest.xml "$tmp_trampoline/root/lib/svc/manifest/site/installinator.xml"
cd -
# Extract the mg-ddm tarball for re-packaging into the layered GZ archive.
pkg_dir="$tmp_trampoline/root/opt/oxide/mg-ddm"
mkdir -p "$pkg_dir"
cd "$pkg_dir"
tar -xvfz "$tarball_src_dir/maghemite.tar"
cd -

mkdir -p /work
cd "$tmp_trampoline" && tar cvfz /work/trampoline-global-zone-packages.tar.gz oxide.json root
cd -


