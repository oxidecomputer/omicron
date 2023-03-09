#!/bin/bash
#:
#: name = "helios / package"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "1.66.1"
#: output_rules = [
#:	"=/work/package.tar.gz",
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
ptime -m cargo run --locked --release --bin omicron-package -- \
  -t 'image_type=standard machine_type=nongimlet switch_variant=stub' package

tarball_src_dir="$(pwd)/out"

# Assemble some utilities into a tarball that can be used by deployment
# phases of buildomat.

files=(
	out/*.tar
	package-manifest.toml
	smf/sled-agent/config.toml
	target/release/omicron-package
	tools/create_virtual_hardware.sh
)

ptime -m tar cvzf /work/package.tar.gz "${files[@]}"

ptime -m cargo run --locked --release --bin omicron-package -- \
  -t 'image_type=standard machine_type=gimlet switch_variant=asic' package

# Assemble global zone files in a temporary directory.
if ! tmp=$(mktemp -d); then
  exit 1
fi
trap 'cd /; rm -rf "$tmp"' EXIT

# Header file, identifying this is intended to be layered in the global zone.
# Within the ramdisk, this means that all files under "root/foo" should appear
# in the global zone as "/foo".
echo '{"v":"1","t":"layer"}' > "$tmp/oxide.json"

# Extract the sled-agent tarball for re-packaging into the layered GZ archive.
pkg_dir="$tmp/root/opt/oxide/sled-agent"
mkdir -p "$pkg_dir"
cd "$pkg_dir"
tar -xvfz "$tarball_src_dir/omicron-sled-agent.tar"
# Ensure that the manifest for the sled agent exists in a location where it may
# be automatically initialized.
mkdir -p "$tmp/root/lib/svc/manifest/site/"
mv pkg/manifest.xml "$tmp/root/lib/svc/manifest/site/sled-agent.xml"
cd -

# Extract the mg-ddm tarball for re-packaging into the layered GZ archive.
pkg_dir="$tmp/root/opt/oxide/mg-ddm"
mkdir -p "$pkg_dir"
cd "$pkg_dir"
tar -xvfz "$tarball_src_dir/maghemite.tar"
cd -

mkdir -p /work
cd "$tmp" && tar cvfz /work/global-zone-packages.tar.gz oxide.json root
cd -

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
	out/switch-stub.tar.gz
)
cp "${zones[@]}" /work/zones/
