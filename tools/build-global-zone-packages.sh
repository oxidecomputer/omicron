#!/usr/bin/env bash

set -eux

TOOLS_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Use the default "out" dir in omicron to find the needed packages if one isn't given
tarball_src_dir="$(readlink -f "${1:-"$TOOLS_DIR/../out"}")"
# Stash the final tgz in the given src dir if a different target isn't given
out_dir="$(readlink -f "${2:-"$tarball_src_dir"}")"

# Make sure needed packages exist
deps=(
    "$tarball_src_dir/omicron-sled-agent.tar"
    "$tarball_src_dir/mg-ddm-gz.tar"
    "$tarball_src_dir/pumpkind-gz.tar"
    "$tarball_src_dir/propolis-server.tar.gz"
    "$tarball_src_dir/overlay.tar.gz"
    "$tarball_src_dir/oxlog.tar"
)
for dep in "${deps[@]}"; do
    if [[ ! -e $dep ]]; then
        echo "Missing Global Zone dep: $(basename "$dep")"
        exit 1
    fi
done

# Assemble global zone files in a temporary directory.
tmp_gz=$(mktemp -d)
trap 'cd /; rm -rf "$tmp_gz"' EXIT # Cleanup on exit

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
tar -xvfz "$tarball_src_dir/mg-ddm-gz.tar"
cd -
# Extract the pumpkind tarball for re-packaging into the layered GZ archive.
pkg_dir="$tmp_gz/root/opt/oxide/pumpkind"
mkdir -p "$pkg_dir"
cd "$pkg_dir"
tar -xvfz "$tarball_src_dir/pumpkind-gz.tar"
cd -
# Extract the oxlog tarball for re-packaging into the layered GZ archive.
pkg_dir="$tmp_gz/root/opt/oxide/oxlog"
mkdir -p "$pkg_dir"
cd "$pkg_dir"
tar -xvfz "$tarball_src_dir/oxlog.tar"
cd -

# propolis should be bundled with this OS: Put the propolis-server zone image
# under /opt/oxide in the gz.
cp "$tarball_src_dir/propolis-server.tar.gz" "$tmp_gz/root/opt/oxide"

# The zone overlay should also be bundled.
cp "$tarball_src_dir/overlay.tar.gz" "$tmp_gz/root/opt/oxide"

# Create the final output and we're done
cd "$tmp_gz" && tar cvfz "$out_dir"/global-zone-packages.tar.gz oxide.json root
