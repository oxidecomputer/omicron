#!/bin/bash

set -eux

TOOLS_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Use the default "out" dir in omicron to find the needed packages if one isn't given
tarball_src_dir="$(readlink -f "${1:-"$TOOLS_DIR/../out"}")"
# Stash the final tgz in the given src dir if a different target isn't given
out_dir="$(readlink -f "${2:-$tarball_src_dir}")"

# Make sure needed packages exist
deps=(
    "$tarball_src_dir"/installinator.tar
    "$tarball_src_dir"/mg-ddm-gz.tar
)
for dep in "${deps[@]}"; do
    if [[ ! -e $dep ]]; then
        echo "Missing Trampoline Global Zone dep: $(basename "$dep")"
        exit 1
    fi
done

# Assemble global zone files in a temporary directory.
tmp_trampoline=$(mktemp -d)
trap 'cd /; rm -rf "$tmp_trampoline"' EXIT # Cleanup on exit

# Header file, identifying this is intended to be layered in the global zone.
# Within the ramdisk, this means that all files under "root/foo" should appear
# in the global zone as "/foo".
echo '{"v":"1","t":"layer"}' > "$tmp_trampoline/oxide.json"

# Extract the installinator tarball for re-packaging into the layered GZ archive.
pkg_dir="$tmp_trampoline/root/opt/oxide/installinator"
mkdir -p "$pkg_dir"
cd "$pkg_dir"
tar -xvfz "$tarball_src_dir/installinator.tar"
# Ensure that the manifest for the sled agent exists in a location where it may
# be automatically initialized.
mkdir -p "$tmp_trampoline/root/lib/svc/manifest/site/"
mv pkg/manifest.xml "$tmp_trampoline/root/lib/svc/manifest/site/installinator.xml"
cd -
# Extract the mg-ddm tarball for re-packaging into the layered GZ archive.
pkg_dir="$tmp_trampoline/root/opt/oxide/mg-ddm"
mkdir -p "$pkg_dir"
cd "$pkg_dir"
tar -xvfz "$tarball_src_dir/mg-ddm-gz.tar"
cd -

# Create the final output and we're done
cd "$tmp_trampoline" && tar cvfz "$out_dir"/trampoline-global-zone-packages.tar.gz oxide.json root
