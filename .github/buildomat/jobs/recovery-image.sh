#!/bin/bash
#:
#: name = "helios / build recovery OS image"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "1.66.1"
#: output_rules = [
#:	"=/work/helios/image/output/zfs.img",
#:	"=/work/helios/image/output/rom",
#: ]
#: access_repos = [
#:	"oxidecomputer/amd-apcb",
#:	"oxidecomputer/amd-efs",
#:	"oxidecomputer/amd-firmware",
#:	"oxidecomputer/amd-flash",
#:	"oxidecomputer/amd-host-image-builder",
#:	"oxidecomputer/boot-image-tools",
#:	"oxidecomputer/chelsio-t6-roms",
#:	"oxidecomputer/compliance-pilot",
#:	"oxidecomputer/facade",
#:	"oxidecomputer/helios",
#:	"oxidecomputer/helios-omnios-build",
#:	"oxidecomputer/helios-omnios-extra",
#:	"oxidecomputer/nanobl-rs",
#: ]
#:
#: [dependencies.package]
#: job = "helios / package"
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

source "$(pwd)/tools/helios_version"

#
# The token authentication mechanism that affords us access to other private
# repositories requires that we use HTTPS URLs for GitHub, rather than SSH.
#
override_urls=(
    'git://github.com/'
    'git@github.com:'
    'ssh://github.com/'
    'ssh://git@github.com/'
    'git+ssh://git@github.com/'
)
for (( i = 0; i < ${#override_urls[@]}; i++ )); do
	git config --add --global url.https://github.com/.insteadOf \
	    "${override_urls[$i]}"
done

#
# Require that cargo use the git CLI instead of the built-in support.  This
# achieves two things: first, SSH URLs should be transformed on fetch without
# requiring Cargo.toml rewriting, which is especially difficult in transitive
# dependencies; second, Cargo does not seem willing on its own to look in
# ~/.netrc and find the temporary token that buildomat generates for our job,
# so we must use git which uses curl.
#
export CARGO_NET_GIT_FETCH_WITH_CLI=true

# Checkout helios at a pinned commit into /work/helios
pfexec mkdir -p /work
pushd /work
git clone https://github.com/oxidecomputer/helios.git
cd helios
git checkout "$COMMIT"
popd

./tools/build-host-image.sh -R helios /input/package/work/trampoline-global-zone-packages.tar.gz
