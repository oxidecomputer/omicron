#!/bin/bash
#:
#: name = "helios / package image"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "1.66.1"
#: output_rules = [
#:	"=/work/helios/image/output/zfs.img",
#:	"=/work/helios/image/output/rom",
#: ]
#: skip_clone = true
#: access_repos = [
#:	"oxidecomputer/amd-host-image-builder",
#:	"oxidecomputer/boot-image-tools",
#:	"oxidecomputer/chelsio-t6-roms",
#:	"oxidecomputer/helios",
#:	"oxidecomputer/helios-omnios-extra",
#:	"oxidecomputer/helios-omnios-build",
#: ]
set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

#
# The token authentication mechanism that affords us access to other private
# repositories requires that we use HTTPS URLs for GitHub, rather than SSH.
#
override_urls=(
    'git://github.com/'
    'git@github.com:'
    'ssh://github.com/'
    'ssh://git@github.com/'
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

pfexec mkdir -p /work
cd /work

# /work/gz: Global Zone artifacts to be placed in the Helios image.
# mkdir gz && cd gz
# ptime -m tar xvzf /input/package/work/global-zone-packages.tar.gz
# cd -

# TODO: Consider importing zones here too?

# Checkout helios at a pinned commit
git clone https://github.com/oxidecomputer/helios.git
cd helios
git checkout ac8a7e7ef9e9b5ef27334bc8016f5d123f852449

gmake setup
./helios-build experiment-image \
	-p helios-netdev=https://pkg.oxide.computer/helios-netdev \
	-F optever=0.21 \
	-B
