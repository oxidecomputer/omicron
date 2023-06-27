#!/bin/bash
#:
#: name = "helios / build OS image"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "1.70.0"
#: output_rules = [
#:	"=/work/helios/image/output/os.tar.gz",
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
#:	"oxidecomputer/helios-omicron-brand",
#:	"oxidecomputer/helios-omnios-build",
#:	"oxidecomputer/helios-omnios-extra",
#:	"oxidecomputer/nanobl-rs",
#: ]
#:
#: [dependencies.package]
#: job = "helios / package"
#:
#: [[publish]]
#: series = "image"
#: name = "os.tar.gz"
#: from_output = "/work/helios/image/output/os.tar.gz"
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

TOP=$PWD

source "$TOP/tools/include/force-git-over-https.sh"

# Checkout helios at a pinned commit into /work/helios
git clone https://github.com/oxidecomputer/helios.git /work/helios
cd /work/helios

# TODO: Consider importing zones here too?

cd "$TOP"
./tools/build-host-image.sh -B \
    -S /input/package/work/zones/switch-asic.tar.gz \
    /work/helios \
    /input/package/work/global-zone-packages.tar.gz
