#!/bin/bash
#:
#: name = "helios / build OS images"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = "1.72.1"
#: output_rules = [
#:	"=/work/helios/upload/os-host.tar.gz",
#:	"=/work/helios/upload/os-trampoline.tar.gz",
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

# Check out helios into /work/helios
HELIOSDIR=/work/helios
git clone https://github.com/oxidecomputer/helios.git "$HELIOSDIR"
cd "$HELIOSDIR"
# Record the branch and commit in the output
git status --branch --porcelain=2
# Setting BUILD_OS to no makes setup skip repositories we don't need for
# building the OS itself (we are just building an image from already built OS).
BUILD_OS=no gmake setup

# Commands that "helios-build" would ask us to run (either explicitly or
# implicitly, to avoid an error).
rc=0
pfexec pkg install -q /system/zones/brand/omicron1/tools || rc=$?
case $rc in
    # `man pkg` notes that exit code 4 means no changes were made because
    # there is nothing to do; that's fine. Any other exit code is an error.
    0 | 4) ;;
    *) exit $rc ;;
esac

pfexec zfs create -p "rpool/images/$USER"


# TODO: Consider importing zones here too?

cd "$TOP"
OUTPUTDIR="$HELIOSDIR/upload"
mkdir "$OUTPUTDIR"

banner OS
./tools/build-host-image.sh -B \
    -S /input/package/work/zones/switch-asic.tar.gz \
    "$HELIOSDIR" \
    /input/package/work/global-zone-packages.tar.gz

mv "$HELIOSDIR/image/output/os.tar.gz" "$OUTPUTDIR/os-host.tar.gz"

banner Trampoline

./tools/build-host-image.sh -R \
    "$HELIOSDIR" \
    /input/package/work/trampoline-global-zone-packages.tar.gz

mv "$HELIOSDIR/image/output/os.tar.gz" "$OUTPUTDIR/os-trampoline.tar.gz"

