#!/bin/bash
#:
#: name = "helios / build TUF repo"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = "1.77.2"
#: output_rules = [
#:	"=/work/manifest.toml",
#:	"=/work/repo.zip",
#:	"=/work/repo.zip.sha256.txt",
#:	"%/work/*.log",
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
#: [[publish]]
#: series = "rot-all"
#: name = "manifest.toml"
#: from_output = "/work/manifest.toml"
#:
#: [[publish]]
#: series = "rot-all"
#: name = "repo.zip"
#: from_output = "/work/repo.zip"
#:
#: [[publish]]
#: series = "rot-all"
#: name = "repo.zip.sha256.txt"
#: from_output = "/work/repo.zip.sha256.txt"
#:

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

ptime -m ./tools/install_builder_prerequisites.sh -yp
source ./tools/include/force-git-over-https.sh

rc=0
pfexec pkg install -q /system/zones/brand/omicron1/tools || rc=$?
case $rc in
    # `man pkg` notes that exit code 4 means no changes were made because
    # there is nothing to do; that's fine. Any other exit code is an error.
    0 | 4) ;;
    *) exit $rc ;;
esac

pfexec zfs create -p "rpool/images/$USER/host"
pfexec zfs create -p "rpool/images/$USER/recovery"

cargo xtask releng --output-dir /work
