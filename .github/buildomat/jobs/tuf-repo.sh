#!/bin/bash
#:
#: name = "helios / build TUF repo"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = true
#: output_rules = [
#:	"=/work/manifest.toml",
#:	"=/work/repo.zip",
#:	"=/work/repo.zip.sha256.txt",
#:	"=/work/helios.json",
#:	"=/work/incorporation.p5m",
#:	"=/work/incorporation.p5p",
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
#: [[publish]]
#: series = "rot-all"
#: name = "helios.json"
#: from_output = "/work/helios.json"
#:
#: [[publish]]
#: series = "rot-all"
#: name = "incorporation.p5m"
#: from_output = "/work/incorporation.p5m"
#:
#: [[publish]]
#: series = "rot-all"
#: name = "incorporation.p5p"
#: from_output = "/work/incorporation.p5p"
#:

set -o errexit
set -o pipefail
set -o xtrace

# ARTEMIS TESTING
pfexec zfs set compression=lz4 rpool

# shellcheck source=/dev/null
source .github/buildomat/ci-env.sh

cargo --version
rustc --version

# Before we do _anything_, quickly check that Cargo.lock is properly locked.
# Most of our tools (including releng!) eventually call `cargo xtask`, which
# runs without `--locked` and will update the lockfile.
cargo tree --locked >/dev/null

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

cargo xtask releng --output-dir /work --mkincorp
