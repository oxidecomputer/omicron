#!/bin/bash
#:
#: name = "helios / build TUF repo"
#: variety = "basic"
#: target = "helios-2.0"
#: output_rules = [
#:	"=/work/manifest*.toml",
#:	"=/work/repo-dogfood.zip*",
#:	"=/work/repo-pvt1.zip*",
#: ]
#: access_repos = [
#:	"oxidecomputer/dvt-dock",
#: ]
#:
#: [dependencies.ci-tools]
#: job = "helios / CI tools"
#:
#: [dependencies.package]
#: job = "helios / package"
#:
#: [dependencies.host]
#: job = "helios / build OS image"
#:
#: [dependencies.trampoline]
#: job = "helios / build trampoline OS image"
#:
#: [[publish]]
#: series = "dogfood"
#: name = "repo.zip.parta"
#: from_output = "/work/repo-dogfood.zip.parta"
#:
#: [[publish]]
#: series = "dogfood"
#: name = "repo.zip.partb"
#: from_output = "/work/repo-dogfood.zip.partb"
#:
#: [[publish]]
#: series = "dogfood"
#: name = "repo.zip.sha256.txt"
#: from_output = "/work/repo-dogfood.zip.sha256.txt"
#:
#: [[publish]]
#: series = "pvt1"
#: name = "repo.zip.parta"
#: from_output = "/work/repo-pvt1.zip.parta"
#:
#: [[publish]]
#: series = "pvt1"
#: name = "repo.zip.partb"
#: from_output = "/work/repo-pvt1.zip.partb"
#:
#: [[publish]]
#: series = "pvt1"
#: name = "repo.zip.sha256.txt"
#: from_output = "/work/repo-pvt1.zip.sha256.txt"
#:

set -o errexit
set -o pipefail
set -o xtrace

TOP=$PWD
VERSION=$(< /input/package/work/version.txt)

for bin in caboose-util tufaceous; do
    ptime -m gunzip < /input/ci-tools/work/$bin.gz > /work/$bin
    chmod a+x /work/$bin
done

#
# We do two things here:
# 1. Run `omicron-package stamp` on all the zones.
# 2. Run `omicron-package unpack` to switch from "package-name.tar.gz" to "service_name.tar.gz".
#
mkdir /work/package
pushd /work/package
tar xf /input/package/work/package.tar.gz out package-manifest.toml target/release/omicron-package
target/release/omicron-package -t default target create -i standard -m gimlet -s asic
ln -s /input/package/work/zones/* out/
rm out/switch-softnpu.tar.gz  # not used when target switch=asic
rm out/omicron-gateway-softnpu.tar.gz  # not used when target switch=asic
for zone in out/*.tar.gz; do
    target/release/omicron-package stamp "$(basename "${zone%.tar.gz}")" "$VERSION"
done
mv out/versioned/* out/
OMICRON_NO_UNINSTALL=1 target/release/omicron-package unpack --out install
popd

# Generate a throwaway repository key.
python3 -c 'import secrets; open("/work/key.txt", "w").write("ed25519:%s\n" % secrets.token_hex(32))'
read -r TUFACEOUS_KEY </work/key.txt
export TUFACEOUS_KEY

cat >/work/manifest.toml <<EOF
system_version = "$VERSION"

[artifact.control_plane]
name = "control-plane"
version = "$VERSION"
[artifact.control_plane.source]
kind = "composite-control-plane"
EOF

# Exclude a handful of tarballs from the list of control plane zones because
# they are bundled into the OS ramdisk. They still show up under `.../install`
# for development workflows using `omicron-package install` that don't build a
# full omicron OS ramdisk, so we filter them out here.
for zone in $(find /work/package/install -maxdepth 1 -type f -name '*.tar.gz' \
    | grep -v propolis-server.tar.gz \
    | grep -v mgs.tar.gz \
    | grep -v overlay.tar.gz \
    | grep -v switch.tar.gz); do
    cat >>/work/manifest.toml <<EOF
[[artifact.control_plane.source.zones]]
kind = "file"
path = "$zone"
EOF
done

for kind in host trampoline; do
    cat >>/work/manifest.toml <<EOF
[artifact.$kind]
name = "$kind"
version = "$VERSION"
[artifact.$kind.source]
kind = "file"
path = "/input/$kind/work/helios/image/output/os.tar.gz"
EOF
done

source "$TOP/tools/dvt_dock_version"
git init /work/dvt-dock
(
    cd /work/dvt-dock
    git remote add origin https://github.com/oxidecomputer/dvt-dock.git
    git fetch --depth 1 origin "$COMMIT"
    git checkout FETCH_HEAD
)

add_hubris_artifacts() {
    series="$1"
    rot_dir="$2"
    rot_version="$3"
    shift 3

    manifest=/work/manifest-$series.toml
    cp /work/manifest.toml "$manifest"

    for board_rev in "$@"; do
        board=${board_rev%-?}
        tufaceous_board=${board//sidecar/switch}

        rot_image_a="/work/dvt-dock/${rot_dir}/${board}/build-${board}-rot-image-a-${rot_version}.zip"
        rot_image_b="/work/dvt-dock/${rot_dir}/${board}/build-${board}-rot-image-b-${rot_version}.zip"
        sp_image="/work/dvt-dock/sp/${board}/build-${board_rev}-image-default.zip"

        rot_version_a=$(/work/caboose-util read-version "$rot_image_a")
        rot_version_b=$(/work/caboose-util read-version "$rot_image_b")
        if [[ "$rot_version_a" != "$rot_version_b" ]]; then
            echo "version mismatch:"
            echo "  $rot_image_a: $rot_version_a"
            echo "  $rot_image_b: $rot_version_b"
            exit 1
        fi
        sp_version=$(/work/caboose-util read-version "$sp_image")

        cat >>"$manifest" <<EOF
[artifact.${tufaceous_board}_rot]
name = "${tufaceous_board}_rot"
version = "$rot_version_a"
[artifact.${tufaceous_board}_rot.source]
kind = "composite-rot"
[artifact.${tufaceous_board}_rot.source.archive_a]
kind = "file"
path = "$rot_image_a"
[artifact.${tufaceous_board}_rot.source.archive_b]
kind = "file"
path = "$rot_image_b"
[artifact.${tufaceous_board}_sp]
name = "${tufaceous_board}_sp"
version = "$sp_version"
[artifact.${tufaceous_board}_sp.source]
kind = "file"
path = "$sp_image"
EOF
    done
}
# usage:              SERIES   ROT_DIR      ROT_VERSION              BOARDS...
add_hubris_artifacts  dogfood  staging/dev  cert-staging-dev-v1.0.0  gimlet-c psc-b sidecar-b
add_hubris_artifacts  pvt1     prod/rel     cert-prod-rel-v1.0.0     gimlet-d psc-c sidecar-c

for series in dogfood pvt1; do
    /work/tufaceous assemble --no-generate-key /work/manifest-$series.toml /work/repo-$series.zip
    digest -a sha256 /work/repo-$series.zip > /work/repo-$series.zip.sha256.txt

    #
    # XXX: Buildomat currently does not support uploads greater than 1 GiB. This is
    # an awful temporary hack which we need to strip out the moment it does.
    #
    split -a 1 -b 1024m /work/repo-$series.zip /work/repo-$series.zip.part
    rm /work/repo-$series.zip
    # Ensure the build doesn't fail if the repo gets smaller than 1 GiB.
    touch /work/repo-$series.zip.partb
done
