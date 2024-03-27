#!/bin/bash
#:
#: name = "helios / build TUF repo"
#: variety = "basic"
#: target = "helios-2.0"
#: output_rules = [
#:	"=/work/manifest*.toml",
#:	"=/work/repo-*.zip.part*",
#:	"=/work/repo-*.zip.sha256.txt",
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
#: job = "helios / build OS images"
#:
#: [[publish]]
#: series = "rot-all"
#: name = "repo.zip.parta"
#: from_output = "/work/repo-rot-all.zip.parta"
#:
#: [[publish]]
#: series = "rot-all"
#: name = "repo.zip.partb"
#: from_output = "/work/repo-rot-all.zip.partb"
#:
#: [[publish]]
#: series = "rot-all"
#: name = "repo.zip.sha256.txt"
#: from_output = "/work/repo-rot-all.zip.sha256.txt"
#:

set -o errexit
set -o pipefail
set -o xtrace

ALL_BOARDS=(gimlet-{c..f} psc-{b..c} sidecar-{b..c})

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
target/release/omicron-package -t default target create -i standard -m gimlet -s asic -r multi-sled
ln -s /input/package/work/zones/* out/
rm out/switch-softnpu.tar.gz  # not used when target switch=asic
rm out/omicron-gateway-softnpu.tar.gz  # not used when target switch=asic
rm out/nexus-single-sled.tar.gz # only used for deploy tests
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

[[artifact.control_plane]]
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
    | egrep -v '(propolis-server|mgs|overlay|switch)\.tar\.gz'); do
    cat >>/work/manifest.toml <<EOF
[[artifact.control_plane.source.zones]]
kind = "file"
path = "$zone"
EOF
done

for kind in host trampoline; do
    cat >>/work/manifest.toml <<EOF
[[artifact.$kind]]
name = "$kind"
version = "$VERSION"
[artifact.$kind.source]
kind = "file"
path = "/input/host/work/helios/upload/os-$kind.tar.gz"
EOF
done

# Fetch SP images from a Hubris release.
mkdir /work/hubris
pushd /work/hubris
source "$TOP/tools/hubris_version"
for tag in "${TAGS[@]}"; do
    for board in "${ALL_BOARDS[@]}"; do
        if [[ "${tag%-*}" = "${board%-*}" ]]; then
            file=build-${board}-image-default-${tag#*-}.zip
            curl -fLOsS "https://github.com/oxidecomputer/hubris/releases/download/$tag/$file"
            grep -F "$file" "$TOP/tools/hubris_checksums" | shasum -a 256 -c -
            mv "$file" "$board.zip"
        fi
    done
done
popd

# Fetch signed ROT images from dvt-dock.
source "$TOP/tools/dvt_dock_version"
git init /work/dvt-dock
(
    cd /work/dvt-dock
    git remote add origin https://github.com/oxidecomputer/dvt-dock.git
    git fetch --depth 1 origin "$COMMIT"
    git checkout FETCH_HEAD
)

caboose_util_rot() {
    # usage: caboose_util_rot ACTION IMAGE_A IMAGE_B
    output_a=$(/work/caboose-util "$1" "$2")
    output_b=$(/work/caboose-util "$1" "$3")
    if [[ "$output_a" != "$output_b" ]]; then
        >&2 echo "\`caboose-util $1\` mismatch:"
        >&2 echo "  $2: $output_a"
        >&2 echo "  $3: $output_b"
        exit 1
    fi
    echo "$output_a"
}

# Add the SP images.
for board_rev in "${ALL_BOARDS[@]}"; do
    board=${board_rev%-?}
    tufaceous_board=${board//sidecar/switch}
    sp_image="/work/hubris/${board_rev}.zip"
    sp_caboose_version=$(/work/caboose-util read-version "$sp_image")
    sp_caboose_board=$(/work/caboose-util read-board "$sp_image")

    cat >>/work/manifest.toml <<EOF
[[artifact.${tufaceous_board}_sp]]
name = "$sp_caboose_board"
version = "$sp_caboose_version"
[artifact.${tufaceous_board}_sp.source]
kind = "file"
path = "$sp_image"
EOF
done

# Add the ROT images.
add_hubris_artifacts() {
    rot_dir="$1"
    rot_version="$2"

    for board in gimlet psc sidecar; do
        tufaceous_board=${board//sidecar/switch}
        rot_image_a="/work/dvt-dock/${rot_dir}/${board}/build-${board}-rot-image-a-${rot_version}.zip"
        rot_image_b="/work/dvt-dock/${rot_dir}/${board}/build-${board}-rot-image-b-${rot_version}.zip"
        rot_caboose_version=$(caboose_util_rot read-version "$rot_image_a" "$rot_image_b")
        rot_caboose_board=$(caboose_util_rot read-board "$rot_image_a" "$rot_image_b")

        cat >>/work/manifest.toml <<EOF
[[artifact.${tufaceous_board}_rot]]
name = "$rot_caboose_board-${rot_dir//\//-}"
version = "$rot_caboose_version"
[artifact.${tufaceous_board}_rot.source]
kind = "composite-rot"
[artifact.${tufaceous_board}_rot.source.archive_a]
kind = "file"
path = "$rot_image_a"
[artifact.${tufaceous_board}_rot.source.archive_b]
kind = "file"
path = "$rot_image_b"
EOF
    done
}
# usage:              ROT_DIR      ROT_VERSION
add_hubris_artifacts  staging/dev  cert-staging-dev-v1.0.7
add_hubris_artifacts  prod/rel     cert-prod-rel-v1.0.7

# Build the TUF ZIP.
/work/tufaceous assemble --no-generate-key /work/manifest.toml /work/repo-rot-all.zip
digest -a sha256 /work/repo-rot-all.zip > /work/repo-rot-all.zip.sha256.txt

#
# XXX: There are some issues downloading Buildomat artifacts > 1 GiB, see
# oxidecomputer/buildomat#36.
#
split -a 1 -b 1024m /work/repo-rot-all.zip /work/repo-rot-all.zip.part
rm /work/repo-rot-all.zip
# Ensure the build doesn't fail if the repo gets smaller than 1 GiB.
touch /work/repo-rot-all.zip.partb
