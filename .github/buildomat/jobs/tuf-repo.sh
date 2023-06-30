#!/bin/bash
#:
#: name = "helios / build TUF repo"
#: variety = "basic"
#: target = "helios-latest"
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

# Exclude `propolis-server` from the list of control plane zones: it is bundled
# into the OS ramdisk. It still shows up under `.../install` for development
# workflows using `omicron-package install` that don't build a full omicron OS
# ramdisk, so we filter it out here.
for zone in $(find /work/package/install -maxdepth 1 -type f -name '*.tar.gz' | grep -v propolis-server.tar.gz); do
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

# Fetch signed ROT images from oxidecomputer/dvt-dock.
source "$TOP/tools/dvt_dock_version"
git clone https://github.com/oxidecomputer/dvt-dock.git /work/dvt-dock
(cd /work/dvt-dock; git checkout "$COMMIT")
# Fetch SP images from oxidecomputer/hubris GHA artifacts.
source "$TOP/tools/hubris_version"
run_id=$(curl --netrc -fsS "https://api.github.com/repos/oxidecomputer/hubris/actions/runs?head_sha=$COMMIT" \
    | /opt/ooce/bin/jq -r '.workflow_runs[] | select(.path == ".github/workflows/dist.yml") | .id')
hubris_artifacts=$(curl --netrc -fsS "https://api.github.com/repos/oxidecomputer/hubris/actions/runs/$run_id/artifacts")

add_hubris_artifacts() {
    series="$1"
    dockdir="$2"
    shift 2

    manifest=/work/manifest-$series.toml
    cp /work/manifest.toml "$manifest"

    # ROT images from dvt-dock
    for noun in "$@"; do
        # tufaceous_kind: gimlet-c => gimlet_rot
        # hubris_kind: gimlet-c => gimlet-rot
        noun=${noun%-?}
        tufaceous_kind=${noun//sidecar/switch}_rot
        hubris_kind=${noun}-rot
        path_a="/work/dvt-dock/$dockdir/build-$hubris_kind-image-a-cert-dev.zip"
        path_b="/work/dvt-dock/$dockdir/build-$hubris_kind-image-b-cert-dev.zip"
        version_a=$(/work/caboose-util read-version "$path_a")
        version_b=$(/work/caboose-util read-version "$path_b")
        if [[ "$version_a" != "$version_b" ]]; then
            echo "version mismatch:"
            echo "  $path_a: $version_a"
            echo "  $path_b: $version_b"
            exit 1
        fi

        cat >>"$manifest" <<EOF
[artifact.$tufaceous_kind]
name = "$tufaceous_kind"
version = "$version_a"
[artifact.$tufaceous_kind.source]
kind = "composite-rot"
[artifact.$tufaceous_kind.source.archive_a]
kind = "file"
path = "$path_a"
[artifact.$tufaceous_kind.source.archive_b]
kind = "file"
path = "$path_b"
EOF
    done

    # SP images from hubris
    for noun in "$@"; do
        # tufaceous_kind: gimlet-c => gimlet_sp
        # job_name: gimlet-c => dist-ubuntu-latest-gimlet-c
        tufaceous_kind=${noun%-?}
        tufaceous_kind=${tufaceous_kind//sidecar/switch}_sp
        job_name=dist-ubuntu-latest-$noun
        if ! [[ -f "$job_name.zip" ]]; then
            url=$(/opt/ooce/bin/jq --arg name "$job_name" -r '.artifacts[] | select(.name == $name) | .archive_download_url' <<<"$hubris_artifacts")
            curl --netrc -fsSL -o "$job_name.zip" "$url"
            unzip "$job_name.zip"
        fi
        path="$PWD/build-$noun-image-default.zip"
        version=$(/work/caboose-util read-version "$path")
        cat >>"$manifest" <<EOF
[artifact.$tufaceous_kind]
name = "$tufaceous_kind"
version = "$version"
[artifact.$tufaceous_kind.source]
kind = "file"
path = "$path"
EOF
    done
}

# usage:              SERIES   DVT_DOCK_DIR  BOARDS...
add_hubris_artifacts  dogfood  staging       gimlet-c psc-b sidecar-b
add_hubris_artifacts  pvt1     staging       gimlet-d psc-b sidecar-c

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
