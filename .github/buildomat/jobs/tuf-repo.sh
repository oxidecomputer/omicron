#!/bin/bash
#:
#: name = "helios / build TUF repo"
#: variety = "basic"
#: target = "helios-latest"
#: output_rules = [
#:	"=/work/manifest.toml",
#:	"=/work/repo-dogfood.zip*",
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
#: series = "tuf-repo"
#: name = "repo-dogfood.zip.parta"
#: from_output = "/work/repo-dogfood.zip.parta"
#:
#: [[publish]]
#: series = "tuf-repo"
#: name = "repo-dogfood.zip.partb"
#: from_output = "/work/repo-dogfood.zip.partb"
#:
#: [[publish]]
#: series = "tuf-repo"
#: name = "repo-dogfood.zip.sha256.txt"
#: from_output = "/work/repo-dogfood.zip.sha256.txt"
#:

set -o errexit
set -o pipefail
set -o xtrace

TOP=$PWD
VERSION=$(< /input/package/work/version.txt)

source "$TOP/tools/dvt_dock_version"
DVT_DOCK_COMMIT=$COMMIT
source "$TOP/tools/hubris_version"
HUBRIS_COMMIT=$COMMIT

ptime -m gunzip < /input/ci-tools/work/tufaceous.gz > /work/tufaceous
chmod a+x /work/tufaceous

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
git clone https://github.com/oxidecomputer/dvt-dock.git /work/dvt-dock
(cd /work/dvt-dock; git checkout "$DVT_DOCK_COMMIT")
DVT_DOCK_VERSION="1.0.0-alpha+git${DVT_DOCK_COMMIT:0:11}"

for noun in gimlet psc sidecar; do
    tufaceous_kind=${noun//sidecar/switch}_rot
    hubris_kind=${noun}-rot
    cat >>/work/manifest.toml <<EOF
[artifact.$tufaceous_kind]
name = "$tufaceous_kind"
version = "$DVT_DOCK_VERSION"
[artifact.$tufaceous_kind.source]
kind = "composite-rot"
[artifact.$tufaceous_kind.source.archive_a]
kind = "file"
path = "/work/dvt-dock/staging/build-$hubris_kind-image-a-cert-dev.zip"
[artifact.$tufaceous_kind.source.archive_b]
kind = "file"
path = "/work/dvt-dock/staging/build-$hubris_kind-image-b-cert-dev.zip"
EOF
done

# Fetch SP images from oxidecomputer/hubris GHA artifacts.
HUBRIS_VERSION="1.0.0-alpha+git${HUBRIS_COMMIT:0:11}"
run_id=$(curl --netrc -fsS "https://api.github.com/repos/oxidecomputer/hubris/actions/runs?head_sha=$HUBRIS_COMMIT" \
    | /opt/ooce/bin/jq -r '.workflow_runs[] | select(.path == ".github/workflows/dist.yml") | .id')
artifacts=$(curl --netrc -fsS "https://api.github.com/repos/oxidecomputer/hubris/actions/runs/$run_id/artifacts")
for noun in gimlet-c psc-b sidecar-b; do
    tufaceous_kind=${noun%-?}
    tufaceous_kind=${tufaceous_kind//sidecar/switch}_sp
    job_name=dist-ubuntu-latest-$noun
    url=$(/opt/ooce/bin/jq --arg name "$job_name" -r '.artifacts[] | select(.name == $name) | .archive_download_url' <<<"$artifacts")
    curl --netrc -fsSL -o $job_name.zip "$url"
    unzip $job_name.zip
    cat >>/work/manifest.toml <<EOF
[artifact.$tufaceous_kind]
name = "$tufaceous_kind"
version = "$HUBRIS_VERSION"
[artifact.$tufaceous_kind.source]
kind = "file"
path = "$PWD/build-$noun-image-default.zip"
EOF
done

/work/tufaceous assemble --no-generate-key /work/manifest.toml /work/repo-dogfood.zip
digest -a sha256 /work/repo-dogfood.zip > /work/repo-dogfood.zip.sha256.txt

#
# XXX: Buildomat currently does not support uploads greater than 1 GiB. This is
# an awful temporary hack which we need to strip out the moment it does.
#
split -a 1 -b 1024m /work/repo-dogfood.zip /work/repo-dogfood.zip.part
rm /work/repo-dogfood.zip
# Ensure the build doesn't fail if the repo gets smaller than 1 GiB.
touch /work/repo-dogfood.zip.partb
