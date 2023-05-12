#!/bin/bash
#:
#: name = "helios / build TUF repo"
#: variety = "basic"
#: target = "helios-latest"
#: output_rules = [
#:	"=/work/manifest.toml",
#:	"=/work/repo.zip.part*",
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

set -o errexit
set -o pipefail
set -o xtrace

COMMIT=$(git rev-parse HEAD)
VERSION="1.0.0-alpha+git${COMMIT:0:11}"

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

for zone in /work/package/install/*.tar.gz; do
    cat >>/work/manifest.toml <<EOF
[[artifact.control_plane.source.zones]]
kind = "file"
path = "$zone"
EOF
done

for kind in host trampoline; do
    mkdir -p /work/os/$kind
    pushd /work/os/$kind
    # https://github.com/oxidecomputer/helios#os-image-archives
    tar xf /input/$kind/work/helios/image/output/os.tar.gz image/rom image/zfs.img
    popd

    cat >>/work/manifest.toml <<EOF
[artifact.$kind]
name = "$kind"
version = "$VERSION"
[artifact.$kind.source]
kind = "file"
path = "/input/$kind/work/helios/image/output/os.tar.gz"
EOF
done

/work/tufaceous assemble --no-generate-key --skip-all-present /work/manifest.toml /work/repo.zip

#
# XXX: Buildomat currently does not support uploads greater than 1 GiB. This is
# an awful temporary hack which we need to strip out the moment it does.
#
split -a 1 -b 1024m /work/repo.zip /work/repo.zip.part
