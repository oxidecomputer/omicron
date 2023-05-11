#!/bin/bash
#:
#: name = "helios / tuf-repo"
#: variety = "basic"
#: target = "helios-latest"
#: rust_toolchain = "1.68.2"
#: output_rules = [
#:	"=/work/manifest.toml",
#:	"=/work/repo.zip",
#: ]
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

cargo --version
rustc --version

COMMIT=$(git rev-parse HEAD)
VERSION="1.0.0-alpha+git${COMMIT:0:11}"

#
# The package job builds two switch zones. The one we need should be named "switch.tar.gz" so sled-agent can find it.
#
mv /input/package/work/zones/{switch-asic,switch}.tar.gz
rm /input/package/work/zones/switch-softnpu.tar.gz

cargo build --locked --release --bin tufaceous

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

for zone in /input/package/work/zones/*; do
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
kind = "composite-host"
phase_1 = { kind = "file", path = "/work/os/$kind/image/rom" }
phase_2 = { kind = "file", path = "/work/os/$kind/image/zfs.img" }
EOF
done

target/release/tufaceous assemble --no-generate-key --skip-all-present /work/manifest.toml /work/repo.zip
