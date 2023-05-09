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

set -o errexit
set -o pipefail
set -o xtrace

cargo --version
rustc --version

COMMIT=$(git rev-parse HEAD)
VERSION="1.0.0-alpha+git${COMMIT:0:11}"

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

target/release/tufaceous assemble --no-generate-key --skip-all-present /work/manifest.toml /work/repo.zip
