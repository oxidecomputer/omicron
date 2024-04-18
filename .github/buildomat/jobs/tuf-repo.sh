#!/bin/bash
#:
#: name = "helios / build TUF repo"
#: variety = "basic"
#: target = "helios-2.0"
#: output_rules = [
#:	"=/work/manifest*.toml",
#:	"=/work/repo-*.zip",
#:	"=/work/repo-*.zip.sha256.txt",
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
#: name = "manifest.toml"
#: from_output = "/work/manifest.toml"
#:
#: [[publish]]
#: series = "rot-all"
#: name = "repo.zip"
#: from_output = "/work/repo-rot-all.zip"
#:
#: [[publish]]
#: series = "rot-all"
#: name = "repo.zip.sha256.txt"
#: from_output = "/work/repo-rot-all.zip.sha256.txt"
#:

set -o errexit
set -o pipefail
set -o xtrace

TOP=$PWD
VERSION=$(< /input/package/work/version.txt)

for bin in caboose-util tufaceous permslip; do
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

download_region_manifests() {
	url=$1
	name=$2
	mkdir $2
	pushd $2
	while read -r manifest_hash manifest_name; do
		/work/permslip --url=$url --anonymous get-artifact $manifest_hash --out $manifest_name
		# hash refers to the hash in permission slip
		grep -F "hash =" $manifest_name | cut -d "=" -f 2 | tr -d "\" " | xargs -L 1 -I {} /work/permslip --url=$1 --anonymous get-artifact {} --out {}.zip
		# turn the hash entry into the path we just downloaded in the manifest
		sed "s|hash = \"\(.*\)\"|path = \"$PWD\/\1.zip\"|" $manifest_name >> /work/manifest.toml
	done < $TOP/tools/permslip_$name
	popd
}

mkdir /work/hubris
pushd /work/hubris
download_region_manifests https://permslip-staging.corp.oxide.computer staging
download_region_manifests https://signer-us-west.corp.oxide.computer production
popd

/work/tufaceous assemble --no-generate-key /work/manifest.toml /work/repo-rot-all.zip
digest -a sha256 /work/repo-rot-all.zip > /work/repo-rot-all.zip.sha256.txt
