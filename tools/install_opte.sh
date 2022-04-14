#!/bin/bash
#
# Small tool to install OPTE and the xde kernel driver and ONU bits.

set -e
set -u
set -x

if [[ "$(uname)" != "SunOS" ]]; then
    echo "This script is intended for Helios only"
fi

if [[ $(id -u) -ne 0 ]]; then
    echo "This must be run as root"
    exit 1
fi

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."
OMICRON_TOP="$PWD"
OUT_DIR="$OMICRON_TOP/out"
mkdir -p "$OUT_DIR"

# Download a file from $1 and compare its sha256 to the value provided in $2
function download_and_check_sha {
    local URL="$1"
    local FILENAME="$(basename "$URL")"
    local OUT_PATH="$OUT_DIR/$FILENAME"
    local SHA="$2"
    curl -L -o "$OUT_PATH" "$URL" 2> /dev/null
    local ACTUAL_SHA="$(sha256sum "$OUT_PATH" | cut -d ' ' -f 1)"
    if [[ "$ACTUAL_SHA" != "$SHA" ]]; then
        echo "SHA mismatch downloding file $FILENAME"
        exit 1
    fi
}

# Download a SHA-256 sum output from $1 and return just the SHA
function sha_from_url {
    local SHA_URL="$1"
    curl -L "$SHA_URL" 2> /dev/null | cut -d ' ' -f 1
}

# TODO(ben): Download SHA when on merged to master
OPTE_P5P_URL="https://buildomat.eng.oxide.computer/wg/0/artefact/01G0MRWX9Y0X46HBEJBW245DJY/PM097Agvf89uKmVRZ890z6saoeLp6RCcVsbYRa5PDv9DnLDT/01G0MRX6GMBV34CNANABXZXX25/01G0MSFZZWPFEQBW7JRS7ST99G/opte-0.1.58.p5p"
OPTE_P5P_SHA_URL="https://buildomat.eng.oxide.computer/wg/0/artefact/01G0MRWX9Y0X46HBEJBW245DJY/PM097Agvf89uKmVRZ890z6saoeLp6RCcVsbYRa5PDv9DnLDT/01G0MRX6GMBV34CNANABXZXX25/01G0MSG01CGP6TH9THNY39G88Z/opte-0.1.58.p5p.sha256"
OPTE_P5P_REPO_PATH="$OUT_DIR/$(basename "$OPTE_P5P_URL")"
XDE_URL="https://buildomat.eng.oxide.computer/wg/0/artefact/01G0DM53XR4E008D6ET5T8DXP6/wBWo0Jsg1AG19toIyAY23xAWhzmuNKmAsF6tL18ypZODNuHK/01G0DM5DMQHF5B89VGHZ05Z4E0/01G0DMHNYQ1NS7DBX8VG3JPAP0/xde"
XDE_SHA_URL="https://buildomat.eng.oxide.computer/wg/0/artefact/01G0DM53XR4E008D6ET5T8DXP6/wBWo0Jsg1AG19toIyAY23xAWhzmuNKmAsF6tL18ypZODNuHK/01G0DM5DMQHF5B89VGHZ05Z4E0/01G0DMHP47353961S3ETXBSD2T/xde.sha256"

download_and_check_sha "$OPTE_P5P_URL" "$(sha_from_url "$OPTE_P5P_SHA_URL")"
download_and_check_sha "$XDE_URL" "$(sha_from_url "$XDE_SHA_URL")"

# Move the XDE driver into it the expected location to allow operating on it
# with `add_drv` and `rem_drv`
DRIVER_DIR="/kernel/drv/amd64"
XDE_FILENAME="$(basename "$XDE_URL")"
if [[ -f "$DRIVER_DIR/$XDE_FILENAME" ]]; then
    echo "XDE driver is already installed, it will be replaced"
fi
mv -f "$OUT_DIR/$XDE_FILENAME" "$DRIVER_DIR"

# Add the OPTE P5P package repository (at the top of the search order) and
# update the OS packages. This may require a reboot.
pkg set-publisher -p "$OPTE_P5P_REPO_PATH" --search-first
pkg set-publisher --non-sticky helios-dev
pkg update
