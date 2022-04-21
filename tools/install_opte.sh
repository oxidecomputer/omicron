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
XDE_DIR="$OUT_DIR/xde"
mkdir -p "$XDE_DIR"

# Compute the SHA256 of the path in $1, returning just the sum
function file_sha {
    sha256sum "$1" | cut -d ' ' -f 1
}

# Download a file from $1 and compare its sha256 to the value provided in $2
function download_and_check_sha {
    local URL="$1"
    local FILENAME="$(basename "$URL")"
    local OUT_PATH="$XDE_DIR/$FILENAME"
    local SHA="$2"

    # Check if the file already exists, with the expected SHA
    if ! [[ -f "$OUT_PATH" ]] || [[ "$SHA" != "$(file_sha "$OUT_PATH")" ]]; then
        curl -L -o "$OUT_PATH" "$URL" 2> /dev/null
        local ACTUAL_SHA="$(sha256sum "$OUT_PATH" | cut -d ' ' -f 1)"
        if [[ "$ACTUAL_SHA" != "$SHA" ]]; then
            echo "SHA mismatch downloding file $FILENAME"
            exit 1
        fi
    else
        echo "File $FILENAME already exists with correct SHA"
    fi
}

# Download a SHA-256 sum output from $1 and return just the SHA
function sha_from_url {
    local SHA_URL="$1"
    curl -L "$SHA_URL" 2> /dev/null | cut -d ' ' -f 1
}

# The `helios-netdev` provides the XDE kernel driver and the `opteadm` userland
# tool for interacting with it.
HELIOS_NETDEV_REPO_URL="https://buildomat.eng.oxide.computer/wg/0/artefact/01G11AT7E4XV9J1J54GE2YDJT6/CB4WF4BVgnbvf5NI573z9osAV2LNIKogPtWJ5sfW2cNxUYQO/01G11ATFVTWAC2HSNV148PQ4ER/01G11B5MPQRBX3Q5EF45YDAW6Q/opte-0.1.60.p5p"
HELIOS_NETDEV_REPO_SHA_URL="https://buildomat.eng.oxide.computer/wg/0/artefact/01G11AT7E4XV9J1J54GE2YDJT6/CB4WF4BVgnbvf5NI573z9osAV2LNIKogPtWJ5sfW2cNxUYQO/01G11ATFVTWAC2HSNV148PQ4ER/01G11B5MR60H4N13NJKGWEEA69/opte-0.1.60.p5p.sha256"
HELIOS_NETDEV_REPO_PATH="$XDE_DIR/$(basename "$HELIOS_NETDEV_REPO_URL")"

# The XDE repo provides a full OS/Net incorporation, with updated kernel bits
# that the `xde` kernel module and OPTE rely on.
XDE_REPO_URL="https://buildomat.eng.oxide.computer/wg/0/artefact/01G0ZKH44GQF88GB0GQBG9TQGW/7eOYj8L8E4MLrtvdTgGMyMu5qjYTRheV250bEvh2OkBrggX4/01G0ZKHBQ33K40S5ABZMRNWS5P/01G0ZYDDRXQ3Y4E5SG9QX8N9FK/repo.p5p"
XDE_REPO_SHA_URL="https://buildomat.eng.oxide.computer/wg/0/artefact/01G0ZKH44GQF88GB0GQBG9TQGW/7eOYj8L8E4MLrtvdTgGMyMu5qjYTRheV250bEvh2OkBrggX4/01G0ZKHBQ33K40S5ABZMRNWS5P/01G0ZYDJDMJAYHFV9Z6XVE30X5/repo.p5p.sha256"
XDE_REPO_PATH="$XDE_DIR/$(basename "$XDE_REPO_URL")"

# Download and verify the package repositorieies
download_and_check_sha "$HELIOS_NETDEV_REPO_URL" "$(sha_from_url "$HELIOS_NETDEV_REPO_SHA_URL")"
download_and_check_sha "$XDE_REPO_URL" "$(sha_from_url "$XDE_REPO_SHA_URL")"

# Set the `helios-dev` repo as non-sticky, meaning that packages that were
# originally provided by it may be updated by another repository, if that repo
# provides newer versions of the packages.
pkg set-publisher --non-sticky helios-dev

# Add the OPTE and XDE repositories and update packages.
pkg set-publisher -p "$HELIOS_NETDEV_REPO_PATH" --search-first
pkg set-publisher -p "$XDE_REPO_PATH" --search-first

# Actually update packages, handling case where no updates are needed
RC=0
pkg update || RC=$?;
if [[ "$RC" -eq 0 ]] || [[ "$RC" -eq 4 ]]; then
    return 0
else
    return "$RC"
fi

# Actually install the xde kernel module and opteadm tool
pkg install driver/network/opte
