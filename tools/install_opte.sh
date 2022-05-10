#!/bin/bash
#
# Small tool to install OPTE and the xde kernel driver and ONU bits.

set -e
set -u
set -x

MARKER=/etc/opt/oxide/NO_INSTALL
if [[ -f "$MARKER" ]]; then
    echo "This system has the marker file $MARKER, aborting." >&2
    exit 1
fi

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

# Echo the stickiness, 'sticky' or 'non-sticky' of the `helios-dev` publisher
function helios_dev_stickiness {
    local LINE="$(pkg publisher | grep '^helios-dev')"
    if [[ -z "$LINE" ]]; then
        echo "Expected a publisher named helios-dev, exiting!"
        exit 1
    fi
    if [[ -z "$(echo "$LINE" | grep 'non-sticky')" ]]; then
        echo "sticky"
    else
        echo "non-sticky"
    fi
}

# Ensure that the `helios-dev` publisher is non-sticky. This does not modify the
# publisher, if it is already non-sticky.
function ensure_helios_dev_is_non_sticky {
    local STICKINESS="$(helios_dev_stickiness)"
    if [[ "$STICKINESS" = "sticky" ]]; then
        pkg set-publisher --non-sticky helios-dev
        STICKINESS="$(helios_dev_stickiness)"
        if [[ "$STICKINESS" = "sticky" ]]; then
            echo "Failed to make helios-dev publisher non-sticky"
            exit 1
        fi
    else
        echo "helios-dev publisher is already non-sticky"
    fi
}

function verify_publisher_search_order {
    local EXPECTED=("on-nightly" "helios-netdev" "helios-dev")
    local N_EXPECTED="${#EXPECTED[*]}"
    readarray -t ACTUAL < <(pkg publisher -H | cut -d ' ' -f 1)
    local N_ACTUAL="${#ACTUAL[*]}"
    if [[ $N_EXPECTED -ne $N_ACTUAL ]]; then
        echo "Mismatched number of publishers, expected: $N_EXPECTED, found: $N_ACTUAL"
        exit 1
    fi
    for ((i=0;i<N_ACTUAL;i++))
    do
        if [[ "${EXPECTED[i]}" != "${ACTUAL[i]}" ]]; then
            echo "Mismatched publishers: ${EXPECTED[i]} vs ${ACTUAL[i]}"
            exit 1
        fi
    done
}

# `helios-netdev` provides the xde kernel driver and the `opteadm` userland tool
# for interacting with it.
HELIOS_NETDEV_BASE_URL="https://buildomat.eng.oxide.computer/public/file/oxidecomputer/opte/repo"
HELIOS_NETDEV_COMMIT="b9980158540d15d44cfc5d17fc0a5d1848c5e1ae"
HELIOS_NETDEV_REPO_URL="$HELIOS_NETDEV_BASE_URL/$HELIOS_NETDEV_COMMIT/opte.p5p"
HELIOS_NETDEV_REPO_SHA_URL="$HELIOS_NETDEV_BASE_URL/$HELIOS_NETDEV_COMMIT/opte.p5p.sha256"
HELIOS_NETDEV_REPO_PATH="$XDE_DIR/$(basename "$HELIOS_NETDEV_REPO_URL")"

# The xde repo provides a full OS/Net incorporation, with updated kernel bits
# that the `xde` kernel module and OPTE rely on.
XDE_REPO_BASE_URL="https://buildomat.eng.oxide.computer/public/file/oxidecomputer/os-build/xde"
XDE_REPO_COMMIT="fc0717b76a92d1e317955ec33477133257982670"
XDE_REPO_URL="$XDE_REPO_BASE_URL/$XDE_REPO_COMMIT/repo.p5p"
XDE_REPO_SHA_URL="$XDE_REPO_BASE_URL/$XDE_REPO_COMMIT/repo.p5p.sha256"
XDE_REPO_PATH="$XDE_DIR/$(basename "$XDE_REPO_URL")"

# Download and verify the package repositorieies
download_and_check_sha "$HELIOS_NETDEV_REPO_URL" "$(sha_from_url "$HELIOS_NETDEV_REPO_SHA_URL")"
download_and_check_sha "$XDE_REPO_URL" "$(sha_from_url "$XDE_REPO_SHA_URL")"

# Set the `helios-dev` repo as non-sticky, meaning that packages that were
# originally provided by it may be updated by another repository, if that repo
# provides newer versions of the packages.
ensure_helios_dev_is_non_sticky

# Add the OPTE and xde repositories and update packages.
pkg set-publisher -p "$HELIOS_NETDEV_REPO_PATH" --search-first
pkg set-publisher -p "$XDE_REPO_PATH" --search-first

verify_publisher_search_order

# Actually update packages, handling case where no updates are needed
RC=0
pkg update || RC=$?;
if [[ "$RC" -ne 0 ]] && [[ "$RC" -ne 4 ]]; then
    echo "Adding OPTE and/or xde package repositories failed"
    exit "$RC"
fi

# Actually install the xde kernel module and opteadm tool
pkg install driver/network/opte
