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
    exit 1
fi

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."
OMICRON_TOP="$PWD"
OUT_DIR="$OMICRON_TOP/out"
XDE_DIR="$OUT_DIR/xde"
mkdir -p "$XDE_DIR"

# Create a temporary directory in which to download artifacts, removing it on
# exit
DOWNLOAD_DIR="$(mktemp -t -d)"
trap remove_download_dir EXIT ERR

function remove_download_dir {
    rm -rf $DOWNLOAD_DIR
}

# Compute the SHA256 of the path in $1, returning just the sum
function file_sha {
    sha256sum "$1" | cut -d ' ' -f 1
}

# Download a file from $1 and compare its sha256 to the value provided in $2
function download_and_check_sha {
    local URL="$1"
    local SHA="$2"
    local FILENAME="$(basename "$URL")"
    local DOWNLOAD_PATH="$(mktemp -p $DOWNLOAD_DIR)"
    local OUT_PATH="$XDE_DIR/$FILENAME"

    # Check if the file already exists, with the expected SHA
    if ! [[ -f "$OUT_PATH" ]] || [[ "$SHA" != "$(file_sha "$OUT_PATH")" ]]; then
        curl -L -o "$DOWNLOAD_PATH" "$URL" 2> /dev/null
        local ACTUAL_SHA="$(sha256sum "$DOWNLOAD_PATH" | cut -d ' ' -f 1)"
        if [[ "$ACTUAL_SHA" != "$SHA" ]]; then
            echo "SHA mismatch downloding file $FILENAME"
            exit 1
        fi
        mv -f "$DOWNLOAD_PATH" "$OUT_PATH"
        echo "\"$OUT_PATH\" downloaded and has verified SHA"
    else
        echo "\"$OUT_PATH\" already exists with correct SHA"
    fi
}

# Download a SHA-256 sum output from $1 and return just the SHA
function sha_from_url {
    local SHA_URL="$1"
    curl -L "$SHA_URL" 2> /dev/null | cut -d ' ' -f 1
}

# Add the publisher specified by the provided path. If that publisher already
# exists, set the origin instead. If more than one publisher with that name
# exists, abort with an error.
function add_publisher {
    local ARCHIVE_PATH="$1"
    local PUBLISHER_NAME="$(pkgrepo info -H -s "$ARCHIVE_PATH" | cut -d ' ' -f 1)"
    local N_PUBLISHERS="$(pkg publisher | grep -c "$PUBLISHER_NAME")"
    if [[ "$N_PUBLISHERS" -gt 1 ]]; then
        echo "More than one publisher named \"$PUBLISHER_NAME\" found"
        echo "Removing all publishers and installing from scratch"
        pfexec pkg unset-publisher "$PUBLISHER_NAME"
        pfexec pkg set-publisher -p "$ARCHIVE_PATH" --search-first
    elif [[ "$N_PUBLISHERS" -eq 1 ]]; then
        echo "Publisher \"$PUBLISHER_NAME\" already exists, setting"
        echo "the origin to "$ARCHIVE_PATH""
        pfexec pkg set-publisher --origin-uri "$ARCHIVE_PATH" --search-first "$PUBLISHER_NAME"
    else
        echo "Publisher \"$PUBLISHER_NAME\" does not exist, adding"
        pfexec pkg set-publisher -p "$ARCHIVE_PATH" --search-first
    fi
}

# The xde driver no longer requires separate kernel bits as of API version 21
# see https://github.com/oxidecomputer/opte/pull/321. Check if an older version
# of the driver is installed and prompt the user to remove it first.
RC=0
pkg info -lq driver/network/opte || RC=$?
if [[ "$RC" -eq 0 ]]; then
    # Grab the minor version of the package which corresponds to the API version
    # and prompt the user to run the uninstall script first if the version is < 21
    OPTE_VERSION="$(pkg info -l driver/network/opte | grep Version | tr -s ' ' | cut -d ' ' -f 3 | cut -d '.' -f 2)"
    if [[ "$OPTE_VERSION" -lt 21 ]]; then
        echo "The xde driver no longer requires custom kernel bits."
        echo "Please run \`tools/uninstall_opte.sh\` first to remove the old xde driver and associated kernel bits."
        exit 1
    fi
fi

# While separate kernel bits are no longer required, we still need to make sure that the
# required APIs are available i.e., a build including https://www.illumos.org/issues/15342
# Just checking for the presence of the mac_getinfo(9f) man page is a good enough proxy for this.
RC=0
man -l mac_getinfo || RC=$?
if [[ "$RC" -ne 0 ]]; then
    echo "xde driver requires updated kernel bits."
    echo "Please run \`pkg update\` first."
    exit 1
fi

# `helios-netdev` provides the xde kernel driver and the `opteadm` userland tool
# for interacting with it.
HELIOS_NETDEV_BASE_URL="https://buildomat.eng.oxide.computer/public/file/oxidecomputer/opte/repo"
HELIOS_NETDEV_COMMIT="b274cad9e981cbb7de1d3a48834b1f97500a13a8"
HELIOS_NETDEV_REPO_URL="$HELIOS_NETDEV_BASE_URL/$HELIOS_NETDEV_COMMIT/opte.p5p"
HELIOS_NETDEV_REPO_SHA_URL="$HELIOS_NETDEV_BASE_URL/$HELIOS_NETDEV_COMMIT/opte.p5p.sha256"
HELIOS_NETDEV_REPO_PATH="$XDE_DIR/$(basename "$HELIOS_NETDEV_REPO_URL")"

# Download and verify the package repositories
download_and_check_sha "$HELIOS_NETDEV_REPO_URL" "$(sha_from_url "$HELIOS_NETDEV_REPO_SHA_URL")"

# Add the OPTE and xde repositories and update packages.
add_publisher "$HELIOS_NETDEV_REPO_PATH"

# Actually install the xde kernel module and opteadm tool
RC=0
pfexec pkg install -v pkg://helios-netdev/driver/network/opte || RC=$?
if [[ "$RC" -ne 0 ]] && [[ "$RC" -ne 4 ]]; then
    echo "Installing xde kernel driver and opteadm tool failed"
    exit "$RC"
fi

# Check the user's path
RC=0
which opteadm > /dev/null || RC=$?
if [[ "$RC" -ne 0 ]]; then
    echo "The \`opteadm\` administration tool is not on your path."
    echo "You may add \"/opt/oxide/opte/bin\" to your path to access it."
fi
