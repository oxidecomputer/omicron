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
        pfexec pkg set-publisher --non-sticky helios-dev
        STICKINESS="$(helios_dev_stickiness)"
        if [[ "$STICKINESS" = "sticky" ]]; then
            echo "Failed to make helios-dev publisher non-sticky"
            exit 1
        fi
    else
        echo "helios-dev publisher is already non-sticky"
    fi
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

# `helios-netdev` provides the xde kernel driver and the `opteadm` userland tool
# for interacting with it.
HELIOS_NETDEV_BASE_URL="https://buildomat.eng.oxide.computer/public/file/oxidecomputer/opte/repo"
HELIOS_NETDEV_COMMIT="f501445f5a6c275c79f08a876fff6a861df31d46"
HELIOS_NETDEV_REPO_URL="$HELIOS_NETDEV_BASE_URL/$HELIOS_NETDEV_COMMIT/opte.p5p"
HELIOS_NETDEV_REPO_SHA_URL="$HELIOS_NETDEV_BASE_URL/$HELIOS_NETDEV_COMMIT/opte.p5p.sha256"
HELIOS_NETDEV_REPO_PATH="$XDE_DIR/$(basename "$HELIOS_NETDEV_REPO_URL")"

# The stlouis repo provides a full OS/Net incorporation, with updated kernel bits
# that the `xde` kernel module and OPTE rely on.
STLOUIS_REPO_BASE_URL="https://buildomat.eng.oxide.computer/public/file/oxidecomputer/os-build/stlouis"
STLOUIS_REPO_COMMIT="1c8f32867ae3131a9b5c096af80b8058f78ef94f"
STLOUIS_REPO_URL="$STLOUIS_REPO_BASE_URL/$STLOUIS_REPO_COMMIT/repo.p5p"
STLOUIS_REPO_SHA_URL="$STLOUIS_REPO_BASE_URL/$STLOUIS_REPO_COMMIT/repo.p5p.sha256"
STLOUIS_REPO_PATH="$XDE_DIR/$(basename "$STLOUIS_REPO_URL")"

# Download and verify the package repositorieies
download_and_check_sha "$HELIOS_NETDEV_REPO_URL" "$(sha_from_url "$HELIOS_NETDEV_REPO_SHA_URL")"
download_and_check_sha "$STLOUIS_REPO_URL" "$(sha_from_url "$STLOUIS_REPO_SHA_URL")"

# Set the `helios-dev` repo as non-sticky, meaning that packages that were
# originally provided by it may be updated by another repository, if that repo
# provides newer versions of the packages.
ensure_helios_dev_is_non_sticky

# Add the OPTE and xde repositories and update packages.
add_publisher "$HELIOS_NETDEV_REPO_PATH"
add_publisher "$STLOUIS_REPO_PATH"

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

# Install the kernel bits required for the xde kernel driver to operate
# correctly
RC=0
pfexec pkg install -v pkg://on-nightly/consolidation/osnet/osnet-incorporation* || RC=$?
if [[ "$RC" -eq 0 ]]; then
    echo "The xde kernel driver, opteadm tool, and xde-related kernel bits"
    echo "have successfully been installed. A reboot may be required to activate"
    echo "the new boot environment, if the kernel has been changed (upgrade"
    echo "or downgrade)"
    exit 0
elif [[ "$RC" -eq 4 ]]; then
    echo "The kernel appears to be up-to-date for use with opte"
    exit 0
else
    echo "Installing kernel bits for xde failed"
    exit "$RC"
fi
