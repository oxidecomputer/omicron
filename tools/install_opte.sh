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

# Grab the version of the opte package to install
OPTE_VERSION="$(cat "$OMICRON_TOP/tools/opte_version")"

# Actually install the xde kernel module and opteadm tool
RC=0
pfexec pkg install -v pkg://helios-dev/driver/network/opte@"$OPTE_VERSION" || RC=$?
if [[ "$RC" -eq 0 ]]; then
    echo "xde driver installed successfully"
elif [[ "$RC" -eq 4 ]]; then
    echo "Correct xde driver already installed"
else
    echo "Installing xde driver failed"
    exit "$RC"
fi

# Check the user's path
RC=0
which opteadm > /dev/null || RC=$?
if [[ "$RC" -ne 0 ]]; then
    echo "The \`opteadm\` administration tool is not on your path."
    echo "You may add \"/opt/oxide/opte/bin\" to your path to access it."
fi
