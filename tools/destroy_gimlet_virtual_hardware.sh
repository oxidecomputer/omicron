#!/usr/bin/env bash
#
# Unmake me a Gimlet!
#
# This tool undoes the operations of # `create_gimlet_virtual_hardware.sh`,
# destroying VNICs and ZFS zpools, to the extent possible. Note that if an
# operation fails, for example because a VNIC link is busy, a warning is
# printed. The user is responsible for deleting these entirely.

set -u
set -x

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.." || exit
OMICRON_TOP="$PWD"

. "$SOURCE_DIR/virtual_hardware.sh"

MARKER=/etc/opt/oxide/NO_INSTALL
if [[ -f "$MARKER" ]]; then
    echo "This system has the marker file $MARKER, aborting." >&2
    exit 1
fi

if [[ "$(id -u)" -ne 0 ]]; then
    echo "This must be run as root"
    exit 1
fi

function try_remove_addrs {
    try_remove_address "lo0/underlay"
}

verify_omicron_uninstalled
unload_xde_driver
try_remove_addrs
try_destroy_zpools
