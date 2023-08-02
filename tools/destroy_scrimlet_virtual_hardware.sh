#!/bin/bash
#
# Unmake me a Scrimlet!
#
# This tool undoes the operations of # `create_scrimlet_virtual_hardware.sh`,
# destroying VNICs and ZFS zpools, to the extent possible. Note that if an
# operation fails, for example because a VNIC link is busy, a warning is
# printed. The user is responsible for deleting these entirely.

set -u

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."
OMICRON_TOP="$PWD"

. "$SOURCE_DIR/virtual_hardware.sh"

TFP0=${TFP0:-igb0}
TFP1=${TFP1:-igb2}
TFP2=${TFP2:-cxgbe0}
TFP3=${TFP3:-cxgbe1}

if [[ "$(id -u)" -ne 0 ]]; then
    echo "This must be run as root"
    exit 1
fi

function try_remove_vnics {
    try_remove_address "lo0/underlay"
    try_remove_vnic up0
}

function remove_softnpu_zone {
    out/npuzone/npuzone destroy sidecar \
        --omicron-zone \
        --ports $TFP0,tfportrear0_0 \
        --ports $TFP1,tfportrear1_0 \
        --ports $TFP2,tfportrear2_0 \
        --ports $TFP3,tfportrear3_0 \
        --ports up0,tfportqsfp0_0
}

verify_omicron_uninstalled
unload_xde_driver
remove_softnpu_zone
try_remove_vnics
try_destroy_zpools
