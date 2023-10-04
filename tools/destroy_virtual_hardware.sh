#!/bin/bash
#
# Unmake me a Gimlet!
#
# This tool undoes the operations of `./tools/create_virtual_hardware.sh`,
# destroying VNICs and ZFS zpools, to the extent possible. Note that if an
# operation fails, for example because a VNIC link is busy, a warning is
# printed. The user is responsible for deleting these entirely.

set -u
set -x

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.." || exit
OMICRON_TOP="$PWD"

. "$SOURCE_DIR/virtual_hardware.sh"

if [[ "$(id -u)" -ne 0 ]]; then
    echo "This must be run as root"
    exit 1
fi

function try_remove_interface {
    local IFACE="$1"
    if [[ "$(ipadm show-if -p -o IFNAME "$IFACE")" ]]; then
        ipadm delete-if "$IFACE" || warn "Failed to delete interface $IFACE"
    fi
    success "Verified IP interface $IFACE does not exist"
}

function try_remove_simnet {
    local LINK="$1"
    if [[ "$(dladm show-simnet -p -o LINK "$LINK")" ]]; then
        dladm delete-simnet -t "$LINK" || warn "Failed to delete simnet link $LINK"
    fi
    success "Verified simnet link $LINK does not exist"
}

function try_remove_vnics {
    try_remove_address "lo0/underlay"
    try_remove_interface sc0_1
    try_remove_vnic sc0_1
    INDICES=("0" "1")
    for I in "${INDICES[@]}"; do
        try_remove_interface "net$I"
        try_remove_simnet "net$I"
        try_remove_simnet "sc${I}_0"
    done
}

function remove_softnpu_zone {
    out/npuzone/npuzone destroy sidecar \
        --omicron-zone \
        --ports sc0_0,tfportrear0_0 \
        --ports sc0_1,tfportqsfp0_0
}

verify_omicron_uninstalled
unload_xde_driver
remove_softnpu_zone
try_remove_vnics
try_destroy_zpools
