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
cd "${SOURCE_DIR}/.."
OMICRON_TOP="$PWD"

if [[ "$(id -u)" -ne 0 ]]; then
    echo "This must be run as root"
    exit 1
fi

function warn {
    echo -e "\e[1;31m$1\e[0m"
}

function success {
    echo -e "\e[1;36m$1\e[0m"
}

function try_remove_address {
    local ADDRESS="$1"
    RC=0
    if [[ "$(ipadm show-addr -p -o addr "$ADDRESS")" ]]; then
        ipadm delete-addr "$ADDRESS"
        RC=$?
    fi
    if [[ $RC -eq 0 ]]; then
        success "Address $ADDRESS destroyed"
    else
        warn "Failed to delete address $ADDRESS"
    fi
}

function try_remove_vnic {
    local LINK="$1"
    RC=0
    if [[ "$(dladm show-vnic -p -o LINK "$LINK")" ]]; then
        dladm delete-vnic "$LINK"
        RC=$?
    fi
    if [[ $RC -eq 0 ]]; then
        success "VNIC link $LINK destroyed"
    else
        warn "Failed to delete VNIC link $LINK"
    fi
}

function try_remove_vnics {
    try_remove_address "lo0/underlay"
    VNIC_LINKS=("net0" "net1")
    for LINK in "${VNIC_LINKS[@]}"; do
        try_remove_vnic "$LINK"
    done
}

function try_destroy_zpools {
    readarray -t ZPOOLS < <(zfs list -d 0 -o name | grep "^oxp_")
    for ZPOOL in "${ZPOOLS[@]}"; do
        RC=0
        VDEV_FILE="$OMICRON_TOP/$ZPOOL.vdev"
        zfs destroy -r "$ZPOOL" && zfs unmount "$ZPOOL" && zpool destroy "$ZPOOL" && rm -f "$VDEV_FILE"
        RC=$?

        if [[ $RC -eq 0 ]]; then
            success "Removed ZFS pool and vdev: $ZPOOL"
        else
            warn "Failed to remove ZFS pool and vdev: $ZPOOL"
        fi
    done
}

try_remove_vnics
try_destroy_zpools
