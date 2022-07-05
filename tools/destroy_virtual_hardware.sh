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

MARKER=/etc/opt/oxide/NO_INSTALL
if [[ -f "$MARKER" ]]; then
    echo "This system has the marker file $MARKER, aborting." >&2
    exit 1
fi

if [[ "$(id -u)" -ne 0 ]]; then
    echo "This must be run as root"
    exit 1
fi

function warn {
    set +x
    echo -e "\e[1;31m$1\e[0m"
    set -x
}

function success {
    set +x
    echo -e "\e[1;36m$1\e[0m"
    set -x
}

function verify_omicron_uninstalled {
    svcs "svc:/system/illumos/sled-agent:default" 2>&1 > /dev/null
    if [[ $? -eq 0 ]]; then
        set +x
        warn "Omicron is still installed, please run \`omicron-package uninstall\`, and then re-run this script"
        exit 1
    fi
}

function unload_xde_driver {
    local ID="$(modinfo | grep xde | cut -d ' ' -f 1)"
    if [[ "$ID" ]]; then
        local RC=0
        modunload -i "$ID"
        RC=$?
        if [[ $RC -ne 0 ]]; then
            warn "Failed to unload xde driver"
            exit 1
        fi
    fi
    success "Verified the xde kernel driver is unloaded"
}

function try_remove_address {
    local ADDRESS="$1"
    RC=0
    if [[ "$(ipadm show-addr -p -o addr "$ADDRESS")" ]]; then
        ipadm delete-addr "$ADDRESS"
        RC=$?
    fi
    if [[ $RC -eq 0 ]]; then
        success "Verified address $ADDRESS does not exist"
    else
        warn "Failed to delete address $ADDRESS"
    fi
}

function try_remove_interface {
    local IFACE="$1"
    RC=0
    if [[ "$(ipadm show-if -p -o IFNAME "$IFACE")" ]]; then
        ipadm delete-if "$IFACE"
        RC=$?
    fi
    if [[ $RC -eq 0 ]]; then
        success "Verified IP interface $IFACE does not exist"
    else
        warn "Failed to delete interface $IFACE"
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
        success "Verified VNIC link $LINK does not exist"
    else
        warn "Failed to delete VNIC link $LINK"
    fi
}

function try_remove_vnics {
    try_remove_address "lo0/underlay"
    VNIC_LINKS=("net0" "net1")
    for LINK in "${VNIC_LINKS[@]}"; do
        try_remove_interface "$LINK" && try_remove_vnic "$LINK"
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
            success "Verified ZFS pool and vdev $ZPOOL does not exist"
        else
            warn "Failed to remove ZFS pool and vdev: $ZPOOL"
        fi
    done
}

verify_omicron_uninstalled
unload_xde_driver
try_remove_vnics
try_destroy_zpools
