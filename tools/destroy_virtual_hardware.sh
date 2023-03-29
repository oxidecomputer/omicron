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

function fail {
    warn "$1"
    exit 1
}

function verify_omicron_uninstalled {
    svcs "svc:/system/illumos/sled-agent:default" 2>&1 > /dev/null && \
        fail "Omicron is still installed, please run \`omicron-package uninstall\`, and then re-run this script"
}

function unload_xde_driver {
    local ID="$(modinfo | grep xde | cut -d ' ' -f 1)"
    if [[ "$ID" ]]; then
        modunload -i "$ID" || fail "Failed to unload xde driver"
    fi
    success "Verified the xde kernel driver is unloaded"
}

function try_remove_address {
    local ADDRESS="$1"
    if [[ "$(ipadm show-addr -p -o addr "$ADDRESS")" ]]; then
        ipadm delete-addr "$ADDRESS" || warn "Failed to delete address $ADDRESS"
    fi
    success "Verified address $ADDRESS does not exist"
}

function try_remove_interface {
    local IFACE="$1"
    if [[ "$(ipadm show-if -p -o IFNAME "$IFACE")" ]]; then
        ipadm delete-if "$IFACE" || warn "Failed to delete interface $IFACE"
    fi
    success "Verified IP interface $IFACE does not exist"
}

function try_remove_vnic {
    local LINK="$1"
    if [[ "$(dladm show-vnic -p -o LINK "$LINK")" ]]; then
        dladm delete-vnic "$LINK" || warn "Failed to delete VNIC link $LINK"
    fi
    success "Verified VNIC link $LINK does not exist"
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
        try_remove_simnet "sr0_$I"
        try_remove_simnet "scr0_$I"
    done
}

function try_destroy_zpools {
    readarray -t ZPOOLS < <(zfs list -d 0 -o name | grep "^oxp_")
    for ZPOOL in "${ZPOOLS[@]}"; do
        VDEV_FILE="$OMICRON_TOP/$ZPOOL.vdev"
        zfs destroy -r "$ZPOOL" && \
                zfs unmount "$ZPOOL" && \
                zpool destroy "$ZPOOL" && \
                rm -f "$VDEV_FILE" || \
                warn "Failed to remove ZFS pool and vdev: $ZPOOL"

        success "Verified ZFS pool and vdev $ZPOOL does not exist"
    done
}

function remove_softnpu_zone {
    zoneadm -z softnpu halt
    zoneadm -z softnpu uninstall -F
    zonecfg -z softnpu delete -F

    rm -rf /opt/oxide/softnpu/stuff
    zfs destroy rpool/softnpu-zone
    rm -rf /softnpu-zone
}

verify_omicron_uninstalled
unload_xde_driver
remove_softnpu_zone
try_remove_vnics
try_destroy_zpools
