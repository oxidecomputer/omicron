#!/bin/bash
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
    svcs "svc:/oxide/sled-agent:default" 2>&1 > /dev/null && \
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

function try_remove_addrs {
    try_remove_address "lo0/underlay"
}

function try_destroy_zpools {
    ZPOOL_TYPES=('oxp_' 'oxi_')
    for ZPOOL_TYPE in "${ZPOOL_TYPES[@]}"; do
        readarray -t ZPOOLS < <(zfs list -d 0 -o name | grep "^$ZPOOL_TYPE")
        for ZPOOL in "${ZPOOLS[@]}"; do
            VDEV_FILE="$OMICRON_TOP/$ZPOOL.vdev"
            zfs destroy -r "$ZPOOL" && \
                    zfs unmount "$ZPOOL" && \
                    zpool destroy "$ZPOOL" && \
                    rm -f "$VDEV_FILE" || \
                    warn "Failed to remove ZFS pool and vdev: $ZPOOL"

            success "Verified ZFS pool and vdev $ZPOOL does not exist"
        done
    done
}

verify_omicron_uninstalled
unload_xde_driver
try_remove_addrs
try_destroy_zpools
