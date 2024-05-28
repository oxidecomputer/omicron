#!/usr/bin/env bash

MARKER=/etc/opt/oxide/NO_INSTALL
if [[ -f "$MARKER" ]]; then
    echo "This system has the marker file $MARKER, aborting." >&2
    exit 1
fi

function success {
    set +x
    echo -e "\e[1;36m$1\e[0m"
    set -x
}

function warn {
    set +x
    echo -e "\e[1;31m$1\e[0m"
    set -x
}

function fail {
    warn "$1"
    exit 1
}

# Create the ZFS zpools required for the sled agent, backed by file-based vdevs.
function ensure_zpools {
    # Find the list of zpools the sled agent expects, from its configuration
    # file.
    ZPOOL_TYPES=('oxp_' 'oxi_')
    for ZPOOL_TYPE in "${ZPOOL_TYPES[@]}"; do
        readarray -t ZPOOLS < <( \
                grep "\"$ZPOOL_TYPE" "$OMICRON_TOP/smf/sled-agent/non-gimlet/config.toml" | \
                sed 's/[ ",]//g' \
            )
        for ZPOOL in "${ZPOOLS[@]}"; do
            echo "Zpool: [$ZPOOL]"
            VDEV_PATH="${ZPOOL_VDEV_DIR:-$OMICRON_TOP}/$ZPOOL.vdev"
            if ! [[ -f "$VDEV_PATH" ]]; then
                dd if=/dev/zero of="$VDEV_PATH" bs=1 count=0 seek=20G
            fi
            success "ZFS vdev $VDEV_PATH exists"
            if [[ -z "$(zpool list -o name | grep $ZPOOL)" ]]; then
                zpool create -o ashift=12 -f "$ZPOOL" "$VDEV_PATH"
            fi
            success "ZFS zpool $ZPOOL exists"
        done
    done
}

function try_destroy_zpools {
    ZPOOL_TYPES=('oxp_' 'oxi_')
    for ZPOOL_TYPE in "${ZPOOL_TYPES[@]}"; do
        readarray -t ZPOOLS < <(zfs list -d 0 -o name | grep "^$ZPOOL_TYPE")
        for ZPOOL in "${ZPOOLS[@]}"; do
            VDEV_FILE="${ZPOOL_VDEV_DIR:-$OMICRON_TOP}/$ZPOOL.vdev"
            zfs destroy -r "$ZPOOL" && \
                    (zfs unmount "$ZPOOL" || true) && \
                    zpool destroy "$ZPOOL" && \
                    rm -f "$VDEV_FILE" || \
                    warn "Failed to remove ZFS pool and vdev: $ZPOOL"

            success "Verified ZFS pool and vdev $ZPOOL does not exist"
        done
    done
}

# Return the name of a VNIC link if it exists, or the empty string if not.
#
# Arguments:
#   $1: The name of the VNIC to look for
function get_vnic_name_if_exists {
    dladm show-vnic -p -o LINK "$1" 2> /dev/null || echo ""
}

function get_simnet_name_if_exists {
    dladm show-simnet -p -o LINK "$1" 2> /dev/null || echo ""
}

function ensure_run_as_root {
    if [[ "$(id -u)" -ne 0 ]]; then
        echo "This script must be run as root"
        exit 1
    fi
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

function try_remove_vnic {
    local LINK="$1"
    if [[ "$(dladm show-vnic -p -o LINK "$LINK")" ]]; then
        dladm delete-vnic "$LINK" || warn "Failed to delete VNIC link $LINK"
    fi
    success "Verified VNIC link $LINK does not exist"
}

function try_remove_address {
    local ADDRESS="$1"
    if [[ "$(ipadm show-addr -p -o addr "$ADDRESS")" ]]; then
        ipadm delete-addr "$ADDRESS" || warn "Failed to delete address $ADDRESS"
    fi
    success "Verified address $ADDRESS does not exist"
}
