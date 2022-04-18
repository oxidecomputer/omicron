#!/bin/bash
#
# Make me a Gimlet!
#
# The entire control plane stack is designed to run and operate on the Oxide
# rack, in each Gimlet. But Gimlet's don't quite exist yet. In the meantime,
# this script can be used to create a few pieces of virtual hardware that
# _simulate_ a Gimlet, allowing us to develop software that approximates the
# eventual operation on Oxide hardware.
#
# See `docs/how-to-run.adoc` section "Make me a Gimlet" for more details.

set -e
set -u
set -x

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
OMICRON_TOP="$SOURCE_DIR/.."

# Select the physical link over which to simulate the Chelsio links
if [[ $# -ge 1 ]]; then
    PHYSICAL_LINK="$1"
else
    PHYSICAL_LINK="$(dladm show-phys -p -o LINK | head -1)"
fi
echo "Using $PHYSICAL_LINK as physical link"

function success {
    echo -e "\e[1;36m$1\e[0m"
}

# Create the ZFS zpools required for the sled agent, backed by file-based vdevs.
function ensure_zpools {
    # Find the list of zpools the sled agent expects, from its configuration
    # file.
    readarray -t ZPOOLS < <( \
            grep '"oxp_' "$OMICRON_TOP/smf/sled-agent/config.toml" | \
            sed 's/[ ",]//g' \
        )
    for ZPOOL in "${ZPOOLS[@]}"; do
        VDEV_PATH="$OMICRON_TOP/$ZPOOL.vdev"
        if ! [[ -f "$VDEV_PATH" ]]; then
            truncate -s 10GB "$VDEV_PATH"
        fi
        success "ZFS vdev $VDEV_PATH exists"
        if [[ -z "$(zpool list -o name | grep $ZPOOL)" ]]; then
            zpool create "$ZPOOL" "$VDEV_PATH"
        fi
        success "ZFS zpool $ZPOOL exists"
    done
}

# Create VNICs to represent the Chelsio physical links
#
# Arguments:
#   $1: Optional name of the physical link to use. If not provided, use the
#   first physical link available on the machine.
function ensure_simulated_chelsios {
    local PHYSICAL_LINK="$1"
    VNIC_NAMES=("vioif0" "vioif1")
    for VNIC in "${VNIC_NAMES[@]}"; do
        if [[ -z "$(dladm show-vnic -p -o LINK "$VNIC")" ]]; then
            dladm create-vnic -t -l "$PHYSICAL_LINK" "$VNIC"
        fi
        success "VNIC $VNIC exists"
        if [[ -z "$(ipadm show-addr -p -o ADDR "$VNIC/v6")" ]]; then
            ipadm create-addr -t -T addrconf "$VNIC/v6"
        fi
        success "IP address $VNIC/v6 exists"
    done

    # Create an address on the underlay network
    UNDERLAY_ADDR="lo0/underlay"
    if [[ -z "$(ipadm show-addr -p -o ADDR "$UNDERLAY_ADDR")" ]]; then
        ipadm create-addr -t -T static -a fd00:1::1/64 lo0/underlay
    fi
    success "IP address $UNDERLAY_ADDR exists"
}

function ensure_xde_driver {
    # Always remove the driver first. There seems to be a bug in the driver,
    # preventing it from showing up in `modinfo` on boot, even if it's actually
    # installed.
    if [[ -z "$(modinfo | grep xde)" ]]; then
        rem_drv xde
        add_drv xde
    fi
}

function ensure_run_as_root {
    if [[ "$(id -u)" -ne 0 ]]; then
        echo "This script must be run as root"
        exit 1
    fi
}

ensure_run_as_root
ensure_zpools
ensure_simulated_chelsios "$PHYSICAL_LINK"
ensure_xde_driver
