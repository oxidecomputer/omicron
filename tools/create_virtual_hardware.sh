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

MARKER=/etc/opt/oxide/NO_INSTALL
if [[ -f "$MARKER" ]]; then
    echo "This system has the marker file $MARKER, aborting." >&2
    exit 1
fi

# Select the physical link over which to simulate the Chelsio links
if [[ $# -ge 1 ]]; then
    PHYSICAL_LINK="$1"
else
    PHYSICAL_LINK="$(dladm show-phys -p -o LINK | head -1)"
fi
echo "Using $PHYSICAL_LINK as physical link"

function success {
    set +x
    echo -e "\e[1;36m$1\e[0m"
    set -x
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
            dd if=/dev/zero of="$VDEV_PATH" bs=1 count=0 seek=10G
        fi
        success "ZFS vdev $VDEV_PATH exists"
        if [[ -z "$(zpool list -o name | grep $ZPOOL)" ]]; then
            zpool create -f "$ZPOOL" "$VDEV_PATH"
        fi
        success "ZFS zpool $ZPOOL exists"
    done
}

# Return the name of a VNIC link if it exists, or the empty string if not.
#
# Arguments:
#   $1: The name of the VNIC to look for
function get_vnic_name_if_exists {
    dladm show-vnic -p -o LINK "$1" 2> /dev/null || echo ""
}

# Create VNICs to represent the Chelsio physical links
#
# Arguments:
#   $1: Optional name of the physical link to use. If not provided, use the
#   first physical link available on the machine.
function ensure_simulated_chelsios {
    local PHYSICAL_LINK="$1"
    VNIC_NAMES=("net0" "net1")
    for VNIC in "${VNIC_NAMES[@]}"; do
        if [[ -z "$(get_vnic_name_if_exists "$VNIC")" ]]; then
            dladm create-vnic -t -l "$PHYSICAL_LINK" "$VNIC"
        fi
        success "VNIC $VNIC exists"
    done
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
