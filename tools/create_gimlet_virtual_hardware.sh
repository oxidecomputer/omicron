#!/bin/bash
#
# Make me a Gimlet!
#
# The entire control plane stack is designed to run and operate on the Oxide
# rack, on each Gimlet. However, Gimlets are not always available for
# development. This script can be used to create a few pieces of virtual
# hardware that _simulate_ a Gimlet, allowing us to develop software that
# approximates operation on Oxide hardware.
#
# This script is specific to gimlet virtual hardware setup. If you only need a
# gimlet, use create_gimlet_virtual_hardware.sh.
#
# See `docs/how-to-run.adoc` section "Set up virtual hardware" for more details.

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

function success {
    set +x
    echo -e "\e[1;36m$1\e[0m"
    set -x
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
            VDEV_PATH="$OMICRON_TOP/$ZPOOL.vdev"
            if ! [[ -f "$VDEV_PATH" ]]; then
                dd if=/dev/zero of="$VDEV_PATH" bs=1 count=0 seek=6G
            fi
            success "ZFS vdev $VDEV_PATH exists"
            if [[ -z "$(zpool list -o name | grep $ZPOOL)" ]]; then
                zpool create -f "$ZPOOL" "$VDEV_PATH"
            fi
            success "ZFS zpool $ZPOOL exists"
        done
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
