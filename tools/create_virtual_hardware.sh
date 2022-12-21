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
PHYSICAL_LINK=${PHYSICAL_LINK:=$(dladm show-phys -p -o LINK | head -1)}
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
            grep '"oxp_' "$OMICRON_TOP/smf/sled-agent/nongimlet/config.toml" | \
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

function get_simnet_name_if_exists {
    dladm show-simnet -p -o LINK "$1" 2> /dev/null || echo ""
}

# Create virtual links to represent the Chelsio physical links
#
# Arguments:
#   $1: Optional name of the physical link to use. If not provided, use the
#   first physical link available on the machine.
function ensure_simulated_chelsios {
    local PHYSICAL_LINK="$1"
    INDICES=("0" "1")
    for I in "${INDICES[@]}"; do
        if [[ -z "$(get_simnet_name_if_exists "net$I")" ]]; then
            # sidecar ports
            dladm create-simnet -t "net$I"
            dladm create-simnet -t "sc${I}_0"
            dladm modify-simnet -t -p "net$I" "sc${I}_0"
            dladm set-linkprop -p mtu=1600 "net$I" # encap headroom
            dladm set-linkprop -p mtu=1600 "sc${I}_0" # encap headroom

            # corresponding scrimlet ports
            dladm create-simnet -t "sr0_$I"
            dladm create-simnet -t "scr0_$I"
            dladm modify-simnet -t -p "sr0_$I" "scr0_$I"
        fi
        success "Simnet net$I/sc${I}_0/sr0_$I/scr0_$I exists"
    done

    if [[ -z "$(get_vnic_name_if_exists "sc0_1")" ]]; then
        dladm create-vnic -t "sc0_1" -l $PHYSICAL_LINK -m a8:e1:de:01:70:1d
    fi
    success "Vnic sc0_1 exists"
}

function ensure_run_as_root {
    if [[ "$(id -u)" -ne 0 ]]; then
        echo "This script must be run as root"
        exit 1
    fi
}

function ensure_softnpu_zone {
    zoneadm list | grep -q softnpu || {
        mkdir -p /softnpu-zone
        mkdir -p /opt/oxide/softnpu/stuff
        cp tools/scrimlet/softnpu.toml /opt/oxide/softnpu/stuff/
        cp tools/scrimlet/softnpu-init.sh /opt/oxide/softnpu/stuff/
        cp out/softnpu/libsidecar_lite.so /opt/oxide/softnpu/stuff/
        cp out/softnpu/softnpu /opt/oxide/softnpu/stuff/
        cp out/softnpu/scadm /opt/oxide/softnpu/stuff/

        zfs create -p -o mountpoint=/softnpu-zone rpool/softnpu-zone

        # TODO-remove
        # Is this command still necessary with a omicron1 zone?
        # pkg set-publisher --search-first helios-dev

        zonecfg -z softnpu -f tools/scrimlet/softnpu-zone.txt
        zoneadm -z softnpu install
        zoneadm -z softnpu boot

        # TODO-remove
        # Is this command still necessary with a omicron1 zone?
        # pkg set-publisher --search-first helios-netdev
    }
    success "softnpu zone exists"
}

function enable_softnpu {
    zlogin softnpu uname -a || {
        echo "softnpu zone not ready"
        exit 1
    }
    zlogin softnpu pgrep softnpu || {
        zlogin softnpu /stuff/softnpu /stuff/softnpu.toml &
    }
    success "softnpu started"
}

function ensure_ext_ip_hack_disabled {
    grep "ext_ip_hack = 1;" /kernel/drv/xde.conf && {
       sed -i 's/ext_ip_hack = 1;/ext_ip_hack = 0;/g' /kernel/drv/xde.conf
    }

    grep "ext_ip_hack = 0;" /kernel/drv/xde.conf || {
        echo "failed to disable ext_ip_hack"
        exit 1
    }
    success "ext_ip_hack disabled"
}

ensure_run_as_root
ensure_ext_ip_hack_disabled
ensure_zpools
ensure_simulated_chelsios "$PHYSICAL_LINK"
ensure_softnpu_zone
enable_softnpu
