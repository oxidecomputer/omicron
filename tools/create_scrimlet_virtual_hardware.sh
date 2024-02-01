#!/usr/bin/env bash
#
# Make me a Scrimlet!
#
# The entire control plane stack is designed to run and operate on the Oxide
# rack, on each Gimlet. However, Gimlets are not always available for
# development. This script can be used to create a few pieces of virtual
# hardware that _simulate_ a Gimlet, allowing us to develop software that
# approximates operation on Oxide hardware.
#
# This script is specific to scrimlet virtual hardware setup. If you only need a
# gimlet, use create_gimlet_virtual_hardware.sh.
#
# See `docs/how-to-run.adoc` section "Set up virtual hardware" for more details.

set -e
set -u
set -x

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
OMICRON_TOP="$SOURCE_DIR/.."

. "$SOURCE_DIR/virtual_hardware.sh"

TFP0=${TFP0:-igb0}
TFP1=${TFP1:-igb2}
TFP2=${TFP2:-cxgbe0}
TFP3=${TFP3:-cxgbe1}

# Select the physical uplink softnpu will use as an emulated sidecar uplink
# port.
PHYSICAL_LINK=${PHYSICAL_LINK:=$(dladm show-phys -p -o LINK | head -1)}
echo "Using $PHYSICAL_LINK as physical link"

# Create virtual links to represent the Chelsio physical links
#
# Arguments:
#   $1: Optional name of the physical link to use. If not provided, use the
#   first physical link available on the machine.
function ensure_uplink_vnic {
    local PHYSICAL_LINK="$1"
    if [[ -z "$(get_vnic_name_if_exists "up0")" ]]; then
        dladm create-vnic -t "up0" -l $PHYSICAL_LINK -m a8:e1:de:01:70:1d
    fi
    success "Vnic up0 exists"
}

function ensure_softnpu_zone {
    zoneadm list | grep -q sidecar_softnpu || {
        out/npuzone/npuzone create sidecar \
            --omicron-zone \
            --ports $TFP0,tfportrear0_0 \
            --ports $TFP1,tfportrear1_0 \
            --ports $TFP2,tfportrear2_0 \
            --ports $TFP3,tfportrear3_0 \
            --ports up0,tfportqsfp0_0
    }
    $SOURCE_DIR/scrimlet/softnpu-init.sh
    success "softnpu zone exists"
}

ensure_run_as_root
ensure_zpools
ensure_uplink_vnic "$PHYSICAL_LINK"
ensure_softnpu_zone
