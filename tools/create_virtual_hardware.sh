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

. "$SOURCE_DIR/virtual_hardware.sh"

# Select the physical link over which to simulate the Chelsio links
PHYSICAL_LINK=${PHYSICAL_LINK:=$(dladm show-phys -p -o LINK | head -1)}
echo "Using $PHYSICAL_LINK as physical link"

# Create virtual links
#
# Arguments:
#   $1: Optional name of the physical uplink to use. If not provided, use the
#   first physical link available on the machine.
function ensure_simulated_links {
    local PHYSICAL_LINK="$1"
    INDICES=("0" "1")
    for I in "${INDICES[@]}"; do
        if [[ -z "$(get_simnet_name_if_exists "net$I")" ]]; then
            # sidecar ports
            dladm create-simnet -t "net$I"
            dladm create-simnet -t "sc${I}_0"
            dladm modify-simnet -t -p "net$I" "sc${I}_0"
            dladm set-linkprop -p mtu=1600 "sc${I}_0" # encap headroom
        fi
        success "Simnet net$I/sc${I}_0 exists"
    done

    if [[ -z "$(get_vnic_name_if_exists "sc0_1")" ]]; then
        dladm create-vnic -t "sc0_1" -l $PHYSICAL_LINK -m a8:e1:de:01:70:1d
    fi
    success "Vnic sc0_1 exists"
}

function ensure_softnpu_zone {
    zoneadm list | grep -q sidecar_softnpu || {
        if ! [[ -f "out/npuzone/npuzone" ]]; then
            echo "npuzone binary is not installed"
            echo "please re-run ./tools/install_prerequisites.sh"
            exit 1
        fi
        out/npuzone/npuzone create sidecar \
            --omicron-zone \
            --ports sc0_0,tfportrear0_0 \
            --ports sc0_1,tfportqsfp0_0
    }
    $SOURCE_DIR/scrimlet/softnpu-init.sh
    success "softnpu zone exists"
}

function warn_if_physical_link_and_no_proxy_arp {
    local PHYSICAL_LINK="$1"
    dladm show-phys "$PHYSICAL_LINK" || return
    if ! [[ -v PXA_START ]] || ! [[ -v PXA_END ]]; then
        warn "You are running with a real physical link, but have not\n\
set up the proxy-ARP environment variables PXA_{START,END}.\n\
This implies you're trying to slice out a portion of your\n\
local network for Omicron. The PXA_* variables are necessary\n\
to allow SoftNPU to respond to ARP requests for the portion\n\
of the network you've dedicated to Omicron. Things will not\n\
work until you add those.\n\
\n\
You must either destroy / recreate the Omicron environment,\n\
or run \`scadm standalone add-proxy-arp\` in the SoftNPU zone\n\
later to add those entries."
    fi
}

ensure_run_as_root
ensure_zpools
ensure_simulated_links "$PHYSICAL_LINK"
warn_if_physical_link_and_no_proxy_arp "$PHYSICAL_LINK"
ensure_softnpu_zone
