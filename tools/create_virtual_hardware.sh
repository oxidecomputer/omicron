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
SOFTNPU_MODE=${SOFTNPU_MODE:-zone};

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
            dladm set-linkprop -p mtu=9000 "sc${I}_0" # match emulated devices
        fi
        success "Simnet net$I/sc${I}_0 exists"
    done

    if [[ -z "$(get_vnic_name_if_exists "sc0_1")" ]]; then
        dladm create-vnic -t "sc0_1" -l "$PHYSICAL_LINK" -m a8:e1:de:01:70:1d
        if [[ -v PROMISC_FILT_OFF ]]; then
            dladm set-linkprop -p promisc-filtered=off sc0_1
        fi
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
            --ports sc0_1,tfportqsfp0_0 \
            --sidecar-lite-commit 6007a1b0ffe57ee738222867c343bde7e47bbf60 \
            --softnpu-commit dbab082dfa89da5db5ca2325c257089d2f130092
     }
    "$SOURCE_DIR"/scrimlet/softnpu-init.sh
    success "softnpu zone exists"
}

function warn_if_no_proxy_arp {
    if ! [[ -v PXA_START ]] || ! [[ -v PXA_END ]]; then
        warn \
"You have not set up the proxy-ARP environment variables PXA_START and\n\
PXA_END.  These variables are necessary to allow SoftNPU to respond to\n\
ARP requests for the portion of the network you've dedicated to Omicron.\n\
\n\
You must either destroy / recreate the Omicron environment with\n\
PXA_START and PXA_END set or run \`scadm standalone add-proxy-arp\`\n\
in the SoftNPU zone later to add those entries."
    fi
}

ensure_run_as_root
ensure_zpools

if [[ "$SOFTNPU_MODE" == "zone" ]]; then
    ensure_simulated_links "$PHYSICAL_LINK"
    warn_if_no_proxy_arp
    ensure_softnpu_zone
fi
