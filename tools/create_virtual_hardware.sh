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
PORT_COUNT=${PORT_COUNT:=2}
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

# These are the ports that connect the ASIC to the Switch Zone
# They are not mounted on a physical interface
function create_tfport {
    number="${1}_0"
    tfport="tfport$2$number"
    softnpu="softnpu$2$number"
    if [[ -z "$(get_simnet_name_if_exists "$tfport")" ]]; then
        dladm create-simnet -t "$tfport"
        dladm create-simnet -t "$softnpu"
        dladm modify-simnet -t -p "$tfport" "$softnpu"
    fi
    success "virtual $tfport exists"
}

# These are the "front facing" ports. These are mounted on a
# physical interface on your machine for external connectivity
# to the "rack"
function create_qsfp_port {
    target="qsfp$1"
    stub="front_stub$1"
    if [[ -z "$(get_vnic_name_if_exists "$target")" ]]; then
        if [[ -v $target ]]; then
            echo "found physical mapping for $target"
            mapping="${!target}"
            mapping_type=${mapping#*,}
            mapping_over=${mapping%,*}
            if [[ "$mapping_type" == "vnic" ]]; then
                dladm "create-$mapping_type" -t "$target" -l "$mapping_over"
            elif [[ "mapping_type" == "simnet" ]]; then
                dladm create-simnet -t "$target"
                dladm create-simnet -t "$mapping_over"
                dladm modify-simnet -t -p "$target" "$mapping_over"
            fi
            echo "mounted $target on $mapping_over"
        else
            echo "no mapping for $target found"
            dladm create-etherstub "$stub"
            dladm create-vnic -t "$target" -l "$stub"
            echo "mounted $target on $stub"
        fi
    fi
    success "$target exists"
}

# These are the "rear facing" ports. These are mounted on a
# physical interface so your "scrimlet" can have a direct
# network connection to another "gimlet" or "scrimlet"
function create_rear_port {
    target="rear$1"
    stub="rear_stub$1"
    if [[ -z "$(get_vnic_name_if_exists "$target")" ]]; then
        if [[ -v $target ]]; then
            echo "found physical mapping for $target"
            mapping="${!target}"
            mapping_type=${mapping#*,}
            mapping_over=${mapping%,*}
            if [[ "$mapping_type" == "vnic" ]]; then
                dladm create-vnic -t "$target" -l "$mapping_over"
            elif [[ "$mapping_type" == "simnet" ]]; then
                dladm create-simnet -t "$target"
                dladm modify-simnet -t -p "$target" "$mapping_over"
                dladm set-linkprop -p mtu=1600 "$target" # encap headroom
            fi
            echo "mounted $target on $mapping_over"
        else
            echo "no mapping for $target found"
            dladm create-etherstub "$stub"
            dladm create-vnic -t "$target" -l "$stub"
            echo "mounted $target on $stub"
        fi
    fi
    success "$target exists"
}

function ensure_switchports {
    source ./tools/scrimlet/port_mappings.conf
    count=$(($PORT_COUNT - 1))
    for i in $(seq 0 $count); do
        create_qsfp_port $i
        create_tfport $i "qsfp"
    done

    for i in $(seq 0 $count); do
        create_rear_port $i
        create_tfport $i "rear"
    done
}

function generate_softnpu_zone_config {
    file="./tools/scrimlet/generated-softnpu-zone.txt"
    cat > "$file" <<EOF
create
set brand=omicron1
set zonepath=/softnpu-zone
set ip-type=exclusive
set autoboot=false
add fs
    set dir=/stuff
    set special=/opt/oxide/softnpu/stuff
    set type=lofs
end
EOF

    count=$(($PORT_COUNT - 1))
    for i in $(seq 0 $count); do
        echo "add net" >> $file
        echo "    set physical=qsfp$i" >> $file
        echo "end" >> $file
        echo "add net" >> $file
        echo "    set physical=softnpuqsfp${i}_0" >> $file
        echo "end" >> $file
    done

    for i in $(seq 0 $count); do
        echo "add net" >> $file
        echo "    set physical=rear$i" >> $file
        echo "end" >> $file
        echo "add net" >> $file
        echo "    set physical=softnpurear${i}_0" >> $file
        echo "end" >> $file
    done

    echo "commit" >> $file

    success "configuration created"
}

function generate_softnpu_toml {
    file="./tools/scrimlet/generated-softnpu.toml"
    cat > "$file" <<EOF
p4_program = "/stuff/libsidecar_lite.so"
ports = [
EOF

    count=$(($PORT_COUNT - 1))
    for i in $(seq 0 $count); do
        echo "    { sidecar = \"rear$i\", scrimlet = \"softnpurear${i}_0\", mtu = 1600 }," >> $file
    done

    for i in $(seq 0 $count); do
        echo "    { sidecar = \"qsfp$i\", scrimlet = \"softnpuqsfp${i}_0\", mtu = 1500 }," >> $file
    done
    echo "]" >> $file
}

function ensure_chelsio_links {
    if [[ -z "$(get_simnet_name_if_exists "net0")" ]]; then
        dladm create-simnet -t "net0"
    fi

    if [[ -z "$(get_simnet_name_if_exists "net1")" ]]; then
        dladm create-simnet -t "net1"
    fi
    success "virtual chelsio links created"
}

function ensure_tfpkt0 {
    if [[ -z "$(get_simnet_name_if_exists "tfpkt0")" ]]; then
        dladm create-simnet -t "tfpkt0"
    fi
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
        cp tools/scrimlet/generated-softnpu.toml /opt/oxide/softnpu/stuff/
        cp tools/scrimlet/softnpu-init.sh /opt/oxide/softnpu/stuff/
        cp out/softnpu/libsidecar_lite.so /opt/oxide/softnpu/stuff/
        cp out/softnpu/softnpu /opt/oxide/softnpu/stuff/
        cp out/softnpu/scadm /opt/oxide/softnpu/stuff/

        zfs create -p -o mountpoint=/softnpu-zone rpool/softnpu-zone

        zonecfg -z softnpu -f tools/scrimlet/generated-softnpu-zone.txt
        zoneadm -z softnpu install
        zoneadm -z softnpu boot
    }
    success "softnpu zone exists"
}

function enable_softnpu {
    zlogin softnpu uname -a || {
        echo "softnpu zone not ready"
        exit 1
    }
    zlogin softnpu pgrep softnpu || {
        zlogin softnpu 'RUST_LOG=debug /stuff/softnpu /stuff/generated-softnpu.toml &> /softnpu.log &'
    }
    success "softnpu started"
}

ensure_run_as_root
ensure_chelsio_links
ensure_tfpkt0
ensure_switchports $PORT_COUNT
generate_softnpu_zone_config $PORT_COUNT
generate_softnpu_toml
ensure_zpools
ensure_softnpu_zone
enable_softnpu
