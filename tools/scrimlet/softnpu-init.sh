#!/bin/bash

set -e
set -x

# Gateway ip is automatically configured based on the default route on your development machine
# Can be overridden by setting GATEWAY_IP
GATEWAY_IP=${GATEWAY_IP:=$(netstat -rn -f inet | grep default | awk -F ' ' '{print $2}')}
echo "Using $GATEWAY_IP as gateway ip"

if [[ ! -v GATEWAY_MAC ]]; then
    ping $GATEWAY_IP
    sleep 1
    ping $GATEWAY_IP
    sleep 1
    ping $GATEWAY_IP
    sleep 1
    ping $GATEWAY_IP
    sleep 1
fi

# Gateway mac is determined automatically by inspecting the arp table on the
# development machine. Can be overridden by setting GATEWAY_MAC
# TODO arp without -a seems broken on illumos
#   $ arp 192.168.21.1
#   192.168.21.1 (192.168.21.1) -- no entry

#   $ arp -a | grep 192.168.21.1
#   e1000g1 192.168.21.1         255.255.255.255          90:ec:77:2e:70:27
GATEWAY_MAC=${GATEWAY_MAC:=$(arp -a | grep "$GATEWAY_IP" | awk -F ' ' '{print $NF}')}
echo "Using $GATEWAY_MAC as gateway mac"

z_scadm () {
    pfexec zlogin sidecar_softnpu /softnpu/scadm \
        --server /softnpu/server \
        --client /softnpu/client \
        standalone \
        $@
}


# Configure upstream network gateway ARP entry
z_scadm add-arp-entry $GATEWAY_IP $GATEWAY_MAC

PXA_MAC=${PXA_MAC:-a8:e1:de:01:70:1d}

if [[ -v PXA_START ]]; then
    z_scadm add-proxy-arp $PXA_START $PXA_END $PXA_MAC
fi
z_scadm dump-state
