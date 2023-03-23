#!/bin/bash

set -e
set -x

# Gateway ip is automatically configured based on the default route on your development machine
# Can be overridden by setting GATEWAY_IP
GATEWAY_IP=${GATEWAY_IP:=$(netstat -rn -f inet | grep default | awk -F ' ' '{print $2}')}
echo "Using $GATEWAY_IP as gateway ip"

# Gateway mac is determined automatically by inspecting the arp table on the development machine
gateway_mac=$(arp "$GATEWAY_IP" | awk -F ' ' '{print $4}')

# Sidecar Interface facing "sled"
./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" port create 1:0 100G RS

# Sidecar Interface facing external network
./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" port create 2:0 100G RS

# Configure sidecar local ipv6 addresses
./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" addr add 1:0 fe80::aae1:deff:fe01:701c
./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" addr add 2:0 fe80::aae1:deff:fe01:701d
./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" addr add 1:0 fd00:99::1

# Configure route to oxide rack
./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" route add fd00:1122:3344:0101::/64 1:0 fe80::aae1:deff:fe00:1
./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" arp add fe80::aae1:deff:fe00:1 a8:e1:de:00:00:01

# Configure default route
./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" route add 0.0.0.0/0 2:0 "$GATEWAY_IP"
./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" arp add "$GATEWAY_IP" "$gateway_mac"


./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" port list
./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" addr list
./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" route list
./out/softnpu/swadm -h "[fd00:1122:3344:101::2]" arp list
