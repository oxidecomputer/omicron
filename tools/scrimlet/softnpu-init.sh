#!/bin/bash

set -e
set -x

./softnpuadm add-address6 fe80::aae1:deff:fe01:701c
./softnpuadm add-address6 fe80::aae1:deff:fe01:701d
./softnpuadm add-address6 fd00:99::1

./softnpuadm add-route6 fd00:1122:3344:0101:: 64 1 fe80::aae1:deff:fe00:1
./softnpuadm add-ndp-entry fe80::aae1:deff:fe00:1 a8:e1:de:00:00:01

# ========================================================
# Change these for your upstream nexthop gateway
#
./softnpuadm add-route4 0.0.0.0 0 2 10.100.0.1
#                                   ^^^^^^^^^^
#                                   upstream gateway
#
./softnpuadm add-arp-entry 10.100.0.1 90:ec:77:2e:70:27
#                          ^^^^^^^^^^ ^^^^^^^^^^^^^^^^^
#                          gateway ip    gateway mac
#
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

./softnpuadm add-nat4 10.100.0.6 1024 65535 fd00:1122:3344:0101::1 13609650 A8:40:25:F3:C8:3D

./softnpuadm set-mac 1 a8:e1:de:1:70:1c
./softnpuadm set-mac 2 a8:e1:de:1:70:1d

# ========================================================
# Change these for your ip pool
#
# The mac address is the one assigned to `vgw0`.
#
./softnpuadm add-proxy-arp 10.100.0.5 10.100.0.25 a8:e1:de:01:70:1d
#                          ^^^^^^^^^^ ^^^^^^^^^^^
#                           ip-pool     ip-pool
#                            begin        end

./softnpuadm dump-state
