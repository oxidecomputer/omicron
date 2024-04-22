#!/usr/bin/env bash

set -x
set -e

mkdir -p /softnpu-zone
mkdir -p /opt/oxide/softnpu/stuff
cp tools/scrimlet/softnpu.toml /opt/oxide/softnpu/stuff/
cp out/softnpu/libsidecar_lite.so /opt/oxide/softnpu/stuff/
cp out/softnpu/softnpu /opt/oxide/softnpu/stuff/
cp out/softnpu/scadm /opt/oxide/softnpu/stuff/

zfs create -p -o mountpoint=/softnpu-zone rpool/softnpu-zone

pkg set-publisher --search-first helios-dev

zonecfg -z softnpu -f tools/scrimlet/softnpu-zone.txt
zoneadm -z softnpu install
zoneadm -z softnpu boot

pkg set-publisher --search-first helios-netdev
