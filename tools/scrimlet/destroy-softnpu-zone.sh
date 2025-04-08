#!/usr/bin/env bash

set -x
set -e

zoneadm -z softnpu halt
zoneadm -z softnpu uninstall
zonecfg -z softnpu delete

zfs destroy -r rpool/softnpu-zone
rm -rf /opt/oxide/softnpu/stuff
rm -rf /softnpu-zone
