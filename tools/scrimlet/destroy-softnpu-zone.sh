#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


set -x
set -e

zoneadm -z softnpu halt
zoneadm -z softnpu uninstall
zonecfg -z softnpu delete

zfs destroy -r rpool/softnpu-zone
rm -rf /opt/oxide/softnpu/stuff
rm -rf /softnpu-zone
