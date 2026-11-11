#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


size=`pfexec /usr/lib/rsrvrctl -q | grep Free | awk '{print $3}'`
let x=$size/1024

pfexec /usr/lib/rsrvrctl -r $x
