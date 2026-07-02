#!/bin/bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

ZONE_ID="$(svcprop -c -p config/zone_id "${SMF_FMRI}")"
COCKROACH_ADDR="$(svcprop -c -p config/cockroach_address "${SMF_FMRI}")"
COCKROACH_HTTP_ADDR="$(svcprop -c -p config/cockroach_http_address "${SMF_FMRI}")"
HTTP_ADDR="$(svcprop -c -p config/http_address "${SMF_FMRI}")"

args=(
  'run'
  '--config-file-path' "/opt/oxide/lib/svc/cockroach-admin/config.toml"
  '--path-to-cockroach-binary' "/opt/oxide/cockroachdb/bin/cockroach"
  '--cockroach-address' "$COCKROACH_ADDR"
  '--cockroach-http-address' "$COCKROACH_HTTP_ADDR"
  '--http-address' "$HTTP_ADDR"
  '--zone-id' "$ZONE_ID"
)

exec /opt/oxide/cockroach-admin/bin/cockroach-admin "${args[@]}" &
