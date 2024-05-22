#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

COCKROACH_ADDR="$(svcprop -c -p config/cockroach_address "${SMF_FMRI}")"
HTTP_ADDR="$(svcprop -c -p config/http_address "${SMF_FMRI}")"

args=(
  '--config-file-path' "/opt/oxide/lib/svc/cockroach-admin/config.toml"
  '--path-to-cockroach-binary' "/opt/oxide/cockroachdb/bin/cockroach"
  '--cockroach-address' "$COCKROACH_ADDR"
  '--http-address' "$HTTP_ADDR"
)

exec /opt/oxide/cockroach-admin/bin/cockroach-admin "${args[@]}" &
