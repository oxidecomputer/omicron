#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

ADDR="$(svcprop -c -p config/address "${SMF_FMRI}")"

args=(
  'run'
  '--config-file-path' "/opt/oxide/lib/svc/ntp-admin/config.toml"
  '--address' "$ADDR"
)

exec /opt/oxide/ntp-admin/bin/ntp-admin "${args[@]}" &
