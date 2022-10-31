#!/bin/bash

set -o errexit
set -o pipefail

args=(
        '--insecure'
        '--listen-addr' "$(svcprop -c -p config/listen_addr "${SMF_FMRI}")"
        '--store' "$(svcprop -c -p config/store "${SMF_FMRI}")"
)

exec /opt/oxide/cockroachdb/bin/cockroach start-single-node "${args[@]}"
