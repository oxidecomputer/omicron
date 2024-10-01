#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

LISTEN_ADDR="$(svcprop -c -p config/listen_addr "${SMF_FMRI}")"
LISTEN_PORT="$(svcprop -c -p config/listen_port "${SMF_FMRI}")"
DATASTORE="$(svcprop -c -p config/store "${SMF_FMRI}")"

args=(
"--log-file" "/var/tmp/clickhouse-server.log"
"--errorlog-file" "/var/tmp/clickhouse-server.errlog"
"--config-file" "/opt/oxide/clickhouse/config.xml"
"--"
"--path" "${DATASTORE}"
"--listen_host" "$LISTEN_ADDR"
"--http_port" "$LISTEN_PORT"
)

exec /opt/oxide/clickhouse/clickhouse server "${args[@]}" &
