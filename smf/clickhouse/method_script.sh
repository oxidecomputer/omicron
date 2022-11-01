#!/bin/bash

set -x
set -o errexit
set -o pipefail

LISTEN_ADDR="$(svcprop -c -p config/listen_addr "${SMF_FMRI}")"
LISTEN_PORT="$(svcprop -c -p config/listen_port "${SMF_FMRI}")"
DATASTORE="$(svcprop -c -p config/store "${SMF_FMRI}")"
DATALINK="$(svcprop -c -p config/datalink "${SMF_FMRI}")"
GATEWAY="$(svcprop -c -p config/gateway "${SMF_FMRI}")"

ipadm show-addr "$DATALINK/linklocal" || ipadm create-addr -t -T addrconf "$DATALINK/linklocal"
ipadm show-addr "$DATALINK/omicron6"  || ipadm create-addr -t -T static -a "$LISTEN_ADDR" "$DATALINK/omicron6"
route get -inet6 default -inet6 "$GATEWAY" || route add -inet6 default -inet6 "$GATEWAY"

args=(
  "--log-file" "/var/tmp/clickhouse-server.log"
  "--errorlog-file" "/var/tmp/clickhouse-server.errlog"
  "--"
  "--path" "${DATASTORE}"
  "--listen_host" "$LISTEN_ADDR"
  "--http_port" "$LISTEN_PORT"
)

exec /opt/oxide/clickhouse/clickhouse server "${args[@]}"
