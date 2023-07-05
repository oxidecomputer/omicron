#!/bin/bash

set -x
set -o errexit
set -o pipefail

. /lib/svc/share/smf_include.sh

LISTEN_ADDR="$(svcprop -c -p config/listen_addr "${SMF_FMRI}")"
LISTEN_PORT="$(svcprop -c -p config/listen_port "${SMF_FMRI}")"
DATASTORE="$(svcprop -c -p config/store "${SMF_FMRI}")"
DATALINK="$(svcprop -c -p config/datalink "${SMF_FMRI}")"
GATEWAY="$(svcprop -c -p config/gateway "${SMF_FMRI}")"

if [[ $DATALINK == unknown ]] || [[ $GATEWAY == unknown ]]; then
    printf 'ERROR: missing datalink or gateway\n' >&2
    exit "$SMF_EXIT_ERR_CONFIG"
fi

ipadm show-addr "$DATALINK/ll" || ipadm create-addr -t -T addrconf "$DATALINK/ll"
ipadm show-addr "$DATALINK/omicron6"  || ipadm create-addr -t -T static -a "$LISTEN_ADDR" "$DATALINK/omicron6"
route get -inet6 default -inet6 "$GATEWAY" || route add -inet6 default -inet6 "$GATEWAY"

# Retrieve addresses of the other clickhouse nodes, order them and assign them to be a replica or keeper node.
# In a follow up PR, keepers will be their own service.
#
# Populate corresponding template and copy to /opt/oxide/clickhouse/clickhouse/config.d/
# or /opt/oxide/clickhouse/clickhouse/keeper_config.xml
# Clickhouse recommends to have these files in `/etc/clickhouse-keeper/` and `/etc/clickhouse-server/config.d/`,
# but I'm not sure this makes sense to us since we're building the entire clickhouse binary instead of separate
# `clickhouse-server`, 'clickhouse-keeper' and 'clickhouse-client' binaries.
#
# If this is a clickhouse repica node then run:
args=(
  # TODO: These arguments won't be necessary anymore with the config templates
  "--log-file" "/var/tmp/clickhouse-server.log"
  "--errorlog-file" "/var/tmp/clickhouse-server.errlog"
  "--"
  "--path" "${DATASTORE}"
  "--listen_host" "$LISTEN_ADDR"
  "--http_port" "$LISTEN_PORT"
)

exec /opt/oxide/clickhouse/clickhouse server "${args[@]}" &
#
# If this is a clickhouse keeper node then run:
#
# exec /opt/oxide/clickhouse/clickhouse keeper enable &